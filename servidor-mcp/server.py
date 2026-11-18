"""Servidor MCP da Central de Salas (Streamable HTTP, porta 7301, endpoint /mcp).

Tres tools, um resource e o ciclo MRTR na reserva. O servidor nao guarda nada
entre o input_required e o retry: o estado viaja no requestState, que o SDK sela
com AES-256-GCM usando a chave de REQUEST_STATE_SECRET.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Literal

from mcp.server.elicitation import ElicitationResult
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.resolve import Elicit, Resolve
from mcp.server.request_state import RequestStateSecurity
from pydantic import BaseModel, Field, create_model

DADOS = Path(os.environ.get("DADOS_DIR", Path(__file__).resolve().parent.parent / "dados"))
FUSO = timezone(timedelta(hours=-3))
JANELA_INICIO, JANELA_FIM = 8, 20
DURACAO_MAXIMA = timedelta(hours=2)
TTL_REQUEST_STATE = 600.0  # 10 minutos, dentro da faixa de 5 a 30 exigida

# --- chave de integridade: so de variavel de ambiente, nunca do codigo -------------------

segredo = os.environ.get("REQUEST_STATE_SECRET", "")
if len(segredo.encode()) < 32:
    sys.exit(
        "REQUEST_STATE_SECRET ausente ou com menos de 32 bytes. Gere uma com:\n"
        '  python3 -c "import secrets; print(secrets.token_hex(32))"'
    )

# --- dados ------------------------------------------------------------------------------

SALAS: list[dict[str, Any]] = json.loads((DADOS / "salas.json").read_text(encoding="utf-8"))
RESERVAS: list[dict[str, Any]] = json.loads((DADOS / "reservas.json").read_text(encoding="utf-8"))
POLITICA_TEXTO = (DADOS / "politica-de-uso.md").read_text(encoding="utf-8")
POLITICA_VERSAO = POLITICA_TEXTO.splitlines()[0].split(":", 1)[1].strip()

# --- modelos de saida (structuredContent) ----------------------------------------------


class SalaOut(BaseModel):
    id: str
    nome: str
    capacidade: int
    recursos: list[str]


class ListaDeSalas(BaseModel):
    salas: list[SalaOut]


class ConflitoOut(BaseModel):
    id: str
    inicio: str
    fim: str
    responsavel: str


class Disponibilidade(BaseModel):
    sala: str
    livre: bool
    conflitos: list[ConflitoOut]


class ReservaOut(BaseModel):
    reserva: str | None = None
    reservado: bool = True
    sala: str | None = None
    inicio: str | None = None
    fim: str | None = None
    responsavel: str | None = None
    politica: str | None = None
    motivo: str | None = None


# --- regras (so o servidor conhece o dominio) ------------------------------------------


def _parse(valor: str) -> datetime:
    try:
        dt = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    except ValueError:
        raise ToolError(f"Intervalo invalido: data/hora ilegivel: {valor}") from None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=FUSO)
    return dt


def validar(sala: str, inicio: str, fim: str) -> tuple[datetime, datetime]:
    if sala not in {s["id"] for s in SALAS}:
        raise ToolError(f"Sala inexistente: {sala}")
    ini, end = _parse(inicio), _parse(fim)
    if end <= ini:
        raise ToolError("Intervalo invalido: fim deve ser posterior a inicio")
    ini_sp, fim_sp = ini.astimezone(FUSO), end.astimezone(FUSO)
    abre = ini_sp.replace(hour=JANELA_INICIO, minute=0, second=0, microsecond=0)
    fecha = ini_sp.replace(hour=JANELA_FIM, minute=0, second=0, microsecond=0)
    if ini_sp < abre or fim_sp > fecha:
        raise ToolError("Fora da janela de uso: a politica permite reservas entre 08:00 e 20:00")
    if end - ini > DURACAO_MAXIMA:
        raise ToolError("Duracao acima do limite: a politica permite no maximo 2 horas")
    return ini, end


def conflitos(sala: str, ini: datetime, end: datetime) -> list[dict[str, Any]]:
    return [
        r
        for r in RESERVAS
        if r["sala"] == sala and _parse(r["inicio"]) < end and ini < _parse(r["fim"])
    ]


def alternativas(sala: str, ini: datetime, end: datetime) -> list[str]:
    capacidade = next(s["capacidade"] for s in SALAS if s["id"] == sala)
    livres = [
        s
        for s in SALAS
        if s["id"] != sala and s["capacidade"] >= capacidade and not conflitos(s["id"], ini, end)
    ]
    livres.sort(key=lambda s: (s["capacidade"], s["id"]))
    return [s["id"] for s in livres[:3]]


# --- MRTR: o resolver pergunta, o SDK transforma em input_required ---------------------


def escolha_de_sala(sala: str, inicio: str, fim: str) -> Elicit[BaseModel] | None:
    """Se a sala pedida esta ocupada, pede a escolha de uma alternativa (form mode).

    Nao ha canal de volta: o resolver apenas devolve a pergunta, o SDK encerra a
    resposta com resultType=input_required e o cliente volta com um request novo.
    """
    ini, end = validar(sala, inicio, fim)
    if not conflitos(sala, ini, end):
        return None
    opcoes = alternativas(sala, ini, end)
    if not opcoes:
        raise ToolError("Sem alternativas disponiveis no intervalo")
    esquema = create_model(
        "EscolhaDeSala",
        sala=(Literal[tuple(opcoes)], Field(description="Sala alternativa escolhida", title="Sala")),  # type: ignore[valid-type]
    )
    return Elicit("A sala pedida esta ocupada nesse intervalo. Escolha uma alternativa.", esquema)


# --- servidor ---------------------------------------------------------------------------

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s", force=True)


class LogDeRequests:
    """Middleware ASGI: registra no stderr todo POST JSON-RPC, inclusive os que o
    transporte rejeita por falta de _meta antes de chegarem ao roteamento do MCP."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or scope["method"] != "POST":
            return await self.app(scope, receive, send)
        mensagens: list[dict[str, Any]] = []
        corpo = b""
        while True:
            msg = await receive()
            mensagens.append(msg)
            if msg["type"] != "http.request":
                break
            corpo += msg.get("body", b"")
            if not msg.get("more_body"):
                break
        try:
            req = json.loads(corpo)
            meta = (req.get("params") or {}).get("_meta") or {}
            linha = f"metodo={req.get('method')} id={req.get('id')} traceparent={meta.get('traceparent')}"
        except (ValueError, AttributeError):
            linha = "metodo=? id=? traceparent=None (corpo nao e JSON-RPC)"
        print(f"[mcp] {linha}", file=sys.stderr, flush=True)

        pendentes = iter(mensagens)

        async def replay() -> Any:  # devolve o corpo lido e depois delega ao receive original
            return next(pendentes, None) or await receive()

        await self.app(scope, replay, send)


mcp = MCPServer(
    "central-de-salas",
    version="1.0.0",
    request_state_security=RequestStateSecurity(keys=[segredo], ttl=TTL_REQUEST_STATE),
)


@mcp.tool(description="Lista todas as salas com capacidade e recursos.")
def listar_salas() -> ListaDeSalas:
    return ListaDeSalas(salas=[SalaOut(**s) for s in SALAS])


@mcp.tool(description="Diz se uma sala esta livre no intervalo, e quais reservas conflitam.")
def consultar_disponibilidade(sala: str, inicio: str, fim: str) -> Disponibilidade:
    ini, end = validar(sala, inicio, fim)
    achados = conflitos(sala, ini, end)
    return Disponibilidade(
        sala=sala,
        livre=not achados,
        conflitos=[
            ConflitoOut(id=r["id"], inicio=r["inicio"], fim=r["fim"], responsavel=r["responsavel"])
            for r in achados
        ],
    )


@mcp.tool(description="Reserva uma sala. Se o intervalo estiver ocupado, pergunta qual alternativa usar.")
def reservar_sala(
    sala: str,
    inicio: str,
    fim: str,
    responsavel: str,
    escolha: Annotated[ElicitationResult[Any], Resolve(escolha_de_sala)],
) -> ReservaOut:
    if escolha.action != "accept":  # decline ou cancel: conclui sem reservar, sem erro
        return ReservaOut(reservado=False, motivo="recusado")
    destino = escolha.data.sala if escolha.data is not None else sala
    ini, end = validar(destino, inicio, fim)
    if conflitos(destino, ini, end):  # defesa: a sala escolhida tambem precisa estar livre
        raise ToolError("Sem alternativas disponiveis no intervalo")
    nova = {
        "id": f"res-{len(RESERVAS) + 1:04d}",
        "sala": destino,
        "inicio": inicio,
        "fim": fim,
        "responsavel": responsavel,
    }
    RESERVAS.append(nova)
    return ReservaOut(
        reserva=nova["id"],
        sala=destino,
        inicio=inicio,
        fim=fim,
        responsavel=responsavel,
        politica=POLITICA_VERSAO,
    )


@mcp.resource("politica://uso", mime_type="text/markdown", description="Politica de uso das salas")
def politica_de_uso() -> str:
    return POLITICA_TEXTO


if __name__ == "__main__":
    import uvicorn

    app = LogDeRequests(mcp.streamable_http_app(streamable_http_path="/mcp", stateless_http=True))
    uvicorn.run(
        app,
        host=os.environ.get("MCP_HOST", "127.0.0.1"),
        port=int(os.environ.get("MCP_PORT", "7301")),
        log_level="warning",
    )
