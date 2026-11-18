"""Servidor MCP da Central de Salas (Streamable HTTP, porta 7301, endpoint /mcp).

Tres tools e um resource. Conflito de horario ainda e erro de execucao simples.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel

DADOS = Path(os.environ.get("DADOS_DIR", Path(__file__).resolve().parent.parent / "dados"))
FUSO = timezone(timedelta(hours=-3))
JANELA_INICIO, JANELA_FIM = 8, 20
DURACAO_MAXIMA = timedelta(hours=2)

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


# --- servidor ---------------------------------------------------------------------------

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s", force=True)


async def log_de_requests(ctx: Any, call_next: Any) -> Any:
    meta = dict(ctx.params.get("_meta") or {}) if ctx.params else {}
    print(
        f"[mcp] metodo={ctx.method} id={ctx.request_id} traceparent={meta.get('traceparent')}",
        file=sys.stderr,
        flush=True,
    )
    return await call_next(ctx)


mcp = MCPServer(
    "central-de-salas",
    version="1.0.0",
    middleware=[log_de_requests],
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
) -> ReservaOut:
    destino = sala
    ini, end = validar(destino, inicio, fim)
    if conflitos(destino, ini, end):
        raise ToolError("Sala ocupada no intervalo")
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
    mcp.run(
        "streamable-http",
        host=os.environ.get("MCP_HOST", "127.0.0.1"),
        port=int(os.environ.get("MCP_PORT", "7301")),
        streamable_http_path="/mcp",
        stateless_http=True,
    )
