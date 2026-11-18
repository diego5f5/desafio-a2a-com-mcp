"""Agente da Central de Salas: servidor A2A por fora, host MCP por dentro.

Sem LLM: o pedido tem formato fixo e a decisao e por regra. O agente traduz
protocolo; conflito, politica e alternativas sao decisao do servidor MCP.

A ponte esta em `Agente._executar` (input_required do MCP -> TASK_STATE_INPUT_REQUIRED)
e em `Agente._continuar` (escolha do cliente -> retry MCP com id novo e requestState).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import sys
from datetime import datetime, timezone
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from mcp_host import McpHost

MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:7301/mcp")
PORT = int(os.environ.get("AGENT_PORT", "7300"))
HOST = os.environ.get("AGENT_HOST", "127.0.0.1")
PUBLIC_URL = os.environ.get("AGENT_PUBLIC_URL", f"http://127.0.0.1:{PORT}").rstrip("/")

SUBMITTED, WORKING = "TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"
INPUT_REQUIRED, COMPLETED = "TASK_STATE_INPUT_REQUIRED", "TASK_STATE_COMPLETED"
CANCELED, FAILED = "TASK_STATE_CANCELED", "TASK_STATE_FAILED"
TERMINAIS = {COMPLETED, CANCELED, FAILED}

FERRAMENTA_RESERVA = "reservar_sala"
URI_POLITICA = "politica://uso"

# Erros JSON-RPC / A2A
INVALID_REQUEST, METHOD_NOT_FOUND, INVALID_PARAMS = -32600, -32601, -32602
TASK_NOT_FOUND, UNSUPPORTED_OPERATION = -32001, -32004

AGENT_CARD = {
    "name": "Central de Salas",
    "description": "Reserva salas de reuniao da Hill Valley Tech.",
    "provider": {"organization": "Hill Valley Tech", "url": "https://hillvalley.example"},
    "version": "1.0.0",
    "supportedInterfaces": [{"url": f"{PUBLIC_URL}/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}],
    "capabilities": {"streaming": False, "pushNotifications": False, "extendedAgentCard": False},
    "defaultInputModes": ["text/plain"],
    "defaultOutputModes": ["text/plain"],
    "skills": [
        {
            "id": "reservar-sala",
            "name": "Reservar sala",
            "description": "Reserva uma sala em um intervalo. Se houver conflito, pergunta qual alternativa usar.",
            "tags": ["salas", "agenda"],
            "inputModes": ["text/plain"],
            "outputModes": ["text/plain"],
            "examples": [
                "reservar sala=sala-garagem inicio=2026-11-03T14:00:00-03:00 fim=2026-11-03T15:00:00-03:00 responsavel=Marty"
            ],
        }
    ],
}

PEDIDO = re.compile(r"^\s*reservar\s+sala=(\S+)\s+inicio=(\S+)\s+fim=(\S+)\s+responsavel=(.+?)\s*$")
ESCOLHA = re.compile(r"^\s*escolha=(\S+)\s*$")


def log(msg: str) -> None:
    print(f"[agente] {msg}", file=sys.stderr, flush=True)


def _id(prefixo: str) -> str:
    return f"{prefixo}-{secrets.token_hex(6)}"


def _traceparent(recebido: str | None) -> str:
    """Mantem o trace-id do cliente A2A e gera um span-id novo; sem header, abre um trace."""
    partes = (recebido or "").split("-")
    if len(partes) == 4 and len(partes[1]) == 32 and partes[1] != "0" * 32:
        trace_id, flags = partes[1], partes[3]
    else:
        trace_id, flags = secrets.token_hex(16), "01"
    return f"00-{trace_id}-{secrets.token_hex(8)}-{flags}"


class Pausa:
    """O que o agente guarda de uma Task interrompida. Privado: nunca sai pela API A2A."""

    def __init__(self, chave: str, alternativas: list[str], request_state: str, argumentos: dict[str, str]) -> None:
        self.chave = chave  # chave de inputRequests atribuida pelo servidor MCP
        self.alternativas = alternativas
        self.request_state = request_state  # opaco: guardar e ecoar, nunca abrir
        self.argumentos = argumentos  # argumentos do tools/call original


class Task:
    def __init__(self, traceparent: str) -> None:
        self.id = _id("task")
        self.context_id = _id("ctx")
        self.estado = SUBMITTED
        self.mensagem_status: dict[str, Any] | None = None
        self.timestamp = _agora()
        self.history: list[dict[str, Any]] = []
        self.artifacts: list[dict[str, Any]] = []
        self.traceparent = traceparent
        self.pausa: Pausa | None = None
        self.lock = asyncio.Lock()

    def visao(self) -> dict[str, Any]:  # o que o cliente A2A enxerga: sem pausa, sem requestState
        status: dict[str, Any] = {"state": self.estado, "timestamp": self.timestamp}
        if self.mensagem_status:
            status["message"] = self.mensagem_status
        return {
            "id": self.id,
            "contextId": self.context_id,
            "status": status,
            "history": list(self.history),
            "artifacts": list(self.artifacts),
        }


def _agora() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ErroRpc(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code, self.message = code, message


class Agente:
    def __init__(self) -> None:
        self.mcp = McpHost(MCP_URL)
        self.tasks: dict[str, Task] = {}  # estado das Tasks: em memoria, por Task

    # ----- helpers de Task -------------------------------------------------------------

    def _mudar(self, task: Task, estado: str, texto: str | None = None) -> None:
        if task.estado in TERMINAIS:  # estado terminal e definitivo
            return
        task.estado = estado
        task.timestamp = _agora()
        task.mensagem_status = None
        if texto is not None:
            msg = {
                "messageId": _id("msg"),
                "role": "ROLE_AGENT",
                "parts": [{"text": texto}],
                "taskId": task.id,
                "contextId": task.context_id,
            }
            task.mensagem_status = msg
            task.history.append(msg)
        log(f"task={task.id} estado={estado}")

    # ----- A2A: SendMessage ------------------------------------------------------------

    async def send_message(self, params: dict[str, Any], traceparent: str | None) -> dict[str, Any]:
        mensagem = params.get("message")
        if not isinstance(mensagem, dict) or not isinstance(mensagem.get("parts"), list):
            raise ErroRpc(INVALID_PARAMS, "message com parts e obrigatoria")
        texto = " ".join(p.get("text", "") for p in mensagem["parts"] if isinstance(p, dict))
        task_id = mensagem.get("taskId")

        if not task_id:
            task = Task(_traceparent(traceparent))
            self.tasks[task.id] = task
            task.history.append(mensagem)
            log(f"task={task.id} estado={SUBMITTED}")
            async with task.lock:
                await asyncio.sleep(0)  # cede o loop: um GetTask concorrente ainda ve SUBMITTED
                await self._executar(task, texto)
            return {"task": task.visao()}

        task = self.tasks.get(task_id)
        if task is None:
            raise ErroRpc(TASK_NOT_FOUND, f"Task nao encontrada: {task_id}")
        async with task.lock:
            if task.estado in TERMINAIS:
                raise ErroRpc(UNSUPPORTED_OPERATION, f"Task {task.id} ja esta em estado terminal ({task.estado})")
            if task.estado != INPUT_REQUIRED or task.pausa is None:
                raise ErroRpc(UNSUPPORTED_OPERATION, f"Task {task.id} nao aguarda resposta ({task.estado})")
            task.history.append(mensagem)
            if traceparent:
                task.traceparent = _traceparent(traceparent)
            await self._continuar(task, texto)
        return {"task": task.visao()}

    async def get_task(self, params: dict[str, Any]) -> dict[str, Any]:
        task = self.tasks.get(str(params.get("id", "")))
        if task is None:
            raise ErroRpc(TASK_NOT_FOUND, f"Task nao encontrada: {params.get('id')}")
        return {"task": task.visao()}

    # ----- a ponte ---------------------------------------------------------------------

    async def _executar(self, task: Task, texto: str) -> None:
        casou = PEDIDO.match(texto)
        if not casou:
            self._mudar(task, FAILED, "Pedido invalido. Use: reservar sala=<id> inicio=<iso8601> fim=<iso8601> responsavel=<nome>")
            return
        sala, inicio, fim, responsavel = casou.groups()
        argumentos = {"sala": sala, "inicio": inicio, "fim": fim, "responsavel": responsavel}
        self._mudar(task, WORKING)
        try:
            # Descoberta em runtime: tools/list antes do primeiro tools/call.
            tools = {t["name"] for t in await self.mcp.list_tools(task.traceparent)}
            if FERRAMENTA_RESERVA not in tools:
                self._mudar(task, FAILED, f"O servidor MCP nao oferece a ferramenta {FERRAMENTA_RESERVA}")
                return
            resposta = await self.mcp.call_tool(FERRAMENTA_RESERVA, argumentos, task.traceparent)
            await self._tratar_resposta(task, resposta, argumentos)
        except Exception as e:  # falha de transporte e falha da Task, nao do processo
            log(f"task={task.id} erro={e!r}")
            self._mudar(task, FAILED, f"Falha ao falar com o servidor MCP: {e}")

    async def _continuar(self, task: Task, texto: str) -> None:
        pausa = task.pausa
        assert pausa is not None
        casou = ESCOLHA.match(texto)
        escolha = casou.group(1) if casou else None
        if escolha is None or (escolha != "recusar" and escolha not in pausa.alternativas):
            # Escolha invalida: a Task continua pausada e repete a lista, byte a byte igual.
            self._mudar(task, INPUT_REQUIRED, _linha_alternativas(pausa.alternativas))
            return
        resposta_usuario = {"action": "decline"} if escolha == "recusar" else {"action": "accept", "content": {"sala": escolha}}
        self._mudar(task, WORKING)
        try:
            # O retry e um request novo (id novo): mesmos argumentos, mesma chave de
            # inputRequests, requestState ecoado sem modificacao.
            resposta = await self.mcp.call_tool(
                FERRAMENTA_RESERVA,
                pausa.argumentos,
                task.traceparent,
                input_responses={pausa.chave: resposta_usuario},
                request_state=pausa.request_state,
            )
            task.pausa = None
            await self._tratar_resposta(task, resposta, pausa.argumentos)
        except Exception as e:
            log(f"task={task.id} erro={e!r}")
            task.pausa = None
            self._mudar(task, FAILED, f"Falha ao falar com o servidor MCP: {e}")

    async def _tratar_resposta(self, task: Task, resposta: dict[str, Any], argumentos: dict[str, str]) -> None:
        erro = resposta.get("error")
        if erro:
            self._mudar(task, FAILED, str(erro.get("message", "erro do servidor MCP")))
            return
        resultado = resposta.get("result") or {}

        if resultado.get("resultType") == "input_required":
            # input_required do MCP vira TASK_STATE_INPUT_REQUIRED; o requestState fica na Task.
            pedidos = resultado.get("inputRequests") or {}
            chave = next(iter(pedidos), None)
            if chave is None or not resultado.get("requestState"):
                self._mudar(task, FAILED, "O servidor MCP pediu informacao sem inputRequests/requestState")
                return
            esquema = (pedidos[chave].get("params") or {}).get("requestedSchema") or {}
            campo = (esquema.get("properties") or {}).get("sala") or {}
            alternativas = list(campo.get("enum") or ([campo["const"]] if "const" in campo else []))
            task.pausa = Pausa(chave, alternativas, resultado["requestState"], argumentos)
            self._mudar(task, INPUT_REQUIRED, _linha_alternativas(alternativas))
            return

        texto = " ".join(c.get("text", "") for c in resultado.get("content", []) if c.get("type") == "text")
        if resultado.get("isError"):
            self._mudar(task, FAILED, texto)  # a mensagem exata da tool chega ao cliente A2A
            return

        dados = resultado.get("structuredContent") or {}
        if dados.get("reservado") is False:
            self._mudar(task, CANCELED, "Reserva recusada: nenhuma sala foi reservada.")
            return

        politica = await self._versao_da_politica(task.traceparent)
        reserva = {
            "reserva": dados.get("reserva"),
            "sala": dados.get("sala"),
            "inicio": dados.get("inicio"),
            "fim": dados.get("fim"),
            "responsavel": dados.get("responsavel"),
            "politica": politica,
        }
        task.artifacts.append(
            {"artifactId": _id("art"), "name": "reserva", "parts": [{"text": json.dumps(reserva)}]}
        )
        self._mudar(task, COMPLETED, f"Reserva {reserva['reserva']} confirmada na {reserva['sala']}.")

    async def _versao_da_politica(self, traceparent: str) -> str:
        """Le o resource politica://uso e extrai a versao da primeira linha."""
        texto = await self.mcp.read_resource(URI_POLITICA, traceparent)
        primeira = texto.splitlines()[0] if texto else ""
        return primeira.split(":", 1)[1].strip() if ":" in primeira else primeira.strip()


def _linha_alternativas(alternativas: list[str]) -> str:
    return "alternativas: " + ", ".join(alternativas)


# ----- HTTP ----------------------------------------------------------------------------

agente = Agente()


async def card(_: Request) -> JSONResponse:
    return JSONResponse(AGENT_CARD)


def _erro(id_: Any, code: int, message: str, http: int = 200) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}, status_code=http)


async def a2a(request: Request) -> JSONResponse:
    try:
        corpo = await request.json()
    except ValueError:
        return _erro(None, -32700, "JSON invalido")
    if not isinstance(corpo, dict) or corpo.get("jsonrpc") != "2.0" or "method" not in corpo:
        return _erro(corpo.get("id") if isinstance(corpo, dict) else None, INVALID_REQUEST, "request JSON-RPC invalido")
    id_, metodo = corpo.get("id"), corpo["method"]
    params = corpo.get("params") or {}
    traceparent = request.headers.get("traceparent")
    log(f"a2a metodo={metodo} id={id_} traceparent={traceparent}")
    try:
        if metodo == "SendMessage":
            resultado = await agente.send_message(params, traceparent)
        elif metodo == "GetTask":
            resultado = await agente.get_task(params)
        else:
            return _erro(id_, METHOD_NOT_FOUND, f"metodo nao suportado: {metodo}")
    except ErroRpc as e:
        return _erro(id_, e.code, e.message)
    return JSONResponse({"jsonrpc": "2.0", "id": id_, "result": resultado})


app = Starlette(
    routes=[
        Route("/.well-known/agent-card.json", card, methods=["GET"]),
        Route("/a2a", a2a, methods=["POST"]),
    ]
)

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
