"""Cliente MCP do agente (o agente como host MCP).

Fala Streamable HTTP com o servidor MCP, request por request, sem sessao: todo
request leva io.modelcontextprotocol/protocolVersion e clientCapabilities no
_meta, os headers espelhados do corpo (MCP-Protocol-Version, Mcp-Method, Mcp-Name)
e o traceparent da Task.

Por que nao usar o cliente do SDK: ele aceita um callback de elicitacao, responde
a pergunta sozinho e fecha o ciclo sem nunca devolver o input_required cru. O
agente precisa enxergar esse resultado para pausar a Task A2A, entao este modulo
devolve a resposta JSON-RPC do jeito que veio.
"""

from __future__ import annotations

import json
import secrets
from typing import Any

import httpx

PROTOCOL_VERSION = "2026-07-28"
CLIENT_CAPABILITIES = {"elicitation": {"form": {}}}
CLIENT_INFO = {"name": "agente-central-de-salas", "version": "1.0.0"}


class McpHost:
    def __init__(self, url: str) -> None:
        self.url = url
        self._http = httpx.AsyncClient(timeout=30)  # objeto vivo entre chamadas; sem estado de protocolo

    async def aclose(self) -> None:
        await self._http.aclose()

    async def request(
        self, method: str, params: dict[str, Any], traceparent: str | None, name: str | None = None
    ) -> dict[str, Any]:
        """Envia um request JSON-RPC com id novo e devolve o corpo JSON-RPC inteiro."""
        meta: dict[str, Any] = {
            "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
            "io.modelcontextprotocol/clientInfo": CLIENT_INFO,
            "io.modelcontextprotocol/clientCapabilities": CLIENT_CAPABILITIES,
        }
        if traceparent:
            meta["traceparent"] = traceparent
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
            "Mcp-Method": method,
        }
        if name:
            headers["Mcp-Name"] = name
        body = {
            "jsonrpc": "2.0",
            "id": secrets.token_hex(6),  # sempre id novo, inclusive no retry do MRTR
            "method": method,
            "params": {**params, "_meta": meta},
        }
        resposta = await self._http.post(self.url, json=body, headers=headers)
        return _decodificar(resposta)

    async def list_tools(self, traceparent: str | None) -> list[dict[str, Any]]:
        corpo = await self.request("tools/list", {}, traceparent)
        return (corpo.get("result") or {}).get("tools", [])

    async def read_resource(self, uri: str, traceparent: str | None) -> str:
        corpo = await self.request("resources/read", {"uri": uri}, traceparent, name=uri)
        contents = (corpo.get("result") or {}).get("contents") or []
        if not contents:
            raise RuntimeError(f"resource {uri} indisponivel: {json.dumps(corpo.get('error'))}")
        return contents[0].get("text", "")

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
        traceparent: str | None,
        input_responses: dict[str, Any] | None = None,
        request_state: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"name": name, "arguments": arguments}
        if input_responses is not None:
            params["inputResponses"] = input_responses
        if request_state is not None:
            params["requestState"] = request_state  # ecoado sem modificacao
        return await self.request("tools/call", params, traceparent, name=name)


def _decodificar(resposta: httpx.Response) -> dict[str, Any]:
    tipo = resposta.headers.get("content-type", "")
    texto = resposta.text
    if "text/event-stream" in tipo:  # o transporte pode responder em SSE; pega o evento com a resposta
        for linha in texto.splitlines():
            if linha.startswith("data:"):
                texto = linha[5:].strip()
                break
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        return {"error": {"code": -32603, "message": f"resposta invalida do servidor MCP (HTTP {resposta.status_code})"}}
