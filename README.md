# A Ponte: um agente A2A com MCP por dentro

Central de Salas da Hill Valley Tech. Dois processos separados:

- `servidor-mcp/`: servidor MCP (Streamable HTTP, porta 7301, endpoint `/mcp`) com três tools, um resource e o ciclo MRTR na reserva.
- `agente/`: agente que é **host MCP por dentro** (chama o servidor por HTTP) e **servidor A2A por fora** (porta 7300, endpoint `/a2a`, card em `/.well-known/agent-card.json`). Sem LLM.

Stack: Python 3.10+, SDK oficial `mcp` v2 (2.3.0, revisão `2026-07-28`), A2A v1.0 JSON-RPC. Versões travadas em `requirements.txt` e nos `pyproject.toml`.

## Como rodar

A partir de um clone limpo (Linux/macOS; no Windows troque `source .venv/bin/activate` por `.venv\Scripts\activate`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# chave de integridade do requestState (>= 32 bytes). Gere a sua, nunca commite:
export REQUEST_STATE_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
```

Terminal 1, servidor MCP (o stderr dele é o instrumento de depuração, deixe visível):

```bash
python3 servidor-mcp/server.py
```

Terminal 2, agente (mesmo venv):

```bash
cd agente && python3 agente.py
```

Terminal 3, validador (só biblioteca padrão), com os dois processos recém-iniciados:

```bash
python3 validador/validar.py --agente http://localhost:7300 --mcp http://localhost:7301
```

Variáveis opcionais (os padrões são os do enunciado): `MCP_HOST`, `MCP_PORT` (7301), `AGENT_HOST`, `AGENT_PORT` (7300), `AGENT_PUBLIC_URL` (URL do card), `MCP_URL` (onde o agente acha o servidor, `http://127.0.0.1:7301/mcp`). Sem `REQUEST_STATE_SECRET` válida o servidor MCP se recusa a subir.

## Onde a ponte acontece

Tudo está em `agente/agente.py`. Em `Agente._tratar_resposta`, quando o `tools/call` volta com `resultType: "input_required"`, o agente extrai a chave de `inputRequests`, as alternativas do `enum` do `requestedSchema` e o `requestState`, guarda os três (mais os argumentos originais) num objeto `Pausa` ligado àquela `Task`, e muda a Task para `TASK_STATE_INPUT_REQUIRED` com a mensagem `alternativas: ...`. A `Pausa` nunca sai pela API A2A (`Task.visao()` não a inclui). O caminho de volta é `Agente._continuar`: o `SendMessage` com `taskId` e `escolha=<sala>` vira `inputResponses` (`accept`, ou `decline` para `escolha=recusar`) e o agente repete o `tools/call` original por `McpHost.call_tool` (`agente/mcp_host.py`), com **id JSON-RPC novo**, a mesma chave de `inputRequests` e o `requestState` ecoado sem modificação. O agente não usa o cliente do SDK com callback de elicitation, porque ele responderia sozinho e a Task nunca pausaria; por isso `McpHost` é um cliente HTTP fino que devolve o JSON-RPC cru. Isso não reescreve o protocolo: ele só monta o `_meta`, os headers espelhados e o corpo, que é o que qualquer host faz. O `traceparent` do header A2A entra no `_meta` de todos os requests MCP da Task (trace-id preservado, span-id novo).

No servidor, o MRTR usa o mecanismo de primeira classe do SDK: o resolver `escolha_de_sala` em `servidor-mcp/server.py` devolve uma `Elicit` quando há conflito, e o SDK encerra a resposta com `input_required`. A regra de conflito, política e alternativas vive só no servidor.

## Decisões técnicas

- **Proteção do `requestState`:** o `RequestStateBoundary` do SDK sela o estado com AES-256-GCM (chave derivada por HKDF-SHA256 de `REQUEST_STATE_SECRET`), então além de íntegro ele é cifrado. O envelope sela também expiração, método, nome da tool, hash dos argumentos e audiência (`central-de-salas`). Adulteração, expiração ou argumentos divergentes no retry dão `-32602` (`Invalid or expired requestState`). O servidor não guarda nada entre o `input_required` e o retry, então ele continua válido após restart (testado: pedir, matar o servidor, subir de novo com a mesma chave, retry conclui a reserva).
- **Validade:** 10 minutos (`TTL_REQUEST_STATE = 600`), dentro da faixa de 5 a 30.
- **Chave:** só de `REQUEST_STATE_SECRET`, mínimo de 32 bytes verificado na subida (o SDK também recusa chaves curtas). Nada no código.
- **Capability:** o SDK devolve `-32021` com `data.requiredCapabilities` e HTTP 400 quando o cliente não declara `elicitation.form`. `_meta` sem `protocolVersion` ou `clientCapabilities` dá `-32602` com HTTP 400.
- **Stateless:** `stateless_http=True`; nenhum lado infere versão ou capabilities de request anterior. O agente mantém um `httpx.AsyncClient` vivo, que não carrega estado de protocolo.
- **Estado das Tasks:** em memória no agente (`Agente.tasks`, um dicionário por id), cada Task com o seu próprio `Pausa` e o seu lock. Duas Tasks pausadas ao mesmo tempo nunca trocam de `requestState`. Reservas também ficam em memória no servidor. Não sobrevivem a restart; o `requestState` sobrevive.
- **Log:** o servidor registra no stderr `[mcp] metodo=... id=... traceparent=...` por request. Requests rejeitados antes do roteamento por falta de `_meta` não passam pelo middleware e aparecem só como erro HTTP.
- **Determinismo:** sem LLM; o pedido é parseado por regex e a decisão é por regra.

## Saída do validador

```text
trace-id desta execucao: f72f776e7fbaeb263f407641651d2c81
procure esse valor no stderr do servidor MCP para conferir a propagacao do traceparent.

PASS 01 tools/list traz as tres tools
PASS 02 toda tool tem inputSchema de objeto
PASS 03 listar_salas devolve structuredContent e o mesmo JSON em texto
PASS 04 _meta sem protocolVersion devolve -32602 e HTTP 400
PASS 05 _meta sem clientCapabilities devolve -32602 e HTTP 400
PASS 06 tool inexistente e recusada, por -32602 ou por isError
PASS 07 resources/read de politica://uso devolve a politica
PASS 08 resources/read de URI inexistente devolve -32602
PASS 09 sala inexistente devolve isError com a mensagem exata
PASS 10 fora da janela devolve isError com a mensagem exata
PASS 11 duracao acima de 2h devolve isError com a mensagem exata
PASS 12 intervalo invertido devolve isError com a mensagem exata
PASS 13 conflito devolve input_required com inputRequests e requestState
PASS 14 a elicitation e form mode e oferece as alternativas na ordem certa
PASS 15 conflito sem a capability elicitation devolve -32021 e HTTP 400
PASS 16 retry com inputResponses e requestState conclui a reserva
PASS 17 requestState adulterado e rejeitado com -32602
PASS 18 argumentos adulterados no retry nao tomam efeito
PASS 19 recusa conclui sem reservar e sem isError
PASS 20 conflito sem alternativa possivel devolve isError com a mensagem exata

PASS 21 agent card responde 200 no well-known com JSON
PASS 22 o card declara a interface JSON-RPC com url e versao 1.0
PASS 23 o card declara a skill reservar-sala
PASS 24 SendMessage com sala livre conclui a Task
PASS 25 o artifact chama reserva e traz a versao da politica
PASS 26 GetTask devolve id, contextId e estado corrente
PASS 27 SendMessage com sala ocupada pausa a Task
PASS 28 a Task pausada lista as alternativas na ordem certa
PASS 29 escolha fora do enum mantem a Task pausada
PASS 30 a continuacao conclui a Task na sala escolhida
PASS 31 SendMessage em Task terminal e recusado
PASS 32 a recusa termina a Task em CANCELED
PASS 33 duas Tasks pausadas ao mesmo tempo concluem cada uma com a sua reserva
PASS 34 nenhuma resposta A2A carrega o requestState
PASS 35 sala inexistente termina a Task em FAILED com a mensagem da tool
PASS 36 o agente e deterministico: o mesmo pedido produz a mesma pausa

resumo: 36 passaram, 0 falharam, de 36 verificacoes
```
