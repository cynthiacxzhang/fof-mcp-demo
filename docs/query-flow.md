# Query Flow: How a Request Moves Through the System

This traces the full lifecycle of a user query — from natural language input to tool result — including what gets passed at each stage, where schema context gets used, and where feedback loops happen.

---

## 1. Startup & Handshake

When Claude Desktop launches, it starts the MCP server as a subprocess via stdio:

```
uv --directory /path/to/fof-mcp-demo run python main.py
```

The server and host perform an `initialize` handshake over JSON-RPC 2.0:

```
Host  →  { method: "initialize", params: { protocolVersion, clientInfo, capabilities } }
Server →  { result: { serverInfo, capabilities: { tools, resources } } }
```

The host then calls `tools/list` and `resources/list` to get the full manifest. This is what Claude sees when it decides which tools exist and what they do. The tool name, description, and parameter schema (auto-generated from Python type hints and docstrings by FastMCP) are all part of this manifest.

At this point `src/db.py` also initializes — DuckDB spins up in-memory, synthetic data is seeded across all four tables. This happens once and the connection is held for the lifetime of the server process.

---

## 2. User Query Intake

The user types something like:

> "What's our net outflow to external banks for Q4, and which institutions are getting the most?"

Claude (the host LLM) receives this along with:
- The conversation history
- The full tools manifest (names, descriptions, parameter schemas)
- Any MCP resources the user has attached as context

Claude does not call any tool yet. It first interprets the intent and maps it to one or more tool calls. The tool descriptions in `fof.py` are the main signal for this — they're essentially a routing layer written in plain English.

---

## 3. Tool Selection

Claude looks at the user's query and the available tools. For the example above it would likely plan:

1. `get_flow_summary(start_date="2025-10-01", end_date="2025-12-31")` — net outflow overview
2. `get_ofi_breakdown(start_date="2025-10-01", end_date="2025-12-31")` — institution-level breakdown

It may call these sequentially (using result 1 as context for interpreting result 2) or in parallel depending on whether there's a dependency.

**What drives tool selection:**
- Tool description text — the LLM matches intent to description
- Parameter names and types — guides argument construction
- Prior tool results in context — if `list_tables` was called earlier, Claude already knows the table names and date ranges available

If the user's query is ambiguous (e.g., "show me outflows" without a date), Claude may ask a clarifying question rather than guess — or it may call `list_tables` first to ground itself.

---

## 4. Tool Call (JSON-RPC)

Claude emits a `tools/call` request:

```json
{
  "method": "tools/call",
  "params": {
    "name": "get_ofi_breakdown",
    "arguments": {
      "start_date": "2025-10-01",
      "end_date": "2025-12-31",
      "top_n": 10
    }
  }
}
```

FastMCP receives this, validates the arguments against the tool's parameter schema, and routes to the registered function in `fof.py`. That function delegates to `src/core/tools.py`.

---

## 5. Execution in src/core/tools.py

Inside the tool function:

1. **Parameter validation** — type checks, whitelist checks (e.g., metric names in `get_trend`), SQL keyword check in `run_query`
2. **Query construction** — SQL is built with parameterized placeholders, never string-interpolated with user input (except whitelisted column/table names)
3. **DuckDB execution** — `get_conn()` returns the singleton connection, `.execute(sql, params)` runs the query
4. **Result formatting** — rows are rendered as plain-text ASCII tables, summaries are formatted with alignment and unicode indicators (▲▼ for direction, sparklines for trends)
5. **Return** — a plain string is returned to FastMCP, which wraps it in the JSON-RPC response

The result travels back to Claude as a tool result block in the conversation context.

---

## 6. Schema Context & the Validation Loop

The schema layer (`src/schemas.py`) acts like a lightweight RAG retrieval step. Rather than the LLM guessing column names from training data, it can:

1. Call `list_tables` → get all table names and descriptions
2. Call `get_schema("banking.transactions")` → get exact column names, types, nullability
3. Call `validate_metric("banking.transactions", "net_flow")` → confirm a specific column exists before using it

This chain is the feedback mechanism that prevents hallucinated column names from reaching `run_query`. In a well-prompted session, Claude will do this automatically before writing SQL — especially if the system instructions tell it to validate first.

```
User asks about "outflow"
        │
        ▼
list_tables()  ──►  knows tables exist
        │
        ▼
get_schema("banking.daily_aggregates")  ──►  sees ofi_outflow, total_debits
        │
        ▼
validate_metric("banking.daily_aggregates", "ofi_outflow")  ──►  PASS, type confirmed
        │
        ▼
run_query("SELECT SUM(ofi_outflow) FROM banking.daily_aggregates WHERE ...")
```

Each step passes information forward as context. This is retrieval-augmented generation in practice — the schema data is retrieved on demand and used to ground the next action, rather than relying on what the model knows from training.

---

## 7. Resources vs. Tools in Practice

**Resources** (`schema://tables`, `schema://tables/{name}`, `pipeline://lineage/{name}`) are fetched via `resources/read`:

```json
{ "method": "resources/read", "params": { "uri": "schema://tables" } }
```

They return static or semi-static blobs (the full schema catalog as markdown). The intended use is to attach them at the start of a session — Claude then has the full data model in its context window without needing to call `list_tables` or `get_schema` repeatedly mid-conversation.

Tools are for dynamic operations that need parameters and produce variable output. Resources are for reference material that doesn't change per-call.

In Claude Desktop, you attach resources via the paperclip → "Add from MCP" flow. In an API/agent setup, you'd `resources/read` them programmatically at session start and inject them into the system prompt.

---

## 8. Response Synthesis

Claude receives the tool result block(s) and synthesizes a natural language response. It interprets the formatted text output, pulls out key numbers, adds narrative context, and may suggest follow-up queries.

If a tool returns an error (malformed SQL, missing table), Claude sees that error text and can either self-correct (try a different query) or surface it to the user with an explanation.

---

## Full Sequence Diagram

```
User          Claude (Host)          MCP Server           DuckDB
 │                │                      │                   │
 │  natural       │                      │                   │
 │  language ────►│                      │                   │
 │                │  tools/list ─────────►                   │
 │                │◄──── manifest ────────                   │
 │                │                      │                   │
 │                │  [optional]          │                   │
 │                │  resources/read ─────►                   │
 │                │◄──── schema blob ─────                   │
 │                │                      │                   │
 │                │  tools/call ─────────►                   │
 │                │  (validate_metric)   │  schema lookup    │
 │                │◄──── PASS ────────────  (no DB hit)      │
 │                │                      │                   │
 │                │  tools/call ─────────►  execute SQL ────►│
 │                │  (run_query / etc.)  │◄──── rows ────────│
 │                │◄──── formatted ───────                   │
 │                │      result          │                   │
 │                │                      │                   │
 │◄── response ───│                      │                   │
```

---

## Error Paths

| What goes wrong | Where it's caught | What happens |
|---|---|---|
| Invalid SQL keyword (INSERT etc.) | `run_query` input check | Returns error string before DB hit |
| Unknown column name | `validate_metric` | Returns FAIL + suggestions |
| Unknown table name | `get_schema`, `validate_metric` | Returns available table list |
| DuckDB query error (syntax etc.) | `duckdb.Error` catch in each tool | Returns `"Query error: ..."` string |
| Wrong metric name in `get_trend` | Whitelist check | Returns valid options |
| No rows returned | Result formatting | Returns `"(no rows returned)"` |

All errors are returned as plain strings, not exceptions. The host sees them as tool results and can reason about them — Claude can self-correct on a bad SQL query or ask the user for clarification.
