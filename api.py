"""
FastAPI backend for the Flow-of-Funds demo UI.
Runs the Anthropic agentic loop server-side and streams SSE events to the frontend.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import AsyncGenerator

import anthropic
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

import src.core.tools as tools_impl

app = FastAPI(title="Flow of Funds Demo")

client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
_raw_db_url = os.getenv("DATABASE_URL", "")
DATABASE_URL = _raw_db_url.replace("postgres://", "postgresql://", 1) if _raw_db_url else None

MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = """You are a flow-of-funds analyst assistant connected to a personal banking data lake.

The data lake has four Hive tables with synthetic Q4 2025 data:
- banking.transactions — 636 raw transaction events (Oct–Dec 2025)
- banking.accounts — 30 accounts across RETAIL, BUSINESS, PREMIUM segments
- banking.ofi_transfers — 39 external institution transfers (ACH/WIRE)
- banking.daily_aggregates — pre-aggregated daily flow metrics per account

Help users explore the data and understand flow-of-funds patterns. Use get_schema or validate_metric before writing custom SQL. Prefer the pre-built analytics tools for common questions. Be concise — let the tool results speak for themselves."""

TOOL_DEFINITIONS = [
    {
        "name": "list_tables",
        "description": "List all available Hive tables in the banking data lake with descriptions and metadata.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_schema",
        "description": "Return the full Hive schema for a table — columns, types, nullability, descriptions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "e.g. 'banking.transactions'"},
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "validate_metric",
        "description": "Check that a column exists in a table before using it. Returns type info or suggestions if not found.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name":  {"type": "string"},
                "column_name": {"type": "string"},
            },
            "required": ["table_name", "column_name"],
        },
    },
    {
        "name": "run_query",
        "description": "Execute a read-only SQL query. Only SELECT and WITH allowed. Results capped at 100 rows.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SQL using banking.transactions, banking.accounts, banking.ofi_transfers, or banking.daily_aggregates"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_flow_summary",
        "description": "Summarize net flow, inflows, outflows, and OFI activity for a time period. Filter by account or segment.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
                "account_id": {"type": "string"},
                "segment":    {"type": "string", "description": "RETAIL, BUSINESS, or PREMIUM"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_ofi_breakdown",
        "description": "Analyze outflows to Other Financial Institutions — which external banks, how much, return rates.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
                "top_n":      {"type": "integer", "description": "Default 10"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_trend",
        "description": "Return time-series trend data for a metric over a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric":      {"type": "string", "description": "net_flow | total_credits | total_debits | ofi_outflow | ofi_inflow | txn_count"},
                "start_date":  {"type": "string", "description": "YYYY-MM-DD"},
                "end_date":    {"type": "string", "description": "YYYY-MM-DD"},
                "granularity": {"type": "string", "description": "daily | weekly | monthly"},
                "segment":     {"type": "string"},
            },
            "required": ["metric", "start_date", "end_date"],
        },
    },
    {
        "name": "explain_pipeline",
        "description": "Return Spark pipeline metadata for a table — schedule, source, SLA, lineage.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string"},
            },
            "required": ["table_name"],
        },
    },
]

TOOL_MAP = {
    "list_tables":       lambda a: tools_impl.list_tables(),
    "get_schema":        lambda a: tools_impl.get_schema(a["table_name"]),
    "validate_metric":   lambda a: tools_impl.validate_metric(a["table_name"], a["column_name"]),
    "run_query":         lambda a: tools_impl.run_query(a["sql"]),
    "get_flow_summary":  lambda a: tools_impl.get_flow_summary(
                             a["start_date"], a["end_date"], a.get("account_id"), a.get("segment")),
    "get_ofi_breakdown": lambda a: tools_impl.get_ofi_breakdown(
                             a["start_date"], a["end_date"], a.get("top_n", 10)),
    "get_trend":         lambda a: tools_impl.get_trend(
                             a["metric"], a["start_date"], a["end_date"],
                             a.get("granularity", "daily"), a.get("segment")),
    "explain_pipeline":  lambda a: tools_impl.explain_pipeline(a["table_name"]),
}

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _db() -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row)


@app.on_event("startup")
async def startup():
    if DATABASE_URL:
        async with await _db() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id  TEXT PRIMARY KEY,
                    title       TEXT NOT NULL DEFAULT 'Untitled',
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    session_id  TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                    messages    JSONB NOT NULL DEFAULT '[]'
                )
            """)
            await conn.commit()


async def _ensure_session(session_id: str, first_message: str) -> None:
    title = first_message[:60] + ("…" if len(first_message) > 60 else "")
    async with await _db() as conn:
        await conn.execute(
            "INSERT INTO sessions (session_id, title) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (session_id, title),
        )
        await conn.execute(
            "INSERT INTO messages (session_id, messages) VALUES (%s, '[]'::jsonb) ON CONFLICT DO NOTHING",
            (session_id,),
        )
        await conn.commit()


async def _load_messages(session_id: str) -> list:
    async with await _db() as conn:
        row = await (await conn.execute(
            "SELECT messages FROM messages WHERE session_id = %s", (session_id,)
        )).fetchone()
    return row["messages"] if row else []


async def _save_messages(session_id: str, messages: list) -> None:
    async with await _db() as conn:
        await conn.execute(
            "UPDATE messages SET messages = %s::jsonb WHERE session_id = %s",
            (json.dumps(messages), session_id),
        )
        await conn.execute(
            "UPDATE sessions SET updated_at = NOW() WHERE session_id = %s",
            (session_id,),
        )
        await conn.commit()


async def _list_sessions() -> list[dict]:
    async with await _db() as conn:
        rows = await (await conn.execute(
            "SELECT session_id, title, updated_at FROM sessions ORDER BY updated_at DESC LIMIT 50"
        )).fetchall()
    return [
        {**r, "updated_at": r["updated_at"].isoformat()}
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sse(event_type: str, **kwargs) -> str:
    return f"data: {json.dumps({'type': event_type, **kwargs})}\n\n"


async def run_tool(name: str, tool_input: dict) -> str:
    fn = TOOL_MAP.get(name)
    if not fn:
        return f"Unknown tool: {name}"
    try:
        return await asyncio.to_thread(fn, tool_input)
    except Exception as e:
        return f"Tool error: {e}"


# ---------------------------------------------------------------------------
# Agentic stream
# ---------------------------------------------------------------------------

async def agent_stream(message: str, session_id: str) -> AsyncGenerator[str, None]:
    if DATABASE_URL:
        await _ensure_session(session_id, message)
        messages = await _load_messages(session_id)
    else:
        messages = []

    messages.append({"role": "user", "content": message})

    try:
        while True:
            tool_uses_this_turn: list[dict] = []
            current_tool: dict | None = None
            current_input_str = ""
            in_tool = False

            async with client.messages.stream(
                model=MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            ) as stream:
                async for event in stream:
                    etype = event.type

                    if etype == "content_block_start":
                        cb = event.content_block
                        if cb.type == "tool_use":
                            in_tool = True
                            current_tool = {"id": cb.id, "name": cb.name}
                            current_input_str = ""
                            yield sse("tool_start", id=cb.id, name=cb.name)

                    elif etype == "content_block_delta":
                        d = event.delta
                        if d.type == "text_delta":
                            yield sse("text", content=d.text)
                        elif d.type == "input_json_delta":
                            current_input_str += d.partial_json

                    elif etype == "content_block_stop":
                        if in_tool and current_tool:
                            try:
                                tool_input = json.loads(current_input_str) if current_input_str else {}
                            except json.JSONDecodeError:
                                tool_input = {}

                            yield sse("tool_input", id=current_tool["id"], input=tool_input)

                            result = await run_tool(current_tool["name"], tool_input)

                            yield sse("tool_result",
                                      id=current_tool["id"],
                                      name=current_tool["name"],
                                      result=result)

                            tool_uses_this_turn.append({
                                "id":     current_tool["id"],
                                "name":   current_tool["name"],
                                "input":  tool_input,
                                "result": result,
                            })
                            in_tool = False
                            current_tool = None

                final_msg = await stream.get_final_message()

            messages.append({"role": "assistant", "content": [b.model_dump() for b in final_msg.content]})

            if final_msg.stop_reason == "end_turn":
                if DATABASE_URL:
                    await _save_messages(session_id, messages)
                break

            messages.append({
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": tu["id"], "content": tu["result"]}
                    for tu in tool_uses_this_turn
                ],
            })

    except anthropic.APIError as e:
        if DATABASE_URL:
            await _save_messages(session_id, messages)
        yield sse("error", message=str(e))

    yield sse("done")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    message:    str
    session_id: str


@app.post("/chat")
async def chat(request: ChatRequest, response: Response):
    response.set_cookie(
        key="session_id",
        value=request.session_id,
        max_age=60 * 60 * 24 * 30,
        httponly=False,
        samesite="lax",
    )
    return StreamingResponse(
        agent_stream(request.message, request.session_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/sessions")
async def list_sessions():
    if not DATABASE_URL:
        return []
    return await _list_sessions()


@app.get("/sessions/{session_id}/messages")
async def get_session_messages(session_id: str):
    if not DATABASE_URL:
        return {"session_id": session_id, "messages": []}
    msgs = await _load_messages(session_id)
    return {"session_id": session_id, "messages": msgs}


@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    if not DATABASE_URL:
        return {"ok": True}
    async with await _db() as conn:
        await conn.execute("DELETE FROM sessions WHERE session_id = %s", (session_id,))
        await conn.commit()
    return {"ok": True}


@app.get("/")
async def index():
    return HTMLResponse(Path("static/index.html").read_text())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
