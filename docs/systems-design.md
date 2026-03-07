# Systems Design Notes

## What This Is

An MCP (Model Context Protocol) server that sits between an AI host (Claude Desktop) and a banking data lake. The server exposes schema metadata, query execution, and pre-built analytics as callable tools. The host decides which tools to call based on user intent — the server just handles execution and returns results.

The real system runs against Hive tables in HDFS queried via Trino, fed by Spark pipelines. This demo replaces that stack with DuckDB in-memory and synthetic data. The interface is identical.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   MCP Host                          │
│              (Claude Desktop / LLM)                 │
│                                                     │
│  - Receives user query                              │
│  - Selects tools based on intent + tool manifest    │
│  - Synthesizes response from tool results           │
└────────────────────┬────────────────────────────────┘
                     │  stdio (JSON-RPC 2.0)
                     │  tools/call, resources/read
                     ▼
┌─────────────────────────────────────────────────────┐
│                  MCP Server                         │
│              (fof.py / FastMCP)                     │
│                                                     │
│  Tools:      list_tables, get_schema,               │
│              validate_metric, run_query,            │
│              get_flow_summary, get_ofi_breakdown,   │
│              get_trend, explain_pipeline            │
│                                                     │
│  Resources:  schema://tables                        │
│              schema://tables/{table_name}           │
│              pipeline://lineage/{table_name}        │
└────────────────────┬────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
┌──────────────────┐   ┌─────────────────────────────┐
│   src/schemas.py │   │         src/db.py            │
│                  │   │                              │
│  Hive table      │   │  DuckDB (demo)               │
│  schema defs     │   │  ── or ──                    │
│  Pipeline        │   │  Trino → Hive → HDFS (real)  │
│  metadata        │   │                              │
└──────────────────┘   └─────────────────────────────┘
```

### Real vs. Demo Stack

| Layer | Real System | This Demo |
|---|---|---|
| Storage | HDFS (ORC/Parquet) | in-memory |
| Query engine | Trino | DuckDB |
| Table metadata | Hive Metastore | `src/schemas.py` |
| Data pipelines | Spark (Hadoop cluster) | `src/synthetic.py` |
| Transport | HTTP/SSE (prod MCP) | stdio |
| Auth | Kerberos / IAM | none |

The MCP interface is the same in both cases. Swapping DuckDB for Trino is a config change in `src/db.py`.

---

## Key Design Decisions

### Schema-first

The server exposes schema metadata as a first-class tool (`get_schema`, `validate_metric`), not just a prerequisite. This matters because:

- Business users don't know column names. They'll say "outflow" and mean `ofi_outflow` or `total_debits` depending on context.
- `validate_metric` can catch a wrong column name before it causes a query error, and suggest the right one.
- The LLM can use schema info to construct correct SQL without hallucinating columns.

This is the core value add over just exposing a raw query interface.

### Tools vs. Resources

**Tools** are callable functions — they take parameters, execute something, and return a result. Every analytics operation is a tool.

**Resources** are readable data blobs — the schema catalog, table schemas, pipeline lineage. They're meant to be attached as context at the start of a conversation, not called mid-reasoning. Think of resources as reference docs; tools as actions.

In practice: attach `schema://tables` at the start of a session so the LLM has the full data model in context, then call tools to do analysis.

### Pre-built analytics tools vs. raw SQL

Both exist for a reason. `get_flow_summary` and `get_ofi_breakdown` produce consistent, validated output that doesn't depend on the LLM writing correct SQL. `run_query` is the escape hatch for ad-hoc analysis. The pre-built tools should cover 80% of use cases and make the output predictable enough to use in reports or downstream systems.

### Read-only enforcement

`run_query` only allows `SELECT` and `WITH`. No DDL, no writes. The DuckDB connection is in-memory so nothing persists anyway, but the guard is there for the real Trino system where it matters.

---

## Data Model

Four tables, modeled after the real Hive schema:

```
banking.accounts ──────────────┐
        │                      │
        │ account_id            │ account_id, segment
        ▼                      ▼
banking.transactions ──► banking.daily_aggregates
        │
        │ txn_id (for ACH/WIRE transfers)
        ▼
banking.ofi_transfers
```

`daily_aggregates` is the primary analytics table. It's pre-aggregated per account per day by a Spark job, so most trend/summary queries hit it rather than the raw transactions table. In the real system this is a major performance optimization — scanning raw transaction partitions for a 90-day summary would be expensive.

---

## Where This Goes

### Immediate (current state)
Analytical queries for business teams. Replace ad-hoc Slack messages to data engineers with direct tool calls. Schema validation prevents the "this metric doesn't exist" feedback cycle.

### Near-term
- Add write-back: flag anomalies, annotate transactions, trigger downstream workflows
- Multi-tenant: scope queries by team or business unit
- Richer OFI intelligence: institution risk scoring, velocity alerts

### ML path
`banking.daily_aggregates` is already structured as a feature table. The columns (`net_flow`, `ofi_outflow`, `ofi_inflow`, `txn_count` by account by day) map directly to features for:
- Liquidity forecasting: predict 30-day net flow per account
- Churn risk: dropping inflow + rising OFI outflow signals money leaving
- Anomaly detection: spikes in OFI transfers relative to account baseline

The `get_trend` tool output is structured to feed a training pipeline directly. The MCP layer would let a model training orchestrator query its own feature data through the same interface business users use.
