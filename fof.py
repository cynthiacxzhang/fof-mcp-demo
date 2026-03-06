"""
Flow-of-Funds MCP Server — tool and resource registration.
All implementation logic lives in src/core/tools.py.
"""

from mcp.server.fastmcp import FastMCP

import src.core.tools as impl

mcp = FastMCP(
    "flow-of-funds",
    instructions=(
        "Flow-of-Funds MCP server for personal banking transaction data. "
        "Available tables: banking.transactions, banking.accounts, "
        "banking.ofi_transfers, banking.daily_aggregates. "
        "Start with list_tables() to explore, get_schema() to inspect columns, "
        "validate_metric() to verify a column before use, and run_query() for "
        "custom SQL. Pre-built tools: get_flow_summary, get_ofi_breakdown, "
        "get_trend, explain_pipeline."
    ),
)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_tables() -> str:
    """List all available Hive tables in the banking data lake with descriptions and metadata."""
    return impl.list_tables()


@mcp.tool()
def get_schema(table_name: str) -> str:
    """
    Return the full Hive schema for a table: columns, data types, nullability, descriptions.

    Args:
        table_name: Fully qualified table name, e.g. 'banking.transactions'
    """
    return impl.get_schema(table_name)


@mcp.tool()
def validate_metric(table_name: str, column_name: str) -> str:
    """
    Validate that a column exists in a table before using it in a query or metric.
    Returns type info on success, or similar column suggestions on failure.

    Args:
        table_name:  Fully qualified table name, e.g. 'banking.daily_aggregates'
        column_name: Column name to check, e.g. 'net_flow'
    """
    return impl.validate_metric(table_name, column_name)


@mcp.tool()
def run_query(sql: str) -> str:
    """
    Execute a read-only SQL query against the banking data lake.
    Only SELECT and WITH (CTE) statements are allowed. Results capped at 100 rows.

    Args:
        sql: SQL to execute. Use fully-qualified names: banking.transactions,
             banking.accounts, banking.ofi_transfers, banking.daily_aggregates
    """
    return impl.run_query(sql)


@mcp.tool()
def get_flow_summary(
    start_date: str,
    end_date:   str,
    account_id: str | None = None,
    segment:    str | None = None,
) -> str:
    """
    Summarize net flow, inflows, outflows, and OFI activity for a time period.
    Optionally filter to a single account or customer segment.

    Args:
        start_date: Start date inclusive, YYYY-MM-DD
        end_date:   End date inclusive, YYYY-MM-DD
        account_id: Optional — filter to one account, e.g. 'ACC000001'
        segment:    Optional — RETAIL, BUSINESS, or PREMIUM
    """
    return impl.get_flow_summary(start_date, end_date, account_id, segment)


@mcp.tool()
def get_ofi_breakdown(start_date: str, end_date: str, top_n: int = 10) -> str:
    """
    Analyze outflows to Other Financial Institutions: which external banks are
    receiving transfers, volumes, counts, and return rates.

    Args:
        start_date: Start date inclusive, YYYY-MM-DD
        end_date:   End date inclusive, YYYY-MM-DD
        top_n:      Number of top institutions to return (default: 10)
    """
    return impl.get_ofi_breakdown(start_date, end_date, top_n)


@mcp.tool()
def get_trend(
    metric:      str,
    start_date:  str,
    end_date:    str,
    granularity: str = "daily",
    segment:     str | None = None,
) -> str:
    """
    Return time-series trend data for a metric over a date range.

    Args:
        metric:      net_flow | total_credits | total_debits | ofi_outflow | ofi_inflow | txn_count
        start_date:  Start date inclusive, YYYY-MM-DD
        end_date:    End date inclusive, YYYY-MM-DD
        granularity: daily | weekly | monthly  (default: daily)
        segment:     Optional — RETAIL, BUSINESS, or PREMIUM
    """
    return impl.get_trend(metric, start_date, end_date, granularity, segment)


@mcp.tool()
def explain_pipeline(table_name: str) -> str:
    """
    Return Spark pipeline metadata for a table: job name, schedule, source systems,
    SLA, owner team, and upstream/downstream lineage.

    Args:
        table_name: Fully qualified table name, e.g. 'banking.daily_aggregates'
    """
    return impl.explain_pipeline(table_name)


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

@mcp.resource("schema://tables")
def schema_catalog() -> str:
    """Full schema catalog for all tables in the banking data lake."""
    from src.schemas import TABLES
    sections = []
    for name, meta in TABLES.items():
        lines = [
            f"## {name}",
            meta["description"],
            f"- Storage: {meta['storage_format']}  |  Partitioned by: {', '.join(meta['partitioned_by']) or 'N/A'}",
            f"- Location: {meta['location']}",
            "",
            "| Column | Type | Nullable | Description |",
            "|--------|------|----------|-------------|",
        ]
        for col in meta["columns"]:
            lines.append(
                f"| {col['name']} | {col['type']} | {'YES' if col['nullable'] else 'NO'} | {col['description']} |"
            )
        sections.append("\n".join(lines))
    return "\n\n---\n\n".join(sections)


@mcp.resource("schema://tables/{table_name}")
def schema_for_table(table_name: str) -> str:
    """Schema for a specific table. Pass e.g. 'banking.transactions'."""
    return impl.get_schema(table_name)


@mcp.resource("pipeline://lineage/{table_name}")
def pipeline_lineage(table_name: str) -> str:
    """Spark pipeline metadata and lineage for a specific table."""
    return impl.explain_pipeline(table_name)
