"""
Flow-of-Funds MCP Server

Exposes Hive schema introspection, Trino-style SQL execution, and pre-built
analytics tools over synthetic personal banking transaction data.

Real system: Hadoop data lake, Hive metastore, Trino query engine, Spark pipelines.
Demo system:  DuckDB in-memory with identical schema and synthetic data.
"""

from typing import Any

import duckdb
from mcp.server.fastmcp import FastMCP

from src.db import get_conn
from src.schemas import PIPELINE_METADATA, TABLES

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
# Helpers
# ---------------------------------------------------------------------------

def _format_table(columns: list[str], rows: list[tuple], limit: int = 100) -> str:
    """Render DuckDB result rows as a plain-text ASCII table."""
    truncated = len(rows) > limit
    display   = rows[:limit]

    if not display:
        return "(no rows returned)"

    widths = [len(c) for c in columns]
    str_rows = []
    for row in display:
        sr = [str(v) if v is not None else "NULL" for v in row]
        str_rows.append(sr)
        for i, v in enumerate(sr):
            widths[i] = max(widths[i], len(v))

    sep    = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "|" + "|".join(f" {c:<{widths[i]}} " for i, c in enumerate(columns)) + "|"
    lines  = [sep, header, sep]

    for sr in str_rows:
        lines.append("|" + "|".join(f" {v:<{widths[i]}} " for i, v in enumerate(sr)) + "|")
    lines.append(sep)

    if truncated:
        lines.append(f"(first {limit} rows shown — query returned more)")
    else:
        n = len(display)
        lines.append(f"({n} row{'s' if n != 1 else ''})")

    return "\n".join(lines)


def _sparkline(values: list[float]) -> str:
    """Unicode block sparkline for a sequence of values."""
    bars = "▁▂▃▄▅▆▇█"
    if not values:
        return ""
    mn, mx = min(values), max(values)
    if mx == mn:
        return bars[3] * len(values)
    return "".join(bars[int((v - mn) / (mx - mn) * 7)] for v in values)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_tables() -> str:
    """List all available Hive tables in the banking data lake with descriptions and metadata."""
    lines = ["Banking data lake — available tables:\n"]
    for name, meta in TABLES.items():
        partitions = ", ".join(meta["partitioned_by"]) or "none"
        lines += [
            f"  {name}",
            f"    Description : {meta['description']}",
            f"    Columns     : {len(meta['columns'])}",
            f"    Partitioned : {partitions}",
            f"    Format      : {meta['storage_format']}",
            f"    Location    : {meta['location']}",
            "",
        ]
    return "\n".join(lines)


@mcp.tool()
def get_schema(table_name: str) -> str:
    """
    Return the full Hive schema for a table: columns, data types, nullability, descriptions.

    Args:
        table_name: Fully qualified table name, e.g. 'banking.transactions'
    """
    if table_name not in TABLES:
        return f"Table '{table_name}' not found. Available: {', '.join(TABLES)}"

    meta = TABLES[table_name]
    lines = [
        f"Schema: {table_name}",
        f"Description: {meta['description']}",
        f"Storage: {meta['storage_format']}  |  Location: {meta['location']}",
        f"Partitioned by: {', '.join(meta['partitioned_by']) or 'N/A'}",
        "",
        f"{'Column':<22} {'Type':<18} {'Nullable':<10} Description",
        "-" * 82,
    ]
    for col in meta["columns"]:
        lines.append(
            f"{col['name']:<22} {col['type']:<18} {'YES' if col['nullable'] else 'NO':<10} {col['description']}"
        )
    return "\n".join(lines)


@mcp.tool()
def validate_metric(table_name: str, column_name: str) -> str:
    """
    Validate that a column exists in a table before using it in a query or metric.
    Returns type info and usage notes on success, or similar column suggestions on failure.

    Args:
        table_name:  Fully qualified table name, e.g. 'banking.daily_aggregates'
        column_name: Column name to check, e.g. 'net_flow'
    """
    if table_name not in TABLES:
        return f"Table '{table_name}' not found. Run list_tables() to see available tables."

    columns = {c["name"]: c for c in TABLES[table_name]["columns"]}

    if column_name in columns:
        col = columns[column_name]
        return (
            f"PASS  '{column_name}' exists in {table_name}\n"
            f"  Type     : {col['type']}\n"
            f"  Nullable : {'YES' if col['nullable'] else 'NO'}\n"
            f"  Meaning  : {col['description']}"
        )

    suggestions = [
        c for c in columns
        if column_name.lower() in c.lower() or c.lower() in column_name.lower()
    ]
    msg = f"FAIL  '{column_name}' does not exist in {table_name}.\n"
    if suggestions:
        msg += f"  Did you mean: {', '.join(suggestions)}?\n"
    msg += f"  Available columns: {', '.join(columns)}"
    return msg


@mcp.tool()
def run_query(sql: str) -> str:
    """
    Execute a read-only SQL query against the banking data lake.
    Trino-compatible SQL. Only SELECT and WITH (CTE) statements are allowed.
    Results are capped at 100 rows.

    Args:
        sql: SQL to execute. Use fully-qualified names: banking.transactions,
             banking.accounts, banking.ofi_transfers, banking.daily_aggregates
    """
    stripped    = sql.strip()
    first_token = stripped.split()[0].upper() if stripped else ""
    if first_token not in ("SELECT", "WITH"):
        return "Only SELECT and WITH (CTE) queries are permitted."

    try:
        conn    = get_conn()
        result  = conn.execute(stripped)
        columns = [d[0] for d in result.description]
        rows    = result.fetchmany(101)
        return _format_table(columns, rows, limit=100)
    except duckdb.Error as e:
        return f"Query error: {e}"


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
    conn    = get_conn()
    filters = ["agg_date BETWEEN ? AND ?"]
    params: list[Any] = [start_date, end_date]

    if account_id:
        filters.append("account_id = ?")
        params.append(account_id)
    if segment:
        filters.append("segment = ?")
        params.append(segment.upper())

    where = " AND ".join(filters)

    try:
        row = conn.execute(f"""
            SELECT
                COUNT(DISTINCT account_id)          AS accounts,
                COUNT(DISTINCT agg_date)            AS active_days,
                COALESCE(SUM(total_credits), 0)     AS total_inflow,
                COALESCE(SUM(total_debits),  0)     AS total_outflow,
                COALESCE(SUM(net_flow),      0)     AS net_flow,
                COALESCE(SUM(ofi_outflow),   0)     AS ofi_outflow,
                COALESCE(SUM(ofi_inflow),    0)     AS ofi_inflow,
                COALESCE(SUM(txn_count),     0)     AS total_txns,
                COALESCE(AVG(net_flow),      0)     AS avg_daily_net_flow
            FROM banking.daily_aggregates
            WHERE {where}
        """, params).fetchone()

        if not row or row[0] == 0:
            return f"No data found for {start_date} to {end_date} with the given filters."

        accounts_n, days, inflow, outflow, net, ofi_out, ofi_in, txns, avg_net = row

        seg_rows = conn.execute(f"""
            SELECT
                segment,
                COALESCE(SUM(total_credits), 0)  AS inflow,
                COALESCE(SUM(total_debits),  0)  AS outflow,
                COALESCE(SUM(net_flow),      0)  AS net_flow,
                COALESCE(SUM(ofi_outflow),   0)  AS ofi_outflow,
                COUNT(DISTINCT account_id)        AS accounts
            FROM banking.daily_aggregates
            WHERE {where}
            GROUP BY segment
            ORDER BY ABS(SUM(net_flow)) DESC
        """, params).fetchall()

        arrow = "▲" if net >= 0 else "▼"
        lines = [
            f"Flow-of-Funds Summary  {start_date} to {end_date}",
            "=" * 52,
            f"  Accounts tracked   : {accounts_n:,}",
            f"  Active days        : {days:,}",
            f"  Total inflow       : ${inflow:,.2f}",
            f"  Total outflow      : ${outflow:,.2f}",
            f"  Net flow           : ${net:,.2f}  {arrow}",
            f"  OFI outflow        : ${ofi_out:,.2f}",
            f"  OFI inflow         : ${ofi_in:,.2f}",
            f"  Total transactions : {txns:,}",
            f"  Avg daily net flow : ${avg_net:,.2f}",
            "",
            "Segment breakdown:",
            f"  {'Segment':<12} {'Inflow':>14} {'Outflow':>14} {'Net Flow':>14} {'OFI Out':>12} {'Accounts':>10}",
            "  " + "-" * 78,
        ]
        for seg, inf, ouf, nf, ofi, accts in seg_rows:
            lines.append(
                f"  {seg:<12} ${inf:>13,.2f} ${ouf:>13,.2f} ${nf:>13,.2f} ${ofi:>11,.2f} {accts:>10,}"
            )

        return "\n".join(lines)

    except duckdb.Error as e:
        return f"Error: {e}"


@mcp.tool()
def get_ofi_breakdown(start_date: str, end_date: str, top_n: int = 10) -> str:
    """
    Analyze outflows to Other Financial Institutions (OFIs): which external banks
    are receiving transfers, how much, how often, and return rates.

    Args:
        start_date: Start date inclusive, YYYY-MM-DD
        end_date:   End date inclusive, YYYY-MM-DD
        top_n:      Number of top institutions to return (default: 10)
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                dest_institution,
                transfer_type,
                COUNT(*)                                        AS transfer_count,
                SUM(amount)                                     AS total_amount,
                AVG(amount)                                     AS avg_amount,
                COUNT(CASE WHEN status = 'SETTLED'  THEN 1 END) AS settled,
                COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned,
                COUNT(CASE WHEN status = 'PENDING'  THEN 1 END) AS pending
            FROM banking.ofi_transfers
            WHERE transfer_date BETWEEN ? AND ?
              AND dest_institution != 'INTERNAL_BANK'
            GROUP BY dest_institution, transfer_type
            ORDER BY total_amount DESC
            LIMIT ?
        """, [start_date, end_date, top_n]).fetchall()

        if not rows:
            return f"No OFI outflow transfers found between {start_date} and {end_date}."

        lines = [
            f"OFI Outflow Breakdown  {start_date} to {end_date}",
            "=" * 72,
            f"{'Institution':<22} {'Type':<6} {'Count':>6} {'Total':>14} {'Avg':>10} {'Settled':>8} {'Returned':>9} {'Pending':>8}",
            "-" * 90,
        ]
        for dest, xtype, cnt, total, avg, settled, returned, pending in rows:
            lines.append(
                f"{dest:<22} {xtype:<6} {cnt:>6,} ${total:>13,.2f} ${avg:>9,.2f}"
                f" {settled:>8,} {returned:>9,} {pending:>8,}"
            )

        total_vol = sum(r[3] for r in rows)
        lines += ["", f"Total OFI outflow in view: ${total_vol:,.2f}"]
        return "\n".join(lines)

    except duckdb.Error as e:
        return f"Error: {e}"


@mcp.tool()
def get_trend(
    metric:      str,
    start_date:  str,
    end_date:    str,
    granularity: str = "daily",
    segment:     str | None = None,
) -> str:
    """
    Return time-series trend data for a metric — useful for pattern detection,
    anomaly spotting, and building ML training feature sets.

    Args:
        metric:      net_flow | total_credits | total_debits | ofi_outflow | ofi_inflow | txn_count
        start_date:  Start date inclusive, YYYY-MM-DD
        end_date:    End date inclusive, YYYY-MM-DD
        granularity: daily | weekly | monthly  (default: daily)
        segment:     Optional — RETAIL, BUSINESS, or PREMIUM
    """
    valid_metrics = {"net_flow", "total_credits", "total_debits", "ofi_outflow", "ofi_inflow", "txn_count"}
    if metric not in valid_metrics:
        return f"Invalid metric '{metric}'. Valid options: {', '.join(sorted(valid_metrics))}"

    gran_map = {
        "daily":   "CAST(agg_date AS DATE)",
        "weekly":  "DATE_TRUNC('week',  agg_date)",
        "monthly": "DATE_TRUNC('month', agg_date)",
    }
    if granularity not in gran_map:
        return f"Invalid granularity '{granularity}'. Valid options: daily, weekly, monthly"

    period_expr = gran_map[granularity]
    filters     = ["agg_date BETWEEN ? AND ?"]
    params: list[Any] = [start_date, end_date]

    if segment:
        filters.append("segment = ?")
        params.append(segment.upper())

    where = " AND ".join(filters)
    conn  = get_conn()

    try:
        rows = conn.execute(f"""
            SELECT
                {period_expr}    AS period,
                SUM({metric})    AS value
            FROM banking.daily_aggregates
            WHERE {where}
            GROUP BY period
            ORDER BY period
        """, params).fetchall()

        if not rows:
            return f"No data for metric '{metric}' between {start_date} and {end_date}."

        values = [float(r[1]) for r in rows]
        spark  = _sparkline(values)
        mean   = sum(values) / len(values)
        seg_label = f"  [segment={segment.upper()}]" if segment else ""

        lines = [
            f"Trend: {metric} ({granularity}){seg_label}  {start_date} to {end_date}",
            "=" * 60,
            f"  Periods   : {len(values)}",
            f"  Min       : {min(values):>16,.2f}",
            f"  Max       : {max(values):>16,.2f}",
            f"  Mean      : {mean:>16,.2f}",
            f"  Sparkline : {spark}",
            "",
            f"{'Period':<14} {'Value':>16}",
            "-" * 32,
        ]
        for period, value in rows:
            lines.append(f"{str(period):<14} {float(value):>16,.2f}")

        return "\n".join(lines)

    except duckdb.Error as e:
        return f"Error: {e}"


@mcp.tool()
def explain_pipeline(table_name: str) -> str:
    """
    Return Spark pipeline metadata for a table: job name, schedule, source systems,
    SLA, owner team, and upstream/downstream lineage.

    Args:
        table_name: Fully qualified table name, e.g. 'banking.daily_aggregates'
    """
    if table_name not in PIPELINE_METADATA:
        return f"No pipeline metadata for '{table_name}'. Available: {', '.join(PIPELINE_METADATA)}"

    p  = PIPELINE_METADATA[table_name]
    up = "\n".join(f"    <- {s}" for s in p["upstream"])
    dn = "\n".join(f"    -> {s}" for s in p["downstream"])

    return (
        f"Pipeline: {p['job_name']}\n"
        f"Target table  : {table_name}\n"
        f"Schedule      : {p['schedule']}  ({p['schedule_desc']})\n"
        f"Source        : {p['source']}\n"
        f"Owner team    : {p['owner_team']}\n"
        f"SLA           : {p['sla_minutes']} min after schedule trigger\n"
        f"Spark version : {p['spark_version']}\n"
        f"Cluster       : {p['cluster']}\n"
        f"Last run      : {p['last_run']}\n"
        f"\nLineage:\n"
        f"  Upstream inputs:\n{up}\n"
        f"  Downstream consumers:\n{dn}"
    )


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

@mcp.resource("schema://tables")
def schema_catalog() -> str:
    """Full schema catalog for all tables in the banking data lake."""
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
    return get_schema(table_name)


@mcp.resource("pipeline://lineage/{table_name}")
def pipeline_lineage(table_name: str) -> str:
    """Spark pipeline metadata and lineage for a specific table."""
    return explain_pipeline(table_name)
