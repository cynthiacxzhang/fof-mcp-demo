"""
DuckDB in-memory database — stand-in for Trino querying Hive tables in HDFS.
The schema mirrors the real banking data lake structure. Seeded once at startup.
"""

import duckdb

from src.synthetic import generate_accounts, generate_daily_aggregates, generate_transactions

_conn: duckdb.DuckDBPyConnection | None = None


def get_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = _build()
    return _conn


def _build() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA banking")

    conn.execute("""
        CREATE TABLE banking.accounts (
            account_id      VARCHAR        NOT NULL,
            account_type    VARCHAR        NOT NULL,
            segment         VARCHAR        NOT NULL,
            open_date       DATE           NOT NULL,
            status          VARCHAR        NOT NULL,
            current_balance DECIMAL(18,2)  NOT NULL,
            credit_limit    DECIMAL(18,2)
        )
    """)

    conn.execute("""
        CREATE TABLE banking.transactions (
            txn_id      VARCHAR        NOT NULL,
            account_id  VARCHAR        NOT NULL,
            txn_date    DATE           NOT NULL,
            posted_date DATE,
            amount      DECIMAL(18,2)  NOT NULL,
            direction   VARCHAR        NOT NULL,
            category    VARCHAR,
            merchant    VARCHAR,
            channel     VARCHAR        NOT NULL,
            ofi_id      VARCHAR,
            description VARCHAR,
            status      VARCHAR        NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE banking.ofi_transfers (
            transfer_id        VARCHAR        NOT NULL,
            txn_id             VARCHAR        NOT NULL,
            source_institution VARCHAR        NOT NULL,
            dest_institution   VARCHAR        NOT NULL,
            routing_number     VARCHAR        NOT NULL,
            amount             DECIMAL(18,2)  NOT NULL,
            transfer_date      DATE           NOT NULL,
            transfer_type      VARCHAR        NOT NULL,
            status             VARCHAR        NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE banking.daily_aggregates (
            account_id    VARCHAR        NOT NULL,
            agg_date      DATE           NOT NULL,
            segment       VARCHAR        NOT NULL,
            total_credits DECIMAL(18,2)  NOT NULL,
            total_debits  DECIMAL(18,2)  NOT NULL,
            net_flow      DECIMAL(18,2)  NOT NULL,
            txn_count     INTEGER        NOT NULL,
            ofi_outflow   DECIMAL(18,2)  NOT NULL,
            ofi_inflow    DECIMAL(18,2)  NOT NULL
        )
    """)

    accounts                  = generate_accounts(30)
    transactions, ofi_transfers = generate_transactions(accounts)
    daily_aggs                = generate_daily_aggregates(accounts, transactions)

    conn.executemany(
        "INSERT INTO banking.accounts VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            [a["account_id"], a["account_type"], a["segment"], a["open_date"],
             a["status"], a["current_balance"], a["credit_limit"]]
            for a in accounts
        ],
    )

    conn.executemany(
        "INSERT INTO banking.transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            [t["txn_id"], t["account_id"], t["txn_date"], t["posted_date"],
             t["amount"], t["direction"], t["category"], t["merchant"],
             t["channel"], t["ofi_id"], t["description"], t["status"]]
            for t in transactions
        ],
    )

    conn.executemany(
        "INSERT INTO banking.ofi_transfers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            [o["transfer_id"], o["txn_id"], o["source_institution"], o["dest_institution"],
             o["routing_number"], o["amount"], o["transfer_date"], o["transfer_type"], o["status"]]
            for o in ofi_transfers
        ],
    )

    conn.executemany(
        "INSERT INTO banking.daily_aggregates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            [d["account_id"], d["agg_date"], d["segment"], d["total_credits"],
             d["total_debits"], d["net_flow"], d["txn_count"], d["ofi_outflow"], d["ofi_inflow"]]
            for d in daily_aggs
        ],
    )

    return conn
