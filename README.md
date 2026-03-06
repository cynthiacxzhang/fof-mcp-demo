# Flow-of-Funds MCP Server

A demo MCP server that bridges an AI host with banking transaction data — schema introspection, SQL queries, and pre-built analytics tools over a Hadoop-style data lake.

## The Problem

Flow-of-funds analysis on personal banking data is tedious. Business teams and internal-facing groups need to answer questions like "where is money leaving to?", "what's net outflow for this segment over Q4?", or "is this metric even a real column?" — and right now that means pinging a data engineer, waiting, and hoping the query is right.

This project is a proof of concept for cutting out that back-and-forth. Instead of manually pulling data, an AI agent can hit the MCP server directly: browse the schema, validate metrics, run queries, and get pre-aggregated analysis on demand.

## What It Does

The server exposes four Hive tables (transactions, accounts, OFI transfers, daily aggregates) through a set of tools:

- **Schema tools** — list tables, inspect columns, validate that a metric exists before using it
- **Query execution** — run arbitrary read-only SQL against the data
- **Pre-built analytics** — net flow summaries, OFI breakdowns (which external banks are receiving transfers and how much), time-series trends with configurable granularity
- **Pipeline metadata** — see which Spark jobs produce a table, their schedules, SLAs, and upstream/downstream lineage

In the real system this sits on top of Trino querying ORC/Parquet files in HDFS. This demo runs the same interface over DuckDB with synthetic data.

## Who It's For

Data engineers who want to stop being the middleman. Business and ops teams who need answers from transaction data without writing SQL. Anyone building internal tooling around a data lake.

Longer term, the structured output from these tools — especially the trend data — is a direct input to predictive modeling. The flow patterns here are the kind of features you'd use to train a model for things like churn prediction, liquidity forecasting, or anomaly detection. This is a step toward making that data accessible enough to actually get used.

## Stack

- [MCP](https://modelcontextprotocol.io) via `fastmcp`
- DuckDB (stand-in for Trino)
- Python 3.11+, uv
