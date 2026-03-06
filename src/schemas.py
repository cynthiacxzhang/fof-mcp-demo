"""
Hive table schema definitions and Spark pipeline metadata.
Mirrors what DESCRIBE TABLE / SHOW TBLPROPERTIES would return in Trino.
"""

TABLES: dict[str, dict] = {
    "banking.transactions": {
        "description": (
            "Core personal banking transaction ledger. "
            "One row per transaction event, partitioned by txn_date."
        ),
        "partitioned_by": ["txn_date"],
        "storage_format": "ORC",
        "location": "hdfs://datalake/banking/transactions",
        "columns": [
            {"name": "txn_id",      "type": "VARCHAR",        "nullable": False, "description": "Unique transaction identifier"},
            {"name": "account_id",  "type": "VARCHAR",        "nullable": False, "description": "Account identifier (FK to banking.accounts)"},
            {"name": "txn_date",    "type": "DATE",           "nullable": False, "description": "Transaction date — partition key"},
            {"name": "posted_date", "type": "DATE",           "nullable": True,  "description": "Date transaction posted to ledger (may lag txn_date)"},
            {"name": "amount",      "type": "DECIMAL(18,2)",  "nullable": False, "description": "Transaction amount in USD"},
            {"name": "direction",   "type": "VARCHAR",        "nullable": False, "description": "CREDIT or DEBIT"},
            {"name": "category",    "type": "VARCHAR",        "nullable": True,  "description": "PAYROLL, TRANSFER, PURCHASE, BILL_PAYMENT, or ATM"},
            {"name": "merchant",    "type": "VARCHAR",        "nullable": True,  "description": "Merchant name or counterparty label"},
            {"name": "channel",     "type": "VARCHAR",        "nullable": False, "description": "ACH, WIRE, CARD, INTERNAL, or CHECK"},
            {"name": "ofi_id",      "type": "VARCHAR",        "nullable": True,  "description": "OFI institution ID — NULL for internal transactions"},
            {"name": "description", "type": "VARCHAR",        "nullable": True,  "description": "Transaction memo or free-text description"},
            {"name": "status",      "type": "VARCHAR",        "nullable": False, "description": "POSTED, PENDING, or REVERSED"},
        ],
    },
    "banking.accounts": {
        "description": (
            "Account master table. One row per account, "
            "full snapshot refreshed daily from core banking system."
        ),
        "partitioned_by": [],
        "storage_format": "ORC",
        "location": "hdfs://datalake/banking/accounts",
        "columns": [
            {"name": "account_id",      "type": "VARCHAR",        "nullable": False, "description": "Unique account identifier"},
            {"name": "account_type",    "type": "VARCHAR",        "nullable": False, "description": "CHECKING, SAVINGS, or MONEY_MARKET"},
            {"name": "segment",         "type": "VARCHAR",        "nullable": False, "description": "Customer segment: RETAIL, BUSINESS, or PREMIUM"},
            {"name": "open_date",       "type": "DATE",           "nullable": False, "description": "Date the account was opened"},
            {"name": "status",          "type": "VARCHAR",        "nullable": False, "description": "ACTIVE, CLOSED, or SUSPENDED"},
            {"name": "current_balance", "type": "DECIMAL(18,2)",  "nullable": False, "description": "Current ledger balance in USD"},
            {"name": "credit_limit",    "type": "DECIMAL(18,2)",  "nullable": True,  "description": "Credit limit — NULL for non-credit account types"},
        ],
    },
    "banking.ofi_transfers": {
        "description": (
            "External institution transfer details enriched from NACHA ACH and Fed Wire feeds. "
            "Joined to banking.transactions on txn_id. Partitioned by transfer_date."
        ),
        "partitioned_by": ["transfer_date"],
        "storage_format": "ORC",
        "location": "hdfs://datalake/banking/ofi_transfers",
        "columns": [
            {"name": "transfer_id",        "type": "VARCHAR",        "nullable": False, "description": "Unique transfer identifier"},
            {"name": "txn_id",             "type": "VARCHAR",        "nullable": False, "description": "FK to banking.transactions.txn_id"},
            {"name": "source_institution", "type": "VARCHAR",        "nullable": False, "description": "Originating institution name"},
            {"name": "dest_institution",   "type": "VARCHAR",        "nullable": False, "description": "Destination institution name"},
            {"name": "routing_number",     "type": "VARCHAR",        "nullable": False, "description": "ABA routing number of the destination"},
            {"name": "amount",             "type": "DECIMAL(18,2)",  "nullable": False, "description": "Transfer amount in USD"},
            {"name": "transfer_date",      "type": "DATE",           "nullable": False, "description": "Date of transfer — partition key"},
            {"name": "transfer_type",      "type": "VARCHAR",        "nullable": False, "description": "ACH or WIRE"},
            {"name": "status",             "type": "VARCHAR",        "nullable": False, "description": "SETTLED, PENDING, or RETURNED"},
        ],
    },
    "banking.daily_aggregates": {
        "description": (
            "Pre-aggregated daily flow metrics per account. "
            "Refreshed nightly by the daily_flow_aggregation Spark job. "
            "Primary table for trend analysis and ML feature generation."
        ),
        "partitioned_by": ["agg_date"],
        "storage_format": "PARQUET",
        "location": "hdfs://datalake/banking/daily_aggregates",
        "columns": [
            {"name": "account_id",    "type": "VARCHAR",        "nullable": False, "description": "Account identifier"},
            {"name": "agg_date",      "type": "DATE",           "nullable": False, "description": "Aggregation date — partition key"},
            {"name": "segment",       "type": "VARCHAR",        "nullable": False, "description": "Customer segment: RETAIL, BUSINESS, PREMIUM"},
            {"name": "total_credits", "type": "DECIMAL(18,2)",  "nullable": False, "description": "Sum of all credit amounts for the day"},
            {"name": "total_debits",  "type": "DECIMAL(18,2)",  "nullable": False, "description": "Sum of all debit amounts for the day"},
            {"name": "net_flow",      "type": "DECIMAL(18,2)",  "nullable": False, "description": "total_credits minus total_debits"},
            {"name": "txn_count",     "type": "INTEGER",        "nullable": False, "description": "Number of transactions for the day"},
            {"name": "ofi_outflow",   "type": "DECIMAL(18,2)",  "nullable": False, "description": "Total outflow amount sent to external institutions"},
            {"name": "ofi_inflow",    "type": "DECIMAL(18,2)",  "nullable": False, "description": "Total inflow amount received from external institutions"},
        ],
    },
}

PIPELINE_METADATA: dict[str, dict] = {
    "banking.transactions": {
        "job_name":      "txn_ingestion_pipeline",
        "schedule":      "0 2 * * *",
        "schedule_desc": "Daily at 02:00 UTC",
        "source":        "core_banking_system (CDC via Kafka topic txn_events_raw)",
        "owner_team":    "Data Engineering — Payments",
        "sla_minutes":   60,
        "spark_version": "3.4.1",
        "cluster":       "hadoop-prod-cluster-01",
        "last_run":      "SUCCESS",
        "upstream":      ["kafka.txn_events_raw", "banking.accounts"],
        "downstream":    ["banking.daily_aggregates", "analytics.ofi_summary"],
    },
    "banking.accounts": {
        "job_name":      "account_master_sync",
        "schedule":      "0 1 * * *",
        "schedule_desc": "Daily at 01:00 UTC",
        "source":        "core_banking_system (full snapshot extract)",
        "owner_team":    "Data Engineering — Core",
        "sla_minutes":   45,
        "spark_version": "3.4.1",
        "cluster":       "hadoop-prod-cluster-01",
        "last_run":      "SUCCESS",
        "upstream":      ["core_banking.account_master"],
        "downstream":    ["banking.transactions", "banking.daily_aggregates"],
    },
    "banking.ofi_transfers": {
        "job_name":      "ofi_transfer_enrichment",
        "schedule":      "30 2 * * *",
        "schedule_desc": "Daily at 02:30 UTC",
        "source":        "banking.transactions + fed_wire_feed + nacha_ach_feed",
        "owner_team":    "Data Engineering — Payments",
        "sla_minutes":   90,
        "spark_version": "3.4.1",
        "cluster":       "hadoop-prod-cluster-02",
        "last_run":      "SUCCESS",
        "upstream":      ["banking.transactions", "reference.institution_registry"],
        "downstream":    ["analytics.ofi_summary", "risk.ofi_velocity_alerts"],
    },
    "banking.daily_aggregates": {
        "job_name":      "daily_flow_aggregation",
        "schedule":      "0 4 * * *",
        "schedule_desc": "Daily at 04:00 UTC",
        "source":        "banking.transactions (previous day partition)",
        "owner_team":    "Data Engineering — Analytics",
        "sla_minutes":   120,
        "spark_version": "3.4.1",
        "cluster":       "hadoop-prod-cluster-01",
        "last_run":      "SUCCESS",
        "upstream":      ["banking.transactions", "banking.accounts"],
        "downstream":    ["analytics.flow_dashboard", "ml.training_features"],
    },
}
