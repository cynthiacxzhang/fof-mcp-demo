"""
Deterministic synthetic data generator for the Flow-of-Funds demo.
All data is fictional. Fixed random seed ensures reproducible results.
"""

import random
from datetime import date, timedelta

_SEED = 42

_OFI_INSTITUTIONS = [
    ("Chase",            "021000021"),
    ("Bank of America",  "026009593"),
    ("Wells Fargo",      "121042882"),
    ("Citibank",         "021272655"),
    ("US Bancorp",       "081000210"),
    ("TD Bank",          "031101266"),
    ("Capital One",      "051405515"),
    ("PNC Bank",         "043000096"),
    ("Ally Bank",        "124003116"),
    ("Discover Bank",    "031100649"),
]

_MERCHANTS: dict[str, list[str]] = {
    "PURCHASE":     ["Amazon", "Walmart", "Target", "Costco", "Whole Foods", "CVS",
                     "Home Depot", "Apple Store", "Best Buy", "Shell Gas", "Uber", "DoorDash"],
    "BILL_PAYMENT": ["AT&T", "Comcast", "Con Edison", "State Farm", "Verizon",
                     "Geico", "Blue Cross Blue Shield", "IRS", "Student Loans Co", "NYC Water"],
    "PAYROLL":      ["EMPLOYER DIRECT DEP", "ADP PAYROLL", "GUSTO PAYROLL", "PAYCHEX DIRECT"],
    "ATM":          ["ATM WITHDRAWAL"],
    "TRANSFER":     ["INTERNAL TRANSFER", "ZELLE TRANSFER", "ACH TRANSFER"],
}

_SEGMENTS   = ["RETAIL", "RETAIL", "RETAIL", "BUSINESS", "PREMIUM"]
_ACCT_TYPES = ["CHECKING", "SAVINGS", "MONEY_MARKET"]


def generate_accounts(n: int = 30) -> list[dict]:
    rng = random.Random(_SEED)
    accounts = []
    for i in range(n):
        acct_type = rng.choice(_ACCT_TYPES)
        accounts.append({
            "account_id":      f"ACC{str(i + 1).zfill(6)}",
            "account_type":    acct_type,
            "segment":         rng.choice(_SEGMENTS),
            "open_date":       date(2018, 1, 1) + timedelta(days=rng.randint(0, 1825)),
            "status":          "ACTIVE",
            "current_balance": round(rng.uniform(500.0, 75000.0), 2),
            "credit_limit":    round(rng.uniform(5000.0, 25000.0), 2) if acct_type == "CHECKING" else None,
        })
    return accounts


def generate_transactions(
    accounts:   list[dict],
    start_date: date = date(2025, 10, 1),
    end_date:   date = date(2025, 12, 31),
) -> tuple[list[dict], list[dict]]:
    rng   = random.Random(_SEED + 1)
    txns: list[dict] = []
    ofis: list[dict] = []
    txn_n = 1
    xfr_n = 1
    span  = (end_date - start_date).days + 1

    for acct in accounts:
        for _ in range(rng.randint(12, 30)):
            txn_date = start_date + timedelta(days=rng.randint(0, span - 1))

            category = rng.choices(
                ["PURCHASE", "BILL_PAYMENT", "PAYROLL", "ATM", "TRANSFER"],
                weights=[40, 20, 15, 10, 15],
            )[0]

            direction = "CREDIT" if category == "PAYROLL" else "DEBIT"
            if category == "TRANSFER" and rng.random() < 0.35:
                direction = "CREDIT"

            merchant = rng.choice(_MERCHANTS[category])

            if   category == "PAYROLL":      amount = round(rng.uniform(1500, 5500), 2)
            elif category == "PURCHASE":     amount = round(rng.uniform(5,    600),  2)
            elif category == "BILL_PAYMENT": amount = round(rng.uniform(50,   400),  2)
            elif category == "ATM":          amount = float(rng.choice([40, 60, 80, 100, 200, 300]))
            else:                            amount = round(rng.uniform(100,  3000), 2)

            channel = rng.choices(
                ["CARD", "ACH", "WIRE", "INTERNAL", "CHECK"],
                weights=[35, 25, 10, 20, 10],
            )[0]
            if category == "PAYROLL":
                channel = "ACH"

            is_ofi    = channel in ("ACH", "WIRE") and category == "TRANSFER"
            ofi_name, routing = rng.choice(_OFI_INSTITUTIONS)
            ofi_id    = ofi_name.upper().replace(" ", "_") if is_ofi else None

            txn_id = f"TXN{str(txn_n).zfill(8)}"
            txn_n += 1

            txns.append({
                "txn_id":      txn_id,
                "account_id":  acct["account_id"],
                "txn_date":    txn_date,
                "posted_date": txn_date + timedelta(days=rng.randint(0, 2)),
                "amount":      amount,
                "direction":   direction,
                "category":    category,
                "merchant":    merchant,
                "channel":     channel,
                "ofi_id":      ofi_id,
                "description": f"{merchant} - {category}",
                "status": rng.choices(
                    ["POSTED", "PENDING", "REVERSED"],
                    weights=[75, 15, 10],
                )[0],
            })

            if is_ofi:
                src = "INTERNAL_BANK" if direction == "DEBIT" else ofi_name
                dst = ofi_name        if direction == "DEBIT" else "INTERNAL_BANK"
                ofis.append({
                    "transfer_id":        f"XFR{str(xfr_n).zfill(7)}",
                    "txn_id":             txn_id,
                    "source_institution": src,
                    "dest_institution":   dst,
                    "routing_number":     routing,
                    "amount":             amount,
                    "transfer_date":      txn_date,
                    "transfer_type":      channel,
                    "status": rng.choices(
                        ["SETTLED", "PENDING", "RETURNED"],
                        weights=[75, 15, 10],
                    )[0],
                })
                xfr_n += 1

    return txns, ofis


def generate_daily_aggregates(accounts: list[dict], transactions: list[dict]) -> list[dict]:
    from collections import defaultdict

    by_acct_date: dict[tuple, list] = defaultdict(list)
    for t in transactions:
        by_acct_date[(t["account_id"], t["txn_date"])].append(t)

    seg_map = {a["account_id"]: a["segment"] for a in accounts}
    rows = []

    for (account_id, agg_date), day_txns in by_acct_date.items():
        credits = sum(t["amount"] for t in day_txns if t["direction"] == "CREDIT")
        debits  = sum(t["amount"] for t in day_txns if t["direction"] == "DEBIT")
        ofi_out = sum(t["amount"] for t in day_txns if t["ofi_id"] and t["direction"] == "DEBIT")
        ofi_in  = sum(t["amount"] for t in day_txns if t["ofi_id"] and t["direction"] == "CREDIT")

        rows.append({
            "account_id":    account_id,
            "agg_date":      agg_date,
            "segment":       seg_map.get(account_id, "RETAIL"),
            "total_credits": round(credits,          2),
            "total_debits":  round(debits,           2),
            "net_flow":      round(credits - debits, 2),
            "txn_count":     len(day_txns),
            "ofi_outflow":   round(ofi_out, 2),
            "ofi_inflow":    round(ofi_in,  2),
        })

    return rows
