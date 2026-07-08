from __future__ import annotations

import os
import random
from collections import OrderedDict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

"""Utility helpers for generating, saving, and validating synthetic banking data.

This module acts as a small data engineering toolkit: it seeds deterministic
random behavior, creates reference datasets for customers and loans, generates
CSV files for multiple banking tables, and runs lightweight quality checks on
those outputs.
"""

# ============================================================
# GLOBAL CONFIGURATION
# ============================================================

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

TODAY_STAMP = datetime.now().strftime("%Y%m%d")
DEFAULT_TOTAL_ROWS = 5000
DEFAULT_FX_ROWS = 4000
DEFAULT_INTRADAY_ROWS = 4000

REGIONS: "OrderedDict[str, Dict[str, Any]]" = OrderedDict(
    {
        "NAMER": {
            "currency": "USD",
            "base_fx_to_usd": 1.0000,
            "txn_multiplier": 1.08,
            "loan_multiplier": 1.12,
            "cash_volatility": 1.10,
        },
        "EUROPE": {
            "currency": "EUR",
            "base_fx_to_usd": 1.0900,
            "txn_multiplier": 1.00,
            "loan_multiplier": 1.00,
            "cash_volatility": 1.00,
        },
        "APAC": {
            "currency": "JPY",
            "base_fx_to_usd": 0.0068,
            "txn_multiplier": 1.12,
            "loan_multiplier": 0.92,
            "cash_volatility": 1.13,
        },
        "MEA": {
            "currency": "AED",
            "base_fx_to_usd": 0.2723,
            "txn_multiplier": 0.95,
            "loan_multiplier": 1.05,
            "cash_volatility": 1.07,
        },
    }
)

TABLE_ALIASES: Dict[str, str] = {
    "cash": "cash",
    "fx": "fx_rates",
    "fx_rates": "fx_rates",
    "intraday": "intraday",
    "loans": "loans",
    "scenario": "scenario",
    "transactions": "transactions",
}

TABLE_DESCRIPTIONS: Dict[str, str] = {
    "intraday": "Intraday liquidity movements",
    "loans": "Loan master records",
    "fx_rates": "Foreign exchange rates",
    "cash": "Daily cash positions",
    "transactions": "Transaction activity",
    "scenario": "Stress testing scenarios",
}

TABLE_DIRECTORY_MAP: Dict[str, str] = {
    "cash": "cash",
    "fx_rates": "fx",
    "intraday": "intraday",
    "loans": "loans",
    "scenario": "scenario",
    "transactions": "transactions",
}
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_BASE_PATH = PROJECT_ROOT / "data" / "raw"


USER_WINDOWS_RAW_DIRS = {
    "cash": str(RAW_BASE_PATH / "cash"),
    "fx_rates": str(RAW_BASE_PATH / "fx"),
    "intraday": str(RAW_BASE_PATH / "intraday"),
    "loans": str(RAW_BASE_PATH / "loans"),
    "scenario": str(RAW_BASE_PATH / "scenario"),
    "transactions": str(RAW_BASE_PATH / "transactions"),
}

PRIMARY_KEYS: Dict[str, List[str]] = {
    "cash": ["cash_position_id"],
    "fx_rates": ["fx_rate_id"],
    "intraday": ["intraday_id"],
    "loans": ["loan_id"],
    "scenario": ["scenario_id"],
    "transactions": ["transaction_id"],
}

EXPECTED_SCHEMAS: Dict[str, "OrderedDict[str, str]"] = {
    "cash": OrderedDict(
        [
            ("cash_position_id", "string"),
            ("account_id", "string"),
            ("customer_id", "string"),
            ("branch_id", "string"),
            ("business_date", "date"),
            ("opening_balance", "float"),
            ("closing_balance", "float"),
            ("available_liquidity", "float"),
            ("currency_code", "string"),
            ("region_code", "string"),
            ("start_date", "date"),
            ("end_date", "date"),
            ("baseline", "string"),
        ]
    ),
    "fx_rates": OrderedDict(
        [
            ("fx_rate_id", "string"),
            ("currency_code", "string"),
            ("region_code", "string"),
            ("fx_rate_to_usd", "float"),
            ("fx_rate_from_usd", "float"),
            ("as_of_timestamp", "datetime"),
            ("start_date", "date"),
            ("end_date", "date"),
            ("baseline", "string"),
        ]
    ),
    "intraday": OrderedDict(
        [
            ("intraday_id", "string"),
            ("account_id", "string"),
            ("customer_id", "string"),
            ("loan_id", "string"),
            ("branch_id", "string"),
            ("as_of_timestamp", "datetime"),
            ("inflow_amount", "float"),
            ("outflow_amount", "float"),
            ("net_position", "float"),
            ("currency_code", "string"),
            ("region_code", "string"),
            ("start_date", "date"),
            ("end_date", "date"),
            ("baseline", "string"),
        ]
    ),
    "loans": OrderedDict(
        [
            ("loan_id", "string"),
            ("customer_id", "string"),
            ("account_id", "string"),
            ("branch_id", "string"),
            ("loan_start_date", "date"),
            ("loan_amount", "float"),
            ("outstanding_balance", "float"),
            ("interest_rate", "float"),
            ("currency_code", "string"),
            ("region_code", "string"),
            ("start_date", "date"),
            ("end_date", "date"),
            ("baseline", "string"),
            ("loan_type", "string"),
        ]
    ),
    "scenario": OrderedDict(
        [
            ("scenario_id", "int"),
            ("scenario_name", "string"),
            ("deposit_runoff_pct", "float"),
            ("loan_drawdown_pct", "float"),
            ("fx_shock_pct", "float"),
            ("liquidity_haircut_pct", "float"),
            ("funding_cost_increase_pct", "float"),
            ("market_stress_factor", "float"),
            ("effective_date", "date"),
            ("expiry_date", "date"),
        ]
    ),
    "transactions": OrderedDict(
        [
            ("transaction_id", "string"),
            ("account_id", "string"),
            ("customer_id", "string"),
            ("branch_id", "string"),
            ("transaction_date", "date"),
            ("transaction_type", "string"),
            ("transaction_amount", "float"),
            ("currency_code", "string"),
            ("region_code", "string"),
            ("loan_id", "string"),
            ("start_date", "date"),
            ("end_date", "date"),
            ("baseline", "string"),
            ("amount_bucket", "string"),
        ]
    ),
}


# ============================================================
# GENERAL HELPERS
# ============================================================

# These helpers provide a stable interface for working with the supported
# banking tables. They normalize user input, distribute row counts fairly,
# and create output paths in a predictable way so downstream generation code
# can stay simple and consistent.
def normalize_table_name(table_name: str) -> str:
    cleaned = table_name.strip().lower()
    if cleaned not in TABLE_ALIASES:
        raise ValueError(
            f"Invalid table name: {table_name}. Choose from: {', '.join(TABLE_DESCRIPTIONS.keys())}"
        )
    return TABLE_ALIASES[cleaned]


def parse_user_choice(user_input: str) -> List[str]:
    cleaned = user_input.strip().lower()
    valid = list(TABLE_DESCRIPTIONS.keys())

    if cleaned in {"all", "*", "everything"}:
        return valid

    tokens = [part.strip().lower() for part in user_input.replace(",", " ").split() if part.strip()]
    resolved: List[str] = []

    for token in tokens:
        if token in TABLE_ALIASES:
            canonical = TABLE_ALIASES[token]
            if canonical not in resolved:
                resolved.append(canonical)

    if not resolved:
        raise ValueError(
            f"Invalid selection: {user_input}. Choose from: {', '.join(valid)} or 'all'."
        )

    return resolved


def assign_rows_evenly(total_rows: int, buckets: Iterable[str]) -> Dict[str, int]:
    """Split a total row count across regions or categories as evenly as possible.

    When the number of rows cannot be divided perfectly, the remainder is
    distributed one-by-one to the first buckets so the output remains balanced.
    """
    buckets = list(buckets)
    base = total_rows // len(buckets)
    rem = total_rows % len(buckets)
    out = {bucket: base for bucket in buckets}
    for i in range(rem):
        out[buckets[i]] += 1
    return out


def safe_round(value: Any, decimals: int = 2) -> float:
    return round(float(value), decimals)


def random_date(start_dt: datetime, end_dt: datetime) -> datetime:
    delta_days = max((end_dt - start_dt).days, 0)
    return start_dt + timedelta(days=random.randint(0, delta_days))


def random_timestamp_for_day(day_value: Any) -> datetime:
    base = pd.to_datetime(day_value).to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_offset = random.randint(0, 86399)
    return base + timedelta(seconds=seconds_offset)


def weighted_amount_bucket() -> str:
    return np.random.choice(
        ["Small", "Medium", "Large", "Extreme"],
        p=[0.30, 0.40, 0.20, 0.10]
    ).item()


def amount_from_bucket(bucket: str, multiplier: float = 1.0) -> float:
    ranges = {
        "Small": (100, 1000),
        "Medium": (1000, 10000),
        "Large": (10000, 100000),
        "Extreme": (100000, 1000000),
    }
    low, high = ranges[bucket]
    return safe_round(np.random.uniform(low, high) * multiplier)


def loan_type_and_amount(multiplier: float = 1.0) -> Tuple[str, float]:
    loan_type = np.random.choice(
        ["Retail", "SME", "Corporate", "Mega Corporate"],
        p=[0.40, 0.35, 0.20, 0.05]
    ).item()

    ranges = {
        "Retail": (50_000, 500_000),
        "SME": (250_000, 2_000_000),
        "Corporate": (1_000_000, 10_000_000),
        "Mega Corporate": (8_000_000, 50_000_000),
    }
    low, high = ranges[loan_type]
    amount = safe_round(np.random.uniform(low, high) * multiplier)
    return loan_type, amount


def generate_branch_id(region_code: str, idx: int) -> str:
    return f"BR_{region_code}_{idx:04d}"


def generate_customer_id(region_code: str, idx: int) -> str:
    return f"CUST_{region_code}_{idx:06d}"


def generate_account_id(region_code: str, idx: int) -> str:
    return f"ACC_{region_code}_{idx:06d}"


def generate_loan_id(region_code: str, idx: int) -> str:
    return f"LOAN_{region_code}_{idx:06d}"


def table_output_dir(table_name: str) -> Path:
    canonical = normalize_table_name(table_name)
    if os.name == "nt":
        path = Path(USER_WINDOWS_RAW_DIRS[canonical])
        path.mkdir(parents=True, exist_ok=True)
        return path

    fallback_dirname = TABLE_DIRECTORY_MAP[canonical]
    path = LOCAL_FALLBACK_BASE / fallback_dirname
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_output_path(table_name: str, date_stamp: Optional[str] = None) -> Path:
    canonical = normalize_table_name(table_name)
    return table_output_dir(canonical) / f"{canonical}_{date_stamp or TODAY_STAMP}.csv"


def save_csv(df: pd.DataFrame, table_name: str, date_stamp: Optional[str] = None) -> Path:
    output_path = build_output_path(table_name, date_stamp=date_stamp)
    df.to_csv(output_path, index=False)
    return output_path


def find_latest_csv(table_name: str) -> Optional[Path]:
    canonical = normalize_table_name(table_name)
    folder = table_output_dir(canonical)
    candidates = sorted(
        folder.glob(f"{canonical}_*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


# ============================================================
# MASTER REFERENCE BUILDERS
# ============================================================

# These reference tables form the backbone of the synthetic data. They create
# reusable customer, account, branch, and loan identities so that the generated
# transactions, balances, and intraday movements stay connected and realistic.
def build_master_ref_df(customers_per_region: int = 1600, branches_per_region: int = 200) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for region_code, cfg in REGIONS.items():
        for idx in range(1, customers_per_region + 1):
            branch_idx = ((idx - 1) % branches_per_region) + 1
            rows.append(
                {
                    "region_code": region_code,
                    "currency_code": cfg["currency"],
                    "customer_id": generate_customer_id(region_code, idx),
                    "account_id": generate_account_id(region_code, idx),
                    "branch_id": generate_branch_id(region_code, branch_idx),
                }
            )
    return pd.DataFrame(rows)


def build_master_loans_ref_df(
    loans_per_region: int = 1800,
    customers_per_region: int = 1600,
    branches_per_region: int = 200,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for region_code, cfg in REGIONS.items():
        for idx in range(1, loans_per_region + 1):
            customer_idx = ((idx - 1) % customers_per_region) + 1
            branch_idx = ((customer_idx - 1) % branches_per_region) + 1
            rows.append(
                {
                    "region_code": region_code,
                    "currency_code": cfg["currency"],
                    "loan_id": generate_loan_id(region_code, idx),
                    "customer_id": generate_customer_id(region_code, customer_idx),
                    "account_id": generate_account_id(region_code, customer_idx),
                    "branch_id": generate_branch_id(region_code, branch_idx),
                }
            )
    return pd.DataFrame(rows)


# ============================================================
# DATA GENERATORS
# ============================================================

# The generators below create realistic banking-style datasets for each table.
# Each function uses the master reference data and regional multipliers to make
# the output look varied across geographies while remaining deterministic.
def generate_loans_df(total_rows: int = DEFAULT_TOTAL_ROWS) -> pd.DataFrame:
    rows_per_region = assign_rows_evenly(total_rows, REGIONS.keys())
    master_ref = build_master_loans_ref_df()
    records: List[Dict[str, Any]] = []

    for region_code, row_count in rows_per_region.items():
        region_master = master_ref[master_ref["region_code"] == region_code].reset_index(drop=True)
        multiplier = REGIONS[region_code]["loan_multiplier"]
        picks = np.random.choice(region_master.index, size=row_count, replace=False)

        for master_idx in picks:
            base_row = region_master.iloc[int(master_idx)]
            loan_type, loan_amount = loan_type_and_amount(multiplier)
            start_dt = random_date(datetime(2025, 1, 1), datetime(2026, 6, 10))
            end_dt = start_dt + timedelta(days=random.randint(90, 720))
            outstanding_balance = safe_round(loan_amount * np.random.uniform(0.15, 0.95))
            interest_low, interest_high = (4.0, 16.0) if loan_type != "Mega Corporate" else (3.5, 10.0)

            records.append(
                {
                    "loan_id": base_row["loan_id"],
                    "customer_id": base_row["customer_id"],
                    "account_id": base_row["account_id"],
                    "branch_id": base_row["branch_id"],
                    "loan_start_date": start_dt.date(),
                    "loan_amount": loan_amount,
                    "outstanding_balance": min(outstanding_balance, loan_amount),
                    "interest_rate": safe_round(np.random.uniform(interest_low, interest_high)),
                    "currency_code": base_row["currency_code"],
                    "region_code": base_row["region_code"],
                    "start_date": start_dt.date(),
                    "end_date": end_dt.date(),
                    "baseline": f"loan_{TODAY_STAMP}",
                    "loan_type": loan_type,
                }
            )

    return pd.DataFrame(records)


def generate_cash_df(total_rows: int = DEFAULT_TOTAL_ROWS) -> pd.DataFrame:
    rows_per_region = assign_rows_evenly(total_rows, REGIONS.keys())
    master_ref = build_master_ref_df()
    records: List[Dict[str, Any]] = []
    sequence = 1

    for region_code, row_count in rows_per_region.items():
        region_master = master_ref[master_ref["region_code"] == region_code].reset_index(drop=True)
        volatility = REGIONS[region_code]["cash_volatility"]
        picks = np.random.choice(region_master.index, size=row_count, replace=True)

        for master_idx in picks:
            base_row = region_master.iloc[int(master_idx)]
            today = datetime.now()
            end_date = min(today, datetime(2026, 6, 30))
            business_dt = random_date(datetime(2026, 6, 1), end_date).date()
            opening = safe_round(np.random.uniform(50_000, 5_000_000) * volatility)
            delta = np.random.uniform(-0.12, 0.12)
            closing = safe_round(max(opening * (1 + delta), 0.0))
            available = safe_round(max(closing * np.random.uniform(0.70, 0.98), 0.0))

            records.append(
                {
                    "cash_position_id": f"CASHPOS_{sequence:07d}",
                    "account_id": base_row["account_id"],
                    "customer_id": base_row["customer_id"],
                    "branch_id": base_row["branch_id"],
                    "business_date": business_dt,
                    "opening_balance": opening,
                    "closing_balance": closing,
                    "available_liquidity": available,
                    "currency_code": base_row["currency_code"],
                    "region_code": base_row["region_code"],
                    "start_date": business_dt,
                    "end_date": business_dt + timedelta(days=1),
                    "baseline": f"cash_{TODAY_STAMP}",
                }
            )
            sequence += 1

    return pd.DataFrame(records)


def generate_transactions_df(
    loans_df: Optional[pd.DataFrame] = None,
    total_rows: int = DEFAULT_TOTAL_ROWS
) -> pd.DataFrame:
    rows_per_region = assign_rows_evenly(total_rows, REGIONS.keys())
    master_ref = build_master_ref_df()
    if loans_df is None:
        loans_df = generate_loans_df(total_rows=max(total_rows, DEFAULT_TOTAL_ROWS))

    records: List[Dict[str, Any]] = []
    sequence = 1
    transaction_types = ["Deposit", "Withdrawal", "Transfer", "Loan Payment", "Card Payment"]
    transaction_weights = [0.25, 0.20, 0.22, 0.18, 0.15]

    for region_code, row_count in rows_per_region.items():
        region_master = master_ref[master_ref["region_code"] == region_code].reset_index(drop=True)
        region_loans = loans_df[loans_df["region_code"] == region_code].reset_index(drop=True)
        picks = np.random.choice(region_master.index, size=row_count, replace=True)

        for master_idx in picks:
            base_row = region_master.iloc[int(master_idx)]
            bucket = weighted_amount_bucket()
            amount = amount_from_bucket(bucket, REGIONS[region_code]["txn_multiplier"])
            business_end_date = min(datetime.now(), datetime(2026, 6, 30))
            txn_dt = random_date(datetime(2026, 6, 1), business_end_date).date()
            txn_type = random.choices(transaction_types, weights=transaction_weights, k=1)[0]
            loan_id = region_loans.sample(1).iloc[0]["loan_id"]

            records.append(
                {
                    "transaction_id": f"TXN_{sequence:08d}",
                    "account_id": base_row["account_id"],
                    "customer_id": base_row["customer_id"],
                    "branch_id": base_row["branch_id"],
                    "transaction_date": txn_dt,
                    "transaction_type": txn_type,
                    "transaction_amount": amount,
                    "currency_code": base_row["currency_code"],
                    "region_code": base_row["region_code"],
                    "loan_id": loan_id,
                    "start_date": txn_dt,
                    "end_date": txn_dt,
                    "baseline": f"transaction_{TODAY_STAMP}",
                    "amount_bucket": bucket,
                }
            )
            sequence += 1

    return pd.DataFrame(records)


def generate_scenario_df() -> pd.DataFrame:
    """Create a small set of stress-test scenarios used for scenario analysis."""
    rows = [
        {
            "scenario_id": 1,
            "scenario_name": "Base",
            "deposit_runoff_pct": 0.00,
            "loan_drawdown_pct": 0.00,
            "fx_shock_pct": 0.00,
            "liquidity_haircut_pct": 0.00,
            "funding_cost_increase_pct": 0.00,
            "market_stress_factor": 1.00,
            "effective_date": date(2026, 6, 10),
            "expiry_date": date(2099, 12, 31),
        },
        {
            "scenario_id": 2,
            "scenario_name": "1 Notch Downgrade",
            "deposit_runoff_pct": 0.05,
            "loan_drawdown_pct": 0.04,
            "fx_shock_pct": 0.03,
            "liquidity_haircut_pct": 0.06,
            "funding_cost_increase_pct": 0.02,
            "market_stress_factor": 1.10,
            "effective_date": date(2026, 6, 10),
            "expiry_date": date(2099, 12, 31),
        },
        {
            "scenario_id": 3,
            "scenario_name": "3 Notch Downgrade",
            "deposit_runoff_pct": 0.10,
            "loan_drawdown_pct": 0.08,
            "fx_shock_pct": 0.07,
            "liquidity_haircut_pct": 0.12,
            "funding_cost_increase_pct": 0.05,
            "market_stress_factor": 1.22,
            "effective_date": date(2026, 6, 10),
            "expiry_date": date(2099, 12, 31),
        },
        {
            "scenario_id": 4,
            "scenario_name": "Severe Downgrade",
            "deposit_runoff_pct": 0.18,
            "loan_drawdown_pct": 0.14,
            "fx_shock_pct": 0.12,
            "liquidity_haircut_pct": 0.20,
            "funding_cost_increase_pct": 0.09,
            "market_stress_factor": 1.40,
            "effective_date": date(2026, 6, 10),
            "expiry_date": date(2099, 12, 31),
        },
        {
            "scenario_id": 5,
            "scenario_name": "Combined",
            "deposit_runoff_pct": 0.25,
            "loan_drawdown_pct": 0.18,
            "fx_shock_pct": 0.15,
            "liquidity_haircut_pct": 0.25,
            "funding_cost_increase_pct": 0.12,
            "market_stress_factor": 1.55,
            "effective_date": date(2026, 6, 10),
            "expiry_date": date(2099, 12, 31),
        },
    ]
    return pd.DataFrame(rows)


def generate_fx_rates_df(total_rows: int = DEFAULT_FX_ROWS) -> pd.DataFrame:
    rows_per_region = assign_rows_evenly(total_rows, REGIONS.keys())
    rows: List[Dict[str, Any]] = []

    for region_code, row_count in rows_per_region.items():
        cfg = REGIONS[region_code]
        base_rate = cfg["base_fx_to_usd"]
        shock_up = set(np.random.choice(np.arange(row_count), size=max(4, row_count // 100), replace=False))
        remaining = list(set(range(row_count)) - shock_up)
        shock_down = set(np.random.choice(remaining, size=max(4, row_count // 100), replace=False))

        for idx in range(row_count):
            rate = base_rate * (1 + np.random.normal(0.0, 0.015))

            if idx in shock_up:
                rate = base_rate * np.random.uniform(1.03, 1.10)
            elif idx in shock_down:
                rate = base_rate * np.random.uniform(0.90, 0.97)

            rate = round(float(rate), 6)
            as_of_ts = random_timestamp_for_day(datetime.now().date())

            rows.append(
                {
                    "fx_rate_id": f"FX_{region_code}_{idx + 1:06d}",
                    "currency_code": cfg["currency"],
                    "region_code": region_code,
                    "fx_rate_to_usd": rate,
                    "fx_rate_from_usd": round(1 / rate, 6) if rate else None,
                    "as_of_timestamp": as_of_ts,
                    "start_date": date(2026, 6, 10),
                    "end_date": date(2099, 12, 31),
                    "baseline": f"fx_rates_{TODAY_STAMP}",
                }
            )

    return pd.DataFrame(rows)


def generate_intraday_df(
    total_rows: int = DEFAULT_INTRADAY_ROWS,
    loans_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    rows_per_region = assign_rows_evenly(total_rows, REGIONS.keys())
    master_ref = build_master_ref_df(customers_per_region=300, branches_per_region=25)

    if loans_df is None:
        loans_df = generate_loans_df(total_rows=max(total_rows, DEFAULT_TOTAL_ROWS))

    rows: List[Dict[str, Any]] = []
    sequence = 1

    for region_code, row_count in rows_per_region.items():
        region_master = master_ref[master_ref["region_code"] == region_code].reset_index(drop=True)
        region_loans = loans_df[loans_df["region_code"] == region_code].reset_index(drop=True)
        cfg = REGIONS[region_code]
        picks = np.random.choice(region_master.index, size=row_count, replace=True)

        dormant_positions = set(np.random.choice(np.arange(row_count), size=max(10, row_count // 50), replace=False))
        remaining = list(set(range(row_count)) - dormant_positions)
        negative_positions = set(np.random.choice(remaining, size=max(8, row_count // 66), replace=False))
        remaining = list(set(remaining) - negative_positions)
        extreme_positions = set(np.random.choice(remaining, size=max(6, row_count // 80), replace=False))

        for pos, master_idx in enumerate(picks):
            base_row = region_master.iloc[int(master_idx)]
            bucket = weighted_amount_bucket()
            inflow = amount_from_bucket(bucket, cfg["txn_multiplier"])
            outflow = amount_from_bucket(bucket, cfg["cash_volatility"])

            if pos in dormant_positions:
                inflow = 0.0
                outflow = 0.0

            if pos in extreme_positions:
                inflow = safe_round(inflow * np.random.uniform(2.0, 4.0))
                outflow = safe_round(outflow * np.random.uniform(2.0, 4.0))

            net_position = safe_round(inflow - outflow)

            if pos in negative_positions:
                net_position = -abs(net_position) if net_position != 0 else -safe_round(np.random.uniform(1000, 10000))
                outflow = safe_round(inflow + abs(net_position))

            as_of_ts = random_timestamp_for_day(datetime.now().date())
            loan_id = region_loans.sample(1).iloc[0]["loan_id"] if not region_loans.empty else None

            rows.append(
                {
                    "intraday_id": f"INTRA_{sequence:08d}",
                    "account_id": base_row["account_id"],
                    "customer_id": base_row["customer_id"],
                    "loan_id": loan_id,
                    "branch_id": base_row["branch_id"],
                    "as_of_timestamp": as_of_ts,
                    "inflow_amount": inflow,
                    "outflow_amount": outflow,
                    "net_position": safe_round(net_position),
                    "currency_code": base_row["currency_code"],
                    "region_code": base_row["region_code"],
                    "start_date": as_of_ts.date(),
                    "end_date": as_of_ts.date(),
                    "baseline": f"intraday_{TODAY_STAMP}",
                }
            )
            sequence += 1

    return pd.DataFrame(rows)


# ============================================================
# GENERATION DRIVER HELPERS
# ============================================================

def default_rows_for_table(table_name: str) -> Optional[int]:
    canonical = normalize_table_name(table_name)

    if canonical == "scenario":
        return None
    if canonical == "fx_rates":
        return DEFAULT_FX_ROWS
    if canonical == "intraday":
        return DEFAULT_INTRADAY_ROWS
    return DEFAULT_TOTAL_ROWS


def generate_table(
    table_name: str,
    total_rows: Optional[int] = None,
    shared_context: Optional[Dict[str, pd.DataFrame]] = None,
) -> pd.DataFrame:
    canonical = normalize_table_name(table_name)
    shared_context = shared_context or {}

    if canonical == "scenario":
        return generate_scenario_df()

    if canonical == "loans":
        df = generate_loans_df(total_rows or DEFAULT_TOTAL_ROWS)
        shared_context["loans"] = df
        return df

    if canonical == "transactions":
        loans_df = shared_context.get("loans")
        return generate_transactions_df(loans_df=loans_df, total_rows=total_rows or DEFAULT_TOTAL_ROWS)

    if canonical == "intraday":
        loans_df = shared_context.get("loans")
        return generate_intraday_df(total_rows=total_rows or DEFAULT_INTRADAY_ROWS, loans_df=loans_df)

    if canonical == "fx_rates":
        return generate_fx_rates_df(total_rows=total_rows or DEFAULT_FX_ROWS)

    if canonical == "cash":
        return generate_cash_df(total_rows or DEFAULT_TOTAL_ROWS)

    raise ValueError(f"Unsupported table name: {table_name}")


def generate_and_save_table(
    table_name: str,
    total_rows: Optional[int] = None,
    shared_context: Optional[Dict[str, pd.DataFrame]] = None,
) -> Tuple[pd.DataFrame, Path]:
    canonical = normalize_table_name(table_name)
    df = generate_table(canonical, total_rows=total_rows, shared_context=shared_context)
    output_path = save_csv(df, canonical)
    return df, output_path


# ============================================================
# QUALITY CHECK HELPERS
# ============================================================

# These functions inspect generated CSV outputs and turn raw data into a simple
# quality report. They check for missing values, duplicates, schema drift,
# invalid formats, and a handful of business rules that are common in banking data.
def read_csv_safely(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(file_path)


def type_matches(actual: str, expected: str) -> bool:
    if expected == "float":
        return actual in {"float", "int"}
    if expected == "datetime":
        return actual in {"datetime", "date"}
    return actual == expected


def detect_semantic_type(series: pd.Series) -> str:
    non_null = series.dropna()

    if pd.api.types.is_integer_dtype(series):
        return "int"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    if non_null.empty:
        return "unknown"

    numeric_parsed = pd.to_numeric(non_null.astype(str).str.strip(), errors="coerce")
    if numeric_parsed.notna().all():
        if np.allclose(numeric_parsed.dropna() % 1, 0):
            return "int"
        return "float"

    datetime_parsed = pd.to_datetime(non_null.astype(str).str.strip(), errors="coerce")
    if datetime_parsed.notna().all():
        time_present = (
            (datetime_parsed.dt.hour != 0)
            | (datetime_parsed.dt.minute != 0)
            | (datetime_parsed.dt.second != 0)
        )
        return "datetime" if time_present.any() else "date"

    return "string"


def column_null_metrics(df: pd.DataFrame) -> Tuple[List[Tuple[str, int, float]], int, float]:
    total_rows = len(df)
    total_cells = len(df) * len(df.columns)
    rows = []
    total_nulls = 0

    for column in df.columns:
        null_count = int(df[column].isna().sum())
        total_nulls += null_count
        null_pct = (null_count / total_rows * 100.0) if total_rows else 0.0
        rows.append((column, null_count, round(null_pct, 2)))

    completeness = (1 - (total_nulls / total_cells)) * 100.0 if total_cells else 100.0
    return rows, total_nulls, round(completeness, 2)


def duplicate_metrics(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    canonical = normalize_table_name(table_name)
    total_rows = len(df)
    duplicate_rows = int(df.duplicated().sum())
    duplicate_pct = round((duplicate_rows / total_rows) * 100.0, 2) if total_rows else 0.0

    pk_cols = PRIMARY_KEYS.get(canonical, [])
    pk_duplicate_count = 0
    if pk_cols and all(col in df.columns for col in pk_cols):
        pk_duplicate_count = int(df.duplicated(subset=pk_cols).sum())

    return {
        "duplicate_rows": duplicate_rows,
        "duplicate_pct": duplicate_pct,
        "pk_columns": pk_cols,
        "pk_duplicate_count": pk_duplicate_count,
    }


def schema_validation(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    canonical = normalize_table_name(table_name)
    expected = EXPECTED_SCHEMAS[canonical]
    expected_columns = list(expected.keys())
    actual_columns = list(df.columns)

    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]
    mismatches = []

    for column, expected_type in expected.items():
        if column in df.columns:
            actual_type = detect_semantic_type(df[column])
            if not type_matches(actual_type, expected_type):
                mismatches.append((column, expected_type, actual_type))

    return {
        "expected_count": len(expected_columns),
        "actual_count": len(actual_columns),
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "type_mismatches": mismatches,
    }


def format_validity(df: pd.DataFrame, table_name: str) -> List[Tuple[str, str, int]]:
    canonical = normalize_table_name(table_name)
    checks: List[Tuple[str, str, int]] = []

    for column, expected_type in EXPECTED_SCHEMAS[canonical].items():
        if column not in df.columns:
            continue

        non_null = df[column].dropna()
        if non_null.empty:
            continue

        if expected_type in {"float", "int"}:
            invalid = int(pd.to_numeric(non_null.astype(str).str.strip(), errors="coerce").isna().sum())
            checks.append((column, "Invalid numeric values", invalid))
        elif expected_type in {"date", "datetime"}:
            invalid = int(pd.to_datetime(non_null.astype(str).str.strip(), errors="coerce").isna().sum())
            label = "Invalid datetime format" if expected_type == "datetime" else "Invalid date format"
            checks.append((column, label, invalid))

    return checks


def column_statistics(df: pd.DataFrame, max_columns: int = 4) -> List[Dict[str, Any]]:
    stats: List[Dict[str, Any]] = []

    for column in df.columns:
        if len(stats) >= max_columns:
            break

        non_null = df[column].dropna()
        if non_null.empty:
            continue

        numeric = pd.to_numeric(non_null, errors="coerce")
        if numeric.notna().all():
            stats.append(
                {
                    "column": column,
                    "kind": "numeric",
                    "min": round(float(numeric.min()), 4),
                    "max": round(float(numeric.max()), 4),
                    "mean": round(float(numeric.mean()), 4),
                    "distinct": int(non_null.nunique(dropna=True)),
                }
            )
        else:
            value_counts = non_null.astype(str).value_counts(dropna=True)
            top_value = value_counts.index[0]
            top_pct = round((value_counts.iloc[0] / len(non_null)) * 100.0, 2)
            stats.append(
                {
                    "column": column,
                    "kind": "categorical",
                    "distinct": int(non_null.nunique(dropna=True)),
                    "top_value": top_value,
                    "top_pct": top_pct,
                }
            )

    return stats


def business_rule_checks(df: pd.DataFrame, table_name: str) -> List[Tuple[str, int, str]]:
    canonical = normalize_table_name(table_name)
    results: List[Tuple[str, int, str]] = []
    today = pd.Timestamp(datetime.now().date())

    if canonical == "transactions":
        if "transaction_amount" in df.columns:
            count = int((pd.to_numeric(df["transaction_amount"], errors="coerce") < 0).fillna(False).sum())
            results.append(("Negative transaction amount", count, "FAIL" if count else "PASS"))

        if "transaction_date" in df.columns:
            dt = pd.to_datetime(df["transaction_date"], errors="coerce")
            count = int((dt > today).fillna(False).sum())
            results.append(("Future transaction date", count, "FAIL" if count else "PASS"))

    elif canonical == "fx_rates":
        if "fx_rate_to_usd" in df.columns:
            count = int((pd.to_numeric(df["fx_rate_to_usd"], errors="coerce") <= 0).fillna(False).sum())
            results.append(("FX rate <= 0", count, "FAIL" if count else "PASS"))

    elif canonical == "intraday":
        if all(col in df.columns for col in ["inflow_amount", "outflow_amount", "net_position"]):
            inflow = pd.to_numeric(df["inflow_amount"], errors="coerce")
            outflow = pd.to_numeric(df["outflow_amount"], errors="coerce")
            netpos = pd.to_numeric(df["net_position"], errors="coerce")
            expected_net = (inflow - outflow).round(2)
            count = int(
                (
                    (expected_net.round(2) != netpos.round(2))
                    & expected_net.notna()
                    & netpos.notna()
                ).sum()
            )
            results.append(("Net position mismatch (inflow - outflow)", count, "FAIL" if count else "PASS"))

    elif canonical == "cash":
        if all(col in df.columns for col in ["opening_balance", "closing_balance", "available_liquidity"]):
            avail = pd.to_numeric(df["available_liquidity"], errors="coerce")
            close = pd.to_numeric(df["closing_balance"], errors="coerce")
            count = int((avail > close).fillna(False).sum())
            results.append(("Available liquidity > closing balance", count, "FAIL" if count else "PASS"))

    elif canonical == "loans":
        if all(col in df.columns for col in ["outstanding_balance", "loan_amount"]):
            out_bal = pd.to_numeric(df["outstanding_balance"], errors="coerce")
            loan_amt = pd.to_numeric(df["loan_amount"], errors="coerce")
            count = int((out_bal > loan_amt).fillna(False).sum())
            results.append(("Outstanding balance > loan amount", count, "FAIL" if count else "PASS"))

    return results


def record_count_check(df: pd.DataFrame, expected_rows: Optional[int] = None) -> Tuple[str, int, str]:
    actual = len(df)
    if expected_rows is None:
        return "Not enforced", actual, "PASS"

    status = "PASS" if abs(actual - expected_rows) <= max(5, int(0.05 * expected_rows)) else "FAIL"
    return f"~{expected_rows}", actual, status


def quality_score(
    completeness_rate: float,
    duplicate_pct: float,
    invalid_count: int,
    schema_issues: int,
    total_rows: int,
) -> Dict[str, Any]:
    uniqueness = max(0.0, 100.0 - duplicate_pct)
    denominator = max(total_rows, 1)
    validity = max(0.0, 100.0 - ((invalid_count + schema_issues) / denominator * 100.0))
    final_score = round((completeness_rate + uniqueness + validity) / 3.0, 2)

    if final_score >= 95:
        status = "GOOD"
    elif final_score >= 85:
        status = "NEEDS ATTENTION"
    else:
        status = "POOR"

    return {
        "Completeness": round(completeness_rate, 2),
        "Uniqueness": round(uniqueness, 2),
        "Validity": round(validity, 2),
        "Final Score": final_score,
        "Status": status,
    }


def run_quality_check(
    table_name: str,
    file_path: Optional[Path] = None,
    expected_rows: Optional[int] = None,
) -> Dict[str, Any]:
    canonical = normalize_table_name(table_name)
    file_path = Path(file_path) if file_path else find_latest_csv(canonical)

    if not file_path or not Path(file_path).exists():
        raise FileNotFoundError(f"No CSV file found for table '{canonical}'.")

    df = read_csv_safely(Path(file_path))

    total_rows = len(df)
    total_columns = len(df.columns)
    total_cells = total_rows * total_columns

    null_rows, overall_nulls, completeness_rate = column_null_metrics(df)
    duplicate_info = duplicate_metrics(df, canonical)
    schema_info = schema_validation(df, canonical)
    format_info = format_validity(df, canonical)
    invalid_format_count = sum(count for _, _, count in format_info)
    business_rules = business_rule_checks(df, canonical)
    business_rule_failures = sum(count for _, count, _ in business_rules)

    schema_issue_count = (
        len(schema_info["missing_columns"])
        + len(schema_info["extra_columns"])
        + len(schema_info["type_mismatches"])
    )

    score = quality_score(
        completeness_rate=completeness_rate,
        duplicate_pct=duplicate_info["duplicate_pct"],
        invalid_count=invalid_format_count + business_rule_failures,
        schema_issues=schema_issue_count,
        total_rows=total_rows,
    )

    expected_text, actual_rows, count_status = record_count_check(df, expected_rows=expected_rows)

    checks_for_summary = [
        overall_nulls == 0,
        duplicate_info["duplicate_rows"] == 0,
        duplicate_info["pk_duplicate_count"] == 0,
        not schema_info["missing_columns"],
        not schema_info["extra_columns"],
        not schema_info["type_mismatches"],
        invalid_format_count == 0,
        business_rule_failures == 0,
        count_status == "PASS",
    ]

    passed_checks = sum(bool(x) for x in checks_for_summary)
    failed_checks = len(checks_for_summary) - passed_checks

    major_issues: List[str] = []
    if overall_nulls:
        top_null = sorted(null_rows, key=lambda x: x[1], reverse=True)[0]
        if top_null[1] > 0:
            major_issues.append(f"{top_null[0]} has nulls")
    if duplicate_info["pk_duplicate_count"]:
        major_issues.append(f"duplicate {', '.join(duplicate_info['pk_columns'])} found")
    if schema_info["missing_columns"]:
        major_issues.append(f"missing column(s): {', '.join(schema_info['missing_columns'])}")
    if schema_info["extra_columns"]:
        major_issues.append(f"extra column(s): {', '.join(schema_info['extra_columns'])}")
    if not major_issues:
        major_issues.append("No major issues detected")

    recommendation = (
        "Proceed to ingestion"
        if failed_checks == 0
        else "Fix schema / validity issues and deduplicate before ingestion"
    )

    return {
        "table_name": canonical,
        "file_path": str(file_path),
        "total_rows": total_rows,
        "total_columns": total_columns,
        "total_cells": total_cells,
        "null_rows": null_rows,
        "overall_nulls": overall_nulls,
        "completeness_rate": completeness_rate,
        "duplicate_info": duplicate_info,
        "schema_info": schema_info,
        "format_info": format_info,
        "column_stats": column_statistics(df),
        "business_rules": business_rules,
        "record_count": {"expected": expected_text, "actual": actual_rows, "status": count_status},
        "score": score,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "major_issues": major_issues,
        "recommendation": recommendation,
    }


def print_quality_report(report: Dict[str, Any]) -> None:
    print("=" * 78)
    print("CSV DATA QUALITY REPORT")
    print("=" * 78)
    print(f"Table Name   : {report['table_name']}")
    print(f"File Checked : {report['file_path']}")
    print()

    print(f"Total Rows   : {report['total_rows']:,}")
    print(f"Total Columns: {report['total_columns']:,}")
    print(f"Total Cells  : {report['total_cells']:,}")
    print()

    print("--- NULL VALUE ANALYSIS ---")
    print("Column-wise Null Count + %:")
    for column, count, pct in report["null_rows"]:
        print(f"{column:<22}: {count:>8,} ({pct:>6.2f}%)")
    print()
    print(f"Overall Null Cells  : {report['overall_nulls']:,}")
    print(f"Completeness Rate   : {report['completeness_rate']:.2f}%")
    print()

    dup = report["duplicate_info"]
    print("--- DUPLICATE ANALYSIS ---")
    print(f"Total Duplicate Rows : {dup['duplicate_rows']:,}")
    print(f"Duplicate Percentage : {dup['duplicate_pct']:.2f}%")
    if dup["pk_columns"]:
        print(f"Primary Key Duplicate Check ({', '.join(dup['pk_columns'])}):")
        print(f"Duplicate Keys Found : {dup['pk_duplicate_count']:,}")
    print()

    schema = report["schema_info"]
    print("--- SCHEMA VALIDATION ---")
    print(f"Expected Columns : {schema['expected_count']}")
    print(f"Actual Columns   : {schema['actual_count']}")
    print()
    print("Missing Columns:")
    if schema["missing_columns"]:
        for col in schema["missing_columns"]:
            print(f"- {col}")
    else:
        print("- None")
    print()
    print("Extra Columns:")
    if schema["extra_columns"]:
        for col in schema["extra_columns"]:
            print(f"- {col}")
    else:
        print("- None")
    print()
    print("Data Type Mismatch:")
    if schema["type_mismatches"]:
        for col, expected, actual in schema["type_mismatches"]:
            print(f"{col} -> Expected: {expected} | Found: {actual}")
    else:
        print("None")
    print()

    print("--- DATA TYPE VALIDATION ---")
    if report["format_info"]:
        for column, label, count in report["format_info"]:
            print(f"{column}: {label}: {count:,}")
    else:
        print("No data type / format violations detected.")
    print()

    print("--- COLUMN STATISTICS ---")
    if report["column_stats"]:
        for item in report["column_stats"]:
            print(f"{item['column']}:")
            if item["kind"] == "numeric":
                print(f"  Min: {item['min']}")
                print(f"  Max: {item['max']}")
                print(f"  Mean: {item['mean']}")
                print(f"  Distinct Values: {item['distinct']}")
            else:
                print(f"  Distinct Values: {item['distinct']}")
                print(f"  Top Value: {item['top_value']} ({item['top_pct']}%)")
    else:
        print("No stats available.")
    print()

    print("--- BUSINESS RULE VALIDATION ---")
    if report["business_rules"]:
        for rule_name, count, status in report["business_rules"]:
            print(f"{rule_name}: {count:,} rows {status}")
    else:
        print("No table-specific business rules configured.")
    print()

    rc = report["record_count"]
    print("--- RECORD COUNT CHECK ---")
    print(f"Expected Rows : {rc['expected']}")
    print(f"Actual Rows   : {rc['actual']:,} {rc['status']}")
    print()

    print("--- OVERALL DATA QUALITY SCORE ---")
    for key, value in report["score"].items():
        if isinstance(value, (int, float)):
            print(f"{key:<14}: {value:.2f}%")
        else:
            print(f"{key:<14}: {value}")
    print()

    print("--- SUMMARY ---")
    print(f"Passed Checks : {report['passed_checks']}")
    print(f"Failed Checks : {report['failed_checks']}")
    print()
    print("Major Issues:")
    for issue in report["major_issues"]:
        print(f"- {issue}")
    print()
    print("Recommendation:")
    print(report["recommendation"])
    print("=" * 78)
