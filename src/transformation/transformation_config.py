from pathlib import Path

# ============================================================
# Project root and workspace resolution
# Resolves the repository root so all layer paths can be built
# consistently from the location of this configuration module.
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ============================================================
# Data layer root directories
# Defines the bronze, silver, and gold storage locations used by
# the transformation pipeline.
# ============================================================

DATA_ROOT = PROJECT_ROOT / "data"

BRONZE_ROOT = DATA_ROOT / "bronze"
SILVER_ROOT = DATA_ROOT / "silver"
GOLD_ROOT = DATA_ROOT / "gold"

# Create directories if not exist
for path in [BRONZE_ROOT, SILVER_ROOT, GOLD_ROOT]:
    path.mkdir(parents=True, exist_ok=True)

# ============================================================
# Bronze input paths
# Maps each source table name to its location in the bronze layer.
# ============================================================

BRONZE_TABLE_PATHS = {
    "transactions": BRONZE_ROOT / "transactions",
    "cash": BRONZE_ROOT / "cash",
    "loans": BRONZE_ROOT / "loans",
    "intraday": BRONZE_ROOT / "intraday",
    "fx_rates": BRONZE_ROOT / "fx",
    "scenario": BRONZE_ROOT / "scenario",
}

# ============================================================
# Silver and gold output paths
# Maps each business table to its target output directory for the
# intermediate and final transformation layers.
# ============================================================

SILVER_TABLE_PATHS = {
    "transactions": SILVER_ROOT / "transactions",
    "cash": SILVER_ROOT / "cash",
    "loans": SILVER_ROOT / "loans",
    "intraday": SILVER_ROOT / "intraday",
}

GOLD_TABLE_PATHS = {
    "transactions": GOLD_ROOT / "transactions",
    "cash": GOLD_ROOT / "cash",
    "loans": GOLD_ROOT / "loans",
    "intraday": GOLD_ROOT / "intraday",
}

# ============================================================
# Date column mapping
# Associates each table with the date field that should be used when
# normalizing or partitioning temporal data.
# ============================================================

TABLE_DATE_COLUMNS = {
    "transactions": "transaction_date",
    "cash": "business_date",
    "loans": "business_date",
    "intraday": "as_of_timestamp_date",
    "fx_rates": "as_of_date",
}

# ============================================================
# Partition configuration
# Specifies the columns used to partition silver and gold outputs so
# that data is organized by region, date, and scenario.
# ============================================================

# Silver → Region + Date
SILVER_PARTITIONS = {
    "transactions": ["region_code", "transaction_date"],
    "cash": ["region_code", "business_date"],
    "loans": ["region_code", "business_date"],
    "intraday": ["region_code", "as_of_timestamp_date"],
}

# Gold → Scenario + Region + Date
GOLD_PARTITIONS = {
    "transactions": ["scenario_id", "region_code", "transaction_date"],
    "cash": ["scenario_id", "region_code", "business_date"],
    "loans": ["scenario_id", "region_code", "business_date"],
    "intraday": ["scenario_id", "region_code", "as_of_timestamp_date"],
}

# ============================================================
# Monetary columns for FX conversion
# Lists the amount fields that should receive USD-based transformed
# values when the currency conversion step runs.
# ============================================================

TABLE_AMOUNT_COLUMNS = {
    "transactions": ["transaction_amount"],
    "cash": ["closing_balance", "available_liquidity"],
    "loans": ["loan_amount", "outstanding_balance"],
    "intraday": ["inflow_amount", "outflow_amount", "net_position"],
}

# ============================================================
# Transformable tables
# Defines the set of business tables that should be processed by the
# transformation pipeline.
# ============================================================

TRANSFORMABLE_TABLES = ["transactions", "cash", "loans", "intraday"]