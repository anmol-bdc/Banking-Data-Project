from __future__ import annotations

import os
import sys
import shutil
from pathlib import Path
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F


# Resolve the repository root from this module so the ingest pipeline can
# reliably locate the data, temporary, and output directories.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Define the main data layer folders once so all ingestion steps use a
# consistent structure for raw source files and bronze output files.
DATA_ROOT = PROJECT_ROOT / "data"

RAW_BASE_PATH = DATA_ROOT / "raw"
BRONZE_BASE_PATH = DATA_ROOT / "bronze"

RAW_BASE_PATH.mkdir(parents=True, exist_ok=True)
BRONZE_BASE_PATH.mkdir(parents=True, exist_ok=True)

# Create dedicated local Spark working directories to keep shuffle and spill
# files isolated from the rest of the machine and avoid path conflicts.
SPARK_LOCAL_DIR = PROJECT_ROOT / "tmp" / "spark-local"
SPARK_TEMP_DIR = PROJECT_ROOT / "tmp" / "spark-temp"
SPARK_WAREHOUSE_DIR = PROJECT_ROOT / "spark-warehouse"

SPARK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
SPARK_TEMP_DIR.mkdir(parents=True, exist_ok=True)
SPARK_WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

os.environ["TEMP"] = str(SPARK_TEMP_DIR)
os.environ["TMP"] = str(SPARK_TEMP_DIR)
os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_LOCAL_DIR)

# Remove any stale temporary Spark files from the shared Windows temp path.
shutil.rmtree(SPARK_LOCAL_DIR, ignore_errors=True)
shutil.rmtree(SPARK_TEMP_DIR, ignore_errors=True)

SPARK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
SPARK_TEMP_DIR.mkdir(parents=True, exist_ok=True)


# Configure Java and PySpark environment variables so local Spark runs with
# the same interpreter and runtime settings expected by this project.
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

os.environ["_JAVA_OPTIONS"] = "-Djava.security.manager=allow"
os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

# Keep a simple lookup of supported tables and their purpose so the ingestion
# workflow can present readable names and validate user input consistently.
TABLE_DESCRIPTIONS: Dict[str, str] = {
    "all": "All available tables",
    "transactions": "Customer transaction records",
    "cash": "Cash position data",
    "loans": "Loan portfolio data",
    "intraday": "Intraday liquidity data",
    "fx_rates": "Foreign exchange rates",
    "scenario": "Stress testing scenarios",
}

# Map each table name to the folder that holds its raw CSV files. This keeps
# the code independent from hard-coded file paths scattered throughout the module.
USER_WINDOWS_RAW_DIRS: Dict[str, str] = {
    "cash": str(RAW_BASE_PATH / "cash"),
    "fx_rates": str(RAW_BASE_PATH / "fx"),
    "intraday": str(RAW_BASE_PATH / "intraday"),
    "loans": str(RAW_BASE_PATH / "loans"),
    "scenario": str(RAW_BASE_PATH / "scenario"),
    "transactions": str(RAW_BASE_PATH / "transactions"),
}

# Map each table name to the bronze output directory where its processed data
# should be written for downstream consumption.
USER_WINDOWS_PROCESSED_DIRS: Dict[str, str] = {
    "transactions": str(BRONZE_BASE_PATH / "transactions"),
    "cash": str(BRONZE_BASE_PATH / "cash"),
    "loans": str(BRONZE_BASE_PATH / "loans"),
    "intraday": str(BRONZE_BASE_PATH / "intraday"),
    "fx_rates": str(BRONZE_BASE_PATH / "fx"),
    "scenario": str(BRONZE_BASE_PATH / "scenario"),
}

# Build and cache a Spark session with Delta support so each ingestion job can
# reuse the same local execution context without recreating it repeatedly.
def get_spark() -> SparkSession:

    warehouse_dir = str(SPARK_WAREHOUSE_DIR.resolve()).replace("\\", "/")
    spark_local_dir = str(SPARK_LOCAL_DIR.resolve()).replace("\\", "/")

    builder = (
        SparkSession.builder
        .appName("BankingIngestionPipeline")
        .master("local[*]")

        # Point Spark at the dedicated local directory for temporary files.
        .config("spark.local.dir", spark_local_dir)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")


        # Set the Java temp directory explicitly so Spark writes do not fall back
        # to a less predictable system location.
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={spark_local_dir}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={spark_local_dir}")

        # Use a small, predictable number of partitions for local development.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")

        # Enable Delta Lake support for transactional table writes.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark

# Parse a user-provided table selection into a validated list of table names.
def parse_user_choice(user_input: str) -> List[str]:
    """
    Parse user input for table selection.
    """
    if not user_input:
        return []

    user_input = user_input.lower().strip()

    if user_input == "all":
        return list(TABLE_DESCRIPTIONS.keys())

    # Split input on commas or whitespace so users can enter a simple list.
    tokens = [t.strip() for t in user_input.replace(",", " ").split()]

    return [t for t in tokens if t in TABLE_DESCRIPTIONS]


# Locate the newest CSV file for a given table so ingestion uses the latest
# available source file rather than an older artifact in the folder.
def latest_csv_for_table(table_name: str) -> str | None:
    """
    Get latest CSV file for a table from RAW layer.
    """
    folder = USER_WINDOWS_RAW_DIRS.get(table_name)

    if not folder or not os.path.exists(folder):
        return None

    csv_files = [
        Path(folder) / f
        for f in os.listdir(folder)
        if f.endswith(".csv")
    ]

    if not csv_files:
        return None

    # Choose the most recently modified file to make the ingestion step robust
    # to repeated data drops into the same raw folder.
    latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)

    return str(latest_file)


# Return the bronze output path for a given table so the ingest logic does not
# need to hard-code storage locations in multiple places.
def get_output_path(table_name: str) -> str:
    """
    Get bronze output path.
    """
    return USER_WINDOWS_PROCESSED_DIRS[table_name]


# Define how each table should be partitioned in the bronze layer when written
# as Delta tables. The scenario table is treated separately because it is not
# partitioned in the same way as the operational data feeds.
TABLE_PARTITIONS = {
    "transactions": ["region_code", "transaction_date"],
    "cash": ["region_code", "business_date"],
    "loans": ["region_code", "business_date"],
    "intraday": ["region_code", "as_of_timestamp_date"],
    "fx_rates": ["region_code", "as_of_date"],
    "scenario": []
}


def ingest_table(spark: SparkSession, table_name: str):
    """
    Ingest raw CSV into bronze layer.

    - Scenario is written as Parquet without partitioning.
    - Other tables are written as Delta tables with table-specific region and
      date partitioning.
    - This step focuses only on ingestion and basic normalization.
    """

    csv_path = latest_csv_for_table(table_name)

    if not csv_path:
        print(f"[FAILED] No CSV found for table: {table_name}")
        return

    print(f"[STARTED] Reading CSV for {table_name}: {csv_path}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )

    # Normalize the incoming schema in a lightweight way so downstream writes
    # can rely on a consistent set of columns even when the source files are
    # slightly incomplete.
    if "region_code" not in df.columns:
        df = df.withColumn("region_code", F.lit("UNKNOWN"))

    # Add a default date column when the source file does not provide one.
    if table_name == "transactions":
        if "transaction_date" not in df.columns:
            df = df.withColumn("transaction_date", F.to_date(F.current_date()))

    elif table_name in ["cash", "loans"]:
        if "business_date" not in df.columns:
            df = df.withColumn("business_date", F.to_date(F.current_date()))

    elif table_name == "intraday":
        if "as_of_timestamp" in df.columns:
            df = df.withColumn(
                "as_of_timestamp_date",
                F.to_date(F.col("as_of_timestamp"))
            )
        else:
            df = df.withColumn("as_of_timestamp_date", F.to_date(F.current_date()))

    elif table_name == "fx_rates":
        if "as_of_date" not in df.columns:
            df = df.withColumn("as_of_date", F.to_date(F.current_date()))

    output_path = get_output_path(table_name)

    partition_cols = TABLE_PARTITIONS.get(table_name, [])

    # Write scenario data as Parquet because it does not need the same partition
    # strategy as the operational tables.
    if table_name == "scenario":
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)

        print(f"[SUCCESS] Scenario saved as parquet at {output_path}")
        return

    # Validate that the required partition columns are present before writing a
    # partitioned Delta table. This avoids ambiguous or incomplete output paths.
    missing_cols = [col for col in partition_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(
            f"[ERROR] Missing partition columns for {table_name}: {missing_cols}. "
            f"Available columns: {df.columns}"
        )

    # Write the data as a Delta table with the configured partitioning so the
    # bronze layer remains query-friendly and easy to maintain.
    df.write \
        .mode("overwrite") \
        .partitionBy(*partition_cols) \
        .format("delta") \
        .save(output_path)

    print(
        f"[SUCCESS] {table_name} ingested to {output_path} "
        f"with partition columns {partition_cols}"
    )