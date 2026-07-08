from __future__ import annotations

import os
import sys
import shutil
from pathlib import Path
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

# ============================================================
# Environment setup
# Configures Java, Python, and Spark runtime variables so local
# PySpark execution can start correctly on this machine.
# ============================================================

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ.get("PATH", "")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

os.environ["_JAVA_OPTIONS"] = "-Djava.security.manager=allow"
os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

# ============================================================
# Project paths
# Resolves the workspace and local Spark directories used for reading,
# writing, and caching temporary transformation data.
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SPARK_LOCAL_DIR = PROJECT_ROOT / "tmp" / "spark-local"
SPARK_WAREHOUSE_DIR = PROJECT_ROOT / "spark-warehouse"

SPARK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
SPARK_WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# Spark session creation
# Builds a local Spark session with Delta support and a stable memory
# configuration for development and testing.
# ============================================================

def get_spark(app_name: str = "TransformationPipeline") -> SparkSession:

    spark_local_dir = str(SPARK_LOCAL_DIR.resolve()).replace("\\", "/")
    warehouse_dir = str(SPARK_WAREHOUSE_DIR.resolve()).replace("\\", "/")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        .config("spark.local.dir", spark_local_dir)
        .config("spark.sql.warehouse.dir", warehouse_dir)

        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={spark_local_dir}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={spark_local_dir}")

        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")

        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")

        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "1")

        .config("spark.sql.autoBroadcastJoinThreshold", "-1")

        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark

# ============================================================
# Read helpers
# Provides simple wrappers for loading Delta and Parquet data into
# Spark DataFrames with lightweight logging.
# ============================================================

def read_delta(spark: SparkSession, path: Path) -> DataFrame:
    df = spark.read.format("delta").load(str(path))
    print(f"[INFO] Loaded data from {path}")
    return df

def read_parquet(spark: SparkSession, path: Path) -> DataFrame:
    df = spark.read.parquet(str(path))
    print(f"[INFO] Loaded data from {path}")
    return df

# ============================================================
# Write helpers
# Safely prepares output directories and writes partitioned Delta data
# for the silver and gold transformation layers.
# ============================================================

def safe_rmtree(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def write_delta_partitioned(
    df: DataFrame,
    output_path: Path,
    partition_cols: List[str]
):
    ensure_dir(output_path.parent)
    safe_rmtree(output_path)

    df = df.coalesce(2)

    df.write \
        .mode("overwrite") \
        .partitionBy(*partition_cols) \
        .format("delta") \
        .save(str(output_path))

    print(f"[SUCCESS] Written to {output_path} with partitions {partition_cols}")

# ============================================================
# Cleaning helpers
# Standardizes column names and removes duplicate rows so that the
# downstream transformation steps work with consistent inputs.
# ============================================================

def normalize_columns(df: DataFrame) -> DataFrame:
    new_cols = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df.toDF(*new_cols)

def deduplicate(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()

# ============================================================
# Type helpers
# Ensures that important columns are cast to numeric, string, or date
# values so the transformation logic can operate reliably.
# ============================================================

def ensure_numeric(df: DataFrame, col: str, default=0.0) -> DataFrame:
    if col in df.columns:
        return df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(default)))
    return df.withColumn(col, F.lit(default))

def ensure_string(df: DataFrame, col: str, default="UNKNOWN") -> DataFrame:
    if col in df.columns:
        return df.withColumn(col, F.coalesce(F.col(col).cast("string"), F.lit(default)))
    return df.withColumn(col, F.lit(default))

def ensure_date(df: DataFrame, col: str) -> DataFrame:
    if col in df.columns:
        return df.withColumn(col, F.to_date(F.col(col)))
    return df.withColumn(col, F.current_date())

# ============================================================
# Logging helpers
# Exposes a simple mechanism for reporting pipeline progress without
# introducing heavy dependency on external logging tools.
# ============================================================

def print_record_count(message: str, df):
    print(f"[INFO] {message}")