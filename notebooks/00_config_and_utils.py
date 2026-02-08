# Notebook: 00_config_and_utils
# Purpose: Shared configuration helpers and utilities for Snowflake Medallion notebooks.

import os
from typing import List


def _get_env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


# ------------------------------
# Editable parameters (override via env vars if present)
# ------------------------------
DATASET_NAME = _get_env_or_default("DATASET_NAME", "nyc_taxi_yellow")
SOURCE_FORMAT = _get_env_or_default("SOURCE_FORMAT", "parquet")  # parquet|csv
SOURCE_TYPE = _get_env_or_default("SOURCE_TYPE", "stage")  # stage|table
SOURCE_STAGE = _get_env_or_default("SOURCE_STAGE", "@RAW_NYC_TAXI")
SOURCE_PATH = _get_env_or_default("SOURCE_PATH", "yellow/")
SOURCE_TABLE = _get_env_or_default("SOURCE_TABLE", "")
DATABASE = _get_env_or_default("DATABASE", "")
SCHEMA = _get_env_or_default("SCHEMA", "")
BRONZE_TABLE = _get_env_or_default("BRONZE_TABLE", "BRONZE_NYC_TAXI_YELLOW")
SILVER_TABLE = _get_env_or_default("SILVER_TABLE", "SILVER_NYC_TAXI_YELLOW")
GOLD_TABLE_PREFIX = _get_env_or_default("GOLD_TABLE_PREFIX", "GOLD_NYC_TAXI_YELLOW")
PRIMARY_KEYS = _get_env_or_default(
    "PRIMARY_KEYS",
    "VENDORID,TPEP_PICKUP_DATETIME,TPEP_DROPOFF_DATETIME,PULOCATIONID,DOLOCATIONID",
)
EVENT_TIME_COLUMN = _get_env_or_default("EVENT_TIME_COLUMN", "TPEP_PICKUP_DATETIME")
DEDUPE_ORDER_COLUMN = _get_env_or_default("DEDUPE_ORDER_COLUMN", "_INGEST_TS")
MERGE_MODE = _get_env_or_default("MERGE_MODE", "append")  # append|merge
WAREHOUSE = _get_env_or_default("WAREHOUSE", "")


def parse_csv_list(value: str) -> List[str]:
    """
    Parse a CSV string into a list of uppercase, trimmed values.
    """
    if value is None:
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def qualify_name(database: str, schema: str, object_name: str) -> str:
    """
    Qualify object name with optional database and schema.
    """
    parts = [p for p in [database, schema, object_name] if p]
    return ".".join(parts)


def ensure_db_schema(session, database: str, schema: str) -> None:
    """
    Use database/schema if provided.
    """
    if database:
        session.sql(f"USE DATABASE {database}").collect()
    if schema:
        session.sql(f"USE SCHEMA {schema}").collect()


def ensure_table(session, full_table_name: str, ddl_sql: str) -> None:
    """
    Create table if not exists using provided DDL.
    """
    session.sql(ddl_sql.format(table_name=full_table_name)).collect()


def require_non_null(session, full_table_name: str, cols: List[str]) -> None:
    """
    Raise ValueError if any columns contain NULL values.
    """
    if not cols:
        return
    checks = ", ".join([f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls" for col in cols])
    result = session.sql(f"SELECT {checks} FROM {full_table_name}").collect()[0].as_dict()
    nulls = {k: v for k, v in result.items() if v and int(v) > 0}
    if nulls:
        details = ", ".join([f"{k}={v}" for k, v in nulls.items()])
        raise ValueError(f"Null values found in required columns: {details}")


def dedupe_latest_sql(src_table: str, keys: List[str], order_col: str) -> str:
    """
    Return SQL for deduplicating to latest record per key.
    """
    key_expr = ", ".join(keys)
    return (
        "SELECT * FROM "
        f"{src_table} "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {key_expr} ORDER BY {order_col} DESC)=1"
    )


def safe_identifier(name: str) -> str:
    """
    Basic validation helper for identifiers.
    Note: For full escaping, consider quoting identifiers explicitly.
    """
    if not name:
        raise ValueError("Identifier cannot be empty")
    return name


# Standard ingestion metadata columns
# _INGEST_TS TIMESTAMP_NTZ default CURRENT_TIMESTAMP()
# _SOURCE_FILE STRING (best-effort; if unavailable, set NULL)
# _BATCH_ID STRING default UUID_STRING()
