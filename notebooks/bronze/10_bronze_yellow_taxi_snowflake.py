# Notebook: 10_bronze_yellow_taxi_snowflake
# Purpose: Land NYC Taxi Yellow data into Bronze layer.

import os
from snowflake.snowpark.functions import col

from notebooks.00_config_and_utils import (
    BRONZE_TABLE,
    DATABASE,
    DEDUPE_ORDER_COLUMN,
    EVENT_TIME_COLUMN,
    PRIMARY_KEYS,
    SCHEMA,
    SOURCE_FORMAT,
    SOURCE_PATH,
    SOURCE_STAGE,
    SOURCE_TABLE,
    SOURCE_TYPE,
    WAREHOUSE,
    ensure_db_schema,
    ensure_table,
    qualify_name,
    safe_identifier,
)


# ------------------------------
# Editable parameters (override via env vars if present)
# ------------------------------
DATASET_NAME = os.getenv("DATASET_NAME", "nyc_taxi_yellow")
SOURCE_FORMAT = os.getenv("SOURCE_FORMAT", SOURCE_FORMAT).lower()  # parquet|csv
SOURCE_TYPE = os.getenv("SOURCE_TYPE", SOURCE_TYPE).lower()  # stage|table
SOURCE_STAGE = os.getenv("SOURCE_STAGE", SOURCE_STAGE)
SOURCE_PATH = os.getenv("SOURCE_PATH", SOURCE_PATH)
SOURCE_TABLE = os.getenv("SOURCE_TABLE", SOURCE_TABLE)
DATABASE = os.getenv("DATABASE", DATABASE)
SCHEMA = os.getenv("SCHEMA", SCHEMA)
BRONZE_TABLE = os.getenv("BRONZE_TABLE", BRONZE_TABLE)
WAREHOUSE = os.getenv("WAREHOUSE", WAREHOUSE)


# ------------------------------
# Session expectations
# ------------------------------
# Assumes an existing Snowpark Session is available as `session`.

if WAREHOUSE:
    session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()

ensure_db_schema(session, DATABASE, SCHEMA)

full_bronze_table = qualify_name(DATABASE, SCHEMA, safe_identifier(BRONZE_TABLE))

bronze_ddl = f"""
CREATE TABLE IF NOT EXISTS {{table_name}} (
    VENDORID NUMBER,
    TPEP_PICKUP_DATETIME TIMESTAMP_NTZ,
    TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
    PASSENGER_COUNT NUMBER,
    TRIP_DISTANCE FLOAT,
    RATECODEID NUMBER,
    STORE_AND_FWD_FLAG STRING,
    PULOCATIONID NUMBER,
    DOLOCATIONID NUMBER,
    PAYMENT_TYPE NUMBER,
    FARE_AMOUNT FLOAT,
    EXTRA FLOAT,
    MTA_TAX FLOAT,
    TIP_AMOUNT FLOAT,
    TOLLS_AMOUNT FLOAT,
    IMPROVEMENT_SURCHARGE FLOAT,
    TOTAL_AMOUNT FLOAT,
    CONGESTION_SURCHARGE FLOAT,
    AIRPORT_FEE FLOAT,
    _INGEST_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE STRING,
    _BATCH_ID STRING DEFAULT UUID_STRING()
    -- RAW_RECORD VARIANT -- Optional: store raw record for schema drift
)
"""

ensure_table(session, full_bronze_table, bronze_ddl)

if SOURCE_TYPE == "stage":
    stage_path = f"{SOURCE_STAGE}/{SOURCE_PATH}" if SOURCE_PATH else SOURCE_STAGE
    if SOURCE_FORMAT == "parquet":
        file_format = "(TYPE=PARQUET)"
    elif SOURCE_FORMAT == "csv":
        file_format = "(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1)"
    else:
        raise ValueError("Unsupported SOURCE_FORMAT. Use 'parquet' or 'csv'.")

    copy_sql = f"""
    COPY INTO {full_bronze_table} (
        VENDORID,
        TPEP_PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        RATECODEID,
        STORE_AND_FWD_FLAG,
        PULOCATIONID,
        DOLOCATIONID,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        TOTAL_AMOUNT,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE,
        _INGEST_TS,
        _SOURCE_FILE,
        _BATCH_ID
    )
    FROM (
        SELECT
            $1:VENDORID::NUMBER AS VENDORID,
            $1:TPEP_PICKUP_DATETIME::TIMESTAMP_NTZ AS TPEP_PICKUP_DATETIME,
            $1:TPEP_DROPOFF_DATETIME::TIMESTAMP_NTZ AS TPEP_DROPOFF_DATETIME,
            $1:PASSENGER_COUNT::NUMBER AS PASSENGER_COUNT,
            $1:TRIP_DISTANCE::FLOAT AS TRIP_DISTANCE,
            $1:RATECODEID::NUMBER AS RATECODEID,
            $1:STORE_AND_FWD_FLAG::STRING AS STORE_AND_FWD_FLAG,
            $1:PULOCATIONID::NUMBER AS PULOCATIONID,
            $1:DOLOCATIONID::NUMBER AS DOLOCATIONID,
            $1:PAYMENT_TYPE::NUMBER AS PAYMENT_TYPE,
            $1:FARE_AMOUNT::FLOAT AS FARE_AMOUNT,
            $1:EXTRA::FLOAT AS EXTRA,
            $1:MTA_TAX::FLOAT AS MTA_TAX,
            $1:TIP_AMOUNT::FLOAT AS TIP_AMOUNT,
            $1:TOLLS_AMOUNT::FLOAT AS TOLLS_AMOUNT,
            $1:IMPROVEMENT_SURCHARGE::FLOAT AS IMPROVEMENT_SURCHARGE,
            $1:TOTAL_AMOUNT::FLOAT AS TOTAL_AMOUNT,
            $1:CONGESTION_SURCHARGE::FLOAT AS CONGESTION_SURCHARGE,
            $1:AIRPORT_FEE::FLOAT AS AIRPORT_FEE,
            CURRENT_TIMESTAMP() AS _INGEST_TS,
            METADATA$FILENAME AS _SOURCE_FILE,
            UUID_STRING() AS _BATCH_ID
        FROM {stage_path}
    )
    FILE_FORMAT = {file_format}
    """
    session.sql(copy_sql).collect()
else:
    if not SOURCE_TABLE:
        raise ValueError("SOURCE_TABLE must be set when SOURCE_TYPE='table'.")
    full_source_table = qualify_name(DATABASE, SCHEMA, safe_identifier(SOURCE_TABLE))
    insert_sql = f"""
    INSERT INTO {full_bronze_table} (
        VENDORID,
        TPEP_PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        RATECODEID,
        STORE_AND_FWD_FLAG,
        PULOCATIONID,
        DOLOCATIONID,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        TOTAL_AMOUNT,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE,
        _INGEST_TS,
        _SOURCE_FILE,
        _BATCH_ID
    )
    SELECT
        VENDORID,
        TPEP_PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        RATECODEID,
        STORE_AND_FWD_FLAG,
        PULOCATIONID,
        DOLOCATIONID,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        TOTAL_AMOUNT,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE,
        CURRENT_TIMESTAMP(),
        NULL,
        UUID_STRING()
    FROM {full_source_table}
    """
    session.sql(insert_sql).collect()

row_count = session.sql(f"SELECT COUNT(*) AS CNT FROM {full_bronze_table}").collect()[0]["CNT"]
min_max = session.sql(
    f"SELECT MIN({EVENT_TIME_COLUMN}) AS MIN_EVENT, MAX({EVENT_TIME_COLUMN}) AS MAX_EVENT "
    f"FROM {full_bronze_table}"
).collect()[0]

print(f"Bronze row count: {row_count}")
print(f"Event time range: {min_max['MIN_EVENT']} - {min_max['MAX_EVENT']}")
