# Notebook: 20_silver_yellow_taxi_snowflake
# Purpose: Clean and conform NYC Taxi Yellow data into Silver layer.

import os
from snowflake.snowpark.functions import (
    col,
    trim,
    to_date,
    date_part,
    when,
)

from notebooks.00_config_and_utils import (
    BRONZE_TABLE,
    DATABASE,
    DEDUPE_ORDER_COLUMN,
    EVENT_TIME_COLUMN,
    MERGE_MODE,
    PRIMARY_KEYS,
    SCHEMA,
    SILVER_TABLE,
    WAREHOUSE,
    ensure_db_schema,
    ensure_table,
    parse_csv_list,
    qualify_name,
    require_non_null,
    safe_identifier,
)


# ------------------------------
# Editable parameters (override via env vars if present)
# ------------------------------
DATABASE = os.getenv("DATABASE", DATABASE)
SCHEMA = os.getenv("SCHEMA", SCHEMA)
BRONZE_TABLE = os.getenv("BRONZE_TABLE", BRONZE_TABLE)
SILVER_TABLE = os.getenv("SILVER_TABLE", SILVER_TABLE)
PRIMARY_KEYS = os.getenv("PRIMARY_KEYS", PRIMARY_KEYS)
EVENT_TIME_COLUMN = os.getenv("EVENT_TIME_COLUMN", EVENT_TIME_COLUMN)
DEDUPE_ORDER_COLUMN = os.getenv("DEDUPE_ORDER_COLUMN", DEDUPE_ORDER_COLUMN)
MERGE_MODE = os.getenv("MERGE_MODE", MERGE_MODE).lower()  # append|merge
WAREHOUSE = os.getenv("WAREHOUSE", WAREHOUSE)

if WAREHOUSE:
    session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()

ensure_db_schema(session, DATABASE, SCHEMA)

full_bronze_table = qualify_name(DATABASE, SCHEMA, safe_identifier(BRONZE_TABLE))
full_silver_table = qualify_name(DATABASE, SCHEMA, safe_identifier(SILVER_TABLE))

silver_ddl = f"""
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
    PICKUP_DATE DATE,
    PICKUP_HOUR NUMBER,
    _INGEST_TS TIMESTAMP_NTZ,
    _SOURCE_FILE STRING,
    _BATCH_ID STRING
)
"""

ensure_table(session, full_silver_table, silver_ddl)

bronze_df = session.table(full_bronze_table)

cleaned_df = (
    bronze_df.select(
        col("VENDORID"),
        col("TPEP_PICKUP_DATETIME").cast("timestamp_ntz").alias("TPEP_PICKUP_DATETIME"),
        col("TPEP_DROPOFF_DATETIME").cast("timestamp_ntz").alias("TPEP_DROPOFF_DATETIME"),
        col("PASSENGER_COUNT"),
        when(col("TRIP_DISTANCE") < 0, None).otherwise(col("TRIP_DISTANCE")).alias("TRIP_DISTANCE"),
        col("RATECODEID"),
        trim(col("STORE_AND_FWD_FLAG")).alias("STORE_AND_FWD_FLAG"),
        col("PULOCATIONID"),
        col("DOLOCATIONID"),
        col("PAYMENT_TYPE"),
        when(col("FARE_AMOUNT") < 0, None).otherwise(col("FARE_AMOUNT")).alias("FARE_AMOUNT"),
        when(col("EXTRA") < 0, None).otherwise(col("EXTRA")).alias("EXTRA"),
        when(col("MTA_TAX") < 0, None).otherwise(col("MTA_TAX")).alias("MTA_TAX"),
        when(col("TIP_AMOUNT") < 0, None).otherwise(col("TIP_AMOUNT")).alias("TIP_AMOUNT"),
        when(col("TOLLS_AMOUNT") < 0, None).otherwise(col("TOLLS_AMOUNT")).alias("TOLLS_AMOUNT"),
        when(col("IMPROVEMENT_SURCHARGE") < 0, None).otherwise(col("IMPROVEMENT_SURCHARGE")).alias(
            "IMPROVEMENT_SURCHARGE"
        ),
        when(col("TOTAL_AMOUNT") < 0, None).otherwise(col("TOTAL_AMOUNT")).alias("TOTAL_AMOUNT"),
        when(col("CONGESTION_SURCHARGE") < 0, None)
        .otherwise(col("CONGESTION_SURCHARGE"))
        .alias("CONGESTION_SURCHARGE"),
        when(col("AIRPORT_FEE") < 0, None).otherwise(col("AIRPORT_FEE")).alias("AIRPORT_FEE"),
        to_date(col(EVENT_TIME_COLUMN)).alias("PICKUP_DATE"),
        date_part("HOUR", col(EVENT_TIME_COLUMN)).alias("PICKUP_HOUR"),
        col("_INGEST_TS"),
        col("_SOURCE_FILE"),
        col("_BATCH_ID"),
    )
)

keys = parse_csv_list(PRIMARY_KEYS)
keys_expr = ", ".join(keys)

staged_view = "_SILVER_STAGED"
cleaned_df.create_or_replace_temp_view(staged_view)

dedupe_sql = f"""
SELECT * FROM {staged_view}
QUALIFY ROW_NUMBER() OVER (PARTITION BY {keys_expr} ORDER BY {DEDUPE_ORDER_COLUMN} DESC)=1
"""

dedupe_df = session.sql(dedupe_sql)

if MERGE_MODE == "append":
    # For simplicity and idempotence, overwrite by truncating then inserting.
    session.sql(f"TRUNCATE TABLE {full_silver_table}").collect()
    dedupe_df.write.save_as_table(full_silver_table, mode="append")
elif MERGE_MODE == "merge":
    staged_table = "_SILVER_DEDUPED"
    dedupe_df.create_or_replace_temp_view(staged_table)
    merge_sql = f"""
    MERGE INTO {full_silver_table} AS tgt
    USING {staged_table} AS src
    ON {" AND ".join([f"tgt.{k} = src.{k}" for k in keys])}
    WHEN MATCHED THEN UPDATE SET
        TPEP_DROPOFF_DATETIME = src.TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT = src.PASSENGER_COUNT,
        TRIP_DISTANCE = src.TRIP_DISTANCE,
        RATECODEID = src.RATECODEID,
        STORE_AND_FWD_FLAG = src.STORE_AND_FWD_FLAG,
        PAYMENT_TYPE = src.PAYMENT_TYPE,
        FARE_AMOUNT = src.FARE_AMOUNT,
        EXTRA = src.EXTRA,
        MTA_TAX = src.MTA_TAX,
        TIP_AMOUNT = src.TIP_AMOUNT,
        TOLLS_AMOUNT = src.TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE = src.IMPROVEMENT_SURCHARGE,
        TOTAL_AMOUNT = src.TOTAL_AMOUNT,
        CONGESTION_SURCHARGE = src.CONGESTION_SURCHARGE,
        AIRPORT_FEE = src.AIRPORT_FEE,
        PICKUP_DATE = src.PICKUP_DATE,
        PICKUP_HOUR = src.PICKUP_HOUR,
        _INGEST_TS = src._INGEST_TS,
        _SOURCE_FILE = src._SOURCE_FILE,
        _BATCH_ID = src._BATCH_ID
    WHEN NOT MATCHED THEN INSERT (
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
        PICKUP_DATE,
        PICKUP_HOUR,
        _INGEST_TS,
        _SOURCE_FILE,
        _BATCH_ID
    ) VALUES (
        src.VENDORID,
        src.TPEP_PICKUP_DATETIME,
        src.TPEP_DROPOFF_DATETIME,
        src.PASSENGER_COUNT,
        src.TRIP_DISTANCE,
        src.RATECODEID,
        src.STORE_AND_FWD_FLAG,
        src.PULOCATIONID,
        src.DOLOCATIONID,
        src.PAYMENT_TYPE,
        src.FARE_AMOUNT,
        src.EXTRA,
        src.MTA_TAX,
        src.TIP_AMOUNT,
        src.TOLLS_AMOUNT,
        src.IMPROVEMENT_SURCHARGE,
        src.TOTAL_AMOUNT,
        src.CONGESTION_SURCHARGE,
        src.AIRPORT_FEE,
        src.PICKUP_DATE,
        src.PICKUP_HOUR,
        src._INGEST_TS,
        src._SOURCE_FILE,
        src._BATCH_ID
    )
    """
    session.sql(merge_sql).collect()
else:
    raise ValueError("Unsupported MERGE_MODE. Use 'append' or 'merge'.")

require_non_null(session, full_silver_table, keys)

row_count = session.sql(f"SELECT COUNT(*) AS CNT FROM {full_silver_table}").collect()[0]["CNT"]
min_max = session.sql(
    f"SELECT MIN({EVENT_TIME_COLUMN}) AS MIN_EVENT, MAX({EVENT_TIME_COLUMN}) AS MAX_EVENT "
    f"FROM {full_silver_table}"
).collect()[0]

distinct_dates = session.sql(
    f"SELECT COUNT(DISTINCT PICKUP_DATE) AS DATE_CNT FROM {full_silver_table}"
).collect()[0]["DATE_CNT"]

print(f"Silver row count: {row_count}")
print(f"Pickup time range: {min_max['MIN_EVENT']} - {min_max['MAX_EVENT']}")
print(f"Distinct pickup dates: {distinct_dates}")
