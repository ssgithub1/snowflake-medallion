# Notebook: 30_gold_yellow_taxi_snowflake
# Purpose: Create Gold marts from Silver layer.

import os
from snowflake.snowpark.functions import col, count, sum as sum_, avg, datediff

from notebooks.00_config_and_utils import (
    DATABASE,
    EVENT_TIME_COLUMN,
    GOLD_TABLE_PREFIX,
    SCHEMA,
    SILVER_TABLE,
    WAREHOUSE,
    ensure_db_schema,
    qualify_name,
    safe_identifier,
)


# ------------------------------
# Editable parameters (override via env vars if present)
# ------------------------------
DATABASE = os.getenv("DATABASE", DATABASE)
SCHEMA = os.getenv("SCHEMA", SCHEMA)
SILVER_TABLE = os.getenv("SILVER_TABLE", SILVER_TABLE)
GOLD_TABLE_PREFIX = os.getenv("GOLD_TABLE_PREFIX", GOLD_TABLE_PREFIX)
EVENT_TIME_COLUMN = os.getenv("EVENT_TIME_COLUMN", EVENT_TIME_COLUMN)
WAREHOUSE = os.getenv("WAREHOUSE", WAREHOUSE)

if WAREHOUSE:
    session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()

ensure_db_schema(session, DATABASE, SCHEMA)

full_silver_table = qualify_name(DATABASE, SCHEMA, safe_identifier(SILVER_TABLE))

_daily_table = f"{GOLD_TABLE_PREFIX}_DAILY_TRIP_METRICS"
_hourly_table = f"{GOLD_TABLE_PREFIX}_HOURLY_TRIP_METRICS"

full_daily_table = qualify_name(DATABASE, SCHEMA, safe_identifier(_daily_table))
full_hourly_table = qualify_name(DATABASE, SCHEMA, safe_identifier(_hourly_table))

silver_df = session.table(full_silver_table)

# Daily metrics

daily_df = (
    silver_df.group_by(col("PICKUP_DATE"))
    .agg(
        count("*").alias("TRIP_COUNT"),
        sum_(col("FARE_AMOUNT")).alias("TOTAL_FARE"),
        sum_(col("TOTAL_AMOUNT")).alias("TOTAL_REVENUE"),
        avg(col("TRIP_DISTANCE")).alias("AVG_DISTANCE"),
        avg(datediff("minute", col("TPEP_PICKUP_DATETIME"), col("TPEP_DROPOFF_DATETIME"))).alias(
            "AVG_DURATION_MIN"
        ),
    )
)

daily_df.write.save_as_table(full_daily_table, mode="overwrite")

# Hourly metrics

hourly_df = (
    silver_df.group_by(col("PICKUP_DATE"), col("PICKUP_HOUR"))
    .agg(
        count("*").alias("TRIP_COUNT"),
        sum_(col("TOTAL_AMOUNT")).alias("TOTAL_REVENUE"),
    )
)

hourly_df.write.save_as_table(full_hourly_table, mode="overwrite")

count_daily = session.sql(f"SELECT COUNT(*) AS CNT FROM {full_daily_table}").collect()[0]["CNT"]
count_hourly = session.sql(f"SELECT COUNT(*) AS CNT FROM {full_hourly_table}").collect()[0]["CNT"]

print(f"Daily mart row count: {count_daily}")
print(f"Hourly mart row count: {count_hourly}")
