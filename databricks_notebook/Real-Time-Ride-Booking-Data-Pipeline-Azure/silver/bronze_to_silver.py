# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG real_time_ride_booking_data_pipeline_azure;
# MAGIC USE SCHEMA SILVER

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Get last_processed_ingestion_ts from watermark table
last_processed_ingestion_ts = spark.sql("""
                                        SELECT source_last_ingestion_ts 
                                        FROM real_time_ride_booking_data_pipeline_azure.silver.etl_watermark 
                                        WHERE source_table = 'uber_ride_events'
                                        """)\
                                            .collect()[0][0]


# COMMAND ----------

# Read Incremental Bronze event Data
uber_ride_events_df = spark.table(
    "real_time_ride_booking_data_pipeline_azure.bronze.uber_ride_events"
).filter(
    col("bronze_ingestion_time") > last_processed_ingestion_ts
)

# COMMAND ----------

# Remove " from booking_id and customer_id
df_clean = uber_ride_events_df\
    .withColumn(
        "booking_id", regexp_replace(col("booking_id"), '"', "")
    )\
    .withColumn(
        "customer_id", regexp_replace(col("customer_id"), '"', "")
    )

# COMMAND ----------

# FIll Null Numeric value with 0.0 and String value with NA
df_fill_Null = df_clean.fillna(0.0, subset=["ride_distance", "booking_value", "driver_ratings", "customer_rating"])\
                        .fillna("NA", subset=["payment_method", "cancelled_by", "cancellation_reason", "incomplete_rides_reason"])

# COMMAND ----------

# Drop Duplicates
df_drop_duplicates = df_fill_Null.dropDuplicates(["booking_id", "event_type", "event_time"])

# COMMAND ----------

df_agg = df_drop_duplicates.groupBy(col("booking_id")).agg(
    first(col("customer_id"), ignorenulls=True).alias("customer_id"),
    first(col("booking_status"), ignorenulls=True).alias("booking_status"),
    first(col("vehicle_type"), ignorenulls=True).alias("vehicle_type"),
    first(col("pickup_location"), ignorenulls=True).alias("pickup_location"),
    first(col("drop_location"), ignorenulls=True).alias("drop_location"),
    max(
        when(col("event_type") == "BOOKING_CREATED", col("event_time"))
    ).alias("booking_created_time"),
    max(
        when(col("event_type") == "DRIVER_ASSIGNED", col("event_time"))
    ).alias("driver_assigned_time"),
    max(
        when(col("event_type") == "RIDE_STARTED", col("event_time"))
    ).alias("ride_started_time"),
    max(
        when(col("event_type") == "RIDE_COMPLETED", col("event_time"))
    ).alias("ride_completed_time"),
    max(
        when(col("event_type") == "PAYMENT_COMPLETED", col("event_time"))
    ).alias("payment_completed_time"),
    max(
        when(col("event_type") == "RATING_SUBMITTED", col("event_time"))
    ).alias("rating_submitted_time"),
    first(col("ride_distance"), ignorenulls=True).alias("ride_distance"),
    first(col("booking_value"), ignorenulls=True).alias("booking_value"),
    first(col("payment_method"), ignorenulls=True).alias("payment_method"),
    first(col("driver_ratings"), ignorenulls=True).alias("driver_rating"),
    first(col("customer_rating"), ignorenulls=True).alias("customer_rating"),
    first(col("cancelled_by"), ignorenulls=True).alias("cancelled_by"),
    first(col("cancellation_reason"), ignorenulls=True).alias("cancellation_reason"),
    coalesce(
    max(when(col("incomplete_rides") == "1", True)),
    lit(False)
    ).alias("incomplete_ride"),
    first(col("incomplete_rides_reason"), ignorenulls=True).alias("incomplete_ride_reason")
)

# COMMAND ----------

# Add Additional columns
df_final = df_agg\
    .withColumn(
        "ride_duration_minutes", (
            (
                col("ride_completed_time").cast("long") - col("ride_started_time").cast("long")
            ) / 60
        ).cast("int")
    )\
    .withColumn("created_date", to_date(col("booking_created_time")))\
    .withColumn(
        "silver_ingestion_time", current_timestamp()
    )

# COMMAND ----------

if not df_final.isEmpty():
    df_final.write.mode("append")\
        .format("delta")\
        .saveAsTable("real_time_ride_booking_data_pipeline_azure.silver.uber_ride_bookings")

    # Get the max ingestion_timestamp
    max_ingestion_ts = uber_ride_events_df.agg(
        max("bronze_ingestion_time")
    ).collect()[0][0]

    # Update etl_watermark table for
    spark.sql(f"""
    UPDATE real_time_ride_booking_data_pipeline_azure.silver.etl_watermark
    SET source_last_ingestion_ts = TIMESTAMP('{max_ingestion_ts}'),
        last_run_ts = current_timestamp()
    WHERE source_table = 'uber_ride_events'
    """)

# COMMAND ----------

