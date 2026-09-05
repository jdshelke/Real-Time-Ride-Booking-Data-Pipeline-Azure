# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create Water_mark table
# MAGIC DROP TABLE IF EXISTS real_time_ride_booking_data_pipeline_azure.silver.etl_watermark;
# MAGIC CREATE TABLE real_time_ride_booking_data_pipeline_azure.silver.etl_watermark (
# MAGIC   source_table STRING,
# MAGIC   target_table STRING,
# MAGIC   source_last_ingestion_ts TIMESTAMP,
# MAGIC   last_run_ts TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO real_time_ride_booking_data_pipeline_azure.silver.etl_watermark(
# MAGIC   source_table, target_table,  source_last_ingestion_ts
# MAGIC )
# MAGIC   VALUES
# MAGIC     ('uber_ride_events', 'uber_ride_bookings', '1900-01-01 00:00:00');

# COMMAND ----------

