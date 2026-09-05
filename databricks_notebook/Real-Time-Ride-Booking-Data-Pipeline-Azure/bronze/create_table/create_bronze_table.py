# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG real_time_ride_booking_data_pipeline_azure;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS real_time_ride_booking_data_pipeline_azure.bronze.uber_ride_events;
# MAGIC
# MAGIC CREATE TABLE real_time_ride_booking_data_pipeline_azure.bronze.uber_ride_events(
# MAGIC     booking_id STRING,
# MAGIC     booking_status STRING,
# MAGIC     customer_id STRING,
# MAGIC     vehicle_type STRING,
# MAGIC     pickup_location STRING,
# MAGIC     drop_location STRING,
# MAGIC
# MAGIC     event_type STRING,
# MAGIC     event_time TIMESTAMP,
# MAGIC
# MAGIC     ride_distance DOUBLE,
# MAGIC     booking_value DOUBLE,
# MAGIC
# MAGIC     payment_method STRING,
# MAGIC
# MAGIC     driver_ratings DOUBLE,
# MAGIC     customer_rating DOUBLE,
# MAGIC
# MAGIC     cancelled_by STRING,
# MAGIC     cancellation_reason STRING,
# MAGIC
# MAGIC     incomplete_rides STRING,
# MAGIC     incomplete_rides_reason STRING
# MAGIC ) 
# MAGIC USING DELTA;