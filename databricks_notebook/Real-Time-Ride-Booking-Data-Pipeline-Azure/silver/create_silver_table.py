# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS real_time_ride_booking_data_pipeline_azure.silver.uber_ride_bookings;
# MAGIC
# MAGIC CREATE TABLE real_time_ride_booking_data_pipeline_azure.silver.uber_ride_bookings(
# MAGIC     booking_id string,
# MAGIC     customer_id string,
# MAGIC     booking_status string,
# MAGIC     vehicle_type string,
# MAGIC     pickup_location string,
# MAGIC     drop_location string,
# MAGIC     booking_created_time timestamp,
# MAGIC     driver_assigned_time timestamp,
# MAGIC     ride_started_time timestamp,
# MAGIC     ride_completed_time timestamp,
# MAGIC     payment_completed_time timestamp,
# MAGIC     rating_submitted_time timestamp,
# MAGIC     ride_distance double,
# MAGIC     booking_value double,
# MAGIC     payment_method string,
# MAGIC     driver_rating double,
# MAGIC     customer_rating double,
# MAGIC     cancelled_by string,
# MAGIC     cancellation_reason string,
# MAGIC     incomplete_ride boolean,
# MAGIC     incomplete_ride_reason string,
# MAGIC     ride_duration_minutes integer,
# MAGIC     created_date date,
# MAGIC     silver_ingestion_time timestamp
# MAGIC )USING DELTA
# MAGIC PARTITIONED BY (created_date)

# COMMAND ----------

