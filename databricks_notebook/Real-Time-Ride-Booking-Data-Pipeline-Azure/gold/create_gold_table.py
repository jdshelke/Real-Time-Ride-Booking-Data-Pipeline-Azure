# Databricks notebook source
# MAGIC %md
# MAGIC ## Daily Ride Metrics Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS real_time_ride_booking_data_pipeline_azure.gold.daily_ride_metrics;
# MAGIC
# MAGIC CREATE TABLE real_time_ride_booking_data_pipeline_azure.gold.daily_ride_metrics (
# MAGIC     ride_date DATE,
# MAGIC     total_rides BIGINT,
# MAGIC     completed_rides BIGINT,
# MAGIC     cancelled_rides BIGINT,
# MAGIC     incomplete_rides BIGINT,
# MAGIC     no_driver_found_rides BIGINT,
# MAGIC     total_revenue DOUBLE,
# MAGIC     avg_fare DOUBLE,
# MAGIC     created_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vehicle Performance Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS real_time_ride_booking_data_pipeline_azure.gold.vehicle_performance;
# MAGIC
# MAGIC CREATE TABLE real_time_ride_booking_data_pipeline_azure.gold.vehicle_performance (
# MAGIC     vehicle_type STRING,
# MAGIC     total_rides BIGINT,
# MAGIC     completed_rides BIGINT,
# MAGIC     cancelled_rides BIGINT,
# MAGIC     incomplete_rides BIGINT,
# MAGIC     no_driver_found_rides BIGINT,
# MAGIC     avg_distance DOUBLE,
# MAGIC     total_revenue DOUBLE,
# MAGIC     avg_fare DOUBLE,
# MAGIC     completion_rate_percentage DOUBLE,
# MAGIC     created_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Analytics Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS real_time_ride_booking_data_pipeline_azure.gold.customer_analytics;
# MAGIC
# MAGIC CREATE TABLE real_time_ride_booking_data_pipeline_azure.gold.customer_analytics (
# MAGIC     customer_id STRING,
# MAGIC     total_bookings BIGINT,
# MAGIC     completed_bookings BIGINT,
# MAGIC     cancelled_bookings BIGINT,
# MAGIC     incomplete_bookings BIGINT,
# MAGIC     no_driver_found_bookings BIGINT,
# MAGIC     total_spend DOUBLE,
# MAGIC     avg_booking_value DOUBLE,
# MAGIC     avg_customer_rating DOUBLE,
# MAGIC     avg_driver_rating DOUBLE,
# MAGIC     created_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Booking Status Metrics Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS real_time_ride_booking_data_pipeline_azure.gold.booking_status_metrics;
# MAGIC
# MAGIC CREATE TABLE real_time_ride_booking_data_pipeline_azure.gold.booking_status_metrics (
# MAGIC     total_rides BIGINT,
# MAGIC     completed_count BIGINT,
# MAGIC     cancelled_count BIGINT,
# MAGIC     incomplete_count BIGINT,
# MAGIC     no_driver_found_count BIGINT,
# MAGIC     completed_percentage DOUBLE,
# MAGIC     cancelled_percentage DOUBLE,
# MAGIC     incomplete_percentage DOUBLE,
# MAGIC     no_driver_found_percentage DOUBLE,
# MAGIC     total_revenue DOUBLE,
# MAGIC     created_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;