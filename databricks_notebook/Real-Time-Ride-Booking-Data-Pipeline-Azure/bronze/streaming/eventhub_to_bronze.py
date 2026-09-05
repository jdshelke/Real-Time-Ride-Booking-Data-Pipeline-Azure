# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Event Hub connection string
event_hub_connection_string = dbutils.secrets.get(
    scope="azure-keyvault-scope",
    key="eventhub-connection-string"
)

# COMMAND ----------

if "EntityPath" in event_hub_connection_string:
    print("Correct connection string")
else:
    print("Wrong connection string")

# COMMAND ----------

# Encrypt connection string
ehConf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string)
}

# COMMAND ----------

# Read stream from Event Hub
raw_df = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

# COMMAND ----------

# Convert binary body to string
json_df = raw_df.selectExpr(
    "CAST(body AS STRING) as json_data",
    "enqueuedTime as eventhub_time"
)

# COMMAND ----------

# Define Schema
schema = StructType([
    StructField("Booking ID", StringType()),
    StructField("Booking Status", StringType()),
    StructField("Customer ID", StringType()),
    StructField("Vehicle Type", StringType()),
    StructField("Pickup Location", StringType()),
    StructField("Drop Location", StringType()),

    StructField("Event Type", StringType()),
    StructField("Event Time", TimestampType()),

    StructField("Ride Distance", DoubleType()),
    StructField("Booking Value", DoubleType()),

    StructField("Payment Method", StringType()),

    StructField("Driver Ratings", DoubleType()),
    StructField("Customer Rating", DoubleType()),

    StructField("Cancelled by", StringType()),
    StructField("Cancellation Reason", StringType()),
    
    StructField("Incomplete Rides", StringType()),
    StructField("Incomplete Rides Reason", StringType())
])

# COMMAND ----------

structured_df = json_df.select(
    from_json(col("json_data"), schema).alias("data")
)
# from_json() returns StructType column not direct flat columns.

# COMMAND ----------

final_df = structured_df.select("data.*")
# data.* expands all fields properly

# COMMAND ----------



# COMMAND ----------

final_df = final_df.toDF(
    "booking_id",
    "booking_status",
    "customer_id",
    "vehicle_type",
    "pickup_location",
    "drop_location",
    "event_type",
    "event_time",
    "ride_distance",
    "booking_value",
    "payment_method",
    "driver_ratings",
    "customer_rating",
    "cancelled_by",
    "cancellation_reason",
    "incomplete_rides",
    "incomplete_rides_reason"
)

# COMMAND ----------

# Add ingestion timestamp
final_df = final_df.withColumn(
    "bronze_ingestion_time",
    current_timestamp()
)

# COMMAND ----------

# Checkpoint path
checkpoint_path = "abfss://real-time-ride-booking-data-pipeline-azure@jdazstorageac.dfs.core.windows.net/bronze/checkpoints/eventhub_to_bronze"

# COMMAND ----------

# Write stream to ADLS Bronze
query = (
    final_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .toTable("real_time_ride_booking_data_pipeline_azure.bronze.uber_ride_events")
)
query.awaitTermination()