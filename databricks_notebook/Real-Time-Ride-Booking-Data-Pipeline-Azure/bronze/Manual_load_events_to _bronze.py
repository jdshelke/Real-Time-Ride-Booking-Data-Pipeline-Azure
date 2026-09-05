# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Read events from event file
raw_df = (
    spark.read.format("csv")\
        .option("inferSchema", "true")\
        .option("header", "true")
        .load("abfss://real-time-ride-booking-data-pipeline-azure@jdazstorageac.dfs.core.windows.net/bronze/manual_load_data/event_data.csv")
)