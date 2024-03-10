# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.table
def temp_bronze_table():
  return spark.readStream.format("delta").table("example.temp_bronze")

dlt.create_streaming_table("target_SCD2")

dlt.apply_changes(
  target = "target_SCD2",
  source = "temp_bronze_table",
  keys = ["deviceId"],
  sequence_by = col("c_time"),
  apply_as_deletes = expr("operation_type = 'delete'"),
  except_column_list = ["rpm"],
  stored_as_scd_type = "2"
)
