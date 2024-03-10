# Databricks notebook source
# MAGIC %md
# MAGIC Enable CDF on cluster level spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

# MAGIC %md
# MAGIC If the table already exists ->
# MAGIC ALTER TABLE example.silverturbinet SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY example.bronzeturbinet

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED example.bronzeturbinet

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO example.bronzeturbinet VALUES (15,17,15,162,'2024-02-01T11:27:23.184+00:00')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE example.bronzeturbinet
# MAGIC SET angle=50
# MAGIC WHERE deviceId=3

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM example.bronzeturbinet
# MAGIC WHERE deviceId=2 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- version as ints or longs e.g. changes from version 0 to 10
# MAGIC --SELECT * FROM table_changes('example.bronzeturbinet', 0, 3)
# MAGIC
# MAGIC -- timestamp as string formatted timestamps
# MAGIC --SELECT * FROM table_changes('example.bronzeturbinet', '2024-02-25T07:48:45Z', '2024-02-25T07:48:45Z')
# MAGIC
# MAGIC -- -- providing only the startingVersion/timestamp
# MAGIC SELECT * FROM table_changes('example.bronzeturbinet', 0)

# COMMAND ----------

from pyspark.sql import functions as F
silver_stream_df = (spark.readStream
                         .format("delta")
                         .option("readChangeData", True)
                         .option("startingVersion",0)
                         .table("example.bronzeturbinet"))

silver_stream_df.filter(F.col("_change_type").isin(["update_preimage"])).selectExpr("*").display()

# COMMAND ----------

from pyspark.sql import functions as F
silver_stream_df = (spark.readStream
                         .format("delta")
                         .option("readChangeData", True)
                         .option("startingVersion", 2)
                         .table("example.bronzeturbinet"))

silver_stream_df.filter(F.col("_change_type").isin(["update_postimage", "insert"])).selectExpr("*").display()

# COMMAND ----------

from pyspark.sql import functions as F
silver_stream_df = (spark.readStream
                         .format("delta")
                         .option("readChangeData", True)
                         .option("startingVersion",1)
                         .table("example.bronzeturbinet"))

silver_stream_df.filter(F.col("_change_type").isin(["delete"])).selectExpr("*").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE
# MAGIC   example.temp_bronze
# MAGIC AS SELECT 
# MAGIC           deviceId,
# MAGIC           rpm,
# MAGIC           angle,
# MAGIC           timestamp,
# MAGIC           _change_type as operation_type,
# MAGIC           _commit_version as c_version,
# MAGIC           _commit_timestamp as c_time
# MAGIC FROM table_changes("example.bronzeturbinet", 2)
