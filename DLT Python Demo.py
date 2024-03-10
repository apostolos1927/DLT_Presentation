# Databricks notebook source
import dlt
import pyspark.sql.types as T
import pyspark.sql.functions as F
# IOT Hubs configuration        
EH_NAMESPACE                    = ""
EH_NAME                         = ""

EH_CONN_SHARED_ACCESS_KEY_NAME  = ""
EH_CONN_SHARED_ACCESS_KEY_VALUE = ""

EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"
# Kafka Consumer configuration

EH_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
}

# PAYLOAD SCHEMA
schema = """messageId string,deviceId int, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"""



# COMMAND ----------

@dlt.create_table(comment="BronzeTurbine")
def BronzeTurbineT():
    return (
        spark.readStream.format("kafka")
        .options(**EH_OPTIONS)                                                         
        .load()                                                                         
        .withColumn('body', F.from_json(F.col('value').cast('string'), schema))
        .withColumn('timestamp', F.current_timestamp())
        .select(
            F.col("body.messageId").alias("messageID"),
            F.col("body.deviceId").alias("deviceId"),
            F.col("body.rpm").alias("rpm"),
            F.col("body.angle").alias("angle"),
            F.col("timestamp").alias("timestamp")
        )
    )

# COMMAND ----------

@dlt.create_table(comment="BronzeWeather")
def BronzeWeatherT():
    return (
        spark.readStream.format("kafka")
        .options(**EH_OPTIONS)                                                            
        .load()                                                                          
        .withColumn('body', F.from_json(F.col('value').cast('string'), schema))
        .withColumn('timestamp', F.current_timestamp())
        .select(
            F.col("body.messageId").alias("messageID"),
            F.col("body.deviceId").alias("deviceId"),
            F.col("body.temperature").alias("temperature"),
            F.col("body.humidity").alias("humidity"),
            F.col("body.windspeed").alias("windspeed"),
            F.col("body.winddirection").alias("winddirection"),
            F.col("timestamp").alias("timestamp")
        )
    )

# COMMAND ----------

@dlt.table(comment="SilverTurbine")
@dlt.expect_or_drop("angleConstraint", "AverageAngle IS NOT NULL")
@dlt.expect_or_fail("rpmConstraint", "AverageRpm > 0")
def SilverTurbineT():
    return (
        dlt.readStream("BronzeTurbineT")
        .withColumn("messageID", F.expr("CAST(messageID AS INT)"))
        .withColumnRenamed("angle", "deviceAngle")
        .withColumnRenamed("rpm", "deviceRPM")
        .withColumnRenamed("deviceId", "deviceID")
        .groupBy('deviceID',F.window('timestamp','5 seconds'))            
        .agg(F.avg('deviceRPM').alias('AverageRpm'), F.avg("deviceAngle").alias("AverageAngle"))
        .select(
            "deviceID",
            "AverageRpm",
            "AverageAngle"
        )
    )

# COMMAND ----------

@dlt.table(comment="SilverWeather")
@dlt.expect_or_drop("temperatureConstraint", "AverageTemperature IS NOT NULL")
@dlt.expect_or_fail("humidityConstraint", "AverageHumidity > 0")
def SilverWeatherT():
    return (
        dlt.readStream("BronzeWeatherT")
        .filter(F.expr("winddirection = 'Left'"))
        .groupBy('deviceID','winddirection',F.window('timestamp','5 seconds'))              
        .agg(F.avg('temperature').alias('AverageTemperature'), F.avg("humidity").alias("AverageHumidity"),F.avg("windspeed").alias("AverageWindspeed"))
        .select(
            "deviceID",
            "AverageTemperature",
            "AverageHumidity",
            "AverageWindspeed",
            "winddirection"
        )
    )

# COMMAND ----------

@dlt.table(comment="Gold layer")
def goldT():
    silver_one = dlt.read("SilverTurbineT")
    silver_two = dlt.read("SilverWeatherT")
    return ( 
     silver_one.join(silver_two, ["deviceID"], how="inner")
     .filter(F.expr("AverageTemperature <> 5"))
    )
