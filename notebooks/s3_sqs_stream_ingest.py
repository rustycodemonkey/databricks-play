# Databricks notebook source
dbutils.fs.ls("s3a://mypersonaldumpingground/")

# COMMAND ----------

dbutils.fs.mount("s3a://mypersonaldumpingground", "/mnt/bucket")

# COMMAND ----------

dbutils.fs.ls("/mnt/bucket/vehicle_position/20190802/15")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

from pyspark.sql.types import *

demo_bus_rain_schema = StructType().add("a", StringType())

demo_bus_rain_df = spark.readStream \
  .format("s3-sqs") \
  .option("fileFormat", "json") \
  .option("queueUrl", "https://sqs.ap-southeast-2.amazonaws.com/421411388928/mypersonalsqs") \
  .schema(demo_bus_rain_schema) \
  .load()

noAggDF = demo_bus_rain_df.select("a")

# COMMAND ----------

noAggDF.writeStream.format("console").start()
