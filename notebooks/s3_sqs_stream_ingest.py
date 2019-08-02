# Databricks notebook source
from pyspark.sql.types import *

demo_bus_rain_schema = StructType().add("b", IntegerType())

demo_bus_rain_df = spark.readStream \
  .format("s3-sqs") \
  .option("fileFormat", "json") \
  .option("queueUrl", "https://sqs.ap-southeast-2.amazonaws.com/421411388928/mypersonalsqs") \
  .schema(demo_bus_rain_schema) \
  .load()

# demo_bus_rain_df
