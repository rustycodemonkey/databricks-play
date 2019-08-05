# Databricks notebook source
dbutils.fs.ls("s3a://mypersonaldumpingground/")

# COMMAND ----------

# dbutils.fs.mount("s3a://mypersonaldumpingground", "/mnt/bucket")

# COMMAND ----------

dbutils.fs.ls("/mnt/bucket/vehicle_position/20190805/16")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col
import time

demo_bus_rain_schema = StructType() \
	.add("headers", StructType() \
		.add("timestamp", StringType()) \
		.add("host", StringType())) \
	.add("body", StructType() \
		.add("id", StringType()) \
		.add("vehicle", StructType() \
			.add("trip", StructType() \
				.add("trip_id", StringType()) \
				.add("start_time", StringType()) \
				.add("start_date", StringType()) \
				.add("schedule_relationship", StringType()) \
				.add("route_id", StringType())) \
			.add("position", StructType() \
				.add("latitude", StringType()) \
				.add("longitude", StringType()) \
				.add("bearing", StringType()) \
				.add("speed", StringType())) \
			.add("timestamp", StringType()) \
			.add("congestion_level", StringType()) \
			.add("vehicle", StructType() \
				.add("id", StringType())) \
			.add("occupancy_status", StringType())))

demo_bus_rain_df = spark.readStream \
  .format("s3-sqs") \
  .option("fileFormat", "json") \
  .option("queueUrl", "https://sqs.ap-southeast-2.amazonaws.com/421411388928/mypersonalsqs") \
  .schema(demo_bus_rain_schema) \
  .load()

query_df = demo_bus_rain_df.select( \
	"headers.timestamp", \
	"headers.host", \
	"body.id", \
	col("body.vehicle.vehicle.id").alias("veh_id"), \
	"body.vehicle.trip.trip_id", \
	"body.vehicle.trip.start_time", \
	"body.vehicle.trip.start_date", \
	"body.vehicle.trip.schedule_relationship", \
	"body.vehicle.trip.route_id", \
	"body.vehicle.position.latitude", \
	"body.vehicle.position.longitude", \
	"body.vehicle.position.bearing", \
	"body.vehicle.position.speed", \
	col("body.vehicle.timestamp").alias("veh_timestamp"), \
	"body.vehicle.congestion_level", \
	"body.vehicle.occupancy_status")

# COMMAND ----------

query_df.printSchema()

# COMMAND ----------

#display(query_df)

query = query_df.writeStream.format("console").start()

# COMMAND ----------

