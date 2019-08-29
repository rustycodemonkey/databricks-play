# Databricks notebook source
# MAGIC %sh
# MAGIC /databricks/python/bin/pip list --outdated

# COMMAND ----------

#dbutils.fs.mount("s3a://mypersonaldumpingground", "/mnt/bucket")
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("s3a://mypersonaldumpingground/")

# COMMAND ----------

dbutils.fs.ls("/mnt/bucket/vehicle_position/20190815/19")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Flattens the first level of JSON line into two columns, body and headers
# MAGIC DROP TABLE IF EXISTS json_input;
# MAGIC CREATE TEMPORARY TABLE json_input
# MAGIC   USING JSON
# MAGIC     OPTIONS (
# MAGIC       path "/mnt/bucket/vehicle_position/20190815/19"
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json_input LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM json_input

# COMMAND ----------

# MAGIC %sql
# MAGIC -- vehpos_p2.1565860721733.json
# MAGIC -- 40287_17275143_2454_691_1
# MAGIC -- {"headers": {"timestamp": "1565860712", "host": "ip-192-168-10-100.ap-southeast-2.compute.internal"}, "body": {"id": "40287_17275143_2454_691_1", "vehicle": {"trip": {"trip_id": "984502", "start_time": "19:15:00", "start_date": "20190815", "schedule_relationship": "SCHEDULED", "route_id": "2454_691"}, "position": {"latitude": -33.743263244628906, "longitude": 150.6097869873047, "bearing": 127.0, "speed": 0.0}, "timestamp": "1565860712", "congestion_level": "UNKNOWN_CONGESTION_LEVEL", "vehicle": {"id": "40287_17275143_2454_691_1"}, "occupancy_status": "MANY_SEATS_AVAILABLE"}}}
# MAGIC SELECT * FROM json_input WHERE body.id = '40287_17275143_2454_691_1' ORDER BY body.vehicle.timestamp DESC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT body.id AS rec_id, from_unixtime(body.vehicle.timestamp, "y-MM-dd'T'hh:mm:ss.SSSZZZZ") AS tstamp, current_timestamp() AS ingested_dt FROM json_input LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS json_staging;
# MAGIC CREATE TABLE json_staging
# MAGIC   USING PARQUET
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/bucket/vehicle_position_staging/20190815/19"
# MAGIC   )
# MAGIC   AS SELECT body.id AS rec_id, from_unixtime(body.vehicle.timestamp, "y-MM-dd'T'hh:mm:ss.SSSZZZZ") AS tstamp, current_timestamp() AS ingested_dt, headers, body FROM json_input;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json_staging LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM json_staging

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS json_raw

# COMMAND ----------

dbutils.fs.ls("/delta/json_raw")

# COMMAND ----------

#display(dbutils.fs)
dbutils.fs.rm("/delta/json_raw", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS json_raw;
# MAGIC CREATE TABLE json_raw
# MAGIC (
# MAGIC     rec_id STRING,
# MAGIC     tstamp TIMESTAMP,
# MAGIC     ingested_dt TIMESTAMP,
# MAGIC     headers STRUCT
# MAGIC     <
# MAGIC         host:STRING,
# MAGIC         timestamp:STRING
# MAGIC     >,
# MAGIC     body STRUCT
# MAGIC     <
# MAGIC         id:STRING,
# MAGIC         vehicle:STRUCT
# MAGIC         <
# MAGIC             congestion_level:STRING,
# MAGIC             occupancy_status:STRING,
# MAGIC             position:STRUCT
# MAGIC             <
# MAGIC                 bearing:DECIMAL(4,1),
# MAGIC                 latitude:DECIMAL(17,15),
# MAGIC                 longitude:DECIMAL(17,14),
# MAGIC                 speed:DECIMAL(20,17)
# MAGIC             >,
# MAGIC             timestamp:STRING,
# MAGIC             trip:STRUCT
# MAGIC             <
# MAGIC                 trip_id:STRING,
# MAGIC                 start_time:STRING,
# MAGIC                 start_date:STRING,
# MAGIC                 schedule_relationship:STRING,
# MAGIC                 route_id:STRING
# MAGIC             >,
# MAGIC             vehicle:STRUCT
# MAGIC             <
# MAGIC                 id:STRING
# MAGIC             >
# MAGIC         >
# MAGIC     >
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/delta/json_raw"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC json_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE json_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (SELECT json_staging.*, RANK() OVER (PARTITION BY rec_id ORDER BY tstamp DESC) AS rnk FROM json_staging) R WHERE R.rnk = 1 LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Need to look at this cell
# MAGIC MERGE INTO json_raw
# MAGIC USING (SELECT * FROM (SELECT json_staging.*, RANK() OVER (PARTITION BY rec_id ORDER BY tstamp DESC) AS rnk FROM json_staging) R WHERE R.rnk = 1) ranked_json_staging
# MAGIC ON json_raw.rec_id = ranked_json_staging.rec_id AND ranked_json_staging.rnk = 1
# MAGIC WHEN MATCHED AND ranked_json_staging.tstamp > json_raw.tstamp  THEN
# MAGIC   UPDATE SET tstamp = ranked_json_staging.tstamp, ingested_dt = ranked_json_staging.ingested_dt, headers = ranked_json_staging.headers, body = ranked_json_staging.body
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json_raw LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM json_raw

# COMMAND ----------

