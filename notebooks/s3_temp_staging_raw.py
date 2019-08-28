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
# MAGIC       path "/mnt/bucket/vehicle_position/20190815/19",
# MAGIC       multiline true
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json_input LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS json_staging;
# MAGIC CREATE TABLE json_staging
# MAGIC   USING PARQUET
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/bucket/vehicle_position_staging/20190815/19"
# MAGIC   )
# MAGIC   AS SELECT *, current_timestamp() ingested_dt FROM json_input;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json_staging LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS json_raw;

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
# MAGIC     id STRING,
# MAGIC     headers STRUCT
# MAGIC     <
# MAGIC         timestamp:STRING,
# MAGIC         host:STRING
# MAGIC     >,
# MAGIC     body STRUCT
# MAGIC     <
# MAGIC         id:STRING,
# MAGIC         vehicle:STRUCT
# MAGIC         <
# MAGIC             trip:STRUCT
# MAGIC             <
# MAGIC                 trip_id:STRING,
# MAGIC                 start_time:STRING,
# MAGIC                 start_date:STRING,
# MAGIC                 schedule_relationship:STRING,
# MAGIC                 route_id:STRING
# MAGIC             >,
# MAGIC             position:STRUCT
# MAGIC             <
# MAGIC                 latitude:DECIMAL(17,15),
# MAGIC                 longitude:DECIMAL(17,14),
# MAGIC                 bearing:DECIMAL(4,1),
# MAGIC                 speed:DECIMAL(20,17)
# MAGIC             >,
# MAGIC             timestamp:STRING,
# MAGIC             congestion_level:STRING,
# MAGIC             vehicle:STRUCT
# MAGIC             <
# MAGIC                 id:STRING
# MAGIC             >,
# MAGIC             occupancy_status:STRING
# MAGIC         >
# MAGIC     >,
# MAGIC     ingested_dt TIMESTAMP
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
# MAGIC -- Need to look at this cell
# MAGIC MERGE INTO json_raw
# MAGIC USING (SELECT * FROM 
# MAGIC           (SELECT json_staging.*, RANK() OVER (PARTITION BY id ORDER BY ingested_dt DESC) AS rnk FROM json_staging) R where R.rnk = 1) ranked_json_staging
# MAGIC ON json_raw.id = ranked_json_staging.id AND ranked_json_staging.rnk = 1
# MAGIC WHEN MATCHED AND ranked_json_staging.ingested_dt > json_raw.ingested_dt  THEN
# MAGIC   UPDATE SET headers = ranked_json_staging.headers, body = ranked_json_staging.body, ingested_dt = ranked_json_staging.ingested_dt
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

