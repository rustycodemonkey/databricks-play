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

# Flattens the first level of JSON line into two columns, body and headers
%sql
DROP TABLE IF EXISTS json_input;
CREATE TEMPORARY TABLE json_input
  USING JSON
    OPTIONS (
      path "/mnt/bucket/vehicle_position/20190815/19",
      multiline true
    )

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

dbutils.fs.ls("/");

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
# MAGIC SHOW CREATE TABLE json_staging