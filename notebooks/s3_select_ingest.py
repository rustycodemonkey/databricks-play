# Databricks notebook source
dbutils.fs.ls("s3a://mypersonaldumpingground/")

# COMMAND ----------

#dbutils.fs.mount("s3a://mypersonaldumpingground", "/mnt/bucket")
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/bucket/vehicle_position/20190814/16")

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip list --outdated

# COMMAND ----------

dbutils.library.installPyPI

# COMMAND ----------

import pkg_resources
print(pkg_resources.get_distribution('boto3').version)
print(pkg_resources.get_distribution('botocore').version)

# COMMAND ----------

dbutils.library.list()

# COMMAND ----------

#dbutils.library.installPyPI('boto3', version="1.9.207")
#dbutils.library.installPyPI('botocore', version="1.12.207")
#dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip list --outdated

# COMMAND ----------

dbutils.library.list()

# COMMAND ----------

import pkg_resources
print(pkg_resources.get_distribution('boto3').version)
print(pkg_resources.get_distribution('botocore').version)

# COMMAND ----------

import boto3
client = boto3.client('s3', 'ap-southeast-2')

response = client.select_object_content(
  Bucket='mypersonaldumpingground',
  Key='vehicle_position/20190814/16/vehpos_p0.1565765202444.json',
  Expression="""select
  s.body.id,
  s.body.vehicle.vehicle.id as veh_id,
  s.body.vehicle.trip.trip_id,
  s.body.vehicle.trip.start_time,
  s.body.vehicle.trip.start_date,
  s.body.vehicle.trip.schedule_relationship,
  s.body.vehicle.trip.route_id,
  s.body.vehicle."position".latitude,
  s.body.vehicle."position".longitude,
  s.body.vehicle."position".bearing,
  s.body.vehicle."position".speed,
  s.body.vehicle."timestamp" as unix_timestamp,
  s.body.vehicle.congestion_level,
  s.body.vehicle.occupancy_status
  from s3object s""",
  ExpressionType='SQL',
  InputSerialization={'JSON': {'Type': 'LINES'},},
  #OutputSerialization={'CSV': {},},
  OutputSerialization={'JSON': {'RecordDelimiter': '\n'},},
)

for event in response['Payload']:
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8')
        print(records)
    elif 'Stats' in event:
        statsDetails = event['Stats']['Details']
        print("Stats details bytesScanned: ")
        print(statsDetails['BytesScanned'])
        print("Stats details bytesProcessed: ")
        print(statsDetails['BytesProcessed'])
        print("Stats details bytesReturned: ")
        print(statsDetails['BytesReturned'])

# COMMAND ----------

