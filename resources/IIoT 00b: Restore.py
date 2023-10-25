# Databricks notebook source
for src in ['weather_raw', 'weather_agg', 'turbine_raw_3', 'turbine_agg_3', 'turbine_enriched_3']:
  dst = src.replace('_3','')
  print(f'Resetting table {dst}...')
  spark.sql(f'ALTER SHARE vr_iiot_share REMOVE TABLE dev.{dst}')
  spark.sql(f'DROP TABLE vr_iiot.dev.{dst}')
  spark.sql(f'CREATE TABLE vr_iiot.dev.{dst} PARTITIONED BY (date) AS SELECT * FROM vr_iiot.backup.{src}')
  spark.sql(f'ALTER SHARE vr_iiot_share ADD TABLE vr_iiot.dev.{dst}')

print('Resetting checkpoint...')
dbutils.fs.rm('s3://databricks-vr/iiot/checkpoints/', True)

# COMMAND ----------

# # THIS CELL SHOULD BE RUN TO CLEAN UP GENERATED EVENTS FROM PREVIOUS DEMOS

# # Parameters
# DB = dbutils.widgets.get("Database")
# ROOT_PATH = dbutils.widgets.get("External Location")
# CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"

# spark.sql(f'USE {DB}')
# spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')
# for tbl in ['turbine_raw', 'turbine_agg', 'turbine_enriched', 'weather_raw', 'weather_agg', 'turbine_life_stream']:
#   print(f'Resetting table {tbl}...')
#   spark.sql(f'RESTORE {tbl} TO VERSION AS OF 0')
#   spark.sql(f'VACUUM {tbl} RETAIN 0 HOURS')
#   # The following lines are only required if you have data ahead of current time in your table
#   # col = 'timestamp' if tbl[-3:] == 'raw' else 'window'
#   # spark.sql(f'DELETE FROM {tbl} WHERE {col} >= current_timestamp()')
# spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'true')

# dbutils.fs.rm(CHECKPOINT_PATH, True)
