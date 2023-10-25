# Databricks notebook source
# MAGIC %md # Simulate Historical Data
# MAGIC In order to train a model, we will need to backfill our streaming data with historical data. The cell below generates 1 year of historical hourly turbine and weather data and inserts it into our Gold Delta table.

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS vr_iiot.backup;
# MAGIC USE vr_iiot.backup;

# COMMAND ----------

# MAGIC %md ## 1. Data Engineering

# COMMAND ----------

# MAGIC %md ### reset

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS turbine_raw;
# MAGIC -- DROP TABLE IF EXISTS weather_raw;
# MAGIC -- DROP TABLE IF EXISTS turbine_agg;
# MAGIC -- DROP TABLE IF EXISTS weather_agg;
# MAGIC -- DROP TABLE IF EXISTS turbine_enriched;

# COMMAND ----------

# MAGIC %md ### turbine_raw, wheather raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vr_iiot.backup.dates AS SELECT explode(sequence(to_timestamp('2022-01-01'), to_timestamp('2022-12-31'), INTERVAL 15 seconds)) AS timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vr_iiot.backup.devices AS SELECT concat('WindTurbine-', CAST(idx AS STRING)) AS deviceId
# MAGIC FROM (
# MAGIC   SELECT explode(sequence(1,500)) AS idx
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vr_iiot.backup.turbine_raw
# MAGIC PARTITIONED BY (date)
# MAGIC TBLPROPERTIES(delta.targetFileSize = 134217728) AS
# MAGIC SELECT
# MAGIC   to_date(dt.timestamp) as date,
# MAGIC   dt.timestamp,
# MAGIC   dv.deviceId,
# MAGIC   7 * (1 + 0.6 * (-1 + 2 * random())) as rpm,
# MAGIC   6 * (1 + 0.6 * (-1 + 2 * random())) as angle
# MAGIC FROM vr_iiot.backup.dates dt
# MAGIC JOIN vr_iiot.backup.devices dv

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vr_iiot.backup.weather_raw
# MAGIC PARTITIONED BY (date)
# MAGIC TBLPROPERTIES(delta.targetFileSize = 134217728) AS
# MAGIC SELECT
# MAGIC   to_date(dt.timestamp) as date,
# MAGIC   dt.timestamp,
# MAGIC   'WeatherCapture' as deviceId,
# MAGIC   27 * (1 + 0.6 * (-1 + 2 * random())) as temperature,
# MAGIC   64 * (1 + 0.6 * (-1 + 2 * random())) as humidity,
# MAGIC   6 * (1 + 0.6 * (-1 + 2 * random())) as windspeed,
# MAGIC   ARRAY('N','NW','W','SW','S','SE','E','NE')[cast(8*random() as int)] as winddirection
# MAGIC FROM vr_iiot.backup.dates dt

# COMMAND ----------

# MAGIC %md ### turbine_agg, wheather_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vr_iiot.backup.turbine_agg
# MAGIC PARTITIONED BY (date)
# MAGIC TBLPROPERTIES(delta.targetFileSize = 134217728) AS
# MAGIC SELECT
# MAGIC   date,
# MAGIC   timestamp as window, -- TBD: USE PROPER WINDOW FUNCTION
# MAGIC   deviceId,
# MAGIC   avg(rpm) as rpm,
# MAGIC   avg(angle) as angle
# MAGIC FROM vr_iiot.backup.turbine_raw
# MAGIC GROUP BY date, window, deviceId

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vr_iiot.backup.weather_agg
# MAGIC PARTITIONED BY (date)
# MAGIC TBLPROPERTIES(delta.targetFileSize = 134217728) AS
# MAGIC SELECT
# MAGIC   date,
# MAGIC   timestamp as window, -- TBD: USE PROPER WINDOW FUNCTION
# MAGIC   deviceId,
# MAGIC   avg(temperature) as temperature,
# MAGIC   avg(humidity) as humidity,
# MAGIC   avg(windspeed) as windspeed,
# MAGIC   last(winddirection) as winddirection
# MAGIC FROM vr_iiot.backup.weather_raw
# MAGIC GROUP BY date, window, deviceId

# COMMAND ----------

# MAGIC %md ### turbine_enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE vr_iiot.backup.turbine_enriched_2
# MAGIC PARTITIONED BY (date)
# MAGIC TBLPROPERTIES(delta.targetFileSize = 134217728) AS
# MAGIC SELECT
# MAGIC   t.date,
# MAGIC   t.window,
# MAGIC   t.deviceId,
# MAGIC   t.rpm,
# MAGIC   t.angle,
# MAGIC   w.temperature,
# MAGIC   w.humidity,
# MAGIC   w.windspeed,
# MAGIC   w.winddirection
# MAGIC FROM vr_iiot.backup.turbine_agg t
# MAGIC LEFT JOIN vr_iiot.backup.weather_agg w ON
# MAGIC   t.date = w.date AND
# MAGIC   t.window = w.window

# COMMAND ----------

# MAGIC %md ### optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE turbine_raw ZORDER BY (deviceId);
# MAGIC OPTIMIZE turbine_agg ZORDER BY (deviceId);
# MAGIC OPTIMIZE turbine_enriched ZORDER BY (deviceId);
# MAGIC OPTIMIZE weather_raw ZORDER BY (deviceId);
# MAGIC OPTIMIZE weather_agg ZORDER BY (deviceId);

# COMMAND ----------

# MAGIC %md ### vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM turbine_raw RETAIN 0 HOURS;
# MAGIC VACUUM turbine_agg RETAIN 0 HOURS;
# MAGIC VACUUM turbine_enriched RETAIN 0 HOURS;
# MAGIC VACUUM weather_raw RETAIN 0 HOURS;
# MAGIC VACUUM weather_agg RETAIN 0 HOURS;
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = true;

# COMMAND ----------

# MAGIC %md ### clone

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS vr_iiot.dev.turbine_raw;
# MAGIC DROP TABLE IF EXISTS vr_iiot.dev.turbine_agg;
# MAGIC DROP TABLE IF EXISTS vr_iiot.dev.turbine_enriched;
# MAGIC DROP TABLE IF EXISTS vr_iiot.dev.weather_raw;
# MAGIC DROP TABLE IF EXISTS vr_iiot.dev.weather_agg;
# MAGIC
# MAGIC CREATE TABLE vr_iiot.dev.turbine_raw CLONE vr_iiot.backup.turbine_raw;
# MAGIC CREATE TABLE vr_iiot.dev.turbine_agg CLONE vr_iiot.backup.turbine_agg;
# MAGIC CREATE TABLE vr_iiot.dev.turbine_enriched CLONE vr_iiot.backup.turbine_enriched;
# MAGIC CREATE TABLE vr_iiot.dev.weather_raw CLONE vr_iiot.backup.weather_raw;
# MAGIC CREATE TABLE vr_iiot.dev.weather_agg CLONE vr_iiot.backup.weather_agg;

# COMMAND ----------

# MAGIC %md ## 2. Data Science

# COMMAND ----------

# MAGIC %md ### power, maintenance

# COMMAND ----------

baselines = {
  'rpm' : 7,
  'angle' : 6,
  'temperature' : 27,
  'humidity' : 64,
  'windspeed' : 6
}

# COMMAND ----------

import pandas as pd
import numpy as np

# Function to simulate generating time-series data given a baseline, slope, and some seasonality
def generate_series(time_index, baseline, slope=0.01, period=365*24*60*4):
  rnd = np.random.RandomState(time_index)
  season_time = (time_index % period) / period
  seasonal_pattern = np.where(season_time < 0.4, np.cos(season_time * 2 * np.pi), 1 / np.exp(3 * season_time))
  return baseline * (1 + 0.1 * seasonal_pattern + 0.1 * rnd.randn(len(time_index)))
  
# Get start and end dates for our historical data
dates = spark.sql('select current_date()-interval 365 days as start, current_date() as end').toPandas()
  
# Get the baseline readings for each sensor for backfilling data
# turbine_enriched_pd = spark.table('turbine_enriched').toPandas()
# baselines = turbine_enriched_pd.min()[3:8]
# devices = turbine_enriched_pd['deviceid'].unique()

# Iterate through each device to generate historical data for that device
print("---Generating Historical Enriched Turbine Readings---")
for idx in range(1,501):
  deviceid = f'WindTurbine-{idx}'
  print(f'Backfilling device {deviceid}')
  windows = pd.date_range(start=dates['start'][0], end=dates['end'][0], freq='15S') # Generate a list of timestamps from start to end date (each 5 seconds)
  historical_values = pd.DataFrame({
    'date': windows.date,
    'window': windows, 
    'winddirection': np.random.choice(['N','NW','W','SW','S','SE','E','NE'], size=len(windows)),
    'deviceId': deviceid
  })
  time_index = historical_values.index.to_numpy()                                 # Generate a time index

  for sensor in baselines:
    historical_values[sensor] = generate_series(time_index, baselines[sensor])    # Generate time-series data from this sensor

  # Write dataframe to enriched_readings Delta table
  spark.createDataFrame(historical_values).write.format("delta").mode("append").partitionBy("date").saveAsTable("turbine_enriched")

# COMMAND ----------

# Create power readings based on weather and operating conditions
print("---Generating Historical Turbine Power Readings---")
spark.sql(f'CREATE TABLE turbine_power USING DELTA PARTITIONED BY (date) LOCATION "{GOLD_PATH + "turbine_power"}" AS SELECT date, window, deviceId, 0.1 * (temperature/humidity) * (3.1416 * 25) * windspeed * rpm AS power FROM turbine_enriched')

# Create a maintenance records based on peak power usage
print("---Generating Historical Turbine Maintenance Records---")
spark.sql(f'CREATE TABLE turbine_maintenance USING DELTA LOCATION "{GOLD_PATH + "turbine_maintenance"}" AS SELECT DISTINCT deviceid, FIRST(date) OVER (PARTITION BY deviceid, year(date), month(date) ORDER BY power) AS date, True AS maintenance FROM turbine_power')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize all 3 tables for querying and model training performance
# MAGIC OPTIMIZE turbine_enriched ZORDER BY deviceid, window;
# MAGIC OPTIMIZE turbine_power ZORDER BY deviceid, window;
# MAGIC OPTIMIZE turbine_maintenance ZORDER BY deviceid;

# COMMAND ----------

# MAGIC %md ### gold_readings
# MAGIC
# MAGIC Our Delta Gold tables are now ready for predictive analytics! We now have hourly weather, turbine operating and power measurements, and daily maintenance logs going back one year. We can see that there is significant correlation between most of the variables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query all 3 tables
# MAGIC CREATE OR REPLACE VIEW gold_readings AS
# MAGIC SELECT r.*, 
# MAGIC   p.power, 
# MAGIC   ifnull(m.maintenance,False) as maintenance
# MAGIC FROM turbine_enriched r 
# MAGIC   JOIN turbine_power p ON (r.date=p.date AND r.window=p.window AND r.deviceid=p.deviceid)
# MAGIC   LEFT JOIN turbine_maintenance m ON (r.date=m.date AND r.deviceid=m.deviceid);
# MAGIC   
# MAGIC SELECT * FROM gold_readings ORDER BY deviceid, window

# COMMAND ----------

# MAGIC %md ## 3. Other Sources

# COMMAND ----------

# MAGIC %md ### location

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE vr_iiot.dev.devices AS 
# MAGIC SELECT
# MAGIC   'WindTurbine-' || idx as deviceId,
# MAGIC   ARRAY('Databreeze','BrixFlux','Wind House')[cast(3*random() as int)] as model_name,
# MAGIC   case when idx <= 250
# MAGIC     then 'Parazinho'
# MAGIC     else 'Joao Camara'
# MAGIC     end as field_name,
# MAGIC   case when idx <= 250
# MAGIC     then -5.281300 + 0.002000 * ((idx - 1) div 10)
# MAGIC     else -5.544559 + 0.002000 * ((idx - 1) div 10)
# MAGIC     end as lat,
# MAGIC   case when idx <= 250
# MAGIC     then -35.919200 + 0.004000 * ((idx - 1) % 10)
# MAGIC     else -35.8658049 + 0.004000 * ((idx - 1) % 10) 
# MAGIC     end as lon
# MAGIC FROM (
# MAGIC   SELECT explode(sequence(1,500)) AS idx
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC with w as (
# MAGIC   select max(`window`) as max_window from vr_iiot.dev.turbine_enriched
# MAGIC )
# MAGIC select
# MAGIC   d.deviceId,
# MAGIC   d.model_name,
# MAGIC   d.field_name,
# MAGIC   d.lat,
# MAGIC   d.lon,
# MAGIC   avg(t.rpm) as rpm,
# MAGIC   avg(t.temperature) as temperature,
# MAGIC   avg(t.remaining_life) as remaining_life
# MAGIC from vr_iiot.dev.devices d
# MAGIC left join vr_iiot.dev.turbine_enriched t
# MAGIC inner join w on
# MAGIC   t.deviceid = d.deviceId
# MAGIC   and t.`window` > w.max_window - INTERVAL 5 minute
# MAGIC group by d.deviceid, d.model_name, d.field_name, d.lat, d.lon

# COMMAND ----------

# MAGIC %md ### ERP (parts)

# COMMAND ----------

# MAGIC %sql create database if not exists vr_iiot.erp;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table vr_iiot.erp.inventory as
# MAGIC SELECT
# MAGIC   ARRAY('Parazinho','Joao Camara')[cast(2*random() as int)] as field_name,
# MAGIC   ARRAY('Gear Box','Brake','Controller','Anemometer','Yaw Drive','Power Cable','Low Speed Shaft','High Speed Shaft','Main Shaft Bearing','Inverter')[cast(10*random() as int)] as part_name,
# MAGIC   ARRAY('Databreeze','BrixFlux','Wind House')[cast(3*random() as int)] as model_name,
# MAGIC   cast(5*random() as int) as quantity
# MAGIC FROM (
# MAGIC   SELECT explode(sequence(1,60)) AS idx
# MAGIC )

# COMMAND ----------

# MAGIC %md ### MES (shifts / capacitated personnel)

# COMMAND ----------

# MAGIC %sql create database if not exists vr_iiot.mes;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table vr_iiot.mes.maintenance_schedule as
# MAGIC SELECT
# MAGIC   field_name,
# MAGIC   model_name,
# MAGIC   date_add(to_date('2023-07-01'), cast(180*random() as int)) as date
# MAGIC FROM (
# MAGIC   select field_name, explode(model_name) as model_name from (
# MAGIC     select explode(ARRAY('Parazinho','Joao Camara')) as field_name, ARRAY('Databreeze','BrixFlux','Wind House') as model_name
# MAGIC   )
# MAGIC )
