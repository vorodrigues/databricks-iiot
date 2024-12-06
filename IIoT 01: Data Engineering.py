# Databricks notebook source
# dbutils.widgets.text("Database", "")
# dbutils.widgets.text("Event Hub Name","")
# dbutils.widgets.text("IoT Hub Connection String", "")
# dbutils.widgets.text("External Location", "")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # End to End Industrial IoT (IIoT) on Databricks
# MAGIC ## Part 1 - Data Engineering
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-1.png" />
# MAGIC
# MAGIC
# MAGIC <br/>
# MAGIC <div style="padding-left: 420px">
# MAGIC Our first step is to ingest and clean the raw data we received so that our Data Analyst team can start running analysis on top of it.
# MAGIC
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right; margin-top: 20px" width="200px">
# MAGIC
# MAGIC ### Delta Lake
# MAGIC
# MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake tables. [Delta Lake](https://delta.io) is an open storage framework for reliability and performance. <br/>
# MAGIC It provides many functionalities such as *(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)* <br />
# MAGIC For more details on Delta Lake, run `dbdemos.install('delta-lake')`
# MAGIC
# MAGIC The notebook is broken into sections following these steps:
# MAGIC 1. **Data Ingestion** - stream real-time raw sensor data from Azure IoT Hubs into the Delta format in cloud storage
# MAGIC 2. **Data Processing** - stream process sensor data from raw (Bronze) to silver (aggregated) to gold (enriched) Delta tables on cloud storage

# COMMAND ----------

# MAGIC %md ## 0. Environment Setup
# MAGIC
# MAGIC The pre-requisites are listed below:
# MAGIC
# MAGIC ### Services Required
# MAGIC * Azure IoT Hub 
# MAGIC
# MAGIC ### Databricks Configuration Required
# MAGIC * 3-node (min) Databricks Cluster running **DBR 11.3 ML** and the following libraries:
# MAGIC   * **Azure Event Hubs Connector for Databricks** - Maven coordinates `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17`

# COMMAND ----------

# Parameters
DB = dbutils.widgets.get("Database")
ROOT_PATH = dbutils.widgets.get("External Location")
BRONZE_PATH = ROOT_PATH + "bronze/"
SILVER_PATH = ROOT_PATH + "silver/"
GOLD_PATH = ROOT_PATH + "gold/"
CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"

# Other initializations
# Optimally, use Databricks Secrets to store/retrieve sensitive information (ie. dbutils.secrets.get('iot','iot-hub-connection-string'))
ehConf = { 
  'ehName':dbutils.widgets.get("Event Hub Name"),
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(dbutils.widgets.get("IoT Hub Connection String")),
  # If it's required to reprocess the whole history from IoT Hub
  # 'eventhubs.startingPosition':'{"offset":"-1", "seqNo":-1, "enqueuedTime":null, "isInclusive":true}'
}

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.sql.shuffle.partitions", 12)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled", "false")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# Imports
from pyspark.sql import functions as F
import mlflow

# Define default database
spark.sql(f'USE {DB}')

# COMMAND ----------

# MAGIC %md ## 1. Data Ingestion from IoT Hub
# MAGIC Databricks provides a native connector to IoT and Event Hubs. Below, we will use PySpark Structured Streaming to read from an IoT Hub stream of data and write the data in it's raw format directly into Delta. 
# MAGIC
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/iot_simulator.gif" width=800>
# MAGIC
# MAGIC We have two separate types of data payloads in our IoT Hub:
# MAGIC 1. **Turbine Sensor readings** - this payload contains `date`,`timestamp`,`deviceid`,`rpm` and `angle` fields
# MAGIC 2. **Weather Sensor readings** - this payload contains `date`,`timestamp`,`temperature`,`humidity`,`windspeed`, and `winddirection` fields
# MAGIC
# MAGIC We split out the two payloads into separate streams and write them both into Delta locations on cloud Storage. We are able to query these two Bronze tables *immediately* as the data streams in.

# COMMAND ----------

# MAGIC %md ### 1a. Bronze Layer

# COMMAND ----------

# Schema of incoming data from IoT hub
schema = "timestamp timestamp, deviceId string, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double, power double"

# Read directly from IoT Hub using the EventHubs library for Databricks
iot_stream = (
  spark.readStream.format("eventhubs")
    .options(**ehConf)
    .load()
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema))
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))
)

# Split our IoT Hub stream into separate streams and write them both into their own Delta locations
# Write turbine events
(iot_stream.filter('temperature is null')
  .select('date','timestamp','deviceId','rpm','angle','power')
  .writeStream.format('delta')
  .partitionBy('date')
  .option("checkpointLocation", CHECKPOINT_PATH + "turbine_raw")
  .toTable('turbine_raw')
)

# Write weather events
(iot_stream.filter(iot_stream.temperature.isNotNull())
  .select('date','deviceid','timestamp','temperature','humidity','windspeed','winddirection')
  .writeStream.format('delta')
  .partitionBy('date')
  .option("checkpointLocation", CHECKPOINT_PATH + "weather_raw")
  .toTable('weather_raw')
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- We can query the data directly from storage immediately as soon as it starts streams into Delta 
# MAGIC SELECT * FROM turbine_raw WHERE deviceid = 'WindTurbine-1' AND `timestamp` > current_timestamp() - INTERVAL 2 minutes

# COMMAND ----------

# MAGIC %md ## 2. Data Processing
# MAGIC While our raw sensor data is being streamed into Bronze Delta tables on cloud storage, we can create streaming pipelines on this data that flow it through Silver and Gold data sets.
# MAGIC
# MAGIC We will use the following schema for Silver and Gold data sets:
# MAGIC
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/iot_delta_bronze_to_gold.png" width=800>

# COMMAND ----------

# MAGIC %md ### 2a. Silver Layer
# MAGIC The first step of our processing pipeline will clean and aggregate the measurements to 1 hour intervals. 
# MAGIC
# MAGIC Since we are aggregating time-series values and there is a likelihood of late-arriving data and data changes, we will use the [**MERGE**](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/merge-into?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fazure-databricks%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json) functionality of Delta to upsert records into target tables. 
# MAGIC
# MAGIC MERGE allows us to upsert source records into a target storage location. This is useful when dealing with time-series data as:
# MAGIC 1. Data often arrives late and requires aggregation states to be updated
# MAGIC 2. Historical data needs to be backfilled while streaming data is feeding into the table
# MAGIC
# MAGIC When streaming source data, `foreachBatch()` can be used to perform a merges on micro-batches of data.

# COMMAND ----------

# Function to merge incremental data from a streaming foreachbatch into a target Delta table
def merge_delta(incremental, target): 
  incremental.dropDuplicates(['date','window','deviceid']).createOrReplaceTempView("incremental")
  
  try:
    # MERGE records into the target table using the specified join key
    # incremental._jdf.sparkSession().sql(f"""
    incremental.sparkSession.sql(f"""
      MERGE INTO {target} t
      USING incremental i
      ON i.date=t.date AND i.window = t.window AND i.deviceId = t.deviceid
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
  except:
    # If the †arget table does not exist, create one
    incremental.writeTo(target).partitionedBy('date').createOrReplace()

# COMMAND ----------

# Create functions to merge turbine and weather data into their target Delta tables
def merge_delta(incremental, target): 
  incremental.dropDuplicates(['date','window','deviceid']).createOrReplaceTempView("incremental")
  
  try:
    # MERGE records into the target table using the specified join key
    incremental._jdf.sparkSession().sql(f"""
      MERGE INTO {target} t
      USING incremental i
      ON i.date=t.date AND i.window = t.window AND i.deviceId = t.deviceid
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
  except:
    # If the †arget table does not exist, create one
    incremental.writeTo(target).partitionedBy('date').createOrReplace()
    
# Stream turbine events to silver layer
(spark.readStream.format('delta').table("turbine_raw")
  .filter("date = current_date()")   # Only for demo purposes
  .withWatermark('timestamp', '30 seconds')
  .groupBy('deviceId','date',F.window('timestamp','15 seconds'))
  .agg(F.avg('rpm').alias('rpm'), F.avg("angle").alias("angle"), F.avg("power").alias("power"))
  .selectExpr('deviceId', 'date', 'window.start as window', 'rpm', 'angle', 'power')
  .writeStream
  .foreachBatch(lambda i, b: merge_delta(i, "turbine_agg"))
  .outputMode("update")
  .option("checkpointLocation", CHECKPOINT_PATH + "turbine_agg")
  .start()
)

# Stream wheather events to silver layer
(spark.readStream.format('delta').table("weather_raw")
  .filter("date = current_date()")   # Only for demo purposes
  .withWatermark('timestamp', '30 seconds')
  .groupBy('deviceid','date',F.window('timestamp','15 seconds'))
  .agg({"temperature":"avg","humidity":"avg","windspeed":"avg","winddirection":"last"})
  .selectExpr('date','window.start as window','deviceid','`avg(temperature)` as temperature','`avg(humidity)` as humidity',
              '`avg(windspeed)` as windspeed','`last(winddirection)` as winddirection')
  .writeStream
  .foreachBatch(lambda i, b: merge_delta(i, "weather_agg"))
  .outputMode("update")
  .option("checkpointLocation", CHECKPOINT_PATH + "weather_agg")
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As data gets merged in real-time to our hourly table, we can query it immediately
# MAGIC SELECT * FROM turbine_agg t JOIN weather_agg w ON (t.date=w.date AND t.window=w.window) 
# MAGIC WHERE t.deviceid='WindTurbine-1' AND t.window > current_timestamp() - INTERVAL 10 minutes
# MAGIC ORDER BY t.window DESC

# COMMAND ----------

# MAGIC %md ### 2b. Gold Layer & ML Inference
# MAGIC Next we perform a streaming join of weather and turbine readings to create one enriched dataset we can use for data science and model training.
# MAGIC
# MAGIC Also, our Data Science team has already used this data to build a predictive maintenance model and saved it into MLflow Model Registry (we'll see how to do that next).
# MAGIC
# MAGIC One of the key value of the Lakehouse is that we can easily load this model and predict faulty turbines with into our pipeline directly.
# MAGIC
# MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLflow abstract that for us.
# MAGIC
# MAGIC All we have to do is load the model, and call it as a Spark UDF (or SQL)

# COMMAND ----------

# Load our model to predict the turbine remaining life
predict_remaining_life = mlflow.pyfunc.spark_udf(spark, 'models:/VR IIoT - Life - WindTurbine-1/production', result_type='float')

# COMMAND ----------

# Read streams from both Silver Delta tables
turbine_agg = (spark.readStream.format('delta')
  .option("ignoreChanges", True)
  .table('turbine_agg')
  .withWatermark('window', '30 seconds')
  .filter("date = current_date()")   # Only for demo purposes
)
weather_agg = (spark.readStream.format('delta')
  .option("ignoreChanges", True)
  .table('weather_agg')
  .withWatermark('window', '30 seconds')
  .drop('deviceid')
  .filter("date = current_date()")   # Only for demo purposes
)

# Identify the last maintenance for each turbine
dates_df = spark.sql('select deviceid, max(date) as maint_date from turbine_maintenance group by deviceid')

# Join all streams
turbine_enriched = (turbine_agg
  .join(weather_agg, ['date','window'])
  .join(dates_df, on='deviceid', how='left')
)

# Write the stream to a foreachBatch function which performs the MERGE as before
(turbine_enriched
  .selectExpr('date','deviceid','window','rpm','angle','power','temperature','humidity','windspeed','winddirection',
              "datediff(date,ifnull(maint_date,to_date('2022-11-21'))) as age")
  .withColumn('remaining_life', predict_remaining_life())
  .writeStream
  .foreachBatch(lambda i, b: merge_delta(i, "turbine_enriched"))
  .option("checkpointLocation", CHECKPOINT_PATH + "turbine_enriched")
  .start()
)

# COMMAND ----------

# MAGIC %sql SELECT * FROM turbine_enriched WHERE deviceid IN ('WindTurbine-10','WindTurbine-20') AND window > current_timestamp() - INTERVAL 5 minutes

# COMMAND ----------

# MAGIC %md ## Generate Events

# COMMAND ----------

from resources.iiot_event_generator import iiot_event_generator
iiot_event_generator(20000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC Our pipeline is now ready. We have an end-to-end cycle and our ML model has been integrated seamlessly by our Data Engineering team.
# MAGIC
# MAGIC For more details on model training, open the [model training notebook]($../IIoT 02: Data Science and Machine Learning).
# MAGIC
# MAGIC Our final dataset includes our ML prediction for our Predictive Maintenance use-case. 
# MAGIC
# MAGIC We are now ready to build our Predictive Maintenance dashboard to track the main KPIs and status of our entire Wind Turbine Farm in <a href="/sql/dashboards/f27ba14b-1be9-4dbf-944b-f40fbbb47aa5">DBSQL</a> and/or <a href="https://vorodrigues.grafana.net/d/acef080c-3e23-4d11-8e1e-8109646c7c76/wind-turbines-monitoring"> Grafana</a>.
