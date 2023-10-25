# Databricks notebook source
# dbutils.widgets.text("Database", "")
# dbutils.widgets.text("External Location", "")
# dbutils.widgets.text("Power Model Name", "")
# dbutils.widgets.text("Life Model Name", "")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # End to End Industrial IoT (IIoT) on Databricks 
# MAGIC ## Part 2 - Data Science and Machine Learning
# MAGIC
# MAGIC <img width="400px" style="float: left; margin-right: 30px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-3.png" />
# MAGIC
# MAGIC Now that our data is flowing reliably from our sensor devices into an enriched Delta table in cloud storage, we can start to build ML models to predict power output and remaining life of our assets using historical sensor, weather, power and maintenance data. 
# MAGIC
# MAGIC
# MAGIC We create two models ***for each Wind Turbine***:
# MAGIC 1. **Turbine Power Output** - using current readings for turbine operating parameters (angle, RPM) and weather (temperature, humidity, etc.), predict the expected power output 6 hours from now
# MAGIC 2. **Turbine Remaining Life** - predict the remaining life in days until the next maintenance event
# MAGIC
# MAGIC <br><br><br><br>
# MAGIC <img style="float: right; margin-right: 10px" src="https://sguptasa.blob.core.windows.net/random/iiot_blog/turbine_models.png" width=800>
# MAGIC
# MAGIC We will use the XGBoost framework to train regression models. Due to the size of the data and number of Wind Turbines, we will use Spark UDFs to distribute training across all the nodes in our cluster.
# MAGIC
# MAGIC
# MAGIC The notebook is broken into sections following these steps:<br>
# MAGIC 1. **Feature Engineering** - prepare features to improve their predictive power
# MAGIC 2. **Model Training** - train XGBoost regression models using distributed ML to predict power output and asset remaining life on historical sensor data<br>
# MAGIC 3. **Model Deployment** - deploy trained models for serving<br>
# MAGIC 4. **Model Inference** - score real data against registered models<br>
# MAGIC 5. **Operational Optimization** - combine both predictions to provide best opertating parameters

# COMMAND ----------

# MAGIC %md ## 0. Environment Setup
# MAGIC
# MAGIC The pre-requisites are listed below:
# MAGIC
# MAGIC #### Databricks Configuration Required
# MAGIC * Databricks Cluster running **DBR 11.3 ML**

# COMMAND ----------

# Parameters
DB = dbutils.widgets.get("Database")
ROOT_PATH = dbutils.widgets.get("External Location")
POWER_MODEL_NAME = dbutils.widgets.get("Power Model Name")
LIFE_MODEL_NAME = dbutils.widgets.get("Life Model Name")

# Imports
from pyspark.sql.functions import mean, col, lit
import numpy as np
import pandas as pd
import xgboost as xgb
import mlflow

# Define default database
spark.sql(f'USE {DB}')

# COMMAND ----------

# MAGIC %md ## 1. Feature Engineering
# MAGIC In order to predict power output 6 hours ahead, we need to first time-shift our data to create our label column. We can do this easily using Spark Window partitioning. 
# MAGIC
# MAGIC In order to predict remaining life, we need to backtrace the remaining life from the maintenance events. We can do this easily using cross joins. The following diagram illustrates the ML Feature Engineering pipeline:
# MAGIC
# MAGIC <img src="files/tables/vr/iiot_ml_pipeline.png" width=800>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate the age of each turbine and the remaining life in days
# MAGIC CREATE OR REPLACE VIEW turbine_age AS
# MAGIC WITH reading_dates AS (SELECT distinct date, deviceid FROM turbine_power),
# MAGIC   maintenance_dates AS (
# MAGIC     SELECT d.*, datediff(nm.date, d.date) as datediff_next, datediff(d.date, lm.date) as datediff_last 
# MAGIC     FROM reading_dates d LEFT JOIN turbine_maintenance nm ON (d.deviceid=nm.deviceid AND d.date<=nm.date)
# MAGIC     LEFT JOIN turbine_maintenance lm ON (d.deviceid=lm.deviceid AND d.date>=lm.date ))
# MAGIC SELECT date, deviceid, ifnull(min(datediff_last),0) AS age, ifnull(min(datediff_next),0) AS remaining_life
# MAGIC FROM maintenance_dates 
# MAGIC GROUP BY deviceid, date;
# MAGIC
# MAGIC -- Calculate the power 6 hours ahead using Spark Windowing and build a feature_table to feed into our ML models
# MAGIC CREATE OR REPLACE VIEW feature_table AS
# MAGIC SELECT r.*, age, remaining_life,
# MAGIC   LEAD(power, 72, power) OVER (PARTITION BY r.deviceid ORDER BY window) as power_6_hours_ahead
# MAGIC FROM gold_readings r JOIN turbine_age a ON (r.date=a.date AND r.deviceid=a.deviceid)
# MAGIC WHERE r.date < CURRENT_DATE();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT window, power, power_6_hours_ahead FROM feature_table WHERE deviceid='WindTurbine-1'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, avg(age) as age, avg(remaining_life) as life FROM feature_table WHERE deviceid='WindTurbine-1' GROUP BY date ORDER BY date

# COMMAND ----------

# MAGIC %md ## 2. Model Training
# MAGIC
# MAGIC The next step is to train lots of different models using different algorithms and parameters in search for the one that optimally solves our business problem.
# MAGIC
# MAGIC That's where the **Spark** + **HyperOpt** + **MLflow** framework can be leveraged to easily distribute the training proccess across a cluster, efficiently optimize hyperparameters and track all experiments in order to quickly evaluate many models, choose the best one and guarantee its reproducibility.<br><br>
# MAGIC
# MAGIC ![](/files/shared_uploads/victor.rodrigues@databricks.com/ml_2.jpg)

# COMMAND ----------

# MAGIC %md ### 2a. Define Experiment for Distributed Model Autotuning with Tracking
# MAGIC
# MAGIC Benefits:
# MAGIC - Pure Python & Pandas: easy to develop, test
# MAGIC - Continue using your favorite libraries
# MAGIC - Simply assume you're working with a Pandas DataFrame for a single device
# MAGIC
# MAGIC <img src="https://github.com/PawaritL/data-ai-world-tour-dsml-jan-2022/blob/main/pandas-udf-workflow.png?raw=true" width=40%>

# COMMAND ----------

from hyperopt import hp, fmin, tpe, STATUS_OK
from sklearn.model_selection import train_test_split
from datetime import datetime

# Create a function to train a XGBoost Regressor on a turbine's data
def train_distributed_xgb(readings_pd, model_type, label_col, prediction_col):
  
  mlflow.xgboost.autolog()
  deviceid = readings_pd['deviceid'][0]
  
  # Split train and test datasets
  train, test = train_test_split(readings_pd, train_size=0.7)
  train_dmatrix = xgb.DMatrix(data=train[feature_cols].astype('float'),label=train[label_col])
  test_dmatrix = xgb.DMatrix(data=test[feature_cols].astype('float'),label=test[label_col])

  # Search space for HyperOpt
  search_space = {
    'learning_rate' : hp.loguniform('learning_rate', np.log(0.05), np.log(0.5)),
    'alpha' : hp.loguniform('alpha', np.log(1), np.log(10)),
    'colsample_bytree' : hp.loguniform('colsample_bytree', np.log(0.1), np.log(1.0)),
    'max_depth' : hp.quniform('max_depth', 1, 10, 1)
  }
  
  # Define how to evaluate each experiment
  def evaluate_model(hyperopt_params):
    
    # Convert parameters that HyperOpt supplies as float, but must be int
    if 'max_depth' in hyperopt_params: hyperopt_params['max_depth'] = int(hyperopt_params['max_depth'])
    
    # Run the experiments
    date = datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")
    with mlflow.start_run(run_name=f'{model_type}_{deviceid}_{date}'):
    
      # Tag the model type and device ID
      mlflow.set_tag('deviceid', deviceid)
      mlflow.set_tag('model', model_type)

      # Train an XGBRegressor on the data for this Turbine
      metrics = {}
      model = xgb.train(params=hyperopt_params, dtrain=train_dmatrix, evals=[(train_dmatrix, 'train'),(test_dmatrix, 'test')], evals_result=metrics)

    return {'status': STATUS_OK, 'loss': metrics['test']['rmse'][0]}
  
  # Autotune hyperparamenters
  best_hparams = fmin(evaluate_model, search_space, algo=tpe.suggest, max_evals=10)
    
  return readings_pd

# COMMAND ----------

# MAGIC %md ### 2b. Power Output
# MAGIC [Pandas UDFs](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/udf-python-pandas?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fazure-databricks%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json) allow us to vectorize Pandas code across multiple nodes in a cluster. Here we create a UDF to train an XGBoost Regressor model against all the historic data for a particular Wind Turbine. We use a Grouped Map UDF as we perform this model training on the Wind Turbine group level.

# COMMAND ----------

# Create a Spark Dataframe that contains the features and labels we need
non_feature_cols = ['date','window','deviceid','winddirection','remaining_life']
feature_cols = ['angle','rpm','temperature','humidity','windspeed','power','age']
label_col = 'power_6_hours_ahead'
prediction_col = label_col + '_predicted'

# Read in our feature table and select the columns of interest
feature_df = spark.table('feature_table').selectExpr(non_feature_cols + feature_cols + [label_col] + [f'0 as {prediction_col}'])

# Register a Pandas UDF to distribute XGB model training using Spark
def train_power_models(readings_pd: pd.DataFrame) -> pd.DataFrame:
  return train_distributed_xgb(readings_pd, 'power_prediction', label_col, prediction_col)

# Run distributed model autotuning
feature_df.groupBy('deviceid').applyInPandas(train_power_models, schema=feature_df.schema).count()

# COMMAND ----------

# Find the best experiment
best_run_id = mlflow.search_runs(filter_string=f'tags.deviceid="WindTurbine-1" and tags.model="power_prediction"')\
  .dropna().sort_values("metrics.train-rmse")['run_id'].iloc[0]

# Get the model
model_udf = mlflow.pyfunc.spark_udf(spark, f'runs:/{best_run_id}/model')

# Score the dataset
preds = feature_df.filter('deviceid="WindTurbine-1"').withColumn(prediction_col, model_udf(*feature_cols))

display(preds.groupBy('date','deviceid').agg(mean('power_6_hours_ahead'), mean('power_6_hours_ahead_predicted')))

# COMMAND ----------

# MAGIC %md **Automated Model Tracking in Databricks**
# MAGIC
# MAGIC As you train the models, notice how Databricks-managed MLflow automatically tracks each run in the "Runs" tab of the notebook. You can open each run and view the parameters, metrics, models and model artifacts that are captured by MLflow Autologging. For XGBoost Regression models, MLflow tracks: 
# MAGIC 1. Any model parameters (alpha, colsample, learning rate, etc.) passed to the `params` variable
# MAGIC 2. Metrics specified in `evals` (RMSE by default)
# MAGIC 3. The trained XGBoost model file
# MAGIC 4. Feature importances
# MAGIC
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/iiot_mlflow_tracking.gif" width=800>

# COMMAND ----------

# MAGIC %md ### 2c. Remaining Life
# MAGIC Our second model predicts the remaining useful life of each Wind Turbine based on the current operating conditions. We have historical maintenance data that indicates when a replacement activity occured - this will be used to calculate the remaining life as our training label. 
# MAGIC
# MAGIC Once again, we train an XGBoost model for each Wind Turbine to predict the remaining life given a set of operating parameters and weather conditions

# COMMAND ----------

# Create a Spark Dataframe that contains the features and labels we need
non_feature_cols = ['date','window','deviceid','winddirection']
feature_cols = ['angle','rpm','temperature','humidity','windspeed','power','age']
label_col = 'remaining_life'
prediction_col = label_col + '_predicted'

# Read in our feature table and select the columns of interest
feature_df = spark.table('feature_table').selectExpr(non_feature_cols + feature_cols + [label_col] + [f'0 as {prediction_col}'])

# Register a Pandas UDF to distribute XGB model training using Spark
def train_life_models(readings_pd: pd.DataFrame) -> pd.DataFrame:
  return train_distributed_xgb(readings_pd, 'life_prediction', label_col, prediction_col)

# Run distributed model autotuning
feature_df.groupBy('deviceid').applyInPandas(train_life_models, schema=feature_df.schema).count()

# COMMAND ----------

# Find the best experiment
best_run_id = mlflow.search_runs(filter_string=f'tags.deviceid="WindTurbine-1" and tags.model="life_prediction"')\
  .dropna().sort_values("metrics.train-rmse")['run_id'].iloc[0]

# Get the model
model_udf = mlflow.pyfunc.spark_udf(spark, f'runs:/{best_run_id}/model')

# Score the dataset
preds = feature_df.filter('deviceid="WindTurbine-1"').withColumn(prediction_col, model_udf(*feature_cols))

display(preds.groupBy('date').agg(mean('remaining_life'),mean('remaining_life_predicted')))

# COMMAND ----------

# MAGIC %md ## 3. Model Deployment
# MAGIC
# MAGIC After choosing a model that best fits our needs, we can then go ahead and kick off its operationalization proccess.
# MAGIC
# MAGIC The first step is to register it to the **Model Registry**, where we can version, manage its life cycle with an workflow and track/audit all changes.<br><br>
# MAGIC
# MAGIC ![](/files/shared_uploads/victor.rodrigues@databricks.com/ml_3.jpg)

# COMMAND ----------

# MAGIC %md ### 3a. Register Models

# COMMAND ----------

# Retrieve the remaining_life and power_output experiments on WindTurbine-1 and get the best performing model (min RMSE)

turbine = "WindTurbine-1"
power_model = "power_prediction"
life_model = "life_prediction"

best_life_model = mlflow.search_runs(filter_string=f'tags.deviceid="{turbine}" and tags.model="{life_model}"')\
  .dropna().sort_values("metrics.train-rmse")['run_id'].iloc[0]

best_power_model = mlflow.search_runs(filter_string=f'tags.deviceid="{turbine}" and tags.model="{power_model}"')\
  .dropna().sort_values("metrics.train-rmse")['run_id'].iloc[0]

reg_life_model = mlflow.register_model(
  model_uri=f'runs:/{best_power_model}/model',
  name=f'{POWER_MODEL_NAME} - {turbine}'
)

reg_power_model = mlflow.register_model(
  model_uri=f'runs:/{best_life_model}/model',
  name=f'{LIFE_MODEL_NAME} - {turbine}'
)

# COMMAND ----------

# MAGIC %md ### 3b. Deploy to Production

# COMMAND ----------

# Transition models to production stage — we're intentionally skipping the validation process for simplicity
client = mlflow.client.MlflowClient()

client.transition_model_version_stage(
  name=reg_life_model.name,
  version=reg_life_model.version,
  stage='production'
)

client.transition_model_version_stage(
  name=reg_power_model.name,
  version=reg_power_model.version,
  stage='production'
)

# COMMAND ----------

# MAGIC %md ## 4. Model Inference
# MAGIC
# MAGIC We can now score this model in batch, streaming or via REST API calls (in case we choose to enable Serverless Model Serving) so we can consume it from any other applications or tools, like Power BI.
# MAGIC
# MAGIC In this example, we'll batch score new data in order to make decisions about our production.<br><br>
# MAGIC
# MAGIC ![](/files/shared_uploads/victor.rodrigues@databricks.com/ml_4.jpg)

# COMMAND ----------

# MAGIC %md ### 4a. Power Output

# COMMAND ----------

power_udf = mlflow.pyfunc.spark_udf(spark, f'models:/{POWER_MODEL_NAME} - {turbine}/production')

# COMMAND ----------

# Create a Spark Dataframe that contains the features and labels we need
non_feature_cols = ['date','window','deviceid','winddirection','remaining_life']
feature_cols = ['angle','rpm','temperature','humidity','windspeed','power','age']
label_col = 'power_6_hours_ahead'
prediction_col = label_col + '_predicted'

# Read in our feature table and select the columns of interest
feature_df = spark.table('feature_table').selectExpr(non_feature_cols + feature_cols + [label_col] + [f'0 as {prediction_col}']).filter(f'deviceid="{turbine}"')

# Score model
scored_df = feature_df.withColumn(
  prediction_col,
  power_udf(*feature_cols)
)

# Save to Delta table
scored_df.writeTo('turbine_power_predictions').createOrReplace()

display(spark.table('turbine_power_predictions'))

# COMMAND ----------

# MAGIC %md ### 4b. Remaining Life

# COMMAND ----------

life_udf = mlflow.pyfunc.spark_udf(spark, f'models:/{LIFE_MODEL_NAME} - {turbine}/production')

# COMMAND ----------

# Create a Spark Dataframe that contains the features and labels we need
non_feature_cols = ['date','window','deviceid','winddirection','power_6_hours_ahead_predicted']
feature_cols = ['angle','rpm','temperature','humidity','windspeed','power','age']
label_col = 'remaining_life'
prediction_col = label_col + '_predicted'

# Read in our feature table and select the columns of interest
feature_df = spark.table('turbine_power_predictions').selectExpr(non_feature_cols + feature_cols + [label_col] + [f'0 as {prediction_col}']).filter(f'deviceid="{turbine}"')

# Score model
scored_df = feature_df.withColumn(
  prediction_col,
  life_udf(*feature_cols)
)

# Save to Delta table
scored_df.writeTo('turbine_life_predictions').createOrReplace()

display(spark.table('turbine_life_predictions'))

# COMMAND ----------

# MAGIC %md ## 5. Operational Optimization
# MAGIC We can now identify the optimal operating conditions for maximizing power output while also maximizing asset useful life. 
# MAGIC
# MAGIC \\(Revenue = Price\displaystyle\sum_{d=1}^{365} Power_d\\)
# MAGIC
# MAGIC \\(Cost = {365 \over Life} Price \displaystyle\sum_{h=1}^{24} Power_h \\)
# MAGIC
# MAGIC \\(Profit = Revenue - Cost\\)
# MAGIC
# MAGIC \\(Power_t\\) and \\(Life\\) will be calculated by scoring many different setting values. The results can be visualized to identify the optimal setting that yields the highest profit.

# COMMAND ----------

# Create a baseline scenario — ideally, it would reflect current operating conditions
scenario = {
  'angle':None,
  'rpm':8.0,
  'temperature':25.0,
  'humidity':50.0,
  'windspeed':5.0,
  'power':150.0,
  'age':10.0
}

# Generate 15 different RPM configurations to be evaluated
scenarios = []
for setting in range(1,15):
  this_scenario = scenario.copy()
  this_scenario['angle'] = float(setting)
  scenarios.append(this_scenario)
scenarios_df = spark.createDataFrame(scenarios)

# Calculalte the Revenue, Cost and Profit generated for each RPM configuration
opt_df = (scenarios_df
  .withColumn('Expected Power', power_udf(*feature_cols))
  .withColumn('Expected Life',life_udf(*feature_cols))
  .withColumn('Revenue', col('Expected Power') * lit(24*365))
  .withColumn('Cost', col('Expected Power') * lit(24*365) / col('Expected Life'))
  .withColumn('Profit', col('Revenue') - col('Cost'))
)

display(opt_df)

# COMMAND ----------

# MAGIC %md The optimal operating parameters for **WindTurbine-1** given the specified weather conditions is **7 degrees** for generating a maximum profit around **$1.4M**! Your results may vary due to the random nature of the sensor readings. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC Now that our models are created and the data is scored, we are now ready to build our Predictive Maintenance dashboard to track the main KPIs and status of our entire Wind Turbine Farm in <a href="/sql/dashboards/f27ba14b-1be9-4dbf-944b-f40fbbb47aa5">DBSQL</a> and/or <a href="https://vorodrigues.grafana.net/d/acef080c-3e23-4d11-8e1e-8109646c7c76/wind-turbines-monitoring"> Grafana</a>.
