# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # IoT Platform with Databricks Lakehouse - Ingesting Industrial Sensor Data for Real-Time Analysis
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-full.png " style="float: left; margin-right: 30px" width="600px" />
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ## What is The Databricks Lakehouse for IoT & Manufacturing?
# MAGIC
# MAGIC It's the only enterprise data platform that allows you to leverage all your data, from any source, running any workload to optimize your production lines with real time data at the lowest cost. 
# MAGIC
# MAGIC The Lakehouse allow you to centralize all your data, from IoT realtime streams to inventory and sales data, providing operational speed and efficiency at a scale never before possible.
# MAGIC
# MAGIC
# MAGIC ### Simple
# MAGIC   One single platform and governance/security layer for your data warehousing and AI to **accelerate innovation** and **reduce risks**. No need to stitch together multiple solutions with disparate governance and high complexity.
# MAGIC
# MAGIC ### Open
# MAGIC   Built on open source and open standards. You own your data and prevent vendor lock-in, with easy integration with a wide range of 3rd party software vendors and services. Being open also lets you share your data with any external organization, regardless of their make-up of their software stack or vendor.
# MAGIC
# MAGIC ### Multi-cloud
# MAGIC   One consistent data platform across clouds. Process your data where you need it or where it resides.
# MAGIC  
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Flakehouse%2Flakehouse-iot-platform%2F00-IOT-wind-turbine-introduction-lakehouse&cid=1444828305810485&uid=1801482128896593">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Wind Turbine Predictive Maintenance with the Lakehouse
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-photo-open-license.jpg" width="400px" style="float:right; margin-left: 20px"/>
# MAGIC
# MAGIC Being able to collect and centralize industrial equipment information in real time is critical in the energy space. When a wind turbine is down, it is not generating power which leads to poor customer service and lost revenue. Data is the key to unlock critical capabilities such as energy optimization, anomaly detection, and/or predictive maintenance. <br/> 
# MAGIC
# MAGIC Predictive maintenance examples include:
# MAGIC
# MAGIC - Predict mechanical failure in an energy pipeline
# MAGIC - Detect abnormal behavior in a production line
# MAGIC - Optimize supply chain of parts and staging for scheduled maintenance and repairs
# MAGIC
# MAGIC ### What we'll build
# MAGIC
# MAGIC In this demo, we'll build an end-to-end IoT platform, collecting data from multiple sources in real time. 
# MAGIC
# MAGIC Based on this information, we will show how analyst can proactively identify and schedule repairs for Wind turbines prior to failure, in order to increase energy production.
# MAGIC
# MAGIC In addition, the business requested a dashboard that would allow their Turbine Maintenance group to monitor the turbines and identify any that are currently inoperable and those that are at risk of failure. This will also allow us to track our ROI and ensure we reach our productivity goals over the year.
# MAGIC
# MAGIC At a very high level, this is the flow we will implement:
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-maintenance.png" />
# MAGIC
# MAGIC
# MAGIC 1. Ingest and create our IoT database and tables which are easily queriable via SQL
# MAGIC 2. Secure data and grant read access to the Data Analyst and Data Science teams.
# MAGIC 3. Run BI queries to analyze existing failures
# MAGIC 4. Build ML model to monitor our wind turbine farm & trigger predictive maintenance operations
# MAGIC
# MAGIC Being able to predict which wind turbine will potentially fail is only the first step to increase our wind turbine farm efficiency. Once we're able to build a model predicting potential maintenance, we can dynamically adapt our spare part stock and even automatically dispatch maintenance team with the proper equipment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our dataset
# MAGIC
# MAGIC To simplify this demo, we'll consider that an external system is periodically sending data into our blob storage (S3/ADLS/GCS):
# MAGIC
# MAGIC - Turbine data *(location, model, identifier – typically a SCD)*
# MAGIC - Wind turbine sensors, every sec *(energy produced, rotation speed – typically in streaming)*
# MAGIC - Weather *(humidity, temperature, ... – typically in streaming)*
# MAGIC - Maintenance data *(when a service was executed on a equipment)*
# MAGIC
# MAGIC *Note that at a technical level, our data could come from any source. Databricks can ingest data from any system (SalesForce, Fivetran, queuing message like kafka, blob storage, SQL & NoSQL databases...).*
# MAGIC
# MAGIC Let's see how this data can be used within the Lakehouse to analyze sensor data & trigger predictive maintenance.

# COMMAND ----------

# MAGIC %md ### Turbine data

# COMMAND ----------

# MAGIC %sql select count(distinct deviceId) as devices from vr_iiot.dev.devices

# COMMAND ----------

# MAGIC %sql select * from vr_iiot.dev.devices

# COMMAND ----------

# MAGIC %md ### Turbine sensors

# COMMAND ----------

# MAGIC %sql select count(*) as readings, count(distinct deviceId) as devices from vr_iiot.dev.turbine_raw

# COMMAND ----------

# MAGIC %sql select * from vr_iiot.dev.turbine_raw

# COMMAND ----------

# MAGIC %md ### Weather

# COMMAND ----------

# MAGIC %sql select * from vr_iiot.dev.weather_raw

# COMMAND ----------

# MAGIC %md ### Maintenance

# COMMAND ----------

# MAGIC %sql select * from vr_iiot.dev.turbine_maintenance
