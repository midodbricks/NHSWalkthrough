# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Lakehouse for NHS - Day in the life of a dataset
# MAGIC <br />
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-0.png" style="float: left; margin-right: 30px; margin-top:10px" width="650px" />
# MAGIC
# MAGIC ## What is The Databricks Lakehouse for NHS?
# MAGIC
# MAGIC It's the only enterprise data platform that allows NHS data engineers, analysts and data scientists to ingest, transform, publish and serve data in an open, secure, cross cloud platform.
# MAGIC
# MAGIC ### 1. Simple
# MAGIC   One single platform and governance/security layer for your data warehousing and AI to **accelerate innovation** and **reduce risks**. No need to stitch together multiple solutions with disparate governance and high complexity.
# MAGIC
# MAGIC ### 2. Open
# MAGIC   Open source in healthcare presents a pivotal opportunity for data ownership, prevention of vendor lock-in, and seamless integration with external solutions. By leveraging open standards, healthcare organizations gain the flexibility to share data with any external entity. This promotes interoperability, advances collaboration, and enables comprehensive data analysis, driving improved patient outcomes and operational efficiency.
# MAGIC
# MAGIC ### 3. Multicloud
# MAGIC   Adoption of a multi-cloud strategy in healthcare organizations is inevitable and integral to competitive success, delivering cost reduction, flexibility, and improved remote services.<br/>
# MAGIC  
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Flakehouse%2Flakehouse-hls-readmission%2F00-patient-readmission-introduction&cid=984752964297111&uid=8971630472972013">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## DEMO: NHS England Data Flow Demonstrator
# MAGIC Showing an example flow of data into NHS England, associated cleansing, analysis and serving
# MAGIC
# MAGIC This example simulates a flow of data from disparate sources inside the NHS ecosystem to help deliver on the following:
# MAGIC
# MAGIC ***Higher Quality Care:***
# MAGIC It enables hospitals to enhance patient care by proactively identifying individuals at a higher risk of readmission. By identifying these patients early on, healthcare providers can implement targeted interventions, such as personalized discharge plans, medication adherence support, or follow-up appointments, to mitigate the likelihood of readmissions.
# MAGIC
# MAGIC  This approach not only improves patient outcomes but also reduces the burden on healthcare facilities and optimizes resource allocation.
# MAGIC
# MAGIC ***Cost Optimization***
# MAGIC Precise readmission prediction plays a pivotal role in cost containment for both hospitals and insurance groups. Hospital readmissions contribute significantly to healthcare expenditures, and accurate prediction can help identify patterns and risk factors associated with costly readmission cases. Developpigng proactive approach not only reduces healthcare costs but also promotes financial sustainability for hospitals and insurance providers.
# MAGIC
# MAGIC
# MAGIC Databricks offers hospitals and insurance companies unique capabilities to predict readmissions and drive value. Databricks' holistic approach empowers healthcare organizations to leverage data effectively and achieve accurate readmission predictions while saving time and resources.
# MAGIC
# MAGIC
# MAGIC ### What we will build
# MAGIC
# MAGIC To predict patient re-admissions, we'll build an end-to-end solution with the Lakehouse, leveraging data from UDAL and the SDE: patient demographics, logitudinal health records (past visits, conditions, allergies, etc), and real-time patient admission, discharge, transofrm (ADT) information...  
# MAGIC
# MAGIC At a high level, we will implement the following flow:
# MAGIC <br/><br/>
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px; height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px; margin-left: -35px;}
# MAGIC .badge_b { 
# MAGIC   margin-left: 25px; min-height: 32px;}
# MAGIC </style>
# MAGIC
# MAGIC
# MAGIC <div style="margin-left: 20px">
# MAGIC   <div class="badge_b"><div class="badge">1</div> Connect to data store in NHS data stores, in this example - UDAL and the SDE and leveraging tools such as Lakehouse Federation to remove data duplication and Lakehouse Assistant to improve productivity</div>
# MAGIC   <div class="badge_b"><div class="badge">2</div>  Provide access for our business analysts to ask natural language questions of our data to obtain insights</div>
# MAGIC   <div class="badge_b"><div class="badge">4</div> Intuitively create and train predictive models using AutoML</div>
# MAGIC   <div class="badge_b"><div class="badge">5</div>  Securely share data both internaly and externally without data movement</div>
# MAGIC </div>
# MAGIC <br/><br/>
# MAGIC <img src ='/files/mido/images/flow'>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1/ Ingesting and preparing the data (Data Engineering)
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-2.png" />
# MAGIC
# MAGIC
# MAGIC <br/>
# MAGIC <div style="padding-left: 420px">
# MAGIC Our first step is to connect to our data across the NHS estate, in this case we'll use lakehouse federation to connect to UDAL inside NHS Englands's Azure tenant and and join it with data inside the former NHS Digital's AWS-based Secure Data Environment
# MAGIC
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right; margin-top: 20px" width="200px">
# MAGIC
# MAGIC ### Databricks Notebooks
# MAGIC
# MAGIC We'll use Lakehouse Assistant to help our data engineer build the data pipelines in the language of their choosing, build code quickly and efficiently and help fix any errors
# MAGIC
# MAGIC ### Lakehouse Assistant
# MAGIC
# MAGIC We'll use Lakehouse Assistant to help our data engineer build the data pipelines in the language of their choosing, build code quickly and efficiently and help fix any errors
# MAGIC
# MAGIC <br/>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Open the NHS Data Engineer Persona [notebook](https://adb-984752964297111.11.azuredatabricks.net/?o=984752964297111#notebook/2619274335399317/command/2619274335399318) 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2/ Securing data & governance (Unity Catalog)
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-6.png" />
# MAGIC
# MAGIC <br/><br/><br/>
# MAGIC <div style="padding-left: 420px">
# MAGIC
# MAGIC   Now that our data has been ingested into UDAL, we can explore the catalogs and schemas created using the [Data Explorer](/explore/data/dbdemos/fsi_credit_decisioning). 
# MAGIC
# MAGIC To leverage our data assets across the entire organization, we need:
# MAGIC
# MAGIC * AI Generated documentation
# MAGIC * Fine grained ACLs for our Analysts & Data Scientists teams
# MAGIC * Lineage between all our data assets
# MAGIC * real-time PII data encryption 
# MAGIC * Audit logs
# MAGIC * Data Sharing with external organization 
# MAGIC
# MAGIC   
# MAGIC  </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3/ Perform ad hoc analysis with natural language with Genie Workspaces
# MAGIC
# MAGIC <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-patient-dashboard.png"  style="float: right; margin: 0px 0px 10px;"/>
# MAGIC
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-3.png" />
# MAGIC  
# MAGIC <br><br>
# MAGIC Our datasets are now properly ingested, secured, with a high quality and easily discoverable within our organization.
# MAGIC
# MAGIC Business Users are now ready to run adhoc analysis, asking natural language questions of our data and producing insights, outputs and visualisations.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Open the [NHS Genie Workspace](https://adb-984752964297111.11.azuredatabricks.net/?o=984752964297111#notebook/2619274335419152/command/2619274335419153) to start interacting with our published dataset</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4/ Predict readmission risk with Data Science & Auto-ML
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-4.png" />
# MAGIC
# MAGIC <br><br>
# MAGIC Being able to run analysis on our past data already gave us a lot of insight to drive understand over-all patient risk factors.
# MAGIC
# MAGIC However, knowing re-admission risk in the past isn't enough. We now need to take it to the next level and build a predictive model to forecast risk. 
# MAGIC
# MAGIC This is where the Lakehouse value comes in. Within the same platform, anyone can start building ML model to run such analysis, including low code solution with AutoML.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Machine Learning next steps:
# MAGIC
# MAGIC [04.1-Feature-Engineering-patient-readmission](https://adb-984752964297111.11.azuredatabricks.net/?o=984752964297111#notebook/2619274335419569/command/2619274335419611): Open the first notebook to analyze our data and start building our model leveraging Databricks AutoML.
# MAGIC

# COMMAND ----------


