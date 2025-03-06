# Databricks notebook source
# MAGIC %pip install databricks-sdk -U -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text(name="catalog", label="catalog", defaultValue="qyu_test")
dbutils.widgets.text(name="schema", label="schema", defaultValue="genai_pdf_workflow")
dbutils.widgets.text(name="job_id", label="job id", defaultValue="423005687915800")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
job_id = dbutils.widgets.get("job_id")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pdf_ingestion_benchmark_run_metadata 
    (
    job_id STRING,
    run_id LONG, 
    instance STRING, 
    num_workers LONG, 
    DBR STRING, 
    engine STRING, 
    start_time STRING, 
    duration_in_mins DOUBLE,
    overriding_params MAP<STRING, STRING>
    );
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Collect Job and Run Statistics

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from datetime import datetime

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Level Information

# COMMAND ----------

job_details = w.jobs.get(job_id=job_id)
cluster = job_details.settings.job_clusters
instance_type = cluster[0].as_dict()['new_cluster']['node_type_id']
num_workers = cluster[0].as_dict()['new_cluster']['num_workers']
runtime_version = cluster[0].as_dict()['new_cluster']['spark_version']
engine = cluster[0].as_dict()['new_cluster']['runtime_engine']
variables = job_details.settings.tasks[0].notebook_task.base_parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last Run Statistics

# COMMAND ----------

job_runs = w.jobs.list_runs(job_id=job_id)
last_run = list(sorted(job_runs, key=lambda x: x.start_time, reverse=True))[0]
start_time = datetime.fromtimestamp(last_run.start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
duration_in_mins = last_run.run_duration / 60000 if last_run.run_duration else None
run_page_url = last_run.run_page_url
if last_run.overriding_parameters:
    overriding_params = last_run.overriding_parameters.notebook_params
else:
    overriding_params = {}

run_status = {
    "job_id": job_id,
    "run_id": last_run.run_id,
    "instance": instance_type,
    "num_workers": num_workers,
    "DBR": runtime_version,
    "engine": engine,
    "start_time": start_time,
    "duration_in_mins": duration_in_mins,
    "overriding_params": overriding_params,
}

run_status_df = spark.createDataFrame([run_status])
run_status_df.write \
    .mode('append') \
    .saveAsTable(f"{catalog}.{schema}.pdf_ingestion_benchmark_run_metadata")