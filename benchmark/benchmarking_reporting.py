# Databricks notebook source
# MAGIC %pip install databricks-sdk -U -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text(name="catalog", label="catalog", defaultValue="qyu_test")
dbutils.widgets.text(name="schema", label="schema", defaultValue="genai_pdf_workflow")
dbutils.widgets.text(name="job_name", label="job name", defaultValue="[dev q_yu] pdf_ingestion_benchmark")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
job_name = dbutils.widgets.get("job_name")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC # Collect Job Runs Info

# COMMAND ----------

job_list = w.jobs.list()
for job in job_list:
    if job.settings.name == job_name:
        print(f"Job ID: {job.job_id}, Name: {job.settings.name}")
        my_job_id = job.job_id
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Config

# COMMAND ----------

job_details = w.jobs.get(job_id=my_job_id)
cluster = job_details.settings.job_clusters
instance_type = cluster[0].as_dict()['new_cluster']['node_type_id']
num_workers = cluster[0].as_dict()['new_cluster']['num_workers']
runtime = cluster[0].as_dict()['new_cluster']['spark_version']
engine = cluster[0].as_dict()['new_cluster']['runtime_engine']

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the benchmark dataframe

# COMMAND ----------

def get_run_params(run):
    params = {}
    for p in run.job_parameters:
        if p.value is None:
            params[p.name] = p.default 
        else:
            params[p.name] = p.value
    return params

# COMMAND ----------

import pandas as pd

job_runs = w.jobs.list_runs(job_id=my_job_id)

data = []
for run in sorted(job_runs, key=lambda x: x.start_time, reverse=True):
    from datetime import datetime
    start_time = datetime.fromtimestamp(run.start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
    end_time = datetime.fromtimestamp(run.end_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if run.end_time else 'N/A'
    duration_mins = run.run_duration / 60000 if run.run_duration else 'N/A'
    cluster_config = run.cluster_spec
    job_params = get_run_params(run)
    data.append({
        "run_id": run.run_id,
        "instance": instance_type,
        "num_workers": num_workers,
        "DBR": runtime,
        "engine": engine,
        "result_state": run.state.result_state.value,
        "start_time": start_time,
        "end_time": end_time,
        "run_duration": duration_mins,
        **job_params
    })

df = spark.createDataFrame(data)
display(df)

# COMMAND ----------

df.filter(df.result_state == 'SUCCESS').write \
    .mode('overwrite') \
    .saveAsTable(f"{catalog}.{schema}.pdf_ingestion_benchmark")