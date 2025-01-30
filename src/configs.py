# Databricks notebook source
# MAGIC %md
# MAGIC # Common Imports and Job Configurations

# COMMAND ----------

import pyspark.sql.functions as F
import json

# COMMAND ----------

# Collect Job Parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
checkpoint_vol = dbutils.widgets.get("checkpoints_volume")
table_prefix = dbutils.widgets.get("table_prefix")
reset_data = dbutils.widgets.get("reset_data") == "true"

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog};")
spark.sql(f"USE SCHEMA {schema};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {checkpoint_vol}")

print(f"Use Unit Catalog: {catalog}")
print(f"Use Schema: {schema}")
print(f"Use Volume: {volume}")
print(f"Use Checkpoint Volume: {checkpoint_vol}")
print(f"Use Table Prefix: {table_prefix}")
print(f"Reset Data: {reset_data}")

# COMMAND ----------

job_config = {
    "file_format": "pdf",
    "source_path": f"/Volumes/{catalog}/{schema}/{volume}",
    "checkpoint_path": f"/Volumes/{catalog}/{schema}/{checkpoint_vol}",
    "raw_files_table_name": f"{catalog}.{schema}.{table_prefix}_raw_files_foreachbatch",
    "parsed_file_table_name": f"{catalog}.{schema}.{table_prefix}_text_from_files_foreachbatch",
    "prepared_text_table_name": f"{catalog}.{schema}.{table_prefix}prepared_text_from_files_foreachbatch",
}
print("-------------------")
print("Job Configuration")
print("-------------------")
print(json.dumps(job_config, indent=4))