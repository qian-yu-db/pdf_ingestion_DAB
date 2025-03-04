# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Table
# MAGIC
# MAGIC In this notebook we will Ingestion Raw PDF files from a Unity Catalog Volume and write to a Bronze Table using `cloudFiles`. The PDFs file will be stored as raw binary
# MAGIC
# MAGIC Use the widgets to define your custom unity catalog setup

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
checkpoint_vol = dbutils.widgets.get("checkpoints_volume")
table_prefix = dbutils.widgets.get("table_prefix")
reset_data = dbutils.widgets.get("reset_data") == "true"

# COMMAND ----------
import pyspark.sql.functions as F
import json

spark.sql(f"USE CATALOG {catalog};")
spark.sql(f"USE SCHEMA {schema};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {checkpoint_vol}")

print(f"Use Unit Catalog: {catalog}")
print(f"Use Schema: {schema}")
print(f"Use Volume: {volume}")
print(f"Use Checkpoint Volume: {checkpoint_vol}")
print(f"Use Table Prefix: {table_prefix}")
print(f"Reset Data: {reset_data}")

job_config = {
    "file_format": "pdf",
    "source_path": f"/Volumes/{catalog}/{schema}/{volume}",
    "checkpoint_path": f"/Volumes/{catalog}/{schema}/{checkpoint_vol}",
    "raw_files_table_name": f"{catalog}.{schema}.{table_prefix}_raw_files_foreachbatch",
}
print("-------------------")
print("Job Configuration")
print("-------------------")
print(json.dumps(job_config, indent=4))

# COMMAND ----------

if reset_data:
    print(f"Delete checkpoints volume folders for {job_config['raw_files_table_name']} ...")
    dbutils.fs.rm(f"/Volumes/{catalog}/{schema}/{checkpoint_vol}/{job_config['raw_files_table_name'].split('.')[-1]}", recurse=True)

    print(f"Delete tables {job_config['raw_files_table_name']}...")
    spark.sql(f"DROP TABLE IF EXISTS {job_config['raw_files_table_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Bronze Table: Ingest pdf File Using Databricks Autoloader

# COMMAND ----------

df_raw_bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobfilter", f"*.{job_config.get('file_format')}")
    .load(job_config.get("source_path"))
)

# COMMAND ----------

(
    df_raw_bronze.withColumn("file_type",
                              F.element_at(F.split("path", "\\."), -1))
    .writeStream
    .trigger(availableNow=True)
    .option(
        "checkpointLocation",
        f"{job_config.get('checkpoint_path')}/{job_config.get('raw_files_table_name').split('.')[-1]}",
    )
    .toTable(job_config.get("raw_files_table_name"))
)