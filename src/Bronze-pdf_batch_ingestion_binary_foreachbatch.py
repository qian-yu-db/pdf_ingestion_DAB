# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Table
# MAGIC
# MAGIC In this notebook we will Ingestion Raw PDF files from a Unity Catalog Volume and write to a Bronze Table using `cloudFiles`. The PDFs file will be stored as raw binary
# MAGIC
# MAGIC Use the widgets to define your custom unity catalog setup

# COMMAND ----------

# MAGIC %run ./configs

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