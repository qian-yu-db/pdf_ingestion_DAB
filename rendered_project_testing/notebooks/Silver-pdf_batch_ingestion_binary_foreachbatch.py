# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table

# COMMAND ----------

# MAGIC %pip install -U -qqq markdownify==0.12.1 "unstructured[local-inference, all-docs]==0.14.4" unstructured-client==0.22.0 nltk==3.8.1
# MAGIC %pip install databricks-sdk -U -q

# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

install_apt_get_packages(["poppler-utils", "tesseract-ocr"])

# COMMAND ----------

dbutils.library.restartPython()

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
    "checkpoint_path": f"/Volumes/{catalog}/{schema}/{checkpoint_vol}",
    "raw_files_table_name": f"{catalog}.{schema}.{table_prefix}_raw_files_foreachbatch",
    "parsed_file_table_name": f"{catalog}.{schema}.{table_prefix}_text_from_files_foreachbatch",
    "parser_name": dbutils.widgets.get("parser_name") or "unstructured",
    "parser_kwargs": {
        # Add any parser-specific configuration here
    }
}
print("-------------------")
print("Job Configuration")
print("-------------------")
print(json.dumps(job_config, indent=4))

# COMMAND ----------

if reset_data:
    print(f"Delete checkpoints volume folder for {job_config['parsed_file_table_name']}...")
    dbutils.fs.rm(f"/Volumes/{catalog}/{schema}/{checkpoint_vol}/{job_config['parsed_file_table_name'].split('.')[-1]}", recurse=True)

    print(f"Delete tables {job_config['parsed_file_table_name']}...")
    spark.sql(f"DROP TABLE IF EXISTS {job_config['parsed_file_table_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Extract Text and Tables from PDF Binary
# MAGIC
# MAGIC * Use `unstructured.io` OSS API
# MAGIC * Turn tables into markdown format for simpler downstream tasks such as: Entity Extraction with LLM, Chunking for creating knowledge base of Retriver Augmented Generation (RAG)

# COMMAND ----------

from unstructured.partition.pdf import partition_pdf
from databricks.sdk import WorkspaceClient
import pandas as pd
import os

HOSTNAME = spark.conf.get('spark.databricks.workspaceUrl')
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
PARSED_IMG_DIR = f"/Volumes/{catalog}/{schema}/{volume}/parsed_images"
SALTED_PANDAS_UDF_MODE = False
LARGE_FILE_THRESHOLD = int(2000) * 1024 * 1024  # MB to bytes
LARGE_FILE_PROCESSING_WORKFLOW_NAME = "Jas_Test_async_large_file_job"

w = WorkspaceClient(host=HOSTNAME, token=TOKEN)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
import pandas as pd
import time
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from pdf_ingestion.parsers.factory import ParserFactory

def retry_on_failure(max_retries=3, delay=1):
    """
    Decorator to retry a function upon failure.

    Parameters:
    - max_retries (int): Maximum number of retry attempts.
    - delay (int): Delay in seconds between retries.

    Returns:
    - function: Wrapped function with retry logic.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    print(f"Attempt {attempts} failed: {e}")
                    if attempts < max_retries:
                        print(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        print("All retry attempts failed.")
                        raise
        return wrapper
    return decorator

# Initialize parser
parser = ParserFactory.get_parser(
    job_config["parser_name"],
    **job_config["parser_kwargs"]
)

@F.pandas_udf("string")
def process_pdf_bytes(contents: pd.Series) -> pd.Series:
    @retry_on_failure(max_retries=5, delay=2)
    def perform_partition(raw_doc_contents_bytes):
        return parser.parse_pdf(raw_doc_contents_bytes)
    
    return contents.apply(perform_partition)

@F.pandas_udf(ArrayType(StringType()))
def process_pdf_bytes_as_array_type(contents: pd.Series) -> pd.Series:
    def perform_partition(raw_doc_contents_bytes):
        return parser.parse_pdf(raw_doc_contents_bytes)
    
    def perform_partition_list(list_contents):
        return parser.parse_pdf_batch(list_contents)
    
    if len(contents) > 0 and isinstance(contents.iloc[0], list):
        return contents.apply(perform_partition_list)
    return contents.apply(perform_partition)

# COMMAND ----------

def get_job_id_by_name(workflow_name: str) -> int:
    """
    Looks up the job_id for a Databricks job (workflow) by its name.
    Raises ValueError if no match is found.
    """
    jobs_list = w.jobs.list(expand_tasks=False, limit=100)  # returns an object with a `.jobs` attribute

    # Each element in jobs_list.jobs is a "Job" descriptor that includes:
    # job_id, created_time, settings, etc.
    for job_desc in jobs_list:
        # job_desc.settings is a "JobSettings" object with a `.name` attribute
        if job_desc.settings.name == workflow_name:
            return job_desc.job_id

    raise ValueError(f"No job found with the name: {workflow_name}")

def submit_offline_job(file_path, silver_target_table):
    """
    Calls 'run_now' on an already-deployed Workflow (job)
    passing the file_path & file_size as notebook parameters.
    That notebook must define corresponding widgets:
        dbutils.widgets.text("file_path", "")
        dbutils.widgets.text("silver_target_table", "")
    """
    try:
        job_id = get_job_id_by_name(LARGE_FILE_PROCESSING_WORKFLOW_NAME)

        run_response = w.jobs.run_now(
            job_id=job_id,
            notebook_params={
                "file_path": file_path,
                "silver_target_table": silver_target_table,
                "parsed_img_dir": PARSED_IMG_DIR,
            }
        )
        run_id = run_response.run_id
        print(f"run_now invoked for job_id={job_id}, run_id={run_id}, file_path={file_path}")
        return run_id
    except Exception as e:
        print(f"Failed to run_now for job_id={job_id}: {e}")
        raise


def foreach_batch_function_silver(batch_df, batch_id):
    # 1) Split data into small vs. large based on "length" column
    df_small = batch_df.filter(F.col("length") <= LARGE_FILE_THRESHOLD)
    df_large = batch_df.filter(F.col("length") > LARGE_FILE_THRESHOLD)

    # Process "large files"
    # 2) For each "large" PDF, call run_now on the offline workflow
    def submit_offline_job(file_path):
        submit_offline_job(file_path, job_config.get("parsed_file_table_name"))

    worker_cpu_scale_factor = 2
    max_tp_workers = os.cpu_count() * worker_cpu_scale_factor
    with ThreadPoolExecutor(max_workers=min(8, max_tp_workers)) as executor:
        future_map = {executor.submit(submit_offline_job, row["path"].replace("dbfs:/", "")): row for row in df_large.select("path").collect()}

    for future, file_path in future_map.items():
        try:
            run_id = future.result()
            print(f"Successfully submitted offline job for {file_path} -> run_id={run_id}")
        except Exception as e:
            print(f"Failed to submit offline job for {file_path}: {e}")


    # 3) Process "small" files
    if SALTED_PANDAS_UDF_MODE:
        (
            df_small.withColumn("batch_rank", (F.floor(F.rand() * 40) + 1).cast("int"))
            .groupby("batch_rank")
            .agg(F.collect_list("content").alias("content"))
            .withColumn("text", F.explode(process_pdf_bytes_as_array_type("content")))
            .drop("content")
            .drop("batch_rank")
            .write.mode("append")
            .saveAsTable(job_config.get("parsed_file_table_name"))
        )
    else:
        (
            df_small.withColumn("text", process_pdf_bytes("content"))
            .drop("content")
            .write.mode("append")
            .saveAsTable(job_config.get("parsed_file_table_name"))
        )

# COMMAND ----------

df_parsed_silver = (
    spark.readStream.table(job_config.get("raw_files_table_name"))
)

# COMMAND ----------

(
    df_parsed_silver.writeStream.trigger(availableNow=True)
    .option(
        "checkpointLocation",
        f"{job_config.get('checkpoint_path')}/{job_config.get('parsed_file_table_name').split('.')[-1]}",
    )
    .foreachBatch(foreach_batch_function_silver)
    .start()
)
