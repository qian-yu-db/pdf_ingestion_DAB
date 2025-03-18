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
# Example threshold in bytes: 1 GB
LARGE_FILE_THRESHOLD = 1 * 1024 * 1024 * 1024
LARGE_FILE_PROCESSING_WORKFLOW_NAME = "{{.project_name}}_async_large_file_job"

w = WorkspaceClient(host=HOSTNAME, token=TOKEN)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
import pandas as pd
import time
from functools import wraps
from concurrent.futures import ThreadPoolExecutor

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



@F.pandas_udf("string")
def process_pdf_bytes(contents: pd.Series) -> pd.Series:
    """A Pandas UDF to perform PDF parsing and text extraction with Unstructured
    OSS API.

    - Multi-theading is enabled to improve performance.
    """
    from unstructured.partition.pdf import partition_pdf
    from unstructured.partition.image import partition_image
    import pandas as pd
    import re
    import io
    from markdownify import markdownify as md

    @retry_on_failure(max_retries=5, delay=2)
    def perform_partition(raw_doc_contents_bytes):
        pdf = io.BytesIO(raw_doc_contents_bytes)
        raw_elements = partition_pdf(
            file=pdf,
            infer_table_structure=True,
            lenguages=["eng"],
            strategy="hi_res",
            extract_image_block_types=["Table", "Image"],
            extract_image_block_output_dir=PARSED_IMG_DIR,
        )

        text_content = ""
        for section in raw_elements:
            # Tables are parsed seperatly, add a \n to give the chunker a hint to split well.
            if section.category == "Table":
                if section.metadata is not None:
                    if section.metadata.text_as_html is not None:
                        # convert table to markdown
                        text_content += "\n" + md(section.metadata.text_as_html) + "\n"
                    else:
                        text_content += " " + section.text
                else:
                    text_content += " " + section.text
            # Other content often has too-aggresive splitting, merge the content
            else:
                text_content += " " + section.text

        return text_content
    worker_cpu_scale_factor = 2
    max_tp_workers = os.cpu_count() * worker_cpu_scale_factor
    with ThreadPoolExecutor(max_workers=min(8, max_tp_workers)) as executor:
        results = list(executor.map(perform_partition, contents))
    return pd.Series(results)


@F.pandas_udf(ArrayType(StringType()))
def process_pdf_bytes_as_array_type(contents: pd.Series) -> pd.Series:
    from unstructured.partition.pdf import partition_pdf
    import pandas as pd
    import io

    def perform_partition(raw_doc_contents_bytes):
        pdf = io.BytesIO(raw_doc_contents_bytes)
        raw_elements = partition_pdf(
            file=pdf,
            infer_table_structure=True,
            lenguages=["eng"],
            strategy="hi_res",
            extract_image_block_types=["Table", "Image"],
            extract_image_block_output_dir=PARSED_IMG_DIR,
        )

        text_content = ""
        for section in raw_elements:
            # Tables are parsed seperatly, add a \n to give the chunker a hint to split well.
            if section.category == "Table":
                if section.metadata is not None:
                    if section.metadata.text_as_html is not None:
                        # convert table to markdown
                        text_content += "\n" + md(section.metadata.text_as_html) + "\n"
                    else:
                        text_content += " " + section.text
                else:
                    text_content += " " + section.text
            # Other content often has too-aggresive splitting, merge the content
            else:
                text_content += " " + section.text

        return text_content

    def perform_partition_list(list_contents):
        results = []
        worker_cpu_scale_factor = 2
        max_tp_workers = os.cpu_count() * worker_cpu_scale_factor
        with ThreadPoolExecutor(max_workers=min(8, max_tp_workers)) as executor:
            results = list(executor.map(perform_partition, list_contents))
        return results

    final_results = []
    for binary_content_list in contents:
        final_results.append(perform_partition_list(binary_content_list))
    return pd.Series(final_results)

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

    # 2) For each "large" PDF, call run_now on the offline workflow
    def submit_offline_job(row):
        file_path = row["path"].replace("dbfs:/", "")
        submit_offline_job(file_path, job_config.get("parsed_file_table_name"))

    worker_cpu_scale_factor = 2
    max_tp_workers = os.cpu_count() * worker_cpu_scale_factor
    with ThreadPoolExecutor(max_workers=min(8, max_tp_workers)) as executor:
        _ = [executor.submit(submit_offline_job, row) for row in df_large.select("path").collect()]


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
