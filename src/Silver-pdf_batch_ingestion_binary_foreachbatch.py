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

# MAGIC %run ./configs

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

HOSTNAME = spark.conf.get('spark.databricks.workspaceUrl')
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
PARSED_IMG_DIR = f"/Volumes/{catalog}/{schema}/{volume}/parsed_images"

w = WorkspaceClient(host=HOSTNAME, token=TOKEN)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col
import pandas as pd
import time
from functools import wraps

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

@pandas_udf("string")
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
    import os

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

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

def foreach_batch_function_silver(batch_df, batch_id):
    batch_df.withColumn("text", process_pdf_bytes("content")) \
            .drop("content")    \
            .write.mode("append") \
            .saveAsTable(job_config.get("parsed_file_table_name"))

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