from unstructured.partition.pdf import partition_pdf
from databricks.sdk import WorkspaceClient
import pandas as pd
import os
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
import pandas as pd
import time
from functools import wraps
from concurrent.futures import ThreadPoolExecutor


PARSED_IMG_DIR = f"/Volumes/{catalog}/{schema}/{volume}/parsed_images"
SALTED_PANDAS_UDF_MODE = False
LARGE_FILE_THRESHOLD = int(2000) * 1024 * 1024  # MB to bytes
LARGE_FILE_PROCESSING_WORKFLOW_NAME = "Jas_Test_async_large_file_job"


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
