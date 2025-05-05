import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import logging

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType

from helper_utils import (
    DatabricksWorkspaceUtils,
    JobConfig,
    parse_args,
    retry_on_failure,
)
from parsers.factory import ParserFactory
from parsers.base import FileType

logging.basicConfig()
logger = logging.getLogger("silver_streaming_task")
logger.setLevel(logging.INFO)

SALTED_PANDAS_UDF_MODE = False
LARGE_FILE_THRESHOLD = int(2000) * 1024 * 1024  # MB to bytes
LARGE_FILE_PROCESSING_WORKFLOW_NAME = "async_large_file_job"

# Global variables
parser = None
PARSED_IMG_DIR = None

@F.pandas_udf("string")
def process_document_bytes(contents: pd.Series, file_paths: pd.Series) -> pd.Series:
    """A Pandas UDF to perform document parsing and text extraction.
    
    Args:
        contents: Series of document contents as bytes
        file_paths: Series of file paths corresponding to the contents
        
    Returns:
        pd.Series: Series of extracted text content
    """
    @retry_on_failure(max_retries=5, delay=2)
    def perform_partition(raw_doc_contents_bytes, file_path):
        try:
            file_type = get_file_type(file_path)
            return parser.parse_document(raw_doc_contents_bytes, file_type)
        except ValueError as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return f"ERROR: {str(e)}"
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return f"ERROR: {str(e)}"
    
    worker_cpu_scale_factor = 2
    max_tp_workers = os.cpu_count() * worker_cpu_scale_factor
    with ThreadPoolExecutor(max_workers=min(8, max_tp_workers)) as executor:
        results = list(executor.map(perform_partition, contents, file_paths))
    return pd.Series(results)

@F.pandas_udf(ArrayType(StringType()))
def process_document_bytes_as_array_type(contents: pd.Series, file_paths: pd.Series) -> pd.Series:
    """A Pandas UDF to perform batch document parsing and text extraction.
    
    Args:
        contents: Series of document contents as bytes or lists of bytes
        file_paths: Series of file paths corresponding to the contents
        
    Returns:
        pd.Series: Series of extracted text content
    """
    def perform_partition(raw_doc_contents_bytes, file_path):
        try:
            file_type = get_file_type(file_path)
            return parser.parse_document(raw_doc_contents_bytes, file_type)
        except ValueError as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return f"ERROR: {str(e)}"
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return f"ERROR: {str(e)}"
    
    def perform_partition_list(list_contents, list_file_paths):
        try:
            file_types = [get_file_type(path) for path in list_file_paths]
            return parser.parse_document_batch(list_contents, file_types)
        except ValueError as e:
            logger.error(f"Error processing batch: {str(e)}")
            return [f"ERROR: {str(e)}"] * len(list_contents)
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            return [f"ERROR: {str(e)}"] * len(list_contents)
    
    if len(contents) > 0 and isinstance(contents.iloc[0], list):
        return contents.apply(perform_partition_list, args=(file_paths,))
    return contents.apply(perform_partition, args=(file_paths,))

def get_file_type(file_path: str) -> FileType:
    """Get the file type from the file path.
    
    Args:
        file_path: Path to the file
        
    Returns:
        FileType: The type of the file
        
    Raises:
        ValueError: If the file extension is not supported
    """
    ext = os.path.splitext(file_path)[1].lower().lstrip('.')
    try:
        return FileType(ext)
    except ValueError:
        supported_formats = ", ".join(f".{ft.value}" for ft in FileType)
        raise ValueError(
            f"Unsupported file extension: .{ext}. "
            f"Supported formats are: {supported_formats}"
        )

def submit_offline_job(file_path, silver_target_table):
    """
    Calls 'run_now' on an already-deployed Workflow (job)
    passing the file_path & file_size as notebook parameters.
    That notebook must define corresponding widgets:
        dbutils.widgets.text("file_path", "")
        dbutils.widgets.text("silver_target_table", "")
    """
    job_id = workspace_utils.get_job_id_by_name(LARGE_FILE_PROCESSING_WORKFLOW_NAME)
    try:
        run_response = workspace_utils.get_client().jobs.run_now(
            job_id=job_id,
            notebook_params={
                "file_path": file_path,
                "silver_target_table": silver_target_table,
                "parsed_img_dir": PARSED_IMG_DIR,
            },
        )
        run_id = run_response.run_id
        logger.info(
            f"run_now invoked for job_id={job_id}, run_id={run_id}, file_path={file_path}"
        )
        return run_id
    except Exception as e:
        logger.error(f"Failed to run_now for job_id={job_id}: {e}")
        raise

def foreach_batch_function_silver(batch_df, batch_id):
    # 1) Split data into small vs. large based on "length" column
    df_small = batch_df.filter(F.col("length") <= LARGE_FILE_THRESHOLD)
    df_large = batch_df.filter(F.col("length") > LARGE_FILE_THRESHOLD)

    # Process "large files"
    # 2) For each "large" file, call run_now on the offline workflow
    def _submit_offline_job(file_path):
        submit_offline_job(file_path, job_config.parsed_files_table_name)

    worker_cpu_scale_factor = 2
    max_tp_workers = os.cpu_count() * worker_cpu_scale_factor
    with ThreadPoolExecutor(max_workers=min(8, max_tp_workers)) as executor:
        future_map = {
            executor.submit(_submit_offline_job, row["path"].replace("dbfs:/", "")): row
            for row in df_large.select("path").collect()
        }

    for future, file_path in future_map.items():
        try:
            run_id = future.result()
            logger.info(
                f"Successfully submitted offline job for {file_path} -> run_id={run_id}"
            )
        except Exception as e:
            logger.error(f"Failed to submit offline job for {file_path}: {e}")

    # 3) Process "small" files
    if SALTED_PANDAS_UDF_MODE:
        (
            df_small.withColumn("batch_rank", (F.floor(F.rand() * 40) + 1).cast("int"))
            .groupby("batch_rank")
            .agg(F.collect_list("content").alias("content"))
            .withColumn("text", F.explode(process_document_bytes_as_array_type("content", "path")))
            .drop("content")
            .drop("batch_rank")
            .write.mode("append")
            .saveAsTable(job_config.parsed_files_table_name)
        )
    else:
        (
            df_small.withColumn("text", process_document_bytes("content", "path"))
            .drop("content")
            .write.mode("append")
            .saveAsTable(job_config.parsed_files_table_name)
        )

def run_silver_task(
    spark: SparkSession,
):
    """
    Core logic for creating the Silver table for PDF Processing,
    optionally resetting data, etc.

    :param spark: SparkSession
    """
    spark.sql(f"USE CATALOG {job_config.catalog};")
    spark.sql(f"USE SCHEMA {job_config.schema};")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {job_config.checkpoints_volume}")

    logger.info(f"Use Unit Catalog: {job_config.catalog}")
    logger.info(f"Use Schema: {job_config.schema}")
    logger.info(f"Use Volume: {job_config.volume}")
    logger.info(f"Use Checkpoint Volume: {job_config.checkpoints_volume}")
    logger.info(f"Use Table Prefix: {job_config.table_prefix}")
    logger.info(f"Reset Data: {job_config.reset_data}")

    logger.info("-------------------")
    logger.info("Job Configuration")
    logger.info("-------------------")
    logger.info(json.dumps(asdict(job_config), indent=4))

    if job_config.reset_data:
        logger.info(
            f"Delete checkpoints volume folder for {job_config.parsed_files_table_name}..."
        )
        checkpoint_remove_path = f"/Volumes/{job_config.catalog}/{job_config.schema}/{job_config.checkpoints_volume}/{job_config.parsed_files_table_name.split('.')[-1]}"
        workspace_utils.get_dbutil().fs.rm(checkpoint_remove_path, recurse=True)

        logger.info(f"Delete tables {job_config.parsed_files_table_name}...")
        spark.sql(f"DROP TABLE IF EXISTS {job_config.parsed_files_table_name}")

    # Perform PDF Processing
    df_parsed_silver = spark.readStream.table(job_config.raw_files_table_name)

    (
        df_parsed_silver.writeStream.trigger(availableNow=True)
        .option(
            "checkpointLocation",
            f"{job_config.checkpoint_path}/{job_config.parsed_files_table_name.split('.')[-1]}",
        )
        .foreachBatch(foreach_batch_function_silver)
        .start()
    )

    logger.info("Silver table ingestion stream has been started with availableNow=True.")


def main():
    """
    Main entrypoint: parse args, create a SparkSession, run the bronze ingestion.
    """
    args = parse_args()

    # In Databricks, spark is usually available automatically.
    # If running locally or in tests, you can create your own SparkSession:
    spark = SparkSession.builder.getOrCreate()

    global PARSED_IMG_DIR, job_config, workspace_utils, parser

    PARSED_IMG_DIR = (
        f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/parsed_images/"
    )
    workspace_utils = DatabricksWorkspaceUtils(spark)
    job_config = JobConfig(
        catalog=args.catalog,
        schema=args.schema,
        volume=args.volume,
        checkpoints_volume=args.checkpoints_volume,
        table_prefix=args.table_prefix,
        reset_data=args.reset_data,
        parser_type=args.parser_type,
    )

    # Initialize parser with configuration
    parser = ParserFactory.get_parser(
        parser_type=job_config.parser_type,
        infer_table_structure=True,
        languages=["eng"],
        strategy="hi_res",
        extract_image_block_types=["Table", "Image"],
        extract_image_block_output_dir=PARSED_IMG_DIR
    )

    run_silver_task(spark)

    logger.info("Processing job completed.")


if __name__ == "__main__":
    main()
