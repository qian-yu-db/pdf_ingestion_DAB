import json
import logging
from dataclasses import asdict

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from .helper_utils import DatabricksWorkspaceUtils, JobConfig, parse_args
from .page_count_util import get_page_count

logging.basicConfig()
logger = logging.getLogger("bronze_streaming_task")
logger.setLevel(logging.INFO)


def run_bronze_task(
    spark: SparkSession,
):
    """
    Core logic for creating the Bronze table with Autoloader,
    optionally resetting data, etc.

    :param spark: SparkSession
    """
    # 1) Switch to given catalog & schema
    spark.sql(f"USE CATALOG {job_config.catalog}")
    spark.sql(f"USE SCHEMA {job_config.schema}")

    # 2) Create volume if not exists (Databricks UC command)
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {job_config.checkpoints_volume}")
    logger.info(f"Job Target: {job_config.target}")
    logger.info(f"Use Unity Catalog: {job_config.catalog}")
    logger.info(f"Use Schema: {job_config.schema}")
    logger.info(f"Use Volume: {job_config.volume}")
    logger.info(f"Use Checkpoint Volume: {job_config.checkpoints_volume}")
    logger.info(f"Use Table Prefix: {job_config.table_prefix}")
    logger.info(f"Reset Data: {job_config.reset_data}")

    logger.info("-------------------")
    logger.info("Job Configuration")
    logger.info("-------------------")
    logger.info(json.dumps(asdict(job_config), indent=4))

    # 4) If reset_data == True, remove existing table/checkpoints
    if job_config.reset_data:
        # We can remove a path using Spark APIs or dbutils:
        checkpoint_remove_raw_path = f"{job_config.checkpoint_path}/{job_config.raw_files_table_name.split('.')[-1]}"
        checkpoint_remove_parsed_path = f"{job_config.checkpoint_path}/{job_config.parsed_files_table_name.split('.')[-1]}"

        if workspace_utils and workspace_utils.get_dbutil():
            logger.info(f"Delete checkpoints volume folder {checkpoint_remove_raw_path} ...")
            workspace_utils.get_dbutil().fs.rm(checkpoint_remove_raw_path, recurse=True)
            logger.info(f"Delete checkpoints volume folder {checkpoint_remove_parsed_path}...")
            workspace_utils.get_dbutil().fs.rm(checkpoint_remove_parsed_path, recurse=True)
        else:
            logger.warning("DBUtils not available, cannot remove checkpoint path.")

        logger.info(f"Delete table {job_config.raw_files_table_name}...")
        spark.sql(f"DROP TABLE IF EXISTS {job_config.raw_files_table_name}")
        logger.info(f"Delete table {job_config.parsed_files_table_name}...")
        spark.sql(f"DROP TABLE IF EXISTS {job_config.parsed_files_table_name}")

    # Create empty tables
    spark.sql(f"""CREATE TABLE {job_config.raw_files_table_name} (
                    path STRING,
                    modificationTime TIMESTAMP,
                    length BIGINT,
                    content BINARY,
                    file_type STRING,
                    page_count INT)
                  USING delta
                  TBLPROPERTIES (
                    'delta.enableDeletionVectors' = 'true',
                    'delta.feature.appendOnly' = 'supported',
                    'delta.feature.deletionVectors' = 'supported',
                    'delta.feature.invariants' = 'supported',
                    'delta.minReaderVersion' = '3',
                    'delta.minWriterVersion' = '7')
    """)
    spark.sql(f"""CREATE TABLE {job_config.parsed_files_table_name} (
                  path STRING,
                  modificationTime TIMESTAMP,
                  length BIGINT,
                  page_count INT,
                  text STRING,
                  file_type STRING)
                  USING delta
                  TBLPROPERTIES (
                    'delta.enableDeletionVectors' = 'true',
                    'delta.feature.appendOnly' = 'supported',
                    'delta.feature.deletionVectors' = 'supported',
                    'delta.feature.invariants' = 'supported',
                    'delta.minReaderVersion' = '3',
                    'delta.minWriterVersion' = '7')
    """)
    # 5) Create a streaming DataFrame with Autoloader
    df_raw_bronze = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobfilter", f"*.{job_config.file_format}")
        .load(job_config.source_path)
    )

    # Register UDF with two arguments
    get_page_count_udf = F.udf(get_page_count, IntegerType())

    # First extract file type, then get page count with both arguments
    df_raw_bronze_with_pages = df_raw_bronze.withColumn(
        "file_type", F.element_at(F.split("path", "\\."), -1)
    ).withColumn("page_count", get_page_count_udf(F.col("content"), F.col("file_type")))

    # 6) Write stream to the Bronze table
    (
        df_raw_bronze_with_pages.writeStream.trigger(availableNow=True)
        .option(
            "checkpointLocation",
            f"{job_config.checkpoint_path}/{job_config.raw_files_table_name.split('.')[-1]}",
        )
        .toTable(job_config.raw_files_table_name)
    )

    logger.info(
        "Bronze table ingestion stream has been started with availableNow=True."
    )


def main():
    """
    Main entrypoint: parse args, create a SparkSession, run the bronze ingestion.
    """
    args = parse_args()

    # In Databricks, spark is usually available automatically.
    # If running locally or in tests, you can create your own SparkSession:
    spark = SparkSession.builder.getOrCreate()

    global job_config, workspace_utils

    workspace_utils = DatabricksWorkspaceUtils(spark)
    job_config = JobConfig(
        catalog=args.catalog,
        schema=args.schema,
        volume=args.volume,
        checkpoints_volume=args.checkpoints_volume,
        table_prefix=args.table_prefix,
        reset_data=args.reset_data,
        file_format=args.file_format,
        parser_name=args.parser_name,
        strategy=args.strategy,
        target=args.target,
    )

    run_bronze_task(spark)

    logger.info("Ingestion job completed.")


if __name__ == "__main__":
    main()
