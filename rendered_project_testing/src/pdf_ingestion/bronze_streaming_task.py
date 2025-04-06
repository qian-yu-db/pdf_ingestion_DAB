import json

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from .helper_utils import DatabricksWorkspaceUtils, JobConfig, parse_args


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
    spark.sql(f"USE SCHEMA {job_config.schema_name}")

    # 2) Create volume if not exists (Databricks UC command)
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {job_config.checkpoints_volume}")

    print(f"Use Unity Catalog: {job_config.catalog}")
    print(f"Use Schema: {job_config.schema}")
    print(f"Use Volume: {job_config.volume}")
    print(f"Use Checkpoint Volume: {job_config.checkpoints_volume}")
    print(f"Use Table Prefix: {job_config.table_prefix}")
    print(f"Reset Data: {job_config.reset_data}")

    print("-------------------")
    print("Job Configuration")
    print("-------------------")
    print(json.dumps(job_config, indent=4))

    # 4) If reset_data == True, remove existing table/checkpoints
    if job_config.reset_data:
        print(f"Delete checkpoints volume folder for {job_config.raw_files_table_name} ...")
        # We can remove a path using Spark APIs or dbutils:
        checkpoint_remove_path = f"/Volumes/{job_config.catalog}/{job_config.schema}/{job_config.checkpoints_volume}/{job_config.raw_files_table_name.split('.')[-1]}"
        workspace_utils.get_dbutil().fs.rm(checkpoint_remove_path, True)

        print(f"Drop table {job_config.raw_files_table_name}...")
        spark.sql(f"DROP TABLE IF EXISTS {job_config.raw_files_table_name}")

    # 5) Create a streaming DataFrame with Autoloader
    df_raw_bronze = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobfilter", f"*.{job_config.file_format}")
        .load(job_config["source_path"])
    )

    # 6) Write stream to the Bronze table
    (
        df_raw_bronze.withColumn("file_type", F.element_at(F.split("path", "\\."), -1))
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation",
                f"{job_config['checkpoint_path']}/{job_config.raw_files_table_name.split('.')[-1]}")
        .toTable(job_config.raw_files_table_name)
    )

    print("Bronze table ingestion stream has been started with availableNow=True.")


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
        reset_data=args.reset_data
    )

    run_bronze_task(
        spark
    )

    print("Ingestion job completed.")


if __name__ == "__main__":
    main()
