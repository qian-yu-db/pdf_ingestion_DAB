import argparse
import sys
import json

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from pdf_ingestion.helper_utils import get_dbutils


def parse_args():
    """
    Parse command-line arguments (similar to dbutils.widgets).
    Returns an argparse.Namespace with all parameters.
    """
    parser = argparse.ArgumentParser(
        description="Ingest raw PDF files into a Bronze table using Databricks Autoloader."
    )

    parser.add_argument("--catalog", required=True, help="Name of the Databricks catalog.")
    parser.add_argument("--schema", required=True, help="Name of the Databricks schema.")
    parser.add_argument("--volume", required=True, help="Name of the volume to read PDF files from.")
    parser.add_argument("--checkpoints_volume", required=True, help="Name of the volume for checkpoints.")
    parser.add_argument("--table_prefix", required=True, help="Prefix for the raw files table.")
    parser.add_argument("--reset_data", default="false",
                        help="Whether to reset data (true/false). Default is 'false'.")

    args = parser.parse_args(sys.argv[1:])

    # Convert reset_data to a boolean
    args.reset_data = (args.reset_data.lower() == "true")

    return args


def run_bronze_task(
        spark: SparkSession,
        catalog: str,
        schema_name: str,
        volume: str,
        checkpoints_volume: str,
        table_prefix: str,
        reset_data: bool
):
    """
    Core logic for creating the Bronze table with Autoloader,
    optionally resetting data, etc.

    :param spark: SparkSession
    :param catalog: Databricks catalog name
    :param schema_name: Databricks schema name
    :param volume: Unity Catalog volume holding PDFs
    :param checkpoints_volume: Volume used for checkpoint folder
    :param table_prefix: Prefix for the Bronze table name
    :param reset_data: Whether to drop existing data & checkpoints
    """
    # 1) Switch to given catalog & schema
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema_name}")

    # 2) Create volume if not exists (Databricks UC command)
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {checkpoints_volume}")

    print(f"Use Unity Catalog: {catalog}")
    print(f"Use Schema: {schema_name}")
    print(f"Use Volume: {volume}")
    print(f"Use Checkpoint Volume: {checkpoints_volume}")
    print(f"Use Table Prefix: {table_prefix}")
    print(f"Reset Data: {reset_data}")

    # 3) Build job configuration
    raw_files_table_name = f"{catalog}.{schema_name}.{table_prefix}_raw_files_foreachbatch"
    job_config = {
        "file_format": "pdf",
        "source_path": f"/Volumes/{catalog}/{schema_name}/{volume}",
        "checkpoint_path": f"/Volumes/{catalog}/{schema_name}/{checkpoints_volume}",
        "raw_files_table_name": raw_files_table_name,
    }
    print("-------------------")
    print("Job Configuration")
    print("-------------------")
    print(json.dumps(job_config, indent=4))

    # 4) If reset_data == True, remove existing table/checkpoints
    if reset_data:
        print(f"Delete checkpoints volume folder for {raw_files_table_name} ...")
        # We can remove a path using Spark APIs or dbutils:
        checkpoint_remove_path = f"/Volumes/{catalog}/{schema_name}/{checkpoints_volume}/{raw_files_table_name.split('.')[-1]}"
        get_dbutils.fs.rm(checkpoint_remove_path, True)

        print(f"Drop table {raw_files_table_name}...")
        spark.sql(f"DROP TABLE IF EXISTS {raw_files_table_name}")

    # 5) Create a streaming DataFrame with Autoloader
    df_raw_bronze = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobfilter", f"*.{job_config.get('file_format')}")
        .load(job_config["source_path"])
    )

    # 6) Write stream to the Bronze table
    (
        df_raw_bronze.withColumn("file_type", F.element_at(F.split("path", "\\."), -1))
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation",
                f"{job_config['checkpoint_path']}/{raw_files_table_name.split('.')[-1]}")
        .toTable(raw_files_table_name)
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

    run_bronze_task(
        spark,
        catalog=args.catalog,
        schema_name=args.schema,
        volume=args.volume,
        checkpoints_volume=args.checkpoints_volume,
        table_prefix=args.table_prefix,
        reset_data=args.reset_data
    )

    print("Ingestion job completed.")


if __name__ == "__main__":
    main()
