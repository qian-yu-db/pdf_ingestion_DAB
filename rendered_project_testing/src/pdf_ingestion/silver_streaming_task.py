import argparse
import sys
import json

from pdf_ingestion.helper_utils import get_dbutils
from pdf_ingestion.pdf_processing import foreach_batch_function_silver
from pyspark.sql import SparkSession


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


def run_silver_task(
        spark: SparkSession,
        catalog: str,
        schema: str,
        volume: str,
        checkpoints_volume: str,
        table_prefix: str,
        reset_data: bool
):
    """
    Core logic for creating the Silver table for PDF Processing,
    optionally resetting data, etc.

    :param spark: SparkSession
    :param catalog: Databricks catalog name
    :param schema_name: Databricks schema name
    :param volume: Unity Catalog volume holding PDFs
    :param checkpoints_volume: Volume used for checkpoint folder
    :param table_prefix: Prefix for the Bronze table name
    :param reset_data: Whether to drop existing data & checkpoints
    """
    spark.sql(f"USE CATALOG {catalog};")
    spark.sql(f"USE SCHEMA {schema};")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {checkpoints_volume}")

    print(f"Use Unit Catalog: {catalog}")
    print(f"Use Schema: {schema}")
    print(f"Use Volume: {volume}")
    print(f"Use Checkpoint Volume: {checkpoints_volume}")
    print(f"Use Table Prefix: {table_prefix}")
    print(f"Reset Data: {reset_data}")

    job_config = {
        "file_format": "pdf",
        "checkpoint_path": f"/Volumes/{catalog}/{schema}/{checkpoints_volume}",
        "raw_files_table_name": f"{catalog}.{schema}.{table_prefix}_raw_files_foreachbatch",
        "parsed_file_table_name": f"{catalog}.{schema}.{table_prefix}_text_from_files_foreachbatch",
    }
    print("-------------------")
    print("Job Configuration")
    print("-------------------")
    print(json.dumps(job_config, indent=4))

    if reset_data:
        print(f"Delete checkpoints volume folder for {job_config['parsed_file_table_name']}...")
        checkpoint_remove_path = f"/Volumes/{catalog}/{schema}/{checkpoints_volume}/{job_config['parsed_file_table_name'].split('.')[-1]}"
        get_dbutils.fs.rm(checkpoint_remove_path, recurse=True)

        print(f"Delete tables {job_config['parsed_file_table_name']}...")
        spark.sql(f"DROP TABLE IF EXISTS {job_config['parsed_file_table_name']}")

    df_parsed_silver = (
        spark.readStream.table(job_config.get("raw_files_table_name"))
    )

    (
        df_parsed_silver.writeStream.trigger(availableNow=True)
        .option(
            "checkpointLocation",
            f"{job_config.get('checkpoint_path')}/{job_config.get('parsed_file_table_name').split('.')[-1]}",
        )
        .foreachBatch(foreach_batch_function_silver)
        .start()
    )

    print("Silver table ingestion stream has been started with availableNow=True.")

def main():
    """
    Main entrypoint: parse args, create a SparkSession, run the bronze ingestion.
    """
    args = parse_args()

    # In Databricks, spark is usually available automatically.
    # If running locally or in tests, you can create your own SparkSession:
    spark = SparkSession.builder.getOrCreate()

    run_silver_task(
        spark,
        catalog=args.catalog,
        schema=args.schema,
        volume=args.volume,
        checkpoints_volume=args.checkpoints_volume,
        table_prefix=args.table_prefix,
        reset_data=args.reset_data
    )

    print("Processing job completed.")


if __name__ == "__main__":
    main()
