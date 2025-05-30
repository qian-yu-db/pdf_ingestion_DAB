import argparse
import sys
import time
from dataclasses import dataclass
from functools import wraps
import logging

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

logging.basicConfig()
logger = logging.getLogger("helper_utils")
logger.setLevel(logging.INFO)


@dataclass
class JobConfig:
    catalog: str
    schema: str
    volume: str
    checkpoints_volume: str
    table_prefix: str
    reset_data: bool
    file_format: str = "pdf"
    parser_type: str = "unstructured"

    @property
    def source_path(self):
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/"

    @property
    def checkpoint_path(self):
        return f"/Volumes/{self.catalog}/{self.schema}/{self.checkpoints_volume}"

    @property
    def raw_files_table_name(self):
        return (
            f"{self.catalog}.{self.schema}.{self.table_prefix}_raw_files_foreachbatch"
        )

    @property
    def parsed_files_table_name(self):
        return f"{self.catalog}.{self.schema}.{self.table_prefix}_text_from_files_foreachbatch"


def parse_args():
    """
    Parse command-line arguments (similar to dbutils.widgets).
    Returns an argparse.Namespace with all parameters.
    """
    parser = argparse.ArgumentParser(
        description="Ingest raw PDF files into a Bronze table using Databricks Autoloader."
    )

    parser.add_argument(
        "--catalog", required=True, help="Name of the Databricks catalog."
    )
    parser.add_argument(
        "--schema", required=True, help="Name of the Databricks schema."
    )
    parser.add_argument(
        "--volume", required=True, help="Name of the volume to read PDF files from."
    )
    parser.add_argument(
        "--checkpoints_volume",
        required=True,
        help="Name of the volume for checkpoints.",
    )
    parser.add_argument(
        "--table_prefix", required=True, help="Prefix for the raw files table."
    )
    parser.add_argument(
        "--reset_data",
        default="false",
        help="Whether to reset data (true/false). Default is 'false'.",
    )
    parser.add_argument(
        "--file_format",
        required=False,
        default="pdf",
        help="input file format."
    )
    parser.add_argument(
        "--parser_type",
        type=str,
        default="unstructured",
        choices=["unstructured"],
        help="Type of parser to use for document processing (default: unstructured)",
    )

    args = parser.parse_args(sys.argv[1:])

    # Convert reset_data to a boolean
    args.reset_data = args.reset_data.lower() == "true"

    return args


def retry_on_failure(max_retries=3, delay=1):
    """Decorator to retry a function upon failure.

    :param max_retries: Maximum number of retry attempts
    :param delay: Delay between retries in seconds
    :return: Decorated function
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
                    logger.info(f"Attempt {attempts} failed: {e}")
                    if attempts < max_retries:
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.info("All retry attempts failed.")
                        raise

        return wrapper

    return decorator


class DatabricksWorkspaceUtils:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.hostname = self.spark.conf.get("spark.databricks.workspaceUrl")
        self.token = (
            self.get_dbutil()
            .notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .apiToken()
            .get()
        )

    def get_client(self):
        """Create a Databricks workspace client."""
        logger.info("Creating Databricks workspace client.")
        return WorkspaceClient(host=self.hostname, token=self.token)

    def get_job_id_by_name(self, workflow_name: str) -> int:
        """
        Looks up the job_id for a Databricks job (workflow) by its name.
        The workflow_name should be contained within the actual job name
        (to handle DAB prefixes). Raises ValueError if no match is found.

        :param workflow_name: Name of the Databricks job (can be partial name)
        :return: job_id of the Databricks job
        """
        # TODO(jas): Look into finding the proper DAB prefix instead of using contains check.
        # This would make the workflow name matching more precise and avoid potential false matches.

        jobs_list = self.get_client().jobs.list(
            expand_tasks=False, limit=100
        )  # returns an object
        # with a `.jobs` attribute

        # Each element in jobs_list.jobs is a "Job" descriptor that includes:
        # job_id, created_time, settings, etc.
        logger.info(f"Finding job containing name: {workflow_name}")
        for job_desc in jobs_list:
            # job_desc.settings is a "JobSettings" object with a `.name` attribute
            if workflow_name in job_desc.settings.name:
                logger.info(f"Found matching job: {job_desc.settings.name} with id: {job_desc.job_id}")
                return job_desc.job_id

        raise ValueError(f"No job found containing the name: {workflow_name}")

    def get_dbutil(self):
        """Get the Databricks DBUtils instance."""

        if "local" not in str(self.spark.sparkContext.master):
            logger.info("Running in a databricks workspace.")
            try:
                import IPython

                return IPython.get_ipython().user_ns["dbutils"]
            except Exception:
                from pyspark.dbutils import DBUtils

                return DBUtils(self.spark)

        logger.info("Running in local mode, dbutils not available.")

        return None
