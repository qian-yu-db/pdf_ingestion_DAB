from unittest.mock import MagicMock
import pyspark.sql.functions as F

from src.pdf_ingestion.bronze_streaming_task import run_bronze_task
from src.pdf_ingestion.helper_utils import JobConfig
import src.pdf_ingestion.bronze_streaming_task


def test_run_bronze_task(mocker):
    """Test run_bronze_task function using mocks."""
    # Create job configuration
    job_config = JobConfig(
        catalog="test_catalog",
        schema="test_schema",
        volume="test_volume",
        checkpoints_volume="test_checkpoints",
        table_prefix="test_prefix",
        reset_data=False,
        file_format="pdf",
    )

    # Create a completely mocked SparkSession
    mock_spark = MagicMock()
    mock_spark.sql = MagicMock()
    mock_read_stream = MagicMock()
    mock_spark.readStream = mock_read_stream

    # Mock DataFrame creation chain
    mock_format = MagicMock()
    mock_option1 = MagicMock()
    mock_option2 = MagicMock()
    mock_df = MagicMock()
    mock_read_stream.format.return_value = mock_format
    mock_format.option.return_value = mock_option1
    mock_option1.option.return_value = mock_option2
    mock_option2.load.return_value = mock_df

    # Mock withColumn operation
    mock_df_with_col = MagicMock()
    mock_df.withColumn.return_value = mock_df_with_col

    # Mock writeStream operation chain
    mock_write_stream = MagicMock()
    mock_df_with_col.writeStream = mock_write_stream
    mock_trigger = MagicMock()
    mock_option = MagicMock()
    mock_write_stream.trigger.return_value = mock_trigger
    mock_trigger.option.return_value = mock_option

    # Mock workspace utilities
    mock_workspace_utils = MagicMock()
    mock_dbutils = MagicMock()
    mock_workspace_utils.get_dbutil.return_value = mock_dbutils

    # Set module globals
    src.pdf_ingestion.bronze_streaming_task.job_config = job_config
    src.pdf_ingestion.bronze_streaming_task.workspace_utils = mock_workspace_utils

    # Mock PySpark SQL functions used in withColumn
    mock_split = mocker.patch.object(F, "split", return_value="mock_split_col")
    mock_element_at = mocker.patch.object(
        F, "element_at", return_value="mock_element_at_col"
    )

    # Run the function under test
    run_bronze_task(mock_spark)

    # Verify SQL commands
    mock_spark.sql.assert_any_call(f"USE CATALOG {job_config.catalog}")
    mock_spark.sql.assert_any_call(f"USE SCHEMA {job_config.schema}")
    mock_spark.sql.assert_any_call(
        f"CREATE VOLUME IF NOT EXISTS {job_config.checkpoints_volume}"
    )

    # Verify stream setup
    mock_read_stream.format.assert_called_with("cloudFiles")
    mock_format.option.assert_called_with("cloudFiles.format", "binaryFile")
    mock_option1.option.assert_called_with("pathGlobfilter", "*.pdf")
    mock_option2.load.assert_called_with(job_config.source_path)

    # Verify PySpark functions were called correctly
    mock_split.assert_called_once_with("path", "\\.")
    mock_element_at.assert_called_once_with("mock_split_col", -1)

    # Verify DataFrame operations
    mock_df.withColumn.assert_called_once_with("file_type", "mock_element_at_col")

    # Verify writeStream setup
    mock_write_stream.trigger.assert_called_with(availableNow=True)
    mock_trigger.option.assert_called_with(
        "checkpointLocation",
        f"{job_config.checkpoint_path}/{job_config.raw_files_table_name.split('.')[-1]}",
    )
    mock_option.toTable.assert_called_with(job_config.raw_files_table_name)

    # test when reset_data is True
    job_config.reset_data = True
    run_bronze_task(mock_spark)

    # Verify SQL commands for dropping tables
    mock_spark.sql.assert_any_call(
        f"DROP TABLE IF EXISTS {job_config.raw_files_table_name}"
    )
    # Verify checkpoint removal
    mock_dbutils.fs.rm.assert_called_with(
        f"/{job_config.checkpoint_path}/{job_config.raw_files_table_name.split('.')[-1]}",
        True,
    )
