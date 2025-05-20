import pytest
import sys
from unittest.mock import patch

from src.pdf_ingestion.helper_utils import (
    JobConfig,
    parse_args,
    retry_on_failure,
    DatabricksWorkspaceUtils,
)
from databricks.sdk.service.jobs import BaseJob, JobSettings


class TestJobConfig:
    def test_init_with_defaults(self):
        """Test JobConfig initialization with default values."""
        config = JobConfig(
            catalog="test_catalog",
            schema="test_schema",
            volume="test_volume",
            checkpoints_volume="test_checkpoints",
            table_prefix="test_prefix",
            reset_data=True,
        )

        assert config.catalog == "test_catalog"
        assert config.schema == "test_schema"
        assert config.volume == "test_volume"
        assert config.checkpoints_volume == "test_checkpoints"
        assert config.table_prefix == "test_prefix"
        assert config.reset_data is True
        assert config.file_format == "pdf"  # Default value

    def test_checkpoint_path_property(self):
        """Test checkpoint_path property generates correct path."""
        config = JobConfig(
            catalog="test_catalog",
            schema="test_schema",
            volume="test_volume",
            checkpoints_volume="test_checkpoints",
            table_prefix="test_prefix",
            reset_data=False,
        )

        assert config.source_path == "/Volumes/test_catalog/test_schema/test_volume/"
        assert (
            config.checkpoint_path
            == "/Volumes/test_catalog/test_schema/test_checkpoints"
        )
        assert (
            config.raw_files_table_name
            == "test_catalog.test_schema.test_prefix_raw_files_foreachbatch"
        )
        assert (
            config.parsed_files_table_name
            == "test_catalog.test_schema.test_prefix_text_from_files_foreachbatch"
        )


class TestParseArgs:
    def test_parse_args_with_defaults(self, monkeypatch):
        """Test parse_args with default values."""
        test_args = [
            "script.py",
            "--catalog",
            "test_catalog",
            "--schema",
            "test_schema",
            "--volume",
            "test_volume",
            "--checkpoints_volume",
            "test_checkpoints",
            "--table_prefix",
            "test_prefix",
        ]
        monkeypatch.setattr(sys, "argv", test_args)

        args = parse_args()

        assert args.catalog == "test_catalog"
        assert args.schema == "test_schema"
        assert args.volume == "test_volume"
        assert args.checkpoints_volume == "test_checkpoints"
        assert args.table_prefix == "test_prefix"
        assert args.reset_data is False  # Default is "false" which converts to False

    def test_parse_args_with_reset_true(self, monkeypatch):
        """Test parse_args with reset_data set to true."""
        test_args = [
            "script.py",
            "--catalog",
            "test_catalog",
            "--schema",
            "test_schema",
            "--volume",
            "test_volume",
            "--checkpoints_volume",
            "test_checkpoints",
            "--table_prefix",
            "test_prefix",
            "--reset_data",
            "true",
            "unstructured"
        ]
        monkeypatch.setattr(sys, "argv", test_args)

        args = parse_args()

        assert args.reset_data is True


class TestRetryOnFailure:
    def test_success_on_first_attempt(self):
        """Test that a function succeeding on first attempt works correctly."""

        # A simple function that works on first try
        def test_func(arg, kwarg1=None):
            return f"{arg}-{kwarg1}"

        decorated_func = retry_on_failure()(test_func)
        result = decorated_func("arg1", kwarg1="kwarg1")

        assert result == "arg1-kwarg1"

    def test_success_after_failures(self):
        """Test that a function succeeding after initial failures works correctly."""
        # Track the number of attempts
        attempts = {"count": 0}

        def fail_then_succeed():
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise ValueError(f"Failed attempt {attempts['count']}")
            return "success"

        decorated_func = retry_on_failure(max_retries=3, delay=0.01)(fail_then_succeed)
        result = decorated_func()

        assert result == "success"
        assert attempts["count"] == 3

    def test_all_attempts_fail(self):
        """Test that the decorator raises an exception if all attempts fail."""

        def always_fails():
            raise ValueError("Always fails")

        decorated_func = retry_on_failure(max_retries=2, delay=0.01)(always_fails)

        with pytest.raises(ValueError, match="Always fails"):
            decorated_func()


class TestDatabricksWorkspaceUtils:
    def test_initialization(self, spark_session, dbutils_mock):
        """Test initialization of DatabricksWorkspaceUtils with a real SparkSession."""
        with (
            patch.object(
                spark_session.conf, "get", return_value="test-workspace.databricks.com"
            ),
            patch(
                "src.pdf_ingestion.helper_utils.DatabricksWorkspaceUtils.get_dbutil",
                return_value=dbutils_mock,
            ),
        ):
            utils = DatabricksWorkspaceUtils(spark_session)

            assert utils.spark == spark_session
            assert utils.hostname == "test-workspace.databricks.com"
            assert utils.token == "mock-token"

    def test_get_client(self, spark_session, dbutils_mock):
        """Test get_client method creates a WorkspaceClient."""
        with (
            patch.object(
                spark_session.conf, "get", return_value="test-workspace.databricks.com"
            ),
            patch(
                "src.pdf_ingestion.helper_utils.DatabricksWorkspaceUtils.get_dbutil",
                return_value=dbutils_mock,
            ),
            patch("src.pdf_ingestion.helper_utils.WorkspaceClient") as mock_client,
        ):
            utils = DatabricksWorkspaceUtils(spark_session)
            client = utils.get_client()

            mock_client.assert_called_once_with(
                host="test-workspace.databricks.com", token="mock-token"
            )

    def test_get_job_id_by_name(self, spark_session, dbutils_mock):
        """Test get_job_id_by_name method returns job_id."""
        with (
            patch.object(
                spark_session.conf, "get", return_value="test-workspace.databricks.com"
            ),
            patch(
                "src.pdf_ingestion.helper_utils.DatabricksWorkspaceUtils.get_dbutil",
                return_value=dbutils_mock,
            ),
            patch("src.pdf_ingestion.helper_utils.WorkspaceClient") as mock_client,
        ):
            utils = DatabricksWorkspaceUtils(spark_session)
            mock_client_instance = mock_client.return_value
            mock_client_instance.jobs.list.return_value = [
                BaseJob(job_id=123, settings=JobSettings(name="test_workflow1")),
                BaseJob(job_id=456, settings=JobSettings(name="test_workflow2")),
            ]

            job_id = utils.get_job_id_by_name("test_workflow1")
            assert job_id == 123

            # test rase ValueError if no job found
            with pytest.raises(
                ValueError, match="No job found with the name: non_existent_job"
            ):
                utils.get_job_id_by_name("non_existent_job")
