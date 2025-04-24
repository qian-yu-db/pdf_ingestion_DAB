from unittest.mock import patch, MagicMock


def test_submit_offline_job():
    """Test submit_offline_job calls the appropriate Databricks API."""
    from src.pdf_ingestion.silver_streaming_task import submit_offline_job
    import src.pdf_ingestion.silver_streaming_task

    # Set up mocks
    workspace_utils = MagicMock()
    client_mock = MagicMock()
    workspace_utils.get_client.return_value = client_mock
    workspace_utils.get_job_id_by_name.return_value = 123

    run_response = MagicMock()
    run_response.run_id = 456
    client_mock.jobs.run_now.return_value = run_response

    # Test parameters
    file_path = "/path/to/large_file.pdf"
    silver_target_table = "test_catalog.test_schema.test_table"
    parsed_img_dir = "/test/img/dir"
    src.pdf_ingestion.silver_streaming_task.PARSED_IMG_DIR = parsed_img_dir
    src.pdf_ingestion.silver_streaming_task.workspace_utils = workspace_utils

    # Execute with patched globals
    with patch(
        "src.pdf_ingestion.silver_streaming_task.LARGE_FILE_PROCESSING_WORKFLOW_NAME",
        "test_workflow",
    ):
        # Call the function
        run_id = submit_offline_job(file_path, silver_target_table)

        # Verify API calls
        workspace_utils.get_job_id_by_name.assert_called_once_with("test_workflow")
        client_mock.jobs.run_now.assert_called_once_with(
            job_id=123,
            notebook_params={
                "file_path": file_path,
                "silver_target_table": silver_target_table,
                "parsed_img_dir": parsed_img_dir,
            },
        )
        assert run_id == 456
