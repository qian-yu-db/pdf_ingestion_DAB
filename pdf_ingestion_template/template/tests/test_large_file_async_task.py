from unittest.mock import patch, MagicMock, mock_open

from src.pdf_ingestion.large_file_async_task import process_single_pdf, run_async_task
import src.pdf_ingestion.large_file_async_task


def test_process_single_pdf():
    """Test process_single_pdf extracts text from PDF content."""
    # Mock PDF bytes
    pdf_bytes = b"mock PDF content"

    # Create mock elements from unstructured partition_pdf
    class MockElement:
        def __init__(self, category, text, metadata=None):
            self.category = category
            self.text = text
            self.metadata = metadata

    class MockMetadata:
        def __init__(self, text_as_html=None):
            self.text_as_html = text_as_html

    # Example elements that would be returned by partition_pdf
    mock_elements = [
        MockElement("Text", "This is regular text content."),
        MockElement("Table", "| Cell data |\n|-----------|"),
    ]

    # Mock the partition_pdf function to return our mock elements
    with (
        patch(
            "src.pdf_ingestion.large_file_async_task.partition_pdf",
            return_value=mock_elements,
        ),
        patch(
            "src.pdf_ingestion.large_file_async_task.md",
            return_value="# Markdown Table",
        ),
    ):
        # Run the function
        src.pdf_ingestion.large_file_async_task.PARSED_IMG_DIR = "/mock/parsed/img/"
        result = process_single_pdf(pdf_bytes)
        print(f"result: {result}")

        # Verify results contain expected content
        assert "This is regular text content." in result
        assert result == " " + "This is regular text content." + " " + (
            "| Cell data |\n|-----------|"
        )


def test_run_async_task(spark_session, mocker):
    """Test run_async_task using a real SparkSession but mock file operations."""
    # Setup test parameters
    file_path = "/path/to/large_file.pdf"
    file_content = b"test PDF content"
    processed_content = "Extracted PDF text"

    # Mock file metadata
    mock_stat = MagicMock()
    mock_stat.st_size = 12345
    mock_stat.st_mtime = 1617548400.0  # April 4, 2021 in epoch time

    # Mock DataFrame operations
    mock_df = MagicMock()
    mock_write = MagicMock()
    mock_mode = MagicMock()

    # Set up the chain of mocks
    mock_df.write = mock_write
    mock_write.mode.return_value = mock_mode

    # Mock createDataFrame to return our mock DataFrame
    mocker.patch.object(spark_session, "createDataFrame", return_value=mock_df)
    src.pdf_ingestion.large_file_async_task.silver_target_table = (
        "test_catalog.test_schema.silver_table"
    )

    # Patch dependencies
    with (
        patch("builtins.open", mock_open(read_data=file_content)),
        patch(
            "src.pdf_ingestion.large_file_async_task.process_single_pdf",
            return_value=processed_content,
        ),
        patch("os.stat", return_value=mock_stat),
    ):
        # Run the function
        run_async_task(spark_session, file_path)

        # Verify DataFrame was created with correct data
        spark_session.createDataFrame.assert_called_once()

        # Verify write operations
        mock_write.mode.assert_called_once_with("append")
        mock_mode.saveAsTable.assert_called_once_with(
            "test_catalog.test_schema.silver_table"
        )


def test_main(mocker, spark_session):
    """Test the main function with simulated command-line arguments."""
    # Mock command-line arguments
    test_file_path = "/path/to/test.pdf"
    test_silver_table = "catalog.schema.table"
    test_img_dir = "/path/for/images"

    mocker.patch(
        "sys.argv",
        [
            "large_file_async_task.py",
            "--file_path",
            test_file_path,
            "--silver_target_table",
            test_silver_table,
            "--parsed_img_dir",
            test_img_dir,
        ],
    )

    # Mock the SparkSession builder
    mocker.patch(
        "pyspark.sql.SparkSession.builder.getOrCreate", return_value=spark_session
    )

    # Mock run_async_task
    mock_run_async = mocker.patch(
        "src.pdf_ingestion.large_file_async_task.run_async_task"
    )

    # Run the main function
    src.pdf_ingestion.large_file_async_task.main()

    # Verify global variables are set correctly
    assert src.pdf_ingestion.large_file_async_task.PARSED_IMG_DIR == test_img_dir
    assert (
        src.pdf_ingestion.large_file_async_task.silver_target_table == test_silver_table
    )

    # Verify run_async_task was called with correct arguments
    mock_run_async.assert_called_once_with(spark_session, test_file_path)
