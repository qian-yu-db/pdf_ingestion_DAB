import argparse
import io
import os
from datetime import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    IntegerType,
)

from .helper_utils import JobConfig, retry_on_failure, write_to_table
from .parsers.factory import ParserFactory
from .parsers.base import FileType
from .page_count_util import get_page_count

logging.basicConfig()
logger = logging.getLogger("large_file_async_task")
logger.setLevel(logging.INFO)


def get_file_type(file_path: str) -> FileType:
    """Determine the file type from the file extension.

    :param file_path: Path to the file to determine type for
    :type file_path: str
    :return: The determined file type
    :rtype: FileType
    :raises ValueError: If the file extension is not supported
    """
    ext = os.path.splitext(file_path)[1].lower().lstrip(".")
    try:
        # Handle image files
        if ext in ["jpg", "jpeg", "png", "gif", "bmp", "tiff", "webp"]:
            return FileType.IMG
        # Handle email files
        if ext in ["eml", "msg"]:
            return FileType.EMAIL
        # Handle other supported formats
        return FileType(ext)
    except ValueError:
        supported_formats = ", ".join(f".{ft.value}" for ft in FileType)
        logger.warning(
            f"Unsupported file extension: '{ext}' for file {file_path}. Supported: {supported_formats}"
        )
        raise ValueError(
            f"Unsupported file extension: '{ext}'. Supported: {supported_formats}"
        )


def process_single_document(doc_bytes: bytes, file_type: FileType, parser) -> str:
    """Parse the given document byte content using the configured parser and 
    returns the combined text (including any table content converted to Markdown).

    :param doc_bytes: The raw document content in bytes
    :type doc_bytes: bytes
    :param file_type: Type of the document
    :type file_type: FileType
    :param parser: Configured parser instance
    :type parser: ParserBase
    :return: The extracted textual content from the document, including tables converted to Markdown
    :rtype: str
    """
    return parser.parse_document(doc_bytes, file_type)


def run_async_task(
    spark: SparkSession,
    file_path: str,
    parser_type: str = "unstructured",
    parsed_img_dir: str = None,
):
    """Perform the async task of processing a single document file and writing results to a Silver table.

    :param spark: SparkSession instance
    :type spark: pyspark.sql.SparkSession
    :param file_path: Path to the document file
    :type file_path: str
    :param parser_type: Type of parser to use
    :type parser_type: str
    :param parsed_img_dir: Directory for parsed images
    :type parsed_img_dir: str or None
    :raises Exception: If parser initialization fails
    """
    # Initialize parser with configuration
    parser_kwargs = {
        "infer_table_structure": True,
        "languages": ["eng"],
        "strategy": "hi_res",
        "extract_image_block_types": ["Table", "Image"],
    }
    if parsed_img_dir:
        parser_kwargs["extract_image_block_output_dir"] = parsed_img_dir

    try:
        parser = ParserFactory.get_parser(parser_type=parser_type, **parser_kwargs)
        logger.info(f"Successfully initialized parser: {parser_type}")
    except Exception as e:
        logger.error(f"Failed to initialize parser: {e}")
        raise

    # Read the document file as binary
    with open(file_path, "rb") as f:
        raw_doc_contents_bytes = f.read()

    # Get file type and process
    file_type = get_file_type(file_path)
    content = process_single_document(raw_doc_contents_bytes, file_type, parser)

    stats = os.stat(file_path)
    page_count = get_page_count(file_path, file_type.value)
    file_size = stats.st_size  # in bytes
    mod_time_epoch = stats.st_mtime  # float, seconds since epoch

    logger.info(f"Page count: {page_count}")
    logger.info(f"file_type: {file_type.value}")

    # Convert epoch seconds to a Python datetime
    mod_time = datetime.fromtimestamp(mod_time_epoch)

    silver_table_schema = StructType(
        [
            StructField("path", StringType(), True),
            StructField("modificationTime", TimestampType(), True),
            StructField("length", LongType(), True),
            StructField("page_count", IntegerType(), True),
            StructField("text", StringType(), True),
            StructField("file_type", StringType(), True),
        ]
    )

    processed_file_data = [
        (
            file_path,
            mod_time,  # We'll rely on Spark to convert Python datetime to timestamp
            file_size,
            page_count,
            content,
            file_type.value,  # Use the actual file type from FileType enum
        )
    ]

    silver_df = spark.createDataFrame(processed_file_data, schema=silver_table_schema)

    write_to_table(silver_df, silver_target_table)


def main():
    """
    Main entrypoint: parse args, create a SparkSession, run the async processing task.
    """
    parser = argparse.ArgumentParser(
        description="Async Job to process large document files and append to a Silver table."
    )

    parser.add_argument("--file_path", required=True, help="Path to the document file.")
    parser.add_argument(
        "--silver_target_table", required=True, help="Path to the Silver table."
    )
    parser.add_argument(
        "--parsed_img_dir", required=True, help="Path to the image files."
    )
    parser.add_argument(
        "--parser_type",
        type=str,
        default="unstructured",
        choices=["unstructured"],
        help="Type of parser to use for document processing (default: unstructured)",
    )

    args = parser.parse_args()

    global silver_target_table
    silver_target_table = args.silver_target_table

    # In Databricks, spark is usually available automatically.
    # If running locally or in tests, you can create your own SparkSession:
    spark = SparkSession.builder.getOrCreate()

    run_async_task(
        spark=spark,
        file_path=args.file_path,
        parser_type=args.parser_type,
        parsed_img_dir=args.parsed_img_dir,
    )

    logger.info("Processing job completed.")


if __name__ == "__main__":
    main()
