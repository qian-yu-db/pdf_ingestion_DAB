import argparse
import io
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    BinaryType,
)

from .helper_utils import JobConfig, get_file_type_from_path, retry_on_failure, write_to_table
from .page_count_util import get_page_count
from .parsers.base import FileType
from .parsers.factory import ParserFactory

logging.basicConfig()
logger = logging.getLogger("large_file_async_task")
logger.setLevel(logging.INFO)


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
    return parser.parse_document(content=doc_bytes, file_type=file_type)


def process_single_document_dataframe(
    doc_df: DataFrame, file_type: FileType, parser
) -> DataFrame:
    """Parse the given document byte content using the configured parser and
    returns the combined text (including any table content converted to Markdown).

    :param doc_df: The raw document content in bytes
    :type doc_df: DataFrame
    :param file_type: Type of the document
    :type file_type: FileType
    :param parser: Configured parser instance
    :type parser: ParserBase
    :return: The extracted textual content from the document, including tables converted to Markdown
    :rtype: str
    """
    assert parser.parse_document(file_type=file_type) == "ai_parse(content)"
    df_pages = (
        doc_df.select(
            "path",
            "modificationTime",
            "length",
            "page_count",
            "file_type",
            F.expr('ai_parse(content)').alias("parsed"),
        )
        .withColumn("parsed_json", F.parse_json(F.col("parsed").cast("string")))
        .select(
            "path",
            "modificationTime",
            "length",
            "page_count",
            "file_type",
            F.expr("parsed_json:pages").alias("pages"),
        )
    )
    df_pages_explode = df_pages.select(
        "path",
        "modificationTime",
        "length",
        "page_count",
        "file_type",
        F.explode(F.col("pages").cast("array<string>")).alias("pages_items"),
    ).withColumn(
        "markdown",
        F.variant_get(
            F.parse_json("pages_items"), "$.representation.markdown", "string"
        ),
    )
    processed_file_data = df_pages_explode.groupBy(
        "path", "modificationTime", "length", "page_count", "file_type"
    ).agg(F.concat_ws("\n", F.collect_list("markdown")).alias("text"))
    return processed_file_data


def run_async_task(
    spark: SparkSession,
    file_path: str,
    parser_name: str = "unstructured",
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
        parser = ParserFactory.get_parser(parser_name=parser_name, **parser_kwargs)
        logger.info(f"Successfully initialized parser: {parser_name}")
    except Exception as e:
        logger.error(f"Failed to initialize parser: {e}")
        raise

    # Read the document file as binary
    with open(file_path, "rb") as f:
        raw_doc_contents_bytes = f.read()

    # Get file type and process
    file_type = get_file_type_from_path(file_path)

    stats = os.stat(file_path)
    page_count = get_page_count(file_path, file_type.value)
    file_size = stats.st_size  # in bytes
    mod_time_epoch = stats.st_mtime  # float, seconds since epoch

    logger.info(f"Page count: {page_count}")
    logger.info(f"file_type: {file_type.value}")

    # Convert epoch seconds to a Python datetime
    mod_time = datetime.fromtimestamp(mod_time_epoch)

    if parser_name == "unstructured":
        content = process_single_document(raw_doc_contents_bytes, file_type, parser)
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
        silver_df = spark.createDataFrame(processed_file_data, schema=silver_table_schema)
    elif parser_name == "databricks_ai_parse":
        bronze_table_schema = StructType(
            [
                StructField("path", StringType(), True),
                StructField("modificationTime", TimestampType(), True),
                StructField("length", LongType(), True),
                StructField("page_count", IntegerType(), True),
                StructField("content", BinaryType(), True),
                StructField("file_type", StringType(), True),
            ]
        )
        file_data = [
            (
                file_path,
                mod_time,  # We'll rely on Spark to convert Python datetime to timestamp
                file_size,
                page_count,
                raw_doc_contents_bytes,
                file_type.value,  # Use the actual file type from FileType enum
            )
        ]
        df_file_data = spark.createDataFrame(file_data, schema=bronze_table_schema)
        silver_df = process_single_document_dataframe(
            df_file_data, file_type, parser
        )
    else:
        raise ValueError(f"Unsupported parser name: {parser_name}")


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
        "--parser_name",
        type=str,
        default="unstructured",
        choices=["unstructured", "databricks_ai_parse"],
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
        parser_name=args.parser_name,
        parsed_img_dir=args.parsed_img_dir,
    )

    logger.info("Processing job completed.")


if __name__ == "__main__":
    main()
