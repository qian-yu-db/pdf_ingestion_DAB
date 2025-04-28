import argparse
import io
import os
from datetime import datetime
import logging

from markdownify import markdownify as md
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)
from unstructured.partition.pdf import partition_pdf

logging.basicConfig()
logger = logging.getLogger("large_file_async_task")
logger.setLevel(logging.INFO)


def process_single_pdf(pdf_bytes) -> str:
    """
    Parses the given PDF byte content using unstructured's `partition_pdf` and
    returns the combined text (including any table content converted to Markdown).
    :param pdf_bytes:
        The raw PDF content in bytes. For example, you could obtain this by:
            with open("/dbfs/path/to/your.pdf", "rb") as f:
                pdf_bytes = f.read()
    :return:
        The extracted textual content from the PDF, including tables converted to
        Markdown whenever possible.

    """

    # Convert the bytes into a BytesIO object for unstructured
    pdf_io = io.BytesIO(pdf_bytes)

    # 3) Partition (extract) text
    raw_elements = partition_pdf(
        file=pdf_io,
        infer_table_structure=True,
        lenguages=["eng"],
        strategy="hi_res",
        extract_image_block_types=["Table", "Image"],
        extract_image_block_output_dir=PARSED_IMG_DIR,  # or your chosen location
    )

    # Build the final text string
    text_content = ""
    for section in raw_elements:
        if section.category == "Table":
            # If the table has HTML metadata, convert it to Markdown
            if section.metadata and section.metadata.text_as_html:
                text_content += "\n" + md(section.metadata.text_as_html) + "\n"
            else:
                text_content += " " + section.text
        else:
            # Merge typical text blocks
            text_content += " " + section.text

    return text_content


def run_async_task(
    spark: SparkSession,
    file_path: str,
):
    """Performs the async task of processing a single PDF file and writing the results to a
    Silver table.

    :param spark:
    :param file_path:
    :return:
    """

    # Read the single PDF file as binary
    with open(file_path, "rb") as f:
        raw_doc_contents_bytes = f.read()

    content = process_single_pdf(raw_doc_contents_bytes)

    stats = os.stat(file_path)
    file_size = stats.st_size  # in bytes
    mod_time_epoch = stats.st_mtime  # float, seconds since epoch

    # Convert epoch seconds to a Python datetime
    mod_time = datetime.fromtimestamp(mod_time_epoch)

    silver_table_schema = StructType(
        [
            StructField("path", StringType(), True),
            StructField("modificationTime", TimestampType(), True),
            StructField("length", LongType(), True),
            StructField("text", StringType(), True),
            StructField("file_type", StringType(), True),
        ]
    )

    processed_file_data = [
        (
            file_path,
            mod_time,  # We'll rely on Spark to convert Python datetime to timestamp
            file_size,
            content,
            "pdf",  # hardcoded for now. TODO: add file type detection
        )
    ]

    silver_df = spark.createDataFrame(processed_file_data, schema=silver_table_schema)

    silver_df.write.mode("append").saveAsTable(silver_target_table)


def main():
    """
    Main entrypoint: parse args, create a SparkSession, run the bronze ingestion.
    """
    parser = argparse.ArgumentParser(
        description="Async Job to Ingest large raw PDF files append to a Silver table using "
        "Databricks Autoloader."
    )

    parser.add_argument("--file_path", required=True, help="Path to the PDF file.")
    parser.add_argument(
        "--silver_target_table", required=True, help="Path to the Silver table."
    )
    parser.add_argument(
        "--parsed_img_dir", required=True, help="Path to the image files."
    )

    args = parser.parse_args()

    file_path = args.file_path

    global PARSED_IMG_DIR, silver_target_table

    PARSED_IMG_DIR = args.parsed_img_dir
    silver_target_table = args.silver_target_table

    # In Databricks, spark is usually available automatically.
    # If running locally or in tests, you can create your own SparkSession:
    spark = SparkSession.builder.getOrCreate()

    run_async_task(spark, file_path)

    logger.info("Processing job completed.")


if __name__ == "__main__":
    main()
