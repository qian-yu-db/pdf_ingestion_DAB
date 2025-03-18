# COMMAND ----------
import io
import os
from datetime import datetime

from markdownify import markdownify as md
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)
from unstructured.partition.pdf import partition_pdf

# COMMAND ----------

# COMMAND ----------
file_path = dbutils.widgets.get("file_path")  # e.g. "/Volumes/a/b/sample.pdf"
silver_target_table = dbutils.widgets.get("silver_target_table")  # e.g. "/Volumes/a/b/sample.pdf"
parsed_img_dir = dbutils.widgets.get("parsed_img_dir")  # e.g. "/Volumes/a/b/sample.pdf"
# COMMAND ----------


# COMMAND ----------
def process_single_pdf(pdf_bytes) -> str:
    """
    Parses the given PDF byte content using unstructured's `partition_pdf` and
    returns the combined text (including any table content converted to Markdown).

    Parameters
    ----------
    pdf_bytes : bytes
        The raw PDF content in bytes. For example, you could obtain this by:
            with open("/dbfs/path/to/your.pdf", "rb") as f:
                pdf_bytes = f.read()

    Returns
    -------
    str
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
        extract_image_block_output_dir=parsed_img_dir,  # or your chosen location
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


# COMMAND ----------


# COMMAND ----------
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
        "pdf",
    )
]

silver_df = spark.createDataFrame(processed_file_data, schema=silver_table_schema)

silver_df.write.mode("append").saveAsTable(silver_target_table)
# COMMAND ----------
