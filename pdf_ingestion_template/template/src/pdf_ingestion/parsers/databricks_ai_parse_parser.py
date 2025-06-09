import io
import json
import logging
from typing import Any, Dict, List

# PySpark imports for Databricks AI Parse UDF
from pyspark.sql.functions import col, expr
from pyspark.sql.types import BinaryType

from .base import BaseParser, FileType

logging.basicConfig()
logger = logging.getLogger("databricks_ai_parse_parser")
logger.setLevel(logging.INFO)


class DatabricksAIParseParser(BaseParser):
    """Parser implementation using Databricks AI Parse UDF"""

    @property
    def parser_name(self) -> str:
        """Return the parser name identifier."""
        return "databricks_ai_parse"

    def __init__(self, **kwargs):
        """Initialize parser with configuration options.

        :param kwargs: Parser configuration options that will be passed to ai_parse function.
                      Common options include:
                      - malformedResponseMode: str ('PERMISSIVE' or 'FAILFAST')
                      - format: str (document format override)
        :type kwargs: dict
        :raises ImportError: If PySpark is not available
        :raises RuntimeError: If no active Spark session is found
        """

        # Supported file types for ai_parse UDF
        self.supported_types = {
            FileType.PDF: "pdf",
            FileType.IMG: "jpg", 
            FileType.IMG: "jpeg",
            FileType.IMG: "png"
        }

    def parse_document(self, **kwargs) -> str:
        """Parse a single document using Databricks ai_parse UDF.

        :param file_type: Type of the document
        :type file_type: FileType
        :returns: ai_parse expression
        :rtype: str
        :raises ValueError: If file_type is not supported
        :raises Exception: If parsing fails
        """
        file_type = kwargs.get("file_type")
        if file_type not in self.supported_types:
            raise ValueError(
                f"Unsupported file type: {file_type}. Supported types are: {list(self.supported_types.keys())}"
            )

        # Build the ai_parse expression with configuration
        ai_parse_expr = "ai_parse(content)"
        logger.info(f"ai_parse_expr: {ai_parse_expr}")

        return ai_parse_expr