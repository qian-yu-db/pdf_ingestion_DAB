import io
import logging
from typing import Any, Dict, List

from markdownify import markdownify as md
from unstructured.partition.docx import partition_docx
from unstructured.partition.email import partition_email
from unstructured.partition.image import partition_image

# All partition functions accept either:
# - file: file-like object (BytesIO, file handle, etc.)
# - filename: str path to file
# We use file=BytesIO(content) to handle in-memory bytes
from unstructured.partition.pdf import partition_pdf
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.xlsx import partition_xlsx

from .base import BaseParser, FileType

logging.basicConfig()
logger = logging.getLogger("unstructured_parser")
logger.setLevel(logging.INFO)


class UnstructuredParser(BaseParser):
    """Parser implementation using the unstructured library.
    
    This parser uses the unstructured library to extract text and tables
    from various document formats including PDF, DOCX, PPTX, images, 
    emails, and spreadsheets.
    """

    @property
    def parser_name(self) -> str:
        """Return the parser name identifier.
        
        :returns: The parser name identifier
        :rtype: str
        """
        return "unstructured"

    def __init__(self, **kwargs):
        """Initialize parser with configuration options.

        :param kwargs: Parser configuration options that will be passed to partition functions
        :type kwargs: dict
        :keyword infer_table_structure: Whether to infer table structure
        :type infer_table_structure: bool
        :keyword languages: List of languages for OCR
        :type languages: List[str]
        :keyword strategy: Processing strategy ('auto', 'hi_res', 'ocr_only')
        :type strategy: str
        :keyword extract_image_block_types: Types of blocks to extract as images
        :type extract_image_block_types: List[str]
        :keyword extract_image_block_output_dir: Directory for extracted images
        :type extract_image_block_output_dir: str
        """
        self.config = kwargs
        self.partition_func = {
            FileType.PDF: partition_pdf,
            FileType.DOCX: partition_docx,
            FileType.PPTX: partition_pptx,
            FileType.IMG: partition_image,
            FileType.EMAIL: partition_email,
            FileType.XLSX: partition_xlsx,
        }

    def parse_document(self, **kwargs) -> str:
        """Parse a single document using unstructured library.

        :param kwargs: Parser-specific keyword arguments
        :type kwargs: dict
        :keyword content: Document content as bytes
        :type content: bytes
        :keyword file_type: Type of the document
        :type file_type: FileType
        :returns: Extracted text content from the document
        :rtype: str
        :raises ValueError: If file_type is not supported or required kwargs are missing
        :raises KeyError: If required parameters are missing from kwargs
        
        .. note::
           All partition functions accept either a file-like object or filename.
           We use BytesIO to create a file-like object from the bytes content.
        """
        # Extract required parameters from kwargs
        content = kwargs.get('content')
        file_type = kwargs.get('file_type')
        
        if content is None:
            raise KeyError("'content' is required in kwargs")
        if file_type is None:
            raise KeyError("'file_type' is required in kwargs")
            
        if file_type not in self.partition_func:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Create a file-like object from bytes for the partition function
        file_obj = io.BytesIO(content)

        # All partition functions accept file=file_obj and additional kwargs
        elements = self.partition_func[file_type](
            file=file_obj,  # Pass as file-like object
            **self.config,  # Pass any additional configuration
        )
        logger.info(
            f"unstructured parser function '{self.partition_func[file_type]}' used for file type {file_type}"
        )
        return self._process_elements(elements)

    def parse_document_batch(
        self, contents: List[bytes], file_types: List[FileType]
    ) -> List[str]:
        """Parse multiple documents in batch.

        This implementation overrides the default to provide the same
        individual processing behavior but with proper kwargs handling.

        :param contents: List of document contents as bytes
        :type contents: List[bytes]
        :param file_types: List of document types corresponding to contents
        :type file_types: List[FileType]
        :returns: List of extracted text content from each document
        :rtype: List[str]
        :raises ValueError: If contents and file_types lists have different lengths
        """
        return [
            self.parse_document(content, file_type)
            for content, file_type in zip(contents, file_types)
        ]

    def _process_elements(self, elements) -> str:
        """Process parsed elements into text content.

        :param elements: List of parsed elements from unstructured
        :type elements: List
        :returns: Processed text content with tables converted to markdown
        :rtype: str
        
        .. note::
           Tables are converted to markdown format using markdownify.
           Other elements are concatenated as plain text.
        """
        text_content = ""
        for element in elements:
            if element.category == "Table":
                if element.metadata and element.metadata.text_as_html:
                    # Convert table to markdown
                    text_content += "\n" + md(element.metadata.text_as_html) + "\n"
                else:
                    text_content += " " + element.text
            else:
                text_content += " " + element.text
        return text_content
