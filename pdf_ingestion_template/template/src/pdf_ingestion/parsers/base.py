from abc import ABC, abstractmethod
from enum import Enum
from typing import List


class FileType(Enum):
    """Supported document formats for parsing.
    
    This enum defines all the file types that can be processed
    by the document parsing system.
    """

    PDF = "pdf"
    DOCX = "docx"
    PPTX = "pptx"
    IMG = "img"  # For image files (jpg, png, etc.)
    EMAIL = "email"  # For email files (eml, msg)
    XLSX = "xlsx"


class BaseParser(ABC):
    """Base interface for document parsers.
    
    This abstract base class defines the contract that all document
    parsers must implement. It provides both single document and
    batch processing capabilities.
    """

    @property
    @abstractmethod
    def parser_name(self) -> str:
        """Return the name/identifier of this parser implementation.

        :returns: The parser name/identifier
        :rtype: str
        """
        pass

    @abstractmethod
    def parse_document(self, **kwargs) -> str:
        """Parse a single document and return extracted text.

        :param kwargs: Parser-specific keyword arguments
        :type kwargs: dict
        :returns: Extracted text content from the document
        :rtype: str
        :raises ValueError: If required parameters are missing or invalid
        :raises NotImplementedError: If the parser doesn't support the file type
        
        .. note::
           Different parsers may require different keyword arguments.
           Check the specific parser implementation for required parameters.
        """
        pass

    def parse_document_batch(
        self, contents: List[bytes], file_types: List[FileType]
    ) -> List[str]:
        """Parse multiple documents in batch.

        Default implementation processes documents individually using
        :meth:`parse_document`. Override this method for optimized
        batch processing.

        :param contents: List of document contents as bytes
        :type contents: List[bytes]
        :param file_types: List of document types corresponding to contents
        :type file_types: List[FileType]
        :returns: List of extracted text content from each document
        :rtype: List[str]
        :raises ValueError: If contents and file_types lists have different lengths
        :raises NotImplementedError: If the parser doesn't support batch processing
        
        .. note::
           The default implementation calls :meth:`parse_document` for each
           document individually. For better performance with large batches,
           override this method with optimized batch processing logic.
           
        """
        return [
            self.parse_document(content=content, file_type=file_type)
            for content, file_type in zip(contents, file_types)
        ]
