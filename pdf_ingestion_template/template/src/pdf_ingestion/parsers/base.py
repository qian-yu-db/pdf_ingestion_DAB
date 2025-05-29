from abc import ABC, abstractmethod
from enum import Enum
from typing import List


class FileType(Enum):
    """Supported document formats"""

    PDF = "pdf"
    DOCX = "docx"
    PPTX = "pptx"
    IMG = "img"  # For image files (jpg, png, etc.)
    EMAIL = "email"  # For email files (eml, msg)
    XLSX = "xlsx"


class BaseParser(ABC):
    """Base interface for document parsers"""

    @abstractmethod
    def parse_document(self, content: bytes, file_type: FileType) -> str:
        """Parse a single document and return extracted text.

        Args:
            content: Document content as bytes
            file_type: Type of the document

        Returns:
            str: Extracted text content
        """
        pass

    @abstractmethod
    def parse_document_batch(
        self, contents: List[bytes], file_types: List[FileType]
    ) -> List[str]:
        """Parse multiple documents in batch.

        Args:
            contents: List of document contents as bytes
            file_types: List of document types

        Returns:
            List[str]: List of extracted text content
        """
        pass
