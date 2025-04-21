from abc import ABC, abstractmethod
from typing import List, Union, BinaryIO
import pandas as pd
from enum import Enum

class FileType(Enum):
    PDF = "pdf"
    DOCX = "docx"
    PPTX = "pptx"

class BaseParser(ABC):
    """Abstract base class for document parsing implementations."""
    
    @abstractmethod
    def parse_document(self, content: Union[bytes, BinaryIO], file_type: FileType) -> str:
        """Parse a single document and return its text content.
        
        Args:
            content: Document content as bytes or file-like object
            file_type: Type of the document to parse
            
        Returns:
            str: Extracted text content from the document
        """
        pass
    
    @abstractmethod
    def parse_document_batch(self, contents: List[Union[bytes, BinaryIO]], file_types: List[FileType]) -> List[str]:
        """Parse multiple documents and return their text contents.
        
        Args:
            contents: List of document contents as bytes or file-like objects
            file_types: List of document types corresponding to the contents
            
        Returns:
            List[str]: List of extracted text contents from the documents
        """
        pass
    
    @abstractmethod
    def get_parser_name(self) -> str:
        """Get the name of the parser implementation.
        
        Returns:
            str: Name of the parser
        """
        pass
    
    @abstractmethod
    def supports_file_type(self, file_type: FileType) -> bool:
        """Check if the parser supports a specific file type.
        
        Args:
            file_type: Type of the document to check
            
        Returns:
            bool: True if the parser supports the file type, False otherwise
        """
        pass 