import io
from typing import List, Dict, Any
from markdownify import markdownify as md

# All partition functions accept either:
# - file: file-like object (BytesIO, file handle, etc.)
# - filename: str path to file
# We use file=BytesIO(content) to handle in-memory bytes
from unstructured.partition.pdf import partition_pdf
from unstructured.partition.docx import partition_docx
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.image import partition_image
from unstructured.partition.email import partition_email

from .base import BaseParser, FileType

class UnstructuredParser(BaseParser):
    """Parser implementation using the unstructured library"""
    
    def __init__(self, **kwargs):
        """Initialize parser with configuration options.
        
        Args:
            **kwargs: Parser configuration options that will be passed to partition functions.
                     Common options include:
                     - infer_table_structure: bool
                     - languages: List[str]
                     - strategy: str
                     - extract_image_block_types: List[str]
                     - extract_image_block_output_dir: str
        """
        self.config = kwargs
        self.partition_func = {
            FileType.PDF: partition_pdf,
            FileType.DOCX: partition_docx,
            FileType.PPTX: partition_pptx,
            FileType.IMG: partition_image,
            FileType.EMAIL: partition_email
        }
    
    def parse_document(self, content: bytes, file_type: FileType) -> str:
        """Parse a single document using unstructured library.
        
        Args:
            content: Document content as bytes
            file_type: Type of the document
            
        Returns:
            str: Extracted text content
            
        Note:
            All partition functions accept either a file-like object or filename.
            We use BytesIO to create a file-like object from the bytes content.
        """
        if file_type not in self.partition_func:
            raise ValueError(f"Unsupported file type: {file_type}")
            
        # Create a file-like object from bytes for the partition function
        file_obj = io.BytesIO(content)
        
        # All partition functions accept file=file_obj and additional kwargs
        elements = self.partition_func[file_type](
            file=file_obj,  # Pass as file-like object
            **self.config    # Pass any additional configuration
        )
        
        return self._process_elements(elements)
    
    def parse_document_batch(self, contents: List[bytes], 
                           file_types: List[FileType]) -> List[str]:
        """Parse multiple documents in batch.
        
        Args:
            contents: List of document contents as bytes
            file_types: List of document types
            
        Returns:
            List[str]: List of extracted text content
        """
        return [self.parse_document(content, file_type) 
                for content, file_type in zip(contents, file_types)]
    
    def _process_elements(self, elements) -> str:
        """Process parsed elements into text content.
        
        Args:
            elements: List of parsed elements
            
        Returns:
            str: Processed text content
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