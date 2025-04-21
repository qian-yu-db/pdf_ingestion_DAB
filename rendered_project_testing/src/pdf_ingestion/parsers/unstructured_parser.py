from typing import List, Union, BinaryIO
from unstructured.partition.pdf import partition_pdf
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.docx import partition_docx
from markdownify import markdownify as md
from .base import BaseParser, FileType

class UnstructuredParser(BaseParser):
    """Document parser implementation using unstructured.io."""
    
    def __init__(self, **kwargs):
        """Initialize the unstructured parser.
        
        Args:
            **kwargs: Additional arguments to pass to partition functions
        """
        self.kwargs = kwargs
    
    def parse_document(self, content: Union[bytes, BinaryIO], file_type: FileType) -> str:
        if not self.supports_file_type(file_type):
            raise ValueError(f"Unsupported file type: {file_type}")
        
        # Select the appropriate partition function based on file type
        partition_func = {
            FileType.PDF: partition_pdf,
            FileType.PPTX: partition_pptx,
            FileType.DOCX: partition_docx,
        }[file_type]
        
        elements = partition_func(file=content, **self.kwargs)
        return self._process_elements(elements)
    
    def parse_document_batch(self, contents: List[Union[bytes, BinaryIO]], file_types: List[FileType]) -> List[str]:
        if len(contents) != len(file_types):
            raise ValueError("Number of contents must match number of file types")
        
        return [self.parse_document(content, file_type) 
                for content, file_type in zip(contents, file_types)]
    
    def get_parser_name(self) -> str:
        return "unstructured"
    
    def supports_file_type(self, file_type: FileType) -> bool:
        return file_type in [FileType.PDF, FileType.PPTX, FileType.DOCX]
    
    def _process_elements(self, elements):
        """Process unstructured elements into text content."""
        text_content = ""
        for section in elements:
            # Tables are parsed separately, add a \n to give the chunker a hint to split well
            if section.category == "Table":
                if section.metadata is not None and section.metadata.text_as_html is not None:
                    # Convert table to markdown
                    text_content += "\n" + md(section.metadata.text_as_html) + "\n"
                else:
                    text_content += " " + section.text
            # Other content often has too-aggressive splitting, merge the content
            else:
                text_content += " " + section.text
        return text_content 