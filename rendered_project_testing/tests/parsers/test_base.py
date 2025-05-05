import pytest
from pdf_ingestion.parsers.base import FileType, BaseParser

def test_file_type_enum():
    """Test FileType enum values and validation"""
    # Test valid file types
    assert FileType.PDF.value == "pdf"
    assert FileType.DOCX.value == "docx"
    assert FileType.PPTX.value == "pptx"
    
    # Test enum creation from string
    assert FileType("pdf") == FileType.PDF
    assert FileType("docx") == FileType.DOCX
    assert FileType("pptx") == FileType.PPTX
    
    # Test invalid file type
    with pytest.raises(ValueError):
        FileType("invalid")

def test_base_parser_interface():
    """Test that BaseParser is an abstract class"""
    # Attempt to instantiate BaseParser should fail
    with pytest.raises(TypeError):
        BaseParser() 