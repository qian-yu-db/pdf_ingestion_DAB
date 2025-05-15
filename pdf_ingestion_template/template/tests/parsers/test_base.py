import pytest
from pdf_ingestion.parsers.base import FileType, BaseParser

def test_file_type_enum():
    """Test FileType enum values and validation"""
    assert FileType.PDF.value == "pdf"
    assert FileType.DOCX.value == "docx"
    assert FileType.PPTX.value == "pptx"
    assert FileType.IMG.value == "img"
    assert FileType.EMAIL.value == "email"
    
    assert FileType("pdf") == FileType.PDF
    assert FileType("docx") == FileType.DOCX
    assert FileType("pptx") == FileType.PPTX
    assert FileType("img") == FileType.IMG
    assert FileType("email") == FileType.EMAIL
    
    with pytest.raises(ValueError):
        FileType("invalid")

def test_base_parser_interface_cannot_be_instantiated():
    """Test that BaseParser (ABC) cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseParser() 