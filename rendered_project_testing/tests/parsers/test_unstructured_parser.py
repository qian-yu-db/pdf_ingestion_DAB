import io
import pytest
from unittest.mock import Mock, patch
from pdf_ingestion.parsers.unstructured_parser import UnstructuredParser
from pdf_ingestion.parsers.base import FileType

@pytest.fixture
def mock_partition_func():
    """Mock partition function that returns test elements"""
    def create_mock_element(text, category="Text", metadata=None):
        element = Mock()
        element.text = text
        element.category = category
        element.metadata = metadata
        return element
    
    def mock_partition(*args, **kwargs):
        return [
            create_mock_element("Regular text"),
            create_mock_element(
                "Table text",
                category="Table",
                metadata=Mock(text_as_html="<table><tr><td>Test</td></tr></table>")
            ),
            create_mock_element("More text")
        ]
    
    return mock_partition

@pytest.fixture
def parser(mock_partition_func):
    """Create parser instance with mocked partition functions"""
    with patch('unstructured.partition.pdf.partition_pdf', mock_partition_func), \
         patch('unstructured.partition.docx.partition_docx', mock_partition_func), \
         patch('unstructured.partition.pptx.partition_pptx', mock_partition_func):
        return UnstructuredParser(
            infer_table_structure=True,
            languages=["eng"],
            strategy="hi_res"
        )

def test_parser_initialization():
    """Test parser initialization with configuration"""
    parser = UnstructuredParser(
        infer_table_structure=True,
        languages=["eng"],
        strategy="hi_res"
    )
    assert parser.config["infer_table_structure"] is True
    assert parser.config["languages"] == ["eng"]
    assert parser.config["strategy"] == "hi_res"

def test_parse_document_pdf(parser):
    """Test parsing a PDF document"""
    content = b"test pdf content"
    result = parser.parse_document(content, FileType.PDF)
    assert "Regular text" in result
    assert "Table text" in result
    assert "More text" in result
    assert "Test" in result  # From table

def test_parse_document_docx(parser):
    """Test parsing a DOCX document"""
    content = b"test docx content"
    result = parser.parse_document(content, FileType.DOCX)
    assert "Regular text" in result
    assert "Table text" in result
    assert "More text" in result

def test_parse_document_pptx(parser):
    """Test parsing a PPTX document"""
    content = b"test pptx content"
    result = parser.parse_document(content, FileType.PPTX)
    assert "Regular text" in result
    assert "Table text" in result
    assert "More text" in result

def test_parse_document_invalid_type(parser):
    """Test parsing with invalid file type"""
    content = b"test content"
    with pytest.raises(ValueError, match="Unsupported file type"):
        parser.parse_document(content, "invalid")

def test_parse_document_batch(parser):
    """Test batch document parsing"""
    contents = [b"content1", b"content2"]
    file_types = [FileType.PDF, FileType.DOCX]
    results = parser.parse_document_batch(contents, file_types)
    assert len(results) == 2
    assert all("Regular text" in result for result in results)
    assert all("Table text" in result for result in results)

def test_parse_document_batch_mismatch(parser):
    """Test batch parsing with mismatched content and file types"""
    contents = [b"content1"]
    file_types = [FileType.PDF, FileType.DOCX]  # More types than contents
    with pytest.raises(ValueError):
        parser.parse_document_batch(contents, file_types)

def test_process_elements_without_metadata(parser):
    """Test processing elements without metadata"""
    elements = [
        Mock(text="Text without metadata", category="Text", metadata=None),
        Mock(text="Table without metadata", category="Table", metadata=None)
    ]
    result = parser._process_elements(elements)
    assert "Text without metadata" in result
    assert "Table without metadata" in result

def test_process_elements_with_html_table(parser):
    """Test processing elements with HTML table"""
    elements = [
        Mock(
            text="Table with HTML",
            category="Table",
            metadata=Mock(text_as_html="<table><tr><td>HTML Table</td></tr></table>")
        )
    ]
    result = parser._process_elements(elements)
    assert "HTML Table" in result
    assert "|" in result  # Markdown table should have pipe characters 