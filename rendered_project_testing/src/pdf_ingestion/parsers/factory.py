from typing import Dict, Type
from .base import BasePDFParser
from .unstructured_parser import UnstructuredParser
from .databricks_parser import DatabricksAIParser

class ParserFactory:
    """Factory class for creating PDF parser instances."""
    
    _parsers: Dict[str, Type[BasePDFParser]] = {
        "unstructured": UnstructuredParser,
        "databricks_ai": DatabricksAIParser,
    }
    
    @classmethod
    def get_parser(cls, parser_name: str, **kwargs) -> BasePDFParser:
        """Get a parser instance by name.
        
        Args:
            parser_name: Name of the parser to create
            **kwargs: Arguments to pass to the parser constructor
            
        Returns:
            BasePDFParser: Instance of the requested parser
            
        Raises:
            ValueError: If the requested parser is not available
        """
        if parser_name not in cls._parsers:
            available_parsers = ", ".join(cls._parsers.keys())
            raise ValueError(f"Parser '{parser_name}' not found. Available parsers: {available_parsers}")
        
        return cls._parsers[parser_name](**kwargs)
    
    @classmethod
    def register_parser(cls, name: str, parser_class: Type[BasePDFParser]):
        """Register a new parser type.
        
        Args:
            name: Name to register the parser under
            parser_class: Parser class to register
        """
        if not issubclass(parser_class, BasePDFParser):
            raise ValueError(f"Parser class must inherit from BasePDFParser")
        cls._parsers[name] = parser_class 