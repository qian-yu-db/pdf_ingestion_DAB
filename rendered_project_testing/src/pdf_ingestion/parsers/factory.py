from typing import Dict, Type
from .base import BaseParser
from .unstructured_parser import UnstructuredParser

class ParserFactory:
    """Factory for creating parser instances"""
    
    _parsers: Dict[str, Type[BaseParser]] = {
        "unstructured": UnstructuredParser
    }
    
    @classmethod
    def get_parser(cls, parser_type: str, **kwargs) -> BaseParser:
        """Get a parser instance.
        
        Args:
            parser_type: Type of parser to create
            **kwargs: Parser configuration options
            
        Returns:
            BaseParser: Parser instance
            
        Raises:
            ValueError: If parser type is not supported
        """
        if parser_type not in cls._parsers:
            supported = ", ".join(cls._parsers.keys())
            raise ValueError(
                f"Unsupported parser type: {parser_type}. "
                f"Supported types are: {supported}"
            )
            
        return cls._parsers[parser_type](**kwargs)
    
    @classmethod
    def register_parser(cls, name: str, parser_class: Type[BaseParser]):
        """Register a new parser type.
        
        Args:
            name: Name of the parser
            parser_class: Parser class to register
        """
        cls._parsers[name] = parser_class 