from typing import Dict, Type

from .base import BaseParser
from .databricks_ai_parse_parser import DatabricksAIParseParser
from .unstructured_parser import UnstructuredParser


class ParserFactory:
    """Factory for creating parser instances"""

    _parsers: Dict[str, Type[BaseParser]] = {
        "unstructured": UnstructuredParser,
        "databricks_ai_parse": DatabricksAIParseParser,
    }

    @classmethod
    def get_parser(cls, parser_name: str, **kwargs) -> BaseParser:
        """Get a parser instance.

        Args:
            parser_name: Type of parser to create
            **kwargs: Parser configuration options

        Returns:
            BaseParser: Parser instance

        Raises:
            ValueError: If parser type is not supported
        """
        parser_class = cls._parsers.get(parser_name)
        if not parser_class:
            supported = ", ".join(cls._parsers.keys())
            raise ValueError(
                f"Unsupported parser: '{parser_name}'. "
                f"Supported parsers are: {supported}"
            )

        return parser_class(**kwargs)

    @classmethod
    def register_parser(cls, name: str, parser_class: Type[BaseParser]):
        """Register a new parser type.

        Args:
            name: Name of the parser
            parser_class: Parser class to register
        """
        if not issubclass(parser_class, BaseParser):
            raise TypeError(
                f"Parser class must inherit from BaseParser. Got {parser_class}"
            )
        cls._parsers[name] = parser_class
