from __future__ import annotations

import typing as t
from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType

class DremioTokenizer(tokens.Tokenizer):
    """Basic tokenizer for Dremio SQL."""
    KEYWORDS = {
        **tokens.Tokenizer.KEYWORDS,
        "SELECT": TokenType.SELECT,
        "FROM": TokenType.FROM,
        "WHERE": TokenType.WHERE,
        "LIMIT": TokenType.LIMIT,
    }

class DremioParser(parser.Parser):
    """Basic parser for Dremio SQL."""
    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
    }

class DremioGenerator(generator.Generator):
    """Basic SQL generator for Dremio."""
    DIALECT = "dremio"
    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
    }

class Dremio(Dialect):
    """Minimal Dremio dialect for SQLGlot."""
    TOKENIZER = DremioTokenizer
    PARSER = DremioParser
    GENERATOR = DremioGenerator
