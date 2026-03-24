from __future__ import annotations


from sqlglot.dialects.trino import Trino
from sqlglot.generators.dune import DuneGenerator
from sqlglot.parsers.dune import DuneParser


class Dune(Trino):
    Parser = DuneParser

    class Tokenizer(Trino.Tokenizer):
        HEX_STRINGS = ["0x", ("X'", "'")]

    Generator = DuneGenerator
