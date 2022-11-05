from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType


class Redshift(Postgres):
    time_format = "'YYYY-MM-DD HH:MI:SS'"
    time_mapping = {
        **Postgres.time_mapping,  # type: ignore
        "MON": "%b",
        "HH": "%H",
    }

    class Tokenizer(Postgres.Tokenizer):
        ESCAPES = ["\\"]

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,  # type: ignore
            "GEOMETRY": TokenType.GEOMETRY,
            "GEOGRAPHY": TokenType.GEOGRAPHY,
            "HLLSKETCH": TokenType.HLLSKETCH,
            "SUPER": TokenType.SUPER,
            "TIME": TokenType.TIMESTAMP,
            "TIMETZ": TokenType.TIMESTAMPTZ,
            "VARBYTE": TokenType.VARBINARY,
            "SIMILAR TO": TokenType.SIMILAR_TO,
        }

    class Generator(Postgres.Generator):
        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.BINARY: "VARBYTE",
            exp.DataType.Type.VARBINARY: "VARBYTE",
            exp.DataType.Type.INT: "INTEGER",
        }
