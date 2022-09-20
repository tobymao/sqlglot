from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType


class Redshift(Postgres):
    class Tokenizer(Postgres.Tokenizer):
        ESCAPE = "\\"

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "GEOMETRY": TokenType.GEOMETRY,
            "GEOGRAPHY": TokenType.GEOGRAPHY,
            "HLLSKETCH": TokenType.HLLSKETCH,
            "SUPER": TokenType.SUPER,
            "TIME": TokenType.TIMESTAMP,
            "TIMETZ": TokenType.TIMESTAMPTZ,
            "VARBYTE": TokenType.VARBYTE,
            "SIMILAR TO": TokenType.SIMILAR_TO,
        }

    class Generator(Postgres.Generator):
        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,
            exp.DataType.Type.BINARY: "VARBYTE",
            exp.DataType.Type.GEOMETRY: "GEOMETRY",
            exp.DataType.Type.HLLSKETCH: "HLLSKETCH",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.SUPER: "SUPER",
        }
