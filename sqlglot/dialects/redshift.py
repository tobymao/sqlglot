from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType


class Redshift(Postgres):
    time_format = "'YYYY-MM-DD HH:MI:SS'"
    time_mapping = {
        "YY": "%y",
        "yy": "%y",
        "YYYY": "%Y",
        "yyyy": "%Y",
        "MM": "%m",
        "mm": "%m",
        "MON": "%b",
        "mon": "%b",
        "DD": "%d",
        "dd": "%d",
        "HH12": "%I",
        "hh12": "%I",
        "HH24": "%H",
        "hh24": "%H",
        "HH": "%H",
        "hh": "%H",
        "MI": "%M",
        "mi": "%M",
        "SS": "%S",
        "ss": "%S",
        "OF": "%z",
        "of": "%z",
        "AM": "%p",
        "PM": "%p",
    }

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
