from __future__ import annotations

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import rename_func
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType


class Redshift(Postgres):
    time_format = "'YYYY-MM-DD HH:MI:SS'"
    time_mapping = {
        **Postgres.time_mapping,  # type: ignore
        "MON": "%b",
        "HH": "%H",
    }

    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,  # type: ignore
            "DECODE": exp.Matches.from_arg_list,
            "NVL": exp.Coalesce.from_arg_list,
        }

    class Tokenizer(Postgres.Tokenizer):
        ESCAPES = ["\\"]

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,  # type: ignore
            "COPY": TokenType.COMMAND,
            "ENCODE": TokenType.ENCODE,
            "GEOMETRY": TokenType.GEOMETRY,
            "GEOGRAPHY": TokenType.GEOGRAPHY,
            "HLLSKETCH": TokenType.HLLSKETCH,
            "SUPER": TokenType.SUPER,
            "TIME": TokenType.TIMESTAMP,
            "TIMETZ": TokenType.TIMESTAMPTZ,
            "UNLOAD": TokenType.COMMAND,
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

        ROOT_PROPERTIES = {
            exp.DistKeyProperty,
            exp.SortKeyProperty,
            exp.DistStyleProperty,
        }

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,  # type: ignore
            **transforms.ELIMINATE_DISTINCT_ON,  # type: ignore
            exp.DistKeyProperty: lambda self, e: f"DISTKEY({e.name})",
            exp.SortKeyProperty: lambda self, e: f"{'COMPOUND ' if e.args['compound'] else ''}SORTKEY({self.format_args(*e.this)})",
            exp.DistStyleProperty: lambda self, e: self.naked_property(e),
            exp.Matches: rename_func("DECODE"),
        }
