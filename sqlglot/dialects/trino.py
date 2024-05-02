from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import merge_without_target_sql, trim_sql
from sqlglot.dialects.presto import Presto


class Trino(Presto):
    SUPPORTS_USER_DEFINED_TYPES = False
    LOG_BASE_FIRST = True

    class Parser(Presto.Parser):
        FUNCTION_PARSERS = {
            **Presto.Parser.FUNCTION_PARSERS,
            "TRIM": lambda self: self._parse_trim(),
        }

    class Generator(Presto.Generator):
        TRANSFORMS = {
            **Presto.Generator.TRANSFORMS,
            exp.ArraySum: lambda self,
            e: f"REDUCE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.Merge: merge_without_target_sql,
            exp.Trim: trim_sql,
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

    class Tokenizer(Presto.Tokenizer):
        HEX_STRINGS = [("X'", "'")]
