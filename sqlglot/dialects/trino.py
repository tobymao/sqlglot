from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import merge_without_target_sql
from sqlglot.dialects.presto import Presto


class Trino(Presto):
    SUPPORTS_USER_DEFINED_TYPES = False

    class Generator(Presto.Generator):
        TRANSFORMS = {
            **Presto.Generator.TRANSFORMS,
            exp.ArraySum: lambda self,
            e: f"REDUCE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.Merge: merge_without_target_sql,
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

    class Tokenizer(Presto.Tokenizer):
        HEX_STRINGS = [("X'", "'")]
