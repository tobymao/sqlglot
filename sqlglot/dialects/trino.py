from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import remove_target_from_merge
from sqlglot.dialects.presto import Presto


class Trino(Presto):
    SUPPORTS_USER_DEFINED_TYPES = False

    class Generator(Presto.Generator):
        TRANSFORMS = {
            **Presto.Generator.TRANSFORMS,
            exp.ArraySum: lambda self, e: f"REDUCE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.Merge: remove_target_from_merge,
        }

    class Tokenizer(Presto.Tokenizer):
        HEX_STRINGS = [("X'", "'")]
