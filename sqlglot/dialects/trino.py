from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.presto import Presto


class Trino(Presto):
    SUPPORTS_USER_DEFINED_TYPES = False

    class Generator(Presto.Generator):
        TRANSFORMS = {
            **Presto.Generator.TRANSFORMS,
            exp.ArraySum: lambda self, e: f"REDUCE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
        }

    class Tokenizer(Presto.Tokenizer):
        HEX_STRINGS = [("X'", "'")]
