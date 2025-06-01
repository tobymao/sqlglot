from __future__ import annotations


from sqlglot import exp
from sqlglot.dialects.trino import Trino
from sqlglot.dialects.dialect import (
    rename_func,
)


class Dune(Trino):
    class Tokenizer(Trino.Tokenizer):
        HEX_STRINGS = ["0x", ("X'", "'")]

    class Generator(Trino.Generator):
        TRANSFORMS = {
            **Trino.Generator.TRANSFORMS,
            exp.ArrayIntersection: rename_func("ARRAY_INTERSECT"),
            exp.HexString: lambda self, e: f"0x{e.this}",
        }
