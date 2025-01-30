from __future__ import annotations


from sqlglot import exp
from sqlglot.dialects.trino import Trino


class Dune(Trino):
    class Tokenizer(Trino.Tokenizer):
        HEX_STRINGS = ["0x", ("X'", "'")]

    class Generator(Trino.Generator):
        TRANSFORMS = {
            **Trino.Generator.TRANSFORMS,
            exp.HexString: lambda self, e: f"0x{e.this}",
        }
