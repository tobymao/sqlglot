from __future__ import annotations

from sqlglot import exp
from sqlglot.generators.trino import TrinoGenerator


class DuneGenerator(TrinoGenerator):
    TRANSFORMS = {
        **TrinoGenerator.TRANSFORMS,
        exp.HexString: lambda self, e: f"0x{e.this}",
    }
