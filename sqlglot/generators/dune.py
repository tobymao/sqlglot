from __future__ import annotations

from sqlglot import exp
from sqlglot.generators.trino import TrinoGenerator


class DuneGenerator(TrinoGenerator):
    TRY_SUPPORTED = False
    SUPPORTS_UESCAPE = False

    def hexstring_sql(
        self, expression: exp.HexString, binary_function_repr: str | None = None
    ) -> str:
        if expression.args.get("is_character"):
            return super().hexstring_sql(expression, binary_function_repr)
        return f"0x{expression.this}"
