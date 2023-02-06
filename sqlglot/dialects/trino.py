from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.presto import Presto
from sqlglot.helper import seq_get


class Trino(Presto):
    class Parser(Presto.Parser):
        FUNCTIONS = {
            **Presto.Parser.FUNCTIONS,  # type: ignore
            "DAY_OF_WEEK": lambda args: exp.DateOfWeek(
                this=seq_get(args, 0),
            ),
            "DAY_OF_MONTH": lambda args: exp.DateOfMonth(
                this=seq_get(args, 0),
            ),
            "DAY_OF_YEAR": lambda args: exp.DateOfYear(
                this=seq_get(args, 0),
            ),
            "WEEK_OF_YEAR": lambda args: exp.WeekOfYear(
                this=seq_get(args, 0),
            ),
        }

    class Generator(Presto.Generator):
        TRANSFORMS = {
            **Presto.Generator.TRANSFORMS,  # type: ignore
            exp.ArraySum: lambda self, e: f"REDUCE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.DateOfWeek: lambda self, e: f"DAY_OF_WEEK({self.sql(e, 'this')})",
            exp.DateOfMonth: lambda self, e: f"DAY_OF_MONTH({self.sql(e, 'this')})",
            exp.DateOfYear: lambda self, e: f"DAY_OF_YEAR({self.sql(e, 'this')})",
            exp.WeekOfYear: lambda self, e: f"WEEK_OF_YEAR({self.sql(e, 'this')})",
        }

    class Tokenizer(Presto.Tokenizer):
        HEX_STRINGS = [("X'", "'")]
