from __future__ import annotations

from sqlglot import exp
from sqlglot.generators.tsql import TSQLGenerator


class SynapseGenerator(TSQLGenerator):
    def openrowset_sql(self, expression: exp.OpenRowset) -> str:
        return (
            f"OPENROWSET(BULK {self.sql(expression, 'bulk')}, "
            f"FORMAT = {self.sql(expression, 'format')})"
        )
