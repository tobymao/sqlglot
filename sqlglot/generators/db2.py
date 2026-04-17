from __future__ import annotations

from sqlglot import exp, generator
from sqlglot.dialects.dialect import rename_func


class Db2(generator.Generator):
    AFTER_HAVING_MODIFIER_TRANSFORMS = {
        "cluster": lambda self, e: "",
        "distribute": lambda self, e: "",
        "sort": lambda self, e: "",
    }

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.StrPosition: rename_func("POSSTR"),
        exp.TimeToStr: rename_func("VARCHAR_FORMAT"),
    }

    def extract_sql(self, expression: exp.Extract) -> str:
        this = self.sql(expression, "this")
        expression_sql = self.sql(expression, "expression")

        if this.upper() in ("DAYOFWEEK", "DAYOFYEAR"):
            return f"{this.upper()}({expression_sql})"

        return f"EXTRACT({this} FROM {expression_sql})"
