from __future__ import annotations

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import rename_func
from sqlglot.generators.postgres import PostgresGenerator


class FelderaGenerator(PostgresGenerator):
    STAR_EXCEPT = "EXCLUDE"

    TYPE_MAPPING = {
        **PostgresGenerator.TYPE_MAPPING,
        exp.DType.DATETIME: "TIMESTAMP",
        exp.DType.FLOAT: "REAL",
        exp.DType.TEXT: "VARCHAR",
        exp.DType.UUID: "UUID",
        exp.DType.VARIANT: "VARIANT",
    }

    TRANSFORMS = {
        **{k: v for k, v in PostgresGenerator.TRANSFORMS.items() if k != exp.TryCast},
        exp.ArgMax: rename_func("ARG_MAX"),
        exp.ArgMin: rename_func("ARG_MIN"),
        exp.ArrayConcat: rename_func("ARRAY_CONCAT"),
        exp.ArrayContains: rename_func("ARRAY_CONTAINS"),
        exp.ArrayDistinct: rename_func("ARRAY_DISTINCT"),
        exp.ArrayOverlaps: rename_func("ARRAYS_OVERLAP"),
        exp.ArrayReverse: rename_func("ARRAY_REVERSE"),
        exp.ArraySize: rename_func("ARRAY_LENGTH"),
        exp.ArraySort: rename_func("SORT_ARRAY"),
        exp.CountIf: rename_func("COUNTIF"),
        exp.IsInf: rename_func("IS_INF"),
        exp.IsNan: rename_func("IS_NAN"),
        exp.Select: transforms.preprocess([transforms.eliminate_semi_and_anti_joins]),
    }

    PROPERTIES_LOCATION = {
        **PostgresGenerator.PROPERTIES_LOCATION,
        exp.LocalProperty: exp.Properties.Location.POST_CREATE,
        exp.LinearProperty: exp.Properties.Location.POST_CREATE,
    }

    def localproperty_sql(self, expression: exp.LocalProperty) -> str:
        return "LOCAL"

    def linearproperty_sql(self, expression: exp.LinearProperty) -> str:
        return "LINEAR"

    def declarerecursiveview_sql(self, expression: exp.DeclareRecursiveView) -> str:
        return f"DECLARE RECURSIVE VIEW {self.sql(expression, 'this')} AS {self.sql(expression, 'expression')}"

    def exists_sql(self, expression: exp.Exists) -> str:
        predicate = expression.args.get("expression")
        if predicate is not None:
            return self.func("EXISTS", expression.this, predicate)
        return super().exists_sql(expression)

    def nullsafeeq_sql(self, expression: exp.NullSafeEQ) -> str:
        return self.binary(expression, "<=>")

    def trycast_sql(self, expression: exp.TryCast) -> str:
        return self.cast_sql(expression, safe_prefix="SAFE_")
