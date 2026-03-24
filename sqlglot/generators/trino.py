from __future__ import annotations

import typing as t

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    merge_without_target_sql,
    trim_sql,
    timestrtotime_sql,
    groupconcat_sql,
    rename_func,
)
from sqlglot.generators.presto import PrestoGenerator, amend_exploded_column_table

_POST_WITH: t.Any = getattr(exp.Properties.Location, "POST_WITH")


class TrinoGenerator(PrestoGenerator):
    EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = True
    PROPERTIES_LOCATION: t.ClassVar[t.Dict[t.Any, t.Any]] = {
        **PrestoGenerator.PROPERTIES_LOCATION,
        exp.LocationProperty: _POST_WITH,
    }

    TRANSFORMS = {
        **PrestoGenerator.TRANSFORMS,
        exp.ArraySum: lambda self, e: (
            f"REDUCE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)"
        ),
        exp.ArrayUniqueAgg: lambda self, e: f"ARRAY_AGG(DISTINCT {self.sql(e, 'this')})",
        exp.CurrentVersion: rename_func("VERSION"),
        exp.GroupConcat: lambda self, e: groupconcat_sql(self, e, on_overflow=True),
        exp.LocationProperty: lambda self, e: self.property_sql(e),
        exp.Merge: merge_without_target_sql,
        exp.Select: transforms.preprocess(
            [
                transforms.eliminate_qualify,
                transforms.eliminate_distinct_on,
                transforms.explode_projection_to_unnest(1),
                transforms.eliminate_semi_and_anti_joins,
                amend_exploded_column_table,
            ]
        ),
        exp.TimeStrToTime: lambda self, e: timestrtotime_sql(self, e, include_precision=True),
        exp.Trim: trim_sql,
    }

    SUPPORTED_JSON_PATH_PARTS = {
        exp.JSONPathKey,
        exp.JSONPathRoot,
        exp.JSONPathSubscript,
    }

    def jsonextract_sql(self, expression: exp.JSONExtract) -> str:
        if not expression.args.get("json_query"):
            return super().jsonextract_sql(expression)

        json_path = self.sql(expression, "expression")
        option = self.sql(expression, "option")
        option = f" {option}" if option else ""

        quote = self.sql(expression, "quote")
        quote = f" {quote}" if quote else ""

        on_condition = self.sql(expression, "on_condition")
        on_condition = f" {on_condition}" if on_condition else ""

        return self.func(
            "JSON_QUERY",
            expression.this,
            json_path + option + quote + on_condition,
        )
