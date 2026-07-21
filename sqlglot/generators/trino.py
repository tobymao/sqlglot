from __future__ import annotations

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    merge_without_target_sql,
    trim_sql,
    timestrtotime_sql,
    groupconcat_sql,
    rename_func,
)
from sqlglot.generators.presto import PrestoGenerator, amend_exploded_column_table


class TrinoGenerator(PrestoGenerator):
    EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = True
    DECLARE_DEFAULT_ASSIGNMENT = "DEFAULT"

    PROPERTIES_LOCATION = {
        **PrestoGenerator.PROPERTIES_LOCATION,
        exp.LocationProperty: exp.Properties.Location.POST_WITH,
    }

    TRANSFORMS = {
        **PrestoGenerator.TRANSFORMS,
        exp.ArraySum: lambda self, e: (
            f"REDUCE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)"
        ),
        exp.ArrayUniqueAgg: lambda self, e: f"ARRAY_AGG(DISTINCT {self.sql(e, 'this')})",
        exp.CurrentVersion: rename_func("VERSION"),
        exp.FromISO8601TimestampNanos: rename_func("FROM_ISO8601_TIMESTAMP_NANOS"),
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
        exp.SqlSecurityProperty: lambda self, e: f"SECURITY {self.sql(e, 'this')}",
        exp.StabilityProperty: lambda _, e: (
            "DETERMINISTIC" if e.name == "IMMUTABLE" else "NOT DETERMINISTIC"
        ),
        exp.TimeStrToTime: lambda self, e: timestrtotime_sql(self, e, include_precision=True),
        exp.Trim: trim_sql,
    }

    SUPPORTED_JSON_PATH_PARTS = {
        exp.JSONPathKey,
        exp.JSONPathRoot,
        exp.JSONPathSubscript,
    }

    def with_sql(self, expression: exp.With) -> str:
        # Inline UDFs are declared in their own `WITH` clause, which precedes the (optional)
        # `WITH` clause of the query: https://trino.io/docs/current/udf/sql.html
        functions = [e for e in expression.expressions if isinstance(e, exp.FunctionSpecification)]
        if not functions:
            return super().with_sql(expression)

        functions_sql = self.expressions(sqls=functions, flat=True)

        ctes = [e for e in expression.expressions if not isinstance(e, exp.FunctionSpecification)]
        if not ctes:
            return f"WITH {functions_sql}"

        recursive = "RECURSIVE " if expression.args.get("recursive") else ""
        search = self.sql(expression, "search")
        search = f" {search}" if search else ""
        ctes_sql = self.expressions(sqls=ctes, flat=True)

        return f"WITH {functions_sql} WITH {recursive}{ctes_sql}{search}"

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
