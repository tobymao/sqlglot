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
        exp.StabilityProperty: lambda self, e: (
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

    def functionspecification_sql(self, expression: exp.FunctionSpecification) -> str:
        characteristics = expression.args.get("characteristics")
        characteristics_sql = (
            self.properties(characteristics, prefix=" ", sep=" ", wrapped=False)
            if characteristics
            else ""
        )
        properties = expression.args.get("properties")
        with_sql = f" {self.with_properties(properties)}" if properties else ""
        body = self.sql(expression, "expression")
        return f"FUNCTION {self.sql(expression, 'this')}{characteristics_sql}{with_sql} {body}"

    def ifblock_sql(self, expression: exp.IfBlock) -> str:
        # ELSEIF chains are nested into `false` at parse time (see
        # TrinoParser._parse_routine_if), so this flattens them back out rather
        # than recursing on ifblock_sql itself, which would re-wrap each link in
        # its own IF ... END IF.
        branches: list[str] = []
        node: exp.Expr | None = expression

        while isinstance(node, exp.IfBlock):
            keyword = "IF" if not branches else "ELSEIF"
            branches.append(f"{keyword} {self.sql(node, 'this')} THEN {self.sql(node, 'true')};")
            node = node.args.get("false")

        if node is not None:
            branches.append(f"ELSE {self.sql(node)};")

        return f"{' '.join(branches)} END IF"

    def casestatement_sql(self, expression: exp.CaseStatement) -> str:
        # Mirrors case_sql, using `;`-terminated statement bodies and END CASE
        # instead of a single value expression per branch and bare END.
        this = self.sql(expression, "this")
        branches = [f"CASE {this}" if this else "CASE"]

        for node in expression.args["ifs"]:
            branches.append(f"WHEN {self.sql(node, 'this')} THEN {self.sql(node, 'true')};")

        default = expression.args.get("default")
        if default:
            branches.append(f"ELSE {self.sql(default)};")

        branches.append("END CASE")
        return " ".join(branches)

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
