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
from sqlglot.dialects.hive import Hive


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
                transforms.explode_projection_to_unnest(1, unnest_map=True),
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

    def getjsonobject_sql(self, expression: exp.GetJsonObject) -> str:
        path = Hive().to_json_path(expression.expression)
        if not isinstance(path, exp.JSONPath):
            return super().getjsonobject_sql(expression)

        path_sql = "strict " + "".join(
            '."' + part.name.replace('"', '""') + '"'
            if isinstance(part, exp.JSONPathKey) and isinstance(part.this, str)
            else self.json_path_part(part)
            for part in path.expressions
        )
        path_sql += " ? (@ != null)"
        return self.sql(
            exp.JSONExtract(
                this=expression.this,
                expression=exp.Literal.string(path_sql),
                json_query=True,
                quote=exp.JSONExtractQuote(option=exp.var("OMIT")),
            )
        )

    def concatws_sql(self, expression: exp.ConcatWs) -> str:
        if expression.args.get("flatten"):
            arrays = []
            has_array = False
            has_unknown = False

            for arg in expression.expressions[1:]:
                if isinstance(arg, exp.Array) or arg.is_type(exp.DType.ARRAY):
                    has_array = True
                    arg = exp.func("COALESCE", exp.cast(arg, "ARRAY<TEXT>"), exp.array())
                else:
                    if (not arg.type or arg.is_type(exp.DType.UNKNOWN)) and not isinstance(
                        arg, exp.CONSTANTS
                    ):
                        has_unknown = True

                    arg = exp.array(exp.cast(arg, exp.DType.TEXT))

                arrays.append(arg)

            if has_unknown:
                self.unsupported("Cannot transpile CONCAT_WS with unknown argument types to Trino.")

            if has_array:
                array = (
                    exp.ArrayConcat(this=arrays[0], expressions=arrays[1:])
                    if len(arrays) > 1
                    else arrays[0]
                )
                return self.func("CONCAT_WS", expression.expressions[0], array)

        return super().concatws_sql(expression)

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

    def whileblock_sql(self, expression: exp.WhileBlock) -> str:
        label = expression.args.get("label")
        label_sql = f"{self.sql(label)}: " if label else ""
        condition = self.sql(expression, "this")
        body = self.sql(expression, "body")
        return f"{label_sql}WHILE {condition} DO {body}; END WHILE"

    def loopblock_sql(self, expression: exp.LoopBlock) -> str:
        label = expression.args.get("label")
        label_sql = f"{self.sql(label)}: " if label else ""
        body = self.sql(expression, "body")
        return f"{label_sql}LOOP {body}; END LOOP"

    def repeatblock_sql(self, expression: exp.RepeatBlock) -> str:
        label = expression.args.get("label")
        label_sql = f"{self.sql(label)}: " if label else ""
        body = self.sql(expression, "body")
        until = self.sql(expression, "until")
        return f"{label_sql}REPEAT {body}; UNTIL {until} END REPEAT"

    def leave_sql(self, expression: exp.Leave) -> str:
        return f"LEAVE {self.sql(expression, 'this')}"

    def iterate_sql(self, expression: exp.Iterate) -> str:
        return f"ITERATE {self.sql(expression, 'this')}"

    def jsonextract_sql(self, expression: exp.JSONExtract) -> str:
        if not expression.args.get("json_query"):
            return super().jsonextract_sql(expression)

        json_path = self.sql(expression, "expression")

        # Trino's JSON_QUERY requires the path to start with a mode specifier. Paths coming from
        # dialects that don't have one (e.g. T-SQL) are parsed into a JSONPath, so we prefix the
        # standard default mode. Paths that failed to parse stay literals and keep their own mode.
        if isinstance(expression.expression, exp.JSONPath):
            quote = self.dialect.QUOTE_START
            json_path = f"{quote}lax {json_path.removeprefix(quote)}"

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
