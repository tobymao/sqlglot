from __future__ import annotations


from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    rename_func,
    unit_to_str,
    inline_array_sql,
    property_sql,
)
from sqlglot.generators.mysql import MySQLGenerator


def _eliminate_between_in_delete(expression: exp.Expr) -> exp.Expr:
    """
    StarRocks doesn't support BETWEEN in DELETE statements, so we convert
    BETWEEN expressions to explicit comparisons.

    https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/DELETE/#parameters

    Example:
        >>> from sqlglot import parse_one
        >>> expr = parse_one("DELETE FROM t WHERE x BETWEEN 1 AND 10")
        >>> print(_eliminate_between_in_delete(expr).sql(dialect="starrocks"))
        DELETE FROM t WHERE x >= 1 AND x <= 10
    """
    if where := expression.args.get("where"):
        for between in where.find_all(exp.Between):
            between.replace(
                exp.and_(
                    exp.GTE(this=between.this.copy(), expression=between.args["low"]),
                    exp.LTE(this=between.this.copy(), expression=between.args["high"]),
                    copy=False,
                )
            )
    return expression


# https://docs.starrocks.io/docs/sql-reference/sql-functions/spatial-functions/st_distance_sphere/
def st_distance_sphere(self, expression: exp.StDistance) -> str:
    point1 = expression.this
    point2 = expression.expression

    point1_x = self.func("ST_X", point1)
    point1_y = self.func("ST_Y", point1)
    point2_x = self.func("ST_X", point2)
    point2_y = self.func("ST_Y", point2)

    return self.func("ST_Distance_Sphere", point1_x, point1_y, point2_x, point2_y)


class StarRocksGenerator(MySQLGenerator):
    EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
    JSON_TYPE_REQUIRED_FOR_EXTRACTION = False
    VARCHAR_REQUIRES_SIZE = False
    PARSE_JSON_NAME: str | None = "PARSE_JSON"
    WITH_PROPERTIES_PREFIX = "PROPERTIES"
    UPDATE_STATEMENT_SUPPORTS_FROM = True
    INSERT_OVERWRITE = " OVERWRITE"

    # StarRocks doesn't support "IS TRUE/FALSE" syntax.
    IS_BOOL_ALLOWED = False
    # StarRocks doesn't support renaming a table with a database.
    RENAME_TABLE_WITH_DB = False

    CAST_MAPPING = {}

    TYPE_MAPPING = {
        **MySQLGenerator.TYPE_MAPPING,
        exp.DType.INT128: "LARGEINT",
        exp.DType.TEXT: "STRING",
        exp.DType.TIMESTAMP: "DATETIME",
        exp.DType.TIMESTAMPTZ: "DATETIME",
    }

    SQL_SECURITY_VIEW_LOCATION = exp.Properties.Location.POST_SCHEMA

    PROPERTIES_LOCATION = {
        **MySQLGenerator.PROPERTIES_LOCATION,
        exp.PrimaryKey: exp.Properties.Location.POST_SCHEMA,
        exp.UniqueKeyProperty: exp.Properties.Location.POST_SCHEMA,
        exp.RollupProperty: exp.Properties.Location.POST_SCHEMA,
        exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
    }

    TRANSFORMS = {
        **{k: v for k, v in MySQLGenerator.TRANSFORMS.items() if k is not exp.DateTrunc},
        exp.Array: inline_array_sql,
        exp.ArrayAgg: rename_func("ARRAY_AGG"),
        exp.ArrayFilter: rename_func("ARRAY_FILTER"),
        exp.ArrayToString: rename_func("ARRAY_JOIN"),
        exp.ApproxDistinct: approx_count_distinct_sql,
        exp.CurrentVersion: lambda *_: "CURRENT_VERSION()",
        exp.DateDiff: lambda self, e: self.func("DATE_DIFF", unit_to_str(e), e.this, e.expression),
        exp.Delete: transforms.preprocess([_eliminate_between_in_delete]),
        exp.Flatten: rename_func("ARRAY_FLATTEN"),
        exp.JSONExtractScalar: arrow_json_extract_sql,
        exp.JSONExtract: arrow_json_extract_sql,
        exp.Property: property_sql,
        exp.RegexpLike: rename_func("REGEXP"),
        # Inherited from MySQL, minus operations StarRocks supports natively
        # (QUALIFY, FULL OUTER JOIN, SEMI/ANTI JOIN)
        exp.Select: transforms.preprocess(
            [
                transforms.eliminate_distinct_on,
                transforms.unnest_generate_date_array_using_recursive_cte,
            ]
        ),
        exp.SchemaCommentProperty: lambda self, e: self.naked_property(e),
        exp.SqlSecurityProperty: lambda self, e: f"SECURITY {self.sql(e.this)}",
        exp.StDistance: st_distance_sphere,
        exp.StrToUnix: lambda self, e: self.func("UNIX_TIMESTAMP", e.this, self.format_time(e)),
        exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
        exp.TimeStrToDate: rename_func("TO_DATE"),
        exp.UnixToStr: lambda self, e: self.func("FROM_UNIXTIME", e.this, self.format_time(e)),
        exp.UnixToTime: rename_func("FROM_UNIXTIME"),
    }

    # https://docs.starrocks.io/docs/sql-reference/sql-statements/keywords/#reserved-keywords
    RESERVED_KEYWORDS = {
        "add",
        "all",
        "alter",
        "analyze",
        "and",
        "array",
        "as",
        "asc",
        "between",
        "bigint",
        "bitmap",
        "both",
        "by",
        "case",
        "char",
        "character",
        "check",
        "collate",
        "column",
        "compaction",
        "convert",
        "create",
        "cross",
        "cube",
        "current_date",
        "current_role",
        "current_time",
        "current_timestamp",
        "current_user",
        "database",
        "databases",
        "decimal",
        "decimalv2",
        "decimal32",
        "decimal64",
        "decimal128",
        "default",
        "deferred",
        "delete",
        "dense_rank",
        "desc",
        "describe",
        "distinct",
        "double",
        "drop",
        "dual",
        "else",
        "except",
        "exists",
        "explain",
        "false",
        "first_value",
        "float",
        "for",
        "force",
        "from",
        "full",
        "function",
        "grant",
        "group",
        "grouping",
        "grouping_id",
        "groups",
        "having",
        "hll",
        "host",
        "if",
        "ignore",
        "immediate",
        "in",
        "index",
        "infile",
        "inner",
        "insert",
        "int",
        "integer",
        "intersect",
        "into",
        "is",
        "join",
        "json",
        "key",
        "keys",
        "kill",
        "lag",
        "largeint",
        "last_value",
        "lateral",
        "lead",
        "left",
        "like",
        "limit",
        "load",
        "localtime",
        "localtimestamp",
        "maxvalue",
        "minus",
        "mod",
        "not",
        "ntile",
        "null",
        "on",
        "or",
        "order",
        "outer",
        "outfile",
        "over",
        "partition",
        "percentile",
        "primary",
        "procedure",
        "qualify",
        "range",
        "rank",
        "read",
        "regexp",
        "release",
        "rename",
        "replace",
        "revoke",
        "right",
        "rlike",
        "row",
        "row_number",
        "rows",
        "schema",
        "schemas",
        "select",
        "set",
        "set_var",
        "show",
        "smallint",
        "system",
        "table",
        "terminated",
        "text",
        "then",
        "tinyint",
        "to",
        "true",
        "union",
        "unique",
        "unsigned",
        "update",
        "use",
        "using",
        "values",
        "varchar",
        "when",
        "where",
        "with",
    }

    def create_sql(self, expression: exp.Create) -> str:
        # Starrocks' primary key is defined outside of the schema, so we need to move it there
        schema = expression.this
        if isinstance(schema, exp.Schema):
            primary_key = schema.find(exp.PrimaryKey)

            if primary_key:
                props = expression.args.get("properties")

                if not props:
                    props = exp.Properties(expressions=[])
                    expression.set("properties", props)

                # Verify if the first one is an engine property. Is true then insert it after the engine,
                # otherwise insert it at the beginning
                engine = props.find(exp.EngineProperty)
                engine_index = (engine.index or 0) if engine else -1
                props.set("expressions", primary_key.pop(), engine_index + 1, overwrite=False)

        return super().create_sql(expression)

    def partitionedbyproperty_sql(self, expression: exp.PartitionedByProperty) -> str:
        this = expression.this
        if isinstance(this, exp.Schema):
            # For MVs, StarRocks needs outer parentheses.
            create = expression.find_ancestor(exp.Create)

            sql = self.expressions(this, flat=True)
            if (create and create.kind == "VIEW") or all(
                isinstance(col, (exp.Column, exp.Identifier)) for col in this.expressions
            ):
                sql = f"({sql})"

            return f"PARTITION BY {sql}"

        return f"PARTITION BY {self.sql(this)}"

    def cluster_sql(self, expression: exp.Cluster) -> str:
        """Generate StarRocks ORDER BY clause for clustering."""
        expressions = self.expressions(expression, flat=True)
        return f"ORDER BY ({expressions})" if expressions else ""

    def refreshtriggerproperty_sql(self, expression: exp.RefreshTriggerProperty) -> str:
        """Generate StarRocks REFRESH clause for materialized views.
        There is a little difference of the syntax between StarRocks and Doris.
        """
        method = self.sql(expression, "method")
        method = f" {method}" if method else ""
        kind = self.sql(expression, "kind")
        kind = f" {kind}" if kind else ""
        starts = self.sql(expression, "starts")
        starts = f" START ({starts})" if starts else ""
        every = self.sql(expression, "every")
        unit = self.sql(expression, "unit")
        every = f" EVERY (INTERVAL {every} {unit})" if every and unit else ""

        return f"REFRESH{method}{kind}{starts}{every}"
