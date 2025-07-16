from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    build_timestamp_trunc,
    rename_func,
    unit_to_str,
    inline_array_sql,
    property_sql,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


# https://docs.starrocks.io/docs/sql-reference/sql-functions/spatial-functions/st_distance_sphere/
def st_distance_sphere(self, expression: exp.StDistance) -> str:
    point1 = expression.this
    point2 = expression.expression

    point1_x = self.func("ST_X", point1)
    point1_y = self.func("ST_Y", point1)
    point2_x = self.func("ST_X", point2)
    point2_y = self.func("ST_Y", point2)

    return self.func("ST_Distance_Sphere", point1_x, point1_y, point2_x, point2_y)


class StarRocks(MySQL):
    STRICT_JSON_PATH_SYNTAX = False

    class Tokenizer(MySQL.Tokenizer):
        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            "LARGEINT": TokenType.INT128,
        }

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "DATE_TRUNC": build_timestamp_trunc,
            "DATEDIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 0), expression=seq_get(args, 1), unit=exp.Literal.string("DAY")
            ),
            "DATE_DIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 1), expression=seq_get(args, 2), unit=seq_get(args, 0)
            ),
            "REGEXP": exp.RegexpLike.from_arg_list,
        }

        PROPERTY_PARSERS = {
            **MySQL.Parser.PROPERTY_PARSERS,
            "PARTITION BY": lambda self: self._parse_partition_by_opt_range(),
            "PROPERTIES": lambda self: self._parse_wrapped_properties(),
            "UNIQUE": lambda self: self._parse_composite_key_property(exp.UniqueKeyProperty),
        }

        def _parse_create(self) -> exp.Create | exp.Command:
            create = super()._parse_create()

            # Starrocks' primary key is defined outside of the schema, so we need to move it there
            # https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/#usage
            if isinstance(create, exp.Create) and isinstance(create.this, exp.Schema):
                props = create.args.get("properties")
                if props:
                    primary_key = props.find(exp.PrimaryKey)
                    if primary_key:
                        create.this.append("expressions", primary_key.pop())

            return create

        def _parse_unnest(self, with_alias: bool = True) -> t.Optional[exp.Unnest]:
            unnest = super()._parse_unnest(with_alias=with_alias)

            if unnest:
                alias = unnest.args.get("alias")

                if not alias:
                    # Starrocks defaults to naming the table alias as "unnest"
                    alias = exp.TableAlias(
                        this=exp.to_identifier("unnest"), columns=[exp.to_identifier("unnest")]
                    )
                    unnest.set("alias", alias)
                elif not alias.args.get("columns"):
                    # Starrocks defaults to naming the UNNEST column as "unnest"
                    # if it's not otherwise specified
                    alias.set("columns", [exp.to_identifier("unnest")])

            return unnest

        def _parse_partitioning_granularity_dynamic(self) -> exp.PartitionByRangePropertyDynamic:
            self._match_text_seq("START")
            start = self._parse_wrapped(self._parse_string)
            self._match_text_seq("END")
            end = self._parse_wrapped(self._parse_string)
            self._match_text_seq("EVERY")
            every = self._parse_wrapped(lambda: self._parse_interval() or self._parse_number())
            return self.expression(
                exp.PartitionByRangePropertyDynamic, start=start, end=end, every=every
            )

        def _parse_partition_by_opt_range(
            self,
        ) -> exp.PartitionedByProperty | exp.PartitionByRangeProperty:
            if self._match_text_seq("RANGE"):
                partition_expressions = self._parse_wrapped_id_vars()
                create_expressions = self._parse_wrapped_csv(
                    self._parse_partitioning_granularity_dynamic
                )
                return self.expression(
                    exp.PartitionByRangeProperty,
                    partition_expressions=partition_expressions,
                    create_expressions=create_expressions,
                )
            return super()._parse_partitioned_by()

    class Generator(MySQL.Generator):
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        JSON_TYPE_REQUIRED_FOR_EXTRACTION = False
        VARCHAR_REQUIRES_SIZE = False
        PARSE_JSON_NAME: t.Optional[str] = "PARSE_JSON"
        WITH_PROPERTIES_PREFIX = "PROPERTIES"

        CAST_MAPPING = {}

        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.INT128: "LARGEINT",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }

        PROPERTIES_LOCATION = {
            **MySQL.Generator.PROPERTIES_LOCATION,
            exp.PrimaryKey: exp.Properties.Location.POST_SCHEMA,
            exp.UniqueKeyProperty: exp.Properties.Location.POST_SCHEMA,
            exp.PartitionByRangeProperty: exp.Properties.Location.POST_SCHEMA,
        }

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.Array: inline_array_sql,
            exp.ArrayAgg: rename_func("ARRAY_AGG"),
            exp.ArrayFilter: rename_func("ARRAY_FILTER"),
            exp.ArrayToString: rename_func("ARRAY_JOIN"),
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", unit_to_str(e), e.this, e.expression
            ),
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.Property: property_sql,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.SchemaCommentProperty: lambda self, e: self.naked_property(e),
            exp.StDistance: st_distance_sphere,
            exp.StrToUnix: lambda self, e: self.func("UNIX_TIMESTAMP", e.this, self.format_time(e)),
            exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.UnixToStr: lambda self, e: self.func("FROM_UNIXTIME", e.this, self.format_time(e)),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }

        TRANSFORMS.pop(exp.DateTrunc)

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
