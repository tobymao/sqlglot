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
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _duplicate_key_sql(self, expression: exp.DuplicateKeyProperty) -> str:
    expressions = self.expressions(expression, flat=True)
    options = self.expressions(expression, key="options", flat=True, sep=" ")
    options = f" {options}" if options else ""
    return f"DUPLICATE KEY ({expressions}){options}"


def _distributed_by_hash_sql(self, expression: exp.DistributedByHashProperty) -> str:
    expressions = self.expressions(expression, key="expressions", flat=True)
    sorted_by = self.expressions(expression, key="sorted_by", flat=True)
    sorted_by = f" ORDER BY ({sorted_by})" if sorted_by else ""
    buckets = self.sql(expression, "buckets")
    # Since StarRocks v2.5.7, the number of buckets is AUTO by default.
    # https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/#distribution_desc
    if expression.auto_bucket:
        buckets = "AUTO"
    # [DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num] [ORDER BY (k1[,k2 ...])]]
    return f"DISTRIBUTED BY HASH ({expressions}) BUCKETS {buckets}{sorted_by}"


def _distributed_by_random_sql(self, expression: exp.DistributedByRandomProperty) -> str:
    expressions = self.expressions(expression, flat=True)
    sorted_by = self.expressions(expression, key="sorted_by", flat=True)
    sorted_by = f" ORDER BY ({sorted_by})" if sorted_by else ""
    buckets = self.sql(expression, "buckets")
    if expression.auto_bucket:
        buckets = "AUTO"
    # [DISTRIBUTED BY RANDOM (k1[,k2 ...]) [BUCKETS num] [ORDER BY (k1[,k2 ...])]]
    return f"DISTRIBUTED BY RANDOM ({expressions}) BUCKETS {buckets}{sorted_by}"


def _property_sql(self: MySQL.Generator, expression: exp.Property) -> str:
    # [PROPERTIES (['key1'='value1', 'key2'='value2', ...])]
    return f"{self.property_name(expression, string_key=True)}={self.sql(expression, 'value')}"


class StarRocks(MySQL):
    STRICT_JSON_PATH_SYNTAX = False

    class Tokenizer(MySQL.Tokenizer):
        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            "DECIMAL64": TokenType.DECIMAL,
            "DECIMAL128": TokenType.BIGDECIMAL,
            "DISTRIBUTED BY HASH": TokenType.DISTRIBUTE_BY,
            "DISTRIBUTED BY RANDOM": TokenType.DISTRIBUTE_BY,
            "DUPLICATE KEY": TokenType.DUPLICATE_KEY,
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
            "DISTRIBUTED BY HASH": lambda self: self._parse_distributed_by("HASH"),
            "DISTRIBUTED BY RANDOM": lambda self: self._parse_distributed_by("RANDOM"),
            "PROPERTIES": lambda self: self._parse_wrapped_csv(self._parse_property),
            "DUPLICATE KEY": lambda self: self._parse_duplicate(),
            "PARTITION BY": lambda self: self._parse_partitioned_by(),
        }

        def _parse_create(self) -> exp.Create | exp.Command:
            create = super()._parse_create()

            # Starrocks's primary is defined outside the schema, so we need to move it into the schema
            # https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/#usage
            if (
                isinstance(create, exp.Create)
                and create.this
                and isinstance(create.this, exp.Schema)
            ):
                props = create.args.get("properties")
                if props:
                    primary_key = next(
                        (expr for expr in props.expressions if isinstance(expr, exp.PrimaryKey)),
                        None,
                    )
                    if primary_key:
                        create.this.expressions.append(primary_key)

                        # Remove the PrimaryKey from properties
                        props.expressions.remove(primary_key)

            return create

        def _parse_distributed_by(self, type: str) -> exp.Expression:
            expressions = self._parse_wrapped_csv(self._parse_id_var)

            # If the BUCKETS keyword not present, the number of buckets is AUTO
            # [ BUCKETS AUTO | BUCKETS <number> ]
            buckets = None
            if self._match_text_seq("BUCKETS", "AUTO"):
                pass
            elif self._match_text_seq("BUCKETS"):
                buckets = self._parse_number()

            if self._match_text_seq("ORDER BY"):
                order_by = self._parse_wrapped_csv(self._parse_ordered)
            else:
                order_by = None

            return self.expression(
                exp.DistributedByHashProperty
                if type == "HASH"
                else exp.DistributedByRandomProperty,
                expressions=expressions,
                buckets=buckets,
                sorted_by=order_by,
            )

        def _parse_duplicate(self) -> exp.DuplicateKeyProperty:
            expressions = self._parse_wrapped_csv(self._parse_id_var, optional=False)
            return self.expression(exp.DuplicateKeyProperty, expressions=expressions)

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

    class Generator(MySQL.Generator):
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        JSON_TYPE_REQUIRED_FOR_EXTRACTION = False
        PARSE_JSON_NAME: t.Optional[str] = "PARSE_JSON"
        WITH_PROPERTIES_PREFIX = "PROPERTIES"

        CAST_MAPPING = {}

        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }

        PROPERTIES_LOCATION = {
            **MySQL.Generator.PROPERTIES_LOCATION,
            exp.DistributedByHashProperty: exp.Properties.Location.POST_SCHEMA,
            exp.DistributedByRandomProperty: exp.Properties.Location.POST_SCHEMA,
            exp.DuplicateKeyProperty: exp.Properties.Location.POST_SCHEMA,
            exp.PrimaryKey: exp.Properties.Location.UNSUPPORTED,
        }

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.Array: inline_array_sql,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", unit_to_str(e), e.this, e.expression
            ),
            exp.DistributedByHashProperty: _distributed_by_hash_sql,
            exp.DistributedByRandomProperty: _distributed_by_random_sql,
            exp.DuplicateKeyProperty: _duplicate_key_sql,
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.Property: _property_sql,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.StrToUnix: lambda self, e: self.func("UNIX_TIMESTAMP", e.this, self.format_time(e)),
            exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.UnixToStr: lambda self, e: self.func("FROM_UNIXTIME", e.this, self.format_time(e)),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }

        TRANSFORMS.pop(exp.DateTrunc)
