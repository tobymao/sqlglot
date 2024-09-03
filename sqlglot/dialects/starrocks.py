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


class StarRocks(MySQL):
    STRICT_JSON_PATH_SYNTAX = False

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
            "PROPERTIES": lambda self: self._parse_wrapped_properties(),
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

        def _parse_distributed_property(self) -> exp.DistributedByProperty:
            kind = "HASH"
            expressions: t.Optional[t.List[exp.Expression]] = None
            if self._match_text_seq("BY", "HASH"):
                expressions = self._parse_wrapped_csv(self._parse_id_var)
            elif self._match_text_seq("BY", "RANDOM"):
                kind = "RANDOM"

            # If the BUCKETS keyword is not present, the number of buckets is AUTO
            buckets: t.Optional[exp.Expression] = None
            if self._match_text_seq("BUCKETS") and not self._match_text_seq("AUTO"):
                buckets = self._parse_number()

            return self.expression(
                exp.DistributedByProperty,
                expressions=expressions,
                kind=kind,
                buckets=buckets,
                order=self._parse_order(),
            )

        def _parse_duplicate(self) -> exp.DuplicateKeyProperty:
            self._match_text_seq("KEY")
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
            exp.PrimaryKey: exp.Properties.Location.POST_SCHEMA,
        }

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.Array: inline_array_sql,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", unit_to_str(e), e.this, e.expression
            ),
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.Property: property_sql,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.StrToUnix: lambda self, e: self.func("UNIX_TIMESTAMP", e.this, self.format_time(e)),
            exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.UnixToStr: lambda self, e: self.func("FROM_UNIXTIME", e.this, self.format_time(e)),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }

        def create_sql(self, e: exp.Create) -> str:
            # Starrocks' primary key is defined outside of the schema, so we need to move it there
            schema = e.this
            if isinstance(schema, exp.Schema):
                primary_key = schema.find(exp.PrimaryKey)

                if primary_key:
                    props = e.args.get("properties")

                    if not props:
                        props = exp.Properties(expressions=[])
                        e.set("properties", props)

                    # Verify if the first one is an engine property. Is true then insert it after the engine,
                    # otherwise insert it at the beginning
                    engine = props.find(exp.EngineProperty)
                    engine_index = (engine.index or 0) if engine else -1
                    props.expressions.insert(engine_index + 1, primary_key.pop())

            return super().create_sql(e)

        TRANSFORMS.pop(exp.DateTrunc)
