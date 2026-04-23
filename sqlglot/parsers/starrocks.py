from __future__ import annotations


from sqlglot import exp
from sqlglot.dialects.dialect import build_date_delta_with_interval, build_timestamp_trunc
from sqlglot.helper import seq_get
from sqlglot.parsers.mysql import MySQLParser
from sqlglot.tokens import TokenType


class StarRocksParser(MySQLParser):
    # StarRocks supports LEFT SEMI JOIN and LEFT ANTI JOIN natively
    # https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT_JOIN/
    TABLE_ALIAS_TOKENS = MySQLParser.TABLE_ALIAS_TOKENS - {TokenType.ANTI, TokenType.SEMI}

    FUNCTIONS = {
        **MySQLParser.FUNCTIONS,
        "ADDDATE": build_date_delta_with_interval(exp.DateAdd, default_unit="DAY"),
        "DATE_ADD": build_date_delta_with_interval(exp.DateAdd, default_unit="DAY"),
        "DATE_SUB": build_date_delta_with_interval(exp.DateSub, default_unit="DAY"),
        "SUBDATE": build_date_delta_with_interval(exp.DateSub, default_unit="DAY"),
        "DATE_TRUNC": build_timestamp_trunc,
        "DATEDIFF": lambda args: exp.DateDiff(
            this=seq_get(args, 0), expression=seq_get(args, 1), unit=exp.Literal.string("DAY")
        ),
        "DATE_DIFF": lambda args: exp.DateDiff(
            this=seq_get(args, 1), expression=seq_get(args, 2), unit=seq_get(args, 0)
        ),
        "ARRAY_FLATTEN": exp.Flatten.from_arg_list,
        "REGEXP": exp.RegexpLike.from_arg_list,
    }

    PROPERTY_PARSERS = {
        **MySQLParser.PROPERTY_PARSERS,
        "PROPERTIES": lambda self: self._parse_wrapped_properties(),
        "UNIQUE": lambda self: self._parse_composite_key_property(exp.UniqueKeyProperty),
        "ROLLUP": lambda self: self._parse_rollup_property(),
        "REFRESH": lambda self: self._parse_refresh_property(),
    }

    def _parse_rollup_property(self) -> exp.RollupProperty:
        # ROLLUP (rollup_name (col1, col2) [FROM from_index] [PROPERTIES (...)], ...)
        def parse_rollup_index() -> exp.RollupIndex:
            return self.expression(
                exp.RollupIndex(
                    this=self._parse_id_var(),
                    expressions=self._parse_wrapped_id_vars(),
                    from_index=self._parse_id_var() if self._match_text_seq("FROM") else None,
                    properties=self.expression(
                        exp.Properties(expressions=self._parse_wrapped_properties())
                    )
                    if self._match_text_seq("PROPERTIES")
                    else None,
                )
            )

        return self.expression(
            exp.RollupProperty(expressions=self._parse_wrapped_csv(parse_rollup_index))
        )

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

    def _parse_unnest(self, with_alias: bool = True) -> exp.Unnest | None:
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

    def _parse_partitioned_by(self) -> exp.PartitionedByProperty:
        return self.expression(
            exp.PartitionedByProperty(
                this=exp.Schema(
                    expressions=self._parse_wrapped_csv(self._parse_assignment, optional=True)
                )
            )
        )

    def _parse_partition_property(
        self,
    ) -> exp.Expr | None | list[exp.Expr]:
        expr = super()._parse_partition_property()

        if not expr:
            return self._parse_partitioned_by()

        if isinstance(expr, exp.Property):
            return expr

        self._match_l_paren()

        if self._match_text_seq("START", advance=False):
            create_expressions = self._parse_csv(self._parse_partitioning_granularity_dynamic)
        else:
            create_expressions = None

        self._match_r_paren()

        return self.expression(
            exp.PartitionByRangeProperty(
                partition_expressions=expr, create_expressions=create_expressions
            )
        )

    def _parse_partitioning_granularity_dynamic(self) -> exp.PartitionByRangePropertyDynamic:
        self._match_text_seq("START")
        start = self._parse_wrapped(self._parse_string)
        self._match_text_seq("END")
        end = self._parse_wrapped(self._parse_string)
        self._match_text_seq("EVERY")
        every = self._parse_wrapped(lambda: self._parse_interval() or self._parse_number())
        return self.expression(
            exp.PartitionByRangePropertyDynamic(start=start, end=end, every=every)
        )

    def _parse_refresh_property(self) -> exp.RefreshTriggerProperty:
        """
        REFRESH [DEFERRED | IMMEDIATE]
                [ASYNC | ASYNC [START (<start_time>)] EVERY (INTERVAL <refresh_interval>) | MANUAL]
        """
        method = self._match_texts(("DEFERRED", "IMMEDIATE")) and self._prev.text.upper()
        kind = self._match_texts(("ASYNC", "MANUAL")) and self._prev.text.upper()
        start = self._match_text_seq("START") and self._parse_wrapped(self._parse_string)

        if self._match_text_seq("EVERY"):
            self._match_l_paren()
            self._match_text_seq("INTERVAL")
            every = self._parse_number()
            unit = self._parse_var(any_token=True)
            self._match_r_paren()
        else:
            every = None
            unit = None

        return self.expression(
            exp.RefreshTriggerProperty(
                method=method, kind=kind, starts=start, every=every, unit=unit
            )
        )
