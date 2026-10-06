from __future__ import annotations


from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_date_delta_with_interval, build_timestamp_trunc
from sqlglot.helper import ensure_list, seq_get
from sqlglot.parsers.mysql import MySQLParser
from sqlglot.tokens import TokenType

# https://docs.starrocks.io/docs/table_design/table_types/aggregate_table/
AGGREGATE_COLUMN_CONSTRAINTS = (
    "SUM",
    "MAX",
    "MIN",
    "REPLACE",
    "REPLACE_IF_NOT_NULL",
    "BITMAP_UNION",
    "HLL_UNION",
)


def _build_time_slice(args: list[exp.Expr]) -> exp.TimeSlice:
    # TIME_SLICE(dt, INTERVAL n unit [, boundary])
    # https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/time_slice/
    interval = seq_get(args, 1)
    if not isinstance(interval, exp.Interval):
        return exp.TimeSlice.from_arg_list(args)

    return exp.TimeSlice(
        this=seq_get(args, 0),
        expression=interval.this,
        unit=interval.args.get("unit"),
        kind=seq_get(args, 2),
    )


class StarRocksParser(MySQLParser):
    # Unlike MySQL, dropping a column requires the COLUMN keyword
    ALTER_DROP_REQUIRES_COLUMN = True

    # StarRocks supports LEFT SEMI JOIN and LEFT ANTI JOIN natively
    # https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT_JOIN/
    TABLE_ALIAS_TOKENS = MySQLParser.TABLE_ALIAS_TOKENS - {TokenType.ANTI, TokenType.SEMI}

    # https://docs.starrocks.io/docs/sql-reference/sql-statements/generated_columns/
    WRAPPED_TRANSFORM_COLUMN_CONSTRAINT = False

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
        # StarRocks' MAP() is a variadic constructor: MAP(k1, v1, k2, v2, ...)
        # https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map/
        "MAP": parser.build_var_map,
        # TABLE(<tvf>) wraps a table function invocation whose arguments are constants
        # https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/generate_series/
        "TABLE": lambda args: exp.TableFromRows(this=seq_get(args, 0)),
        "TIME_SLICE": _build_time_slice,
    }

    PROPERTY_PARSERS = {
        **MySQLParser.PROPERTY_PARSERS,
        "PROPERTIES": lambda self: self._parse_wrapped_properties(),
        "UNIQUE": lambda self: self._parse_composite_key_property(exp.UniqueKeyProperty),
        "ROLLUP": lambda self: self._parse_rollup_property(),
        "REFRESH": lambda self: self._parse_refresh_property(),
    }

    CONSTRAINT_PARSERS = {
        **MySQLParser.CONSTRAINT_PARSERS,
        **dict.fromkeys(
            AGGREGATE_COLUMN_CONSTRAINTS, lambda self: exp.var(self._prev.text.upper())
        ),
    }

    def _parse_rollup_property(self) -> exp.RollupProperty:
        # ROLLUP (rollup_name (col1, col2) [FROM from_index] [PROPERTIES (...)], ...)
        return self.expression(
            exp.RollupProperty(expressions=self._parse_wrapped_csv(self._parse_rollup_index))
        )

    def _parse_rollup_index(self) -> exp.RollupIndex:
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

    def _parse_alter_table_add(self) -> list[exp.Expr]:
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#rollup
        if self._match_text_seq("ROLLUP"):
            return self._parse_csv(self._parse_rollup_index)

        # ADD COLUMN (c1 INT, c2 INT)
        index = self._index
        if self._match(TokenType.COLUMN) and self._match(TokenType.L_PAREN, advance=False):
            return ensure_list(self._parse_schema())
        self._retreat(index)

        return super()._parse_alter_table_add()

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

    def _parse_partition(self) -> exp.Partition | None:
        # Also accept an unparenthesized partition name, e.g. DROP PARTITION p1
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#drop-partition
        if not self._match_texts(self.PARTITION_KEYWORDS, advance=False) or (
            self._next and self._next.token_type == TokenType.L_PAREN
        ):
            return super()._parse_partition()

        subpartition = self._advance_any() and self._prev.text.upper() == "SUBPARTITION"
        return self.expression(
            exp.Partition(subpartition=subpartition, expressions=[self._parse_disjunction()])
        )

    def _parse_partition_range_value(self) -> exp.Expr | None:
        expr = super()._parse_partition_range_value()
        if isinstance(expr, exp.Partition) or not self._match_text_seq("VALUES"):
            return expr

        # PARTITION p1 VALUES [(lower), (upper))
        # https://docs.starrocks.io/docs/table_design/data_distribution/#range-partitioning
        self._match(TokenType.L_BRACKET)
        values = self._parse_csv(lambda: self._parse_wrapped_csv(self._parse_expression))
        self._match(TokenType.R_PAREN)

        part_range = self.expression(exp.PartitionRange(this=expr, expressions=values))
        return self.expression(exp.Partition(expressions=[part_range]))

    def _parse_refresh_property(self) -> exp.RefreshTriggerProperty:
        """
        REFRESH [DEFERRED | IMMEDIATE]
                [ASYNC | ASYNC [START (<start_time>)] EVERY (INTERVAL <refresh_interval>)
                 | MANUAL | SCHEDULE [START (<start_time>)] EVERY (INTERVAL <refresh_interval>)]
        """
        method = self._match_texts(("DEFERRED", "IMMEDIATE")) and self._prev.text.upper()
        kind = self._match_texts(("ASYNC", "MANUAL", "SCHEDULE")) and self._prev.text.upper()
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

    def _parse_kill(self) -> exp.Kill:
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/cbo_stats/KILL_ANALYZE/
        if self._match_text_seq("ANALYZE"):
            return self.expression(exp.Kill(this=self._parse_primary(), kind=exp.var("ANALYZE")))
        return super()._parse_kill()

    def _parse_refresh(self) -> exp.Refresh | exp.Command:
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/dictionary/REFRESH_DICTIONARY/
        if self._match_text_seq("DICTIONARY"):
            return self.expression(exp.Refresh(this=self._parse_table_parts(), kind="DICTIONARY"))
        if self._match_text_seq("CONNECTIONS"):
            return self.expression(exp.Refresh(this=exp.var("CONNECTIONS"), kind="CONNECTIONS"))
        if not self._match_text_seq("MATERIALIZED", "VIEW"):
            return super()._parse_refresh()

        # Not _parse_table, which would consume FORCE / PARTITION as index hints
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/REFRESH_MATERIALIZED_VIEW/
        this = self._parse_table_parts()
        force = self._match_text_seq("FORCE")

        partition_start = None
        partition_end = None
        if self._match_text_seq("PARTITION", "START"):
            partition_start = self._parse_wrapped(self._parse_string)
            self._match_text_seq("END")
            partition_end = self._parse_wrapped(self._parse_string)
            force = self._match_text_seq("FORCE") or force

        mode = (
            self._match_text_seq("WITH")
            and self._match_texts(("SYNC", "ASYNC"))
            and self._prev.text.upper()
        )
        self._match_text_seq("MODE")

        return self.expression(
            exp.Refresh(
                this=this,
                kind="MATERIALIZED VIEW",
                force=force,
                partition_start=partition_start,
                partition_end=partition_end,
                mode=mode,
            )
        )

    def _parse_show_db(self) -> exp.Expr | None:
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/SHOW_TABLES/
        return self._parse_table_parts(is_db_reference=True)

    def _parse_show_mysql(
        self,
        this: str,
        target: bool | str = False,
        full: bool | None = None,
        global_: bool | None = None,
    ) -> exp.Show:
        show = super()._parse_show_mysql(this, target=target, full=full, global_=global_)

        # SHOW CREATE FUNCTION f(INT)
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/Function/SHOW_CREATE_FUNCTION/
        if this == "CREATE FUNCTION" and self._match(TokenType.L_PAREN, advance=False):
            show.set(
                "target",
                self.expression(
                    exp.UserDefinedFunction(
                        this=show.args.get("target"),
                        expressions=self._parse_wrapped_csv(self._parse_types),
                        wrapped=True,
                    )
                ),
            )

        return show

    def _parse_index_constraint_options(self) -> list[exp.IndexConstraintOption]:
        options = super()._parse_index_constraint_options()

        # USING GIN ('parser' = 'english')
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX/
        if (
            options
            and options[-1].args.get("using")
            and self._match(TokenType.L_PAREN, advance=False)
        ):
            options[-1].set(
                "properties",
                self.expression(exp.Properties(expressions=self._parse_wrapped_properties())),
            )
            options.extend(super()._parse_index_constraint_options())

        return options

    def _parse_insert(self) -> exp.Insert | exp.MultitableInserts:
        insert = super()._parse_insert()
        if isinstance(insert, exp.Insert) and insert.this:
            insert.set("label", insert.this.meta.pop("label", None))
        return insert

    def _parse_insert_table(self) -> exp.Expr | None:
        # https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/
        if (
            self._curr
            and self._curr.text.upper() == "FILES"
            and self._next
            and self._next.token_type == TokenType.L_PAREN
        ):
            return self._parse_table()

        this = super()._parse_insert_table()

        # INSERT INTO t WITH LABEL l (c1, c2)
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/
        if isinstance(this, exp.Table) and self._match_text_seq("WITH", "LABEL"):
            label = self._parse_id_var()
            columns = (
                self._parse_wrapped_id_vars()
                if self._match(TokenType.L_PAREN, advance=False)
                else None
            )
            this = self.expression(exp.Schema(this=this, expressions=columns))
            this.meta["label"] = label

        return this

    def _parse_statement(self) -> exp.Expr | None:
        start = self._curr
        if not start:
            return None

        # https://docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/sql_blacklist/
        # https://docs.starrocks.io/docs/administration/management/BE_blacklist/
        index = self._index
        if self._match_texts(("ADD", "DELETE")) and (
            self._match_text_seq("SQLBLACKLIST")
            or self._match_text_seq("BACKEND", "BLACKLIST")
            or self._match_text_seq("COMPUTE", "NODE", "BLACKLIST")
        ):
            return self._parse_as_command(start)
        self._retreat(index)

        # https://docs.starrocks.io/docs/sql-reference/sql-statements/TRANSLATE_TRINO/
        if self._match_text_seq("TRANSLATE", "TRINO"):
            return self._parse_as_command(start)

        return super()._parse_statement()
