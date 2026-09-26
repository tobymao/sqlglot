from __future__ import annotations

import typing as t


from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_date_delta_with_interval
from sqlglot.helper import seq_get
from sqlglot.parsers.mysql import MySQLParser
from sqlglot.tokens import TokenType


# Doris types without a sqlglot data type; they are kept as written.
DORIS_USER_DEFINED_TYPES = frozenset(
    {
        "AGG_STATE",
        "BITMAP",
        "DATETIMEV1",
        "DATETIMEV2",
        "DATEV1",
        "DATEV2",
        "DECIMALV2",
        "DECIMALV3",
        "HLL",
        "QUANTILE_STATE",
    }
)

# Aggregate model column specs, e.g. "b BITMAP BITMAP_UNION"
DORIS_AGGREGATION_TYPES = (
    "BITMAP_UNION",
    "GENERIC",
    "HLL_UNION",
    "MAX",
    "MIN",
    "QUANTILE_UNION",
    "REPLACE",
    "REPLACE_IF_NOT_NULL",
    "SUM",
)


def _aggregation_type_parser(name: str) -> t.Callable[[MySQLParser], exp.Expr]:
    return lambda self: exp.var(name)


# Accept both DATE_TRUNC(datetime, unit) and DATE_TRUNC(unit, datetime)
def _build_date_trunc(args: list[exp.Expr]) -> exp.Expr:
    a0, a1 = seq_get(args, 0), seq_get(args, 1)

    def _is_unit_like(e: exp.Expr | None) -> bool:
        if not (isinstance(e, exp.Literal) and e.is_string):
            return False
        text = e.this
        return not any(ch.isdigit() for ch in text)

    # Determine which argument is the unit
    unit, this = (a0, a1) if _is_unit_like(a0) else (a1, a0)

    return exp.TimestampTrunc(this=this, unit=unit)


class DorisParser(MySQLParser):
    FUNCTIONS = {
        **MySQLParser.FUNCTIONS,
        "ADDDATE": build_date_delta_with_interval(exp.DateAdd, default_unit="DAY"),
        "COLLECT_SET": exp.ArrayUniqueAgg.from_arg_list,
        "DATE_ADD": build_date_delta_with_interval(exp.DateAdd, default_unit="DAY"),
        "DATE_SUB": build_date_delta_with_interval(exp.DateSub, default_unit="DAY"),
        "DATE_TRUNC": _build_date_trunc,
        "L2_DISTANCE": exp.EuclideanDistance.from_arg_list,
        "MAP": parser.build_var_map,
        "MONTHS_ADD": exp.AddMonths.from_arg_list,
        "REGEXP": exp.RegexpLike.from_arg_list,
        "SUBDATE": build_date_delta_with_interval(exp.DateSub, default_unit="DAY"),
        "TO_DATE": exp.TsOrDsToDate.from_arg_list,
    }

    FUNCTION_PARSERS = {
        **MySQLParser.FUNCTION_PARSERS,
        "GROUP_CONCAT": lambda self: self._parse_doris_group_concat(),
    }

    CONSTRAINT_PARSERS = {
        **MySQLParser.CONSTRAINT_PARSERS,
        **{name: _aggregation_type_parser(name) for name in DORIS_AGGREGATION_TYPES},
    }

    NO_PAREN_FUNCTIONS = {
        k: v for k, v in MySQLParser.NO_PAREN_FUNCTIONS.items() if k != TokenType.CURRENT_DATE
    }

    PROPERTY_PARSERS = {
        **MySQLParser.PROPERTY_PARSERS,
        "PROPERTIES": lambda self: self._parse_wrapped_properties(),
        "UNIQUE": lambda self: self._parse_composite_key_property(exp.UniqueKeyProperty),
        # Plain KEY without UNIQUE/DUPLICATE/AGGREGATE prefixes should be treated as UniqueKeyProperty with unique=False
        "KEY": lambda self: self._parse_composite_key_property(exp.UniqueKeyProperty),
        "BUILD": lambda self: self._parse_build_property(),
        "REFRESH": lambda self: self._parse_refresh_property(),
    }

    def _parse_types(
        self,
        check_func: bool = False,
        schema: bool = False,
        allow_identifiers: bool = True,
        with_collation: bool = False,
    ) -> exp.Expr | None:
        token = self._curr
        if (
            token
            and token.token_type == TokenType.VAR
            and token.text.upper() in DORIS_USER_DEFINED_TYPES
        ):
            index = self._index
            self._advance()
            end = token

            # Optional parameters, e.g. DECIMALV3(10, 2) or AGG_STATE<MAX_BY(INT, INT)>
            for opening, closing in (
                (TokenType.L_PAREN, TokenType.R_PAREN),
                (TokenType.LT, TokenType.GT),
            ):
                if not self._match(opening):
                    continue

                depth = 1
                while self._curr and depth:
                    if self._curr.token_type == opening:
                        depth += 1
                    elif self._curr.token_type == closing:
                        depth -= 1
                    end = self._curr
                    self._advance()

                if depth:
                    self._retreat(index)
                    return super()._parse_types(
                        check_func=check_func,
                        schema=schema,
                        allow_identifiers=allow_identifiers,
                        with_collation=with_collation,
                    )
                break

            return exp.DataType(this=exp.DType.USERDEFINED, kind=self._find_sql(token, end))

        return super()._parse_types(
            check_func=check_func,
            schema=schema,
            allow_identifiers=allow_identifiers,
            with_collation=with_collation,
        )

    def _parse_bracket(self, this: exp.Expr | None = None) -> exp.Expr | None:
        if not self._match(TokenType.L_BRACE):
            return super()._parse_bracket(this)

        if (
            self._curr
            and self._curr.token_type == TokenType.VAR
            and self._curr.text.lower() in self.ODBC_DATETIME_LITERALS
        ):
            return self._parse_odbc_datetime_literal()

        # Doris {k: v, ...} is a MAP literal
        keys: list[exp.Expr] = []
        values: list[exp.Expr] = []
        if not self._match(TokenType.R_BRACE):
            while True:
                key = self._parse_disjunction()
                if not self._match(TokenType.COLON):
                    self.raise_error("Expected :")
                value = self._parse_disjunction()
                if key:
                    keys.append(key)
                if value:
                    values.append(value)
                if not self._match(TokenType.COMMA):
                    break
            if not self._match(TokenType.R_BRACE):
                self.raise_error("Expected }")

        return self.expression(exp.VarMap(keys=exp.array(*keys), values=exp.array(*values)))

    def _parse_doris_group_concat(self) -> exp.Expr:
        args = self._parse_csv(self._parse_lambda)
        order = args[-1] if args and isinstance(args[-1], exp.Order) else None
        if order:
            args[-1] = order.this

        separator = self._parse_field() if self._match(TokenType.SEPARATOR) else None
        if separator is None and len(args) == 2:
            # GROUP_CONCAT(expr, separator)
            args, separator = args[:1], args[1]

        this: exp.Expr | None = None
        if len(args) == 1:
            this = args[0]
        elif args:
            this = exp.Concat(expressions=args, safe=True)

        if order:
            order.set("this", this)
            this = order

        return self.expression(exp.GroupConcat(this=this, separator=separator))

    def _parse_partition_property(
        self,
    ) -> exp.Expr | None | list[exp.Expr]:
        expr = super()._parse_partition_property()

        if not expr:
            return self._parse_partitioned_by()

        if isinstance(expr, exp.Property):
            return expr

        self._match_l_paren()

        if self._match_text_seq("FROM", advance=False):
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
        self._match_text_seq("FROM")
        start = self._parse_wrapped(self._parse_string)
        self._match_text_seq("TO")
        end = self._parse_wrapped(self._parse_string)
        self._match_text_seq("INTERVAL")
        number = self._parse_number()
        unit = self._parse_var(any_token=True)
        every = self.expression(exp.Interval(this=number, unit=unit))
        return self.expression(
            exp.PartitionByRangePropertyDynamic(start=start, end=end, every=every)
        )

    def _parse_partition_range_value(self) -> exp.Expr | None:
        expr = super()._parse_partition_range_value()

        if isinstance(expr, exp.Partition):
            return expr

        self._match_text_seq("VALUES")
        name = expr

        # Doris-specific bracket syntax: VALUES [(...), (...))
        self._match(TokenType.L_BRACKET)
        values = self._parse_csv(lambda: self._parse_wrapped_csv(self._parse_expression))

        self._match(TokenType.R_BRACKET)
        self._match(TokenType.R_PAREN)

        part_range = self.expression(exp.PartitionRange(this=name, expressions=values))
        return self.expression(exp.Partition(expressions=[part_range]))

    def _parse_build_property(self) -> exp.BuildProperty:
        return self.expression(exp.BuildProperty(this=self._parse_var(upper=True)))

    def _parse_refresh_property(self) -> exp.RefreshTriggerProperty:
        method = self._parse_var(upper=True)

        self._match(TokenType.ON)

        kind = self._match_texts(("MANUAL", "COMMIT", "SCHEDULE")) and self._prev.text.upper()
        every = self._match_text_seq("EVERY") and self._parse_number()
        unit = self._parse_var(any_token=True) if every else None
        starts = self._match_text_seq("STARTS") and self._parse_string()

        return self.expression(
            exp.RefreshTriggerProperty(
                method=method, kind=kind, every=every, unit=unit, starts=starts
            )
        )
