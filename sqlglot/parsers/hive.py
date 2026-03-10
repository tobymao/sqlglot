from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_formatted_time, build_regexp_extract
from sqlglot.helper import mypyc_attr, seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import F


def build_with_ignore_nulls(
    exp_class: t.Type[exp.Expr],
) -> t.Callable[[t.List[exp.Expr]], exp.Expr]:
    def _parse(args: t.List[exp.Expr]) -> exp.Expr:
        this = exp_class(this=seq_get(args, 0))
        if seq_get(args, 1) == exp.true():
            return exp.IgnoreNulls(this=this)
        return this

    return _parse


def _build_to_date(args: t.List) -> exp.TsOrDsToDate:
    expr = build_formatted_time(exp.TsOrDsToDate, "hive")(args)
    expr.set("safe", True)
    return expr


def _build_date_add(args: t.List) -> exp.TsOrDsAdd:
    expression = seq_get(args, 1)
    if expression:
        expression = expression * -1

    return exp.TsOrDsAdd(
        this=seq_get(args, 0), expression=expression, unit=exp.Literal.string("DAY")
    )


@mypyc_attr(allow_interpreted_subclasses=True)
class HiveParser(parser.Parser):
    LOG_DEFAULTS_TO_LN = True
    STRICT_CAST = False
    VALUES_FOLLOWED_BY_PAREN = False
    JOINS_HAVE_EQUAL_PRECEDENCE = True
    ADD_JOIN_ON_TRUE = True
    ALTER_TABLE_PARTITIONS = True

    CHANGE_COLUMN_ALTER_SYNTAX = False
    # Whether the dialect supports using ALTER COLUMN syntax with CHANGE COLUMN.

    FUNCTION_PARSERS = {
        **parser.Parser.FUNCTION_PARSERS,
        "PERCENTILE": lambda self: self._parse_quantile_function(exp.Quantile),
        "PERCENTILE_APPROX": lambda self: self._parse_quantile_function(exp.ApproxQuantile),
    }

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "BASE64": exp.ToBase64.from_arg_list,
        "COLLECT_LIST": lambda args: exp.ArrayAgg(this=seq_get(args, 0), nulls_excluded=True),
        "COLLECT_SET": exp.ArrayUniqueAgg.from_arg_list,
        "DATE_ADD": lambda args: exp.TsOrDsAdd(
            this=seq_get(args, 0), expression=seq_get(args, 1), unit=exp.Literal.string("DAY")
        ),
        "DATE_FORMAT": lambda args: build_formatted_time(exp.TimeToStr, "hive")(
            [
                exp.TimeStrToTime(this=seq_get(args, 0)),
                seq_get(args, 1),
            ]
        ),
        "DATE_SUB": _build_date_add,
        "DATEDIFF": lambda args: exp.DateDiff(
            this=exp.TsOrDsToDate(this=seq_get(args, 0)),
            expression=exp.TsOrDsToDate(this=seq_get(args, 1)),
        ),
        "DAY": lambda args: exp.Day(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "FIRST": build_with_ignore_nulls(exp.First),
        "FIRST_VALUE": build_with_ignore_nulls(exp.FirstValue),
        "FROM_UNIXTIME": build_formatted_time(exp.UnixToStr, "hive", True),
        "GET_JSON_OBJECT": lambda args, dialect: exp.JSONExtractScalar(
            this=seq_get(args, 0), expression=dialect.to_json_path(seq_get(args, 1))
        ),
        "LAST": build_with_ignore_nulls(exp.Last),
        "LAST_VALUE": build_with_ignore_nulls(exp.LastValue),
        "MAP": parser.build_var_map,
        "MONTH": lambda args: exp.Month(this=exp.TsOrDsToDate.from_arg_list(args)),
        "REGEXP_EXTRACT": build_regexp_extract(exp.RegexpExtract),
        "REGEXP_EXTRACT_ALL": build_regexp_extract(exp.RegexpExtractAll),
        "SEQUENCE": exp.GenerateSeries.from_arg_list,
        "SIZE": exp.ArraySize.from_arg_list,
        "SPLIT": exp.RegexpSplit.from_arg_list,
        "STR_TO_MAP": lambda args: exp.StrToMap(
            this=seq_get(args, 0),
            pair_delim=seq_get(args, 1) or exp.Literal.string(","),
            key_value_delim=seq_get(args, 2) or exp.Literal.string(":"),
        ),
        "TO_DATE": _build_to_date,
        "TO_JSON": exp.JSONFormat.from_arg_list,
        "TRUNC": exp.TimestampTrunc.from_arg_list,
        "UNBASE64": exp.FromBase64.from_arg_list,
        "UNIX_TIMESTAMP": lambda args: build_formatted_time(exp.StrToUnix, "hive", True)(
            args or [exp.CurrentTimestamp()]
        ),
        "YEAR": lambda args: exp.Year(this=exp.TsOrDsToDate.from_arg_list(args)),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        **parser.Parser.NO_PAREN_FUNCTION_PARSERS,
        "TRANSFORM": lambda self: self._parse_transform(),
    }

    NO_PAREN_FUNCTIONS = {
        k: v for k, v in parser.Parser.NO_PAREN_FUNCTIONS.items() if k != TokenType.CURRENT_TIME
    }

    PROPERTY_PARSERS = {
        **parser.Parser.PROPERTY_PARSERS,
        "SERDEPROPERTIES": lambda self: exp.SerdeProperties(
            expressions=self._parse_wrapped_csv(self._parse_property)
        ),
    }

    ALTER_PARSERS = {
        **parser.Parser.ALTER_PARSERS,
        "CHANGE": lambda self: self._parse_alter_table_change(),
    }

    def _parse_transform(self) -> t.Optional[exp.Transform | exp.QueryTransform]:
        if not self._match(TokenType.L_PAREN, advance=False):
            self._retreat(self._index - 1)
            return None

        args = self._parse_wrapped_csv(self._parse_lambda)
        row_format_before = self._parse_row_format(match_row=True)

        record_writer = None
        if self._match_text_seq("RECORDWRITER"):
            record_writer = self._parse_string()

        if not self._match(TokenType.USING):
            return exp.Transform.from_arg_list(args)

        command_script = self._parse_string()

        self._match(TokenType.ALIAS)
        schema = self._parse_schema()

        row_format_after = self._parse_row_format(match_row=True)
        record_reader = None
        if self._match_text_seq("RECORDREADER"):
            record_reader = self._parse_string()

        return self.expression(
            exp.QueryTransform(
                expressions=args,
                command_script=command_script,
                schema=schema,
                row_format_before=row_format_before,
                record_writer=record_writer,
                row_format_after=row_format_after,
                record_reader=record_reader,
            )
        )

    def _parse_quantile_function(self, func: t.Type[F]) -> F:
        if self._match(TokenType.DISTINCT):
            first_arg: t.Optional[exp.Expr] = self.expression(
                exp.Distinct(expressions=[self._parse_lambda()])
            )
        else:
            self._match(TokenType.ALL)
            first_arg = self._parse_lambda()

        args = [first_arg]
        if self._match(TokenType.COMMA):
            args.extend(self._parse_function_args())

        return func.from_arg_list(args)

    def _parse_types(
        self, check_func: bool = False, schema: bool = False, allow_identifiers: bool = True
    ) -> t.Optional[exp.Expr]:
        """
        Spark (and most likely Hive) treats casts to CHAR(length) and VARCHAR(length) as casts to
        STRING in all contexts except for schema definitions. For example, this is in Spark v3.4.0:

            spark-sql (default)> select cast(1234 as varchar(2));
            23/06/06 15:51:18 WARN CharVarcharUtils: The Spark cast operator does not support
            char/varchar type and simply treats them as string type. Please use string type
            directly to avoid confusion. Otherwise, you can set spark.sql.legacy.charVarcharAsString
            to true, so that Spark treat them as string type as same as Spark 3.0 and earlier

            1234
            Time taken: 4.265 seconds, Fetched 1 row(s)

        This shows that Spark doesn't truncate the value into '12', which is inconsistent with
        what other dialects (e.g. postgres) do, so we need to drop the length to transpile correctly.

        Reference: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
        """
        this = super()._parse_types(
            check_func=check_func, schema=schema, allow_identifiers=allow_identifiers
        )

        if this and not schema:
            return this.transform(
                lambda node: (
                    node.replace(exp.DataType.build("text"))
                    if isinstance(node, exp.DataType) and node.is_type("char", "varchar")
                    else node
                ),
                copy=False,
            )

        return this

    def _parse_alter_table_change(self) -> t.Optional[exp.Expr]:
        self._match(TokenType.COLUMN)
        this = self._parse_field(any_token=True)

        if self.CHANGE_COLUMN_ALTER_SYNTAX and self._match_text_seq("TYPE"):
            return self.expression(exp.AlterColumn(this=this, dtype=self._parse_types(schema=True)))

        column_new = self._parse_field(any_token=True)
        dtype = self._parse_types(schema=True)

        comment = self._match(TokenType.COMMENT) and self._parse_string()

        if not this or not column_new or not dtype:
            self.raise_error(
                "Expected 'CHANGE COLUMN' to be followed by 'column_name' 'column_name' 'data_type'"
            )

        return self.expression(
            exp.AlterColumn(this=this, rename_to=column_new, dtype=dtype, comment=comment)
        )

    def _parse_partition_and_order(
        self,
    ) -> t.Tuple[t.List[exp.Expr], t.Optional[exp.Expr]]:
        return (
            (
                self._parse_csv(self._parse_assignment)
                if self._match_set({TokenType.PARTITION_BY, TokenType.DISTRIBUTE_BY})
                else []
            ),
            super()._parse_order(skip_order_token=self._match(TokenType.SORT_BY)),
        )

    def _parse_parameter(self) -> exp.Parameter:
        self._match(TokenType.L_BRACE)
        this = self._parse_identifier() or self._parse_primary_or_var()
        expression = self._match(TokenType.COLON) and (
            self._parse_identifier() or self._parse_primary_or_var()
        )
        self._match(TokenType.R_BRACE)
        return self.expression(exp.Parameter(this=this, expression=expression))

    def _to_prop_eq(self, expression: exp.Expr, index: int) -> exp.Expr:
        if expression.is_star:
            return expression

        if isinstance(expression, exp.Column):
            key = expression.this
        else:
            key = exp.to_identifier(f"col{index + 1}")

        return self.expression(exp.PropertyEQ(this=key, expression=expression))
