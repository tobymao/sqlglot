from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import build_date_delta, build_like
from sqlglot.helper import ensure_list, mypyc_attr, seq_get
from sqlglot.parsers.hive import build_with_ignore_nulls
from sqlglot.parsers.spark2 import Spark2Parser, build_as_cast
from sqlglot.tokens import TokenType


def _build_datediff(args: t.List) -> exp.Expr:
    """
    Although Spark docs don't mention the "unit" argument, Spark3 added support for
    it at some point. Databricks also supports this variant (see below).

    For example, in spark-sql (v3.3.1):
    - SELECT DATEDIFF('2020-01-01', '2020-01-05') results in -4
    - SELECT DATEDIFF(day, '2020-01-01', '2020-01-05') results in 4

    See also:
    - https://docs.databricks.com/sql/language-manual/functions/datediff3.html
    - https://docs.databricks.com/sql/language-manual/functions/datediff.html
    """
    unit = None
    this = seq_get(args, 0)
    expression = seq_get(args, 1)

    if len(args) == 3:
        unit = exp.var(t.cast(exp.Expr, this).name)
        this = args[2]

    return exp.DateDiff(
        this=exp.TsOrDsToDate(this=this), expression=exp.TsOrDsToDate(this=expression), unit=unit
    )


def _build_dateadd(args: t.List) -> exp.Expr:
    expression = seq_get(args, 1)

    if len(args) == 2:
        # DATE_ADD(startDate, numDays INTEGER)
        # https://docs.databricks.com/en/sql/language-manual/functions/date_add.html
        return exp.TsOrDsAdd(
            this=seq_get(args, 0), expression=expression, unit=exp.Literal.string("DAY")
        )

    # DATE_ADD / DATEADD / TIMESTAMPADD(unit, value integer, expr)
    # https://docs.databricks.com/en/sql/language-manual/functions/date_add3.html
    return exp.TimestampAdd(this=seq_get(args, 2), expression=expression, unit=seq_get(args, 0))


@mypyc_attr(allow_interpreted_subclasses=True)
class SparkParser(Spark2Parser):
    SET_PARSERS = {
        **Spark2Parser.SET_PARSERS,
        "VAR": lambda self: self._parse_set_item_assignment("VARIABLE"),
        "VARIABLE": lambda self: self._parse_set_item_assignment("VARIABLE"),
    }

    FUNCTIONS = {
        **Spark2Parser.FUNCTIONS,
        "ANY_VALUE": build_with_ignore_nulls(exp.AnyValue),
        "ARRAY_INSERT": lambda args: exp.ArrayInsert(
            this=seq_get(args, 0),
            position=seq_get(args, 1),
            expression=seq_get(args, 2),
            offset=1,
        ),
        "BIT_AND": exp.BitwiseAndAgg.from_arg_list,
        "BIT_GET": exp.Getbit.from_arg_list,
        "BIT_OR": exp.BitwiseOrAgg.from_arg_list,
        "BIT_XOR": exp.BitwiseXorAgg.from_arg_list,
        "BIT_COUNT": exp.BitwiseCount.from_arg_list,
        "CURDATE": exp.CurrentDate.from_arg_list,
        "DATE_ADD": _build_dateadd,
        "DATEADD": _build_dateadd,
        "MAKE_TIMESTAMP": exp.TimestampFromParts.from_arg_list,
        "TIMESTAMPADD": _build_dateadd,
        "TIMESTAMPDIFF": build_date_delta(exp.TimestampDiff),
        "TRY_ADD": exp.SafeAdd.from_arg_list,
        "TRY_MULTIPLY": exp.SafeMultiply.from_arg_list,
        "TRY_SUBTRACT": exp.SafeSubtract.from_arg_list,
        "DATEDIFF": _build_datediff,
        "DATE_DIFF": _build_datediff,
        "JSON_OBJECT_KEYS": exp.JSONKeys.from_arg_list,
        "LISTAGG": exp.GroupConcat.from_arg_list,
        "TIMESTAMP_LTZ": build_as_cast("TIMESTAMP_LTZ"),
        "TIMESTAMP_NTZ": build_as_cast("TIMESTAMP_NTZ"),
        "TRY_ELEMENT_AT": lambda args: exp.Bracket(
            this=seq_get(args, 0),
            expressions=ensure_list(seq_get(args, 1)),
            offset=1,
            safe=True,
        ),
        "LIKE": build_like(exp.Like),
        "ILIKE": build_like(exp.ILike),
    }

    PLACEHOLDER_PARSERS = {
        **Spark2Parser.PLACEHOLDER_PARSERS,
        TokenType.L_BRACE: lambda self: self._parse_query_parameter(),
    }

    def _parse_query_parameter(self) -> t.Optional[exp.Expr]:
        this = self._parse_id_var()
        self._match(TokenType.R_BRACE)
        return self.expression(exp.Placeholder(this=this, widget=True))

    FUNCTION_PARSERS = {
        **Spark2Parser.FUNCTION_PARSERS,
        "SUBSTR": lambda self: self._parse_substring(),
    }

    STATEMENT_PARSERS = {
        **Spark2Parser.STATEMENT_PARSERS,
        TokenType.DECLARE: lambda self: self._parse_declare(),
    }

    def _parse_generated_as_identity(
        self,
    ) -> (
        exp.GeneratedAsIdentityColumnConstraint
        | exp.ComputedColumnConstraint
        | exp.GeneratedAsRowColumnConstraint
    ):
        this = super()._parse_generated_as_identity()
        if this.expression:
            return self.expression(exp.ComputedColumnConstraint(this=this.expression))
        return this

    def _parse_pivot_aggregation(self) -> t.Optional[exp.Expr]:
        # Spark 3+ and Databricks support non aggregate functions in PIVOT too, e.g
        # PIVOT (..., 'foo' AS bar FOR col_to_pivot IN (...))
        aggregate_expr = self._parse_function() or self._parse_disjunction()
        return self._parse_alias(aggregate_expr)
