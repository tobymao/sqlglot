from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_date_delta, build_formatted_time
from sqlglot.helper import seq_get
from sqlglot.parsers.spark import SparkParser
from sqlglot.tokens import TokenType


class DatabricksParser(SparkParser):
    LOG_DEFAULTS_TO_LN = True
    STRICT_CAST = True
    COLON_IS_VARIANT_EXTRACT = True
    COLON_CHAIN_IS_SINGLE_EXTRACT = False

    FUNC_TOKENS = parser.Parser.FUNC_TOKENS | {TokenType.AND, TokenType.OR}

    FUNCTIONS = {
        **SparkParser.FUNCTIONS,
        "AND": lambda args: exp.And(this=seq_get(args, 0), expression=seq_get(args, 1)),
        "OR": lambda args: exp.Or(this=seq_get(args, 0), expression=seq_get(args, 1)),
        "IFF": exp.If.from_arg_list,
        "GETDATE": exp.CurrentTimestamp.from_arg_list,
        "DATEDIFF": build_date_delta(exp.DateDiff),
        "DATE_DIFF": build_date_delta(exp.DateDiff),
        "NOW": exp.CurrentTimestamp.from_arg_list,
        "TO_DATE": build_formatted_time(exp.TsOrDsToDate),
        "UNIFORM": lambda args: exp.Uniform(
            this=seq_get(args, 0), expression=seq_get(args, 1), seed=seq_get(args, 2)
        ),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        **SparkParser.NO_PAREN_FUNCTION_PARSERS,
        "CURDATE": lambda self: self._parse_curdate(),
    }

    FUNCTION_PARSERS = {
        **SparkParser.FUNCTION_PARSERS,
        "REGR_AVGX": lambda self: self._parse_distinct_arg_function(exp.RegrAvgx, distinct_index=1),
        "REGR_AVGY": lambda self: self._parse_distinct_arg_function(exp.RegrAvgy),
        "REGR_SXX": lambda self: self._parse_distinct_arg_function(exp.RegrSxx, distinct_index=1),
        "REGR_SXY": lambda self: self._parse_distinct_arg_function(exp.RegrSxy),
        "REGR_SYY": lambda self: self._parse_distinct_arg_function(exp.RegrSyy, distinct_index=1),
    }

    FACTOR = {
        **SparkParser.FACTOR,
        TokenType.COLON: exp.JSONExtract,
    }

    COLUMN_OPERATORS = {
        **parser.Parser.COLUMN_OPERATORS,
        TokenType.QDCOLON: lambda self, this, to: self.build_cast(
            False,
            this=this,
            to=to,
        ),
    }
    CAST_COLUMN_OPERATORS = {
        *SparkParser.CAST_COLUMN_OPERATORS,
        TokenType.QDCOLON,
    }

    def _parse_interval_span(
        self, this: exp.Expr, parse_function_unit: bool = True
    ) -> exp.Interval:
        # AND/OR in FUNC_TOKENS would be consumed as interval units; they never are.
        if self._curr and self._curr.token_type in (TokenType.AND, TokenType.OR):
            parse_function_unit = False
        return super()._parse_interval_span(this, parse_function_unit=parse_function_unit)

    def _parse_curdate(self) -> exp.CurrentDate:
        # CURDATE, an alias for CURRENT_DATE, has optional parentheses
        if self._match(TokenType.L_PAREN):
            self._match_r_paren()
        return self.expression(exp.CurrentDate())

    def _parse_primary_key_part(self) -> exp.Expr | None:
        this = super()._parse_primary_key_part()
        if this and self._match_text_seq("TIMESERIES"):
            return self.expression(exp.TimeseriesKey(this=this))
        return this

    def _parse_cluster_property(self):
        if self._match_texts(("AUTO", "NONE")):
            return self.expression(exp.ClusterProperty(this=self._prev.text.upper()))
        return super()._parse_cluster_property()
