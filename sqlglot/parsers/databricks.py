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

    STATEMENT_PARSERS = {
        **SparkParser.STATEMENT_PARSERS,
        TokenType.OPTIMIZE: lambda self: self._parse_optimize(),
    }

    FUNCTIONS = {
        **SparkParser.FUNCTIONS,
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

    def _parse_optimize(self) -> exp.Optimize | exp.Command:
        start = self._prev
        table = self._parse_table_parts_fast(allow_keywords=True)
        if table and len(table.parts) <= 3:
            optimize = exp.Optimize(this=self.validate_expression(table))
            if self._match(TokenType.FULL, expression=optimize):
                optimize.set("full", True)
            optimize.set("where", self._parse_where())
            if self._match_text_seq("ZORDER", "BY"):
                self._add_comments(optimize)
                columns = self._parse_wrapped_csv(
                    lambda: (
                        self.expression(name.to_column(copy=False), comments=name.comments)
                        if (name := self._parse_table_parts_fast(allow_keywords=True))
                        else self.raise_error("Expected a column name in ZORDER BY")
                    )
                )
                optimize.set("zorder", columns)
            if not self._curr:
                return self.expression(optimize)

        command = self._parse_as_command(start)
        command.set("this", start.text.upper())
        return self.expression(command)

    def _parse_atom(self) -> exp.Expr | None:
        # CURRENT_PATH is a session function in scalar expressions, but a name in USING and DDL.
        if (
            self._curr.token_type == TokenType.VAR
            and self._curr.text.upper() == "CURRENT_PATH"
            and self._next.token_type not in (TokenType.L_PAREN, TokenType.DOT)
        ):
            self._advance()
            return self._parse_colon_as_variant_extract(
                self._parse_column_ops(self.expression(exp.CurrentPath(), self._prev))
            )
        return super()._parse_atom()

    def _parse_curdate(self) -> exp.CurrentDate:
        # CURDATE, an alias for CURRENT_DATE, has optional parentheses
        if self._match(TokenType.L_PAREN):
            self._match_r_paren()
        return self.expression(exp.CurrentDate())

    def _parse_star_op(self, *keywords: str) -> list[exp.Expr] | None:
        if (
            keywords[0] == "EXCEPT"
            and self._next.token_type == TokenType.L_PAREN
            and self._match_texts(keywords)
        ):
            return self._parse_wrapped_csv(self._parse_column)
        return super()._parse_star_op(*keywords)

    def _parse_update_assignment(self) -> exp.Expr | None:
        if self._curr.text.upper() == "CURRENT_PATH" and self._next.token_type == TokenType.EQ:
            column = self._parse_column()
            self._advance()
            comments = self._prev_comments
            return self.expression(
                exp.EQ(this=column, expression=self._parse_disjunction()), comments=comments
            )
        return super()._parse_update_assignment()

    def _parse_value(self, values: bool = True) -> exp.Tuple | None:
        if (
            not values
            and self._prev.token_type == TokenType.INSERT
            and self._curr.token_type == TokenType.L_PAREN
        ):
            return self.expression(
                exp.Tuple(expressions=self._parse_wrapped_csv(self._parse_column))
            )
        return super()._parse_value(values)

    def _parse_primary_key_part(self) -> exp.Expr | None:
        this = super()._parse_primary_key_part()
        if this and self._match_text_seq("TIMESERIES"):
            return self.expression(exp.TimeseriesKey(this=this))
        return this

    def _parse_cluster_property(self):
        if self._match_texts(("AUTO", "NONE")):
            return self.expression(exp.ClusterProperty(this=self._prev.text.upper()))
        return super()._parse_cluster_property()
