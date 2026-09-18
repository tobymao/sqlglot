from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_date_delta, build_formatted_time
from sqlglot.helper import seq_get
from sqlglot.parsers.spark import SparkParser
from sqlglot.tokens import Token, TokenType


class DatabricksParser(SparkParser):
    LOG_DEFAULTS_TO_LN = True
    STRICT_CAST = True
    COLON_IS_VARIANT_EXTRACT = True
    COLON_CHAIN_IS_SINGLE_EXTRACT = False

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

    def _parse_create(self) -> exp.Create | exp.Command:
        start = self._prev
        index = self._index
        replace = self._match_pair(TokenType.OR, TokenType.REPLACE)

        if not self._match_text_seq("POLICY"):
            self._retreat(index)
            return super()._parse_create()

        return self._parse_create_policy(start, replace)

    def _parse_create_policy(self, start: Token, replace: bool) -> exp.Create | exp.Command:
        this = self._parse_id_var()

        if (
            not this
            or not self._match(TokenType.ON)
            or not self._match_texts(("CATALOG", "SCHEMA", "TABLE"))
        ):
            return self._parse_as_command(start)

        scope_kind = self._prev.text.upper()
        scope_name = self._parse_table_parts()

        comment = self._match(TokenType.COMMENT) and self._parse_string()

        if self._match_text_seq("ROW", "FILTER"):
            kind = "ROW FILTER"
        elif self._match_text_seq("COLUMN", "MASK"):
            kind = "COLUMN MASK"
        else:
            return self._parse_as_command(start)

        function = self._parse_table_parts()

        if not self._match_text_seq("TO"):
            self.raise_error("Expected TO after the policy's function name")

        to = self._parse_csv(self._parse_policy_principal)

        except_ = (
            self._parse_csv(self._parse_policy_principal)
            if self._match_text_seq("EXCEPT")
            else None
        )

        if not self._match_text_seq("FOR", "TABLES"):
            self.raise_error("Expected FOR TABLES in policy definition")

        when = self._parse_disjunction() if self._match_text_seq("WHEN") else None

        match_columns = (
            self._parse_csv(self._parse_policy_match_column)
            if self._match_text_seq("MATCH", "COLUMNS")
            else None
        )

        on_column = self._parse_id_var() if self._match_text_seq("ON", "COLUMN") else None

        if kind == "COLUMN MASK" and not on_column:
            self.raise_error("Expected ON COLUMN for a COLUMN MASK policy")

        using_columns = (
            self._parse_wrapped_csv(self._parse_conjunction)
            if self._match_text_seq("USING", "COLUMNS")
            else None
        )

        policy_properties = self.expression(
            exp.PolicyProperties(
                scope_kind=scope_kind,
                scope_name=scope_name,
                kind=kind,
                function=function,
                to=to,
                except_=except_,
                when=when,
                match_columns=match_columns,
                on_column=on_column,
                using_columns=using_columns,
                comment=comment,
            )
        )

        return self.expression(
            exp.Create(
                this=this,
                kind="POLICY",
                replace=replace,
                properties=exp.Properties(expressions=[policy_properties]),
            )
        )

    def _parse_policy_principal(self) -> exp.Expr | None:
        return self._parse_string() or self._parse_id_var(any_token=True)

    def _parse_policy_match_column(self) -> exp.Expr | None:
        condition = self._parse_disjunction()
        if condition is None:
            return None

        if self._match_text_seq("AS"):
            return self.expression(exp.Alias(this=condition, alias=self._parse_id_var()))

        index = self._index
        if self._match_text_seq("ON", "COLUMN") or self._match_text_seq("USING", "COLUMNS"):
            self._retreat(index)
            return condition

        alias = self._parse_id_var(any_token=True)
        return self.expression(exp.Alias(this=condition, alias=alias)) if alias else condition
