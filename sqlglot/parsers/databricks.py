from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_date_delta, build_formatted_time
from sqlglot.errors import TokenError
from sqlglot.helper import seq_get
from sqlglot.parsers.spark import SparkParser
from sqlglot.tokens import TokenType


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

    def _parse_statement(self) -> exp.Expr | None:
        if (
            self._curr.token_type != TokenType.COMMAND
            or self._curr.text.upper() != "OPTIMIZE"
            or self._next.token_type != TokenType.STRING
            or self._curr.comments
        ):
            return super()._parse_statement()

        # A SELECT prefix prevents command keywords from swallowing qualified names.
        body = "SELECT " + self._next.text
        try:
            tokens = self.dialect.tokenize(body)[1:]
        except TokenError:
            return super()._parse_statement()
        if (
            len(tokens) not in (1, 3, 5)
            or any(token.comments for token in tokens)
            or any(token.token_type != TokenType.DOT for token in tokens[1::2])
        ):
            return super()._parse_statement()
        for token in tokens[::2]:
            name = token.text
            if not (
                token.token_type != TokenType.NUMBER
                and name
                and (
                    (token.token_type == TokenType.IDENTIFIER and body[token.start] == "`")
                    or (
                        body[token.start : token.end + 1] == name
                        and name.isascii()
                        and name.replace("_", "a").isalnum()
                        and not name.isdigit()
                    )
                )
            ):
                return super()._parse_statement()

        offset = self.sql.find(self._next.text, self._curr.end + 1)
        if offset < 0:
            return super()._parse_statement()
        offset -= len("SELECT ")
        parts = []
        for token in tokens[::2]:
            part = self.expression(
                exp.Identifier(this=token.text, quoted=token.token_type == TokenType.IDENTIFIER),
                token,
            )
            start, end = token.start + offset, token.end + offset
            prefix = self.sql[: end + 1]
            part.meta.update(
                line=1 + prefix.count("\n") + prefix.count("\r") - prefix.count("\r\n"),
                col=end - max(prefix.rfind("\n"), prefix.rfind("\r")),
                start=start,
                end=end,
            )
            parts.append(part)
        this = self.expression(
            exp.Table(this=parts[-1], db=seq_get(parts, -2), catalog=seq_get(parts, -3))
        )
        self._advance(2)
        return self.expression(exp.Optimize(this=this))

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
