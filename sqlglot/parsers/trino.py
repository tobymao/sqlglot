from __future__ import annotations


from sqlglot import exp, parser
from sqlglot.parsers.presto import PrestoParser
from sqlglot.tokens import TokenType


class TrinoParser(PrestoParser):
    NO_PAREN_FUNCTIONS = {
        **PrestoParser.NO_PAREN_FUNCTIONS,
        TokenType.CURRENT_CATALOG: exp.CurrentCatalog,
    }

    FUNCTIONS = {
        **PrestoParser.FUNCTIONS,
        "VERSION": exp.CurrentVersion.from_arg_list,
    }

    FUNCTION_PARSERS = {
        **PrestoParser.FUNCTION_PARSERS,
        "TRIM": lambda self: self._parse_trim(),
        "JSON_QUERY": lambda self: self._parse_json_query(),
        "JSON_VALUE": lambda self: self._parse_json_value(),
        "LISTAGG": lambda self: self._parse_string_agg(),
    }

    JSON_QUERY_OPTIONS: parser.OPTIONS_TYPE = {
        **dict.fromkeys(
            ("WITH", "WITHOUT"),
            (
                ("WRAPPER"),
                ("ARRAY", "WRAPPER"),
                ("CONDITIONAL", "WRAPPER"),
                ("CONDITIONAL", "ARRAY", "WRAPPED"),
                ("UNCONDITIONAL", "WRAPPER"),
                ("UNCONDITIONAL", "ARRAY", "WRAPPER"),
            ),
        ),
    }

    def _parse_json_query_quote(self) -> exp.JSONExtractQuote | None:
        if not (self._match_text_seq("KEEP", "QUOTES") or self._match_text_seq("OMIT", "QUOTES")):
            return None

        return self.expression(
            exp.JSONExtractQuote(
                option=self._tokens[self._index - 2].text.upper(),
                scalar=self._match_text_seq("ON", "SCALAR", "STRING"),
            )
        )

    def _parse_json_query(self) -> exp.JSONExtract:
        return self.expression(
            exp.JSONExtract(
                this=self._parse_bitwise(),
                expression=self._match(TokenType.COMMA) and self._parse_bitwise(),
                option=self._parse_var_from_options(self.JSON_QUERY_OPTIONS, raise_unmatched=False),
                json_query=True,
                quote=self._parse_json_query_quote(),
                on_condition=self._parse_on_condition(),
            )
        )

    def _parse_cte(self) -> exp.CTE | exp.FunctionSpecification | None:
        # A `WITH` clause entry that starts with `FUNCTION <name>` is an inline SQL UDF
        # specification (https://trino.io/docs/current/udf/sql.html), as opposed to a
        # CTE that happens to be named "function", e.g. `WITH function AS (SELECT 1)`
        if (
            self._match(TokenType.FUNCTION, advance=False)
            and self._next
            and self._next.token_type in self.ID_VAR_TOKENS
        ):
            self._advance()
            return self._parse_function_specification()

        return super()._parse_cte()

    def _parse_function_specification(self) -> exp.FunctionSpecification:
        return self.expression(
            exp.FunctionSpecification(
                this=self._parse_user_defined_function(kind=TokenType.FUNCTION),
                properties=self._parse_properties(),
                expression=self._parse_routine_statement(),
            )
        )

    def _parse_routine_statement(self) -> exp.Expr | None:
        if self._match_text_seq("RETURN"):
            return self.expression(exp.Return(this=self._parse_disjunction()))

        self.raise_error("Expected routine statement")
        return None
