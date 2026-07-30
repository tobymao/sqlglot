from __future__ import annotations


from sqlglot import exp, parser
from sqlglot.helper import ensure_list
from sqlglot.parsers.presto import PrestoParser
from sqlglot.tokens import TokenType


class TrinoParser(PrestoParser):
    NO_PAREN_FUNCTIONS = {
        **PrestoParser.NO_PAREN_FUNCTIONS,
        TokenType.CURRENT_CATALOG: exp.CurrentCatalog,
    }

    STATEMENT_PARSERS = {
        **PrestoParser.STATEMENT_PARSERS,
        TokenType.DECLARE: lambda self: self._parse_declare(),
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

    def _parse_property(self) -> exp.Expr | list[exp.Expr] | None:
        if self._match_text_seq("NOT", "DETERMINISTIC"):
            return self.expression(exp.StabilityProperty(this=exp.Literal.string("VOLATILE")))

        return super()._parse_property()

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
        this = self._parse_user_defined_function(kind=TokenType.FUNCTION)

        # Collected separately (rather than one _parse_properties() call) so the
        # generator can place `WITH (...)` in its own bracketed clause at the end,
        # instead of it blending into the bare characteristics list.
        characteristics = []
        properties = []

        while True:
            if self._match(TokenType.WITH):
                properties.extend(self._parse_wrapped_csv(self._parse_key_value_property))
                continue

            characteristic = self._parse_property()
            if not characteristic:
                break

            characteristics.extend(ensure_list(characteristic))

        return self.expression(
            exp.FunctionSpecification(
                this=this,
                characteristics=self.expression(exp.Properties(expressions=characteristics))
                if characteristics
                else None,
                properties=self.expression(exp.Properties(expressions=properties))
                if properties
                else None,
                expression=self._parse_routine_statement(),
            )
        )

    def _parse_routine_block(self) -> exp.Block:
        # _parse_block() assumes its END is the last token of the whole remaining
        # statement (true for e.g. `CREATE PROCEDURE ... AS BEGIN ... END`), so it
        # only recognizes END when nothing follows it. Trino's grammar always has
        # a query after the UDF's END (`... END SELECT f(1)`), so END has to close
        # the block regardless of what comes after it; this loop still reuses the
        # same chunk-advancing primitives (_chunks/_advance_chunk) that
        # _parse_block() relies on to thread a body across the semicolon-delimited
        # chunks _parse() splits the whole script into.
        self._match(TokenType.BEGIN)
        statements: list[exp.Expr] = []

        while True:
            if not self._curr:
                if self._chunk_index >= len(self._chunks):
                    self.raise_error("Unexpected end of routine body")
                    break

                self._advance_chunk()
                continue

            if self._match(TokenType.SEMICOLON):
                continue

            if self._match(TokenType.END):
                statements.append(exp.EndStatement())
                break

            statement = self._parse_routine_statement()
            if not statement:
                break

            statements.append(statement)

        return self.expression(exp.Block(expressions=statements))

    def _parse_routine_statement(self) -> exp.Expr | None:
        if self._match(TokenType.BEGIN, advance=False):
            return self._parse_routine_block()

        if self._match_text_seq("RETURN"):
            return self.expression(exp.Return(this=self._parse_disjunction()))

        # SET/DECLARE are dispatched through the same STATEMENT_PARSERS entries
        # _parse_statement() uses, but going through _parse_statement() itself would
        # also enable its generic expression/SELECT fallback, silently swallowing
        # whatever follows this routine's END as if it were the routine's own body.
        if self._match_set(self.STATEMENT_PARSERS):
            return self.STATEMENT_PARSERS[self._prev.token_type](self)

        self.raise_error("Expected routine statement")
        return None
