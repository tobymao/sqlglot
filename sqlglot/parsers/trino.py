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
            self._curr.token_type == TokenType.FUNCTION
            and self._next.token_type in self.ID_VAR_TOKENS
        ):
            self._advance()
            return self._parse_function_specification()

        return super()._parse_cte()

    def _parse_function_specification(self) -> exp.FunctionSpecification:
        return self.expression(
            exp.FunctionSpecification(
                this=self._parse_user_defined_function(kind=TokenType.FUNCTION),
                properties=self._parse_routine_characteristics(),
                expression=self._parse_routine_statement(),
            )
        )

    def _parse_routine_characteristics(self) -> exp.Properties | None:
        # The `RETURNS` clause, followed by any routine characteristics:
        # https://trino.io/docs/current/udf/function.html
        properties: list[exp.Expr] = []
        while True:
            if self._match_text_seq("NOT", "DETERMINISTIC"):
                prop: exp.Expr = self.expression(
                    exp.StabilityProperty(this=exp.Literal.string("VOLATILE"))
                )
            elif self._match_texts(
                ("RETURNS", "LANGUAGE", "DETERMINISTIC", "CALLED", "SECURITY", "COMMENT")
            ):
                prop = self.PROPERTY_PARSERS[self._prev.text.upper()](self)
            else:
                break

            properties.append(prop)

        return self.expression(exp.Properties(expressions=properties)) if properties else None

    def _parse_routine_statement(self) -> exp.Expr | None:
        # https://trino.io/docs/current/udf/sql.html
        if self._match(TokenType.BEGIN):
            statements = self._parse_routine_statements({"END"})
            self._match(TokenType.END)
            return self.expression(exp.Block(expressions=statements, begin=True))

        if self._match(TokenType.CASE):
            return self._parse_case_statement()

        if self._match_text_seq("IF"):
            return self._parse_if_statement()

        if self._match_text_seq("RETURN"):
            return self.expression(exp.Return(this=self._parse_disjunction()))

        if self._match(TokenType.SET):
            return self._parse_set()

        if self._match_text_seq("DECLARE"):
            declaration = self._parse_declareitem()
            if not declaration:
                self.raise_error("Expected variable declaration")
            return self.expression(exp.Declare(expressions=[declaration]))

        if self._match_text_seq("ITERATE"):
            return self.expression(exp.Iterate(this=self._parse_id_var()))

        if self._match_text_seq("LEAVE"):
            return self.expression(exp.Leave(this=self._parse_id_var()))

        label = None
        if self._next.token_type == TokenType.COLON:
            label = self._parse_id_var()
            self._match(TokenType.COLON)

        if self._match_text_seq("LOOP"):
            statements = self._parse_routine_statements({"END"})
            self._match(TokenType.END)
            self._expect_text("LOOP")
            return self.expression(
                exp.LoopStatement(
                    this=self.expression(exp.Block(expressions=statements)), label=label
                )
            )

        if self._match_text_seq("WHILE"):
            condition = self._parse_disjunction()
            self._expect_text("DO")
            statements = self._parse_routine_statements({"END"})
            self._match(TokenType.END)
            self._expect_text("WHILE")
            return self.expression(
                exp.WhileStatement(
                    this=condition,
                    body=self.expression(exp.Block(expressions=statements)),
                    label=label,
                )
            )

        if self._match_text_seq("REPEAT"):
            statements = self._parse_routine_statements({"UNTIL"})
            self._match_text_seq("UNTIL")
            until = self._parse_disjunction()
            if not self._match(TokenType.END):
                self.raise_error("Expected END REPEAT")
            self._expect_text("REPEAT")
            return self.expression(
                exp.RepeatStatement(
                    body=self.expression(exp.Block(expressions=statements)),
                    until=until,
                    label=label,
                )
            )

        self.raise_error("Expected routine statement")
        return None

    def _parse_routine_statements(self, terminators: set[str]) -> list[exp.Expr]:
        statements: list[exp.Expr] = []
        while True:
            if not self._curr:
                # Routine statements are separated by semicolons, which also delimit the
                # chunks produced in `_parse`, so the routine body continues in the next chunk
                if self._chunk_index >= len(self._chunks):
                    self.raise_error("Unexpected end of routine body", token=self._prev)
                    break

                self._advance_chunk()
                continue

            if self._curr.token_type == TokenType.SEMICOLON:
                # A semicolon that carries comments is kept as a standalone chunk
                self._advance()
                continue

            if self._curr.text.upper() in terminators:
                break

            statement = self._parse_routine_statement()
            if not statement:
                break

            statements.append(statement)

        return statements

    def _parse_case_statement(self) -> exp.CaseStatement:
        this = None if self._curr.token_type == TokenType.WHEN else self._parse_disjunction()

        ifs = []
        while self._match(TokenType.WHEN):
            condition = self._parse_disjunction()
            self._expect_text("THEN")
            statements = self._parse_routine_statements({"WHEN", "ELSE", "END"})
            ifs.append(
                self.expression(
                    exp.If(
                        this=condition,
                        true=self.expression(exp.Block(expressions=statements)),
                    )
                )
            )

        default = self._parse_statements_after_else()

        self._match(TokenType.END)
        self._expect_text("CASE")
        return self.expression(exp.CaseStatement(this=this, ifs=ifs, default=default))

    def _parse_if_statement(self) -> exp.IfStatement:
        ifs = []
        while True:
            condition = self._parse_disjunction()
            self._expect_text("THEN")
            statements = self._parse_routine_statements({"ELSEIF", "ELSE", "END"})
            ifs.append(
                self.expression(
                    exp.If(
                        this=condition,
                        true=self.expression(exp.Block(expressions=statements)),
                    )
                )
            )

            if not self._match_text_seq("ELSEIF"):
                break

        default = self._parse_statements_after_else()

        self._match(TokenType.END)
        self._expect_text("IF")
        return self.expression(exp.IfStatement(ifs=ifs, default=default))

    def _parse_statements_after_else(self) -> exp.Block | None:
        if not self._match(TokenType.ELSE):
            return None

        statements = self._parse_routine_statements({"END"})
        return self.expression(exp.Block(expressions=statements))

    def _expect_text(self, text: str) -> None:
        if not self._match_text_seq(text):
            self.raise_error(f"Expected {text}")
