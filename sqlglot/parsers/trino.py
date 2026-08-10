from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.helper import ensure_list
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

    JSON_QUERY_OPTIONS: t.ClassVar[parser.OPTIONS_TYPE] = {
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

    def _parse_routine_statements(self, *terminators: str) -> list[exp.Expr]:
        # Unlike _parse_block(), stops on any of `terminators` even when tokens
        # follow (Trino's own END is always followed by the enclosing query, left
        # for the caller), matching them as text rather than just TokenType.END, so
        # this same chunk-continuation loop also serves IF/ELSEIF/ELSE.
        statements: list[exp.Expr] = []

        while not self._match_texts(terminators):
            if not self._curr:
                if self._chunk_index >= len(self._chunks):
                    self.raise_error("Unexpected end of routine body")
                    break

                self._advance_chunk()
            elif not self._match(TokenType.SEMICOLON):
                statement = self._parse_routine_statement()
                if not statement:
                    break

                statements.append(statement)

        return statements

    def _parse_routine_block(self) -> exp.Block:
        self._match(TokenType.BEGIN)
        statements = self._parse_routine_statements("END")
        statements.append(exp.EndStatement())

        return self.expression(exp.Block(expressions=statements, begin=True))

    def _parse_routine_if(self) -> exp.IfBlock:
        # https://trino.io/docs/current/udf/sql/if.html
        # ELSEIF chains nest into `false` rather than a flat list, reusing the
        # existing binary IfBlock instead of a new N-way expression; each loop
        # iteration links the next branch into the previous one's `false` slot.
        def parse_branch() -> exp.IfBlock:
            condition = self._parse_disjunction()
            self._match_text_seq("THEN")
            true = self.expression(
                exp.Block(expressions=self._parse_routine_statements("ELSEIF", "ELSE", "END"))
            )
            return self.expression(exp.IfBlock(this=condition, true=true))

        this = tail = parse_branch()
        while self._prev.text.upper() == "ELSEIF":
            node = parse_branch()
            tail.set("false", node)
            tail = node

        if self._prev.text.upper() == "ELSE":
            tail.set(
                "false",
                self.expression(exp.Block(expressions=self._parse_routine_statements("END"))),
            )

        self._match_text_seq("IF")
        return this

    def _parse_routine_case(self) -> exp.CaseStatement:
        # https://trino.io/docs/current/udf/sql/case.html
        this = self._parse_disjunction()

        def parse_branch() -> exp.If:
            condition = self._parse_disjunction()
            self._match_text_seq("THEN")
            true = self.expression(
                exp.Block(expressions=self._parse_routine_statements("WHEN", "ELSE", "END"))
            )
            return self.expression(exp.If(this=condition, true=true))

        ifs = []
        self._match_text_seq("WHEN")
        while self._prev.text.upper() == "WHEN":
            ifs.append(parse_branch())

        default = None
        if self._prev.text.upper() == "ELSE":
            default = self.expression(exp.Block(expressions=self._parse_routine_statements("END")))

        self._match_text_seq("CASE")
        return self.expression(exp.CaseStatement(this=this, ifs=ifs, default=default))

    def _parse_routine_while(self, label: exp.Expr | None = None) -> exp.WhileBlock:
        # https://trino.io/docs/current/udf/sql/while.html
        condition = self._parse_disjunction()
        self._match_text_seq("DO")
        body = self.expression(exp.Block(expressions=self._parse_routine_statements("END")))
        self._match_text_seq("WHILE")
        return self.expression(exp.WhileBlock(this=condition, body=body, label=label))

    def _parse_routine_statement(self) -> exp.Expr | None:
        if self._match(TokenType.BEGIN, advance=False):
            return self._parse_routine_block()

        if self._match_text_seq("RETURN"):
            return self.expression(exp.Return(this=self._parse_disjunction()))

        if self._match_text_seq("IF"):
            return self._parse_routine_if()

        if self._match_text_seq("CASE"):
            return self._parse_routine_case()

        if self._match(TokenType.DECLARE):
            return self._parse_declare()

        if self._match(TokenType.SET):
            return self._parse_set()

        # An optional `label :` can precede WHILE (and, in later phases, LOOP/REPEAT)
        # to name the block for ITERATE/LEAVE.
        label = None
        if self._next and self._next.token_type == TokenType.COLON:
            label = self._parse_id_var()
            self._match(TokenType.COLON)

        if self._match_text_seq("WHILE"):
            return self._parse_routine_while(label=label)

        self.raise_error("Expected routine statement")
        return None
