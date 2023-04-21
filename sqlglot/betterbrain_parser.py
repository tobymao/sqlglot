from sqlglot import exp
from sqlglot.betterbrain import Suggestion, TableIdentifier
from sqlglot.errors import ErrorLevel, ParseError
from sqlglot.helper import seq_get
from sqlglot.parser import Parser
from sqlglot.tokens import Token, TokenType, Tokenizer


import typing as t


class BetterBrainParserMixin:
    KEYWORDS_TO_IDENTIFIER_TYPE = {
        TokenType.SELECT: TokenType.COLUMN,
        TokenType.WHERE: TokenType.COLUMN,
        TokenType.FROM: TokenType.TABLE,
    }



    def suggest(
        self, raw_tokens: t.List[Token], sql: t.Optional[str] = None
    ) -> Suggestion:
        return self._suggest(
            parse_method=self.__class__._parse_suggestion_statement, raw_tokens=raw_tokens, sql=sql
        )

    def _suggest(
        self,
        parse_method: t.Callable[[Parser], t.Optional[exp.Expression]],
        raw_tokens: t.List[Token],
        sql: t.Optional[str] = None,
    ) -> Suggestion:
        self.reset()
        self.sql = sql or ""

        self._index = -1
        self._tokens = raw_tokens
        self._advance()
        expression = parse_method(self)

        if self._index < (len(self._tokens) if self._cursor_position == -1 else self._cursor_position):
            self.raise_error("Invalid expression / Unexpected token")

        self.check_errors()

        suggestions = self._suggestion_options.intersection(self.tokenizer.TOKEN_TO_KEYWORD.keys())
        if TokenType.IDENTIFIER in self._suggestion_options:
            resolved_identifier = self.KEYWORDS_TO_IDENTIFIER_TYPE[self._last_keyword.token_type] if (self._last_keyword and self._last_keyword.token_type in self.KEYWORDS_TO_IDENTIFIER_TYPE) else TokenType.IDENTIFIER
            suggestions.add(resolved_identifier)

        table_ids = []

        if TokenType.COLUMN in suggestions:
            from_clause = expression.find(exp.From)
            if from_clause is None:
                while self._index < (len(raw_tokens) -1) and from_clause is None:
                    self._advance()
                    from_clause = self._parse_from(expression)

            if from_clause is not None:
                from_tables = list(from_clause.find_all(exp.Table))
                join_clauses = expression.find_all(exp.Join)
                join_tables = [table for join_clause in join_clauses for table in join_clause.find_all(exp.Table)]
                all_tables = from_tables + join_tables
                table_ids = [self._get_table_name_and_alias(table) for table in all_tables]

        return Suggestion(suggestions=suggestions, table_ids= table_ids)


    def _get_table_name_and_alias(self, table: exp.Table)-> TableIdentifier:
        id = table.find(exp.Identifier)
        return TableIdentifier(name= id.alias_or_name, alias= table.alias_or_name if table.alias_or_name != id.alias_or_name else None)



    def _parse_suggestion_statement(self) -> t.Optional[exp.Expression]:
        if self._curr is None:
            return None
        if self._match_set(self.STATEMENT_PARSERS):
            return self.STATEMENT_PARSERS[self._prev.token_type](self)
        if self._match_set(Tokenizer.COMMANDS):
            return self._parse_command()
        expression = self._parse_expression()
        expression = self._parse_set_operations(expression) if expression else self._parse_select()

        # Commenting this out for now to ensure that "select asian <<" does not suggest modifiers like WHERE, LIMIT
        # self._parse_query_modifiers(expression)
        return expression


    def _add_to_suggestion_options(self, new_item: t.Union[str, TokenType, t.Set, t.List, t.Dict]):
        if self._cursor_position != -1 and not self._has_hit_error_after_cursor:
                if isinstance(new_item, (list, set, dict)):
                    self._suggestion_options.union(set(new_item))
                else:
                    self._suggestion_options.add(new_item)


    def raise_error(self, message: str, token: t.Optional[Token] = None) -> None:
        """
        Appends an error in the list of recorded errors or raises it, depending on the chosen
        error level setting.
        """
        if self._cursor_position != -1:
            self._has_hit_error_after_cursor = True
            return

        token = token or self._curr or self._prev or Token.string("")
        start = token.start
        end = token.end
        start_context = self.sql[max(start - self.error_message_context, 0) : start]
        highlight = self.sql[start:end]
        end_context = self.sql[end : end + self.error_message_context]
        error = ParseError.new(
            f"{message}. Line {token.line}, Col: {token.col}.\n"
            f"  {start_context}\033[4m{highlight}\033[0m{end_context}",
            description=message,
            line=token.line,
            col=token.col,
            start_context=start_context,
            highlight=highlight,
            end_context=end_context,
        )
        if self.error_level == ErrorLevel.IMMEDIATE:
            raise error
        self.errors.append(error)

    def validate_expression(
        self, expression: exp.Expression, args: t.Optional[t.List] = None
    ) -> None:
        """
        Validates an already instantiated expression, making sure that all its mandatory arguments
        are set.
        Args:
            expression: the expression to validate.
            args: an optional list of items that was used to instantiate the expression, if it's a Func.
        """
        if self.error_level == ErrorLevel.IGNORE:
            return

        error_messages = expression.error_messages(args)
        if not error_messages and not self._has_hit_error_after_cursor:
            self._suggestion_options = set()
        else:
            for error_message in error_messages:
                self.raise_error(error_message)

    def _advance(self, times: int = 1) -> None:
        self._index += times
        next_token = seq_get(self._tokens, self._index)

        if next_token is not None and next_token.token_type == TokenType.CURSOR:
            self._cursor_position = self._index
            self._curr = None
        else:
            if next_token is not None and next_token.token_type in self.tokenizer.TOKEN_TO_KEYWORD:
                self._last_keyword = next_token
            self._curr = next_token

        self._next = seq_get(self._tokens, self._index + 1)
        if self._index > 0:
            self._prev = self._tokens[self._index - 1]
            self._prev_comments = self._prev.comments
        else:
            self._prev = None
            self._prev_comments = None

    def _parse_value(self) -> exp.Expression:
        if self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(self._parse_conjunction)
            self._match_r_paren()
            return self.expression(exp.Tuple, expressions=expressions)

        # In presto we can have VALUES 1, 2 which results in 1 column & 2 rows.
        # Source: https://prestodb.io/docs/current/sql/values.html
        return self.expression(exp.Tuple, expressions=[self._parse_conjunction()])

    def _parse_select(
        self, nested: bool = False, table: bool = False, parse_subquery_alias: bool = True
    ) -> t.Optional[exp.Expression]:
        cte = self._parse_with()
        if cte:
            this = self._parse_statement()

            if not this:
                self.raise_error("Failed to parse any statement following CTE")
                return cte

            if "with" in this.arg_types:
                this.set("with", cte)
            else:
                self.raise_error(f"{this.key} does not support CTE")
                this = cte
        elif self._match(TokenType.SELECT):
            comments = self._prev_comments

            kind = (
                self._match(TokenType.ALIAS)
                and self._match_texts(("STRUCT", "VALUE"))
                and self._prev.text
            )
            hint = self._parse_hint()
            all_ = self._match(TokenType.ALL)
            distinct = self._match(TokenType.DISTINCT)

            if distinct:
                distinct = self.expression(
                    exp.Distinct,
                    on=self._parse_value() if self._match(TokenType.ON) else None,
                )

            if all_ and distinct:
                self.raise_error("Cannot specify both ALL and DISTINCT after SELECT")

            limit = self._parse_limit(top=True)
            expressions = self._parse_csv(self._parse_expression, should_expect_non_null_value=True)

            this = self.expression(
                exp.Select,
                kind=kind,
                hint=hint,
                distinct=distinct,
                expressions=expressions,
                limit=limit,
            )
            this.comments = comments

            into = self._parse_into()
            if into:
                this.set("into", into)

            from_ = self._parse_from()
            if from_:
                this.set("from", from_)
                self._parse_query_modifiers(this)
        elif (table or nested) and self._match(TokenType.L_PAREN):
            this = self._parse_table() if table else self._parse_select(nested=True)
            self._parse_query_modifiers(this)
            this = self._parse_set_operations(this)
            self._match_r_paren()

            # early return so that subquery unions aren't parsed again
            # SELECT * FROM (SELECT 1) UNION ALL SELECT 1
            # Union ALL should be a property of the top select node, not the subquery
            return self._parse_subquery(this, parse_alias=parse_subquery_alias)
        elif self._match(TokenType.VALUES):
            this = self.expression(
                exp.Values,
                expressions=self._parse_csv(self._parse_value),
                alias=self._parse_table_alias(),
            )
        else:
            this = None

        return self._parse_set_operations(this)


    def _parse_from(self, parent_exp: t.Optional[exp.Expression] = None) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.FROM):
            return None

        expression = self.expression(
            exp.From, comments=self._prev_comments, expressions=self._parse_csv(self._parse_table)
        )
        if parent_exp is not None:
            parent_exp.set("from", expression)
            self._parse_query_modifiers(parent_exp)
        return expression

    def _parse_csv(
        self, parse_method: t.Callable, sep: TokenType = TokenType.COMMA, should_expect_non_null_value: bool = False
    ) -> t.List[t.Optional[exp.Expression]]:
        parse_result = parse_method()
        items = [parse_result] if parse_result is not None else []

        while self._match(sep):
            self._suggestion_options = set()
            if parse_result and self._prev_comments:
                parse_result.comments = self._prev_comments

            parse_result = parse_method()
            if parse_result is not None:
                items.append(parse_result)
            elif should_expect_non_null_value:
                self.raise_error(f"Expected non-null value")

        return items

    def _match(self, token_type, advance=True):
        if not self._curr:
            self._add_to_suggestion_options(token_type)
            return None

        if self._curr.token_type == token_type:
            if advance:
                self._advance()
            return True

        return None

    def _match_set(self, types, advance=True):
        if not self._curr:
            self._add_to_suggestion_options(types)
            return None

        if self._curr.token_type in types:
            if advance:
                self._advance()
            return True

        return None

    def _match_pair(self, token_type_a, token_type_b, advance=True):
        if not self._curr or not self._next:
            self._add_to_suggestion_options(token_type_a)
            return None

        if self._curr.token_type == token_type_a and self._next.token_type == token_type_b:
            if advance:
                self._advance(2)
            return True

        return None

    def _match_texts(self, texts, advance=True):
        if self._curr and self._curr.text.upper() in texts:
            if advance:
                self._advance()
            return True

        self._add_to_suggestion_options(texts)
        return False