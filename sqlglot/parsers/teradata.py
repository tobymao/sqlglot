from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_formatted_trunc
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType
from sqlglot.trie import new_trie

# Ranking and navigation functions don't take a frame and FIRST_VALUE/LAST_VALUE have their own default
NON_FRAMED_WINDOW_FUNCTIONS = (
    exp.CumeDist,
    exp.DenseRank,
    exp.FirstValue,
    exp.Lag,
    exp.LastValue,
    exp.Lead,
    exp.NthValue,
    exp.Ntile,
    exp.PercentRank,
    exp.Rank,
)


def is_window_aggregate(window: exp.Window) -> bool:
    return isinstance(window.this, exp.AggFunc) and not isinstance(
        window.this, NON_FRAMED_WINDOW_FUNCTIONS
    )


class TeradataParser(parser.Parser):
    TABLESAMPLE_CSV = True
    VALUES_FOLLOWED_BY_PAREN = False

    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {
        TokenType.ANTI,
        TokenType.SEMI,
    }

    # Maximum number of literal type parameters. The tokenizer also recognizes
    # aliases from other dialects, including INT32 and BYTE for integer types.
    CONVERSION_TYPES: t.ClassVar = {
        "BYTEINT": 0,
        "SMALLINT": 0,
        "INT": 0,
        "INTEGER": 0,
        "BIGINT": 0,
        "DEC": 2,
        "DECIMAL": 2,
        "NUMERIC": 2,
        "FLOAT": 1,
        "REAL": 0,
        "DOUBLE PRECISION": 0,
        "CHAR": 1,
        "CHARACTER": 1,
        "VARCHAR": 1,
        "CHAR VARYING": 1,
        "CHARACTER VARYING": 1,
        "DATE": 0,
        "TIME": 1,
        "TIMESTAMP": 1,
        "INTERVAL": 0,
    }
    CONVERSION_ATTRIBUTES = {"FORMAT", "TITLE", "NAMED"}

    CHARSET_TRANSLATORS: t.ClassVar = {
        "GRAPHIC_TO_KANJISJIS",
        "GRAPHIC_TO_LATIN",
        "GRAPHIC_TO_UNICODE",
        "GRAPHIC_TO_UNICODE_PadSpace",
        "KANJI1_KanjiEBCDIC_TO_UNICODE",
        "KANJI1_KanjiEUC_TO_UNICODE",
        "KANJI1_KANJISJIS_TO_UNICODE",
        "KANJI1_SBC_TO_UNICODE",
        "KANJISJIS_TO_GRAPHIC",
        "KANJISJIS_TO_LATIN",
        "KANJISJIS_TO_UNICODE",
        "LATIN_TO_GRAPHIC",
        "LATIN_TO_KANJISJIS",
        "LATIN_TO_UNICODE",
        "LOCALE_TO_UNICODE",
        "UNICODE_TO_GRAPHIC",
        "UNICODE_TO_GRAPHIC_PadGraphic",
        "UNICODE_TO_GRAPHIC_VarGraphic",
        "UNICODE_TO_KANJI1_KanjiEBCDIC",
        "UNICODE_TO_KANJI1_KanjiEUC",
        "UNICODE_TO_KANJI1_KANJISJIS",
        "UNICODE_TO_KANJI1_SBC",
        "UNICODE_TO_KANJISJIS",
        "UNICODE_TO_LATIN",
        "UNICODE_TO_LOCALE",
        "UNICODE_TO_UNICODE_FoldSpace",
        "UNICODE_TO_UNICODE_Fullwidth",
        "UNICODE_TO_UNICODE_Halfwidth",
        "UNICODE_TO_UNICODE_NFC",
        "UNICODE_TO_UNICODE_NFD",
        "UNICODE_TO_UNICODE_NFKC",
        "UNICODE_TO_UNICODE_NFKD",
    }

    FUNC_TOKENS = (parser.Parser.FUNC_TOKENS - {TokenType.REPLACE}) | {
        TokenType.BEGIN,
        TokenType.END,
        TokenType.MOD,
    }

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.DATABASE: lambda self: self.expression(
            exp.Use(this=self._parse_table(schema=False))
        ),
        TokenType.EXECUTE: lambda self: self._parse_execute(),
        TokenType.REPLACE: lambda self: self._parse_create(),
        TokenType.LOCK: lambda self: self._parse_locking_statement(),
    }

    def _is_hex_character_string(self) -> bool:
        return (
            self._curr.token_type == TokenType.STRING
            and self._next.token_type == TokenType.VAR
            and self._next.text.upper() in {"XC", "XCV"}
            and self._curr.end + 1 == self._next.start
        )

    def _parse_atom(self) -> exp.Expr | None:
        if self._is_hex_character_string():
            return None
        return super()._parse_atom()

    def _parse_string(self) -> exp.Expr | None:
        if self._is_hex_character_string():
            self.raise_error("Hexadecimal character literals are not supported in this context")
            return None
        return super()._parse_string()

    def _parse_primary(self) -> exp.Expr | None:
        if self._is_hex_character_string():
            token = self._curr
            if any(char not in "0123456789abcdefABCDEF" for char in token.text):
                self.raise_error("Invalid hexadecimal character literal", token)
            self._advance()
            expression = self.expression(exp.HexString(this=token.text, is_character=True), token)
            expression.meta["end"] = self._curr.end
            expression.meta["col"] = self._curr.col
            self._advance()
            self._add_comments(expression)
            return expression
        primary = super()._parse_primary()
        if (
            self._prev.token_type == TokenType.STRING
            and self._curr.token_type == TokenType.VAR
            and self._curr.text.upper() in {"XC", "XCV"}
            and self._is_connected()
        ):
            self.raise_error("Adjacent hexadecimal character literals are not supported")
        return primary

    def _parse_function_call(
        self,
        functions: dict[str, t.Callable] | None = None,
        anonymous: bool = False,
        optional_parens: bool = True,
        any_token: bool = False,
    ) -> exp.Expr | None:
        expression = super()._parse_function_call(functions, anonymous, optional_parens, any_token)
        conversion = expression.this if isinstance(expression, exp.Window) else expression
        if isinstance(conversion, (exp.StrToDate, exp.StrToTime, exp.TsOrDsToDate)):
            fmt = conversion.args.get("format")
            if isinstance(fmt, exp.Expr) and any(
                e.args.get("is_character") for e in fmt.find_all(exp.HexString)
            ):
                self.raise_error(
                    "Hexadecimal character literals are not supported in conversion formats"
                )
        return expression

    def _parse_interval_span(
        self, this: exp.Expr, parse_function_unit: bool = True
    ) -> exp.Interval:
        return super()._parse_interval_span(
            this, parse_function_unit=parse_function_unit and not self._conversion_follows()
        )

    def _parse_interval(
        self, require_interval: bool = True, parse_function_unit: bool = True
    ) -> exp.Add | exp.Interval | None:
        interval = super()._parse_interval(require_interval, parse_function_unit)
        if interval and any(e.args.get("is_character") for e in interval.find_all(exp.HexString)):
            self.raise_error("Hexadecimal character literals are not supported in intervals")
        return interval

    def _parse_execute(self) -> exp.Execute | exp.Command:
        start = self._prev
        this = self._parse_table_parts(schema=True)
        expressions = []

        if self._match(TokenType.L_PAREN) and not self._match(TokenType.R_PAREN):
            while True:
                argument = self._parse_assignment()
                if argument is None:
                    # Do not discard omitted arguments or replace them with NULL.
                    return self._parse_as_command(start)
                expressions.append(argument)
                if not self._match(TokenType.COMMA):
                    break
            self._match_r_paren()

        if self._curr:
            # Preserve other EXECUTE forms, such as EXECUTE IMMEDIATE.
            return self._parse_as_command(start)

        return self.expression(exp.Execute(this=this, expressions=expressions))

    def _parse_locking_statement(self) -> exp.LockingStatement:
        # Reuse exp.LockingProperty parsing for the lock kind, type etc
        locking_property = self._parse_locking()
        wrapped_query = self._parse_select()

        if not wrapped_query:
            self.raise_error("Expected SELECT statement after LOCKING clause")

        return self.expression(
            exp.LockingStatement(this=locking_property, expression=wrapped_query)
        )

    SET_PARSERS = {
        **parser.Parser.SET_PARSERS,
        "QUERY_BAND": lambda self: self._parse_query_band(),
    }

    SET_TRIE = new_trie(key.split(" ") for key in SET_PARSERS)

    FUNCTION_PARSERS = {
        **parser.Parser.FUNCTION_PARSERS,
        # https://docs.teradata.com/r/SQL-Functions-Operators-Exprs-and-Predicates/June-2017/Data-Type-Conversions/TRYCAST
        "TRYCAST": parser.Parser.FUNCTION_PARSERS["TRY_CAST"],
        "RANGE_N": lambda self: self._parse_rangen(),
        "TRANSLATE": lambda self: self._parse_translate(),
    }

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "CARDINALITY": exp.ArraySize.from_arg_list,
        "CHARACTERS": exp.Length.from_arg_list,
        "CURDATE": exp.CurrentDate.from_arg_list,
        "CURTIME": exp.CurrentTime.from_arg_list,
        "RANDOM": lambda args: exp.Rand(lower=seq_get(args, 0), upper=seq_get(args, 1)),
        "TRUNC": build_formatted_trunc,
    }

    EXPONENT = {
        TokenType.DSTAR: exp.Pow,
    }

    def _parse_translate(self) -> exp.TranslateCharacters:
        this = self._parse_assignment()
        self._match(TokenType.USING)
        self._match_texts(self.CHARSET_TRANSLATORS)

        return self.expression(
            exp.TranslateCharacters(
                this=this,
                expression=self._prev.text.upper(),
                with_error=self._match_text_seq("WITH", "ERROR"),
            )
        )

    def _parse_delete(self, alias_tokens: t.Collection[TokenType] | None = None) -> exp.Delete:
        expression = super()._parse_delete(
            alias_tokens=alias_tokens or self.TABLE_ALIAS_TOKENS - {TokenType.ALL}
        )
        if not expression.args.get("where"):
            self._match(TokenType.ALL)
        return expression

    def _parse_insert(self) -> exp.Insert | exp.MultitableInserts:
        expression = super()._parse_insert()
        target = expression.this
        if (
            isinstance(expression, exp.Insert)
            and not expression.expression
            and not expression.args.get("default")
            and not expression.args.get("source")
            and isinstance(target, exp.Schema)
            and target.expressions
            and all(
                isinstance(value, (exp.Literal, exp.Func, exp.Null)) for value in target.expressions
            )
        ):
            expression.set("this", target.this)
            expression.set(
                "expression",
                self.expression(
                    exp.Values(expressions=[exp.Tuple(expressions=target.expressions)]),
                    comments=target.pop_comments(),
                ),
            )
        return expression

    # FROM before SET in Teradata UPDATE syntax
    # https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-SQL-Data-Manipulation-Language-17.20/Statement-Syntax/UPDATE/UPDATE-Syntax-Basic-Form-FROM-Clause
    def _parse_update(self) -> exp.Update:
        return self.expression(
            exp.Update(
                this=self._parse_table(alias_tokens=self.UPDATE_ALIAS_TOKENS),
                from_=self._parse_from(joins=True),
                expressions=self._match(TokenType.SET)
                and self._parse_csv(self._parse_update_assignment),
                where=self._parse_where(),
            )
        )

    def _parse_table_sample(self, as_modifier: bool = False) -> exp.TableSample | None:
        sample = super()._parse_table_sample(as_modifier=as_modifier)

        # A single integer sample is a row count, while a fraction is a proportion of rows
        # https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Manipulation-Language/SELECT-Statements/SAMPLE-Clause/SAMPLE-Clause-Syntax
        if sample and len(sample.expressions) == 1 and sample.expressions[0].is_number:
            num = sample.expressions[0].pop()
            if num.is_int:
                sample.set("size", num)
            else:
                percent = format(num.to_py().scaleb(2).normalize(), "f")
                sample.set("percent", exp.Literal.number(percent))

        return sample

    def _parse_rangen(self):
        this = self._parse_id_var()
        self._match(TokenType.BETWEEN)

        expressions = self._parse_csv(self._parse_assignment)
        each = self._match_text_seq("EACH") and self._parse_assignment()

        return self.expression(exp.RangeN(this=this, expressions=expressions, each=each))

    def _parse_query_band(self) -> exp.QueryBand:
        # Parse: SET QUERY_BAND = 'key=value;key2=value2;' FOR SESSION|TRANSACTION
        # Also supports: SET QUERY_BAND = 'key=value;' UPDATE FOR SESSION|TRANSACTION
        # Also supports: SET QUERY_BAND = NONE FOR SESSION|TRANSACTION
        self._match(TokenType.EQ)

        # Handle both string literals and NONE keyword
        if self._match_text_seq("NONE"):
            query_band_string: exp.Expr | None = exp.Var(this="NONE")
        else:
            query_band_string = self._parse_string()

        update = self._match_text_seq("UPDATE")
        self._match_text_seq("FOR")

        # Handle scope - can be SESSION, TRANSACTION, VOLATILE, or SESSION VOLATILE
        if self._match_text_seq("SESSION", "VOLATILE"):
            scope = "SESSION VOLATILE"
        elif self._match_texts(("SESSION", "TRANSACTION")):
            scope = self._prev.text.upper()
        else:
            scope = None

        return self.expression(exp.QueryBand(this=query_band_string, scope=scope, update=update))

    def _parse_index_params(self) -> exp.IndexParameters:
        this = super()._parse_index_params()

        if this.args.get("on"):
            this.set("on", None)
            self._retreat(self._index - 2)
        return this

    def _parse_table_part(self, schema: bool = False) -> exp.Expr | None:
        if not schema and self._match_pair(TokenType.TABLE, TokenType.L_PAREN, advance=False):
            if function := self._try_parse(self._parse_table_function):
                return function
        return super()._parse_table_part(schema)

    def _parse_table_function(self) -> exp.TableFromRows | None:
        # TABLE(db.f(DATE)) requires a function call even when DATE could be a target type.
        self._advance(2)
        parts: list[exp.Expr] = []
        while self._next.token_type == TokenType.DOT:
            qualifier = self._parse_id_var()
            if not qualifier or not self._match(TokenType.DOT):
                return None
            parts.append(qualifier)

        call = self._parse_function(
            anonymous=bool(parts) or self._curr.token_type == TokenType.IDENTIFIER,
            optional_parens=False,
            any_token=bool(parts),
        )
        if not call or not self._match(TokenType.R_PAREN):
            return None
        return self.expression(
            exp.TableFromRows(this=exp.Dot.build([*parts, call]) if parts else call)
        )

    def _parse_function(
        self,
        functions: dict[str, t.Callable] | None = None,
        anonymous: bool = False,
        optional_parens: bool = True,
        any_token: bool = False,
    ) -> exp.Expr | None:
        if optional_parens and self._conversion_follows():
            name = self._curr.text.upper()
            if self._curr.token_type == TokenType.IDENTIFIER:
                return None
            if self._prev.token_type != TokenType.DOT:
                value = self.NO_PAREN_FUNCTIONS.get(self._curr.token_type)
                if name == "CURDATE":
                    value = exp.CurrentDate
                elif name == "CURTIME":
                    value = exp.CurrentTime
                if value:
                    self._advance()
                    return self.expression(value())
                if name == "DATE":
                    return None

            if self._tokens[self._index + 2].text.upper() in self.CONVERSION_ATTRIBUTES or (
                name not in (self.FUNCTIONS if functions is None else functions)
                and name not in self.FUNCTION_PARSERS
            ):
                return None

        return super()._parse_function(functions, anonymous, optional_parens, any_token)

    def _conversion_follows(self) -> bool:
        if self._next.token_type != TokenType.L_PAREN:
            return False
        index = self._index
        self._advance(2)
        if self._match_texts(self.CONVERSION_ATTRIBUTES, advance=False):
            # Attribute keywords can also be function names, e.g. f(TITLE(x)).
            follows = self._next.token_type != TokenType.L_PAREN
        else:
            follows = self._match_texts(self.CONVERSION_TYPES, advance=False) and bool(
                self._try_parse(self._parse_conversion_items)
            )
        self._retreat(index)
        return follows

    def _parse_conversion_type(self) -> exp.Expr | None:
        index = self._index
        if self._next.token_type == TokenType.L_PAREN:
            # _parse_types also accepts expressions for sizes, e.g. DECIMAL(x). In
            # this ambiguous position only a literal precision, scale or length counts.
            max_params = self.CONVERSION_TYPES[self._curr.text.upper()]
            self._advance(2)
            for _ in range(max_params):
                if not self._match(TokenType.NUMBER) or not self._prev.text.isdecimal():
                    return None
                if self._match(TokenType.R_PAREN):
                    break
                if not self._match(TokenType.COMMA):
                    return None
            else:
                return None
            self._retreat(index)

        return self._parse_types(allow_identifiers=False)

    def _parse_conversion_items(self) -> dict[str, exp.Expr] | None:
        items: dict[str, exp.Expr] = {}
        while True:
            if self._match_texts(self.CONVERSION_ATTRIBUTES):
                key = self._prev.text.upper()
                if key == "NAMED":
                    value = self._parse_id_var(
                        any_token=False, tokens={TokenType.VAR, TokenType.STRING}
                    )
                    if not isinstance(value, exp.Identifier):
                        return None
                else:
                    value = self._parse_string()
                    if not value or (key == "TITLE" and not value.is_string):
                        return None
            elif self._match_texts(self.CONVERSION_TYPES, advance=False):
                key = "TYPE"
                value = self._parse_conversion_type()
                if not isinstance(value, exp.DataType):
                    return None
            else:
                return None

            if key in items:
                return None
            items[key] = value
            if not self._match(TokenType.COMMA):
                break

        return items if self._match(TokenType.R_PAREN) else None

    def _parse_type(
        self, parse_interval: bool = True, fallback_to_identifier: bool = False
    ) -> exp.Expr | None:
        this = super()._parse_type(parse_interval, fallback_to_identifier)
        while this and self._match(TokenType.L_PAREN):
            index = self._index - 1
            items = self._try_parse(self._parse_conversion_items)
            if not items:
                if self._match_texts(self.CONVERSION_ATTRIBUTES, advance=False):
                    self.raise_error("Invalid Teradata conversion attribute")
                self._retreat(index)
                break

            to = items.get("TYPE")
            fmt = items.get("FORMAT")
            if to is not None:
                if fmt or not to.is_type(*exp.DataType.NUMERIC_TYPES):
                    this = self.expression(exp.TeradataConversion(this=this, to=to, format=fmt))
                else:
                    this = self.expression(exp.Cast(this=this, to=to))
            elif fmt:
                this = self.expression(exp.FormatPhrase(this=this, format=fmt))
            if "TITLE" in items:
                this = self.expression(exp.TeradataTitle(this=this, title=items["TITLE"]))
            if "NAMED" in items:
                this = self.expression(exp.TeradataNamed(this=this, alias=items["NAMED"]))
            this = self._parse_column_ops(this)
        return this

    def _parse_projections(self) -> tuple[list[exp.Expr], list[exp.Expr] | None]:
        projections, exclude = super()._parse_projections()
        for i, projection in enumerate(projections):
            alias = (
                projection
                if isinstance(projection, exp.Alias)
                and not isinstance(projection, exp.TeradataNamed)
                else None
            )
            this = alias.this if alias else projection
            node = this
            named_alias = None
            while isinstance(node, (exp.FormatPhrase, exp.TeradataTitle, exp.TeradataNamed)):
                child = node.this
                if isinstance(node, exp.TeradataNamed):
                    if alias or named_alias:
                        child.add_comments(node.args["alias"].pop_comments())
                    named_alias = named_alias or node.args["alias"]
                    child.add_comments(node.pop_comments())
                    if node is this:
                        this = child
                    node.replace(child)
                node = child
            if named_alias:
                if alias:
                    alias.set("this", this)
                else:
                    projections[i] = self.expression(exp.Alias(this=this, alias=named_alias))
        return projections, exclude

    def _parse_window(self, this: exp.Expr | None, alias: bool = False) -> exp.Expr | None:
        window = super()._parse_window(this, alias)

        # Unlike ANSI, Teradata window aggregates with ORDER BY and no ROWS clause span the whole partition
        # https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Functions-Expressions-and-Predicates/Ordered-Analytical/Window-Aggregate-Functions/The-Window-Specification/ANSI-Compliance
        if (
            isinstance(window, exp.Window)
            and window.args.get("order")
            and not window.args.get("spec")
            and is_window_aggregate(window)
        ):
            window.set(
                "spec",
                exp.WindowSpec(
                    kind="ROWS",
                    start="UNBOUNDED",
                    start_side="PRECEDING",
                    end="UNBOUNDED",
                    end_side="FOLLOWING",
                ),
            )

        return window
