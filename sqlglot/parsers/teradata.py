from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


class TeradataParser(parser.Parser):
    TABLESAMPLE_CSV = True
    VALUES_FOLLOWED_BY_PAREN = False

    CHARSET_TRANSLATORS = {
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

    FUNC_TOKENS = parser.Parser.FUNC_TOKENS - {TokenType.REPLACE}

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.DATABASE: lambda self: self.expression(
            exp.Use(this=self._parse_table(schema=False))
        ),
        TokenType.REPLACE: lambda self: self._parse_create(),
        TokenType.LOCK: lambda self: self._parse_locking_statement(),
    }

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
        "RANDOM": lambda args: exp.Rand(lower=seq_get(args, 0), upper=seq_get(args, 1)),
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

    # FROM before SET in Teradata UPDATE syntax
    # https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-SQL-Data-Manipulation-Language-17.20/Statement-Syntax/UPDATE/UPDATE-Syntax-Basic-Form-FROM-Clause
    def _parse_update(self) -> exp.Update:
        return self.expression(
            exp.Update(
                this=self._parse_table(alias_tokens=self.UPDATE_ALIAS_TOKENS),
                from_=self._parse_from(joins=True),
                expressions=self._match(TokenType.SET) and self._parse_csv(self._parse_equality),
                where=self._parse_where(),
            )
        )

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
            query_band_string: t.Optional[exp.Expr] = exp.Var(this="NONE")
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

    def _parse_function(
        self,
        functions: t.Optional[t.Dict[str, t.Callable]] = None,
        anonymous: bool = False,
        optional_parens: bool = True,
        any_token: bool = False,
    ) -> t.Optional[exp.Expr]:
        # Teradata uses a `(FORMAT <format_string>)` clause after column references to
        # override the output format. When we see this pattern we do not
        # parse it as a function call.  The syntax is documented at
        # https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
        if (
            self._next
            and self._next.token_type == TokenType.L_PAREN
            and self._index + 2 < len(self._tokens)
            and self._tokens[self._index + 2].token_type == TokenType.FORMAT
        ):
            return None

        return super()._parse_function(
            functions=functions,
            anonymous=anonymous,
            optional_parens=optional_parens,
            any_token=any_token,
        )

    def _parse_column_ops(self, this: t.Optional[exp.Expr]) -> t.Optional[exp.Expr]:
        this = super()._parse_column_ops(this)

        if self._match_pair(TokenType.L_PAREN, TokenType.FORMAT):
            # `(FORMAT <format_string>)` after a column specifies a Teradata format override.
            # See https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
            fmt_string = self._parse_string()
            self._match_r_paren()

            this = self.expression(exp.FormatPhrase(this=this, format=fmt_string))

        return this
