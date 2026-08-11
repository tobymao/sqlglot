from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.parser import binary_range_parser
from sqlglot.tokens import TokenType


def _build_strftime(args: list) -> exp.Anonymous | exp.TimeToStr:
    if len(args) == 1:
        args.append(exp.CurrentTimestamp())
    if len(args) == 2:
        return exp.TimeToStr(this=exp.TsOrDsToTimestamp(this=args[1]), format=args[0])
    return exp.Anonymous(this="STRFTIME", expressions=args)


def _build_dpipe(
    self: parser.Parser, this: exp.Expr | None, expression: exp.Expr | None
) -> exp.DPipe:
    return self.expression(
        exp.DPipe(this=this, expression=expression, safe=not self.dialect.STRICT_STRING_CONCAT)
    )


def _build_json_extract(args: list, dialect: t.Any) -> exp.JSONExtract | exp.JSONExtractScalar:
    # Single-path json_extract() returns an SQL representation like ->>, except
    # that object/array results keep the JSON subtype (json_subtype); the
    # multi-path form returns a JSON array. https://www.sqlite.org/json1.html#jex
    if len(args) == 2:
        extract = parser.build_extract_json_with_path(exp.JSONExtractScalar)(args, dialect)
        extract.set("json_subtype", True)
        return extract
    return parser.build_extract_json_with_path(exp.JSONExtract)(args, dialect)


class SQLiteParser(parser.Parser):
    STRING_ALIASES = True
    ALTER_RENAME_REQUIRES_COLUMN = False
    JOINS_HAVE_EQUAL_PRECEDENCE = True
    ADD_JOIN_ON_TRUE = True

    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {
        TokenType.ANTI,
        TokenType.SEMI,
    }

    COLUMN_OPERATORS = {
        k: v
        for k, v in parser.Parser.COLUMN_OPERATORS.items()
        if k not in (TokenType.ARROW, TokenType.DARROW)
    }

    # ||, -> and ->> form a single left-associative precedence tier that binds tighter
    # than multiplication, so it's consumed in _parse_factor_operand rather than in
    # _parse_bitwise: https://sqlite.org/lang_expr.html
    CONCAT_OPERATORS: t.ClassVar[dict[TokenType, t.Callable]] = {
        TokenType.DPIPE: _build_dpipe,
        TokenType.ARROW: parser.build_json_extract,
        TokenType.DARROW: parser.build_json_extract_scalar,
    }

    ARITHMETIC_TOKENS = {
        *parser.Parser.BITWISE,
        *parser.Parser.TERM,
        *parser.Parser.FACTOR,
    } - {TokenType.COLLATE}

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "DATETIME": lambda args: exp.Anonymous(this="DATETIME", expressions=args),
        "EDITDIST3": exp.Levenshtein.from_arg_list,
        "JSON_EXTRACT": _build_json_extract,
        "JSON_GROUP_ARRAY": exp.JSONArrayAgg.from_arg_list,
        "JSON_GROUP_OBJECT": lambda args: exp.JSONObjectAgg(expressions=args),
        "STRFTIME": _build_strftime,
        "SQLITE_VERSION": exp.CurrentVersion.from_arg_list,
        "TIME": lambda args: exp.Anonymous(this="TIME", expressions=args),
    }

    PROPERTY_PARSERS = {
        **parser.Parser.PROPERTY_PARSERS,
        "USING": lambda self, **kwargs: self._parse_module_property(),
        "VIRTUAL": lambda self: self.expression(exp.VirtualProperty()),
    }

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.ATTACH: lambda self: self._parse_attach_detach(),
        TokenType.DETACH: lambda self: self._parse_attach_detach(is_attach=False),
        TokenType.PRAGMA: lambda self: self._parse_pragma(),
    }

    RANGE_PARSERS = {
        **parser.Parser.RANGE_PARSERS,
        # https://www.sqlite.org/lang_expr.html
        TokenType.MATCH: binary_range_parser(exp.Match),
    }

    def _parse_factor_operand(self) -> exp.Expr | None:
        in_arithmetic_operand = self._prev.token_type in self.ARITHMETIC_TOKENS
        this = self._parse_concat_operand()
        parsed_op = False

        while self._curr and (build := self.CONCAT_OPERATORS.get(self._curr.token_type)):
            if self._curr.token_type != TokenType.DPIPE and isinstance(this, exp.DPipe):
                # Parenthesize so that dialects where the arrows bind tighter
                # than || still produce the same, left-associative grouping
                this = self.expression(exp.Paren(this=this))

            self._advance()
            this = build(self, this, self._parse_concat_operand())
            parsed_op = True

        if parsed_op and (
            in_arithmetic_operand
            or (self._curr and self._curr.token_type in self.ARITHMETIC_TOKENS)
        ):
            # This tier binds tighter than arithmetic in SQLite but looser elsewhere,
            # so parenthesize it to preserve the grouping in other dialects
            this = self.expression(exp.Paren(this=this))

        return this

    def _parse_concat_operand(self) -> exp.Expr | None:
        this = self._parse_unary()

        # COLLATE binds tighter than any binary operator, including this tier
        while self._match(TokenType.COLLATE):
            collate = self.expression(exp.Collate(this=this, expression=self._parse_unary()))
            self._normalize_collate(collate)
            this = collate

        return this

    def _parse_unique(self) -> exp.UniqueColumnConstraint:
        # Do not consume more tokens if UNIQUE is used as a standalone constraint, e.g:
        # CREATE TABLE foo (bar TEXT UNIQUE REFERENCES baz ...)
        if self._curr.text.upper() in self.CONSTRAINT_PARSERS:
            return self.expression(exp.UniqueColumnConstraint())

        return super()._parse_unique()

    def _parse_module_property(self, **kwargs: t.Any) -> exp.ModuleProperty:
        name = self._parse_id_var()
        expressions = (
            self._parse_wrapped_csv(self._parse_id_var)
            if self._match(TokenType.L_PAREN, advance=False)
            else None
        )
        return self.expression(exp.ModuleProperty(this=name, expressions=expressions))

    def _parse_pragma(self) -> exp.Pragma:
        name = self._parse_var(any_token=True)

        if self._match(TokenType.DOT):
            name = exp.Dot(this=name, expression=self._parse_var(any_token=True))

        self._match(TokenType.EQ)

        value = self._parse_wrapped(
            lambda: self._parse_unary() or self._parse_var(any_token=True), optional=True
        )

        if value:
            return self.expression(exp.Pragma(this=exp.EQ(this=name, expression=value)))

        return self.expression(exp.Pragma(this=name))

    def _parse_attach_detach(self, is_attach=True) -> exp.Attach | exp.Detach:
        self._match(TokenType.DATABASE)
        this = self._parse_expression()

        return (
            self.expression(exp.Attach(this=this))
            if is_attach
            else self.expression(exp.Detach(this=this))
        )

    # https://www.sqlite.org/gencol.html
    def _parse_generated_as_identity(
        self,
    ) -> (
        exp.GeneratedAsIdentityColumnConstraint
        | exp.ComputedColumnConstraint
        | exp.GeneratedAsRowColumnConstraint
    ):
        this = super()._parse_generated_as_identity()

        # Only expression-bearing GENERATED ALWAYS AS (expr) forms are computed
        # columns. Match STORED/VIRTUAL inside this branch so true identity
        # (AS IDENTITY) does not silently consume a trailing storage keyword.
        if (
            isinstance(this, exp.GeneratedAsIdentityColumnConstraint)
            and this.expression is not None
        ):
            persisted = (
                self._match_texts(("STORED", "VIRTUAL")) and self._prev.text.upper() == "STORED"
            )
            return self.expression(
                exp.ComputedColumnConstraint(this=this.expression, persisted=persisted)
            )

        return this
