from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.parser import binary_range_parser
from sqlglot.tokens import TokenType


def _build_strftime(args: t.List) -> exp.Anonymous | exp.TimeToStr:
    if len(args) == 1:
        args.append(exp.CurrentTimestamp())
    if len(args) == 2:
        return exp.TimeToStr(this=exp.TsOrDsToTimestamp(this=args[1]), format=args[0])
    return exp.Anonymous(this="STRFTIME", expressions=args)


class SQLiteParser(parser.Parser):
    STRING_ALIASES = True
    ALTER_RENAME_REQUIRES_COLUMN = False
    JOINS_HAVE_EQUAL_PRECEDENCE = True
    ADD_JOIN_ON_TRUE = True

    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {
        TokenType.ANTI,
        TokenType.SEMI,
    }

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "DATETIME": lambda args: exp.Anonymous(this="DATETIME", expressions=args),
        "EDITDIST3": exp.Levenshtein.from_arg_list,
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

        value = self._parse_wrapped(self._parse_primary_or_var, optional=True)

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
