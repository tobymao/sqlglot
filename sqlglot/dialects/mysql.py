from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    no_ilike_sql,
    no_paren_current_date_sql,
    no_tablesample_sql,
    no_trycast_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _date_trunc_sql(self, expression):
    unit = expression.name.lower()

    expr = self.sql(expression.expression)

    if unit == "day":
        return f"DATE({expr})"

    if unit == "week":
        concat = f"CONCAT(YEAR({expr}), ' ', WEEK({expr}, 1), ' 1')"
        date_format = "%Y %u %w"
    elif unit == "month":
        concat = f"CONCAT(YEAR({expr}), ' ', MONTH({expr}), ' 1')"
        date_format = "%Y %c %e"
    elif unit == "quarter":
        concat = f"CONCAT(YEAR({expr}), ' ', QUARTER({expr}) * 3 - 2, ' 1')"
        date_format = "%Y %c %e"
    elif unit == "year":
        concat = f"CONCAT(YEAR({expr}), ' 1 1')"
        date_format = "%Y %c %e"
    else:
        self.unsupported("Unexpected interval unit: {unit}")
        return f"DATE({expr})"

    return f"STR_TO_DATE({concat}, '{date_format}')"


def _str_to_date(args):
    date_format = MySQL.format_time(seq_get(args, 1))
    return exp.StrToDate(this=seq_get(args, 0), format=date_format)


def _str_to_date_sql(self, expression):
    date_format = self.format_time(expression)
    return f"STR_TO_DATE({self.sql(expression.this)}, {date_format})"


def _trim_sql(self, expression):
    target = self.sql(expression, "this")
    trim_type = self.sql(expression, "position")
    remove_chars = self.sql(expression, "expression")

    # Use TRIM/LTRIM/RTRIM syntax if the expression isn't mysql-specific
    if not remove_chars:
        return self.trim_sql(expression)

    trim_type = f"{trim_type} " if trim_type else ""
    remove_chars = f"{remove_chars} " if remove_chars else ""
    from_part = "FROM " if trim_type or remove_chars else ""
    return f"TRIM({trim_type}{remove_chars}{from_part}{target})"


def _date_add(expression_class):
    def func(args):
        interval = seq_get(args, 1)
        return expression_class(
            this=seq_get(args, 0),
            expression=interval.this,
            unit=exp.Literal.string(interval.text("unit").lower()),
        )

    return func


def _date_add_sql(kind):
    def func(self, expression):
        this = self.sql(expression, "this")
        unit = expression.text("unit").upper() or "DAY"
        expression = self.sql(expression, "expression")
        return f"DATE_{kind}({this}, INTERVAL {expression} {unit})"

    return func


class MySQL(Dialect):
    # https://prestodb.io/docs/current/functions/datetime.html#mysql-date-functions
    time_mapping = {
        "%M": "%B",
        "%c": "%-m",
        "%e": "%-d",
        "%h": "%I",
        "%i": "%M",
        "%s": "%S",
        "%S": "%S",
        "%u": "%W",
        "%k": "%-H",
        "%l": "%-I",
    }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("b'", "'"), ("B'", "'"), ("0b", "")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", "")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "SEPARATOR": TokenType.SEPARATOR,
            "_ARMSCII8": TokenType.INTRODUCER,
            "_ASCII": TokenType.INTRODUCER,
            "_BIG5": TokenType.INTRODUCER,
            "_BINARY": TokenType.INTRODUCER,
            "_CP1250": TokenType.INTRODUCER,
            "_CP1251": TokenType.INTRODUCER,
            "_CP1256": TokenType.INTRODUCER,
            "_CP1257": TokenType.INTRODUCER,
            "_CP850": TokenType.INTRODUCER,
            "_CP852": TokenType.INTRODUCER,
            "_CP866": TokenType.INTRODUCER,
            "_CP932": TokenType.INTRODUCER,
            "_DEC8": TokenType.INTRODUCER,
            "_EUCJPMS": TokenType.INTRODUCER,
            "_EUCKR": TokenType.INTRODUCER,
            "_GB18030": TokenType.INTRODUCER,
            "_GB2312": TokenType.INTRODUCER,
            "_GBK": TokenType.INTRODUCER,
            "_GEOSTD8": TokenType.INTRODUCER,
            "_GREEK": TokenType.INTRODUCER,
            "_HEBREW": TokenType.INTRODUCER,
            "_HP8": TokenType.INTRODUCER,
            "_KEYBCS2": TokenType.INTRODUCER,
            "_KOI8R": TokenType.INTRODUCER,
            "_KOI8U": TokenType.INTRODUCER,
            "_LATIN1": TokenType.INTRODUCER,
            "_LATIN2": TokenType.INTRODUCER,
            "_LATIN5": TokenType.INTRODUCER,
            "_LATIN7": TokenType.INTRODUCER,
            "_MACCE": TokenType.INTRODUCER,
            "_MACROMAN": TokenType.INTRODUCER,
            "_SJIS": TokenType.INTRODUCER,
            "_SWE7": TokenType.INTRODUCER,
            "_TIS620": TokenType.INTRODUCER,
            "_UCS2": TokenType.INTRODUCER,
            "_UJIS": TokenType.INTRODUCER,
            # https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
            "N": TokenType.INTRODUCER,
            "n": TokenType.INTRODUCER,
            "_UTF8": TokenType.INTRODUCER,
            "_UTF16": TokenType.INTRODUCER,
            "_UTF16LE": TokenType.INTRODUCER,
            "_UTF32": TokenType.INTRODUCER,
            "_UTF8MB3": TokenType.INTRODUCER,
            "_UTF8MB4": TokenType.INTRODUCER,
        }

    class Parser(parser.Parser):
        STRICT_CAST = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "DATE_ADD": _date_add(exp.DateAdd),
            "DATE_SUB": _date_add(exp.DateSub),
            "STR_TO_DATE": _str_to_date,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "GROUP_CONCAT": lambda self: self.expression(
                exp.GroupConcat,
                this=self._parse_lambda(),
                separator=self._match(TokenType.SEPARATOR) and self._parse_field(),
            ),
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,
            TokenType.ENGINE: lambda self: self._parse_property_assignment(exp.EngineProperty),
        }

    class Generator(generator.Generator):
        NULL_ORDERING_SUPPORTED = False

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.ILike: no_ilike_sql,
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
            exp.DateAdd: _date_add_sql("ADD"),
            exp.DateSub: _date_add_sql("SUB"),
            exp.DateTrunc: _date_trunc_sql,
            exp.GroupConcat: lambda self, e: f"""GROUP_CONCAT({self.sql(e, "this")} SEPARATOR {self.sql(e, "separator") or "','"})""",
            exp.StrToDate: _str_to_date_sql,
            exp.StrToTime: _str_to_date_sql,
            exp.Trim: _trim_sql,
        }

        ROOT_PROPERTIES = {
            exp.EngineProperty,
            exp.AutoIncrementProperty,
            exp.CharacterSetProperty,
            exp.CollateProperty,
            exp.SchemaCommentProperty,
        }

        WITH_PROPERTIES: t.Set[t.Type[exp.Property]] = set()
