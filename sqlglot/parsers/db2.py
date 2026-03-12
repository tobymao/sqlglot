from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.helper import seq_get

if t.TYPE_CHECKING:
    pass


class Db2Parser(parser.Parser):
    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "CHAR": exp.Cast.from_arg_list,
        "DAYOFWEEK": lambda args: exp.Extract(
            this=exp.var("DAYOFWEEK"), expression=seq_get(args, 0)
        ),
        "DAYOFYEAR": lambda args: exp.Extract(
            this=exp.var("DAYOFYEAR"), expression=seq_get(args, 0)
        ),
        "DAYS": lambda args: exp.DateDiff(
            this=seq_get(args, 0),
            expression=exp.Literal.string("1970-01-01"),
            unit=exp.var("DAY"),
        ),
        "MIDNIGHT_SECONDS": lambda args: exp.Anonymous(this="MIDNIGHT_SECONDS", expressions=args),
        "POSSTR": lambda args: exp.StrPosition(this=seq_get(args, 0), substr=seq_get(args, 1)),
        "VARCHAR_FORMAT": lambda args: exp.TimeToStr(
            this=seq_get(args, 0), format=seq_get(args, 1)
        ),
    }

    FUNCTION_PARSERS = {
        **parser.Parser.FUNCTION_PARSERS,
        "CAST": lambda self: self._parse_cast(self.STRICT_CAST),
    }
