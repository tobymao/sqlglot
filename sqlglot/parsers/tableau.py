from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.helper import seq_get


class TableauParser(parser.Parser):
    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "COUNTD": lambda args: exp.Count(this=exp.Distinct(expressions=args)),
        "FIND": exp.StrPosition.from_arg_list,
        "FINDNTH": lambda args: exp.StrPosition(
            this=seq_get(args, 0), substr=seq_get(args, 1), occurrence=seq_get(args, 2)
        ),
    }
    NO_PAREN_IF_COMMANDS = False
