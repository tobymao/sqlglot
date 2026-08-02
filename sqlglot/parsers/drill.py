from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_formatted_time
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _build_ilike(args: list) -> exp.Expr:
    ilike = exp.ILike(this=seq_get(args, 0), expression=seq_get(args, 1))
    escape = seq_get(args, 2)
    return exp.Escape(this=ilike, expression=escape) if escape else ilike


class DrillParser(parser.Parser):
    STRICT_CAST = False

    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {
        TokenType.ANTI,
        TokenType.SEMI,
    }

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "ILIKE": _build_ilike,
        "REPEATED_COUNT": exp.ArraySize.from_arg_list,
        "TO_TIMESTAMP": exp.TimeStrToTime.from_arg_list,
        "TO_CHAR": build_formatted_time(exp.TimeToStr),
        "LEVENSHTEIN_DISTANCE": exp.Levenshtein.from_arg_list,
    }

    LOG_DEFAULTS_TO_LN = True
