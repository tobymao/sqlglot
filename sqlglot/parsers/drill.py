from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_formatted_time
from sqlglot.tokens import TokenType


class DrillParser(parser.Parser):
    STRICT_CAST = False

    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {
        TokenType.ANTI,
        TokenType.SEMI,
    }

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "REPEATED_COUNT": exp.ArraySize.from_arg_list,
        "TO_TIMESTAMP": exp.TimeStrToTime.from_arg_list,
        "TO_CHAR": build_formatted_time(exp.TimeToStr, "drill"),
        "LEVENSHTEIN_DISTANCE": exp.Levenshtein.from_arg_list,
    }

    LOG_DEFAULTS_TO_LN = True
