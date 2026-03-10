from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_formatted_time


class DrillParser(parser.Parser):
    STRICT_CAST = False

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "REPEATED_COUNT": exp.ArraySize.from_arg_list,
        "TO_TIMESTAMP": exp.TimeStrToTime.from_arg_list,
        "TO_CHAR": build_formatted_time(exp.TimeToStr, "drill"),
        "LEVENSHTEIN_DISTANCE": exp.Levenshtein.from_arg_list,
    }

    LOG_DEFAULTS_TO_LN = True
