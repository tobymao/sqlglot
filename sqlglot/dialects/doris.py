from __future__ import annotations

from sqlglot.dialects.mysql import MySQL
from sqlglot.generators.doris import DorisGenerator
from sqlglot.parsers.doris import DorisParser


class Doris(MySQL):
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    class Tokenizer(MySQL.Tokenizer):
        DASH_COMMENT_REQUIRES_BOUNDARY = False
        COMMENTS_TERMINATE_AT_NEWLINE_ONLY = False

    Parser = DorisParser

    Generator = DorisGenerator
