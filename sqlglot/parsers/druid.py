from __future__ import annotations

from sqlglot.parser import Parser


class DruidParser(Parser):
    FUNCTION_PARSERS = {
        **Parser.FUNCTION_PARSERS,
        "JSON_VALUE": lambda self: self._parse_json_value(),
    }
