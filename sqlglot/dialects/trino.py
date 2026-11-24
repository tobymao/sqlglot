from __future__ import annotations

from sqlglot import expressions as exp
from sqlglot.dialects.presto import Presto
from sqlglot.generators.trino import TrinoGenerator
from sqlglot.parsers.trino import TrinoParser
from sqlglot.tokens import TokenType


class Trino(Presto):
    SUPPORTS_USER_DEFINED_TYPES = False
    LOG_BASE_FIRST = True
    CONCAT_WS_COALESCE = True
    NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = True

    def int_literal_type(self, expression: exp.Literal) -> exp.DataType | exp.DType:
        value = int(expression.to_py())
        if isinstance(expression.parent, exp.Neg):
            value -= 1
        return self.integer_type_for_value(value)

    class Tokenizer(Presto.Tokenizer):
        COMMANDS = Presto.Tokenizer.COMMANDS - {TokenType.EXECUTE}
        KEYWORDS = {
            **Presto.Tokenizer.KEYWORDS,
            "REFRESH": TokenType.REFRESH,
            "DECLARE": TokenType.DECLARE,
        }
        # Trino has no `SQL SECURITY` clause, only bare `SECURITY DEFINER`/`INVOKER`;
        # the merged base keyword otherwise eats the `SQL` in `LANGUAGE SQL SECURITY DEFINER`.
        KEYWORDS.pop("SQL SECURITY")

    Parser = TrinoParser

    Generator = TrinoGenerator
