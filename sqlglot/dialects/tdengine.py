from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import ErrorLevel
from sqlglot.tokens import TokenType

BACKTICK = "`"


class TDengine(Dialect):
    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', BACKTICK]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "TIMETRUNCATE": TokenType.TIMETRUNCATE,
        }

        TIME_UNITS = {"b", "u", "a", "s", "m", "h", "d", "w"}

        def _scan_number(self) -> None:
            start_pos = self._current - 1
            decimal = False
            scientific = 0

            while True:
                if self._peek.isdigit():
                    self._advance()
                elif self._peek == "." and not decimal:
                    decimal = True
                    self._advance()
                elif self._peek in ("-", "") and scientific == 1:
                    scientific = 1
                    self._advance()
                elif self._peek.upper() == "E" and not scientific:
                    scientific = 1
                    self._advance()
                else:
                    break

            numeric_part = self.sql[start_pos : self._current]
            current_peek = self._peek

            if current_peek.lower() in self.TIME_UNITS:
                full_literal = numeric_part + current_peek
                self._advance()
                self._add(TokenType.VAR, text=full_literal)
            else:
                self._add(TokenType.NUMBER, text=numeric_part)

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "TIMETRUNCATE": exp.TimeTruncate.from_arg_list,
        }

    class Generator(generator.Generator):
        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Count: lambda self, e: self.count_sql(e),
            exp.TimeTruncate: lambda self, e: self.timetruncate_sql(e),
        }

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.unsupported_level = ErrorLevel.IMMEDIATE
            self._identifier_start = BACKTICK
            self._identifier_end = BACKTICK

        def count_sql(self, expression: exp.Count) -> str:
            if isinstance(expression.this, exp.Distinct):
                self.unsupported("TDengine does not support COUNT(DISTINCT ...)")

            this = self.sql(expression, "this")
            return f"COUNT({this or '*'})"

        def timetruncate_sql(self, expression: exp.TimeTruncate) -> str:
            this = self.sql(expression.this)
            unit = self.sql(expression.args.get("unit")).strip(BACKTICK)
            tz = expression.args.get("use_current_timezone")
            if tz is not None:
                tz_sql = self.sql(tz)
                return f"TIMETRUNCATE({this}, {unit}, {tz_sql})"
            return f"TIMETRUNCATE({this}, {unit})"
