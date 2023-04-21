from sqlglot.betterbrain import Suggestion
from sqlglot.tokens import Token


import typing as t


class BetterBrainDialectMixin:
    @t.final
    def suggest(self, sql: str, **opts) -> Suggestion:
        return self.parser(**{**opts, "tokenizer": self.tokenizer}).suggest(self.tokenize_with_cursor(sql), sql)

    @t.final
    def tokenize_with_cursor(self, sql: str) -> t.List[Token]:
        return self.tokenizer.tokenize(sql, True)