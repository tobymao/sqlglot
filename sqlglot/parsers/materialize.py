from __future__ import annotations


from sqlglot import exp
from sqlglot.helper import seq_get
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import TokenType


class MaterializeParser(PostgresParser):
    TYPED_LAMBDA_ARGS = True

    NO_PAREN_FUNCTION_PARSERS = {
        **PostgresParser.NO_PAREN_FUNCTION_PARSERS,
        "MAP": lambda self: self._parse_map(),
    }

    LAMBDAS = {
        **PostgresParser.LAMBDAS,
        TokenType.FARROW: lambda self, expressions: self.expression(
            exp.Kwarg(this=seq_get(expressions, 0), expression=self._parse_assignment())
        ),
    }

    def _parse_lambda_arg(self) -> exp.Expr | None:
        return self._parse_field()

    def _parse_map(self) -> exp.ToMap:
        if self._match(TokenType.L_PAREN):
            to_map = self.expression(exp.ToMap(this=self._parse_select()))
            self._match_r_paren()
            return to_map

        if not self._match(TokenType.L_BRACKET):
            self.raise_error("Expecting [")

        entries = [
            exp.PropertyEQ(this=e.this, expression=e.expression)
            for e in self._parse_csv(self._parse_lambda)
        ]

        if not self._match(TokenType.R_BRACKET):
            self.raise_error("Expecting ]")

        return self.expression(exp.ToMap(this=self.expression(exp.Struct(expressions=entries))))
