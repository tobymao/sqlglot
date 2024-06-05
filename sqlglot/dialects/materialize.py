from __future__ import annotations

from sqlglot import exp
from sqlglot.helper import seq_get
from sqlglot.dialects.postgres import Postgres

from sqlglot.tokens import TokenType
from sqlglot.transforms import (
    remove_unique_constraints,
    ctas_with_tmp_tables_to_create_tmp_view,
    preprocess,
)


class Materialize(Postgres):
    class Parser(Postgres.Parser):
        NO_PAREN_FUNCTION_PARSERS = {
            **Postgres.Parser.NO_PAREN_FUNCTION_PARSERS,
            "LIST": lambda self: self._parse_list(),
            "MAP": lambda self: self._parse_map(),
        }

        def _parse_list(self) -> exp.List:
            if self._match(TokenType.L_PAREN):
                e = self.expression(exp.List, expressions=[self._parse_select()])
                self._match_r_paren()
                return e

            if not self._match(TokenType.L_BRACKET):
                self.raise_error("Expecting [")
            entries = self._parse_csv(self._parse_conjunction)
            if not self._match(TokenType.R_BRACKET):
                self.raise_error("Expecting ]")
            return self.expression(exp.List, expressions=entries)

        def _parse_map(self) -> exp.ToMap:
            if self._match(TokenType.L_PAREN):
                to_map = self.expression(exp.ToMap, this=self._parse_select())
                self._match_r_paren()
                return to_map

            if not self._match(TokenType.L_BRACKET):
                self.raise_error("Expecting [")
            entries = self._parse_csv(self._parse_map_entry)
            if not self._match(TokenType.R_BRACKET):
                self.raise_error("Expecting ]")
            return self.expression(exp.ToMap, this=self.expression(exp.Struct, expressions=entries))

        def _parse_map_entry(self) -> exp.PropertyEQ | None:
            key = self._parse_conjunction()
            if not key:
                return None
            if not self._match(TokenType.FARROW):
                self.raise_error("Expected =>")
            value = self._parse_conjunction()
            return self.expression(exp.PropertyEQ, this=key, expression=value)

    class Generator(Postgres.Generator):
        SUPPORTS_CREATE_TABLE_LIKE = False

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.AutoIncrementColumnConstraint: lambda self, e: "",
            exp.Create: preprocess(
                [
                    remove_unique_constraints,
                    ctas_with_tmp_tables_to_create_tmp_view,
                ]
            ),
            exp.DataType: lambda self, e: self._datatype_sql(e),
            exp.GeneratedAsIdentityColumnConstraint: lambda self, e: "",
            exp.OnConflict: lambda self, e: "",
            exp.PrimaryKeyColumnConstraint: lambda self, e: "",
            exp.List: lambda self, e: self._list_sql(e),
            exp.ToMap: lambda self, e: self._to_map_sql(e),
        }

        def _datatype_sql(self: Materialize.Generator, e: exp.DataType) -> str:
            if e.is_type("list"):
                if e.expressions:
                    return f"{self.expressions(e, flat=True)} LIST"
                return "LIST"
            elif e.is_type("map"):
                if e.expressions:
                    key, value = e.expressions
                    return f"MAP[{self.sql(key)} => {self.sql(value)}]"
            return Postgres.Generator.TRANSFORMS[exp.DataType](self, e)

        def _list_sql(self: Materialize.Generator, e: exp.List) -> str:
            if isinstance(seq_get(e.expressions, 0), exp.Select):
                return f"{self.normalize_func('LIST')}({self.sql(seq_get(e.expressions, 0))})"

            return f"{self.normalize_func('LIST')}[{self.expressions(e, flat=True)}]"

        def _to_map_sql(self: Materialize.Generator, e: exp.ToMap) -> str:
            if isinstance(e.this, exp.Select):
                return f"{self.normalize_func('MAP')}({self.sql(e.this)})"

            entries = ", ".join(
                f"{self.sql(e.this)} => {self.sql(e.expression)}" for e in e.this.expressions
            )
            return f"{self.normalize_func('MAP')}[{entries}]"
