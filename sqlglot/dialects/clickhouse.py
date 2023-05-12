from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect, inline_array_sql, var_map_sql
from sqlglot.errors import ParseError
from sqlglot.helper import ensure_list, seq_get
from sqlglot.parser import parse_var_map
from sqlglot.tokens import TokenType


def _lower_func(sql: str) -> str:
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


class ClickHouse(Dialect):
    normalize_functions = None
    null_ordering = "nulls_are_last"

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "#", "#!", ("/*", "*/")]
        IDENTIFIERS = ['"', "`"]
        BIT_STRINGS = [("0b", "")]
        HEX_STRINGS = [("0x", ""), ("0X", "")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ASOF": TokenType.ASOF,
            "GLOBAL": TokenType.GLOBAL,
            "DATETIME64": TokenType.DATETIME64,
            "FINAL": TokenType.FINAL,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
            "INT8": TokenType.TINYINT,
            "UINT8": TokenType.UTINYINT,
            "INT16": TokenType.SMALLINT,
            "UINT16": TokenType.USMALLINT,
            "INT32": TokenType.INT,
            "UINT32": TokenType.UINT,
            "INT64": TokenType.BIGINT,
            "UINT64": TokenType.UBIGINT,
            "INT128": TokenType.INT128,
            "UINT128": TokenType.UINT128,
            "INT256": TokenType.INT256,
            "UINT256": TokenType.UINT256,
            "TUPLE": TokenType.STRUCT,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "EXPONENTIALTIMEDECAYEDAVG": lambda params, args: exp.ExponentialTimeDecayedAvg(
                this=seq_get(args, 0),
                time=seq_get(args, 1),
                decay=seq_get(params, 0),
            ),
            "GROUPUNIQARRAY": lambda params, args: exp.GroupUniqArray(
                this=seq_get(args, 0), size=seq_get(params, 0)
            ),
            "HISTOGRAM": lambda params, args: exp.Histogram(
                this=seq_get(args, 0), bins=seq_get(params, 0)
            ),
            "MAP": parse_var_map,
            "MATCH": exp.RegexpLike.from_arg_list,
            "QUANTILE": lambda params, args: exp.Quantile(this=args, quantile=params),
            "QUANTILES": lambda params, args: exp.Quantiles(parameters=params, expressions=args),
            "QUANTILEIF": lambda params, args: exp.QuantileIf(parameters=params, expressions=args),
        }

        FUNCTION_PARSERS = parser.Parser.FUNCTION_PARSERS.copy()
        FUNCTION_PARSERS.pop("MATCH")

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.GLOBAL: lambda self, this: self._match(TokenType.IN)
            and self._parse_in(this, is_global=True),
        }

        # The PLACEHOLDER entry is popped because 1) it doesn't affect Clickhouse (it corresponds to
        # the postgres-specific JSONBContains parser) and 2) it makes parsing the ternary op simpler.
        COLUMN_OPERATORS = parser.Parser.COLUMN_OPERATORS.copy()
        COLUMN_OPERATORS.pop(TokenType.PLACEHOLDER)

        JOIN_KINDS = {*parser.Parser.JOIN_KINDS, TokenType.ANY, TokenType.ASOF}

        TABLE_ALIAS_TOKENS = {*parser.Parser.TABLE_ALIAS_TOKENS} - {TokenType.ANY}

        LOG_DEFAULTS_TO_LN = True

        def _parse_expression(self, explicit_alias: bool = False) -> t.Optional[exp.Expression]:
            return self._parse_alias(self._parse_ternary(), explicit=explicit_alias)

        def _parse_ternary(self) -> t.Optional[exp.Expression]:
            this = self._parse_conjunction()

            if self._match(TokenType.PLACEHOLDER):
                return self.expression(
                    exp.If,
                    this=this,
                    true=self._parse_conjunction(),
                    false=self._match(TokenType.COLON) and self._parse_conjunction(),
                )

            return this

        def _parse_in(
            self, this: t.Optional[exp.Expression], is_global: bool = False
        ) -> exp.Expression:
            this = super()._parse_in(this)
            this.set("is_global", is_global)
            return this

        def _parse_table(
            self, schema: bool = False, alias_tokens: t.Optional[t.Collection[TokenType]] = None
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_table(schema=schema, alias_tokens=alias_tokens)

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

        def _parse_position(self, haystack_first: bool = False) -> exp.Expression:
            return super()._parse_position(haystack_first=True)

        # https://clickhouse.com/docs/en/sql-reference/statements/select/with/
        def _parse_cte(self) -> exp.Expression:
            index = self._index
            try:
                # WITH <identifier> AS <subquery expression>
                return super()._parse_cte()
            except ParseError:
                # WITH <expression> AS <identifier>
                self._retreat(index)
                statement = self._parse_statement()

                if statement and isinstance(statement.this, exp.Alias):
                    self.raise_error("Expected CTE to have alias")

                return self.expression(exp.CTE, this=statement, alias=statement and statement.this)

    class Generator(generator.Generator):
        STRUCT_DELIMITER = ("(", ")")

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.NULLABLE: "Nullable",
            exp.DataType.Type.DATETIME64: "DateTime64",
            exp.DataType.Type.MAP: "Map",
            exp.DataType.Type.ARRAY: "Array",
            exp.DataType.Type.STRUCT: "Tuple",
            exp.DataType.Type.TINYINT: "Int8",
            exp.DataType.Type.UTINYINT: "UInt8",
            exp.DataType.Type.SMALLINT: "Int16",
            exp.DataType.Type.USMALLINT: "UInt16",
            exp.DataType.Type.INT: "Int32",
            exp.DataType.Type.UINT: "UInt32",
            exp.DataType.Type.BIGINT: "Int64",
            exp.DataType.Type.UBIGINT: "UInt64",
            exp.DataType.Type.INT128: "Int128",
            exp.DataType.Type.UINT128: "UInt128",
            exp.DataType.Type.INT256: "Int256",
            exp.DataType.Type.UINT256: "UInt256",
            exp.DataType.Type.FLOAT: "Float32",
            exp.DataType.Type.DOUBLE: "Float64",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.Array: inline_array_sql,
            exp.ExponentialTimeDecayedAvg: lambda self, e: f"exponentialTimeDecayedAvg{self._param_args_sql(e, 'decay', ['this', 'time'])}",
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
            exp.GroupUniqArray: lambda self, e: f"groupUniqArray{self._param_args_sql(e, 'size', 'this')}",
            exp.Histogram: lambda self, e: f"histogram{self._param_args_sql(e, 'bins', 'this')}",
            exp.Map: lambda self, e: _lower_func(var_map_sql(self, e)),
            exp.Quantile: lambda self, e: f"quantile{self._param_args_sql(e, 'quantile', 'this')}",
            exp.Quantiles: lambda self, e: f"quantiles{self._param_args_sql(e, 'parameters', 'expressions')}",
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.QuantileIf: lambda self, e: f"quantileIf{self._param_args_sql(e, 'parameters', 'expressions')}",
            exp.RegexpLike: lambda self, e: f"match({self.format_args(e.this, e.expression)})",
            exp.StrPosition: lambda self, e: f"position({self.format_args(e.this, e.args.get('substr'), e.args.get('position'))})",
            exp.VarMap: lambda self, e: _lower_func(var_map_sql(self, e)),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
        }

        JOIN_HINTS = False
        TABLE_HINTS = False
        EXPLICIT_UNION = True

        def _param_args_sql(
            self,
            expression: exp.Expression,
            param_names: str | t.List[str],
            arg_names: str | t.List[str],
        ) -> str:
            params = self.format_args(
                *(
                    arg
                    for name in ensure_list(param_names)
                    for arg in ensure_list(expression.args.get(name))
                )
            )
            args = self.format_args(
                *(
                    arg
                    for name in ensure_list(arg_names)
                    for arg in ensure_list(expression.args.get(name))
                )
            )
            return f"({params})({args})"

        def cte_sql(self, expression: exp.CTE) -> str:
            if isinstance(expression.this, exp.Alias):
                return self.sql(expression, "this")

            return super().cte_sql(expression)
