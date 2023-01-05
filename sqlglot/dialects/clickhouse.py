from __future__ import annotations

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect, inline_array_sql, var_map_sql
from sqlglot.parser import parse_var_map
from sqlglot.tokens import TokenType


def _lower_func(sql):
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


def _quantile_sql(self, expression: exp.Quantile) -> str:
    if_suffix = "If" if len(expression.this) == 2 else ""
    args = self.format_args(self.expressions(expression, "quantile"))
    params = self.format_args(self.expressions(expression, "this"))
    return f"quantile{if_suffix}({args})({params})"


class ClickHouse(Dialect):
    normalize_functions = None
    null_ordering = "nulls_are_last"

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "#", "#!", ("/*", "*/")]
        IDENTIFIERS = ['"', "`"]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ASOF": TokenType.ASOF,
            "DATETIME64": TokenType.DATETIME,
            "FINAL": TokenType.FINAL,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
            "INT16": TokenType.SMALLINT,
            "INT32": TokenType.INT,
            "INT64": TokenType.BIGINT,
            "INT8": TokenType.TINYINT,
            "TUPLE": TokenType.STRUCT,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "MAP": parse_var_map,
            "QUANTILE": lambda args, params: exp.Quantile(this=params, quantile=args),
            "QUANTILEIF": lambda args, params: exp.Quantile(this=params, quantile=args),
        }

        JOIN_KINDS = {*parser.Parser.JOIN_KINDS, TokenType.ANY, TokenType.ASOF}  # type: ignore

        TABLE_ALIAS_TOKENS = {*parser.Parser.TABLE_ALIAS_TOKENS} - {TokenType.ANY}  # type: ignore

        def _parse_table(self, schema=False):
            this = super()._parse_table(schema)

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

    class Generator(generator.Generator):
        STRUCT_DELIMITER = ("(", ")")

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.NULLABLE: "Nullable",
            exp.DataType.Type.DATETIME: "DateTime64",
            exp.DataType.Type.MAP: "Map",
            exp.DataType.Type.ARRAY: "Array",
            exp.DataType.Type.STRUCT: "Tuple",
            exp.DataType.Type.TINYINT: "Int8",
            exp.DataType.Type.SMALLINT: "Int16",
            exp.DataType.Type.INT: "Int32",
            exp.DataType.Type.BIGINT: "Int64",
            exp.DataType.Type.FLOAT: "Float32",
            exp.DataType.Type.DOUBLE: "Float64",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.Array: inline_array_sql,
            exp.StrPosition: lambda self, e: f"position({self.format_args(e.this, e.args.get('substr'), e.args.get('position'))})",
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
            exp.Map: lambda self, e: _lower_func(var_map_sql(self, e)),
            exp.VarMap: lambda self, e: _lower_func(var_map_sql(self, e)),
            exp.Quantile: _quantile_sql,
        }

        EXPLICIT_UNION = True
