from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    inline_array_sql,
    no_pivot_sql,
    rename_func,
    var_map_sql,
)
from sqlglot.errors import ParseError
from sqlglot.parser import parse_var_map
from sqlglot.tokens import Token, TokenType


def _lower_func(sql: str) -> str:
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


class ClickHouse(Dialect):
    NORMALIZE_FUNCTIONS: bool | str = False
    NULL_ORDERING = "nulls_are_last"
    STRICT_STRING_CONCAT = True

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "#", "#!", ("/*", "*/")]
        IDENTIFIERS = ['"', "`"]
        STRING_ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("0b", "")]
        HEX_STRINGS = [("0x", ""), ("0X", "")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ATTACH": TokenType.COMMAND,
            "DATETIME64": TokenType.DATETIME64,
            "DICTIONARY": TokenType.DICTIONARY,
            "FINAL": TokenType.FINAL,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
            "GLOBAL": TokenType.GLOBAL,
            "INT128": TokenType.INT128,
            "INT16": TokenType.SMALLINT,
            "INT256": TokenType.INT256,
            "INT32": TokenType.INT,
            "INT64": TokenType.BIGINT,
            "INT8": TokenType.TINYINT,
            "MAP": TokenType.MAP,
            "TUPLE": TokenType.STRUCT,
            "UINT128": TokenType.UINT128,
            "UINT16": TokenType.USMALLINT,
            "UINT256": TokenType.UINT256,
            "UINT32": TokenType.UINT,
            "UINT64": TokenType.UBIGINT,
            "UINT8": TokenType.UTINYINT,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ANY": exp.AnyValue.from_arg_list,
            "MAP": parse_var_map,
            "MATCH": exp.RegexpLike.from_arg_list,
            "UNIQ": exp.ApproxDistinct.from_arg_list,
            "XOR": lambda args: exp.Xor(expressions=args),
        }

        FUNCTIONS_WITH_ALIASED_ARGS = {*parser.Parser.FUNCTIONS_WITH_ALIASED_ARGS, "TUPLE"}

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "QUANTILE": lambda self: self._parse_quantile(),
        }

        FUNCTION_PARSERS.pop("MATCH")

        NO_PAREN_FUNCTION_PARSERS = parser.Parser.NO_PAREN_FUNCTION_PARSERS.copy()
        NO_PAREN_FUNCTION_PARSERS.pop(TokenType.ANY)

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.GLOBAL: lambda self, this: self._match(TokenType.IN)
            and self._parse_in(this, is_global=True),
        }

        # The PLACEHOLDER entry is popped because 1) it doesn't affect Clickhouse (it corresponds to
        # the postgres-specific JSONBContains parser) and 2) it makes parsing the ternary op simpler.
        COLUMN_OPERATORS = parser.Parser.COLUMN_OPERATORS.copy()
        COLUMN_OPERATORS.pop(TokenType.PLACEHOLDER)

        JOIN_KINDS = {
            *parser.Parser.JOIN_KINDS,
            TokenType.ANY,
            TokenType.ASOF,
            TokenType.ANTI,
            TokenType.SEMI,
            TokenType.ARRAY,
        }

        TABLE_ALIAS_TOKENS = {*parser.Parser.TABLE_ALIAS_TOKENS} - {
            TokenType.ANY,
            TokenType.SEMI,
            TokenType.ANTI,
            TokenType.SETTINGS,
            TokenType.FORMAT,
            TokenType.ARRAY,
        }

        LOG_DEFAULTS_TO_LN = True

        QUERY_MODIFIER_PARSERS = {
            **parser.Parser.QUERY_MODIFIER_PARSERS,
            TokenType.SETTINGS: lambda self: (
                "settings",
                self._advance() or self._parse_csv(self._parse_conjunction),
            ),
            TokenType.FORMAT: lambda self: ("format", self._advance() or self._parse_id_var()),
        }

        def _parse_conjunction(self) -> t.Optional[exp.Expression]:
            this = super()._parse_conjunction()

            if self._match(TokenType.PLACEHOLDER):
                return self.expression(
                    exp.If,
                    this=this,
                    true=self._parse_conjunction(),
                    false=self._match(TokenType.COLON) and self._parse_conjunction(),
                )

            return this

        def _parse_placeholder(self) -> t.Optional[exp.Expression]:
            """
            Parse a placeholder expression like SELECT {abc: UInt32} or FROM {table: Identifier}
            https://clickhouse.com/docs/en/sql-reference/syntax#defining-and-using-query-parameters
            """
            if not self._match(TokenType.L_BRACE):
                return None

            this = self._parse_id_var()
            self._match(TokenType.COLON)
            kind = self._parse_types(check_func=False) or (
                self._match_text_seq("IDENTIFIER") and "Identifier"
            )

            if not kind:
                self.raise_error("Expecting a placeholder type or 'Identifier' for tables")
            elif not self._match(TokenType.R_BRACE):
                self.raise_error("Expecting }")

            return self.expression(exp.Placeholder, this=this, kind=kind)

        def _parse_in(self, this: t.Optional[exp.Expression], is_global: bool = False) -> exp.In:
            this = super()._parse_in(this)
            this.set("is_global", is_global)
            return this

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_table(
                schema=schema, joins=joins, alias_tokens=alias_tokens, parse_bracket=parse_bracket
            )

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

        def _parse_position(self, haystack_first: bool = False) -> exp.StrPosition:
            return super()._parse_position(haystack_first=True)

        # https://clickhouse.com/docs/en/sql-reference/statements/select/with/
        def _parse_cte(self) -> exp.CTE:
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

        def _parse_join_parts(
            self,
        ) -> t.Tuple[t.Optional[Token], t.Optional[Token], t.Optional[Token]]:
            is_global = self._match(TokenType.GLOBAL) and self._prev
            kind_pre = self._match_set(self.JOIN_KINDS, advance=False) and self._prev

            if kind_pre:
                kind = self._match_set(self.JOIN_KINDS) and self._prev
                side = self._match_set(self.JOIN_SIDES) and self._prev
                return is_global, side, kind

            return (
                is_global,
                self._match_set(self.JOIN_SIDES) and self._prev,
                self._match_set(self.JOIN_KINDS) and self._prev,
            )

        def _parse_join(
            self, skip_join_token: bool = False, parse_bracket: bool = False
        ) -> t.Optional[exp.Join]:
            join = super()._parse_join(skip_join_token=skip_join_token, parse_bracket=True)

            if join:
                join.set("global", join.args.pop("method", None))
            return join

        def _parse_function(
            self,
            functions: t.Optional[t.Dict[str, t.Callable]] = None,
            anonymous: bool = False,
            optional_parens: bool = True,
        ) -> t.Optional[exp.Expression]:
            func = super()._parse_function(
                functions=functions, anonymous=anonymous, optional_parens=optional_parens
            )

            if isinstance(func, exp.Anonymous):
                params = self._parse_func_params(func)

                if params:
                    return self.expression(
                        exp.ParameterizedAgg,
                        this=func.this,
                        expressions=func.expressions,
                        params=params,
                    )

            return func

        def _parse_func_params(
            self, this: t.Optional[exp.Func] = None
        ) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
            if self._match_pair(TokenType.R_PAREN, TokenType.L_PAREN):
                return self._parse_csv(self._parse_lambda)

            if self._match(TokenType.L_PAREN):
                params = self._parse_csv(self._parse_lambda)
                self._match_r_paren(this)
                return params

            return None

        def _parse_quantile(self) -> exp.Quantile:
            this = self._parse_lambda()
            params = self._parse_func_params()
            if params:
                return self.expression(exp.Quantile, this=params[0], quantile=this)
            return self.expression(exp.Quantile, this=this, quantile=exp.Literal.number(0.5))

        def _parse_wrapped_id_vars(
            self, optional: bool = False
        ) -> t.List[t.Optional[exp.Expression]]:
            return super()._parse_wrapped_id_vars(optional=True)

        def _parse_primary_key(
            self, wrapped_optional: bool = False, in_props: bool = False
        ) -> exp.PrimaryKeyColumnConstraint | exp.PrimaryKey:
            return super()._parse_primary_key(
                wrapped_optional=wrapped_optional or in_props, in_props=in_props
            )

        def _parse_on_property(self) -> t.Optional[exp.Expression]:
            index = self._index
            if self._match_text_seq("CLUSTER"):
                this = self._parse_id_var()
                if this:
                    return self.expression(exp.OnCluster, this=this)
                else:
                    self._retreat(index)
            return None

    class Generator(generator.Generator):
        QUERY_HINTS = False
        STRUCT_DELIMITER = ("(", ")")

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.ARRAY: "Array",
            exp.DataType.Type.BIGINT: "Int64",
            exp.DataType.Type.DATETIME64: "DateTime64",
            exp.DataType.Type.DOUBLE: "Float64",
            exp.DataType.Type.FLOAT: "Float32",
            exp.DataType.Type.INT: "Int32",
            exp.DataType.Type.INT128: "Int128",
            exp.DataType.Type.INT256: "Int256",
            exp.DataType.Type.MAP: "Map",
            exp.DataType.Type.NULLABLE: "Nullable",
            exp.DataType.Type.SMALLINT: "Int16",
            exp.DataType.Type.STRUCT: "Tuple",
            exp.DataType.Type.TINYINT: "Int8",
            exp.DataType.Type.UBIGINT: "UInt64",
            exp.DataType.Type.UINT: "UInt32",
            exp.DataType.Type.UINT128: "UInt128",
            exp.DataType.Type.UINT256: "UInt256",
            exp.DataType.Type.USMALLINT: "UInt16",
            exp.DataType.Type.UTINYINT: "UInt8",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: rename_func("any"),
            exp.ApproxDistinct: rename_func("uniq"),
            exp.Array: inline_array_sql,
            exp.CastToStrType: rename_func("CAST"),
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
            exp.Map: lambda self, e: _lower_func(var_map_sql(self, e)),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.Pivot: no_pivot_sql,
            exp.Quantile: lambda self, e: self.func("quantile", e.args.get("quantile"))
            + f"({self.sql(e, 'this')})",
            exp.RegexpLike: lambda self, e: f"match({self.format_args(e.this, e.expression)})",
            exp.StrPosition: lambda self, e: f"position({self.format_args(e.this, e.args.get('substr'), e.args.get('position'))})",
            exp.VarMap: lambda self, e: _lower_func(var_map_sql(self, e)),
            exp.Xor: lambda self, e: self.func("xor", e.this, e.expression, *e.expressions),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.OnCluster: exp.Properties.Location.POST_NAME,
        }

        JOIN_HINTS = False
        TABLE_HINTS = False
        EXPLICIT_UNION = True
        GROUPINGS_SEP = ""

        # there's no list in docs, but it can be found in Clickhouse code
        # see `ClickHouse/src/Parsers/ParserCreate*.cpp`
        ON_CLUSTER_TARGETS = {
            "DATABASE",
            "TABLE",
            "VIEW",
            "DICTIONARY",
            "INDEX",
            "FUNCTION",
            "NAMED COLLECTION",
        }

        def safeconcat_sql(self, expression: exp.SafeConcat) -> str:
            # Clickhouse errors out if we try to cast a NULL value to TEXT
            return self.func(
                "CONCAT",
                *[
                    exp.func("if", e.is_(exp.null()), e, exp.cast(e, "text"))
                    for e in t.cast(t.List[exp.Condition], expression.expressions)
                ],
            )

        def cte_sql(self, expression: exp.CTE) -> str:
            if isinstance(expression.this, exp.Alias):
                return self.sql(expression, "this")

            return super().cte_sql(expression)

        def after_limit_modifiers(self, expression: exp.Expression) -> t.List[str]:
            return super().after_limit_modifiers(expression) + [
                self.seg("SETTINGS ") + self.expressions(expression, key="settings", flat=True)
                if expression.args.get("settings")
                else "",
                self.seg("FORMAT ") + self.sql(expression, "format")
                if expression.args.get("format")
                else "",
            ]

        def parameterizedagg_sql(self, expression: exp.Anonymous) -> str:
            params = self.expressions(expression, key="params", flat=True)
            return self.func(expression.name, *expression.expressions) + f"({params})"

        def placeholder_sql(self, expression: exp.Placeholder) -> str:
            return f"{{{expression.name}: {self.sql(expression, 'kind')}}}"

        def oncluster_sql(self, expression: exp.OnCluster) -> str:
            return f"ON CLUSTER {self.sql(expression, 'this')}"

        def createable_sql(
            self,
            expression: exp.Create,
            locations: dict[exp.Properties.Location, list[exp.Property]],
        ) -> str:
            kind = self.sql(expression, "kind").upper()
            if kind in self.ON_CLUSTER_TARGETS and locations.get(exp.Properties.Location.POST_NAME):
                this_name = self.sql(expression.this, "this")
                this_properties = " ".join(
                    [self.sql(prop) for prop in locations[exp.Properties.Location.POST_NAME]]
                )
                this_schema = self.schema_columns_sql(expression.this)
                return f"{this_name}{self.sep()}{this_properties}{self.sep()}{this_schema}"
            return super().createable_sql(expression, locations)
