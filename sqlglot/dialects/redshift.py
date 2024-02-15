from __future__ import annotations

import typing as t

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    NormalizationStrategy,
    concat_to_dpipe_sql,
    concat_ws_to_dpipe_sql,
    date_delta_sql,
    generatedasidentitycolumnconstraint_sql,
    json_extract_segments,
    no_tablesample_sql,
    rename_func,
)
from sqlglot.dialects.postgres import Postgres
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def _build_date_delta(expr_type: t.Type[E]) -> t.Callable[[t.List], E]:
    def _builder(args: t.List) -> E:
        expr = expr_type(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0))
        if expr_type is exp.TsOrDsAdd:
            expr.set("return_type", exp.DataType.build("TIMESTAMP"))

        return expr

    return _builder


class Redshift(Postgres):
    # https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    SUPPORTS_USER_DEFINED_TYPES = False
    INDEX_OFFSET = 0

    TIME_FORMAT = "'YYYY-MM-DD HH:MI:SS'"
    TIME_MAPPING = {
        **Postgres.TIME_MAPPING,
        "MON": "%b",
        "HH": "%H",
    }

    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,
            "ADD_MONTHS": lambda args: exp.TsOrDsAdd(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                unit=exp.var("month"),
                return_type=exp.DataType.build("TIMESTAMP"),
            ),
            "DATEADD": _build_date_delta(exp.TsOrDsAdd),
            "DATE_ADD": _build_date_delta(exp.TsOrDsAdd),
            "DATEDIFF": _build_date_delta(exp.TsOrDsDiff),
            "DATE_DIFF": _build_date_delta(exp.TsOrDsDiff),
            "GETDATE": exp.CurrentTimestamp.from_arg_list,
            "LISTAGG": exp.GroupConcat.from_arg_list,
            "STRTOL": exp.FromBase.from_arg_list,
        }

        NO_PAREN_FUNCTION_PARSERS = {
            **Postgres.Parser.NO_PAREN_FUNCTION_PARSERS,
            "APPROXIMATE": lambda self: self._parse_approximate_count(),
            "SYSDATE": lambda self: self.expression(exp.CurrentTimestamp, transaction=True),
        }

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
            is_db_reference: bool = False,
        ) -> t.Optional[exp.Expression]:
            # Redshift supports UNPIVOTing SUPER objects, e.g. `UNPIVOT foo.obj[0] AS val AT attr`
            unpivot = self._match(TokenType.UNPIVOT)
            table = super()._parse_table(
                schema=schema,
                joins=joins,
                alias_tokens=alias_tokens,
                parse_bracket=parse_bracket,
                is_db_reference=is_db_reference,
            )

            return self.expression(exp.Pivot, this=table, unpivot=True) if unpivot else table

        def _parse_types(
            self, check_func: bool = False, schema: bool = False, allow_identifiers: bool = True
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_types(
                check_func=check_func, schema=schema, allow_identifiers=allow_identifiers
            )

            if (
                isinstance(this, exp.DataType)
                and this.is_type("varchar")
                and this.expressions
                and this.expressions[0].this == exp.column("MAX")
            ):
                this.set("expressions", [exp.var("MAX")])

            return this

        def _parse_convert(
            self, strict: bool, safe: t.Optional[bool] = None
        ) -> t.Optional[exp.Expression]:
            to = self._parse_types()
            self._match(TokenType.COMMA)
            this = self._parse_bitwise()
            return self.expression(exp.TryCast, this=this, to=to, safe=safe)

        def _parse_approximate_count(self) -> t.Optional[exp.ApproxDistinct]:
            index = self._index - 1
            func = self._parse_function()

            if isinstance(func, exp.Count) and isinstance(func.this, exp.Distinct):
                return self.expression(exp.ApproxDistinct, this=seq_get(func.this.expressions, 0))
            self._retreat(index)
            return None

        def _parse_query_modifiers(
            self, this: t.Optional[exp.Expression]
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_query_modifiers(this)

            if this:
                refs = set()

                for i, join in enumerate(this.args.get("joins", [])):
                    refs.add(
                        (
                            this.args["from"] if i == 0 else this.args["joins"][i - 1]
                        ).this.alias.lower()
                    )

                    table = join.this
                    if isinstance(table, exp.Table) and not join.args.get("on"):
                        if table.parts[0].name.lower() in refs:
                            table.replace(table.to_column())
            return this

    class Tokenizer(Postgres.Tokenizer):
        BIT_STRINGS = []
        HEX_STRINGS = []
        STRING_ESCAPES = ["\\", "'"]

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "HLLSKETCH": TokenType.HLLSKETCH,
            "SUPER": TokenType.SUPER,
            "TOP": TokenType.TOP,
            "UNLOAD": TokenType.COMMAND,
            "VARBYTE": TokenType.VARBINARY,
        }
        KEYWORDS.pop("VALUES")

        # Redshift allows # to appear as a table identifier prefix
        SINGLE_TOKENS = Postgres.Tokenizer.SINGLE_TOKENS.copy()
        SINGLE_TOKENS.pop("#")

    class Generator(Postgres.Generator):
        LOCKING_READS_SUPPORTED = False
        QUERY_HINTS = False
        VALUES_AS_TABLE = False
        TZ_TO_WITH_TIME_ZONE = True
        NVL2_SUPPORTED = True
        LAST_DAY_SUPPORTS_DATE_PART = False

        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,
            exp.DataType.Type.BINARY: "VARBYTE",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.TIMETZ: "TIME",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.VARBINARY: "VARBYTE",
        }

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.Concat: concat_to_dpipe_sql,
            exp.ConcatWs: concat_ws_to_dpipe_sql,
            exp.ApproxDistinct: lambda self,
            e: f"APPROXIMATE COUNT(DISTINCT {self.sql(e, 'this')})",
            exp.CurrentTimestamp: lambda self, e: (
                "SYSDATE" if e.args.get("transaction") else "GETDATE()"
            ),
            exp.DateAdd: date_delta_sql("DATEADD"),
            exp.DateDiff: date_delta_sql("DATEDIFF"),
            exp.DistKeyProperty: lambda self, e: self.func("DISTKEY", e.this),
            exp.DistStyleProperty: lambda self, e: self.naked_property(e),
            exp.FromBase: rename_func("STRTOL"),
            exp.GeneratedAsIdentityColumnConstraint: generatedasidentitycolumnconstraint_sql,
            exp.JSONExtract: json_extract_segments("JSON_EXTRACT_PATH_TEXT"),
            exp.JSONExtractScalar: json_extract_segments("JSON_EXTRACT_PATH_TEXT"),
            exp.GroupConcat: rename_func("LISTAGG"),
            exp.ParseJSON: rename_func("JSON_PARSE"),
            exp.Select: transforms.preprocess(
                [transforms.eliminate_distinct_on, transforms.eliminate_semi_and_anti_joins]
            ),
            exp.SortKeyProperty: lambda self,
            e: f"{'COMPOUND ' if e.args['compound'] else ''}SORTKEY({self.format_args(*e.this)})",
            exp.TableSample: no_tablesample_sql,
            exp.TsOrDsAdd: date_delta_sql("DATEADD"),
            exp.TsOrDsDiff: date_delta_sql("DATEDIFF"),
        }

        # Postgres maps exp.Pivot to no_pivot_sql, but Redshift support pivots
        TRANSFORMS.pop(exp.Pivot)

        # Redshift uses the POW | POWER (expr1, expr2) syntax instead of expr1 ^ expr2 (postgres)
        TRANSFORMS.pop(exp.Pow)

        # Redshift supports ANY_VALUE(..)
        TRANSFORMS.pop(exp.AnyValue)

        # Redshift supports LAST_DAY(..)
        TRANSFORMS.pop(exp.LastDay)

        RESERVED_KEYWORDS = {*Postgres.Generator.RESERVED_KEYWORDS, "snapshot", "type"}

        def with_properties(self, properties: exp.Properties) -> str:
            """Redshift doesn't have `WITH` as part of their with_properties so we remove it"""
            return self.properties(properties, prefix=" ", suffix="")

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            if expression.is_type(exp.DataType.Type.JSON):
                # Redshift doesn't support a JSON type, so casting to it is treated as a noop
                return self.sql(expression, "this")

            return super().cast_sql(expression, safe_prefix=safe_prefix)

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Redshift converts the `TEXT` data type to `VARCHAR(255)` by default when people more generally mean
            VARCHAR of max length which is `VARCHAR(max)` in Redshift. Therefore if we get a `TEXT` data type
            without precision we convert it to `VARCHAR(max)` and if it does have precision then we just convert
            `TEXT` to `VARCHAR`.
            """
            if expression.is_type("text"):
                expression.set("this", exp.DataType.Type.VARCHAR)
                precision = expression.args.get("expressions")

                if not precision:
                    expression.append("expressions", exp.var("MAX"))

            return super().datatype_sql(expression)
