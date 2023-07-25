from __future__ import annotations

import typing as t

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import concat_to_dpipe_sql, rename_func
from sqlglot.dialects.postgres import Postgres
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _json_sql(self: Postgres.Generator, expression: exp.JSONExtract | exp.JSONExtractScalar) -> str:
    return f'{self.sql(expression, "this")}."{expression.expression.name}"'


class Redshift(Postgres):
    # https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    RESOLVES_IDENTIFIERS_AS_UPPERCASE = None

    TIME_FORMAT = "'YYYY-MM-DD HH:MI:SS'"
    TIME_MAPPING = {
        **Postgres.TIME_MAPPING,
        "MON": "%b",
        "HH": "%H",
    }

    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,
            "ADD_MONTHS": lambda args: exp.DateAdd(
                this=exp.TsOrDsToDate(this=seq_get(args, 0)),
                expression=seq_get(args, 1),
                unit=exp.var("month"),
            ),
            "DATEADD": lambda args: exp.DateAdd(
                this=exp.TsOrDsToDate(this=seq_get(args, 2)),
                expression=seq_get(args, 1),
                unit=seq_get(args, 0),
            ),
            "DATEDIFF": lambda args: exp.DateDiff(
                this=exp.TsOrDsToDate(this=seq_get(args, 2)),
                expression=exp.TsOrDsToDate(this=seq_get(args, 1)),
                unit=seq_get(args, 0),
            ),
            "STRTOL": exp.FromBase.from_arg_list,
        }

        def _parse_types(
            self, check_func: bool = False, schema: bool = False
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_types(check_func=check_func, schema=schema)

            if (
                isinstance(this, exp.DataType)
                and this.is_type("varchar")
                and this.expressions
                and this.expressions[0].this == exp.column("MAX")
            ):
                this.set("expressions", [exp.var("MAX")])

            return this

        def _parse_convert(self, strict: bool) -> t.Optional[exp.Expression]:
            to = self._parse_types()
            self._match(TokenType.COMMA)
            this = self._parse_bitwise()
            return self.expression(exp.TryCast, this=this, to=to)

    class Tokenizer(Postgres.Tokenizer):
        BIT_STRINGS = []
        HEX_STRINGS = []
        STRING_ESCAPES = ["\\"]

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "HLLSKETCH": TokenType.HLLSKETCH,
            "SUPER": TokenType.SUPER,
            "SYSDATE": TokenType.CURRENT_TIMESTAMP,
            "TIME": TokenType.TIMESTAMP,
            "TIMETZ": TokenType.TIMESTAMPTZ,
            "TOP": TokenType.TOP,
            "UNLOAD": TokenType.COMMAND,
            "VARBYTE": TokenType.VARBINARY,
        }

        # Redshift allows # to appear as a table identifier prefix
        SINGLE_TOKENS = Postgres.Tokenizer.SINGLE_TOKENS.copy()
        SINGLE_TOKENS.pop("#")

    class Generator(Postgres.Generator):
        LOCKING_READS_SUPPORTED = False
        RENAME_TABLE_WITH_DB = False
        QUERY_HINTS = False
        VALUES_AS_TABLE = False

        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,
            exp.DataType.Type.BINARY: "VARBYTE",
            exp.DataType.Type.VARBINARY: "VARBYTE",
            exp.DataType.Type.INT: "INTEGER",
        }

        PROPERTIES_LOCATION = {
            **Postgres.Generator.PROPERTIES_LOCATION,
            exp.LikeProperty: exp.Properties.Location.POST_WITH,
        }

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.Concat: concat_to_dpipe_sql,
            exp.CurrentTimestamp: lambda self, e: "SYSDATE",
            exp.DateAdd: lambda self, e: self.func(
                "DATEADD", exp.var(e.text("unit") or "day"), e.expression, e.this
            ),
            exp.DateDiff: lambda self, e: self.func(
                "DATEDIFF", exp.var(e.text("unit") or "day"), e.expression, e.this
            ),
            exp.DistKeyProperty: lambda self, e: f"DISTKEY({e.name})",
            exp.DistStyleProperty: lambda self, e: self.naked_property(e),
            exp.FromBase: rename_func("STRTOL"),
            exp.JSONExtract: _json_sql,
            exp.JSONExtractScalar: _json_sql,
            exp.SafeConcat: concat_to_dpipe_sql,
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
            exp.SortKeyProperty: lambda self, e: f"{'COMPOUND ' if e.args['compound'] else ''}SORTKEY({self.format_args(*e.this)})",
            exp.TsOrDsToDate: lambda self, e: self.sql(e.this),
        }

        # Postgres maps exp.Pivot to no_pivot_sql, but Redshift support pivots
        TRANSFORMS.pop(exp.Pivot)

        # Redshift uses the POW | POWER (expr1, expr2) syntax instead of expr1 ^ expr2 (postgres)
        TRANSFORMS.pop(exp.Pow)

        RESERVED_KEYWORDS = {*Postgres.Generator.RESERVED_KEYWORDS, "snapshot", "type"}

        def with_properties(self, properties: exp.Properties) -> str:
            """Redshift doesn't have `WITH` as part of their with_properties so we remove it"""
            return self.properties(properties, prefix=" ", suffix="")

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Redshift converts the `TEXT` data type to `VARCHAR(255)` by default when people more generally mean
            VARCHAR of max length which is `VARCHAR(max)` in Redshift. Therefore if we get a `TEXT` data type
            without precision we convert it to `VARCHAR(max)` and if it does have precision then we just convert
            `TEXT` to `VARCHAR`.
            """
            if expression.is_type("text"):
                expression = expression.copy()
                expression.set("this", exp.DataType.Type.VARCHAR)
                precision = expression.args.get("expressions")

                if not precision:
                    expression.append("expressions", exp.var("MAX"))

            return super().datatype_sql(expression)
