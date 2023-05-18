from __future__ import annotations

import typing as t

from sqlglot import exp, transforms
from sqlglot.dialects.postgres import Postgres
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _json_sql(self, e) -> str:
    return f'{self.sql(e, "this")}."{e.expression.name}"'


class Redshift(Postgres):
    time_format = "'YYYY-MM-DD HH:MI:SS'"
    time_mapping = {
        **Postgres.time_mapping,  # type: ignore
        "MON": "%b",
        "HH": "%H",
    }

    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,  # type: ignore
            "DATEADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2),
                expression=seq_get(args, 1),
                unit=seq_get(args, 0),
            ),
            "DATEDIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 2),
                expression=seq_get(args, 1),
                unit=seq_get(args, 0),
            ),
            "NVL": exp.Coalesce.from_arg_list,
        }

        CONVERT_TYPE_FIRST = True

        def _parse_types(self, check_func: bool = False) -> t.Optional[exp.Expression]:
            this = super()._parse_types(check_func=check_func)

            if (
                isinstance(this, exp.DataType)
                and this.this == exp.DataType.Type.VARCHAR
                and this.expressions
                and this.expressions[0].this == exp.column("MAX")
            ):
                this.set("expressions", [exp.Var(this="MAX")])

            return this

    class Tokenizer(Postgres.Tokenizer):
        BIT_STRINGS = []
        HEX_STRINGS = []
        STRING_ESCAPES = ["\\"]

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,  # type: ignore
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

        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.BINARY: "VARBYTE",
            exp.DataType.Type.VARBINARY: "VARBYTE",
            exp.DataType.Type.INT: "INTEGER",
        }

        PROPERTIES_LOCATION = {
            **Postgres.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.LikeProperty: exp.Properties.Location.POST_WITH,
        }

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,  # type: ignore
            exp.CurrentTimestamp: lambda self, e: "SYSDATE",
            exp.DateAdd: lambda self, e: self.func(
                "DATEADD", exp.var(e.text("unit") or "day"), e.expression, e.this
            ),
            exp.DateDiff: lambda self, e: self.func(
                "DATEDIFF", exp.var(e.text("unit") or "day"), e.expression, e.this
            ),
            exp.DistKeyProperty: lambda self, e: f"DISTKEY({e.name})",
            exp.DistStyleProperty: lambda self, e: self.naked_property(e),
            exp.JSONExtract: _json_sql,
            exp.JSONExtractScalar: _json_sql,
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
            exp.SortKeyProperty: lambda self, e: f"{'COMPOUND ' if e.args['compound'] else ''}SORTKEY({self.format_args(*e.this)})",
        }

        # Postgres maps exp.Pivot to no_pivot_sql, but Redshift support pivots
        TRANSFORMS.pop(exp.Pivot)

        # Redshift uses the POW | POWER (expr1, expr2) syntax instead of expr1 ^ expr2 (postgres)
        TRANSFORMS.pop(exp.Pow)

        RESERVED_KEYWORDS = {*Postgres.Generator.RESERVED_KEYWORDS, "snapshot"}

        def values_sql(self, expression: exp.Values) -> str:
            """
            Converts `VALUES...` expression into a series of unions.

            Note: If you have a lot of unions then this will result in a large number of recursive statements to
            evaluate the expression. You may need to increase `sys.setrecursionlimit` to run and it can also be
            very slow.
            """

            # The VALUES clause is still valid in an `INSERT INTO ..` statement, for example
            if not expression.find_ancestor(exp.From, exp.Join):
                return super().values_sql(expression)

            column_names = expression.alias and expression.args["alias"].columns

            selects = []
            rows = [tuple_exp.expressions for tuple_exp in expression.expressions]

            for i, row in enumerate(rows):
                if i == 0 and column_names:
                    row = [
                        exp.alias_(value, column_name)
                        for value, column_name in zip(row, column_names)
                    ]

                selects.append(exp.Select(expressions=row))

            subquery_expression = selects[0]
            if len(selects) > 1:
                for select in selects[1:]:
                    subquery_expression = exp.union(subquery_expression, select, distinct=False)

            return self.subquery_sql(subquery_expression.subquery(expression.alias))

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
            if expression.this == exp.DataType.Type.TEXT:
                expression = expression.copy()
                expression.set("this", exp.DataType.Type.VARCHAR)
                precision = expression.args.get("expressions")

                if not precision:
                    expression.append("expressions", exp.Var(this="MAX"))

            return super().datatype_sql(expression)
