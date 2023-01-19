from __future__ import annotations

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import rename_func
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType


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
            "DECODE": exp.Matches.from_arg_list,
            "NVL": exp.Coalesce.from_arg_list,
        }

    class Tokenizer(Postgres.Tokenizer):
        ESCAPES = ["\\"]

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,  # type: ignore
            "COPY": TokenType.COMMAND,
            "ENCODE": TokenType.ENCODE,
            "GEOMETRY": TokenType.GEOMETRY,
            "GEOGRAPHY": TokenType.GEOGRAPHY,
            "HLLSKETCH": TokenType.HLLSKETCH,
            "SUPER": TokenType.SUPER,
            "TIME": TokenType.TIMESTAMP,
            "TIMETZ": TokenType.TIMESTAMPTZ,
            "UNLOAD": TokenType.COMMAND,
            "VARBYTE": TokenType.VARBINARY,
        }

    class Generator(Postgres.Generator):
        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.BINARY: "VARBYTE",
            exp.DataType.Type.VARBINARY: "VARBYTE",
            exp.DataType.Type.INT: "INTEGER",
        }

        ROOT_PROPERTIES = {
            exp.DistKeyProperty,
            exp.SortKeyProperty,
            exp.DistStyleProperty,
        }

        WITH_PROPERTIES = {
            exp.LikeProperty,
        }

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,  # type: ignore
            **transforms.ELIMINATE_DISTINCT_ON,  # type: ignore
            exp.DistKeyProperty: lambda self, e: f"DISTKEY({e.name})",
            exp.SortKeyProperty: lambda self, e: f"{'COMPOUND ' if e.args['compound'] else ''}SORTKEY({self.format_args(*e.this)})",
            exp.DistStyleProperty: lambda self, e: self.naked_property(e),
            exp.Matches: rename_func("DECODE"),
        }

        def values_sql(self, expression: exp.Values) -> str:
            """
            Converts `VALUES...` expression into a series of unions.

            Note: If you have a lot of unions then this will result in a large number of recursive statements to
            evaluate the expression. You may need to increase `sys.setrecursionlimit` to run and it can also be
            very slow.
            """
            if not isinstance(expression.unnest().parent, exp.From):
                return super().values_sql(expression)
            rows = [tuple_exp.expressions for tuple_exp in expression.expressions]
            selects = []
            for i, row in enumerate(rows):
                if i == 0:
                    row = [
                        exp.alias_(value, column_name)
                        for value, column_name in zip(row, expression.args["alias"].args["columns"])
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

        def renametable_sql(self, expression: exp.RenameTable) -> str:
            """Redshift only supports defining the table name itself (not the db) when renaming tables"""
            expression = expression.copy()
            target_table = expression.this
            for arg in target_table.args:
                if arg != "this":
                    target_table.set(arg, None)
            this = self.sql(expression, "this")
            return f"RENAME TO {this}"

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
                    expression.append("expressions", exp.to_identifier("max"))
            return super().datatype_sql(expression)
