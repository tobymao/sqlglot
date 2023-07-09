import itertools
import typing as t

from sqlglot import alias, exp
from sqlglot._typing import E
from sqlglot.helper import csv_reader, name_sequence
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import Schema


def qualify_tables(
    expression: E,
    db: t.Optional[str] = None,
    catalog: t.Optional[str] = None,
    schema: t.Optional[Schema] = None,
) -> E:
    """
    Rewrite sqlglot AST to have fully qualified tables. Join constructs such as
    (t1 JOIN t2) AS t will be expanded into (SELECT * FROM t1 AS t1, t2 AS t2) AS t.

    Examples:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT 1 FROM tbl")
        >>> qualify_tables(expression, db="db").sql()
        'SELECT 1 FROM db.tbl AS tbl'
        >>>
        >>> expression = sqlglot.parse_one("SELECT 1 FROM (t1 JOIN t2) AS t")
        >>> qualify_tables(expression).sql()
        'SELECT 1 FROM (SELECT * FROM t1 AS t1, t2 AS t2) AS t'

    Args:
        expression: Expression to qualify
        db: Database name
        catalog: Catalog name
        schema: A schema to populate

    Returns:
        The qualified expression.
    """
    next_alias_name = name_sequence("_q_")

    for scope in traverse_scope(expression):
        for derived_table in itertools.chain(scope.ctes, scope.derived_tables):
            if isinstance(derived_table, exp.Subquery):
                unnested = derived_table.unnest()
                if isinstance(unnested, exp.Table):
                    joins = unnested.args.pop("joins", None)
                    derived_table.this.replace(exp.select("*").from_(unnested.copy(), copy=False))
                    derived_table.this.set("joins", joins)

            if not derived_table.args.get("alias"):
                alias_ = next_alias_name()
                derived_table.set("alias", exp.TableAlias(this=exp.to_identifier(alias_)))
                scope.rename_source(None, alias_)

            pivots = derived_table.args.get("pivots")
            if pivots and not pivots[0].alias:
                pivots[0].set("alias", exp.TableAlias(this=exp.to_identifier(next_alias_name())))

        for name, source in scope.sources.items():
            if isinstance(source, exp.Table):
                if isinstance(source.this, exp.Identifier):
                    if not source.args.get("db"):
                        source.set("db", exp.to_identifier(db))
                    if not source.args.get("catalog"):
                        source.set("catalog", exp.to_identifier(catalog))

                if not source.alias:
                    # Mutates the source by attaching an alias to it
                    alias(source, name or source.name or next_alias_name(), copy=False, table=True)

                pivots = source.args.get("pivots")
                if pivots and not pivots[0].alias:
                    pivots[0].set(
                        "alias", exp.TableAlias(this=exp.to_identifier(next_alias_name()))
                    )

                if schema and isinstance(source.this, exp.ReadCSV):
                    with csv_reader(source.this) as reader:
                        header = next(reader)
                        columns = next(reader)
                        schema.add_table(
                            source, {k: type(v).__name__ for k, v in zip(header, columns)}
                        )
            elif isinstance(source, Scope) and source.is_udtf:
                udtf = source.expression
                table_alias = udtf.args.get("alias") or exp.TableAlias(
                    this=exp.to_identifier(next_alias_name())
                )
                udtf.set("alias", table_alias)

                if not table_alias.name:
                    table_alias.set("this", exp.to_identifier(next_alias_name()))
                if isinstance(udtf, exp.Values) and not table_alias.columns:
                    for i, e in enumerate(udtf.expressions[0].expressions):
                        table_alias.append("columns", exp.to_identifier(f"_col_{i}"))

    return expression
