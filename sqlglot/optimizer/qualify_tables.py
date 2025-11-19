from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.helper import name_sequence
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.scope import Scope, traverse_scope

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def qualify_tables(
    expression: E,
    db: t.Optional[str | exp.Identifier] = None,
    catalog: t.Optional[str | exp.Identifier] = None,
    on_qualify: t.Optional[t.Callable[[exp.Expression], None]] = None,
    dialect: DialectType = None,
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
        on_qualify: Callback after a table has been qualified.
        dialect: The dialect to parse catalog and schema into.

    Returns:
        The qualified expression.
    """
    dialect = Dialect.get_or_raise(dialect)

    alias_sequence = name_sequence("_q_")

    def next_alias_name() -> str:
        return normalize_identifiers(alias_sequence(), dialect=dialect).name

    if db := db or None:
        db = exp.parse_identifier(db, dialect=dialect)
        db.meta["is_table"] = True
        db = normalize_identifiers(db, dialect=dialect)
    if catalog := catalog or None:
        catalog = exp.parse_identifier(catalog, dialect=dialect)
        catalog.meta["is_table"] = True
        catalog = normalize_identifiers(catalog, dialect=dialect)

    def _qualify(table: exp.Table) -> None:
        if isinstance(table.this, exp.Identifier):
            if db and not table.args.get("db"):
                table.set("db", db.copy())
            if catalog and not table.args.get("catalog") and table.args.get("db"):
                table.set("catalog", catalog.copy())

    if (db or catalog) and not isinstance(expression, exp.Query):
        with_ = expression.args.get("with_") or exp.With()
        cte_names = {cte.alias_or_name for cte in with_.expressions}

        for node in expression.walk(prune=lambda n: isinstance(n, exp.Query)):
            if isinstance(node, exp.Table) and node.name not in cte_names:
                _qualify(node)

    for scope in traverse_scope(expression):
        for derived_table in scope.derived_tables:
            unnested = derived_table.unnest()
            if isinstance(unnested, exp.Table):
                joins = unnested.args.get("joins")
                unnested.set("joins", None)
                derived_table.this.replace(exp.select("*").from_(unnested.copy(), copy=False))
                derived_table.this.set("joins", joins)

            if not derived_table.args.get("alias"):
                alias = next_alias_name()
                derived_table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
                scope.rename_source(None, alias)

            pivots = derived_table.args.get("pivots")
            if pivots and not pivots[0].alias:
                pivots[0].set("alias", exp.TableAlias(this=exp.to_identifier(next_alias_name())))

        table_aliases = {}

        for name, source in scope.sources.items():
            if isinstance(source, exp.Table):
                pivots = source.args.get("pivots")
                if not source.alias:
                    # Don't add the pivot's alias to the pivoted table, use the table's name instead
                    if pivots and pivots[0].alias == name:
                        name = source.name

                    # Mutates the source by attaching an alias to it
                    normalized_alias = normalize_identifiers(
                        name or source.name or alias_sequence(), dialect=dialect
                    )
                    exp.alias_(source, normalized_alias, copy=False, table=True)

                table_aliases[".".join(p.name for p in source.parts)] = exp.to_identifier(
                    source.alias
                )

                if pivots:
                    pivot = pivots[0]
                    if not pivot.alias:
                        pivot_alias = normalize_identifiers(
                            source.alias if pivot.unpivot else alias_sequence(),
                            dialect=dialect,
                        )
                        pivot.set("alias", exp.TableAlias(this=exp.to_identifier(pivot_alias)))

                    # This case corresponds to a pivoted CTE, we don't want to qualify that
                    if isinstance(scope.sources.get(source.alias_or_name), Scope):
                        continue

                _qualify(source)

                if on_qualify:
                    on_qualify(source)
            elif isinstance(source, Scope) and source.is_udtf:
                udtf = source.expression
                table_alias = udtf.args.get("alias") or exp.TableAlias(
                    this=exp.to_identifier(next_alias_name())
                )
                udtf.set("alias", table_alias)

                if not table_alias.name:
                    table_alias.set("this", exp.to_identifier(next_alias_name()))
                if isinstance(udtf, exp.Values) and not table_alias.columns:
                    column_aliases = [
                        normalize_identifiers(i, dialect=dialect)
                        for i in dialect.generate_values_aliases(udtf)
                    ]
                    table_alias.set("columns", column_aliases)

        for table in scope.tables:
            if not table.alias and isinstance(table.parent, (exp.From, exp.Join)):
                exp.alias_(table, table.name, copy=False, table=True)

        for column in scope.columns:
            if column.db:
                table_alias = table_aliases.get(".".join(p.name for p in column.parts[0:-1]))

                if table_alias:
                    for p in exp.COLUMN_PARTS[1:]:
                        column.set(p, None)

                    column.set("table", table_alias.copy())

    return expression
