from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.helper import name_sequence, seq_get
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
    canonicalize: bool = False,
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
        canonicalize: Whether to use canonical aliases (_0, _1, ...) for all sources
            instead of preserving table names. Defaults to False.

    Returns:
        The qualified expression.
    """
    dialect = Dialect.get_or_raise(dialect)

    alias_sequence = name_sequence("_" if canonicalize else "_q_")

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

    canonical_aliases: t.Dict[str, str] = {}

    def _set_alias(
        expression: exp.Expression,
        target_alias: t.Optional[str] = None,
        scope: t.Optional[Scope] = None,
        normalize: bool = False,
    ) -> None:
        alias = expression.args.get("alias") or exp.TableAlias()

        if canonicalize:
            new_alias_name = next_alias_name()
            canonical_aliases[alias.name or target_alias or ""] = new_alias_name
        elif not alias.name:
            new_alias_name = target_alias or next_alias_name()
            if normalize:
                new_alias_name = normalize_identifiers(new_alias_name, dialect=dialect).name
        else:
            return

        alias.set("this", exp.to_identifier(new_alias_name))
        expression.set("alias", alias)

        if scope:
            scope.rename_source(None, new_alias_name)

    for scope in traverse_scope(expression):
        for query in scope.subqueries:
            subquery = query.parent
            if isinstance(subquery, exp.Subquery):
                subquery.unwrap().replace(subquery)

        for derived_table in scope.derived_tables:
            unnested = derived_table.unnest()
            if isinstance(unnested, exp.Table):
                joins = unnested.args.get("joins")
                unnested.set("joins", None)
                derived_table.this.replace(exp.select("*").from_(unnested.copy(), copy=False))
                derived_table.this.set("joins", joins)

            _set_alias(derived_table, scope=scope)
            if pivot := seq_get(derived_table.args.get("pivots") or [], 0):
                _set_alias(pivot)

        table_aliases = {}

        for name, source in scope.sources.items():
            if isinstance(source, exp.Table):
                # When the name is empty, it means that we have a non-table source, e.g. a pivoted Cte
                is_real_table_source = bool(name)

                if pivot := seq_get(source.args.get("pivots") or [], 0):
                    name = source.name

                _set_alias(source, target_alias=name or source.name or None, normalize=True)

                source_fqn = ".".join(p.name for p in source.parts)
                table_aliases[source_fqn] = exp.to_identifier(source.alias)

                if pivot:
                    target_alias = source.alias if pivot.unpivot else None
                    _set_alias(pivot, target_alias=target_alias, normalize=True)

                    # This case corresponds to a pivoted CTE, we don't want to qualify that
                    if isinstance(scope.sources.get(source.alias_or_name), Scope):
                        continue

                if is_real_table_source:
                    _qualify(source)

                    if on_qualify:
                        on_qualify(source)
            elif isinstance(source, Scope) and source.is_udtf:
                _set_alias(udtf := source.expression)

                table_alias = udtf.args["alias"]

                if isinstance(udtf, exp.Values) and not table_alias.columns:
                    column_aliases = [
                        normalize_identifiers(i, dialect=dialect)
                        for i in dialect.generate_values_aliases(udtf)
                    ]
                    table_alias.set("columns", column_aliases)

        for table in scope.tables:
            if not table.alias and isinstance(table.parent, (exp.From, exp.Join)):
                _set_alias(table, target_alias=table.name)

        for column in scope.columns:
            table = column.table

            if column.db:
                table_alias = table_aliases.get(".".join(p.name for p in column.parts[0:-1]))

                if table_alias:
                    for p in exp.COLUMN_PARTS[1:]:
                        column.set(p, None)

                    column.set("table", table_alias.copy())
            elif (
                canonical_aliases
                and table
                and (canonical_table := canonical_aliases.get(table, "")) != column.table
            ):
                # Amend existing aliases, e.g. t.c -> _0.c if t is aliased to _0
                column.set("table", exp.to_identifier(canonical_table))
                pass

    return expression
