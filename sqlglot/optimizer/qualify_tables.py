from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.helper import name_sequence, seq_get, ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.scope import Scope, get_source_alias, traverse_scope

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def qualify_tables_in_scope(
    scope: Scope,
    db: t.Optional[exp.Identifier],
    catalog: t.Optional[exp.Identifier],
    dialect: Dialect,
    next_alias_name: t.Callable[[], str],
    on_qualify: t.Optional[t.Callable[[exp.Expression], None]] = None,
    canonicalize_table_aliases: bool = False,
):
    def _set_alias(
        expression: exp.Expression,
        scope: Scope,
        canonical_aliases: t.Dict[str, str],
        old_name: int | str = "",
        target_alias: t.Optional[str | int] = None,
        normalize: bool = False,
        columns: t.Optional[t.List[str | exp.Identifier]] = None,
        rename_source: bool = True,
    ) -> None:
        if isinstance(target_alias, int):
            target_alias = None
        alias = expression.args.get("alias") or exp.TableAlias()

        if canonicalize_table_aliases:
            new_alias_name = next_alias_name()
            canonical_aliases[alias.name or target_alias or ""] = new_alias_name
        elif not alias.name:
            new_alias_name = target_alias or next_alias_name()
            if normalize and target_alias:
                new_alias_name = normalize_identifiers(new_alias_name, dialect=dialect).name
        else:
            return

        # Auto-generated aliases (_1, _2, ...) are quoted in order to be valid across all dialects
        quoted = True if canonicalize_table_aliases or not target_alias else None

        alias.set("this", exp.to_identifier(new_alias_name, quoted=quoted))

        if columns:
            alias.set("columns", [exp.to_identifier(c) for c in columns])

        expression.set("alias", alias)

        if rename_source:
            scope.rename_source(
                old_name if old_name else get_source_alias(expression), new_alias_name
            )

    local_columns = scope.local_columns
    canonical_aliases: t.Dict[str, str] = {}

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

        _set_alias(derived_table, scope, canonical_aliases, old_name=get_source_alias(unnested))
        if pivot := seq_get(derived_table.args.get("pivots") or [], 0):
            _set_alias(pivot, scope, canonical_aliases)

    table_aliases = {}

    for name, source in tuple(scope.sources.items()):
        if isinstance(source, exp.Table):
            # When the name is empty, it means that we have a non-table source, e.g. a pivoted cte
            is_real_table_source = bool(name)

            if pivot := seq_get(source.args.get("pivots") or [], 0):
                name = source.name

            table_this = source.this
            table_alias = source.args.get("alias")
            function_columns: t.List[t.Union[str, exp.Identifier]] = []
            if isinstance(table_this, exp.Func):
                if not table_alias:
                    function_columns = ensure_list(
                        dialect.DEFAULT_FUNCTIONS_COLUMN_NAMES.get(type(table_this))
                    )
                elif columns := table_alias.columns:
                    function_columns = columns
                elif type(table_this) in dialect.DEFAULT_FUNCTIONS_COLUMN_NAMES:
                    function_columns = ensure_list(source.alias_or_name)
                    source.set("alias", None)
                    name = None

            _set_alias(
                source,
                scope,
                canonical_aliases,
                old_name=name,
                target_alias=name or source.name or None,
                normalize=True,
                columns=function_columns,
            )

            source_fqn = ".".join(p.name for p in source.parts)
            table_aliases[source_fqn] = source.args["alias"].this.copy()

            if pivot:
                target_alias = source.alias if pivot.unpivot else None
                _set_alias(
                    pivot,
                    scope,
                    canonical_aliases,
                    old_name=name,
                    target_alias=target_alias,
                    normalize=True,
                    rename_source=False,
                )

                # This case corresponds to a pivoted CTE, we don't want to qualify that
                if isinstance(scope.sources.get(source.alias_or_name), Scope):
                    continue

            if is_real_table_source:
                _qualify(source, db, catalog)

                if on_qualify:
                    on_qualify(source)
        elif isinstance(source, Scope) and source.is_udtf:
            _set_alias(udtf := source.expression, scope, canonical_aliases, old_name=name)

            table_alias = udtf.args["alias"]

            if isinstance(udtf, exp.Values) and not table_alias.columns:
                column_aliases = [
                    normalize_identifiers(i, dialect=dialect)
                    for i in dialect.generate_values_aliases(udtf)
                ]
                table_alias.set("columns", column_aliases)

    for table in scope.tables:
        if not table.alias and isinstance(table.parent, (exp.From, exp.Join)):
            _set_alias(table, scope, canonical_aliases, target_alias=table.name)

    for column in local_columns:
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
            column.set("table", exp.to_identifier(canonical_table, quoted=True))


def normalize_db_and_catalog(
    db: t.Optional[str | exp.Identifier] = None,
    catalog: t.Optional[str | exp.Identifier] = None,
    dialect: DialectType = None,
) -> t.Tuple[t.Optional[exp.Identifier], t.Optional[exp.Identifier]]:
    if db_id := db or None:
        db_id = exp.parse_identifier(db_id, dialect=dialect)
        db_id.meta["is_table"] = True
        db_id = normalize_identifiers(db_id, dialect=dialect)
    if catalog_id := catalog or None:
        catalog_id = exp.parse_identifier(catalog_id, dialect=dialect)
        catalog_id.meta["is_table"] = True
        catalog_id = normalize_identifiers(catalog_id, dialect=dialect)
    return db_id, catalog_id


def qualify_tables(
    expression: E,
    db: t.Optional[str | exp.Identifier] = None,
    catalog: t.Optional[str | exp.Identifier] = None,
    on_qualify: t.Optional[t.Callable[[exp.Expression], None]] = None,
    dialect: DialectType = None,
    canonicalize_table_aliases: bool = False,
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
        canonicalize_table_aliases: Whether to use canonical aliases (_0, _1, ...) for all sources
            instead of preserving table names. Defaults to False.

    Returns:
        The qualified expression.
    """
    dialect = Dialect.get_or_raise(dialect)
    next_alias_name = name_sequence("_")
    db_id, catalog_id = normalize_db_and_catalog(db, catalog, dialect)

    if (db or catalog) and not isinstance(expression, exp.Query):
        with_ = expression.args.get("with_") or exp.With()
        cte_names = {cte.alias_or_name for cte in with_.expressions}

        for node in expression.walk(prune=lambda n: isinstance(n, exp.Query)):
            if isinstance(node, exp.Table) and node.name not in cte_names:
                _qualify(node, db_id, catalog_id)

    for scope in traverse_scope(expression):
        qualify_tables_in_scope(
            scope,
            db=db_id,
            catalog=catalog_id,
            dialect=dialect,
            on_qualify=on_qualify,
            next_alias_name=next_alias_name,
            canonicalize_table_aliases=canonicalize_table_aliases,
        )

    return expression


def _qualify(
    table: exp.Table, db: t.Optional[exp.Identifier], catalog: t.Optional[exp.Identifier]
) -> None:
    if isinstance(table.this, exp.Identifier):
        if db and not table.args.get("db"):
            table.set("db", db.copy())
        if catalog and not table.args.get("catalog") and table.args.get("db"):
            table.set("catalog", catalog.copy())
