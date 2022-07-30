import itertools

from sqlglot import alias, exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.schema import ensure_schema
from sqlglot.optimizer.scope import traverse_scope


SKIP_QUALIFY = (exp.Unnest, exp.Lateral)


def qualify_columns(expression, schema):
    """
    Rewrite sqlglot AST to have fully qualified columns.

    Example:
        >>> import sqlglot
        >>> schema = {"tbl": {"col": "INT"}}
        >>> expression = sqlglot.parse_one("SELECT col FROM tbl")
        >>> qualify_columns(expression, schema).sql()
        'SELECT tbl.col AS col FROM tbl'

    Args:
        expression (sqlglot.Expression): expression to qualify
        schema (dict|sqlglot.optimizer.Schema): Database schema
    Returns:
        sqlglot.Expression: qualified expression
    """
    schema = ensure_schema(schema)

    for scope in traverse_scope(expression):
        _pop_table_column_aliases(scope.ctes)
        _pop_table_column_aliases(scope.derived_tables)
        _expand_using(scope, schema)
        _qualify_columns(scope, schema)
        if not isinstance(scope.expression, SKIP_QUALIFY):
            _expand_stars(scope, schema)
            _qualify_outputs(scope)
        _check_unknown_tables(scope)

    return expression


def _pop_table_column_aliases(derived_tables):
    """
    Remove table column aliases.

    (e.g. SELECT ... FROM (SELECT ...) AS foo(col1, col2)
    """
    for derived_table in derived_tables:
        if isinstance(derived_table, SKIP_QUALIFY):
            continue
        table_alias = derived_table.args.get("alias")
        if table_alias:
            table_alias.args.pop("columns", None)


def _expand_using(scope, schema):
    joins = list(scope.expression.find_all(exp.Join))
    names = {join.this.alias for join in joins}
    ordered = [key for key in scope.selected_sources if key not in names]

    # Mapping of automatically joined column names to source names
    column_tables = {}

    for join in joins:
        using = join.args.get("using")

        if not using:
            continue

        join_table = join.this.alias_or_name

        columns = {}

        for k in scope.selected_sources:
            if k in ordered:
                for column in _get_source_columns(k, scope.sources, schema):
                    if column not in columns:
                        columns[column] = k

        ordered.append(join_table)
        join_columns = _get_source_columns(join_table, scope.sources, schema)
        conditions = []

        for identifier in using:
            identifier = identifier.name
            table = columns.get(identifier)

            if not table or identifier not in join_columns:
                raise OptimizeError(f"Cannot automatically join: {identifier}")

            conditions.append(
                exp.condition(
                    exp.EQ(
                        this=exp.column(identifier, table=table),
                        expression=exp.column(identifier, table=join_table),
                    )
                )
            )

            tables = column_tables.setdefault(identifier, [])
            if table not in tables:
                tables.append(table)
            if join_table not in tables:
                tables.append(join_table)

        join.args.pop("using")
        join.set("on", exp.and_(*conditions))

    if column_tables:
        for column in scope.columns:
            if not column.table and column.name in column_tables:
                tables = column_tables[column.name]
                coalesce = [exp.column(column.name, table=table) for table in tables]
                replacement = exp.Coalesce(this=coalesce[0], expressions=coalesce[1:])

                # Ensure selects keep their output name
                if isinstance(column.parent, exp.Select):
                    replacement = exp.alias_(replacement, alias=column.name)

                scope.replace(column, replacement)


def _qualify_columns(scope, schema):
    """Disambiguate columns, ensuring each column specifies a source"""
    # lazily loaded
    unambiguous_columns = None
    all_columns = None

    for column in scope.columns:
        column_table = column.table
        column_name = column.name

        if (
            column_table
            and column_table in scope.sources
            and column_name
            not in _get_source_columns(column_table, scope.sources, schema)
        ):
            raise OptimizeError(f"Unknown column: {column_name}")

        if not column_table:
            if unambiguous_columns is None:
                source_columns = {
                    k: _get_source_columns(k, scope.sources, schema)
                    for k in scope.selected_sources
                }

                unambiguous_columns = _get_unambiguous_columns(source_columns)
                all_columns = set(
                    column for columns in source_columns.values() for column in columns
                )

            column_table = unambiguous_columns.get(column_name)

            if not scope.is_subquery and not scope.is_unnest:
                if column_name not in all_columns:
                    raise OptimizeError(f"Unknown column: {column_name}")

                if column_table is None:
                    raise OptimizeError(f"Ambiguous column: {column_name}")

            # column_table can be a '' because bigquery unnest has no table alias
            if column_table:
                column.set("table", exp.to_identifier(column_table))


def _expand_stars(scope, schema):
    """Expand stars to lists of column selections"""

    new_selections = []
    except_columns = {}
    replace_columns = {}

    for expression in scope.selects:
        if isinstance(expression, exp.Star):
            tables = list(scope.selected_sources)
            _add_except_columns(expression, tables, except_columns)
            _add_replace_columns(expression, tables, replace_columns)
        elif isinstance(expression, exp.Column) and isinstance(
            expression.this, exp.Star
        ):
            tables = [expression.table]
            _add_except_columns(expression.this, tables, except_columns)
            _add_replace_columns(expression.this, tables, replace_columns)
        else:
            new_selections.append(expression)
            continue

        for table in tables:
            if table not in scope.sources:
                raise OptimizeError(f"Unknown table: {table}")
            columns = _get_source_columns(table, scope.sources, schema)
            table_id = id(table)
            for name in columns:
                if name not in except_columns.get(table_id, set()):
                    alias_ = replace_columns.get(table_id, {}).get(name, name)
                    column = exp.column(name, table)
                    new_selections.append(
                        alias(column, alias_) if alias_ != name else column
                    )

    scope.expression.set("expressions", new_selections)


def _add_except_columns(expression, tables, except_columns):
    except_ = expression.args.get("except")

    if not except_:
        return

    columns = {e.name for e in except_}

    for table in tables:
        except_columns[id(table)] = columns


def _add_replace_columns(expression, tables, replace_columns):
    replace = expression.args.get("replace")

    if not replace:
        return

    columns = {e.this.name: e.alias for e in replace}

    for table in tables:
        replace_columns[id(table)] = columns


def _qualify_outputs(scope):
    """Ensure all output columns are aliased"""
    new_selections = []

    for i, (selection, aliased_column) in enumerate(
        itertools.zip_longest(scope.selects, scope.outer_column_list)
    ):
        if isinstance(selection, exp.Column):
            # convoluted setter because a simple selection.replace(alias) would require a copy
            alias_ = alias(exp.column(""), alias=selection.name)
            alias_.set("this", selection)
            selection = alias_
        elif not isinstance(selection, exp.Alias):
            alias_ = alias(exp.column(""), f"_col_{i}")
            alias_.set("this", selection)
            selection = alias_

        if aliased_column:
            selection.set("alias", exp.to_identifier(aliased_column))

        new_selections.append(selection)

    scope.expression.set("expressions", new_selections)


def _check_unknown_tables(scope):
    if (
        scope.external_columns
        and not scope.is_unnest
        and not scope.is_correlated_subquery
    ):
        raise OptimizeError(f"Unknown table: {scope.external_columns[0].text('table')}")


def _get_unambiguous_columns(source_columns):
    """
    Find all the unambiguous columns in sources.

    Args:
        source_columns (dict): Mapping of names to source columns
    Returns:
        dict: Mapping of column name to source name
    """
    if not source_columns:
        return {}

    source_columns = list(source_columns.items())

    first_table, first_columns = source_columns[0]
    unambiguous_columns = {
        col: first_table for col in _find_unique_columns(first_columns)
    }
    all_columns = set(unambiguous_columns)

    for table, columns in source_columns[1:]:
        unique = _find_unique_columns(columns)
        ambiguous = set(all_columns).intersection(unique)
        all_columns.update(columns)
        for column in ambiguous:
            unambiguous_columns.pop(column, None)
        for column in unique.difference(ambiguous):
            unambiguous_columns[column] = table

    return unambiguous_columns


def _find_unique_columns(columns):
    """
    Find the unique columns in a list of columns.

    Example:
        >>> sorted(_find_unique_columns(["a", "b", "b", "c"]))
        ['a', 'c']

    This is necessary because duplicate column names are ambiguous.
    """
    counts = {}
    for column in columns:
        counts[column] = counts.get(column, 0) + 1
    return {column for column, count in counts.items() if count == 1}


def _get_source_columns(name, sources, schema):
    """Resolve the source columns for a given source `name`"""
    if name not in sources:
        raise OptimizeError(f"Unknown table: {name}")

    source = sources[name]

    # If referencing a table, return the columns from the schema
    if isinstance(source, exp.Table):
        try:
            return schema.column_names(source)
        except Exception as e:
            raise OptimizeError(str(e)) from e

    # Otherwise, if referencing another scope, return that scope's named selects
    return source.expression.named_selects
