import itertools

import sqlglot.expressions as exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.schema import ensure_schema
from sqlglot.optimizer.scope import traverse_scope


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

    # We'll use this when generating alias names
    sequence = itertools.count()

    for scope in traverse_scope(expression):
        _check_union_outputs(scope)
        _qualify_derived_tables(scope.ctes, scope, sequence)
        _qualify_derived_tables(scope.derived_tables, scope, sequence)
        _qualify_columns(scope, schema)
        _expand_stars(scope, schema)
        _qualify_outputs(scope)
        _check_unknown_tables(scope)

    return expression


def _check_union_outputs(scope):
    """Assert that the outputs of both sides of a UNION are the same"""
    if not isinstance(scope.expression, exp.Union):
        return
    left, right = scope.union
    if left.expression.named_selects != right.expression.named_selects:
        raise OptimizeError(
            f"UNION outputs not equal: {left.expression.named_selects} vs. {left.expression.named_selects}"
        )


def _qualify_derived_tables(derived_tables, scope, sequence):
    """Ensure all derived tables have aliases"""
    for derived_table in derived_tables:
        table_alias = derived_table.args.get("alias")

        if not table_alias:
            table_alias = exp.TableAlias()
            derived_table.set("alias", table_alias)

        alias = table_alias.args.get("this")
        if not alias:
            alias = exp.to_identifier(f"_q_{next(sequence)}")
            scope.rename_source(None, alias.name)
            table_alias.set("this", alias)

        # Remove any alias column list
        # (e.g. SELECT ... FROM (SELECT ...) AS foo(col1, col2)
        table_alias.args.pop("columns", None)


def _qualify_columns(scope, schema):
    """Disambiguate columns, ensuring each column specifies a source"""
    unambiguous_columns = None  # lazily loaded

    for column in scope.columns:
        column_table = column.text("table")
        column_name = column.text("this")

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

            column_table = unambiguous_columns.get(column_name)
            if not column_table and not scope.is_subquery:
                raise OptimizeError(f"Ambiguous column: {column_name}")
            column.set("table", exp.to_identifier(column_table))


def _expand_stars(scope, schema):
    """Expand stars to lists of column selections"""

    for expression in scope.selects:
        if isinstance(expression, exp.Star):
            tables = list(scope.selected_sources)
        elif isinstance(expression, exp.Column) and isinstance(
            expression.this, exp.Star
        ):
            tables = [expression.text("table")]
        else:
            continue

        new_columns = []

        for table in tables:
            if table not in scope.sources:
                raise OptimizeError(f"Unknown table: {table}")
            columns = _get_source_columns(table, scope.sources, schema)
            for column in columns:
                new_columns.append(
                    exp.Column(
                        this=exp.to_identifier(column), table=exp.to_identifier(table)
                    )
                )

        expression.replace(*new_columns)


def _qualify_outputs(scope):
    """Ensure all output columns are aliased"""

    for i, (selection, aliased_column) in enumerate(
        itertools.zip_longest(scope.selects, scope.outer_column_list)
    ):
        if isinstance(selection, exp.Column):
            selection_name = selection.text("this")
            new_selection = exp.alias_(selection.copy(), selection_name)
            selection.replace(new_selection)
            selection = new_selection
        elif not isinstance(selection, exp.Alias):
            selection_name = f"_col_{i}"
            new_selection = exp.alias_(selection.copy(), selection_name)
            selection.replace(new_selection)
            selection = new_selection

        if aliased_column:
            selection.set("alias", exp.to_identifier(aliased_column))


def _check_unknown_tables(scope):
    if scope.external_columns and not scope.is_correlated_subquery:
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

    for table, columns in source_columns[1:]:
        unique = _find_unique_columns(columns)
        ambiguous = set(unambiguous_columns).intersection(unique)
        for column in ambiguous:
            unambiguous_columns.pop(column)
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
