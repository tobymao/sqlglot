import itertools

import sqlglot.expressions as exp
from sqlglot import parse_one


def qualify_columns(expression, schema):
    """
    Rewrite sqlglot AST to have fully qualified columns.

    Example:
        >>> import sqlglot
        >>> schema = {"tbl": {"col": "INT"}}
        >>> expression = sqlglot.parse_one("SELECT col FROM tbl")
        >>> qualify_columns(expression, schema).sql()
        'SELECT "tbl"."col" AS "col" FROM tbl'

    Args:
        expression (sqlglot.Expression): expression to qualify
        schema (dict): Mapping of table names to all available columns
    Returns:
        sqlglot.Expression: qualified expression
    """
    expression = expression.copy()
    _qualify_statement(expression, schema, itertools.count(), {}, [])
    return expression


def _qualify_statement(
    expression, schema, sequence, parent_selectables, aliased_columns
):
    """
    Search SELECT or UNION for columns to qualify.

    Args:
        expression (exp.Select or exp.Union): expression to search
        schema (dict): Mapping of table names to all available columns.
        sequence (iterator): Auto-incrementing sequence.
        parent_selectables (dict): Mapping of name to selectable columns.
            This can include names set by CTEs or names that are available
            from the parent context (e.g. in the case of correlated subqueries).
        aliased_columns (list): List of column names.
            This should be set for derived tables or CTEs where the parent context
            defines alias column names, e.g. "SELECT * FROM (SELECT * FROM x) AS y(a, b)"
    Returns:
        list: output column names
    """
    if isinstance(expression, exp.Select):
        return _qualify_select(
            expression, schema, sequence, parent_selectables, aliased_columns
        )
    if isinstance(expression, exp.Union):
        left = _qualify_select(
            expression.this, schema, sequence, parent_selectables, aliased_columns
        )
        right = _qualify_statement(
            expression.args.get("expression"),
            schema,
            sequence,
            parent_selectables,
            aliased_columns,
        )
        if set(left) != set(right):
            raise RuntimeError("UNION columns not equal")
        return left

    raise RuntimeError("Unexpected statement type")


def _qualify_select(expression, schema, sequence, parent_selectables, aliased_columns):
    """
    Search SELECT for columns to qualify.

    Args:
        (Same as `_qualify_union`)
    Returns:
        (Same as `_qualify_union`)
    """
    # pylint: disable=too-many-locals
    ctes = []
    derived_tables = []  # SELECT * FROM (SELECT ...) <- derived table
    subqueries = []  # SELECT * FROM x WHERE a IN (SELECT ...) <- subquery
    tables = []
    columns = []
    selections = []  # SELECT x.a <- selection
    select_stars = []  # SELECT * <- select_star

    # Collect all the selections in this context
    for selection in expression.args.get("expressions", []):
        if isinstance(selection, exp.Star) or (
            isinstance(selection, exp.Column) and isinstance(selection.this, exp.Star)
        ):
            select_stars.append(selection)
        else:
            selections.append(selection)

    # Collect all the other relevant nodes in this context
    # Only traverse down to the next SELECT statements - we'll recurse into those later
    for node, parent, _ in expression.walk(stop_types=exp.Select):
        if node is expression:
            continue  # Skip this node itself - we only care about children
        if isinstance(node, exp.CTE):
            ctes.append(node)
        elif isinstance(node, (exp.Select, exp.Union)):
            if isinstance(parent, exp.Subquery):
                derived_tables.append(parent)
            else:
                subqueries.append(node)
        elif isinstance(node, exp.Table):
            tables.append(node)
        elif isinstance(node, exp.Column):
            columns.append(node)

    parent_selectables = _merge(
        parent_selectables,
        _qualify_derived_tables(ctes, schema, sequence, parent_selectables, chain=True),
    )
    selectables = _merge({}, _qualify_tables(tables, schema, parent_selectables))
    selectables = _merge(
        selectables,
        _qualify_derived_tables(derived_tables, schema, sequence, parent_selectables),
    )
    expansions = _expand_stars(select_stars, selectables)
    columns.extend(expansions)
    selections.extend(expansions)
    selectables = _merge(parent_selectables, selectables)
    _qualify_columns(columns, selectables)
    _qualify_subqueries(subqueries, schema, sequence, selectables)
    return _qualify_outputs(selections, aliased_columns)


def _qualify_derived_tables(
    derived_tables, schema, sequence, parent_selectables, chain=False
):
    """
    Derived tables are subqueries that are selectable in a surrounding context.

    For example:
        SELECT * FROM (SELECT ...) AS x  <- derived table
        WITH x AS (SELECT ...) <- derived table

    In both cases, x can be selected from.

    Some derived tables can have their selectable context chained (e.g. CTEs and lateral joins).
    That is, a CTE can refer to selectables in preceding CTEs.
    """
    selectables = {}
    for subquery in derived_tables:
        table_alias = subquery.args.get("alias")
        if not table_alias:
            table_alias = exp.TableAlias()
            subquery.set("alias", table_alias)

        alias = table_alias.args.get("this")
        if not alias:
            alias = exp.to_identifier(f"_q_{next(sequence)}")
            table_alias.set("this", alias)

        pushdown_aliased_columns = []
        aliased_columns = table_alias.args.get("columns", [])
        if aliased_columns:
            pushdown_aliased_columns = [c.text("this") for c in aliased_columns]
            table_alias.args.pop("columns")

        selectable_table = alias.text("this")

        subquery_outputs = _qualify_statement(
            expression=subquery.this,
            schema=schema,
            sequence=sequence,
            parent_selectables=_merge(parent_selectables, selectables)
            if chain
            else parent_selectables,
            aliased_columns=pushdown_aliased_columns,
        )

        selectables = _merge(selectables, {selectable_table: subquery_outputs})

    return selectables


def _qualify_tables(tables, schema, parent_selectables):
    selectables = {}
    for table in tables:
        table_name = table.text("this")

        if isinstance(table.parent, exp.Alias):
            selectable_table = table.parent.alias
        else:
            selectable_table = table_name

        if table_name in parent_selectables:
            selectable_columns = parent_selectables[table_name]
        else:
            selectable_columns = list(schema[table_name])

        selectables = _merge(selectables, {selectable_table: selectable_columns})
    return selectables


def _expand_stars(select_stars, selectables):
    result = []
    for star in select_stars:
        new_columns = []

        if isinstance(star, exp.Column):
            tables = [star.text("table")]
        else:
            tables = list(selectables)

        for table in tables:
            columns = selectables.get(table, [])
            for column in sorted(columns):
                new_columns.append(parse_one(f'"{table}"."{column}"'))
        star.replace(*new_columns)
        result.extend(new_columns)
    return result


def _qualify_columns(columns, selectables):
    unambiguous_columns = None  # lazily loaded

    for column in columns:
        column_table = column.text("table")

        if column_table and column_table not in selectables:
            raise RuntimeError(f"Unknown table reference: {column_table}")
        if not column_table:
            if unambiguous_columns is None:
                unambiguous_columns = _get_unambiguous_columns(selectables)

            column_table = unambiguous_columns.get(column.text("this"))
            if not column_table:
                raise RuntimeError(f"Ambiguous column: {column}")
            column.set("table", exp.to_identifier(column_table))

        column.args.get("table").set("quoted", True)
        column.args.get("this").set("quoted", True)


def _qualify_subqueries(subqueries, schema, sequence, selectables):
    """
    Derived tables are subqueries that are NOT selectable in a surrounding context.

    For example:
        SELECT * FROM x WHERE a IN (SELECT ...)

    In this case, the subquery cannot be selected from (as opposed to derived tables).
    Also unlike derived tables, these subqueries CAN reference selectables from the surrounding
    context (making a subquery a "correlated subquery").
    """
    for subquery in subqueries:
        _qualify_statement(subquery, schema, sequence, selectables, [])


def _qualify_outputs(selections, aliased_columns):
    outputs = []

    for i, (selection, aliased_column) in enumerate(
        itertools.zip_longest(selections, aliased_columns)
    ):
        if isinstance(selection, exp.Alias):
            selection_name = selection.text("alias")
        elif isinstance(selection, exp.Column):
            selection_name = selection.text("this")
            new_selection = exp.alias_(selection.copy(), selection_name)
            selection.replace(new_selection)
            selection = new_selection
        else:
            selection_name = f"_col_{i}"
            new_selection = exp.alias_(selection.copy(), selection_name)
            selection.replace(new_selection)
            selection = new_selection

        if aliased_column:
            selection_name = aliased_column
            selection.set("alias", exp.to_identifier(aliased_column))

        selection.args.get("alias").set("quoted", True)

        outputs.append(selection_name)

    return outputs


def _merge(*selectables):
    """
    Merge two "selectable" dictionaries.

    Raise an error if there are any duplicate selectable names.
    """
    result = {}
    for s in selectables:
        for selectable_table, selectable_columns in s.items():
            if selectable_table in selectables:
                raise RuntimeError(f"Duplicate name declared: {selectable_table}")
            result[selectable_table] = selectable_columns
    return result


def _get_unambiguous_columns(selectables):
    """
    Find all the unambiguous columns in selectables.

    Args:
        selectables (dict): Mapping of name to selectable columns
    Returns:
        dict: Mapping of column name to selectable name
    """
    if not selectables:
        return {}

    selectables = list(selectables.items())

    first_table, first_columns = selectables[0]
    unambiguous_columns = {col: first_table for col in _unique_columns(first_columns)}

    for table, columns in selectables[1:]:
        unique = _unique_columns(columns)
        ambiguous = set(unambiguous_columns).intersection(unique)
        for column in ambiguous:
            unambiguous_columns.pop(column)
        for column in unique.difference(ambiguous):
            unambiguous_columns[column] = table

    return unambiguous_columns


def _unique_columns(columns):
    """
    Find the unique columns in a list of columns.

    Example:
        >>> sorted(_unique_columns(["a", "b", "b", "c"]))
        ['a', 'c']

    This is necessary because duplicate column names are ambiguous.
    """
    counts = {}
    for column in columns:
        counts[column] = counts.get(column, 0) + 1
    return {column for column, count in counts.items() if count == 1}
