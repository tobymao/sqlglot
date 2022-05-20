import sqlglot.expressions as exp


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
        schema (dict): Mapping of table names to all available columns
    Returns:
        sqlglot.Expression: qualified expression
    """
    expression = expression.copy()
    _qualify_union(expression, schema, Sequence(), {})
    return expression


class Sequence:
    """
    Auto-incrementing sequence.

    Example:
        >>> seq = Sequence()
        >>> seq.next()
        0
        >>> seq.next()
        1

    This is useful for autogenerating unique alias names.
    """

    def __init__(self):
        self.i = -1

    def next(self):
        self.i += 1
        return self.i


def _qualify_union(expression, schema, sequence, parent_selectables):
    """
    Search UNION for columns to qualify.

    Args:
        expression (exp.Select or exp.Union): expression to search
        schema (dict): Mapping of table names to all available columns.
        sequence (Sequence): Auto-incrementing sequence.
        parent_selectables (dict): Mapping of name to selectable columns.
            This can include names set by CTEs or names that are available
            from the parent context (e.g. in the case of correlated subqueries).
    Returns:
        list: output column names
    """
    if isinstance(expression, exp.Select):
        return _qualify_select(expression, schema, sequence, parent_selectables)
    if isinstance(expression, exp.Union):
        left = _qualify_select(expression.this, schema, sequence, parent_selectables)
        right = _qualify_union(
            expression.args.get("expression"), schema, sequence, parent_selectables
        )
        if set(left) != set(right):
            raise RuntimeError("UNION columns not equal")
        return left

    raise RuntimeError("Unexpected statement type")


def _qualify_select(expression, schema, sequence, parent_selectables):
    """
    Search SELECT for columns to qualify.

    Args:
        (Same as `_qualify_union`)
    Returns:
        (Same as `_qualify_union`)
    """
    # pylint: disable=too-many-locals
    ctes = []
    derived_tables = []
    subqueries = []
    tables = []
    columns = []
    selections = []
    select_stars = []

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
    return _qualify_outputs(selections)


def _qualify_derived_tables(
    derived_tables, schema, sequence, parent_selectables, chain=False
):
    selectables = {}
    for subquery in derived_tables:
        subquery_outputs = _qualify_union(
            expression=subquery.this,
            schema=schema,
            sequence=sequence,
            parent_selectables=_merge(parent_selectables, selectables)
            if chain
            else parent_selectables,
        )

        table_alias = subquery.args.get("alias")
        if not table_alias:
            table_alias = exp.TableAlias()
            subquery.set("alias", table_alias)

        alias = table_alias.args.get("this")
        if not alias:
            alias = exp.to_identifier(f"_alias{sequence.next()}")
            table_alias.set("this", alias)

        alias_columns = table_alias.args.get("columns")
        if not alias_columns:
            alias_columns = [exp.to_identifier(o) for o in subquery_outputs]
            table_alias.set("columns", alias_columns)

        selectable_table = alias.text("this")

        selectable_columns = [c.text("this") for c in alias_columns]
        selectables = _merge(selectables, {selectable_table: selectable_columns})

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
                new_columns.append(
                    exp.Column(
                        this=exp.to_identifier(column), table=exp.to_identifier(table)
                    )
                )
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


def _qualify_subqueries(subqueries, schema, sequence, selectables):
    for subquery in subqueries:
        _qualify_union(subquery, schema, sequence, selectables)


def _qualify_outputs(selections):
    outputs = []

    for i, selection in enumerate(selections):
        if isinstance(selection, exp.Alias):
            selection_name = selection.text("alias")
        elif isinstance(selection, exp.Column):
            selection_name = selection.text("this")
            selection.replace(exp.alias_(selection.copy(), selection_name))
        else:
            selection_name = f"_col{i}"
            selection.replace(exp.alias_(selection.copy(), selection_name))

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
    unique = set()
    ambiguous = set()
    for column in columns:
        if column in ambiguous:
            continue
        if column in unique:
            unique.remove(column)
            ambiguous.add(column)
        else:
            unique.add(column)
    return unique
