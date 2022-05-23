import dataclasses
import itertools
import typing

import sqlglot.expressions as exp
from sqlglot import parse_one
from sqlglot.errors import OptimizeError


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
    _qualify_statement(expression, Context(schema=schema))
    return expression


@dataclasses.dataclass
class Context:
    """
    Scoped context for a SELECT statement.

    This is a convenience for passing information between CTEs, subqueries, etc.

    Attributes:
        schema: Mapping of table names to all available columns
        sequence: Auto-incrementing sequence
        selectables: Mapping of name to selectable columns
        aliased_columns: List of column names.
            This should be set for derived tables or CTEs where the parent context
            defines column names for an alias, e.g. "SELECT * FROM (SELECT * FROM x) AS y(a, b)"
    """

    schema: dict
    sequence: typing.Iterable = dataclasses.field(default_factory=itertools.count)
    selectables: dict = dataclasses.field(default_factory=dict)
    aliased_columns: list = dataclasses.field(default_factory=list)

    def add_selectables(self, *selectables, check_unique=True):
        return Context(
            schema=self.schema,
            sequence=self.sequence,
            selectables=_merge(
                self.selectables, *selectables, check_unique=check_unique
            ),
            aliased_columns=self.aliased_columns,
        )

    def branch(self, aliased_columns=None):
        return Context(
            schema=self.schema,
            sequence=self.sequence,
            selectables=self.selectables,
            aliased_columns=aliased_columns or [],
        )


def _qualify_statement(expression, context):
    """
    Search SELECT or UNION for columns to qualify.

    Args:
        expression (exp.Select or exp.Union): expression to search
        context (Context): current context
    Returns:
        list: output column names
    """
    if isinstance(expression, exp.Select):
        return _qualify_select(expression, context)
    if isinstance(expression, exp.Union):
        left = _qualify_select(expression.this, context)
        right = _qualify_statement(expression.args.get("expression"), context)
        if set(left) != set(right):
            raise OptimizeError("UNION columns not equal")
        return left

    raise OptimizeError("Unexpected statement type")


def _qualify_select(expression, context):
    """
    Search SELECT for columns to qualify.

    Args:
        expression (exp.Select): expression to search
        context (Context): current context
    Returns:
        list: output column names
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

    context = context.add_selectables(
        _qualify_derived_tables(ctes, context, chain=True)
    )

    selectables = _qualify_tables(tables, context)

    # Don't update context.selectables yet, as derived tables can't depend on each other.
    selectables = _merge(selectables, _qualify_derived_tables(derived_tables, context))

    # Again, don't update the context yet.
    # We don't want to expand CTEs or outer query selectables that haven't been
    # explicitly selected from in this context.
    expansions = _expand_stars(select_stars, selectables)
    columns.extend(expansions)
    selections.extend(expansions)

    # OK - now we can update the context.
    context = context.add_selectables(selectables, check_unique=False)

    _qualify_columns(columns, context.selectables)
    _qualify_subqueries(subqueries, context)
    return _qualify_outputs(selections, context.aliased_columns)


def _qualify_derived_tables(derived_tables, context, chain=False):
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
            alias = exp.to_identifier(f"_q_{next(context.sequence)}")
            table_alias.set("this", alias)

        # Remove any alias columns (e.g. "a, b" in this: "SELECT * FROM (...) AS x(a, b)").
        # If these are set, push them down to the subquery to overwrite the selection aliases.
        pushdown_aliased_columns = []
        aliased_columns = table_alias.args.get("columns", [])
        if aliased_columns:
            pushdown_aliased_columns = [c.text("this") for c in aliased_columns]
            table_alias.args.pop("columns")

        subquery_outputs = _qualify_statement(
            expression=subquery.this,
            context=context.add_selectables(selectables if chain else {}).branch(
                pushdown_aliased_columns
            ),
        )

        selectable_table = alias.text("this")
        selectables = _merge(selectables, {selectable_table: subquery_outputs})

    return selectables


def _qualify_tables(tables, context):
    """Extract selectables from tables referenced in FROM and JOIN clauses"""

    selectables = {}
    for table in tables:
        table_name = table.text("this")

        if isinstance(table.parent, exp.Alias):
            selectable_table = table.parent.alias
        else:
            selectable_table = table_name

        if table_name in context.selectables:
            # This is a reference to a parent selectable (e.g. a CTE), not an actual table.
            selectable_columns = context.selectables[table_name]
        elif selectable_table in context.selectables:
            raise OptimizeError(f"Duplicate table name: {selectable_table}")
        elif table_name not in context.schema:
            raise OptimizeError(f"Unknown table: {table_name}")
        else:
            selectable_columns = list(context.schema[table_name])

        selectables = _merge(selectables, {selectable_table: selectable_columns})
    return selectables


def _expand_stars(select_stars, selectables):
    """Expand stars to lists of column selections"""

    result = []
    for star in select_stars:
        new_columns = []

        if isinstance(star, exp.Column):
            tables = [star.text("table")]
        else:
            tables = list(selectables)

        for table in tables:
            columns = selectables.get(table, [])
            for column in columns:
                new_columns.append(parse_one(f'"{table}"."{column}"'))
        star.replace(*new_columns)
        result.extend(new_columns)
    return result


def _qualify_columns(columns, selectables):
    """Given the map of selectable tables to columns, fully qualify columns"""

    unambiguous_columns = None  # lazily loaded

    for column in columns:
        column_table = column.text("table")

        if column_table and column_table not in selectables:
            raise OptimizeError(f"Unknown table reference: {column_table}")
        if not column_table:
            if unambiguous_columns is None:
                unambiguous_columns = _get_unambiguous_columns(selectables)

            column_table = unambiguous_columns.get(column.text("this"))
            if not column_table:
                raise OptimizeError(f"Ambiguous column: {column}")
            column.set("table", exp.to_identifier(column_table))

        column.args.get("table").set("quoted", True)
        column.args.get("this").set("quoted", True)


def _qualify_subqueries(subqueries, context):
    """
    Derived tables are subqueries that are NOT selectable in a surrounding context.

    For example:
        SELECT * FROM x WHERE a IN (SELECT ...)

    In this case, the subquery cannot be selected from (as opposed to derived tables).
    Also unlike derived tables, these subqueries CAN reference selectables from the surrounding
    context (making a subquery a "correlated subquery").
    """
    for subquery in subqueries:
        _qualify_statement(subquery, context.branch())


def _qualify_outputs(selections, aliased_columns):
    """
    Ensure all output columns have proper aliases.

    `aliased_columns` are passed down from an outer query.

    For example, we want to convert this:
        SELECT * FROM (SELECT a AS b FROM x) AS y(c)
    To this:
        SELECT * FROM (SELECT a AS c FROM x) AS y
    """

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


def _merge(*selectables, check_unique=True):
    """
    Merge two "selectable" dictionaries.

    Raise an error if there are any duplicate selectable names.
    """
    result = {}
    for s in selectables:
        for selectable_table, selectable_columns in s.items():
            if check_unique and selectable_table in result:
                raise OptimizeError(f"Duplicate name declared: {selectable_table}")
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
