"""sqlglot expressions builders."""

from __future__ import annotations

import re
import typing as t

from sqlglot._typing import E
from sqlglot.helper import seq_get, ensure_collection, split_num_words
from sqlglot.errors import ParseError, TokenError
from sqlglot.expressions.core import (
    Alias,
    Anonymous,
    Boolean,
    Column,
    Condition,
    EQ,
    Expr,
    Func,
    Identifier,
    Literal,
    Null,
    Placeholder,
    TABLE_PARTS,
    Var,
    logger,
    ExpOrStr,
    SAFE_IDENTIFIER_RE,
    maybe_parse,
    maybe_copy,
    to_identifier,
    convert,
    alias_,
    column,
)
from sqlglot.expressions.datatypes import DataType, DType, Interval, DATA_TYPE
from sqlglot.expressions.query import (
    CTE,
    From,
    Query,
    Schema,
    Select,
    Table,
    TableAlias,
    Tuple,
    Values,
    Where,
    With,
)
from sqlglot.expressions.ddl import Alter, AlterRename, RenameColumn
from sqlglot.expressions.dml import Delete, Insert, Merge, Update, When, Whens
from sqlglot.expressions.functions import Case, Cast
from sqlglot.expressions.array import Array

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


def select(*expressions: ExpOrStr, dialect: DialectType = None, **opts) -> Select:
    """
    Initializes a syntax tree from one or multiple SELECT expressions.

    Example:
        >>> select("col1", "col2").from_("tbl").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressions: the SQL code string to parse as the expressions of a
            SELECT statement. If an Expr instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().select(*expressions, dialect=dialect, **opts)


def from_(expression: ExpOrStr, dialect: DialectType = None, **opts) -> Select:
    """
    Initializes a syntax tree from a FROM expression.

    Example:
        >>> from_("tbl").select("col1", "col2").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expression: the SQL code string to parse as the FROM expressions of a
            SELECT statement. If an Expr instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().from_(expression, dialect=dialect, **opts)


def update(
    table: str | Table,
    properties: t.Optional[dict] = None,
    where: t.Optional[ExpOrStr] = None,
    from_: t.Optional[ExpOrStr] = None,
    with_: t.Optional[t.Dict[str, ExpOrStr]] = None,
    dialect: DialectType = None,
    **opts,
) -> Update:
    """
    Creates an update statement.

    Example:
        >>> update("my_table", {"x": 1, "y": "2", "z": None}, from_="baz_cte", where="baz_cte.id > 1 and my_table.id = baz_cte.id", with_={"baz_cte": "SELECT id FROM foo"}).sql()
        "WITH baz_cte AS (SELECT id FROM foo) UPDATE my_table SET x = 1, y = '2', z = NULL FROM baz_cte WHERE baz_cte.id > 1 AND my_table.id = baz_cte.id"

    Args:
        properties: dictionary of properties to SET which are
            auto converted to sql objects eg None -> NULL
        where: sql conditional parsed into a WHERE statement
        from_: sql statement parsed into a FROM statement
        with_: dictionary of CTE aliases / select statements to include in a WITH clause.
        dialect: the dialect used to parse the input expressions.
        **opts: other options to use to parse the input expressions.

    Returns:
        Update: the syntax tree for the UPDATE statement.
    """
    update_expr = Update(this=maybe_parse(table, into=Table, dialect=dialect))
    if properties:
        update_expr.set(
            "expressions",
            [
                EQ(this=maybe_parse(k, dialect=dialect, **opts), expression=convert(v))
                for k, v in properties.items()
            ],
        )
    if from_:
        update_expr.set(
            "from_",
            maybe_parse(from_, into=From, dialect=dialect, prefix="FROM", **opts),
        )
    if isinstance(where, Condition):
        where = Where(this=where)
    if where:
        update_expr.set(
            "where",
            maybe_parse(where, into=Where, dialect=dialect, prefix="WHERE", **opts),
        )
    if with_:
        cte_list = [
            alias_(CTE(this=maybe_parse(qry, dialect=dialect, **opts)), alias, table=True)
            for alias, qry in with_.items()
        ]
        update_expr.set(
            "with_",
            With(expressions=cte_list),
        )
    return update_expr


def delete(
    table: ExpOrStr,
    where: t.Optional[ExpOrStr] = None,
    returning: t.Optional[ExpOrStr] = None,
    dialect: DialectType = None,
    **opts,
) -> Delete:
    """
    Builds a delete statement.

    Example:
        >>> delete("my_table", where="id > 1").sql()
        'DELETE FROM my_table WHERE id > 1'

    Args:
        where: sql conditional parsed into a WHERE statement
        returning: sql conditional parsed into a RETURNING statement
        dialect: the dialect used to parse the input expressions.
        **opts: other options to use to parse the input expressions.

    Returns:
        Delete: the syntax tree for the DELETE statement.
    """
    delete_expr = Delete().delete(table, dialect=dialect, copy=False, **opts)
    if where:
        delete_expr = delete_expr.where(where, dialect=dialect, copy=False, **opts)
    if returning:
        delete_expr = delete_expr.returning(returning, dialect=dialect, copy=False, **opts)
    return delete_expr


def insert(
    expression: ExpOrStr,
    into: ExpOrStr,
    columns: t.Optional[t.Sequence[str | Identifier]] = None,
    overwrite: t.Optional[bool] = None,
    returning: t.Optional[ExpOrStr] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Insert:
    """
    Builds an INSERT statement.

    Example:
        >>> insert("VALUES (1, 2, 3)", "tbl").sql()
        'INSERT INTO tbl VALUES (1, 2, 3)'

    Args:
        expression: the sql string or expression of the INSERT statement
        into: the tbl to insert data to.
        columns: optionally the table's column names.
        overwrite: whether to INSERT OVERWRITE or not.
        returning: sql conditional parsed into a RETURNING statement
        dialect: the dialect used to parse the input expressions.
        copy: whether to copy the expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Insert: the syntax tree for the INSERT statement.
    """
    expr = maybe_parse(expression, dialect=dialect, copy=copy, **opts)
    this: Table | Schema = maybe_parse(into, into=Table, dialect=dialect, copy=copy, **opts)

    if columns:
        this = Schema(this=this, expressions=[to_identifier(c, copy=copy) for c in columns])

    insert = Insert(this=this, expression=expr, overwrite=overwrite)

    if returning:
        insert = insert.returning(returning, dialect=dialect, copy=False, **opts)

    return insert


def merge(
    *when_exprs: ExpOrStr,
    into: ExpOrStr,
    using: ExpOrStr,
    on: ExpOrStr,
    returning: t.Optional[ExpOrStr] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Merge:
    """
    Builds a MERGE statement.

    Example:
        >>> merge("WHEN MATCHED THEN UPDATE SET col1 = source_table.col1",
        ...       "WHEN NOT MATCHED THEN INSERT (col1) VALUES (source_table.col1)",
        ...       into="my_table",
        ...       using="source_table",
        ...       on="my_table.id = source_table.id").sql()
        'MERGE INTO my_table USING source_table ON my_table.id = source_table.id WHEN MATCHED THEN UPDATE SET col1 = source_table.col1 WHEN NOT MATCHED THEN INSERT (col1) VALUES (source_table.col1)'

    Args:
        *when_exprs: The WHEN clauses specifying actions for matched and unmatched rows.
        into: The target table to merge data into.
        using: The source table to merge data from.
        on: The join condition for the merge.
        returning: The columns to return from the merge.
        dialect: The dialect used to parse the input expressions.
        copy: Whether to copy the expression.
        **opts: Other options to use to parse the input expressions.

    Returns:
        Merge: The syntax tree for the MERGE statement.
    """
    expressions: t.List[Expr] = []
    for when_expr in when_exprs:
        expression = maybe_parse(when_expr, dialect=dialect, copy=copy, into=Whens, **opts)
        expressions.extend([expression] if isinstance(expression, When) else expression.expressions)

    merge = Merge(
        this=maybe_parse(into, dialect=dialect, copy=copy, **opts),
        using=maybe_parse(using, dialect=dialect, copy=copy, **opts),
        on=maybe_parse(on, dialect=dialect, copy=copy, **opts),
        whens=Whens(expressions=expressions),
    )
    if returning:
        merge = merge.returning(returning, dialect=dialect, copy=False, **opts)

    if isinstance(using_clause := merge.args.get("using"), Alias):
        using_clause.replace(alias_(using_clause.this, using_clause.args["alias"], table=True))

    return merge


def parse_identifier(name: str | Identifier, dialect: DialectType = None) -> Identifier:
    """
    Parses a given string into an identifier.

    Args:
        name: The name to parse into an identifier.
        dialect: The dialect to parse against.

    Returns:
        The identifier ast node.
    """
    try:
        expression = maybe_parse(name, dialect=dialect, into=Identifier)
    except (ParseError, TokenError):
        expression = to_identifier(name)

    return expression


INTERVAL_STRING_RE = re.compile(r"\s*(-?[0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\s*")


INTERVAL_DAY_TIME_RE = re.compile(
    r"\s*-?\s*\d+(?:\.\d+)?\s+(?:-?(?:\d+:)?\d+:\d+(?:\.\d+)?|-?(?:\d+:){1,2}|:)\s*"
)


def to_interval(interval: str | Expr) -> Interval:
    """Builds an interval expression from a string like '1 day' or '5 months'."""
    if isinstance(interval, Literal):
        if not interval.is_string:
            raise ValueError("Invalid interval string.")

        interval = interval.this

    interval = maybe_parse(f"INTERVAL {interval}")
    assert isinstance(interval, Interval)
    return interval


def to_table(
    sql_path: str | Table, dialect: DialectType = None, copy: bool = True, **kwargs
) -> Table:
    """
    Create a table expression from a `[catalog].[schema].[table]` sql path. Catalog and schema are optional.
    If a table is passed in then that table is returned.

    Args:
        sql_path: a `[catalog].[schema].[table]` string.
        dialect: the source dialect according to which the table name will be parsed.
        copy: Whether to copy a table if it is passed in.
        kwargs: the kwargs to instantiate the resulting `Table` expression with.

    Returns:
        A table expression.
    """
    if isinstance(sql_path, Table):
        return maybe_copy(sql_path, copy=copy)

    try:
        table = maybe_parse(sql_path, into=Table, dialect=dialect)
    except ParseError:
        catalog, db, this = split_num_words(sql_path, ".", 3)

        if not this:
            raise

        table = table_(this, db=db, catalog=catalog)

    for k, v in kwargs.items():
        table.set(k, v)

    return table


def to_column(
    sql_path: str | Column,
    quoted: t.Optional[bool] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **kwargs,
) -> Column:
    """
    Create a column from a `[table].[column]` sql path. Table is optional.
    If a column is passed in then that column is returned.

    Args:
        sql_path: a `[table].[column]` string.
        quoted: Whether or not to force quote identifiers.
        dialect: the source dialect according to which the column name will be parsed.
        copy: Whether to copy a column if it is passed in.
        kwargs: the kwargs to instantiate the resulting `Column` expression with.

    Returns:
        A column expression.
    """
    if isinstance(sql_path, Column):
        return maybe_copy(sql_path, copy=copy)

    try:
        col = maybe_parse(sql_path, into=Column, dialect=dialect)
    except ParseError:
        return column(*reversed(sql_path.split(".")), quoted=quoted, **kwargs)

    for k, v in kwargs.items():
        col.set(k, v)

    if quoted:
        for i in col.find_all(Identifier):
            i.set("quoted", True)

    return col


def subquery(
    expression: ExpOrStr,
    alias: t.Optional[Identifier | str] = None,
    dialect: DialectType = None,
    **opts,
) -> Select:
    """
    Build a subquery expression that's selected from.

    Example:
        >>> subquery('select x from tbl', 'bar').select('x').sql()
        'SELECT x FROM (SELECT x FROM tbl) AS bar'

    Args:
        expression: the SQL code strings to parse.
            If an Expr instance is passed, this is used as-is.
        alias: the alias name to use.
        dialect: the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        A new Select instance with the subquery expression included.
    """

    expression = maybe_parse(expression, dialect=dialect, **opts).subquery(alias, **opts)
    return Select().from_(expression, dialect=dialect, **opts)


def cast(
    expression: ExpOrStr, to: DATA_TYPE, copy: bool = True, dialect: DialectType = None, **opts
) -> Cast:
    """Cast an expression to a data type.

    Example:
        >>> cast('x + 1', 'int').sql()
        'CAST(x + 1 AS INT)'

    Args:
        expression: The expression to cast.
        to: The datatype to cast to.
        copy: Whether to copy the supplied expressions.
        dialect: The target dialect. This is used to prevent a re-cast in the following scenario:
            - The expression to be cast is already a exp.Cast expression
            - The existing cast is to a type that is logically equivalent to new type

            For example, if :expression='CAST(x as DATETIME)' and :to=Type.TIMESTAMP,
            but in the target dialect DATETIME is mapped to TIMESTAMP, then we will NOT return `CAST(x (as DATETIME) as TIMESTAMP)`
            and instead just return the original expression `CAST(x as DATETIME)`.

            This is to prevent it being output as a double cast `CAST(x (as TIMESTAMP) as TIMESTAMP)` once the DATETIME -> TIMESTAMP
            mapping is applied in the target dialect generator.

    Returns:
        The new Cast instance.
    """
    expr = maybe_parse(expression, copy=copy, dialect=dialect, **opts)
    data_type = DataType.build(to, copy=copy, dialect=dialect, **opts)

    # dont re-cast if the expression is already a cast to the correct type
    if isinstance(expr, Cast):
        from sqlglot.dialects.dialect import Dialect

        target_dialect = Dialect.get_or_raise(dialect)
        type_mapping = target_dialect.generator_class.TYPE_MAPPING

        existing_cast_type: DType = expr.to.this
        new_cast_type: DType = data_type.this
        types_are_equivalent = type_mapping.get(
            existing_cast_type, existing_cast_type.value
        ) == type_mapping.get(new_cast_type, new_cast_type.value)

        if expr.is_type(data_type) or types_are_equivalent:
            return expr

    expr = Cast(this=expr, to=data_type)
    expr.type = data_type

    return expr


def table_(
    table: Identifier | str,
    db: t.Optional[Identifier | str] = None,
    catalog: t.Optional[Identifier | str] = None,
    quoted: t.Optional[bool] = None,
    alias: t.Optional[Identifier | str] = None,
) -> Table:
    """Build a Table.

    Args:
        table: Table name.
        db: Database name.
        catalog: Catalog name.
        quote: Whether to force quotes on the table's identifiers.
        alias: Table's alias.

    Returns:
        The new Table instance.
    """
    return Table(
        this=to_identifier(table, quoted=quoted) if table else None,
        db=to_identifier(db, quoted=quoted) if db else None,
        catalog=to_identifier(catalog, quoted=quoted) if catalog else None,
        alias=TableAlias(this=to_identifier(alias)) if alias else None,
    )


def values(
    values: t.Iterable[t.Tuple[t.Any, ...]],
    alias: t.Optional[str] = None,
    columns: t.Optional[t.Iterable[str] | t.Dict[str, DataType]] = None,
) -> Values:
    """Build VALUES statement.

    Example:
        >>> values([(1, '2')]).sql()
        "VALUES (1, '2')"

    Args:
        values: values statements that will be converted to SQL
        alias: optional alias
        columns: Optional list of ordered column names or ordered dictionary of column names to types.
         If either are provided then an alias is also required.

    Returns:
        Values: the Values expression object
    """
    if columns and not alias:
        raise ValueError("Alias is required when providing columns")

    return Values(
        expressions=[convert(tup) for tup in values],
        alias=(
            TableAlias(this=to_identifier(alias), columns=[to_identifier(x) for x in columns])
            if columns
            else (TableAlias(this=to_identifier(alias)) if alias else None)
        ),
    )


def var(name: t.Optional[ExpOrStr]) -> Var:
    """Build a SQL variable.

    Example:
        >>> repr(var('x'))
        'Var(this=x)'

        >>> repr(var(column('x', table='y')))
        'Var(this=x)'

    Args:
        name: The name of the var or an expression who's name will become the var.

    Returns:
        The new variable node.
    """
    if not name:
        raise ValueError("Cannot convert empty name into var.")

    if isinstance(name, Expr):
        name = name.name
    return Var(this=name)


def rename_table(
    old_name: str | Table,
    new_name: str | Table,
    dialect: DialectType = None,
) -> Alter:
    """Build ALTER TABLE... RENAME... expression

    Args:
        old_name: The old name of the table
        new_name: The new name of the table
        dialect: The dialect to parse the table.

    Returns:
        Alter table expression
    """
    old_table = to_table(old_name, dialect=dialect)
    new_table = to_table(new_name, dialect=dialect)
    return Alter(
        this=old_table,
        kind="TABLE",
        actions=[
            AlterRename(this=new_table),
        ],
    )


def rename_column(
    table_name: str | Table,
    old_column_name: str | Column,
    new_column_name: str | Column,
    exists: t.Optional[bool] = None,
    dialect: DialectType = None,
) -> Alter:
    """Build ALTER TABLE... RENAME COLUMN... expression

    Args:
        table_name: Name of the table
        old_column: The old name of the column
        new_column: The new name of the column
        exists: Whether to add the `IF EXISTS` clause
        dialect: The dialect to parse the table/column.

    Returns:
        Alter table expression
    """
    table = to_table(table_name, dialect=dialect)
    old_column = to_column(old_column_name, dialect=dialect)
    new_column = to_column(new_column_name, dialect=dialect)
    return Alter(
        this=table,
        kind="TABLE",
        actions=[
            RenameColumn(this=old_column, to=new_column, exists=exists),
        ],
    )


def replace_children(expression: Expr, fun: t.Callable, *args, **kwargs) -> None:
    """
    Replace children of an expression with the result of a lambda fun(child) -> exp.
    """
    for k, v in tuple(expression.args.items()):
        is_list_arg = type(v) is list

        child_nodes = v if is_list_arg else [v]
        new_child_nodes = []

        for cn in child_nodes:
            if isinstance(cn, Expr):
                for child_node in ensure_collection(fun(cn, *args, **kwargs)):
                    new_child_nodes.append(child_node)
            else:
                new_child_nodes.append(cn)

        expression.set(k, new_child_nodes if is_list_arg else seq_get(new_child_nodes, 0))


def replace_tree(
    expression: Expr,
    fun: t.Callable,
    prune: t.Optional[t.Callable[[Expr], bool]] = None,
) -> Expr:
    """
    Replace an entire tree with the result of function calls on each node.

    This will be traversed in reverse dfs, so leaves first.
    If new nodes are created as a result of function calls, they will also be traversed.
    """
    stack = list(expression.dfs(prune=prune))

    while stack:
        node = stack.pop()
        new_node = fun(node)

        if new_node is not node:
            node.replace(new_node)

            if isinstance(new_node, Expr):
                stack.append(new_node)

    return new_node


def find_tables(expression: Expr) -> t.Set[Table]:
    """
    Find all tables referenced in a query.

    Args:
        expressions: The query to find the tables in.

    Returns:
        A set of all the tables.
    """
    from sqlglot.optimizer.scope import traverse_scope

    return {
        table
        for scope in traverse_scope(expression)
        for table in scope.tables
        if table.name and table.name not in scope.cte_sources
    }


def column_table_names(expression: Expr, exclude: str = "") -> t.Set[str]:
    """
    Return all table names referenced through columns in an expression.

    Example:
        >>> import sqlglot
        >>> sorted(column_table_names(sqlglot.parse_one("a.b AND c.d AND c.e")))
        ['a', 'c']

    Args:
        expression: expression to find table names.
        exclude: a table name to exclude

    Returns:
        A list of unique names.
    """
    return {
        table
        for table in (column.table for column in expression.find_all(Column))
        if table and table != exclude
    }


def table_name(table: Table | str, dialect: DialectType = None, identify: bool = False) -> str:
    """Get the full name of a table as a string.

    Args:
        table: Table expression node or string.
        dialect: The dialect to generate the table name for.
        identify: Determines when an identifier should be quoted. Possible values are:
            False (default): Never quote, except in cases where it's mandatory by the dialect.
            True: Always quote.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> table_name(parse_one("select * from a.b.c").find(exp.Table))
        'a.b.c'

    Returns:
        The table name.
    """

    expr = maybe_parse(table, into=Table, dialect=dialect)

    if not expr:
        raise ValueError(f"Cannot parse {table}")

    return ".".join(
        (
            part.sql(dialect=dialect, identify=True, copy=False, comments=False)
            if identify or not SAFE_IDENTIFIER_RE.match(part.name)
            else part.name
        )
        for part in expr.parts
    )


def normalize_table_name(table: str | Table, dialect: DialectType = None, copy: bool = True) -> str:
    """Returns a case normalized table name without quotes.

    Args:
        table: the table to normalize
        dialect: the dialect to use for normalization rules
        copy: whether to copy the expression.

    Examples:
        >>> normalize_table_name("`A-B`.c", dialect="bigquery")
        'A-B.c'
    """
    from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

    return ".".join(
        p.name
        for p in normalize_identifiers(
            to_table(table, dialect=dialect, copy=copy), dialect=dialect
        ).parts
    )


def replace_tables(
    expression: E, mapping: t.Dict[str, str], dialect: DialectType = None, copy: bool = True
) -> E:
    """Replace all tables in expression according to the mapping.

    Args:
        expression: expression node to be transformed and replaced.
        mapping: mapping of table names.
        dialect: the dialect of the mapping table
        copy: whether to copy the expression.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> replace_tables(parse_one("select * from a.b"), {"a.b": "c"}).sql()
        'SELECT * FROM c /* a.b */'

    Returns:
        The mapped expression.
    """

    mapping = {normalize_table_name(k, dialect=dialect): v for k, v in mapping.items()}

    def _replace_tables(node: Expr) -> Expr:
        if isinstance(node, Table) and node.meta.get("replace") is not False:
            original = normalize_table_name(node, dialect=dialect)
            new_name = mapping.get(original)

            if new_name:
                table = to_table(
                    new_name,
                    **{k: v for k, v in node.args.items() if k not in TABLE_PARTS},
                    dialect=dialect,
                )
                table.add_comments([original])
                return table
        return node

    return expression.transform(_replace_tables, copy=copy)  # type: ignore


def replace_placeholders(expression: Expr, *args, **kwargs) -> Expr:
    """Replace placeholders in an expression.

    Args:
        expression: expression node to be transformed and replaced.
        args: positional names that will substitute unnamed placeholders in the given order.
        kwargs: keyword arguments that will substitute named placeholders.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> replace_placeholders(
        ...     parse_one("select * from :tbl where ? = ?"),
        ...     exp.to_identifier("str_col"), "b", tbl=exp.to_identifier("foo")
        ... ).sql()
        "SELECT * FROM foo WHERE str_col = 'b'"

    Returns:
        The mapped expression.
    """

    def _replace_placeholders(node: Expr, args, **kwargs) -> Expr:
        if isinstance(node, Placeholder):
            if node.this:
                new_name = kwargs.get(node.this)
                if new_name is not None:
                    return convert(new_name)
            else:
                try:
                    return convert(next(args))
                except StopIteration:
                    pass
        return node

    return expression.transform(_replace_placeholders, iter(args), **kwargs)


def expand(
    expression: Expr,
    sources: t.Dict[str, Query | t.Callable[[], Query]],
    dialect: DialectType = None,
    copy: bool = True,
) -> Expr:
    """Transforms an expression by expanding all referenced sources into subqueries.

    Examples:
        >>> from sqlglot import parse_one
        >>> expand(parse_one("select * from x AS z"), {"x": parse_one("select * from y")}).sql()
        'SELECT * FROM (SELECT * FROM y) AS z /* source: x */'

        >>> expand(parse_one("select * from x AS z"), {"x": parse_one("select * from y"), "y": parse_one("select * from z")}).sql()
        'SELECT * FROM (SELECT * FROM (SELECT * FROM z) AS y /* source: y */) AS z /* source: x */'

    Args:
        expression: The expression to expand.
        sources: A dict of name to query or a callable that provides a query on demand.
        dialect: The dialect of the sources dict or the callable.
        copy: Whether to copy the expression during transformation. Defaults to True.

    Returns:
        The transformed expression.
    """
    normalized_sources = {normalize_table_name(k, dialect=dialect): v for k, v in sources.items()}

    def _expand(node: Expr):
        if isinstance(node, Table):
            name = normalize_table_name(node, dialect=dialect)
            source = normalized_sources.get(name)

            if source:
                # Create a subquery with the same alias (or table name if no alias)
                parsed_source = source() if callable(source) else source
                subquery = parsed_source.subquery(node.alias or name)
                subquery.comments = [f"source: {name}"]

                # Continue expanding within the subquery
                return subquery.transform(_expand, copy=False)

        return node

    return expression.transform(_expand, copy=copy)


def func(name: str, *args, copy: bool = True, dialect: DialectType = None, **kwargs) -> Func:
    """
    Returns a Func expression.

    Examples:
        >>> func("abs", 5).sql()
        'ABS(5)'

        >>> func("cast", this=5, to=DataType.build("DOUBLE")).sql()
        'CAST(5 AS DOUBLE)'

    Args:
        name: the name of the function to build.
        args: the args used to instantiate the function of interest.
        copy: whether to copy the argument expressions.
        dialect: the source dialect.
        kwargs: the kwargs used to instantiate the function of interest.

    Note:
        The arguments `args` and `kwargs` are mutually exclusive.

    Returns:
        An instance of the function of interest, or an anonymous function, if `name` doesn't
        correspond to an existing `sqlglot.expressions.Func` class.
    """
    if args and kwargs:
        raise ValueError("Can't use both args and kwargs to instantiate a function.")

    from sqlglot.dialects.dialect import Dialect

    dialect = Dialect.get_or_raise(dialect)

    converted: t.List[Expr] = [maybe_parse(arg, dialect=dialect, copy=copy) for arg in args]
    kwargs = {key: maybe_parse(value, dialect=dialect, copy=copy) for key, value in kwargs.items()}

    constructor = dialect.parser_class.FUNCTIONS.get(name.upper())
    if constructor:
        if converted:
            if "dialect" in constructor.__code__.co_varnames:
                function = constructor(converted, dialect=dialect)
            else:
                function = constructor(converted)
        elif constructor.__name__ == "from_arg_list":
            function = constructor.__self__(**kwargs)  # type: ignore
        else:
            from sqlglot.expressions import FUNCTION_BY_NAME as _FUNCTION_BY_NAME

            constructor = _FUNCTION_BY_NAME.get(name.upper())
            if constructor:
                function = constructor(**kwargs)
            else:
                raise ValueError(
                    f"Unable to convert '{name}' into a Func. Either manually construct "
                    "the Func expression of interest or parse the function call."
                )
    else:
        kwargs = kwargs or {"expressions": converted}
        function = Anonymous(this=name, **kwargs)

    for error_message in function.error_messages(converted):
        raise ValueError(error_message)

    return function


def case(
    expression: t.Optional[ExpOrStr] = None,
    **opts,
) -> Case:
    """
    Initialize a CASE statement.

    Example:
        case().when("a = 1", "foo").else_("bar")

    Args:
        expression: Optionally, the input expression (not all dialects support this)
        **opts: Extra keyword arguments for parsing `expression`
    """
    if expression is not None:
        this = maybe_parse(expression, **opts)
    else:
        this = None
    return Case(this=this, ifs=[])


def array(
    *expressions: ExpOrStr, copy: bool = True, dialect: DialectType = None, **kwargs
) -> Array:
    """
    Returns an array.

    Examples:
        >>> array(1, 'x').sql()
        'ARRAY(1, x)'

    Args:
        expressions: the expressions to add to the array.
        copy: whether to copy the argument expressions.
        dialect: the source dialect.
        kwargs: the kwargs used to instantiate the function of interest.

    Returns:
        An array expression.
    """
    return Array(
        expressions=[
            maybe_parse(expression, copy=copy, dialect=dialect, **kwargs)
            for expression in expressions
        ]
    )


def tuple_(
    *expressions: ExpOrStr, copy: bool = True, dialect: DialectType = None, **kwargs
) -> Tuple:
    """
    Returns an tuple.

    Examples:
        >>> tuple_(1, 'x').sql()
        '(1, x)'

    Args:
        expressions: the expressions to add to the tuple.
        copy: whether to copy the argument expressions.
        dialect: the source dialect.
        kwargs: the kwargs used to instantiate the function of interest.

    Returns:
        A tuple expression.
    """
    return Tuple(
        expressions=[
            maybe_parse(expression, copy=copy, dialect=dialect, **kwargs)
            for expression in expressions
        ]
    )


def true() -> Boolean:
    """
    Returns a true Boolean expression.
    """
    return Boolean(this=True)


def false() -> Boolean:
    """
    Returns a false Boolean expression.
    """
    return Boolean(this=False)


def null() -> Null:
    """
    Returns a Null expression.
    """
    return Null()


def apply_index_offset(
    this: Expr,
    expressions: t.List[E],
    offset: int,
    dialect: DialectType = None,
) -> t.List[E]:
    if not offset or len(expressions) != 1:
        return expressions

    expression = expressions[0]

    from sqlglot.optimizer.annotate_types import annotate_types
    from sqlglot.optimizer.simplify import simplify

    if not this.type:
        annotate_types(this, dialect=dialect)

    if t.cast(DataType, this.type).this not in (
        DType.UNKNOWN,
        DType.ARRAY,
    ):
        return expressions

    if not expression.type:
        annotate_types(expression, dialect=dialect)

    if t.cast(DataType, expression.type).this in DataType.INTEGER_TYPES:
        logger.info("Applying array index offset (%s)", offset)
        expression = simplify(expression + offset)
        return [expression]

    return expressions


NONNULL_CONSTANTS = (
    Literal,
    Boolean,
)

CONSTANTS = (
    Literal,
    Boolean,
    Null,
)
