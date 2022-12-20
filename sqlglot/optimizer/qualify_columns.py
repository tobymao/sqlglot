import itertools

from sqlglot import alias, exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import ensure_schema


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
        resolver = _Resolver(scope, schema)
        _pop_table_column_aliases(scope.ctes)
        _pop_table_column_aliases(scope.derived_tables)
        _expand_using(scope, resolver)
        _expand_group_by(scope, resolver)
        _qualify_columns(scope, resolver)
        _expand_order_by(scope)
        if not isinstance(scope.expression, exp.UDTF):
            _expand_stars(scope, resolver)
            _qualify_outputs(scope)
        _check_unknown_tables(scope)

    return expression


def _pop_table_column_aliases(derived_tables):
    """
    Remove table column aliases.

    (e.g. SELECT ... FROM (SELECT ...) AS foo(col1, col2)
    """
    for derived_table in derived_tables:
        if isinstance(derived_table.unnest(), exp.UDTF):
            continue
        table_alias = derived_table.args.get("alias")
        if table_alias:
            table_alias.args.pop("columns", None)


def _expand_using(scope, resolver):
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
                for column in resolver.get_source_columns(k):
                    if column not in columns:
                        columns[column] = k

        ordered.append(join_table)
        join_columns = resolver.get_source_columns(join_table)
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


def _expand_group_by(scope, resolver):
    group = scope.expression.args.get("group")
    if not group:
        return

    # Replace references to select aliases
    def transform(node, *_):
        if isinstance(node, exp.Column) and not node.table:
            table = resolver.get_table(node.name)

            # Source columns get priority over select aliases
            if table:
                node.set("table", exp.to_identifier(table))
                return node

            selects = {s.alias_or_name: s for s in scope.selects}

            select = selects.get(node.name)
            if select:
                scope.clear_cache()
                if isinstance(select, exp.Alias):
                    select = select.this
                return select.copy()

        return node

    group.transform(transform, copy=False)
    group.set("expressions", _expand_positional_references(scope, group.expressions))
    scope.expression.set("group", group)


def _expand_order_by(scope):
    order = scope.expression.args.get("order")
    if not order:
        return

    ordereds = order.expressions
    for ordered, new_expression in zip(
        ordereds,
        _expand_positional_references(scope, (o.this for o in ordereds)),
    ):
        ordered.set("this", new_expression)


def _expand_positional_references(scope, expressions):
    new_nodes = []
    for node in expressions:
        if node.is_int:
            try:
                select = scope.selects[int(node.name) - 1]
            except IndexError:
                raise OptimizeError(f"Unknown output column: {node.name}")
            if isinstance(select, exp.Alias):
                select = select.this
            new_nodes.append(select.copy())
            scope.clear_cache()
        else:
            new_nodes.append(node)

    return new_nodes


def _qualify_columns(scope, resolver):
    """Disambiguate columns, ensuring each column specifies a source"""
    for column in scope.columns:
        column_table = column.table
        column_name = column.name

        if (
            column_table
            and column_table in scope.sources
            and column_name not in resolver.get_source_columns(column_table)
        ):
            raise OptimizeError(f"Unknown column: {column_name}")

        if not column_table:
            column_table = resolver.get_table(column_name)

            if not scope.is_subquery and not scope.is_udtf:
                if column_name not in resolver.all_columns:
                    raise OptimizeError(f"Unknown column: {column_name}")

                if column_table is None:
                    raise OptimizeError(f"Ambiguous column: {column_name}")

            # column_table can be a '' because bigquery unnest has no table alias
            if column_table:
                column.set("table", exp.to_identifier(column_table))

    columns_missing_from_scope = []
    # Determine whether each reference in the order by clause is to a column or an alias.
    for ordered in scope.find_all(exp.Ordered):
        for column in ordered.find_all(exp.Column):
            if (
                not column.table
                and column.parent is not ordered
                and column.name in resolver.all_columns
            ):
                columns_missing_from_scope.append(column)

    # Determine whether each reference in the having clause is to a column or an alias.
    for having in scope.find_all(exp.Having):
        for column in having.find_all(exp.Column):
            if (
                not column.table
                and column.find_ancestor(exp.AggFunc)
                and column.name in resolver.all_columns
            ):
                columns_missing_from_scope.append(column)

    for column in columns_missing_from_scope:
        column_table = resolver.get_table(column.name)

        if column_table is None:
            raise OptimizeError(f"Ambiguous column: {column.name}")

        column.set("table", exp.to_identifier(column_table))


def _expand_stars(scope, resolver):
    """Expand stars to lists of column selections"""

    new_selections = []
    except_columns = {}
    replace_columns = {}

    for expression in scope.selects:
        if isinstance(expression, exp.Star):
            tables = list(scope.selected_sources)
            _add_except_columns(expression, tables, except_columns)
            _add_replace_columns(expression, tables, replace_columns)
        elif isinstance(expression, exp.Column) and isinstance(expression.this, exp.Star):
            tables = [expression.table]
            _add_except_columns(expression.this, tables, except_columns)
            _add_replace_columns(expression.this, tables, replace_columns)
        else:
            new_selections.append(expression)
            continue

        for table in tables:
            if table not in scope.sources:
                raise OptimizeError(f"Unknown table: {table}")
            columns = resolver.get_source_columns(table, only_visible=True)
            table_id = id(table)
            for name in columns:
                if name not in except_columns.get(table_id, set()):
                    alias_ = replace_columns.get(table_id, {}).get(name, name)
                    column = exp.column(name, table)
                    new_selections.append(alias(column, alias_) if alias_ != name else column)

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
        elif isinstance(selection, exp.Subquery):
            if not selection.alias:
                selection.set("alias", exp.TableAlias(this=exp.to_identifier(f"_col_{i}")))
        elif not isinstance(selection, exp.Alias):
            alias_ = alias(exp.column(""), f"_col_{i}")
            alias_.set("this", selection)
            selection = alias_

        if aliased_column:
            selection.set("alias", exp.to_identifier(aliased_column))

        new_selections.append(selection)

    scope.expression.set("expressions", new_selections)


def _check_unknown_tables(scope):
    if scope.external_columns and not scope.is_udtf and not scope.is_correlated_subquery:
        raise OptimizeError(f"Unknown table: {scope.external_columns[0].text('table')}")


class _Resolver:
    """
    Helper for resolving columns.

    This is a class so we can lazily load some things and easily share them across functions.
    """

    def __init__(self, scope, schema):
        self.scope = scope
        self.schema = schema
        self._source_columns = None
        self._unambiguous_columns = None
        self._all_columns = None

    def get_table(self, column_name):
        """
        Get the table for a column name.

        Args:
            column_name (str)
        Returns:
            (str) table name
        """
        if self._unambiguous_columns is None:
            self._unambiguous_columns = self._get_unambiguous_columns(
                self._get_all_source_columns()
            )
        return self._unambiguous_columns.get(column_name)

    @property
    def all_columns(self):
        """All available columns of all sources in this scope"""
        if self._all_columns is None:
            self._all_columns = set(
                column for columns in self._get_all_source_columns().values() for column in columns
            )
        return self._all_columns

    def get_source_columns(self, name, only_visible=False):
        """Resolve the source columns for a given source `name`"""
        if name not in self.scope.sources:
            raise OptimizeError(f"Unknown table: {name}")

        source = self.scope.sources[name]

        # If referencing a table, return the columns from the schema
        if isinstance(source, exp.Table):
            try:
                return self.schema.column_names(source, only_visible)
            except Exception as e:
                raise OptimizeError(str(e)) from e

        if isinstance(source, Scope) and isinstance(source.expression, exp.Values):
            return source.expression.alias_column_names

        # Otherwise, if referencing another scope, return that scope's named selects
        return source.expression.named_selects

    def _get_all_source_columns(self):
        if self._source_columns is None:
            self._source_columns = {
                k: self.get_source_columns(k) for k in self.scope.selected_sources
            }
        return self._source_columns

    def _get_unambiguous_columns(self, source_columns):
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
        unambiguous_columns = {col: first_table for col in self._find_unique_columns(first_columns)}
        all_columns = set(unambiguous_columns)

        for table, columns in source_columns[1:]:
            unique = self._find_unique_columns(columns)
            ambiguous = set(all_columns).intersection(unique)
            all_columns.update(columns)
            for column in ambiguous:
                unambiguous_columns.pop(column, None)
            for column in unique.difference(ambiguous):
                unambiguous_columns[column] = table

        return unambiguous_columns

    @staticmethod
    def _find_unique_columns(columns):
        """
        Find the unique columns in a list of columns.

        Example:
            >>> sorted(_Resolver._find_unique_columns(["a", "b", "b", "c"]))
            ['a', 'c']

        This is necessary because duplicate column names are ambiguous.
        """
        counts = {}
        for column in columns:
            counts[column] = counts.get(column, 0) + 1
        return {column for column, count in counts.items() if count == 1}
