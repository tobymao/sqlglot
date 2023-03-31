import itertools
import typing as t

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
        resolver = Resolver(scope, schema)
        _pop_table_column_aliases(scope.ctes)
        _pop_table_column_aliases(scope.derived_tables)
        using_column_tables = _expand_using(scope, resolver)
        _qualify_columns(scope, resolver)
        if not isinstance(scope.expression, exp.UDTF):
            _expand_stars(scope, resolver, using_column_tables)
            _qualify_outputs(scope)
        _expand_alias_refs(scope, resolver)
        _expand_group_by(scope, resolver)
        _expand_order_by(scope)

    return expression


def validate_qualify_columns(expression):
    """Raise an `OptimizeError` if any columns aren't qualified"""
    unqualified_columns = []
    for scope in traverse_scope(expression):
        if isinstance(scope.expression, exp.Select):
            unqualified_columns.extend(scope.unqualified_columns)
            if scope.external_columns and not scope.is_correlated_subquery:
                column = scope.external_columns[0]
                raise OptimizeError(f"Unknown table: '{column.table}' for column '{column}'")

    if unqualified_columns:
        raise OptimizeError(f"Ambiguous columns: {unqualified_columns}")
    return expression


def _pop_table_column_aliases(derived_tables):
    """
    Remove table column aliases.

    (e.g. SELECT ... FROM (SELECT ...) AS foo(col1, col2)
    """
    for derived_table in derived_tables:
        table_alias = derived_table.args.get("alias")
        if table_alias:
            table_alias.args.pop("columns", None)


def _expand_using(scope, resolver):
    joins = list(scope.find_all(exp.Join))
    names = {join.this.alias for join in joins}
    ordered = [key for key in scope.selected_sources if key not in names]

    # Mapping of automatically joined column names to an ordered set of source names (dict).
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

            # Set all values in the dict to None, because we only care about the key ordering
            tables = column_tables.setdefault(identifier, {})
            if table not in tables:
                tables[table] = None
            if join_table not in tables:
                tables[join_table] = None

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

    return column_tables


def _expand_alias_refs(scope, resolver):
    selects = {}

    # Replace references to select aliases
    def transform(node, source_first=True):
        if isinstance(node, exp.Column) and not node.table:
            table = resolver.get_table(node.name)

            # Source columns get priority over select aliases
            if source_first and table:
                node.set("table", table)
                return node

            if not selects:
                for s in scope.selects:
                    selects[s.alias_or_name] = s
            select = selects.get(node.name)

            if select:
                scope.clear_cache()
                if isinstance(select, exp.Alias):
                    select = select.this
                return select.copy()

            node.set("table", table)
        elif isinstance(node, exp.Expression) and not isinstance(node, exp.Subqueryable):
            exp.replace_children(node, transform, source_first)

        return node

    for select in scope.expression.selects:
        transform(select)

    for modifier, source_first in (
        ("where", True),
        ("group", True),
        ("having", False),
    ):
        transform(scope.expression.args.get(modifier), source_first=source_first)


def _expand_group_by(scope, resolver):
    group = scope.expression.args.get("group")
    if not group:
        return

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

        if column_table and column_table in scope.sources:
            source_columns = resolver.get_source_columns(column_table)
            if source_columns and column_name not in source_columns and "*" not in source_columns:
                raise OptimizeError(f"Unknown column: {column_name}")

        if not column_table:
            column_table = resolver.get_table(column_name)

            # column_table can be a '' because bigquery unnest has no table alias
            if column_table:
                column.set("table", column_table)
        elif column_table not in scope.sources:
            # structs are used like tables (e.g. "struct"."field"), so they need to be qualified
            # separately and represented as dot(dot(...(<table>.<column>, field1), field2, ...))

            root, *parts = column.parts

            if root.name in scope.sources:
                # struct is already qualified, but we still need to change the AST representation
                column_table = root
                root, *parts = parts
            else:
                column_table = resolver.get_table(root.name)

            if column_table:
                column.replace(exp.Dot.build([exp.column(root, table=column_table), *parts]))

    columns_missing_from_scope = []

    # Determine whether each reference in the order by clause is to a column or an alias.
    order = scope.expression.args.get("order")

    if order:
        for ordered in order.expressions:
            for column in ordered.find_all(exp.Column):
                if (
                    not column.table
                    and column.parent is not ordered
                    and column.name in resolver.all_columns
                ):
                    columns_missing_from_scope.append(column)

    # Determine whether each reference in the having clause is to a column or an alias.
    having = scope.expression.args.get("having")

    if having:
        for column in having.find_all(exp.Column):
            if (
                not column.table
                and column.find_ancestor(exp.AggFunc)
                and column.name in resolver.all_columns
            ):
                columns_missing_from_scope.append(column)

    for column in columns_missing_from_scope:
        column_table = resolver.get_table(column.name)

        if column_table:
            column.set("table", column_table)


def _expand_stars(scope, resolver, using_column_tables):
    """Expand stars to lists of column selections"""

    new_selections = []
    except_columns = {}
    replace_columns = {}
    coalesced_columns = set()

    for expression in scope.selects:
        if isinstance(expression, exp.Star):
            tables = list(scope.selected_sources)
            _add_except_columns(expression, tables, except_columns)
            _add_replace_columns(expression, tables, replace_columns)
        elif expression.is_star:
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

            if columns and "*" not in columns:
                table_id = id(table)
                for name in columns:
                    if name in using_column_tables and table in using_column_tables[name]:
                        if name in coalesced_columns:
                            continue

                        coalesced_columns.add(name)
                        tables = using_column_tables[name]
                        coalesce = [exp.column(name, table=table) for table in tables]

                        new_selections.append(
                            exp.alias_(
                                exp.Coalesce(this=coalesce[0], expressions=coalesce[1:]), alias=name
                            )
                        )
                    elif name not in except_columns.get(table_id, set()):
                        alias_ = replace_columns.get(table_id, {}).get(name, name)
                        column = exp.column(name, table)
                        new_selections.append(alias(column, alias_) if alias_ != name else column)
            else:
                return
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
        if isinstance(selection, exp.Subquery):
            if not selection.output_name:
                selection.set("alias", exp.TableAlias(this=exp.to_identifier(f"_col_{i}")))
        elif not isinstance(selection, exp.Alias) and not selection.is_star:
            alias_ = alias(exp.column(""), alias=selection.output_name or f"_col_{i}")
            alias_.set("this", selection)
            selection = alias_

        if aliased_column:
            selection.set("alias", exp.to_identifier(aliased_column))

        new_selections.append(selection)

    scope.expression.set("expressions", new_selections)


class Resolver:
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

    def get_table(self, column_name: str) -> t.Optional[exp.Identifier]:
        """
        Get the table for a column name.

        Args:
            column_name: The column name to find the table for.
        Returns:
            The table name if it can be found/inferred.
        """
        if self._unambiguous_columns is None:
            self._unambiguous_columns = self._get_unambiguous_columns(
                self._get_all_source_columns()
            )

        table_name = self._unambiguous_columns.get(column_name)

        if not table_name:
            sources_without_schema = tuple(
                source
                for source, columns in self._get_all_source_columns().items()
                if not columns or "*" in columns
            )
            if len(sources_without_schema) == 1:
                table_name = sources_without_schema[0]

        if table_name not in self.scope.selected_sources:
            return exp.to_identifier(table_name)

        node, _ = self.scope.selected_sources.get(table_name)

        if isinstance(node, exp.Subqueryable):
            while node and node.alias != table_name:
                node = node.parent

        node_alias = node.args.get("alias")
        if node_alias:
            return node_alias.this

        return exp.to_identifier(
            table_name, quoted=node.this.quoted if isinstance(node, exp.Table) else None
        )

    @property
    def all_columns(self):
        """All available columns of all sources in this scope"""
        if self._all_columns is None:
            self._all_columns = {
                column for columns in self._get_all_source_columns().values() for column in columns
            }
        return self._all_columns

    def get_source_columns(self, name, only_visible=False):
        """Resolve the source columns for a given source `name`"""
        if name not in self.scope.sources:
            raise OptimizeError(f"Unknown table: {name}")

        source = self.scope.sources[name]

        # If referencing a table, return the columns from the schema
        if isinstance(source, exp.Table):
            return self.schema.column_names(source, only_visible)

        if isinstance(source, Scope) and isinstance(source.expression, exp.Values):
            return source.expression.alias_column_names

        # Otherwise, if referencing another scope, return that scope's named selects
        return source.expression.named_selects

    def _get_all_source_columns(self):
        if self._source_columns is None:
            self._source_columns = {
                k: self.get_source_columns(k)
                for k in itertools.chain(self.scope.selected_sources, self.scope.lateral_sources)
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
            >>> sorted(Resolver._find_unique_columns(["a", "b", "b", "c"]))
            ['a', 'c']

        This is necessary because duplicate column names are ambiguous.
        """
        counts = {}
        for column in columns:
            counts[column] = counts.get(column, 0) + 1
        return {column for column, count in counts.items() if count == 1}
