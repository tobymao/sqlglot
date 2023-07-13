from __future__ import annotations

import itertools
import typing as t

from sqlglot import alias, exp
from sqlglot._typing import E
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.errors import OptimizeError
from sqlglot.helper import seq_get
from sqlglot.optimizer.scope import Scope, traverse_scope, walk_in_scope
from sqlglot.schema import Schema, ensure_schema


def qualify_columns(
    expression: exp.Expression,
    schema: t.Dict | Schema,
    expand_alias_refs: bool = True,
    infer_schema: t.Optional[bool] = None,
) -> exp.Expression:
    """
    Rewrite sqlglot AST to have fully qualified columns.

    Example:
        >>> import sqlglot
        >>> schema = {"tbl": {"col": "INT"}}
        >>> expression = sqlglot.parse_one("SELECT col FROM tbl")
        >>> qualify_columns(expression, schema).sql()
        'SELECT tbl.col AS col FROM tbl'

    Args:
        expression: Expression to qualify.
        schema: Database schema.
        expand_alias_refs: Whether or not to expand references to aliases.
        infer_schema: Whether or not to infer the schema if missing.

    Returns:
        The qualified expression.
    """
    schema = ensure_schema(schema)
    infer_schema = schema.empty if infer_schema is None else infer_schema

    for scope in traverse_scope(expression):
        resolver = Resolver(scope, schema, infer_schema=infer_schema)
        _pop_table_column_aliases(scope.ctes)
        _pop_table_column_aliases(scope.derived_tables)
        using_column_tables = _expand_using(scope, resolver)

        if schema.empty and expand_alias_refs:
            _expand_alias_refs(scope, resolver)

        _qualify_columns(scope, resolver)

        if not schema.empty and expand_alias_refs:
            _expand_alias_refs(scope, resolver)

        if not isinstance(scope.expression, exp.UDTF):
            _expand_stars(scope, resolver, using_column_tables)
            _qualify_outputs(scope)
        _expand_group_by(scope)
        _expand_order_by(scope, resolver)

    return expression


def validate_qualify_columns(expression: E) -> E:
    """Raise an `OptimizeError` if any columns aren't qualified"""
    unqualified_columns = []
    for scope in traverse_scope(expression):
        if isinstance(scope.expression, exp.Select):
            unqualified_columns.extend(scope.unqualified_columns)
            if scope.external_columns and not scope.is_correlated_subquery and not scope.pivots:
                column = scope.external_columns[0]
                raise OptimizeError(
                    f"""Column '{column}' could not be resolved{f" for table: '{column.table}'" if column.table else ''}"""
                )

    if unqualified_columns:
        raise OptimizeError(f"Ambiguous columns: {unqualified_columns}")
    return expression


def _pop_table_column_aliases(derived_tables: t.List[exp.CTE | exp.Subquery]) -> None:
    """
    Remove table column aliases.

    (e.g. SELECT ... FROM (SELECT ...) AS foo(col1, col2)
    """
    for derived_table in derived_tables:
        table_alias = derived_table.args.get("alias")
        if table_alias:
            table_alias.args.pop("columns", None)


def _expand_using(scope: Scope, resolver: Resolver) -> t.Dict[str, t.Any]:
    joins = list(scope.find_all(exp.Join))
    names = {join.alias_or_name for join in joins}
    ordered = [key for key in scope.selected_sources if key not in names]

    # Mapping of automatically joined column names to an ordered set of source names (dict).
    column_tables: t.Dict[str, t.Dict[str, t.Any]] = {}

    for join in joins:
        using = join.args.get("using")

        if not using:
            continue

        join_table = join.alias_or_name

        columns = {}

        for k in scope.selected_sources:
            if k in ordered:
                for column in resolver.get_source_columns(k):
                    if column not in columns:
                        columns[column] = k

        source_table = ordered[-1]
        ordered.append(join_table)
        join_columns = resolver.get_source_columns(join_table)
        conditions = []

        for identifier in using:
            identifier = identifier.name
            table = columns.get(identifier)

            if not table or identifier not in join_columns:
                if columns and join_columns:
                    raise OptimizeError(f"Cannot automatically join: {identifier}")

            table = table or source_table
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
        join.set("on", exp.and_(*conditions, copy=False))

    if column_tables:
        for column in scope.columns:
            if not column.table and column.name in column_tables:
                tables = column_tables[column.name]
                coalesce = [exp.column(column.name, table=table) for table in tables]
                replacement = exp.Coalesce(this=coalesce[0], expressions=coalesce[1:])

                # Ensure selects keep their output name
                if isinstance(column.parent, exp.Select):
                    replacement = alias(replacement, alias=column.name, copy=False)

                scope.replace(column, replacement)

    return column_tables


def _expand_alias_refs(scope: Scope, resolver: Resolver) -> None:
    expression = scope.expression

    if not isinstance(expression, exp.Select):
        return

    alias_to_expression: t.Dict[str, t.Tuple[exp.Expression, int]] = {}

    def replace_columns(
        node: t.Optional[exp.Expression], resolve_table: bool = False, literal_index: bool = False
    ) -> None:
        if not node:
            return

        for column, *_ in walk_in_scope(node):
            if not isinstance(column, exp.Column):
                continue
            table = resolver.get_table(column.name) if resolve_table and not column.table else None
            alias_expr, i = alias_to_expression.get(column.name, (None, 1))
            double_agg = (
                (alias_expr.find(exp.AggFunc) and column.find_ancestor(exp.AggFunc))
                if alias_expr
                else False
            )

            if table and (not alias_expr or double_agg):
                column.set("table", table)
            elif not column.table and alias_expr and not double_agg:
                if isinstance(alias_expr, exp.Literal) and (literal_index or resolve_table):
                    if literal_index:
                        column.replace(exp.Literal.number(i))
                else:
                    column.replace(alias_expr.copy())

    for i, projection in enumerate(scope.expression.selects):
        replace_columns(projection)

        if isinstance(projection, exp.Alias):
            alias_to_expression[projection.alias] = (projection.this, i + 1)

    replace_columns(expression.args.get("where"))
    replace_columns(expression.args.get("group"), literal_index=True)
    replace_columns(expression.args.get("having"), resolve_table=True)
    replace_columns(expression.args.get("qualify"), resolve_table=True)
    scope.clear_cache()


def _expand_group_by(scope: Scope):
    expression = scope.expression
    group = expression.args.get("group")
    if not group:
        return

    group.set("expressions", _expand_positional_references(scope, group.expressions))
    expression.set("group", group)


def _expand_order_by(scope: Scope, resolver: Resolver):
    order = scope.expression.args.get("order")
    if not order:
        return

    ordereds = order.expressions
    for ordered, new_expression in zip(
        ordereds,
        _expand_positional_references(scope, (o.this for o in ordereds)),
    ):
        for agg in ordered.find_all(exp.AggFunc):
            for col in agg.find_all(exp.Column):
                if not col.table:
                    col.set("table", resolver.get_table(col.name))

        ordered.set("this", new_expression)

    if scope.expression.args.get("group"):
        selects = {s.this: exp.column(s.alias_or_name) for s in scope.expression.selects}

        for ordered in ordereds:
            ordered = ordered.this

            ordered.replace(
                exp.to_identifier(_select_by_pos(scope, ordered).alias)
                if ordered.is_int
                else selects.get(ordered, ordered)
            )


def _expand_positional_references(scope: Scope, expressions: t.Iterable[E]) -> t.List[E]:
    new_nodes = []
    for node in expressions:
        if node.is_int:
            select = _select_by_pos(scope, t.cast(exp.Literal, node)).this

            if isinstance(select, exp.Literal):
                new_nodes.append(node)
            else:
                new_nodes.append(select.copy())
                scope.clear_cache()
        else:
            new_nodes.append(node)

    return new_nodes


def _select_by_pos(scope: Scope, node: exp.Literal) -> exp.Alias:
    try:
        return scope.expression.selects[int(node.this) - 1].assert_is(exp.Alias)
    except IndexError:
        raise OptimizeError(f"Unknown output column: {node.name}")


def _qualify_columns(scope: Scope, resolver: Resolver) -> None:
    """Disambiguate columns, ensuring each column specifies a source"""
    for column in scope.columns:
        column_table = column.table
        column_name = column.name

        if column_table and column_table in scope.sources:
            source_columns = resolver.get_source_columns(column_table)
            if source_columns and column_name not in source_columns and "*" not in source_columns:
                raise OptimizeError(f"Unknown column: {column_name}")

        if not column_table:
            if scope.pivots and not column.find_ancestor(exp.Pivot):
                # If the column is under the Pivot expression, we need to qualify it
                # using the name of the pivoted source instead of the pivot's alias
                column.set("table", exp.to_identifier(scope.pivots[0].alias))
                continue

            column_table = resolver.get_table(column_name)

            # column_table can be a '' because bigquery unnest has no table alias
            if column_table:
                column.set("table", column_table)
        elif column_table not in scope.sources and (
            not scope.parent or column_table not in scope.parent.sources
        ):
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

    for pivot in scope.pivots:
        for column in pivot.find_all(exp.Column):
            if not column.table and column.name in resolver.all_columns:
                column_table = resolver.get_table(column.name)
                if column_table:
                    column.set("table", column_table)


def _expand_stars(
    scope: Scope, resolver: Resolver, using_column_tables: t.Dict[str, t.Any]
) -> None:
    """Expand stars to lists of column selections"""

    new_selections = []
    except_columns: t.Dict[int, t.Set[str]] = {}
    replace_columns: t.Dict[int, t.Dict[str, str]] = {}
    coalesced_columns = set()

    # TODO: handle optimization of multiple PIVOTs (and possibly UNPIVOTs) in the future
    pivot_columns = None
    pivot_output_columns = None
    pivot = t.cast(t.Optional[exp.Pivot], seq_get(scope.pivots, 0))

    has_pivoted_source = pivot and not pivot.args.get("unpivot")
    if pivot and has_pivoted_source:
        pivot_columns = set(col.output_name for col in pivot.find_all(exp.Column))

        pivot_output_columns = [col.output_name for col in pivot.args.get("columns", [])]
        if not pivot_output_columns:
            pivot_output_columns = [col.alias_or_name for col in pivot.expressions]

    for expression in scope.expression.selects:
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

            # The _PARTITIONTIME and _PARTITIONDATE pseudo-columns are not returned by a SELECT * statement
            # https://cloud.google.com/bigquery/docs/querying-partitioned-tables#query_an_ingestion-time_partitioned_table
            if resolver.schema.dialect == "bigquery":
                columns = [
                    name
                    for name in columns
                    if name.upper() not in ("_PARTITIONTIME", "_PARTITIONDATE")
                ]

            if columns and "*" not in columns:
                if pivot and has_pivoted_source and pivot_columns and pivot_output_columns:
                    implicit_columns = [col for col in columns if col not in pivot_columns]
                    new_selections.extend(
                        exp.alias_(exp.column(name, table=pivot.alias), name, copy=False)
                        for name in implicit_columns + pivot_output_columns
                    )
                    continue

                table_id = id(table)
                for name in columns:
                    if name in using_column_tables and table in using_column_tables[name]:
                        if name in coalesced_columns:
                            continue

                        coalesced_columns.add(name)
                        tables = using_column_tables[name]
                        coalesce = [exp.column(name, table=table) for table in tables]

                        new_selections.append(
                            alias(
                                exp.Coalesce(this=coalesce[0], expressions=coalesce[1:]),
                                alias=name,
                                copy=False,
                            )
                        )
                    elif name not in except_columns.get(table_id, set()):
                        alias_ = replace_columns.get(table_id, {}).get(name, name)
                        column = exp.column(name, table=table)
                        new_selections.append(
                            alias(column, alias_, copy=False) if alias_ != name else column
                        )
            else:
                return

    # Ensures we don't overwrite the initial selections with an empty list
    if new_selections:
        scope.expression.set("expressions", new_selections)


def _add_except_columns(
    expression: exp.Expression, tables, except_columns: t.Dict[int, t.Set[str]]
) -> None:
    except_ = expression.args.get("except")

    if not except_:
        return

    columns = {e.name for e in except_}

    for table in tables:
        except_columns[id(table)] = columns


def _add_replace_columns(
    expression: exp.Expression, tables, replace_columns: t.Dict[int, t.Dict[str, str]]
) -> None:
    replace = expression.args.get("replace")

    if not replace:
        return

    columns = {e.this.name: e.alias for e in replace}

    for table in tables:
        replace_columns[id(table)] = columns


def _qualify_outputs(scope: Scope):
    """Ensure all output columns are aliased"""
    new_selections = []

    for i, (selection, aliased_column) in enumerate(
        itertools.zip_longest(scope.expression.selects, scope.outer_column_list)
    ):
        if isinstance(selection, exp.Subquery):
            if not selection.output_name:
                selection.set("alias", exp.TableAlias(this=exp.to_identifier(f"_col_{i}")))
        elif not isinstance(selection, exp.Alias) and not selection.is_star:
            selection = alias(
                selection,
                alias=selection.output_name or f"_col_{i}",
            )
        if aliased_column:
            selection.set("alias", exp.to_identifier(aliased_column))

        new_selections.append(selection)

    scope.expression.set("expressions", new_selections)


def quote_identifiers(expression: E, dialect: DialectType = None, identify: bool = True) -> E:
    """Makes sure all identifiers that need to be quoted are quoted."""
    return expression.transform(
        Dialect.get_or_raise(dialect).quote_identifier, identify=identify, copy=False
    )


class Resolver:
    """
    Helper for resolving columns.

    This is a class so we can lazily load some things and easily share them across functions.
    """

    def __init__(self, scope: Scope, schema: Schema, infer_schema: bool = True):
        self.scope = scope
        self.schema = schema
        self._source_columns = None
        self._unambiguous_columns: t.Optional[t.Dict[str, str]] = None
        self._all_columns = None
        self._infer_schema = infer_schema

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

        if not table_name and self._infer_schema:
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
            return exp.to_identifier(node_alias.this)

        return exp.to_identifier(table_name)

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
