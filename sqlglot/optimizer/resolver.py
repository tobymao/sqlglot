from __future__ import annotations

import itertools
import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import OptimizeError
from sqlglot.helper import seq_get, SingleValuedMapping
from sqlglot.optimizer.scope import Scope

if t.TYPE_CHECKING:
    from sqlglot.schema import Schema


class Resolver:
    """
    Helper for resolving columns.

    This is a class so we can lazily load some things and easily share them across functions.
    """

    def __init__(self, scope: Scope, schema: Schema, infer_schema: bool = True):
        self.scope = scope
        self.schema = schema
        self.dialect = schema.dialect or Dialect()
        self._source_columns: t.Optional[t.Dict[str, t.Sequence[str]]] = None
        self._unambiguous_columns: t.Optional[t.Mapping[str, str]] = None
        self._all_columns: t.Optional[t.Set[str]] = None
        self._infer_schema = infer_schema
        self._get_source_columns_cache: t.Dict[t.Tuple[str, bool], t.Sequence[str]] = {}

    def get_table(self, column: str | exp.Column) -> t.Optional[exp.Identifier]:
        """
        Get the table for a column name.

        Args:
            column: The column expression (or column name) to find the table for.
        Returns:
            The table name if it can be found/inferred.
        """
        column_name = column if isinstance(column, str) else column.name

        table_name = self._get_table_name_from_sources(column_name)

        if not table_name and isinstance(column, exp.Column):
            # Fall-back case: If we couldn't find the `table_name` from ALL of the sources,
            # attempt to disambiguate the column based on other characteristics e.g if this column is in a join condition,
            # we may be able to disambiguate based on the source order.
            if join_context := self._get_column_join_context(column):
                # In this case, the return value will be the join that _may_ be able to disambiguate the column
                # and we can use the source columns available at that join to get the table name
                # catch OptimizeError if column is still ambiguous and try to resolve with schema inference below
                try:
                    table_name = self._get_table_name_from_sources(
                        column_name, self._get_available_source_columns(join_context)
                    )
                except OptimizeError:
                    pass

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

        if isinstance(node, exp.Query):
            while node and node.alias != table_name:
                node = node.parent

        node_alias = node.args.get("alias")
        if node_alias:
            return exp.to_identifier(node_alias.this)

        return exp.to_identifier(table_name)

    @property
    def all_columns(self) -> t.Set[str]:
        """All available columns of all sources in this scope"""
        if self._all_columns is None:
            self._all_columns = {
                column for columns in self._get_all_source_columns().values() for column in columns
            }
        return self._all_columns

    def get_source_columns_from_set_op(self, expression: exp.Expression) -> t.List[str]:
        if isinstance(expression, exp.Select):
            return expression.named_selects
        if isinstance(expression, exp.Subquery) and isinstance(expression.this, exp.SetOperation):
            # Different types of SET modifiers can be chained together if they're explicitly grouped by nesting
            return self.get_source_columns_from_set_op(expression.this)
        if not isinstance(expression, exp.SetOperation):
            raise OptimizeError(f"Unknown set operation: {expression}")

        set_op = expression

        # BigQuery specific set operations modifiers, e.g INNER UNION ALL BY NAME
        on_column_list = set_op.args.get("on")

        if on_column_list:
            # The resulting columns are the columns in the ON clause:
            # {INNER | LEFT | FULL} UNION ALL BY NAME ON (col1, col2, ...)
            columns = [col.name for col in on_column_list]
        elif set_op.side or set_op.kind:
            side = set_op.side
            kind = set_op.kind

            # Visit the children UNIONs (if any) in a post-order traversal
            left = self.get_source_columns_from_set_op(set_op.left)
            right = self.get_source_columns_from_set_op(set_op.right)

            # We use dict.fromkeys to deduplicate keys and maintain insertion order
            if side == "LEFT":
                columns = left
            elif side == "FULL":
                columns = list(dict.fromkeys(left + right))
            elif kind == "INNER":
                columns = list(dict.fromkeys(left).keys() & dict.fromkeys(right).keys())
        else:
            columns = set_op.named_selects

        return columns

    def get_source_columns(self, name: str, only_visible: bool = False) -> t.Sequence[str]:
        """Resolve the source columns for a given source `name`."""
        cache_key = (name, only_visible)
        if cache_key not in self._get_source_columns_cache:
            if name not in self.scope.sources:
                raise OptimizeError(f"Unknown table: {name}")

            source = self.scope.sources[name]

            if isinstance(source, exp.Table):
                columns = self.schema.column_names(source, only_visible)
            elif isinstance(source, Scope) and isinstance(
                source.expression, (exp.Values, exp.Unnest)
            ):
                columns = source.expression.named_selects

                # in bigquery, unnest structs are automatically scoped as tables, so you can
                # directly select a struct field in a query.
                # this handles the case where the unnest is statically defined.
                if self.dialect.UNNEST_COLUMN_ONLY and isinstance(source.expression, exp.Unnest):
                    unnest = source.expression

                    # if type is not annotated yet, try to get it from the schema
                    if not unnest.type or unnest.type.is_type(exp.DataType.Type.UNKNOWN):
                        unnest_expr = seq_get(unnest.expressions, 0)
                        if isinstance(unnest_expr, exp.Column) and self.scope.parent:
                            col_type = self._get_unnest_column_type(unnest_expr)
                            # extract element type if it's an ARRAY
                            if col_type and col_type.is_type(exp.DataType.Type.ARRAY):
                                element_types = col_type.expressions
                                if element_types:
                                    unnest.type = element_types[0].copy()
                            else:
                                if col_type:
                                    unnest.type = col_type.copy()
                    # check if the result type is a STRUCT - extract struct field names
                    if unnest.is_type(exp.DataType.Type.STRUCT):
                        for k in unnest.type.expressions:  # type: ignore
                            columns.append(k.name)
            elif isinstance(source, Scope) and isinstance(source.expression, exp.SetOperation):
                columns = self.get_source_columns_from_set_op(source.expression)

            else:
                select = seq_get(source.expression.selects, 0)

                if isinstance(select, exp.QueryTransform):
                    # https://spark.apache.org/docs/3.5.1/sql-ref-syntax-qry-select-transform.html
                    schema = select.args.get("schema")
                    columns = [c.name for c in schema.expressions] if schema else ["key", "value"]
                else:
                    columns = source.expression.named_selects

            node, _ = self.scope.selected_sources.get(name) or (None, None)
            if isinstance(node, Scope):
                column_aliases = node.expression.alias_column_names
            elif isinstance(node, exp.Expression):
                column_aliases = node.alias_column_names
            else:
                column_aliases = []

            if column_aliases:
                # If the source's columns are aliased, their aliases shadow the corresponding column names.
                # This can be expensive if there are lots of columns, so only do this if column_aliases exist.
                columns = [
                    alias or name
                    for (name, alias) in itertools.zip_longest(columns, column_aliases)
                ]

            self._get_source_columns_cache[cache_key] = columns

        return self._get_source_columns_cache[cache_key]

    def _get_all_source_columns(self) -> t.Dict[str, t.Sequence[str]]:
        if self._source_columns is None:
            self._source_columns = {
                source_name: self.get_source_columns(source_name)
                for source_name, source in itertools.chain(
                    self.scope.selected_sources.items(), self.scope.lateral_sources.items()
                )
            }
        return self._source_columns

    def _get_table_name_from_sources(
        self, column_name: str, source_columns: t.Optional[t.Dict[str, t.Sequence[str]]] = None
    ) -> t.Optional[str]:
        if not source_columns:
            # If not supplied, get all sources to calculate unambiguous columns
            if self._unambiguous_columns is None:
                self._unambiguous_columns = self._get_unambiguous_columns(
                    self._get_all_source_columns()
                )

            unambiguous_columns = self._unambiguous_columns
        else:
            unambiguous_columns = self._get_unambiguous_columns(source_columns)

        return unambiguous_columns.get(column_name)

    def _get_column_join_context(self, column: exp.Column) -> t.Optional[exp.Join]:
        """
        Check if a column participating in a join can be qualified based on the source order.
        """
        args = self.scope.expression.args
        joins = args.get("joins")

        if not joins or args.get("laterals") or args.get("pivots"):
            # Feature gap: We currently don't try to disambiguate columns if other sources
            # (e.g laterals, pivots) exist alongside joins
            return None

        join_ancestor = column.find_ancestor(exp.Join, exp.Select)

        if (
            isinstance(join_ancestor, exp.Join)
            and join_ancestor.alias_or_name in self.scope.selected_sources
        ):
            # Ensure that the found ancestor is a join that contains an actual source,
            # e.g in Clickhouse `b` is an array expression in `a ARRAY JOIN b`
            return join_ancestor

        return None

    def _get_available_source_columns(
        self, join_ancestor: exp.Join
    ) -> t.Dict[str, t.Sequence[str]]:
        """
        Get the source columns that are available at the point where a column is referenced.

        For columns in JOIN conditions, this only includes tables that have been joined
        up to that point. Example:

        ```
        SELECT * FROM t_1 INNER JOIN ... INNER JOIN t_n ON t_1.a = c INNER JOIN t_n+1 ON ...
        ```                                                        ^
                                                                   |
                                +----------------------------------+
                                |
                                ⌄
        The unqualified column `c` is not ambiguous if no other sources up until that
        join i.e t_1, ..., t_n, contain a column named `c`.

        """
        args = self.scope.expression.args

        # Collect tables in order: FROM clause tables + joined tables up to current join
        from_name = args["from_"].alias_or_name
        available_sources = {from_name: self.get_source_columns(from_name)}

        for join in args["joins"][: t.cast(int, join_ancestor.index) + 1]:
            available_sources[join.alias_or_name] = self.get_source_columns(join.alias_or_name)

        return available_sources

    def _get_unambiguous_columns(
        self, source_columns: t.Dict[str, t.Sequence[str]]
    ) -> t.Mapping[str, str]:
        """
        Find all the unambiguous columns in sources.

        Args:
            source_columns: Mapping of names to source columns.

        Returns:
            Mapping of column name to source name.
        """
        if not source_columns:
            return {}

        source_columns_pairs = list(source_columns.items())

        first_table, first_columns = source_columns_pairs[0]

        if len(source_columns_pairs) == 1:
            # Performance optimization - avoid copying first_columns if there is only one table.
            return SingleValuedMapping(first_columns, first_table)

        # For BigQuery UNNEST_COLUMN_ONLY, build a mapping of original UNNEST aliases
        # from alias.columns[0] to their source names. This is used to resolve shadowing
        # where an UNNEST alias shadows a column name from another table.
        unnest_original_aliases: t.Dict[str, str] = {
            source.expression.args["alias"].columns[0].name: source_name
            for source_name, source in self.scope.sources.items()
            if (
                self.dialect.UNNEST_COLUMN_ONLY
                and isinstance(source.expression, exp.Unnest)
                and (alias_arg := source.expression.args.get("alias"))
                and alias_arg.columns
            )
        }

        unambiguous_columns = {col: first_table for col in first_columns}
        all_columns = set(unambiguous_columns)

        for table, columns in source_columns_pairs[1:]:
            unique = set(columns)
            ambiguous = all_columns.intersection(unique)
            all_columns.update(columns)

            for column in ambiguous:
                if column in unnest_original_aliases:
                    unambiguous_columns[column] = unnest_original_aliases[column]
                    continue

                unambiguous_columns.pop(column, None)
            for column in unique.difference(ambiguous):
                unambiguous_columns[column] = table

        return unambiguous_columns

    def _get_unnest_column_type(self, column: exp.Column) -> t.Optional[exp.DataType]:
        """
        Get the type of a column being unnested, tracing through CTEs/subqueries to find the base table.

        Args:
            column: The column expression being unnested.

        Returns:
            The DataType of the column, or None if not found.
        """
        scope = self.scope.parent

        # if column is qualified, use that table, otherwise disambiguate using the resolver
        if column.table:
            table_name = column.table
        else:
            # use the parent scope's resolver to disambiguate the column
            parent_resolver = Resolver(scope, self.schema, self._infer_schema)
            table_identifier = parent_resolver.get_table(column)
            if not table_identifier:
                return None
            table_name = table_identifier.name

        source = scope.sources.get(table_name)
        return self._get_column_type_from_scope(source, column) if source else None

    def _get_column_type_from_scope(
        self, source: t.Union[Scope, exp.Table], column: exp.Column
    ) -> t.Optional[exp.DataType]:
        """
        Get a column's type by tracing through scopes/tables to find the base table.

        Args:
            source: The source to search - can be a Scope (to iterate its sources) or a Table.
            column: The column to find the type for.

        Returns:
            The DataType of the column, or None if not found.
        """
        if isinstance(source, exp.Table):
            # base table - get the column type from schema
            col_type: t.Optional[exp.DataType] = self.schema.get_column_type(source, column)
            if col_type and not col_type.is_type(exp.DataType.Type.UNKNOWN):
                return col_type
        elif isinstance(source, Scope):
            # iterate over all sources in the scope
            for source_name, nested_source in source.sources.items():
                col_type = self._get_column_type_from_scope(nested_source, column)
                if col_type and not col_type.is_type(exp.DataType.Type.UNKNOWN):
                    return col_type

        return None
