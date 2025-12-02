from __future__ import annotations

import itertools
import typing as t

from sqlglot import alias, exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.errors import OptimizeError, highlight_sql
from sqlglot.helper import seq_get
from sqlglot.optimizer.annotate_types import TypeAnnotator
from sqlglot.optimizer.resolver import Resolver
from sqlglot.optimizer.scope import Scope, build_scope, traverse_scope, walk_in_scope
from sqlglot.optimizer.simplify import simplify_parens
from sqlglot.schema import Schema, ensure_schema

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def qualify_columns(
    expression: exp.Expression,
    schema: t.Dict | Schema,
    expand_alias_refs: bool = True,
    expand_stars: bool = True,
    infer_schema: t.Optional[bool] = None,
    allow_partial_qualification: bool = False,
    dialect: DialectType = None,
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
        expand_alias_refs: Whether to expand references to aliases.
        expand_stars: Whether to expand star queries. This is a necessary step
            for most of the optimizer's rules to work; do not set to False unless you
            know what you're doing!
        infer_schema: Whether to infer the schema if missing.
        allow_partial_qualification: Whether to allow partial qualification.

    Returns:
        The qualified expression.

    Notes:
        - Currently only handles a single PIVOT or UNPIVOT operator
    """
    schema = ensure_schema(schema, dialect=dialect)
    annotator = TypeAnnotator(schema)
    infer_schema = schema.empty if infer_schema is None else infer_schema
    dialect = schema.dialect or Dialect()
    pseudocolumns = dialect.PSEUDOCOLUMNS

    for scope in traverse_scope(expression):
        if dialect.PREFER_CTE_ALIAS_COLUMN:
            pushdown_cte_alias_columns(scope)

        scope_expression = scope.expression
        is_select = isinstance(scope_expression, exp.Select)

        _separate_pseudocolumns(scope, pseudocolumns)

        resolver = Resolver(scope, schema, infer_schema=infer_schema)
        _pop_table_column_aliases(scope.ctes)
        _pop_table_column_aliases(scope.derived_tables)
        using_column_tables = _expand_using(scope, resolver)

        if (schema.empty or dialect.FORCE_EARLY_ALIAS_REF_EXPANSION) and expand_alias_refs:
            _expand_alias_refs(
                scope,
                resolver,
                dialect,
                expand_only_groupby=dialect.EXPAND_ONLY_GROUP_ALIAS_REF,
            )

        _convert_columns_to_dots(scope, resolver)
        _qualify_columns(
            scope,
            resolver,
            allow_partial_qualification=allow_partial_qualification,
        )

        if not schema.empty and expand_alias_refs:
            _expand_alias_refs(scope, resolver, dialect)

        if is_select:
            if expand_stars:
                _expand_stars(
                    scope,
                    resolver,
                    using_column_tables,
                    pseudocolumns,
                    annotator,
                )
            qualify_outputs(scope)

        _expand_group_by(scope, dialect)

        # DISTINCT ON and ORDER BY follow the same rules (tested in DuckDB, Postgres, ClickHouse)
        # https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT
        _expand_order_by_and_distinct_on(scope, resolver)

        if dialect.ANNOTATE_ALL_SCOPES:
            annotator.annotate_scope(scope)

    return expression


def validate_qualify_columns(expression: E, sql: t.Optional[str] = None) -> E:
    """Raise an `OptimizeError` if any columns aren't qualified"""
    all_unqualified_columns = []
    for scope in traverse_scope(expression):
        if isinstance(scope.expression, exp.Select):
            unqualified_columns = scope.unqualified_columns

            if scope.external_columns and not scope.is_correlated_subquery and not scope.pivots:
                column = scope.external_columns[0]
                for_table = f" for table: '{column.table}'" if column.table else ""
                line = column.this.meta.get("line")
                col = column.this.meta.get("col")
                start = column.this.meta.get("start")
                end = column.this.meta.get("end")

                error_msg = f"Column '{column.name}' could not be resolved{for_table}."
                if line and col:
                    error_msg += f" Line: {line}, Col: {col}"
                if sql and start is not None and end is not None:
                    formatted_sql = highlight_sql(sql, [(start, end)])[0]
                    error_msg += f"\n  {formatted_sql}"

                raise OptimizeError(error_msg)

            if unqualified_columns and scope.pivots and scope.pivots[0].unpivot:
                # New columns produced by the UNPIVOT can't be qualified, but there may be columns
                # under the UNPIVOT's IN clause that can and should be qualified. We recompute
                # this list here to ensure those in the former category will be excluded.
                unpivot_columns = set(_unpivot_columns(scope.pivots[0]))
                unqualified_columns = [c for c in unqualified_columns if c not in unpivot_columns]

            all_unqualified_columns.extend(unqualified_columns)

    if all_unqualified_columns:
        first_column = all_unqualified_columns[0]
        line = first_column.this.meta.get("line")
        col = first_column.this.meta.get("col")
        start = first_column.this.meta.get("start")
        end = first_column.this.meta.get("end")

        error_msg = f"Ambiguous column '{first_column.name}'"
        if line and col:
            error_msg += f" (Line: {line}, Col: {col})"
        if sql and start is not None and end is not None:
            formatted_sql = highlight_sql(sql, [(start, end)])[0]
            error_msg += f"\n  {formatted_sql}"

        raise OptimizeError(error_msg)

    return expression


def _separate_pseudocolumns(scope: Scope, pseudocolumns: t.Set[str]) -> None:
    if not pseudocolumns:
        return

    has_pseudocolumns = False
    scope_expression = scope.expression

    for column in scope.columns:
        name = column.name.upper()
        if name not in pseudocolumns:
            continue

        if name != "LEVEL" or (
            isinstance(scope_expression, exp.Select) and scope_expression.args.get("connect")
        ):
            column.replace(exp.Pseudocolumn(**column.args))
            has_pseudocolumns = True

    if has_pseudocolumns:
        scope.clear_cache()


def _unpivot_columns(unpivot: exp.Pivot) -> t.Iterator[exp.Column]:
    name_columns = [
        field.this
        for field in unpivot.fields
        if isinstance(field, exp.In) and isinstance(field.this, exp.Column)
    ]
    value_columns = (c for e in unpivot.expressions for c in e.find_all(exp.Column))

    return itertools.chain(name_columns, value_columns)


def _pop_table_column_aliases(derived_tables: t.List[exp.CTE | exp.Subquery]) -> None:
    """
    Remove table column aliases.

    For example, `col1` and `col2` will be dropped in SELECT ... FROM (SELECT ...) AS foo(col1, col2)
    """
    for derived_table in derived_tables:
        if isinstance(derived_table.parent, exp.With) and derived_table.parent.recursive:
            continue
        table_alias = derived_table.args.get("alias")
        if table_alias:
            table_alias.set("columns", None)


def _expand_using(scope: Scope, resolver: Resolver) -> t.Dict[str, t.Any]:
    columns = {}

    def _update_source_columns(source_name: str) -> None:
        for column_name in resolver.get_source_columns(source_name):
            if column_name not in columns:
                columns[column_name] = source_name

    joins = list(scope.find_all(exp.Join))
    names = {join.alias_or_name for join in joins}
    ordered = [key for key in scope.selected_sources if key not in names]

    if names and not ordered:
        raise OptimizeError(f"Joins {names} missing source table {scope.expression}")

    # Mapping of automatically joined column names to an ordered set of source names (dict).
    column_tables: t.Dict[str, t.Dict[str, t.Any]] = {}

    for source_name in ordered:
        _update_source_columns(source_name)

    for i, join in enumerate(joins):
        source_table = ordered[-1]
        if source_table:
            _update_source_columns(source_table)

        join_table = join.alias_or_name
        ordered.append(join_table)

        using = join.args.get("using")
        if not using:
            continue

        join_columns = resolver.get_source_columns(join_table)
        conditions = []
        using_identifier_count = len(using)
        is_semi_or_anti_join = join.is_semi_or_anti_join

        for identifier in using:
            identifier = identifier.name
            table = columns.get(identifier)

            if not table or identifier not in join_columns:
                if (columns and "*" not in columns) and join_columns:
                    raise OptimizeError(f"Cannot automatically join: {identifier}")

            table = table or source_table

            if i == 0 or using_identifier_count == 1:
                lhs: exp.Expression = exp.column(identifier, table=table)
            else:
                coalesce_columns = [
                    exp.column(identifier, table=t)
                    for t in ordered[:-1]
                    if identifier in resolver.get_source_columns(t)
                ]
                if len(coalesce_columns) > 1:
                    lhs = exp.func("coalesce", *coalesce_columns)
                else:
                    lhs = exp.column(identifier, table=table)

            conditions.append(lhs.eq(exp.column(identifier, table=join_table)))

            # Set all values in the dict to None, because we only care about the key ordering
            tables = column_tables.setdefault(identifier, {})

            # Do not update the dict if this was a SEMI/ANTI join in
            # order to avoid generating COALESCE columns for this join pair
            if not is_semi_or_anti_join:
                if table not in tables:
                    tables[table] = None
                if join_table not in tables:
                    tables[join_table] = None

        join.set("using", None)
        join.set("on", exp.and_(*conditions, copy=False))

    if column_tables:
        for column in scope.columns:
            if not column.table and column.name in column_tables:
                tables = column_tables[column.name]
                coalesce_args = [exp.column(column.name, table=table) for table in tables]
                replacement: exp.Expression = exp.func("coalesce", *coalesce_args)

                if isinstance(column.parent, exp.Select):
                    # Ensure the USING column keeps its name if it's projected
                    replacement = alias(replacement, alias=column.name, copy=False)
                elif isinstance(column.parent, exp.Struct):
                    # Ensure the USING column keeps its name if it's an anonymous STRUCT field
                    replacement = exp.PropertyEQ(
                        this=exp.to_identifier(column.name), expression=replacement
                    )

                scope.replace(column, replacement)

    return column_tables


def _expand_alias_refs(
    scope: Scope, resolver: Resolver, dialect: Dialect, expand_only_groupby: bool = False
) -> None:
    """
    Expand references to aliases.
    Example:
        SELECT y.foo AS bar, bar * 2 AS baz FROM y
     => SELECT y.foo AS bar, y.foo * 2 AS baz FROM y
    """
    expression = scope.expression

    if not isinstance(expression, exp.Select) or dialect.DISABLES_ALIAS_REF_EXPANSION:
        return

    alias_to_expression: t.Dict[str, t.Tuple[exp.Expression, int]] = {}
    projections = {s.alias_or_name for s in expression.selects}
    replaced = False

    def replace_columns(
        node: t.Optional[exp.Expression], resolve_table: bool = False, literal_index: bool = False
    ) -> None:
        nonlocal replaced
        is_group_by = isinstance(node, exp.Group)
        is_having = isinstance(node, exp.Having)
        if not node or (expand_only_groupby and not is_group_by):
            return

        for column in walk_in_scope(node, prune=lambda node: node.is_star):
            if not isinstance(column, exp.Column):
                continue

            # BigQuery's GROUP BY allows alias expansion only for standalone names, e.g:
            #   SELECT FUNC(col) AS col FROM t GROUP BY col --> Can be expanded
            #   SELECT FUNC(col) AS col FROM t GROUP BY FUNC(col)  --> Shouldn't be expanded, will result to FUNC(FUNC(col))
            # This not required for the HAVING clause as it can evaluate expressions using both the alias & the table columns
            if expand_only_groupby and is_group_by and column.parent is not node:
                continue

            skip_replace = False
            table = resolver.get_table(column.name) if resolve_table and not column.table else None
            alias_expr, i = alias_to_expression.get(column.name, (None, 1))

            if alias_expr:
                skip_replace = bool(
                    alias_expr.find(exp.AggFunc)
                    and column.find_ancestor(exp.AggFunc)
                    and not isinstance(column.find_ancestor(exp.Window, exp.Select), exp.Window)
                )

                # BigQuery's having clause gets confused if an alias matches a source.
                # SELECT x.a, max(x.b) as x FROM x GROUP BY 1 HAVING x > 1;
                # If "HAVING x" is expanded to "HAVING max(x.b)", BQ would blindly replace the "x" reference with the projection MAX(x.b)
                # i.e HAVING MAX(MAX(x.b).b), resulting in the error: "Aggregations of aggregations are not allowed"
                if is_having and dialect.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES:
                    skip_replace = skip_replace or any(
                        node.parts[0].name in projections
                        for node in alias_expr.find_all(exp.Column)
                    )
            elif dialect.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES and (is_group_by or is_having):
                column_table = table.name if table else column.table
                if column_table in projections:
                    # BigQuery's GROUP BY and HAVING clauses get confused if the column name
                    # matches a source name and a projection. For instance:
                    # SELECT id, ARRAY_AGG(col) AS custom_fields FROM custom_fields GROUP BY id HAVING id >= 1
                    # We should not qualify "id" with "custom_fields" in either clause, since the aggregation shadows the actual table
                    # and we'd get the error: "Column custom_fields contains an aggregation function, which is not allowed in GROUP BY clause"
                    column.replace(exp.to_identifier(column.name))
                    replaced = True
                    return

            if table and (not alias_expr or skip_replace):
                column.set("table", table)
            elif not column.table and alias_expr and not skip_replace:
                if (isinstance(alias_expr, exp.Literal) or alias_expr.is_number) and (
                    literal_index or resolve_table
                ):
                    if literal_index:
                        column.replace(exp.Literal.number(i))
                        replaced = True
                else:
                    replaced = True
                    column = column.replace(exp.paren(alias_expr))
                    simplified = simplify_parens(column, dialect)
                    if simplified is not column:
                        column.replace(simplified)

    for i, projection in enumerate(expression.selects):
        replace_columns(projection)
        if isinstance(projection, exp.Alias):
            alias_to_expression[projection.alias] = (projection.this, i + 1)

    parent_scope = scope
    on_right_sub_tree = False
    while parent_scope and not parent_scope.is_cte:
        if parent_scope.is_union:
            on_right_sub_tree = parent_scope.parent.expression.right is parent_scope.expression
        parent_scope = parent_scope.parent

    # We shouldn't expand aliases if they match the recursive CTE's columns
    # and we are in the recursive part (right sub tree) of the CTE
    if parent_scope and on_right_sub_tree:
        cte = parent_scope.expression.parent
        if cte.find_ancestor(exp.With).recursive:
            for recursive_cte_column in cte.args["alias"].columns or cte.this.selects:
                alias_to_expression.pop(recursive_cte_column.output_name, None)

    replace_columns(expression.args.get("where"))
    replace_columns(expression.args.get("group"), literal_index=True)
    replace_columns(expression.args.get("having"), resolve_table=True)
    replace_columns(expression.args.get("qualify"), resolve_table=True)

    if dialect.SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS:
        for join in expression.args.get("joins") or []:
            replace_columns(join)

    if replaced:
        scope.clear_cache()


def _expand_group_by(scope: Scope, dialect: Dialect) -> None:
    expression = scope.expression
    group = expression.args.get("group")
    if not group:
        return

    group.set("expressions", _expand_positional_references(scope, group.expressions, dialect))
    expression.set("group", group)


def _expand_order_by_and_distinct_on(scope: Scope, resolver: Resolver) -> None:
    for modifier_key in ("order", "distinct"):
        modifier = scope.expression.args.get(modifier_key)
        if isinstance(modifier, exp.Distinct):
            modifier = modifier.args.get("on")

        if not isinstance(modifier, exp.Expression):
            continue

        modifier_expressions = modifier.expressions
        if modifier_key == "order":
            modifier_expressions = [ordered.this for ordered in modifier_expressions]

        for original, expanded in zip(
            modifier_expressions,
            _expand_positional_references(
                scope, modifier_expressions, resolver.dialect, alias=True
            ),
        ):
            for agg in original.find_all(exp.AggFunc):
                for col in agg.find_all(exp.Column):
                    if not col.table:
                        col.set("table", resolver.get_table(col.name))

            original.replace(expanded)

        if scope.expression.args.get("group"):
            selects = {s.this: exp.column(s.alias_or_name) for s in scope.expression.selects}

            for expression in modifier_expressions:
                expression.replace(
                    exp.to_identifier(_select_by_pos(scope, expression).alias)
                    if expression.is_int
                    else selects.get(expression, expression)
                )


def _expand_positional_references(
    scope: Scope, expressions: t.Iterable[exp.Expression], dialect: Dialect, alias: bool = False
) -> t.List[exp.Expression]:
    new_nodes: t.List[exp.Expression] = []
    ambiguous_projections = None

    for node in expressions:
        if node.is_int:
            select = _select_by_pos(scope, t.cast(exp.Literal, node))

            if alias:
                new_nodes.append(exp.column(select.args["alias"].copy()))
            else:
                select = select.this

                if dialect.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES:
                    if ambiguous_projections is None:
                        # When a projection name is also a source name and it is referenced in the
                        # GROUP BY clause, BQ can't understand what the identifier corresponds to
                        ambiguous_projections = {
                            s.alias_or_name
                            for s in scope.expression.selects
                            if s.alias_or_name in scope.selected_sources
                        }

                    ambiguous = any(
                        column.parts[0].name in ambiguous_projections
                        for column in select.find_all(exp.Column)
                    )
                else:
                    ambiguous = False

                if (
                    isinstance(select, exp.CONSTANTS)
                    or select.is_number
                    or select.find(exp.Explode, exp.Unnest)
                    or ambiguous
                ):
                    new_nodes.append(node)
                else:
                    new_nodes.append(select.copy())
        else:
            new_nodes.append(node)

    return new_nodes


def _select_by_pos(scope: Scope, node: exp.Literal) -> exp.Alias:
    try:
        return scope.expression.selects[int(node.this) - 1].assert_is(exp.Alias)
    except IndexError:
        raise OptimizeError(f"Unknown output column: {node.name}")


def _convert_columns_to_dots(scope: Scope, resolver: Resolver) -> None:
    """
    Converts `Column` instances that represent STRUCT or JSON field lookup into chained `Dots`.

    These lookups may be parsed as columns (e.g. "col"."field"."field2"), but they need to be
    normalized to `Dot(Dot(...(<table>.<column>, field1), field2, ...))` to be qualified properly.
    """
    converted = False
    for column in itertools.chain(scope.columns, scope.stars):
        if isinstance(column, exp.Dot):
            continue

        column_table: t.Optional[str | exp.Identifier] = column.table
        dot_parts = column.meta.pop("dot_parts", [])
        if (
            column_table
            and column_table not in scope.sources
            and (
                not scope.parent
                or column_table not in scope.parent.sources
                or not scope.is_correlated_subquery
            )
        ):
            root, *parts = column.parts

            if root.name in scope.sources:
                # The struct is already qualified, but we still need to change the AST
                column_table = root
                root, *parts = parts
                was_qualified = True
            else:
                column_table = resolver.get_table(root.name)
                was_qualified = False

            if column_table:
                converted = True
                new_column = exp.column(root, table=column_table)

                if dot_parts:
                    # Remove the actual column parts from the rest of dot parts
                    new_column.meta["dot_parts"] = dot_parts[2 if was_qualified else 1 :]

                column.replace(exp.Dot.build([new_column, *parts]))

    if converted:
        # We want to re-aggregate the converted columns, otherwise they'd be skipped in
        # a `for column in scope.columns` iteration, even though they shouldn't be
        scope.clear_cache()


def _qualify_columns(
    scope: Scope,
    resolver: Resolver,
    allow_partial_qualification: bool,
) -> None:
    """Disambiguate columns, ensuring each column specifies a source"""
    for column in scope.columns:
        column_table = column.table
        column_name = column.name

        if column_table and column_table in scope.sources:
            source_columns = resolver.get_source_columns(column_table)
            if (
                not allow_partial_qualification
                and source_columns
                and column_name not in source_columns
                and "*" not in source_columns
            ):
                raise OptimizeError(f"Unknown column: {column_name}")

        if not column_table:
            if scope.pivots and not column.find_ancestor(exp.Pivot):
                # If the column is under the Pivot expression, we need to qualify it
                # using the name of the pivoted source instead of the pivot's alias
                column.set("table", exp.to_identifier(scope.pivots[0].alias))
                continue

            # column_table can be a '' because bigquery unnest has no table alias
            column_table = resolver.get_table(column)

            if column_table:
                column.set("table", column_table)
            elif (
                resolver.dialect.TABLES_REFERENCEABLE_AS_COLUMNS
                and len(column.parts) == 1
                and column_name in scope.selected_sources
            ):
                # BigQuery and Postgres allow tables to be referenced as columns, treating them as structs/records
                scope.replace(column, exp.TableColumn(this=column.this))

    for pivot in scope.pivots:
        for column in pivot.find_all(exp.Column):
            if not column.table and column.name in resolver.all_columns:
                column_table = resolver.get_table(column.name)
                if column_table:
                    column.set("table", column_table)


def _expand_struct_stars_no_parens(
    expression: exp.Dot,
) -> t.List[exp.Alias]:
    """[BigQuery] Expand/Flatten foo.bar.* where bar is a struct column"""

    dot_column = expression.find(exp.Column)
    if not isinstance(dot_column, exp.Column) or not dot_column.is_type(exp.DataType.Type.STRUCT):
        return []

    # All nested struct values are ColumnDefs, so normalize the first exp.Column in one
    dot_column = dot_column.copy()
    starting_struct = exp.ColumnDef(this=dot_column.this, kind=dot_column.type)

    # First part is the table name and last part is the star so they can be dropped
    dot_parts = expression.parts[1:-1]

    # If we're expanding a nested struct eg. t.c.f1.f2.* find the last struct (f2 in this case)
    for part in dot_parts[1:]:
        for field in t.cast(exp.DataType, starting_struct.kind).expressions:
            # Unable to expand star unless all fields are named
            if not isinstance(field.this, exp.Identifier):
                return []

            if field.name == part.name and field.kind.is_type(exp.DataType.Type.STRUCT):
                starting_struct = field
                break
        else:
            # There is no matching field in the struct
            return []

    taken_names = set()
    new_selections = []

    for field in t.cast(exp.DataType, starting_struct.kind).expressions:
        name = field.name

        # Ambiguous or anonymous fields can't be expanded
        if name in taken_names or not isinstance(field.this, exp.Identifier):
            return []

        taken_names.add(name)

        this = field.this.copy()
        root, *parts = [part.copy() for part in itertools.chain(dot_parts, [this])]
        new_column = exp.column(
            t.cast(exp.Identifier, root),
            table=dot_column.args.get("table"),
            fields=t.cast(t.List[exp.Identifier], parts),
        )
        new_selections.append(alias(new_column, this, copy=False))

    return new_selections


def _expand_struct_stars_with_parens(expression: exp.Dot) -> t.List[exp.Alias]:
    """[RisingWave] Expand/Flatten (<exp>.bar).*, where bar is a struct column"""

    # it is not (<sub_exp>).* pattern, which means we can't expand
    if not isinstance(expression.this, exp.Paren):
        return []

    # find column definition to get data-type
    dot_column = expression.find(exp.Column)
    if not isinstance(dot_column, exp.Column) or not dot_column.is_type(exp.DataType.Type.STRUCT):
        return []

    parent = dot_column.parent
    starting_struct = dot_column.type

    # walk up AST and down into struct definition in sync
    while parent is not None:
        if isinstance(parent, exp.Paren):
            parent = parent.parent
            continue

        # if parent is not a dot, then something is wrong
        if not isinstance(parent, exp.Dot):
            return []

        # if the rhs of the dot is star we are done
        rhs = parent.right
        if isinstance(rhs, exp.Star):
            break

        # if it is not identifier, then something is wrong
        if not isinstance(rhs, exp.Identifier):
            return []

        # Check if current rhs identifier is in struct
        matched = False
        for struct_field_def in t.cast(exp.DataType, starting_struct).expressions:
            if struct_field_def.name == rhs.name:
                matched = True
                starting_struct = struct_field_def.kind  # update struct
                break

        if not matched:
            return []

        parent = parent.parent

    # build new aliases to expand star
    new_selections = []

    # fetch the outermost parentheses for new aliaes
    outer_paren = expression.this

    for struct_field_def in t.cast(exp.DataType, starting_struct).expressions:
        new_identifier = struct_field_def.this.copy()
        new_dot = exp.Dot.build([outer_paren.copy(), new_identifier])
        new_alias = alias(new_dot, new_identifier, copy=False)
        new_selections.append(new_alias)

    return new_selections


def _expand_stars(
    scope: Scope,
    resolver: Resolver,
    using_column_tables: t.Dict[str, t.Any],
    pseudocolumns: t.Set[str],
    annotator: TypeAnnotator,
) -> None:
    """Expand stars to lists of column selections"""

    new_selections: t.List[exp.Expression] = []
    except_columns: t.Dict[int, t.Set[str]] = {}
    replace_columns: t.Dict[int, t.Dict[str, exp.Alias]] = {}
    rename_columns: t.Dict[int, t.Dict[str, str]] = {}

    coalesced_columns = set()
    dialect = resolver.dialect

    pivot_output_columns = None
    pivot_exclude_columns: t.Set[str] = set()

    pivot = t.cast(t.Optional[exp.Pivot], seq_get(scope.pivots, 0))
    if isinstance(pivot, exp.Pivot) and not pivot.alias_column_names:
        if pivot.unpivot:
            pivot_output_columns = [c.output_name for c in _unpivot_columns(pivot)]

            for field in pivot.fields:
                if isinstance(field, exp.In):
                    pivot_exclude_columns.update(
                        c.output_name for e in field.expressions for c in e.find_all(exp.Column)
                    )

        else:
            pivot_exclude_columns = set(c.output_name for c in pivot.find_all(exp.Column))

            pivot_output_columns = [c.output_name for c in pivot.args.get("columns", [])]
            if not pivot_output_columns:
                pivot_output_columns = [c.alias_or_name for c in pivot.expressions]

    if dialect.SUPPORTS_STRUCT_STAR_EXPANSION and any(
        isinstance(col, exp.Dot) for col in scope.stars
    ):
        # Found struct expansion, annotate scope ahead of time
        annotator.annotate_scope(scope)

    for expression in scope.expression.selects:
        tables = []
        if isinstance(expression, exp.Star):
            tables.extend(scope.selected_sources)
            _add_except_columns(expression, tables, except_columns)
            _add_replace_columns(expression, tables, replace_columns)
            _add_rename_columns(expression, tables, rename_columns)
        elif expression.is_star:
            if not isinstance(expression, exp.Dot):
                tables.append(expression.table)
                _add_except_columns(expression.this, tables, except_columns)
                _add_replace_columns(expression.this, tables, replace_columns)
                _add_rename_columns(expression.this, tables, rename_columns)
            elif (
                dialect.SUPPORTS_STRUCT_STAR_EXPANSION
                and not dialect.REQUIRES_PARENTHESIZED_STRUCT_ACCESS
            ):
                struct_fields = _expand_struct_stars_no_parens(expression)
                if struct_fields:
                    new_selections.extend(struct_fields)
                    continue
            elif dialect.REQUIRES_PARENTHESIZED_STRUCT_ACCESS:
                struct_fields = _expand_struct_stars_with_parens(expression)
                if struct_fields:
                    new_selections.extend(struct_fields)
                    continue

        if not tables:
            new_selections.append(expression)
            continue

        for table in tables:
            if table not in scope.sources:
                raise OptimizeError(f"Unknown table: {table}")

            columns = resolver.get_source_columns(table, only_visible=True)
            columns = columns or scope.outer_columns

            if pseudocolumns and dialect.EXCLUDES_PSEUDOCOLUMNS_FROM_STAR:
                columns = [name for name in columns if name.upper() not in pseudocolumns]

            if not columns or "*" in columns:
                return

            table_id = id(table)
            columns_to_exclude = except_columns.get(table_id) or set()
            renamed_columns = rename_columns.get(table_id, {})
            replaced_columns = replace_columns.get(table_id, {})

            if pivot:
                if pivot_output_columns and pivot_exclude_columns:
                    pivot_columns = [c for c in columns if c not in pivot_exclude_columns]
                    pivot_columns.extend(pivot_output_columns)
                else:
                    pivot_columns = pivot.alias_column_names

                if pivot_columns:
                    new_selections.extend(
                        alias(exp.column(name, table=pivot.alias), name, copy=False)
                        for name in pivot_columns
                        if name not in columns_to_exclude
                    )
                    continue

            for name in columns:
                if name in columns_to_exclude or name in coalesced_columns:
                    continue
                if name in using_column_tables and table in using_column_tables[name]:
                    coalesced_columns.add(name)
                    tables = using_column_tables[name]
                    coalesce_args = [exp.column(name, table=table) for table in tables]

                    new_selections.append(
                        alias(exp.func("coalesce", *coalesce_args), alias=name, copy=False)
                    )
                else:
                    alias_ = renamed_columns.get(name, name)
                    selection_expr = replaced_columns.get(name) or exp.column(name, table=table)
                    new_selections.append(
                        alias(selection_expr, alias_, copy=False)
                        if alias_ != name
                        else selection_expr
                    )

    # Ensures we don't overwrite the initial selections with an empty list
    if new_selections and isinstance(scope.expression, exp.Select):
        scope.expression.set("expressions", new_selections)


def _add_except_columns(
    expression: exp.Expression, tables, except_columns: t.Dict[int, t.Set[str]]
) -> None:
    except_ = expression.args.get("except_")

    if not except_:
        return

    columns = {e.name for e in except_}

    for table in tables:
        except_columns[id(table)] = columns


def _add_rename_columns(
    expression: exp.Expression, tables, rename_columns: t.Dict[int, t.Dict[str, str]]
) -> None:
    rename = expression.args.get("rename")

    if not rename:
        return

    columns = {e.this.name: e.alias for e in rename}

    for table in tables:
        rename_columns[id(table)] = columns


def _add_replace_columns(
    expression: exp.Expression, tables, replace_columns: t.Dict[int, t.Dict[str, exp.Alias]]
) -> None:
    replace = expression.args.get("replace")

    if not replace:
        return

    columns = {e.alias: e for e in replace}

    for table in tables:
        replace_columns[id(table)] = columns


def qualify_outputs(scope_or_expression: Scope | exp.Expression) -> None:
    """Ensure all output columns are aliased"""
    if isinstance(scope_or_expression, exp.Expression):
        scope = build_scope(scope_or_expression)
        if not isinstance(scope, Scope):
            return
    else:
        scope = scope_or_expression

    new_selections = []
    for i, (selection, aliased_column) in enumerate(
        itertools.zip_longest(scope.expression.selects, scope.outer_columns)
    ):
        if selection is None or isinstance(selection, exp.QueryTransform):
            break

        if isinstance(selection, exp.Subquery):
            if not selection.output_name:
                selection.set("alias", exp.TableAlias(this=exp.to_identifier(f"_col_{i}")))
        elif not isinstance(selection, (exp.Alias, exp.Aliases)) and not selection.is_star:
            selection = alias(
                selection,
                alias=selection.output_name or f"_col_{i}",
                copy=False,
            )
        if aliased_column:
            selection.set("alias", exp.to_identifier(aliased_column))

        new_selections.append(selection)

    if new_selections and isinstance(scope.expression, exp.Select):
        scope.expression.set("expressions", new_selections)


def quote_identifiers(expression: E, dialect: DialectType = None, identify: bool = True) -> E:
    """Makes sure all identifiers that need to be quoted are quoted."""
    return expression.transform(
        Dialect.get_or_raise(dialect).quote_identifier, identify=identify, copy=False
    )  # type: ignore


def pushdown_cte_alias_columns(scope: Scope) -> None:
    """
    Pushes down the CTE alias columns into the projection,

    This step is useful in Snowflake where the CTE alias columns can be referenced in the HAVING.

    Args:
        scope: Scope to find ctes to pushdown aliases.
    """
    for cte in scope.ctes:
        if cte.alias_column_names and isinstance(cte.this, exp.Select):
            new_expressions = []
            for _alias, projection in zip(cte.alias_column_names, cte.this.expressions):
                if isinstance(projection, exp.Alias):
                    projection.set("alias", exp.to_identifier(_alias))
                else:
                    projection = alias(projection, alias=_alias)
                new_expressions.append(projection)
            cte.this.set("expressions", new_expressions)
