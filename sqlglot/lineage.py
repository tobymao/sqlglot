from __future__ import annotations

import json
import logging
import typing as t
from dataclasses import dataclass, field

from sqlglot import Schema, exp, maybe_parse
from sqlglot.errors import SqlglotError
from sqlglot.optimizer import Scope, build_scope, find_all_in_scope, normalize_identifiers, qualify
from sqlglot.optimizer.scope import ScopeType

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from collections.abc import Iterator, Mapping, Sequence

logger = logging.getLogger("sqlglot")


@dataclass(frozen=True)
class Node:
    name: str
    expression: exp.Expr
    source: exp.Expr
    downstream: list[Node] = field(default_factory=list)
    source_name: str = ""
    reference_node_name: str = ""

    def walk(self) -> Iterator[Node]:
        visited: set[int] = set()
        queue = [self]
        while queue:
            node = queue.pop()
            node_id = id(node)
            if node_id in visited:
                continue
            visited.add(node_id)
            yield node
            queue.extend(reversed(node.downstream))

    def to_html(self, dialect: DialectType = None, **opts) -> GraphHTML:
        nodes = {}
        edges = []

        for node in self.walk():
            if isinstance(node.expression, exp.Table):
                label = f"FROM {node.expression.this}"
                title = f"<pre>SELECT {node.name} FROM {node.expression.this}</pre>"
                group = 1
            else:
                label = node.expression.sql(pretty=True, dialect=dialect)
                source = node.source.transform(
                    lambda n: (
                        exp.Tag(this=n, prefix="<b>", postfix="</b>") if n is node.expression else n
                    ),
                    copy=False,
                ).sql(pretty=True, dialect=dialect)
                title = f"<pre>{source}</pre>"
                group = 0

            node_id = id(node)

            nodes[node_id] = {
                "id": node_id,
                "label": label,
                "title": title,
                "group": group,
            }

            for d in node.downstream:
                edges.append({"from": node_id, "to": id(d)})
        return GraphHTML(nodes, edges, **opts)


def lineage(
    column: str | exp.Column,
    sql: str | exp.Expr,
    schema: t.Optional[dict | Schema] = None,
    sources: t.Optional[Mapping[str, str | exp.Query]] = None,
    dialect: DialectType = None,
    scope: t.Optional[Scope] = None,
    trim_selects: bool = True,
    copy: bool = True,
    **kwargs,
) -> Node:
    """Build the lineage graph for a column of a SQL query.

    Args:
        column: The column to build the lineage for.
        sql: The SQL string or expression.
        schema: The schema of tables.
        sources: A mapping of queries which will be used to continue building lineage.
        dialect: The dialect of input SQL.
        scope: A pre-created scope to use instead.
        trim_selects: Whether to clean up selects by trimming to only relevant columns.
        copy: Whether to copy the Expr arguments.
        **kwargs: Qualification optimizer kwargs.

    Returns:
        A lineage node.
    """

    expression = maybe_parse(sql, copy=copy, dialect=dialect)
    column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name

    if sources:
        expression = exp.expand(
            expression,
            {
                k: t.cast(exp.Query, maybe_parse(v, copy=copy, dialect=dialect))
                for k, v in sources.items()
            },
            dialect=dialect,
            copy=copy,
        )

    if not scope:
        expression = qualify.qualify(
            expression,
            dialect=dialect,
            schema=schema,
            **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
        )

        scope = build_scope(expression)

    if not scope:
        raise SqlglotError("Cannot build lineage, sql must be SELECT")

    selectable = scope.expression
    if not isinstance(selectable, exp.Selectable) or not any(
        select.alias_or_name == column for select in selectable.selects
    ):
        raise SqlglotError(f"Cannot find column '{column}' in query.")

    return to_node(column, scope, dialect, trim_selects=trim_selects, _cache={})


def to_node(
    column: str | int,
    scope: Scope,
    dialect: DialectType,
    scope_name: t.Optional[str] = None,
    upstream: t.Optional[Node] = None,
    source_name: t.Optional[str] = None,
    reference_node_name: t.Optional[str] = None,
    trim_selects: bool = True,
    _cache: t.Optional[t.Dict[t.Tuple, Node]] = None,
) -> Node:
    cache_key = (column, id(scope), scope_name, source_name, reference_node_name)

    if _cache is not None and cache_key in _cache:
        cached_node = _cache[cache_key]
        if upstream:
            upstream.downstream.append(cached_node)
        return cached_node

    # Find the specific select clause that is the source of the column we want.
    # This can either be a specific, named select or a generic `*` clause.
    selectable = t.cast(exp.Selectable, scope.expression)
    select = (
        selectable.selects[column]
        if isinstance(column, int)
        else next(
            (select for select in selectable.selects if select.alias_or_name == column),
            exp.Star() if selectable.is_star else scope.expression,
        )
    )

    if isinstance(scope.expression, exp.Subquery):
        for inner_scope in scope.subquery_scopes:
            result = to_node(
                column,
                scope=inner_scope,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
                _cache=_cache,
            )
            if _cache is not None:
                _cache[cache_key] = result
            return result
    if isinstance(scope.expression, exp.SetOperation):
        name = type(scope.expression).__name__.upper()
        upstream = upstream or Node(name=name, source=scope.expression, expression=select)

        index = (
            column
            if isinstance(column, int)
            else next(
                (
                    i
                    for i, select in enumerate(selectable.selects)
                    if select.alias_or_name == column or select.is_star
                ),
                -1,  # mypy will not allow a None here, but a negative index should never be returned
            )
        )

        if index == -1:
            raise ValueError(f"Could not find {column} in {scope.expression}")

        for s in scope.union_scopes:
            to_node(
                index,
                scope=s,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
                _cache=_cache,
            )

        if _cache is not None:
            _cache[cache_key] = upstream
        return upstream

    if trim_selects and isinstance(scope.expression, exp.Select):
        # For better ergonomics in our node labels, replace the full select with
        # a version that has only the column we care about.
        #   "x", SELECT x, y FROM foo
        #     => "x", SELECT x FROM foo
        source: exp.Expr = scope.expression.select(select, append=False)
    else:
        source = scope.expression

    # Create the node for this step in the lineage chain, and attach it to the previous one.
    node = Node(
        name=f"{scope_name}.{column}" if scope_name else str(column),
        source=source,
        expression=select,
        source_name=source_name or "",
        reference_node_name=reference_node_name or "",
    )

    if upstream:
        upstream.downstream.append(node)

    subquery_scopes = {
        id(subquery_scope.expression): subquery_scope for subquery_scope in scope.subquery_scopes
    }

    for subquery in find_all_in_scope(select, *exp.UNWRAPPED_QUERIES):
        subquery_scope: t.Optional[Scope] = subquery_scopes.get(id(subquery))
        if not subquery_scope:
            logger.warning(f"Unknown subquery scope: {subquery.sql(dialect=dialect)}")
            continue

        for name in subquery.named_selects:
            to_node(
                name,
                scope=subquery_scope,
                dialect=dialect,
                upstream=node,
                trim_selects=trim_selects,
                _cache=_cache,
            )

    # if the select is a star add all scope sources as downstreams
    if isinstance(select, exp.Star):
        for src in scope.sources.values():
            src_expr = src.expression if isinstance(src, Scope) else src
            node.downstream.append(
                Node(name=select.sql(comments=False), source=src_expr, expression=src_expr)
            )

    # Find all columns that went into creating this one to list their lineage nodes.
    source_columns = set(find_all_in_scope(select, exp.Column))

    # If the source is a UDTF find columns used in the UDTF to generate the table
    if isinstance(source, exp.UDTF):
        source_columns |= set(source.find_all(exp.Column))
        derived_tables: Sequence[exp.Expr] = [
            src.expression.parent
            for src in scope.sources.values()
            if isinstance(src, Scope) and src.is_derived_table and src.expression.parent
        ]
    else:
        derived_tables = scope.derived_tables

    source_names = {
        dt.alias: dt.comments[0].split()[1]
        for dt in derived_tables
        if dt.comments and dt.comments[0].startswith("source: ")
    }

    pivots = scope.pivots
    pivot = pivots[0] if len(pivots) == 1 and not pivots[0].unpivot else None
    if pivot:
        # For each aggregation function, the pivot creates a new column for each field in category
        # combined with the aggfunc. So the columns parsed have this order: cat_a_value_sum, cat_a,
        # b_value_sum, b. Because of this step wise manner the aggfunc 'sum(value) as value_sum'
        # belongs to the column indices 0, 2, and the aggfunc 'max(price)' without an alias belongs
        # to the column indices 1, 3. Here, only the columns used in the aggregations are of interest
        # in the lineage, so lookup the pivot column name by index and map that with the columns used
        # in the aggregation.
        #
        # Example: PIVOT (SUM(value) AS value_sum, MAX(price)) FOR category IN ('a' AS cat_a, 'b')
        pivot_columns = pivot.args["columns"]
        pivot_aggs_count = len(pivot.expressions)

        pivot_column_mapping = {}
        for i, agg in enumerate(pivot.expressions):
            agg_cols = list(agg.find_all(exp.Column))
            for col_index in range(i, len(pivot_columns), pivot_aggs_count):
                pivot_column_mapping[pivot_columns[col_index].name] = agg_cols

    for c in source_columns:
        table = c.table
        col_source: t.Optional[exp.Table | Scope] = scope.sources.get(table)

        if isinstance(col_source, Scope):
            reference_node_name = None
            if col_source.scope_type == ScopeType.DERIVED_TABLE and table not in source_names:
                reference_node_name = table
            elif col_source.scope_type == ScopeType.CTE:
                selected_node, _ = scope.selected_sources.get(table, (None, None))
                reference_node_name = selected_node.name if selected_node else None

            # The table itself came from a more specific scope. Recurse into that one using the unaliased column name.
            to_node(
                c.name,
                scope=col_source,
                dialect=dialect,
                scope_name=table,
                upstream=node,
                source_name=source_names.get(table) or source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
                _cache=_cache,
            )
        elif pivot and pivot.alias_or_name == c.table:
            downstream_columns = []

            column_name = c.name
            if any(column_name == pivot_column.name for pivot_column in pivot_columns):
                downstream_columns.extend(pivot_column_mapping[column_name])
            else:
                # The column is not in the pivot, so it must be an implicit column of the
                # pivoted source -- adapt column to be from the implicit pivoted source.
                pivot_parent = pivot.parent
                downstream_columns.append(
                    exp.column(c.this, table=pivot_parent.alias_or_name if pivot_parent else "")
                )

            for downstream_column in downstream_columns:
                table = downstream_column.table
                col_source = scope.sources.get(table)
                if isinstance(col_source, Scope):
                    to_node(
                        downstream_column.name,
                        scope=col_source,
                        scope_name=table,
                        dialect=dialect,
                        upstream=node,
                        source_name=source_names.get(table) or source_name,
                        reference_node_name=reference_node_name,
                        trim_selects=trim_selects,
                        _cache=_cache,
                    )
                else:
                    col_expr = col_source or exp.Placeholder()
                    node.downstream.append(
                        Node(
                            name=downstream_column.sql(comments=False),
                            source=col_expr,
                            expression=col_expr,
                        )
                    )
        else:
            # The source is not a scope and the column is not in any pivot - we've reached the end
            # of the line. At this point, if a source is not found it means this column's lineage
            # is unknown. This can happen if the definition of a source used in a query is not
            # passed into the `sources` map.
            col_expr = col_source or exp.Placeholder()
            node.downstream.append(
                Node(name=c.sql(comments=False), source=col_expr, expression=col_expr)
            )

    if _cache is not None:
        _cache[cache_key] = node

    return node


class GraphHTML:
    """Node to HTML generator using vis.js.

    https://visjs.github.io/vis-network/docs/network/
    """

    def __init__(
        self, nodes: t.Dict, edges: t.List, imports: bool = True, options: t.Optional[t.Dict] = None
    ):
        self.imports = imports

        self.options = {
            "height": "500px",
            "width": "100%",
            "layout": {
                "hierarchical": {
                    "enabled": True,
                    "nodeSpacing": 200,
                    "sortMethod": "directed",
                },
            },
            "interaction": {
                "dragNodes": False,
                "selectable": False,
            },
            "physics": {
                "enabled": False,
            },
            "edges": {
                "arrows": "to",
            },
            "nodes": {
                "font": "20px monaco",
                "shape": "box",
                "widthConstraint": {
                    "maximum": 300,
                },
            },
            **(options or {}),
        }

        self.nodes = nodes
        self.edges = edges

    def __str__(self):
        nodes = json.dumps(list(self.nodes.values()))
        edges = json.dumps(self.edges)
        options = json.dumps(self.options)
        imports = (
            """<script type="text/javascript" src="https://unpkg.com/vis-data@latest/peer/umd/vis-data.min.js"></script>
  <script type="text/javascript" src="https://unpkg.com/vis-network@latest/peer/umd/vis-network.min.js"></script>
  <link rel="stylesheet" type="text/css" href="https://unpkg.com/vis-network/styles/vis-network.min.css" />"""
            if self.imports
            else ""
        )

        return f"""<div>
  <div id="sqlglot-lineage"></div>
  {imports}
  <script type="text/javascript">
    var nodes = new vis.DataSet({nodes})
    nodes.forEach(row => row["title"] = new DOMParser().parseFromString(row["title"], "text/html").body.childNodes[0])

    new vis.Network(
        document.getElementById("sqlglot-lineage"),
        {{
            nodes: nodes,
            edges: new vis.DataSet({edges})
        }},
        {options},
    )
  </script>
</div>"""

    def _repr_html_(self) -> str:
        return self.__str__()
