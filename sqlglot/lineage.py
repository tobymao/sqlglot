from __future__ import annotations

import json
import typing as t
from dataclasses import dataclass, field

from sqlglot import Schema, exp, maybe_parse
from sqlglot.errors import SqlglotError
from sqlglot.optimizer import Scope, build_scope, qualify

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


@dataclass(frozen=True)
class Node:
    name: str
    expression: exp.Expression
    source: exp.Expression
    downstream: t.List[Node] = field(default_factory=list)
    alias: str = ""

    def walk(self) -> t.Iterator[Node]:
        yield self

        for d in self.downstream:
            if isinstance(d, Node):
                yield from d.walk()
            else:
                yield d

    def to_html(self, **opts) -> LineageHTML:
        return LineageHTML(self, **opts)


def lineage(
    column: str | exp.Column,
    sql: str | exp.Expression,
    schema: t.Optional[t.Dict | Schema] = None,
    sources: t.Optional[t.Dict[str, str | exp.Subqueryable]] = None,
    dialect: DialectType = None,
    **kwargs,
) -> Node:
    """Build the lineage graph for a column of a SQL query.

    Args:
        column: The column to build the lineage for.
        sql: The SQL string or expression.
        schema: The schema of tables.
        sources: A mapping of queries which will be used to continue building lineage.
        dialect: The dialect of input SQL.
        **kwargs: Qualification optimizer kwargs.

    Returns:
        A lineage node.
    """

    expression = maybe_parse(sql, dialect=dialect)

    if sources:
        expression = exp.expand(
            expression,
            {
                k: t.cast(exp.Subqueryable, maybe_parse(v, dialect=dialect))
                for k, v in sources.items()
            },
        )

    qualified = qualify.qualify(
        expression,
        dialect=dialect,
        schema=schema,
        **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
    )

    scope = build_scope(qualified)

    if not scope:
        raise SqlglotError("Cannot build lineage, sql must be SELECT")

    def to_node(
        column: str | int,
        scope: Scope,
        scope_name: t.Optional[str] = None,
        upstream: t.Optional[Node] = None,
        alias: t.Optional[str] = None,
    ) -> Node:
        aliases = {
            dt.alias: dt.comments[0].split()[1]
            for dt in scope.derived_tables
            if dt.comments and dt.comments[0].startswith("source: ")
        }

        # Find the specific select clause that is the source of the column we want.
        # This can either be a specific, named select or a generic `*` clause.
        select = (
            scope.expression.selects[column]
            if isinstance(column, int)
            else next(
                (select for select in scope.expression.selects if select.alias_or_name == column),
                exp.Star() if scope.expression.is_star else None,
            )
        )

        if not select:
            raise ValueError(f"Could not find {column} in {scope.expression}")

        if isinstance(scope.expression, exp.Union):
            upstream = upstream or Node(name="UNION", source=scope.expression, expression=select)

            index = (
                column
                if isinstance(column, int)
                else next(
                    i
                    for i, select in enumerate(scope.expression.selects)
                    if select.alias_or_name == column
                )
            )

            for s in scope.union_scopes:
                to_node(index, scope=s, upstream=upstream)

            return upstream

        if isinstance(scope.expression, exp.Select):
            # For better ergonomics in our node labels, replace the full select with
            # a version that has only the column we care about.
            #   "x", SELECT x, y FROM foo
            #     => "x", SELECT x FROM foo
            source = t.cast(exp.Expression, scope.expression.select(select, append=False))
        else:
            source = scope.expression

        # Create the node for this step in the lineage chain, and attach it to the previous one.
        node = Node(
            name=f"{scope_name}.{column}" if scope_name else str(column),
            source=source,
            expression=select,
            alias=alias or "",
        )
        if upstream:
            upstream.downstream.append(node)

        # Find all columns that went into creating this one to list their lineage nodes.
        for c in set(select.find_all(exp.Column)):
            table = c.table
            source = scope.sources.get(table)

            if isinstance(source, Scope):
                # The table itself came from a more specific scope. Recurse into that one using the unaliased column name.
                to_node(
                    c.name, scope=source, scope_name=table, upstream=node, alias=aliases.get(table)
                )
            else:
                # The source is not a scope - we've reached the end of the line. At this point, if a source is not found
                # it means this column's lineage is unknown. This can happen if the definition of a source used in a query
                # is not passed into the `sources` map.
                source = source or exp.Placeholder()
                node.downstream.append(Node(name=c.sql(), source=source, expression=source))

        return node

    return to_node(column if isinstance(column, str) else column.name, scope)


class LineageHTML:
    """Node to HTML generator using vis.js.

    https://visjs.github.io/vis-network/docs/network/
    """

    def __init__(
        self,
        node: Node,
        dialect: DialectType = None,
        imports: bool = True,
        **opts: t.Any,
    ):
        self.node = node
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
            **opts,
        }

        self.nodes = {}
        self.edges = []

        for node in node.walk():
            if isinstance(node.expression, exp.Table):
                label = f"FROM {node.expression.this}"
                title = f"<pre>SELECT {node.name} FROM {node.expression.this}</pre>"
                group = 1
            else:
                label = node.expression.sql(pretty=True, dialect=dialect)
                source = node.source.transform(
                    lambda n: exp.Tag(this=n, prefix="<b>", postfix="</b>")
                    if n is node.expression
                    else n,
                    copy=False,
                ).sql(pretty=True, dialect=dialect)
                title = f"<pre>{source}</pre>"
                group = 0

            node_id = id(node)

            self.nodes[node_id] = {
                "id": node_id,
                "label": label,
                "title": title,
                "group": group,
            }

            for d in node.downstream:
                self.edges.append({"from": node_id, "to": id(d)})

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
