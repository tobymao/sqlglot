from __future__ import annotations

import json
import typing as t
from dataclasses import dataclass, field

from sqlglot import Schema, exp, maybe_parse
from sqlglot.optimizer import Scope, build_scope, optimize
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


@dataclass(frozen=True)
class Node:
    name: str
    expression: exp.Expression
    source: exp.Expression
    downstream: t.List[Node] = field(default_factory=list)

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
    rules: t.Sequence[t.Callable] = (qualify_tables, qualify_columns),
    dialect: DialectType = None,
) -> Node:
    """Build the lineage graph for a column of a SQL query.

    Args:
        column: The column to build the lineage for.
        sql: The SQL string or expression.
        schema: The schema of tables.
        sources: A mapping of queries which will be used to continue building lineage.
        rules: Optimizer rules to apply, by default only qualifying tables and columns.
        dialect: The dialect of input SQL.

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

    optimized = optimize(expression, schema=schema, rules=rules)
    scope = build_scope(optimized)
    tables: t.Dict[str, Node] = {}

    def to_node(
        column_name: str,
        scope: Scope,
        scope_name: t.Optional[str] = None,
        upstream: t.Optional[Node] = None,
    ) -> Node:
        if isinstance(scope.expression, exp.Union):
            for scope in scope.union_scopes:
                node = to_node(
                    column_name,
                    scope=scope,
                    scope_name=scope_name,
                    upstream=upstream,
                )
            return node

        select = next(select for select in scope.selects if select.alias_or_name == column_name)
        source = optimize(scope.expression.select(select, append=False), schema=schema, rules=rules)
        select = source.selects[0]

        node = Node(
            name=f"{scope_name}.{column_name}" if scope_name else column_name,
            source=source,
            expression=select,
        )

        if upstream:
            upstream.downstream.append(node)

        for c in set(select.find_all(exp.Column)):
            table = c.table
            source = scope.sources[table]

            if isinstance(source, Scope):
                to_node(
                    c.name,
                    scope=source,
                    scope_name=table,
                    upstream=node,
                )
            else:
                if table not in tables:
                    tables[table] = Node(name=table, source=source, expression=source)
                node.downstream.append(tables[table])

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
