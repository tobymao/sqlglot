import itertools
from collections import defaultdict

import sqlglot
import sqlglot.expressions as exp
from sqlglot.optimizer.scope import traverse_scope


class Plan:
    def __init__(self, expression):
        self.scope = traverse_scope(expression)[-1]
        self.expression = self.scope.expression
        self.root = Step.from_expression(self.expression, self.scope)
        self._dag = {}

    @property
    def dag(self):
        if not self._dag:
            dag = {}
            nodes = {self.root}

            while nodes:
                node = nodes.pop()
                dag[node] = set()
                for dep in node.dependencies:
                    dag[node].add(dep)
                    nodes.add(dep)
            self._dag = dag

        return self._dag

    @property
    def leaves(self):
        return (node for node, deps in self.dag.items() if not deps)

class Step:
    @classmethod
    def from_expression(cls, expression, scope, name=None):
        step = Scan(name)
        scope = scope.selected_sources[name] if name else scope
        from_ = expression.args.get("from")
        group = expression.args.get("group")
        expressions = expression.args["expressions"]

        if from_:
            from_ = from_.args["expressions"][0]
            alias = from_.alias

            if isinstance(from_, exp.Subquery):
                step.add_dependency(
                    Step.from_expression(
                        from_.this,
                        scope,
                        alias
                    ),
                )
                step.source = alias
            else:
                step.source = from_.this

        join = Join.from_expression(expression, scope, name)

        if join:
            for dependency in step.dependencies:
                join.add_dependency(dependency)
            step = join

        projections = []
        temporary = set()
        aggregations = []
        sequence = itertools.count()

        for e in expressions:
            agg = e.find(exp.AggFunc)

            if agg:
                aggregations.append(e)
                for operand in agg.unnest_operands():
                    if isinstance(operand, exp.Star):
                        continue
                    alias = f"_a_{next(sequence)}"
                    temporary.add(alias)
                    operand.replace(sqlglot.parse_one(alias))
                    projections.append(exp.alias_(operand, alias))
            else:
                projections.append(e)

        step.projections = projections

        where = expression.args.get("where")

        if where:
            step.filter = where.this

        if group:
            aggregate = Aggregate(name)

            aggregate.aggregations = aggregations
            aggregate.group = [
                sqlglot.parse_one(e.alias_or_name)
                for e in group.args["expressions"]
            ]
            aggregate.projections = [
                sqlglot.parse_one(e.alias_or_name)
                for e in projections
                if e.alias_or_name not in temporary
            ]

            aggregate.add_dependency(step)
            step = aggregate

            having = expression.args.get("having")

            if having:
                step.filter = having.this

        return step

    def __init__(self, name):
        self.name = name or "root"
        self.dependencies = set()
        self.dependents = set()
        self.projections = []
        self.filter = None

    def add_dependency(self, dependency):
        self.dependencies.add(dependency)
        dependency.dependents.add(self)

    def __repr__(self):
        return self.to_s()

    def to_s(self, level=0):
        indent = "".join(["  "] * level)
        nested = f"{indent}    "

        context = self._to_s(f"{nested}  ")

        if context:
            context = [f"{nested}Context:"] + context

        lines = [
            f"{indent}- {self.__class__.__name__}: {self.name}",
            *context,
            f"{nested}Projections:",
        ]

        for expression in self.projections:
            lines.append(f"{nested}  - {expression.sql()}")

        if self.filter:
            lines.append(f"{nested}Filter: {self.filter.sql()}")
        if self.dependencies:
            lines.append(f"{nested}Dependencies:")
            for dependency in self.dependencies:
                lines.append("  " + dependency.to_s(level + 1))

        return "\n".join(lines)

    def _to_s(self, indent):
        return []


class Scan(Step):
    def __init__(self, name):
        super().__init__(name)
        self.source = None

    def _to_s(self, indent):
        return [
            f"{indent}Source: {self.source}"
        ]


class Write(Step):
    pass


class Join(Step):
    @classmethod
    def from_expression(cls, expression, scope, name=None):
        joins = expression.args.get("joins")

        if not joins:
            return None

        step = Join(name)

        for join in joins:
            source = join.this
            alias = source.alias

            step.joins[alias] = {
                "kind": join.args["kind"],
                "on": join.args["on"],
            }

            step.add_dependency(
                Step.from_expression(
                    source.this,
                    scope,
                    alias,
                )
            )

        return step

    def __init__(self, name):
        super().__init__(name)
        self.joins = {}

    def _to_s(self, indent):
        lines = []
        for name, join in self.joins.items():
            lines.extend([
                f"{indent}{name}: {join['kind'] or 'INNER'}",
                f"{indent}On: {join['on'].sql()}",
            ])
        return lines


class Aggregate(Step):
    def __init__(self, name):
        super().__init__(name)
        self.aggregations = []
        self.group = []

    def _to_s(self, indent):
        lines = [f"{indent}Aggregations:"]

        for expression in self.aggregations:
            lines.append(f"{indent}  - {expression.sql()}")

        if self.group:
            lines.append(f"{indent}Group:")
        for expression in self.group:
            lines.append(f"{indent}  - {expression.sql()}")

        return lines


class Sort(Step):
    pass
