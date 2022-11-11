from __future__ import annotations

import itertools
import math
import typing as t

from sqlglot import alias, exp
from sqlglot.errors import UnsupportedError
from sqlglot.optimizer.eliminate_joins import join_condition


class Plan:
    def __init__(self, expression: exp.Expression) -> None:
        self.expression = expression
        self.root = Step.from_expression(self.expression)
        self._dag: t.Dict[Step, t.Set[Step]] = {}

    @property
    def dag(self) -> t.Dict[Step, t.Set[Step]]:
        if not self._dag:
            dag: t.Dict[Step, t.Set[Step]] = {}
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
    def leaves(self) -> t.Generator[Step, None, None]:
        return (node for node, deps in self.dag.items() if not deps)

    def __repr__(self) -> str:
        return f"Plan\n----\n{repr(self.root)}"


class Step:
    @classmethod
    def from_expression(
        cls, expression: exp.Expression, ctes: t.Optional[t.Dict[str, Step]] = None
    ) -> Step:
        """
        Builds a DAG of Steps from a SQL expression so that it's easier to execute in an engine.
        Note: the expression's tables and subqueries must be aliased for this method to work. For
        example, given the following expression:

        SELECT
          x.a,
          SUM(x.b)
        FROM x AS x
        JOIN y AS y
          ON x.a = y.a
        GROUP BY x.a

        the following DAG is produced (the expression IDs might differ per execution):

        - Aggregate: x (4347984624)
            Context:
              Aggregations:
                - SUM(x.b)
              Group:
                - x.a
            Projections:
              - x.a
              - "x".""
            Dependencies:
            - Join: x (4347985296)
              Context:
                y:
                On: x.a = y.a
              Projections:
              Dependencies:
              - Scan: x (4347983136)
                Context:
                  Source: x AS x
                Projections:
              - Scan: y (4343416624)
                Context:
                  Source: y AS y
                Projections:

        Args:
            expression: the expression to build the DAG from.
            ctes: a dictionary that maps CTEs to their corresponding Step DAG by name.

        Returns:
            A Step DAG corresponding to `expression`.
        """
        ctes = ctes or {}
        with_ = expression.args.get("with")

        # CTEs break the mold of scope and introduce themselves to all in the context.
        if with_:
            ctes = ctes.copy()
            for cte in with_.expressions:
                step = Step.from_expression(cte.this, ctes)
                step.name = cte.alias
                ctes[step.name] = step  # type: ignore

        from_ = expression.args.get("from")

        if from_:
            from_ = from_.expressions
            if len(from_) > 1:
                raise UnsupportedError(
                    "Multi-from statements are unsupported. Run it through the optimizer"
                )

            step = Scan.from_expression(from_[0], ctes)
        else:
            raise UnsupportedError("Static selects are unsupported.")

        joins = expression.args.get("joins")

        if joins:
            join = Join.from_joins(joins, ctes)
            join.name = step.name
            join.add_dependency(step)
            step = join

        projections = []  # final selects in this chain of steps representing a select
        operands = {}  # intermediate computations of agg funcs eg x + 1 in SUM(x + 1)
        aggregations = []
        sequence = itertools.count()

        for e in expression.expressions:
            aggregation = e.find(exp.AggFunc)

            if aggregation:
                projections.append(exp.column(e.alias_or_name, step.name, quoted=True))
                aggregations.append(e)
                for operand in aggregation.unnest_operands():
                    if isinstance(operand, exp.Column):
                        continue
                    if operand not in operands:
                        operands[operand] = f"_a_{next(sequence)}"
                    operand.replace(exp.column(operands[operand], quoted=True))
            else:
                projections.append(e)

        where = expression.args.get("where")

        if where:
            step.condition = where.this

        group = expression.args.get("group")

        if group:
            aggregate = Aggregate()
            aggregate.source = step.name
            aggregate.name = step.name
            aggregate.operands = tuple(
                alias(operand, alias_) for operand, alias_ in operands.items()
            )
            aggregate.aggregations = aggregations
            aggregate.group = group.expressions
            aggregate.add_dependency(step)
            step = aggregate

            having = expression.args.get("having")

            if having:
                step.condition = having.this

        order = expression.args.get("order")

        if order:
            sort = Sort()
            sort.name = step.name
            sort.key = order.expressions
            sort.add_dependency(step)
            step = sort

        step.projections = projections

        limit = expression.args.get("limit")

        if limit:
            step.limit = int(limit.text("expression"))

        return step

    def __init__(self) -> None:
        self.name: t.Optional[str] = None
        self.dependencies: t.Set[Step] = set()
        self.dependents: t.Set[Step] = set()
        self.projections: t.Sequence[exp.Expression] = []
        self.limit: float = math.inf
        self.condition: t.Optional[exp.Expression] = None

    def add_dependency(self, dependency: Step) -> None:
        self.dependencies.add(dependency)
        dependency.dependents.add(self)

    def __repr__(self) -> str:
        return self.to_s()

    def to_s(self, level: int = 0) -> str:
        indent = "  " * level
        nested = f"{indent}    "

        context = self._to_s(f"{nested}  ")

        if context:
            context = [f"{nested}Context:"] + context

        lines = [
            f"{indent}- {self.id}",
            *context,
            f"{nested}Projections:",
        ]

        for expression in self.projections:
            lines.append(f"{nested}  - {expression.sql()}")

        if self.condition:
            lines.append(f"{nested}Condition: {self.condition.sql()}")

        if self.dependencies:
            lines.append(f"{nested}Dependencies:")
            for dependency in self.dependencies:
                lines.append("  " + dependency.to_s(level + 1))

        return "\n".join(lines)

    @property
    def id(self) -> str:
        return f"{self.__class__.__name__}: {self.name} ({id(self)})"

    def _to_s(self, _indent: str) -> t.List[str]:
        return []


class Scan(Step):
    @classmethod
    def from_expression(
        cls, expression: exp.Expression, ctes: t.Optional[t.Dict[str, Step]] = None
    ) -> Step:
        table = expression
        alias_ = expression.alias

        if not alias_:
            raise UnsupportedError(
                "Tables/Subqueries must be aliased. Run it through the optimizer"
            )

        if isinstance(expression, exp.Subquery):
            table = expression.this
            step = Step.from_expression(table, ctes)
            step.name = alias_
            return step

        step = Scan()
        step.name = alias_
        step.source = expression  # type: ignore
        if ctes and table.name in ctes:
            step.add_dependency(ctes[table.name])

        return step

    def __init__(self) -> None:
        super().__init__()
        self.source = None

    def _to_s(self, indent: str) -> t.List[str]:
        return [f"{indent}Source: {self.source.sql()}"]  # type: ignore


class Write(Step):
    pass


class Join(Step):
    @classmethod
    def from_joins(
        cls, joins: t.Iterable[exp.Join], ctes: t.Optional[t.Dict[str, Step]] = None
    ) -> Step:
        step = Join()

        for join in joins:
            source_key, join_key, condition = join_condition(join)
            step.joins[join.this.alias_or_name] = {
                "side": join.side,
                "join_key": join_key,
                "source_key": source_key,
                "condition": condition,
            }

            step.add_dependency(Scan.from_expression(join.this, ctes))

        return step

    def __init__(self) -> None:
        super().__init__()
        self.joins: t.Dict[str, t.Dict[str, t.List[str] | exp.Expression]] = {}

    def _to_s(self, indent: str) -> t.List[str]:
        lines = []
        for name, join in self.joins.items():
            lines.append(f"{indent}{name}: {join['side']}")
            if join.get("condition"):
                lines.append(f"{indent}On: {join['condition'].sql()}")  # type: ignore
        return lines


class Aggregate(Step):
    def __init__(self) -> None:
        super().__init__()
        self.aggregations: t.List[exp.Expression] = []
        self.operands: t.Tuple[exp.Expression, ...] = ()
        self.group: t.List[exp.Expression] = []
        self.source: t.Optional[str] = None

    def _to_s(self, indent: str) -> t.List[str]:
        lines = [f"{indent}Aggregations:"]

        for expression in self.aggregations:
            lines.append(f"{indent}  - {expression.sql()}")

        if self.group:
            lines.append(f"{indent}Group:")
            for expression in self.group:
                lines.append(f"{indent}  - {expression.sql()}")
        if self.operands:
            lines.append(f"{indent}Operands:")
            for expression in self.operands:
                lines.append(f"{indent}  - {expression.sql()}")

        return lines


class Sort(Step):
    def __init__(self) -> None:
        super().__init__()
        self.key = None

    def _to_s(self, indent: str) -> t.List[str]:
        lines = [f"{indent}Key:"]

        for expression in self.key:  # type: ignore
            lines.append(f"{indent}  - {expression.sql()}")

        return lines
