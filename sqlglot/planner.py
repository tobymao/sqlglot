import itertools
import math

from sqlglot import alias, exp
from sqlglot.errors import UnsupportedError
from sqlglot.optimizer.simplify import simplify


class Plan:
    def __init__(self, expression):
        self.expression = expression
        self.root = Step.from_expression(self.expression)
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
    def from_expression(cls, expression, ctes=None):
        """
        Build a DAG of Steps from a SQL expression.

        Giving an expression like:

        SELECT x.a, SUM(x.b)
        FROM x
        JOIN y
            ON x.a = y.a
        GROUP BY x.a

        Transform it into a DAG of the form:

        Aggregate(x.a, SUM(x.b))
          Join(y)
            Scan(x)
            Scan(y)

        This can then more easily be executed on by an engine.
        """
        ctes = ctes or {}
        with_ = expression.args.get("with")

        # CTEs break the mold of scope and introduce themselves to all in the context.
        if with_:
            ctes = ctes.copy()
            for cte in with_.expressions:
                step = Step.from_expression(cte.this, ctes)
                step.name = cte.alias
                ctes[step.name] = step

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
                    operand.replace(
                        exp.column(operands[operand], step.name, quoted=True)
                    )
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
            aggregate.group = [
                exp.column(e.alias_or_name, step.name, quoted=True)
                for e in group.expressions
            ]
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
            for k in sort.key + projections:
                for column in k.find_all(exp.Column):
                    column.set("table", exp.to_identifier(step.name, quoted=True))

        step.projections = projections

        limit = expression.args.get("limit")

        if limit:
            step.limit = int(limit.text("expression"))

        return step

    def __init__(self):
        self.name = None
        self.dependencies = set()
        self.dependents = set()
        self.projections = []
        self.limit = math.inf
        self.condition = None

    def add_dependency(self, dependency):
        self.dependencies.add(dependency)
        dependency.dependents.add(self)

    def __repr__(self):
        return self.to_s()

    def to_s(self, level=0):
        indent = "  " * level
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

        if self.condition:
            lines.append(f"{nested}Condition: {self.condition.sql()}")

        if self.dependencies:
            lines.append(f"{nested}Dependencies:")
            for dependency in self.dependencies:
                lines.append("  " + dependency.to_s(level + 1))

        return "\n".join(lines)

    def _to_s(self, _indent):
        return []


class Scan(Step):
    @classmethod
    def from_expression(cls, expression, ctes=None):
        table = expression.this
        alias_ = expression.alias

        if not alias_:
            raise UnsupportedError(
                "Tables/Subqueries must be aliased. Run it through the optimizer"
            )

        if isinstance(expression, exp.Subquery):
            step = Step.from_expression(table, ctes)
            step.name = alias_
            return step

        step = Scan()
        step.name = alias_
        step.source = expression
        if table.name in ctes:
            step.add_dependency(ctes[table.name])

        return step

    def __init__(self):
        super().__init__()
        self.source = None

    def _to_s(self, indent):
        return [f"{indent}Source: {self.source.sql()}"]


class Write(Step):
    pass


class Join(Step):
    @classmethod
    def from_joins(cls, joins, ctes=None):
        step = Join()

        for join in joins:
            name = join.this.alias
            on = join.args.get("on") or exp.TRUE
            source_key = []
            join_key = []

            # find the join keys
            # SELECT
            # FROM x
            # JOIN y
            #   ON x.a = y.b AND y.b > 1
            #
            # should pull y.b as the join key and x.a as the source key
            for condition in on.flatten() if isinstance(on, exp.And) else [on]:
                if isinstance(condition, exp.EQ):
                    left, right = condition.unnest_operands()
                    left_tables = exp.column_table_names(left)
                    right_tables = exp.column_table_names(right)

                    if name in left_tables and name not in right_tables:
                        join_key.append(left)
                        source_key.append(right)
                        condition.replace(exp.TRUE)
                    elif name in right_tables and name not in left_tables:
                        join_key.append(right)
                        source_key.append(left)
                        condition.replace(exp.TRUE)

            on = simplify(on)

            step.joins[name] = {
                "side": join.side,
                "join_key": join_key,
                "source_key": source_key,
                "condition": None if on == exp.TRUE else on,
            }

            step.add_dependency(Scan.from_expression(join.this, ctes))

        return step

    def __init__(self):
        super().__init__()
        self.joins = {}

    def _to_s(self, indent):
        lines = []
        for name, join in self.joins.items():
            lines.append(f"{indent}{name}: {join['side']}")
            if join.get("condition"):
                lines.append(f"{indent}On: {join['condition'].sql()}")
        return lines


class Aggregate(Step):
    def __init__(self):
        super().__init__()
        self.aggregations = []
        self.operands = []
        self.group = []
        self.source = None

    def _to_s(self, indent):
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
    def __init__(self):
        super().__init__()
        self.key = None

    def _to_s(self, indent):
        lines = [f"{indent}Key:"]

        for expression in self.key:
            lines.append(f"{indent}  - {expression.sql()}")

        return lines
