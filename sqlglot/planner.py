from collections import defaultdict

import sqlglot.expressions as exp
from sqlglot.helper import tsort
from sqlglot.optimizer import optimize
from sqlglot.optimizer.scope import traverse_scope


class Plan:
    @classmethod
    def from_expression(cls, expression):
        plan = Plan()
        scope = traverse_scope(expression)[-1]
        expression = scope.expression
        print(expression.sql(pretty=True))
        plan.add_expression_tree(expression, scope)
        for step in tsort(plan.dependencies):
            print(step)
            for e in step.projections:
                print(e.sql())
            for dep in step.dependencies:
                print(f"-- {dep}")
            print(f"----")
        return plan


    def __init__(self):
        self.dependencies = defaultdict(set)

    def add_dependency(self, step, dependency):
        step.add_dependency(dependency)
        self.dependencies[step].add(dependency)
        self.dependencies[dependency]

    def add_expression_tree(self, node, scope, name=None):
        scope = scope.selected_sources[name] if name else scope

        if isinstance(node, exp.Select):
            step = Scan(name)
            from_ = node.args.get("from")
            group = node.args.get("group")
            where = node.args.get("where")
            expressions = node.args["expressions"]

            if from_:
                from_ = from_.args["expressions"][0]
                alias = from_.alias

                step.projections = [
                    expression
                    for expression in expressions
                    if exp.column_table_names(expression) in ([alias], [])
                    and (not group or not expression.find(exp.AggFunc))
                ]

                if isinstance(from_, exp.Subquery):
                    self.add_dependency(
                        step,
                        self.add_expression_tree(
                            from_.this,
                            scope,
                            alias
                        ),
                    )
                else:
                    pass

            joins = node.args.get("joins", [])

            if joins:
                join_step = Join(name)

                for join in joins:
                    source = join.this

                    self.add_dependency(
                        join_step,
                        self.add_expression_tree(
                            source.this,
                            scope,
                            source.alias,
                        ),
                    )

                self.add_dependency(join_step, step)
                step = join_step

            if group:
                aggregate = Aggregate(name)
                aggregate.projections = [
                    expression if expression.find(exp.AggFunc) else exp.to_identifier(expression.alias_or_name)
                    for expression in expressions

                ]
                self.add_dependency(aggregate, step)
                step = aggregate

            return step


class Step:
    def __init__(self, name):
        self.name = name
        self.dependencies = set()
        self.dependents = set()
        self.projections = []
        self.filter = None

    def __repr__(self):
        return f"""{self.__class__.__name__}: {self.name or "root"}"""

    def add_dependency(self, dependency):
        self.dependencies.add(dependency)
        dependency.dependents.add(self)


class Scan(Step):
    pass


class Write(Step):
    pass


class Join(Step):
    pass


class Aggregate(Step):
    pass


class Sort(Step):
    pass


import sqlglot

expression = sqlglot.parse_one(
    """
    SELECT x.a, SUM(a + x.b) total
    FROM (
        SELECT *
        FROM x
    ) x
    JOIN (
        SELECT b
        FROM x
    ) y
    ON x.a = y.b
    WHERE a = 1
    GROUP BY x.a
    """
)

expression = optimize(expression, {"x": {"a": "int", "b": "int"}})

Plan.from_expression(expression)
