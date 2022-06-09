import sqlglot.expressions as exp
from sqlglot.optimizer import optimize
from sqlglot.optimizer.scope import traverse_scope


def plan(expression):
    scope = reversed(traverse_scope(expression))[0]
    select = scope.expression
    from_ = select.args.get("from")

    if from_:
        from_ = from_.args["expressions"][0]
        alias = from_.alias
        from_ = from_.this

        if isinstance(from_, exp.Table):
            Scan()
        elif isinstance(from_, exp.Subquery):
            Scan()

    print(select.sql(pretty=True))
    print(scope.parent)


class Plan:
    def __init__(self):
        self.downstream = {}
        self.upstream = {}

    def add(self, downstream, upstream):


class Step:
    def __init__(self):
        self.filter = None


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
    SELECT a + x.b
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
    """
)

expression = optimize(expression, {"x": {"a": "int", "b": "int"}})

plan(expression)
