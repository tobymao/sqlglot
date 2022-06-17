import ast
import csv
import gzip
import itertools

import sqlglot.expressions as exp
from sqlglot import planner
from sqlglot.dialects import Dialect
from sqlglot.executor.context import Context
from sqlglot.executor.env import ENV
from sqlglot.executor.table import Table


class PythonExecutor:
    def __init__(self, env=None):
        self.generator = Python().generator(identify=True)
        self.env = {**ENV, **(env or {})}

    def execute(self, plan):
        running = set()
        finished = set()
        queue = set(plan.leaves)
        contexts = {}

        while queue:
            node = queue.pop()
            context = self.context(
                {
                    name: table
                    for dep in node.dependencies
                    for name, table in contexts[dep].tables.items()
                }
            )
            running.add(node)

            if isinstance(node, planner.Scan):
                contexts[node] = self.scan(node, context)
            elif isinstance(node, planner.Aggregate):
                contexts[node] = self.aggregate(node, context)
            elif isinstance(node, planner.Join):
                contexts[node] = self.join(node, context)
            elif isinstance(node, planner.Sort):
                contexts[node] = self.sort(node, context)
            else:
                raise NotImplementedError

            running.remove(node)
            finished.add(node)

            for dep in node.dependents:
                if dep not in running and all(d in contexts for d in dep.dependencies):
                    queue.add(dep)

            for dep in node.dependencies:
                if all(d in finished for d in dep.dependents):
                    contexts.pop(dep)

        root = plan.root
        return contexts[root].tables[root.name]

    def generate(self, expression):
        """Convert a SQL expression into literal Python code and compile it into bytecode."""
        if not expression:
            return None

        sql = self.generator.generate(expression)
        return compile(sql, sql, "eval", optimize=2)

    def generate_tuple(self, expressions):
        """Convert an array of SQL expressions into tuple of Python byte code."""
        if not expressions:
            return tuple()
        return tuple(self.generate(expression) for expression in expressions)

    def context(self, tables):
        return Context(tables, env=self.env)

    def table(self, expressions):
        return Table(*(expression.alias_or_name for expression in expressions))

    def scan(self, step, context, source=None):
        source = source or step.source.name
        condition = self.generate(step.condition)
        projections = self.generate_tuple(step.projections)

        if source in context:
            if not projections and not condition:
                return self.context({step.name: context.tables[source]})
            table_iter = context.table_iter(source)
        else:
            table_iter = self.scan_csv(source)

        sink = None

        for ctx in table_iter:
            if not sink:
                sink = (
                    self.table(step.projections)
                    if projections
                    else Table(*ctx[source].columns)
                )

            if condition and not ctx.eval(condition):
                continue

            if projections:
                sink.add(ctx.eval_tuple(projections))
            else:
                sink.add(ctx[source].tuple())

            if sink.length >= step.limit:
                break

        return self.context({step.name: sink})

    def scan_csv(self, file):
        # pylint: disable=stop-iteration-return
        with gzip.open(f"tests/fixtures/optimizer/tpc-h/{file}.csv.gz", "rt") as f:
            reader = csv.reader(f, delimiter="|")
            columns = next(reader)
            row = next(reader)

            types = []

            for v in row:
                try:
                    types.append(type(ast.literal_eval(v)))
                except (ValueError, SyntaxError):
                    types.append(str)

            f.seek(0)
            next(reader)

            table = Table(*columns, _columnar=False)
            context = self.context({file: table})

            for row in reader:
                context.set_row(file, tuple(t(v) for t, v in zip(types, row)))
                yield context

    def join(self, step, context):
        source = step.name

        join_context = self.context({source: context.tables[source]})

        def merge_context(ctx, table):
            # create a new context where all existing tables are mapped to a new one
            return self.context({name: table for name in ctx.tables})

        for name, join in step.joins.items():
            join_context = self.context(
                {**join_context.tables, name: context.tables[name]}
            )
            kind = join["kind"]

            if kind == "CROSS":
                table = self.nested_loop_join(join, source, name, join_context)
            else:
                table = self.sort_merge_join(join, source, name, join_context)

            join_context = merge_context(join_context, table)

        # apply projections or conditions
        context = self.scan(step, join_context, source)

        # use the scan context since it returns a single table
        # otherwise there are no projections so all other tables are still in scope
        if step.projections:
            return context

        return merge_context(join_context, context.tables[source])

    def nested_loop_join(self, join, a, b, context):
        table = Table(*(context.tables[a].columns + context.tables[b].columns))

        for _ in context.table_iter(a):
            for ctx in context.table_iter(b):
                table.add(ctx[a].tuple() + ctx[b].tuple())

        return table

    def sort_merge_join(self, join, a, b, context):
        on = join["on"]
        on = on.flatten() if isinstance(on, exp.And) else [on]

        a_key = []
        b_key = []

        for condition in on:
            for column in condition.find_all(exp.Column):
                if b == column.text("table"):
                    b_key.append(self.generate(column))
                else:
                    a_key.append(self.generate(exp.column(column.name, a)))

        context.sort(a, lambda c: c.eval_tuple(a_key))
        context.sort(b, lambda c: c.eval_tuple(b_key))

        a_i = 0
        b_i = 0
        a_n = context.tables[a].length
        b_n = context.tables[b].length

        table = Table(*(context.tables[a].columns + context.tables[b].columns))

        def get_key(source, key, i):
            context.set_row(source, i)
            return context.eval_tuple(key)

        while a_i < a_n and b_i < b_n:
            key = min(get_key(a, a_key, a_i), get_key(b, b_key, b_i))

            a_group = []

            while a_i < a_n and key == get_key(a, a_key, a_i):
                a_group.append(context[a].tuple())
                a_i += 1

            b_group = []

            while b_i < b_n and key == get_key(b, b_key, b_i):
                b_group.append(context[b].tuple())
                b_i += 1

            for a_row, b_row in itertools.product(a_group, b_group):
                table.add(a_row + b_row)

        return table

    def aggregate(self, step, context):
        source = list(context.tables)[0]
        group_by = self.generate_tuple(step.group)
        aggregations = self.generate_tuple(step.aggregations)
        operands = self.generate_tuple(step.operands)

        context.sort(source, lambda ctx: ctx.eval_tuple(group_by))

        if step.operands:
            operand_dt = self.table(step.operands)

            for ctx in context.table_iter(source):
                operand_dt.add(ctx.eval_tuple(operands))

            context = self.context(
                {source: Table(**context.tables[source].data, **operand_dt.data)}
            )

        group = None
        start = 0
        end = 1
        length = context.tables[source].length
        table = self.table(step.group + step.aggregations)

        for i in range(length):
            context.set_row(source, i)
            key = context.eval_tuple(group_by)
            group = key if group is None else group
            end += 1

            if i == length - 1:
                context.set_range(source, start, end - 1)
            elif key != group:
                context.set_range(source, start, end - 2)
            else:
                continue

            table.add(group + context.eval_tuple(aggregations))
            group = key
            start = end - 2

        return self.context({step.name: table})

    def sort(self, step, context):
        table = list(context.tables)[0]
        key = self.generate_tuple(step.key)
        context.sort(table, lambda ctx: ctx.eval_tuple(key))
        return self.scan(step, context, step.name)


class Python(Dialect):
    # pylint: disable=no-member
    def _cast_py(self, expression):
        to = expression.args["to"].this
        this = self.sql(expression, "this")

        if to == exp.DataType.Type.DATE:
            return f"datetime.date.fromisoformat({this})"
        raise NotImplementedError

    def _column_py(self, expression):
        table = self.sql(expression, "table")
        this = self.sql(expression, "this")
        return f"scope[{table}][{this}]"

    def _interval_py(self, expression):
        this = self.sql(expression, "this")
        unit = expression.text("unit").upper()
        if unit == "DAY":
            return f"datetime.timedelta(days=float({this}))"
        raise NotImplementedError

    def _like_py(self, expression):
        this = self.sql(expression, "this")
        expression = self.sql(expression, "expression")
        return (
            f"""re.match({expression}.replace("_", ".").replace("%", ".*"), {this})"""
        )

    def _ordered_py(self, expression):
        this = self.sql(expression, "this")
        desc = expression.args.get("desc")
        return f"desc({this})" if desc else this

    transforms = {
        exp.Alias: lambda self, e: self.sql(e.this),
        exp.And: lambda self, e: self.binary(e, "and"),
        exp.Cast: _cast_py,
        exp.Column: _column_py,
        exp.EQ: lambda self, e: self.binary(e, "=="),
        exp.Interval: _interval_py,
        exp.Like: _like_py,
        exp.Or: lambda self, e: self.binary(e, "or"),
        exp.Ordered: _ordered_py,
        exp.Star: lambda *_: "1",
    }
