import ast
import collections
import itertools

from sqlglot import exp, planner
from sqlglot.executor.context import Context
from sqlglot.dialects.dialect import Dialect, inline_array_sql
from sqlglot.executor.env import ENV
from sqlglot.executor.table import Table
from sqlglot.generator import Generator
from sqlglot.helper import csv_reader


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
        return Table(expression.alias_or_name for expression in expressions)

    def scan(self, step, context):
        if hasattr(step, "source"):
            source = step.source

            if isinstance(source, exp.Expression):
                source = source.this.name or source.alias
        else:
            source = step.name
        condition = self.generate(step.condition)
        projections = self.generate_tuple(step.projections)

        if source in context:
            if not projections and not condition:
                return self.context({step.name: context.tables[source]})
            table_iter = context.table_iter(source)
        else:
            table_iter = self.scan_csv(step)

        if projections:
            sink = self.table(step.projections)
        elif source in context:
            sink = Table(context[source].columns)
        else:
            sink = None

        for reader, ctx in table_iter:
            if sink is None:
                sink = Table(ctx[source].columns)

            if condition and not ctx.eval(condition):
                continue

            if projections:
                sink.append(ctx.eval_tuple(projections))
            else:
                sink.append(reader.row)

            if len(sink) >= step.limit:
                break

        return self.context({step.name: sink})

    def scan_csv(self, step):
        # pylint: disable=stop-iteration-return
        source = step.source
        alias = source.alias

        with csv_reader(source.this) as reader:
            columns = next(reader)
            table = Table(columns)
            context = self.context({alias: table})
            types = []

            for row in reader:
                if not types:
                    for v in row:
                        try:
                            types.append(type(ast.literal_eval(v)))
                        except (ValueError, SyntaxError):
                            types.append(str)
                context.set_row(alias, tuple(t(v) for t, v in zip(types, row)))
                yield context[alias], context

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

            if join.get("source_key"):
                table = self.hash_join(join, source, name, join_context)
            else:
                table = self.nested_loop_join(join, source, name, join_context)

            join_context = merge_context(join_context, table)

        # apply projections or conditions
        context = self.scan(step, join_context)

        # use the scan context since it returns a single table
        # otherwise there are no projections so all other tables are still in scope
        if step.projections:
            return context

        return merge_context(join_context, context.tables[source])

    def nested_loop_join(self, _join, a, b, context):
        table = Table(context.tables[a].columns + context.tables[b].columns)

        for reader_a, _ in context.table_iter(a):
            for reader_b, _ in context.table_iter(b):
                table.append(reader_a.row + reader_b.row)

        return table

    def hash_join(self, join, a, b, context):
        a_key = self.generate_tuple(join["source_key"])
        b_key = self.generate_tuple(join["join_key"])

        results = collections.defaultdict(lambda: ([], []))

        for reader, ctx in context.table_iter(a):
            results[ctx.eval_tuple(a_key)][0].append(reader.row)
        for reader, ctx in context.table_iter(b):
            results[ctx.eval_tuple(b_key)][1].append(reader.row)

        table = Table(context.tables[a].columns + context.tables[b].columns)
        for a_group, b_group in results.values():
            for a_row, b_row in itertools.product(a_group, b_group):
                table.append(a_row + b_row)

        return table

    def sort_merge_join(self, join, a, b, context):
        a_key = self.generate_tuple(join["source_key"])
        b_key = self.generate_tuple(join["join_key"])

        context.sort(a, a_key)
        context.sort(b, b_key)

        a_i = 0
        b_i = 0
        a_n = len(context.tables[a])
        b_n = len(context.tables[b])

        table = Table(context.tables[a].columns + context.tables[b].columns)

        def get_key(source, key, i):
            context.set_index(source, i)
            return context.eval_tuple(key)

        while a_i < a_n and b_i < b_n:
            key = min(get_key(a, a_key, a_i), get_key(b, b_key, b_i))

            a_group = []

            while a_i < a_n and key == get_key(a, a_key, a_i):
                a_group.append(context[a].row)
                a_i += 1

            b_group = []

            while b_i < b_n and key == get_key(b, b_key, b_i):
                b_group.append(context[b].row)
                b_i += 1

            for a_row, b_row in itertools.product(a_group, b_group):
                table.append(a_row + b_row)

        return table

    def aggregate(self, step, context):
        source = step.source
        group_by = self.generate_tuple(step.group)
        aggregations = self.generate_tuple(step.aggregations)
        operands = self.generate_tuple(step.operands)

        context.sort(source, group_by)

        if step.operands:
            source_table = context.tables[source]
            operand_table = Table(
                source_table.columns + self.table(step.operands).columns
            )

            for reader, ctx in context:
                operand_table.append(reader.row + ctx.eval_tuple(operands))

            context = self.context({source: operand_table})

        group = None
        start = 0
        end = 1
        length = len(context.tables[source])
        table = self.table(step.group + step.aggregations)

        for i in range(length):
            context.set_index(source, i)
            key = context.eval_tuple(group_by)
            group = key if group is None else group
            end += 1

            if i == length - 1:
                context.set_range(source, start, end - 1)
            elif key != group:
                context.set_range(source, start, end - 2)
            else:
                continue

            table.append(group + context.eval_tuple(aggregations))
            group = key
            start = end - 2

        return self.scan(step, self.context({source: table}))

    def sort(self, step, context):
        table = list(context.tables)[0]
        key = self.generate_tuple(step.key)
        context.sort(table, key)
        return self.scan(step, context)


# pylint: disable=no-member
def _cast_py(self, expression):
    to = expression.args["to"].this
    this = self.sql(expression, "this")

    if to == exp.DataType.Type.DATE:
        return f"datetime.date.fromisoformat({this})"
    if to == exp.DataType.Type.TEXT:
        return f"str({this})"
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
    return f"""re.match({expression}.replace("_", ".").replace("%", ".*"), {this})"""


def _ordered_py(self, expression):
    this = self.sql(expression, "this")
    desc = expression.args.get("desc")
    return f"desc({this})" if desc else this


class Python(Dialect):
    escape = "\\"

    class Generator(Generator):
        TRANSFORMS = {
            exp.Alias: lambda self, e: self.sql(e.this),
            exp.Array: inline_array_sql,
            exp.And: lambda self, e: self.binary(e, "and"),
            exp.Cast: _cast_py,
            exp.Column: _column_py,
            exp.EQ: lambda self, e: self.binary(e, "=="),
            exp.Interval: _interval_py,
            exp.Is: lambda self, e: self.binary(e, "is"),
            exp.Like: _like_py,
            exp.Not: lambda self, e: f"not {self.sql(e.this)}",
            exp.Null: lambda *_: "None",
            exp.Or: lambda self, e: self.binary(e, "or"),
            exp.Ordered: _ordered_py,
            exp.Star: lambda *_: "1",
        }

        def case_sql(self, expression):
            this = self.sql(expression, "this")
            chain = self.sql(expression, "default") or "None"

            for e in reversed(expression.args["ifs"]):
                true = self.sql(e, "true")
                condition = self.sql(e, "this")
                condition = f"{this} = ({condition})" if this else condition
                chain = f"{true} if {condition} else ({chain})"

            return chain
