import ast
import collections
import itertools
import math

from sqlglot import exp, generator, planner, tokens
from sqlglot.dialects.dialect import Dialect, inline_array_sql
from sqlglot.executor.context import Context
from sqlglot.executor.env import ENV
from sqlglot.executor.table import Table
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
        source = step.source

        if isinstance(source, exp.Expression):
            source = source.name or source.alias

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
        else:
            sink = None

        for reader, ctx in table_iter:
            if sink is None:
                sink = Table(reader.columns)

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
        source = step.source
        alias = source.alias

        with csv_reader(source) as reader:
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
                context.set_row(tuple(t(v) for t, v in zip(types, row)))
                yield context.table.reader, context

    def join(self, step, context):
        source = step.name

        source_table = context.tables[source]
        source_context = self.context({source: source_table})
        column_ranges = {source: range(0, len(source_table.columns))}

        for name, join in step.joins.items():
            table = context.tables[name]
            start = max(r.stop for r in column_ranges.values())
            column_ranges[name] = range(start, len(table.columns) + start)
            join_context = self.context({name: table})

            if join.get("source_key"):
                table = self.hash_join(join, source_context, join_context)
            else:
                table = self.nested_loop_join(join, source_context, join_context)

            source_context = self.context(
                {
                    name: Table(table.columns, table.rows, column_range)
                    for name, column_range in column_ranges.items()
                }
            )

        condition = self.generate(step.condition)
        projections = self.generate_tuple(step.projections)

        if not condition or not projections:
            return source_context

        sink = self.table(step.projections if projections else source_context.columns)

        for reader, ctx in join_context:
            if condition and not ctx.eval(condition):
                continue

            if projections:
                sink.append(ctx.eval_tuple(projections))
            else:
                sink.append(reader.row)

            if len(sink) >= step.limit:
                break

        return self.context({step.name: sink})

    def nested_loop_join(self, _join, source_context, join_context):
        table = Table(source_context.columns + join_context.columns)

        for reader_a, _ in source_context:
            for reader_b, _ in join_context:
                table.append(reader_a.row + reader_b.row)

        return table

    def hash_join(self, join, source_context, join_context):
        source_key = self.generate_tuple(join["source_key"])
        join_key = self.generate_tuple(join["join_key"])

        results = collections.defaultdict(lambda: ([], []))

        for reader, ctx in source_context:
            results[ctx.eval_tuple(source_key)][0].append(reader.row)
        for reader, ctx in join_context:
            results[ctx.eval_tuple(join_key)][1].append(reader.row)

        table = Table(source_context.columns + join_context.columns)

        for a_group, b_group in results.values():
            for a_row, b_row in itertools.product(a_group, b_group):
                table.append(a_row + b_row)

        return table

    def aggregate(self, step, context):
        source = step.source
        group_by = self.generate_tuple(step.group)
        aggregations = self.generate_tuple(step.aggregations)
        operands = self.generate_tuple(step.operands)

        if operands:
            source_table = context.tables[source]
            operand_table = Table(source_table.columns + self.table(step.operands).columns)

            for reader, ctx in context:
                operand_table.append(reader.row + ctx.eval_tuple(operands))

            context = self.context(
                {None: operand_table, **{table: operand_table for table in context.tables}}
            )

        context.sort(group_by)

        group = None
        start = 0
        end = 1
        length = len(context.tables[source])
        table = self.table(step.group + step.aggregations)

        for i in range(length):
            context.set_index(i)
            key = context.eval_tuple(group_by)
            group = key if group is None else group
            end += 1

            if i == length - 1:
                context.set_range(start, end - 1)
            elif key != group:
                context.set_range(start, end - 2)
            else:
                continue

            table.append(group + context.eval_tuple(aggregations))
            group = key
            start = end - 2

        context = self.context({step.name: table, **{name: table for name in context.tables}})

        if step.projections:
            return self.scan(step, context)
        return context

    def sort(self, step, context):
        projections = self.generate_tuple(step.projections)

        sink = self.table(step.projections)

        for reader, ctx in context:
            sink.append(ctx.eval_tuple(projections))

        context = self.context(
            {
                None: sink,
                **{table: sink for table in context.tables},
            }
        )
        context.sort(self.generate_tuple(step.key))

        if not math.isinf(step.limit):
            context.table.rows = context.table.rows[0 : step.limit]

        return self.context({step.name: context.table})


def _cast_py(self, expression):
    to = expression.args["to"].this
    this = self.sql(expression, "this")

    if to == exp.DataType.Type.DATE:
        return f"datetime.date.fromisoformat({this})"
    if to == exp.DataType.Type.TEXT:
        return f"str({this})"
    raise NotImplementedError


def _column_py(self, expression):
    table = self.sql(expression, "table") or None
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
    return f"""bool(re.match({expression}.replace("_", ".").replace("%", ".*"), {this}))"""


def _ordered_py(self, expression):
    this = self.sql(expression, "this")
    desc = expression.args.get("desc")
    return f"desc({this})" if desc else this


class Python(Dialect):
    class Tokenizer(tokens.Tokenizer):
        ESCAPES = ["\\"]

    class Generator(generator.Generator):
        TRANSFORMS = {
            exp.Alias: lambda self, e: self.sql(e.this),
            exp.Array: inline_array_sql,
            exp.And: lambda self, e: self.binary(e, "and"),
            exp.Boolean: lambda self, e: "True" if e.this else "False",
            exp.Cast: _cast_py,
            exp.Column: _column_py,
            exp.EQ: lambda self, e: self.binary(e, "=="),
            exp.In: lambda self, e: f"{self.sql(e, 'this')} in {self.expressions(e)}",
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
