import ast
import csv
import datetime
import gzip
import statistics
from collections import deque

import sqlglot.expressions as exp
from sqlglot import planner
from sqlglot.dialects import Dialect


ENV = {
    "__builtins__": {},
    "datetime": datetime,
    "float": float,
    "int": int,
    "str": str,
    "SUM": sum,
    "AVG": statistics.fmean,
    "COUNT": len,
    "MAX": max,
    "POW": pow,
}


class DataTable:
    def __init__(self, columns):
        self.table = {column: [] for column in columns}
        self.columns = dict(enumerate(self.table))
        self.width = len(self.columns)
        self.length = 0
        self.reader = ColumnarReader(self.table)

    def __iter__(self):
        return DataTableIter(self)

    def __repr__(self):
        widths = {column: len(column) for column in self.table}
        lines = [" ".join(column for column in self.table)]

        for i, row in enumerate(self):
            if i > 10:
                break

            lines.append(
                " ".join(
                    str(row[column]).rjust(widths[column])[0 : widths[column]]
                    for column in self.table
                )
            )
        return "\n".join(lines)

    def add(self, row):
        for i in range(self.width):
            self.table[self.columns[i]].append(row[i])
        self.length += 1

    def pop(self):
        for column in self.table.values():
            column.pop()
        self.length -= 1


class DataTableIter:
    def __init__(self, data_table):
        self.data_table = data_table
        self.index = -1

    def __iter__(self):
        return self

    def __next__(self):
        self.index += 1
        if self.index < self.data_table.length:
            self.data_table.reader.row = self.index
            return self.data_table.reader
        raise StopIteration


class Context:
    def __init__(self, tables, env=None):
        self.data_tables = {
            name: table
            for name, table in tables.items()
            if isinstance(table, DataTable)
        }
        self.range_readers = {
            name: RangeReader(data_table.table)
            for name, data_table in self.data_tables.items()
        }
        self.row_readers = {
            name: dt_or_columns.reader
            if name in self.data_tables
            else RowReader(dt_or_columns)
            for name, dt_or_columns in tables.items()
        }
        self.env = {**(env or {}), "scope": self.row_readers}

    def eval(self, code):
        # pylint: disable=eval-used
        return eval(code, ENV, self.env)

    def sort(self, table, key):
        def _sort(i):
            self.set_row(table, i)
            return key(self)

        data_table = self.data_tables[table]
        index = list(range(data_table.length))
        index.sort(key=_sort)

        for column, rows in data_table.table.items():
            data_table.table[column] = [rows[i] for i in index]

    def set_row(self, table, row):
        self.row_readers[table].row = row
        self.env["scope"] = self.row_readers

    def set_range(self, table, start, end):
        self.range_readers[table].range = range(start, end)
        self.env["scope"] = self.range_readers

    def __getitem__(self, table):
        return self.env["scope"][table]


class RangeReader:
    def __init__(self, columns):
        self.columns = columns
        self.range = range(0)

    def __len__(self):
        return len(self.range)

    def __getitem__(self, column):
        return (self.columns[column][i] for i in self.range)


class ColumnarReader:
    def __init__(self, columns):
        self.columns = columns
        self.row = None

    def __getitem__(self, column):
        return self.columns[column][self.row]


class RowReader:
    def __init__(self, columns):
        self.columns = {column: i for i, column in enumerate(columns)}
        self.row = None

    def __getitem__(self, column):
        return self.row[self.columns[column]]


def execute(plan, env=None):
    env = env or ENV.copy()

    running = set()
    queue = deque((leaf, None) for leaf in plan.leaves)

    while queue:
        node, context = queue.popleft()
        running.add(node)

        if isinstance(node, planner.Scan):
            context = scan(node, context)
        elif isinstance(node, planner.Aggregate):
            context = aggregate(node, context)

        for dep in node.dependents:
            if dep not in running:
                queue.append((dep, context))

    return context.data_tables["root"]


def generate(expression):
    sql = PYTHON_GENERATOR.generate(expression)
    return compile(sql, sql, "eval", optimize=2)


def aggregate(step, context):
    group = []
    projections = []
    aggregations = []

    columns = []

    for expression in step.group:
        columns.append(expression.alias_or_name)
        projections.append(generate(expression))
    for expression in step.aggregations:
        columns.append(expression.alias_or_name)
        aggregations.append(generate(expression))

    sink = DataTable(columns)

    table = step.name
    context.sort(table, lambda c: tuple(c.eval(code) for code in projections))

    group = None
    start = 0
    end = 1
    length = context.data_tables[table].length

    for i in range(length):
        context.set_row(table, i)
        key = tuple(context.eval(code) for code in projections)
        group = key if group is None else group
        end += 1

        if i == length - 1:
            context.set_range(table, start, end - 1)
        elif key != group:
            context.set_range(table, start, end - 2)
        else:
            continue
        aggs = tuple(context.eval(agg) for agg in aggregations)
        sink.add(group + aggs)
        group = key
        start = end - 2

    return Context({step.name: sink})


def scan(step, _context):
    table = step.source.name
    sink = DataTable(expression.alias for expression in step.projections)

    filter_code = generate(step.filter) if step.filter else None
    projections = tuple(generate(expression) for expression in step.projections)

    for csv_context in scan_csv(table):
        if filter_code and not csv_context.eval(filter_code):
            continue
        sink.add(tuple(csv_context.eval(code) for code in projections))
        if step.limit and sink.length >= step.limit:
            break
    return Context({step.name: sink})


def scan_csv(table):
    # pylint: disable=stop-iteration-return
    with gzip.open(f"tests/fixtures/optimizer/tpc-h/{table}.csv.gz", "rt") as f:
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

        context = Context({table: columns})

        for row in reader:
            context.set_row(table, tuple(t(v) for t, v in zip(types, row)))
            yield context


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

    transforms = {
        exp.Alias: lambda self, e: self.sql(e.this),
        exp.Cast: _cast_py,
        exp.Column: _column_py,
        exp.Interval: _interval_py,
        exp.Star: lambda *_: "scope['root']",
    }


PYTHON_GENERATOR = Python().generator(identify=True)
