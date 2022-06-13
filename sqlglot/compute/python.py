import ast
import csv
import gzip
import datetime
from collections import deque, defaultdict
import sqlglot.expressions as exp
import sqlglot.planner as planner
from sqlglot.helper import tsort
from sqlglot.dialects import Dialect


class DataTable:
    def __init__(self, columns):
        self.table = {column: [] for column in columns}
        self.columns = {i: column for i, column in enumerate(self.table)}
        self.width = len(self.columns)
        self.length = 0

    def __repr__(self):
        return str(self.table)

    def add(self, row):
        for i in range(self.width):
            self.table[self.columns[i]].append(row[i])
        self.length += 1

    def pop(self):
        for column in self.table.values():
            column.pop()
        self.length -= 1

    def sort(self, key=None):
        reader = RowReader(self.columns)
        index = list(range(self.length))
        index.sort(key=key)

        for column, rows in self.table.items():
            self.table[column] = [rows[i] for i in index]

    def __iter__(self):
        return DataTableIter(self)

    def __getitem__(self, column):
        return self.table[column]


class DataTableIter:
    def __init__(self, data_table):
        self.data_table = data_table
        self.index = -1

    def __repr__(self):
        return {
            column: rows[self.index]
            for column, rows in self.data_table.table.items()
        }.__repr__()

    def __iter__(self):
        return self

    def __next__(self):
        table = self.data_table.table
        self.index += 1
        if self.index < self.data_table.length:
            return self
        raise StopIteration

    def __getitem__(self, column):
        return self.data_table[column][self.index]


class RowReader:
    def __init__(self, columns):
        self.columns = {column: i for i, column in enumerate(columns)}
        self.row = None

    def __getitem__(self, column):
        return self.row[self.columns[column]]


ENV = {
    "__builtins__": {},
    "globals": None,
    "locals": None,
    "datetime": datetime,
    "float": float,
    "int": int,
    "str": str,
}

def execute(plan, env=None):
    env = env or ENV.copy()
    env["scope"] = {}
    env["table"] = {}

    running = set()
    queue = deque(plan.leaves)

    while queue:
        node = queue.popleft()
        running.add(node)

        if isinstance(node, planner.Scan):
            result = scan(node, env)
            env["scope"][node.name] = result
        elif isinstance(node, planner.Aggregate):
            result = aggregate(node, env)
            env["scope"][node.name] = result

        for dep in node.dependents:
            if dep not in running:
                queue.append(dep)

    return env["scope"][plan.root.name]


def aggregate(step, env):
    #sink = DataTable(expression.alias for expression in step.projections)
    group = []
    projections = []
    aggregations = []

    for expression in step.group:
        projections.append(generate(expression))
    for expression in step.aggregations:
        aggregations.append(generate(expression))

    table = env["scope"][step.name]
    table_iter = iter(table)
    env["table"] = table_iter


    def sortit(i):
        table_iter.index = i
        return tuple(eval(code, env) for code in projections)

    table.sort(key=sortit)

    env["table"] = table
    for i, row in enumerate(table):
        x = tuple(eval(code, env) for code in projections)
        print(x)


def scan(step, env):
    table = step.source.name
    sink = DataTable(expression.alias for expression in step.projections)

    filter_code = generate(step.filter) if step.filter else None
    projections = tuple(generate(expression) for expression in step.projections)

    for i, row in enumerate(scan_csv(table, env)):
        if filter_code and not eval(filter_code, env):
            continue
        sink.add(tuple(eval(code, env) for code in projections))
    return sink


def scan_csv(table, env):
    with gzip.open(f"tests/fixtures/optimizer/tpc-h/{table}.csv.gz", "rt") as f:
        reader = csv.reader(f, delimiter="|")
        columns = next(reader)
        row = next(reader)

        types = []

        for k, v in zip(columns, row):
            try:
                types.append(type(ast.literal_eval(v)))
            except (ValueError, SyntaxError):
                types.append(str)

        f.seek(0)
        next(reader)

        row_reader = RowReader(columns)
        env["scope"][table] = row_reader

        for row in reader:
            row_reader.row = tuple(t(v) for t, v in zip(types, row))
            yield row_reader


class Python(Dialect):
    def _cast_py(self, expression):
        to = expression.args["to"].this
        this = self.sql(expression, 'this')

        if to == exp.DataType.Type.DATE:
            return f"datetime.date.fromisoformat({this})"
        raise

    def _column_py(self, expression):
        table = self.sql(expression, "table")
        this = self.sql(expression, "this")
        if table:
            return f"scope[{table}][{this}]"
        return f"table[{this}]"

    def _interval_py(self, expression):
        this = self.sql(expression, 'this')
        unit = expression.text("unit").upper()
        if unit == "DAY":
            return f"datetime.timedelta(days=float({this}))"
        raise

    transforms = {
        exp.Alias: lambda self, e: self.sql(e.this),
        exp.Cast: _cast_py,
        exp.Column: _column_py,
        exp.Interval: _interval_py,
        exp.Star: lambda *_: "1",
    }

def generate(expression):
    sql = Python().generator(identify=True).generate(expression)
    print(sql)
    return compile(sql, sql, "eval", optimize=2)



import time
import sqlglot
from sqlglot.optimizer import optimize

expression = sqlglot.parse_one(
    """
select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        date l_shipdate <= date '1998-12-01' - interval '90' day
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;
    """
)

#import sys
#
#sql = sys.argv[1]
#expression = sqlglot.parse_one(sql)
tpch_schema = {
    "lineitem": {
        "l_orderkey": "uint64",
        "l_partkey": "uint64",
        "l_suppkey": "uint64",
        "l_linenumber": "uint64",
        "l_quantity": "float64",
        "l_extendedprice": "float64",
        "l_discount": "float64",
        "l_tax": "float64",
        "l_returnflag": "string",
        "l_linestatus": "string",
        "l_shipdate": "date32",
        "l_commitdate": "date32",
        "l_receiptdate": "date32",
        "l_shipinstruct": "string",
        "l_shipmode": "string",
        "l_comment": "string",
    },
}

now = time.time()
expression = optimize(expression, tpch_schema)
plan = planner.Plan(expression)
optimize_time = time.time() - now
now = time.time()
#print(expression.sql(pretty=True))
print(execute(plan))
print(optimize_time, time.time() - now)
