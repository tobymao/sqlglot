import ast
import csv
import gzip
from datetime import date, timedelta
import sqlglot.expressions as exp
import sqlglot.planner as planner
from sqlglot.helper import tsort


class DataTable:
    def __init__(self, columns):
        self.table = {column: [] for column in columns}
        self.columns = {i: column for i, column in enumerate(self.table)}

    def add(self, row):
        for i, value in enumerate(row):
            self.table[self.columns[i]].append(value)

    def pop(self):
        for i, value in enumerate(row):
            self.table[self.columns[i]].pop()

    def __iter__(self):
        return DataTableIter(self)

    def __getitem__(self, column):
        return self.table[column]


class DataTableIter:
    def __init__(self, data_table):
        self.data_table = data_table
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        table = self.data_table.table

        for i in range(len(list(table.values())[0])):
            self.index = i
            yield self

    def __getitem__(self, column):
        return self.data_table[column][self.index]


def execute(step):
    nodes = {step}
    dag = {}

    while nodes:
        node = nodes.pop()
        dag[node] = set()
        for dep in node.dependencies:
            dag[node].add(dep)
            nodes.add(dep)

    for step in tsort(dag):
        if isinstance(step, planner.Scan):
            result = scan(step)
        if isinstance(step, planner.Aggregate):
            aggregate(step)
    return result


def scan(step):
    table = step.source.name

    result = DataTable(expression.alias for expression in step.projections)

    with gzip.open(f"tests/fixtures/optimizer/tpc-h/{table}.csv.gz", "rt") as f:
        reader = csv.reader(f, delimiter="|")
        columns = next(reader)
        dt = DataTable(columns)
        context = {table: iter(dt)}
        row = next(reader)
        types = []
        for k, v in zip(columns, row):
            try:
                types.append(type(ast.literal_eval(v)))
            except (ValueError, SyntaxError):
                types.append(str)
        f.seek(0)
        next(reader)
        count = 0
        for row in reader:
            count += 1
            dt.add((t(v) for t, v in zip(types, row)))

            if step.filter and not evaluate(context, step.filter):
                dt.pop()
                continue
            next(context[table])
            result.add((evaluate(context, expression) for expression in step.projections))
    return result


def evaluate(context, expression):
    expression = expression.unnest()

    if isinstance(expression, exp.Column):
        return context[expression.text("table")][expression.name]
    if isinstance(expression, exp.Literal):
        this = expression.name
        if expression.is_string:
            return this
        return float(this)
    if isinstance(expression, exp.Alias):
        return evaluate(context, expression.this)
    if isinstance(expression, exp.Binary):
        a, b = (evaluate(context, o) for o in expression.unnest_operands())

        if isinstance(expression, exp.Sub):
            return a - b
        if isinstance(expression, exp.Add):
            return a + b
        if isinstance(expression, exp.Mul):
            return a * b
        if isinstance(expression, exp.Div):
            return a / b
        if isinstance(expression, exp.EQ):
            return a == b
        if isinstance(expression, exp.LT):
            return a < b
        if isinstance(expression, exp.LTE):
            return a <= b
        if isinstance(expression, exp.GT):
            return a > b
        if isinstance(expression, exp.GTE):
            return a >= b
    if isinstance(expression, exp.Cast):
        to = expression.args["to"].this
        this = evaluate(context, expression.this)

        if to == exp.DataType.Type.DATE:
            return date.fromisoformat(this)
    if isinstance(expression, exp.Interval):
        this = float(evaluate(context, expression.this))
        unit = expression.text("unit").upper()
        if unit == "DAY":
            return timedelta(days=this)
    print(expression)
    raise



import time
import sqlglot
from sqlglot.planner import plan
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

import sys

sql = sys.argv[1]
expression = sqlglot.parse_one(sql)
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
step = plan(expression)
optimize_time = time.time() - now
now = time.time()
#print(expression.sql(pretty=True))
print(execute(step))
print(optimize_time, time.time() - now)
