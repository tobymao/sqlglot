import time

from sqlglot import parse_one
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan


def execute(sql, schema, read=None):
    from sqlglot.compute import python
    expression = parse_one(sql, read=read)
    expression = optimize(expression, schema)
    plan = Plan(expression)
    now = time.time()
    result = python.execute(plan)
    print(time.time() - now)
    return result


schema = {
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

sql = """
select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        max(l_discount) as avg_disc,
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

print(execute(sql, schema))
