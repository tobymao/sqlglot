def execute(step):
    step


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
        --count(*) as count_order
from
        lineitem
where
        l_shipdate <= date '1998-12-01' - interval '90' day
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;
    """
)
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

expression = optimize(expression, tpch_schema)
step = plan(expression)
print(expression.sql(pretty=True))
print(step)

execute(step)
