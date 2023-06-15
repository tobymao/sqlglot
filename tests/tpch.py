import time

from sqlglot.optimizer import optimize

INPUT = "/home/toby/dev/tpch/{i}.sql"
OUTPUT = "/home/toby/dev/sqlglot/tests/fixtures/optimizer/tpc-h/tpc-h.sql"
NUM = 22
SCHEMA = {
    "lineitem": {
        "l_orderkey": "bigint",
        "l_partkey": "bigint",
        "l_suppkey": "bigint",
        "l_linenumber": "bigint",
        "l_quantity": "double",
        "l_extendedprice": "double",
        "l_discount": "double",
        "l_tax": "double",
        "l_returnflag": "string",
        "l_linestatus": "string",
        "l_shipdate": "string",
        "l_commitdate": "string",
        "l_receiptdate": "string",
        "l_shipinstruct": "string",
        "l_shipmode": "string",
        "l_comment": "string",
    },
    "orders": {
        "o_orderkey": "bigint",
        "o_custkey": "bigint",
        "o_orderstatus": "string",
        "o_totalprice": "double",
        "o_orderdate": "string",
        "o_orderpriority": "string",
        "o_clerk": "string",
        "o_shippriority": "int",
        "o_comment": "string",
    },
    "customer": {
        "c_custkey": "bigint",
        "c_name": "string",
        "c_address": "string",
        "c_nationkey": "bigint",
        "c_phone": "string",
        "c_acctbal": "double",
        "c_mktsegment": "string",
        "c_comment": "string",
    },
    "part": {
        "p_partkey": "bigint",
        "p_name": "string",
        "p_mfgr": "string",
        "p_brand": "string",
        "p_type": "string",
        "p_size": "int",
        "p_container": "string",
        "p_retailprice": "double",
        "p_comment": "string",
    },
    "supplier": {
        "s_suppkey": "bigint",
        "s_name": "string",
        "s_address": "string",
        "s_nationkey": "bigint",
        "s_phone": "string",
        "s_acctbal": "double",
        "s_comment": "string",
    },
    "partsupp": {
        "ps_partkey": "bigint",
        "ps_suppkey": "bigint",
        "ps_availqty": "int",
        "ps_supplycost": "double",
        "ps_comment": "string",
    },
    "nation": {
        "n_nationkey": "bigint",
        "n_name": "string",
        "n_regionkey": "bigint",
        "n_comment": "string",
    },
    "region": {
        "r_regionkey": "bigint",
        "r_name": "string",
        "r_comment": "string",
    },
}
KIND = "H"

with open(OUTPUT, "w", encoding="UTF-8") as fixture:
    for i in range(NUM):
        i = i + 1
        with open(INPUT.format(i=i), encoding="UTF-8") as file:
            original = "\n".join(
                line.rstrip()
                for line in file.read().split(";")[0].split("\n")
                if not line.startswith("--")
            )
            original = original.replace("`", '"').strip()
            now = time.time()
            try:
                optimized = optimize(original, schema=SCHEMA)
            except Exception as e:
                print("****", i, e, "****")
                continue

            fixture.write(
                f"""--------------------------------------
-- TPC-{KIND} {i}
--------------------------------------
{original};
{optimized.sql(pretty=True)};

"""
            )
            print(i, time.time() - now)
