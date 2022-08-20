import os

FILE_DIR = os.path.dirname(__file__)
FIXTURES_DIR = os.path.join(FILE_DIR, "fixtures")


def _filter_comments(s):
    return "\n".join(
        [line for line in s.splitlines() if line and not line.startswith("--")]
    )


def _extract_meta(sql):
    meta = {}
    sql_lines = sql.split("\n")
    i = 0
    while sql_lines[i].startswith("#"):
        key, val = sql_lines[i].split(":", maxsplit=1)
        meta[key.lstrip("#").strip()] = val.strip()
        i += 1
    sql = "\n".join(sql_lines[i:])
    return sql, meta


def assert_logger_contains(message, logger, level="error"):
    output = "\n".join(
        str(args[0][0]) for args in getattr(logger, level).call_args_list
    )
    assert message in output


def load_sql_fixtures(filename):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        for sql in _filter_comments(f.read()).splitlines():
            yield sql


def load_sql_fixture_pairs(filename):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        statements = _filter_comments(f.read()).split(";")

        size = len(statements)

        for i in range(0, size, 2):
            if i + 1 < size:
                sql = statements[i].strip()
                sql, meta = _extract_meta(sql)
                expected = statements[i + 1].strip()
                yield meta, sql, expected


TPCH_SCHEMA = {
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
    "orders": {
        "o_orderkey": "uint64",
        "o_custkey": "uint64",
        "o_orderstatus": "string",
        "o_totalprice": "float64",
        "o_orderdate": "date32",
        "o_orderpriority": "string",
        "o_clerk": "string",
        "o_shippriority": "int32",
        "o_comment": "string",
    },
    "customer": {
        "c_custkey": "uint64",
        "c_name": "string",
        "c_address": "string",
        "c_nationkey": "uint64",
        "c_phone": "string",
        "c_acctbal": "float64",
        "c_mktsegment": "string",
        "c_comment": "string",
    },
    "part": {
        "p_partkey": "uint64",
        "p_name": "string",
        "p_mfgr": "string",
        "p_brand": "string",
        "p_type": "string",
        "p_size": "int32",
        "p_container": "string",
        "p_retailprice": "float64",
        "p_comment": "string",
    },
    "supplier": {
        "s_suppkey": "uint64",
        "s_name": "string",
        "s_address": "string",
        "s_nationkey": "uint64",
        "s_phone": "string",
        "s_acctbal": "float64",
        "s_comment": "string",
    },
    "partsupp": {
        "ps_partkey": "uint64",
        "ps_suppkey": "uint64",
        "ps_availqty": "int32",
        "ps_supplycost": "float64",
        "ps_comment": "string",
    },
    "nation": {
        "n_nationkey": "uint64",
        "n_name": "string",
        "n_regionkey": "uint64",
        "n_comment": "string",
    },
    "region": {
        "r_regionkey": "uint64",
        "r_name": "string",
        "r_comment": "string",
    },
}
