import os

FILE_DIR = os.path.dirname(__file__)
FIXTURES_DIR = os.path.join(FILE_DIR, "fixtures")


def _filter_comments(s):
    return "\n".join([line for line in s.splitlines() if line and not line.startswith("--")])


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
    output = "\n".join(str(args[0][0]) for args in getattr(logger, level).call_args_list)
    assert message in output


def load_sql_fixtures(filename):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        yield from _filter_comments(f.read()).splitlines()


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


def string_to_bool(string):
    if string is None:
        return False
    if string in (True, False):
        return string
    return string and string.lower() in ("true", "1")


SKIP_INTEGRATION = string_to_bool(os.environ.get("SKIP_INTEGRATION", "0").lower())

TPCH_SCHEMA = {
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
