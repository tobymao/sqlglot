"""TPC-H tables, loaded from the scaled-down data that the optimizer tests already use."""

from __future__ import annotations

import csv
import gzip
import typing as t
from datetime import date
from decimal import Decimal
from pathlib import Path

from conformance.dataset.model import Table, shared

TPCH_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "optimizer" / "tpc-h"

_PHYSICAL = {
    "bigint": ("BIGINT", "BIGINT"),
    "int": ("INTEGER", "INT"),
    "decimal": ("NUMERIC(15, 2)", "DECIMAL(15, 2)"),
    "date": ("DATE", "DATE"),
    "text": ("TEXT", "STRING"),
}

_PARSERS: dict[str, t.Callable[[str], t.Any]] = {
    "bigint": int,
    "int": int,
    "decimal": Decimal,
    "date": date.fromisoformat,
    "text": str,
}

_SCHEMA = {
    "lineitem": "l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber int, "
    "l_quantity decimal, l_extendedprice decimal, l_discount decimal, l_tax decimal, "
    "l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, "
    "l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text",
    "orders": "o_orderkey bigint, o_custkey bigint, o_orderstatus text, o_totalprice decimal, "
    "o_orderdate date, o_orderpriority text, o_clerk text, o_shippriority int, o_comment text",
    "customer": "c_custkey bigint, c_name text, c_address text, c_nationkey bigint, c_phone text, "
    "c_acctbal decimal, c_mktsegment text, c_comment text",
    "part": "p_partkey bigint, p_name text, p_mfgr text, p_brand text, p_type text, p_size int, "
    "p_container text, p_retailprice decimal, p_comment text",
    "supplier": "s_suppkey bigint, s_name text, s_address text, s_nationkey bigint, s_phone text, "
    "s_acctbal decimal, s_comment text",
    "partsupp": "ps_partkey bigint, ps_suppkey bigint, ps_availqty int, ps_supplycost decimal, "
    "ps_comment text",
    "nation": "n_nationkey bigint, n_name text, n_regionkey bigint, n_comment text",
    "region": "r_regionkey bigint, r_name text, r_comment text",
}


def _table(name: str, schema: str) -> Table:
    pairs = [tuple(part.split()) for part in schema.split(", ")]
    names = [column for column, _ in pairs]
    parsers = [_PARSERS[kind] for _, kind in pairs]

    def build_rows() -> list[tuple]:
        with gzip.open(TPCH_DIR / f"{name}.csv.gz", "rt", encoding="utf-8", newline="") as file:
            reader = csv.reader(file, delimiter="|", quoting=csv.QUOTE_NONE)
            header = next(reader)
            if header != names:
                raise ValueError(f"unexpected {name} columns: {header}")
            return [tuple(parse(value) for parse, value in zip(parsers, row)) for row in reader]

    columns = [shared(column, *_PHYSICAL[kind]) for column, kind in pairs]
    return Table(name, columns, build_rows, "TPC-H")


TPCH_TABLES = [_table(name, schema) for name, schema in _SCHEMA.items()]
