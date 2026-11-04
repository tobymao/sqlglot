"""
Tables that conformance cases query. Every table is created on each engine that all of its columns
have a physical type for, and verified after loading by reading it back and comparing it against
the rows below.

Guidelines when extending the dataset:
- Prefer adding rows or columns here over writing literals in cases, so every case benefits.
- Shared tables must only hold values that every engine represents identically (e.g. no Postgres
  'infinity' dates or DECIMAL NaN). Engine-specific values go into the *_extras tables.
- Keep TIMESTAMPTZ values a day away from the supported range limits, so they still fit when
  rendered in a non-UTC session time zone.
"""

from __future__ import annotations

import random
import typing as t
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID

from conformance.dataset.model import Table, column, shared

UTC = timezone.utc
D = Decimal

SCALAR_COLUMNS = [
    shared("id", "INTEGER", "INT"),
    shared("label", "TEXT", "STRING"),
    shared("v_bool", "BOOLEAN", "BOOLEAN"),
    shared("v_int2", "SMALLINT", "SMALLINT"),
    shared("v_int4", "INTEGER", "INT"),
    shared("v_int8", "BIGINT", "BIGINT"),
    shared("v_float4", "REAL", "FLOAT"),
    shared("v_float8", "DOUBLE PRECISION", "DOUBLE"),
    shared("v_dec", "NUMERIC(10, 2)", "DECIMAL(10, 2)"),
    shared("v_dec38", "NUMERIC(38, 10)", "DECIMAL(38, 10)"),
    shared("v_text", "TEXT", "STRING"),
    shared("v_varchar", "VARCHAR(20)", "VARCHAR(20)"),
    shared("v_char", "CHAR(5)", "CHAR(5)", char_length=5),
    shared("v_bytes", "BYTEA", "BINARY"),
    shared("v_date", "DATE", "DATE"),
    # A wall-clock timestamp: Spark's TIMESTAMP is an instant, so the equivalent is TIMESTAMP_NTZ
    shared("v_ts", "TIMESTAMP", "TIMESTAMP_NTZ"),
    shared("v_tstz", "TIMESTAMPTZ", "TIMESTAMP"),
    shared("v_int_arr", "INTEGER[]", "ARRAY<INT>"),
    shared("v_text_arr", "TEXT[]", "ARRAY<STRING>"),
    # Spark has no JSON type; JSON functions operate on strings
    shared("v_json", "JSONB", "STRING"),
]

_SCALAR_NAMES = [c.name for c in SCALAR_COLUMNS]


def _scalar(id: int, label: str, **values: t.Any) -> tuple:
    unknown = set(values) - set(_SCALAR_NAMES)
    if unknown:
        raise ValueError(f"unknown scalars columns: {sorted(unknown)}")
    values.update(id=id, label=label)
    return tuple(values.get(name) for name in _SCALAR_NAMES)


_TYPICAL = dict(
    v_bool=True,
    v_int2=1,
    v_int4=42,
    v_int8=1234567890123,
    v_float4=1.5,
    v_float8=3.141592653589793,
    v_dec=D("12345.67"),
    v_dec38=D("123456789012345678.1234567890"),
    v_text="hello world",
    v_varchar="Hello World",
    v_char="abc",
    v_bytes=b"\x01\x02\x03",
    v_date=date(2024, 2, 29),
    v_ts=datetime(2024, 2, 29, 12, 34, 56, 789012),
    v_tstz=datetime(2024, 2, 29, 12, 34, 56, 789012, tzinfo=UTC),
    v_int_arr=[1, 2, 3],
    v_text_arr=["a", "b", "c"],
    v_json='{"a": 1, "b": "x"}',
)


def _scalar_rows() -> list[tuple]:
    return [
        _scalar(1, "all nulls"),
        _scalar(
            2,
            "zeros and empties",
            v_bool=False,
            v_int2=0,
            v_int4=0,
            v_int8=0,
            v_float4=0.0,
            v_float8=0.0,
            v_dec=D("0.00"),
            v_dec38=D("0E-10"),
            v_text="",
            v_varchar="",
            v_char="",
            v_bytes=b"",
            v_date=date(1970, 1, 1),
            v_ts=datetime(1970, 1, 1),
            v_tstz=datetime(1970, 1, 1, tzinfo=UTC),
            v_int_arr=[],
            v_text_arr=[],
            v_json="{}",
        ),
        _scalar(3, "typical", **_TYPICAL),
        _scalar(4, "typical duplicate", **_TYPICAL),
        _scalar(
            5,
            "negative",
            v_bool=False,
            v_int2=-1,
            v_int4=-42,
            v_int8=-1234567890123,
            v_float4=-1.5,
            v_float8=-2.718281828459045,
            v_dec=D("-12345.67"),
            v_dec38=D("-0.0000000001"),
            v_text="Negative Value",
            v_varchar="MiXeD CaSe",
            v_char="x",
            v_bytes=b"\xff",
            v_date=date(1969, 12, 31),
            v_ts=datetime(1969, 12, 31, 23, 59, 59, 999999),
            v_tstz=datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
            v_int_arr=[-1, None, 3],
            v_text_arr=[None, "", "z"],
            v_json="[1, 2, 3]",
        ),
        _scalar(
            6,
            "maximum values",
            v_bool=True,
            v_int2=32767,
            v_int4=2147483647,
            v_int8=9223372036854775807,
            v_float4=3.4028234663852886e38,
            v_float8=1.7976931348623157e308,
            v_dec=D("99999999.99"),
            v_dec38=D("9999999999999999999999999999.9999999999"),
            v_text="zzz",
            v_varchar="ZZZ",
            v_char="zzzzz",
            v_bytes=b"\xff\xff\xff\xff",
            v_date=date(9999, 12, 31),
            v_ts=datetime(9999, 12, 31, 23, 59, 59, 999999),
            v_tstz=datetime(9999, 12, 30, 23, 59, 59, 999999, tzinfo=UTC),
            v_int_arr=[2147483647, -2147483648],
            v_text_arr=["~"],
            v_json='{"key": [1, {"x": null}], "nested": {"deep": true}}',
        ),
        _scalar(
            7,
            "minimum values",
            v_bool=False,
            v_int2=-32768,
            v_int4=-2147483648,
            v_int8=-9223372036854775808,
            v_float4=-3.4028234663852886e38,
            v_float8=-1.7976931348623157e308,
            v_dec=D("-99999999.99"),
            v_dec38=D("-9999999999999999999999999999.9999999999"),
            v_text="AAA",
            v_varchar="aaa",
            v_char="A",
            v_bytes=b"\x00",
            v_date=date(1, 1, 1),
            v_ts=datetime(1, 1, 1),
            v_tstz=datetime(1, 1, 2, tzinfo=UTC),
            v_int_arr=[0],
            v_text_arr=[""],
            v_json="[]",
        ),
        _scalar(
            8,
            "tiny magnitudes",
            v_int2=1,
            v_int4=1,
            v_int8=1,
            v_float4=1e-30,
            v_float8=2.2250738585072014e-308,
            v_dec=D("0.01"),
            v_dec38=D("0.0000000001"),
            v_json="1.5",
        ),
        _scalar(
            9, "NaN", v_float4=float("nan"), v_float8=float("nan"), v_json='{"n": null, "t": true}'
        ),
        _scalar(10, "+Infinity", v_float4=float("inf"), v_float8=float("inf"), v_json="null"),
        _scalar(
            11,
            "-Infinity",
            v_float4=float("-inf"),
            v_float8=float("-inf"),
            v_json='"just a string"',
        ),
        _scalar(12, "negative zero", v_int4=0, v_float4=-0.0, v_float8=-0.0, v_dec=D("-0.00")),
        _scalar(
            13,
            "quotes and escapes",
            v_text='it\'s a "quote" and a \\ backslash',
            v_varchar="tab\there",
            v_char="'",
            v_bytes=b"\\",
            v_text_arr=["a,b", 'c"d', "{}", "NULL"],
            v_json='{"s": "a\\"b"}',
        ),
        _scalar(
            14,
            "whitespace",
            v_text="  padded  ",
            v_varchar=" ",
            v_char="ab",
            v_text_arr=[" ", "\t"],
        ),
        _scalar(15, "newlines", v_text="line1\nline2\r\nline3", v_varchar="\n", v_char="\t"),
        _scalar(
            16,
            "unicode",
            v_text="héllo wörld ß ǅ",
            v_varchar="Ελληνικά",
            v_char="ñ",
            v_bytes="é".encode(),
            v_text_arr=["日本", "😀"],
            v_json='{"u": "ü"}',
        ),
        _scalar(17, "emoji", v_text="😀👍🏽 family 👨‍👩‍👧", v_varchar="🎉", v_char="😀"),
        _scalar(18, "LIKE metacharacters", v_text="50% off_sale", v_varchar="%", v_char="_"),
        _scalar(
            19,
            "numeric strings",
            v_text="123",
            v_varchar=" 42 ",
            v_char="1e3",
            v_text_arr=["1", "2.5", "abc"],
        ),
        _scalar(
            20, "date strings", v_text="2024-01-15", v_varchar="2024-01-15 10:30:00", v_char="Jan"
        ),
        _scalar(21, "sort order", v_text="a", v_varchar="apple"),
        _scalar(22, "sort order", v_text="A", v_varchar="Apple"),
        _scalar(23, "sort order", v_text="b", v_varchar="banana"),
        _scalar(24, "sort order", v_text="B", v_varchar="Banana"),
        _scalar(25, "sort order", v_text="á", v_varchar="äpple"),
        _scalar(26, "sort order", v_text="_", v_varchar=" apple"),
        _scalar(
            27,
            "leap day",
            v_date=date(2000, 2, 29),
            v_ts=datetime(2023, 1, 31, 23, 59, 59),
            v_tstz=datetime(2023, 12, 31, 23, 59, 59, 500000, tzinfo=UTC),
        ),
        _scalar(
            28,
            "month end",
            v_date=date(2023, 2, 28),
            v_ts=datetime(2024, 2, 29),
            v_tstz=datetime(2024, 3, 10, 10, 0, tzinfo=UTC),
        ),
        _scalar(
            29,
            "year boundary",
            v_date=date(2020, 12, 31),
            v_ts=datetime(2021, 1, 1),
            v_tstz=datetime(2020, 12, 31, 23, 0, tzinfo=UTC),
        ),
        _scalar(
            30,
            "US DST transitions",
            v_date=date(2024, 11, 3),
            v_ts=datetime(2024, 3, 10, 2, 30),
            v_tstz=datetime(2024, 11, 3, 8, 30, tzinfo=UTC),
        ),
        _scalar(
            31,
            "fractional seconds, Gregorian cutover",
            v_date=date(1582, 10, 10),
            v_ts=datetime(2024, 1, 1, 0, 0, 0, 100000),
            v_tstz=datetime(2024, 1, 1, 0, 0, 0, 123456, tzinfo=UTC),
        ),
        _scalar(
            32,
            "before 1900",
            v_date=date(1900, 1, 1),
            v_ts=datetime(1899, 12, 31, 12, 0),
            v_tstz=datetime(1900, 1, 1, tzinfo=UTC),
        ),
    ]


FACTS_ROWS = 10_000


def _facts_rows() -> list[tuple]:
    """A deterministic, skewed table with NULLs and ties, for aggregates, windows and joins."""
    rng = random.Random(20240229)
    categories = ["alpha", "beta", "gamma", "delta", "Alpha", "", None]
    category_weights = [30, 25, 15, 10, 5, 5, 10]
    rows = []

    for i in range(1, FACTS_ROWS + 1):
        grp = None if rng.random() < 0.02 else min(int(rng.paretovariate(1.0)) - 1, 49)
        category = rng.choices(categories, category_weights)[0]
        qty = None if rng.random() < 0.05 else rng.randint(-50, 1000)
        price = None if rng.random() < 0.03 else D(rng.randint(0, 10_000_000)).scaleb(-2)
        score = None if rng.random() < 0.03 else round(rng.gauss(50, 15), 6)
        flag = rng.choices([True, False, None], [45, 45, 10])[0]
        day = date(2019, 1, 1) + timedelta(days=rng.randint(0, 2555))
        ts = datetime(2019, 1, 1) + timedelta(
            seconds=rng.randint(0, 220_000_000), microseconds=rng.choice([0, 0, 0, 500000, 123456])
        )
        rows.append((i, grp, category, qty, price, score, flag, day, ts))

    return rows


_JOIN_COLUMNS = [
    shared("id", "INTEGER", "INT"),
    shared("k", "INTEGER", "INT"),
    shared("val", "TEXT", "STRING"),
]

TABLES = [
    Table(
        "scalars",
        SCALAR_COLUMNS,
        _scalar_rows,
        "One row per edge case (see `label`), one column per type",
    ),
    Table(
        "facts",
        [
            shared("id", "BIGINT", "BIGINT"),
            shared("grp", "INTEGER", "INT"),
            shared("cat", "TEXT", "STRING"),
            shared("qty", "INTEGER", "INT"),
            shared("price", "NUMERIC(12, 2)", "DECIMAL(12, 2)"),
            shared("score", "DOUBLE PRECISION", "DOUBLE"),
            shared("flag", "BOOLEAN", "BOOLEAN"),
            shared("d", "DATE", "DATE"),
            shared("ts", "TIMESTAMP", "TIMESTAMP_NTZ"),
        ],
        _facts_rows,
        f"{FACTS_ROWS} generated rows with skew, ties and NULLs",
    ),
    Table(
        "join_left",
        _JOIN_COLUMNS,
        lambda: [
            (1, 1, "l1"),
            (2, 2, "l2"),
            (3, 2, "l3"),
            (4, None, "l4"),
            (5, 5, "l5"),
            (6, None, "l6"),
        ],
        "Join keys with duplicates, NULLs and keys missing from join_right",
    ),
    Table(
        "join_right",
        _JOIN_COLUMNS,
        lambda: [(1, 1, "r1"), (2, 2, "r2"), (3, 3, "r3"), (4, None, "r4"), (5, 1, "r5")],
        "Join keys with duplicates, NULLs and keys missing from join_left",
    ),
    Table(
        "pg_extras",
        [
            column("id", postgres="INTEGER"),
            column("v_uuid", postgres="UUID"),
            column("v_time", postgres="TIME"),
            column("v_interval", postgres="INTERVAL"),
            column("v_numeric", postgres="NUMERIC"),
            column("v_json", postgres="JSON"),
            column("v_inet", postgres="INET"),
            column("v_int_matrix", postgres="INTEGER[][]"),
        ],
        lambda: [
            (
                1,
                UUID("00000000-0000-0000-0000-000000000000"),
                time(0, 0),
                timedelta(0),
                D("0"),
                "{}",
                IPv4Address("127.0.0.1"),
                [[1, 2], [3, 4]],
            ),
            (
                2,
                UUID("123e4567-e89b-12d3-a456-426614174000"),
                time(23, 59, 59, 999999),
                timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500000),
                D("3.14159265358979323846264338327950288419716939937510"),
                '{"b" : [1,2],  "a":1}',
                IPv6Address("::1"),
                [[5]],
            ),
            (3, None, None, None, None, None, None, None),
            (
                4,
                UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                time(12, 30),
                timedelta(hours=-3),
                D("-100000000000000000000"),
                "null",
                IPv4Address("10.0.0.1"),
                [],
            ),
        ],
        "Postgres-only types",
    ),
    Table(
        "spark_extras",
        [
            column("id", spark="INT"),
            column("v_tinyint", spark="TINYINT"),
            column("v_map", spark="MAP<STRING, INT>"),
            column("v_struct", spark="STRUCT<a: INT, b: STRING>"),
            column("v_struct_arr", spark="ARRAY<STRUCT<x: INT, y: STRING>>"),
            column("v_nested_arr", spark="ARRAY<ARRAY<INT>>"),
            column("v_map_arr", spark="MAP<STRING, ARRAY<INT>>"),
        ],
        lambda: [
            (
                1,
                127,
                {"a": 1, "b": 2},
                {"a": 1, "b": "x"},
                [{"x": 1, "y": "p"}, {"x": 2, "y": None}],
                [[1, 2], [], None],
                {"k": [1, None]},
            ),
            (2, -128, {}, {"a": None, "b": None}, [], [], {}),
            (3, None, None, None, None, None, None),
            (4, 0, {"z": None}, {"a": -1, "b": ""}, [None], [[None]], {"e": []}),
        ],
        "Spark-only types",
    ),
]
