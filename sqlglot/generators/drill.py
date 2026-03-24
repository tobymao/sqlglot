from __future__ import annotations

from sqlglot import exp, generator, transforms
from sqlglot.dialects.dialect import (
    datestrtodate_sql,
    no_trycast_sql,
    rename_func,
    strposition_sql,
    timestrtotime_sql,
)
from sqlglot.dialects.mysql import date_add_sql
from sqlglot.transforms import preprocess, move_schema_columns_to_partitioned_by
from sqlglot.generator import unsupported_args


def _str_to_date(self: DrillGenerator, expression: exp.StrToDate) -> str:
    from sqlglot.dialects.drill import Drill

    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format == Drill.DATE_FORMAT:
        return self.sql(exp.cast(this, exp.DType.DATE))
    return self.func("TO_DATE", this, time_format)


class DrillGenerator(generator.Generator):
    JOIN_HINTS = False
    TABLE_HINTS = False
    QUERY_HINTS = False
    NVL2_SUPPORTED = False
    LAST_DAY_SUPPORTS_DATE_PART = False
    SUPPORTS_CREATE_TABLE_LIKE = False
    ARRAY_SIZE_NAME = "REPEATED_COUNT"

    TYPE_MAPPING = {
        **generator.Generator.TYPE_MAPPING,
        exp.DType.INT: "INTEGER",
        exp.DType.SMALLINT: "INTEGER",
        exp.DType.TINYINT: "INTEGER",
        exp.DType.BINARY: "VARBINARY",
        exp.DType.TEXT: "VARCHAR",
        exp.DType.NCHAR: "VARCHAR",
        exp.DType.TIMESTAMPLTZ: "TIMESTAMP",
        exp.DType.TIMESTAMPTZ: "TIMESTAMP",
        exp.DType.DATETIME: "TIMESTAMP",
    }

    PROPERTIES_LOCATION = {
        **generator.Generator.PROPERTIES_LOCATION,
        exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
        exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
    }

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
        exp.ArrayContains: rename_func("REPEATED_CONTAINS"),
        exp.Create: preprocess([move_schema_columns_to_partitioned_by]),
        exp.DateAdd: date_add_sql("ADD"),
        exp.DateStrToDate: datestrtodate_sql,
        exp.DateSub: date_add_sql("SUB"),
        exp.DateToDi: lambda self, e: (
            f"CAST(TO_DATE({self.sql(e, 'this')}, {_drill_dateint_format()}) AS INT)"
        ),
        exp.DiToDate: lambda self, e: (
            f"TO_DATE(CAST({self.sql(e, 'this')} AS VARCHAR), {_drill_dateint_format()})"
        ),
        exp.If: lambda self, e: (
            f"`IF`({self.format_args(e.this, e.args.get('true'), e.args.get('false'))})"
        ),
        exp.ILike: lambda self, e: self.binary(e, "`ILIKE`"),
        exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
            rename_func("LEVENSHTEIN_DISTANCE")
        ),
        exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
        exp.RegexpLike: rename_func("REGEXP_MATCHES"),
        exp.StrToDate: _str_to_date,
        exp.Pow: rename_func("POW"),
        exp.Select: transforms.preprocess(
            [transforms.eliminate_distinct_on, transforms.eliminate_semi_and_anti_joins]
        ),
        exp.StrPosition: strposition_sql,
        exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
        exp.TimeStrToDate: lambda self, e: self.sql(exp.cast(e.this, exp.DType.DATE)),
        exp.TimeStrToTime: timestrtotime_sql,
        exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
        exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
        exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
        exp.ToChar: lambda self, e: self.function_fallback_sql(e),
        exp.TryCast: no_trycast_sql,
        exp.TsOrDsAdd: lambda self, e: (
            f"DATE_ADD(CAST({self.sql(e, 'this')} AS DATE), {self.sql(exp.Interval(this=e.expression, unit=exp.var('DAY')))})"
        ),
        exp.TsOrDiToDi: lambda self, e: (
            f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS VARCHAR), '-', ''), 1, 8) AS INT)"
        ),
    }


def _drill_dateint_format() -> str:
    from sqlglot.dialects.drill import Drill

    return Drill.DATEINT_FORMAT
