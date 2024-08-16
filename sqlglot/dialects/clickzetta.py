from __future__ import annotations

import logging
from collections import defaultdict

from sqlglot import exp, transforms
from sqlglot.dialects.spark import Spark
from sqlglot.expressions import Div
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.dialects.dialect import (
    rename_func,
    if_sql,
 )

logger = logging.getLogger("sqlglot")

def is_read_dialect(target: str) -> bool:
    target = target.upper()
    import os
    read_dialect = os.environ.get('READ_DIALECT')
    if not read_dialect:
        return False
    if target == 'MYSQL' and read_dialect.upper() in ['MYSQL', 'PRESTO', 'TRINO', 'ATHENA', 'STARROCKS', 'DORIS']:
        return True
    if target == 'POSTGRES' and read_dialect.upper() in ['POSTGRES', 'REDSHIFT']:
        return True
    if target == read_dialect.upper():
        return True

    return False

def _transform_create(expression: exp.Expression) -> exp.Expression:
    """Remove index column constraints.
    Remove unique column constraint (due to not buggy input)."""
    schema = expression.this
    if isinstance(expression, exp.Create) and isinstance(schema, exp.Schema):
        to_remove = []
        for e in schema.expressions:
            if isinstance(e, exp.IndexColumnConstraint) or \
                    isinstance(e, exp.UniqueColumnConstraint):
                to_remove.append(e)
        for e in to_remove:
            schema.expressions.remove(e)
    return expression

def _groupconcat_to_wmconcat(self: ClickZetta.Generator, expression: exp.GroupConcat) -> str:
    this = self.sql(expression, "this")
    sep = expression.args.get('separator')
    if not sep:
        sep = exp.Literal.string(',')
    return f"WM_CONCAT({sep}, {self.sql(this)})"

def _anonymous_func(self: ClickZetta.Generator, expression: exp.Anonymous) -> str:
    if expression.this.upper() == 'DATETIME':
        # in MaxCompute, datetime(col) is an alias of cast(col as datetime)
        return f"{self.sql(expression.expressions[0])}::TIMESTAMP"
    elif expression.this.upper() == 'GETDATE':
        return f"CURRENT_TIMESTAMP()"
    elif expression.this.upper() == 'LAST_DAY_OF_MONTH':
        return f"LAST_DAY({self.sql(expression.expressions[0])})"
    elif expression.this.upper() == 'TO_ISO8601':
        return f"DATE_FORMAT({self.sql(expression.expressions[0])}, 'yyyy-MM-dd\\\'T\\\'hh:mm:ss.SSSxxx')"
    elif expression.this.upper() == 'AES_DECRYPT' and is_read_dialect('mysql'):
        return f"AES_DECRYPT_MYSQL({self.sql(expression.expressions[0])}, {self.sql(expression.expressions[1])})"
    elif expression.this.upper() == 'MAP_AGG':
        return f"MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT({self.expressions(expression)})))"
    elif expression.this.upper() == 'JSON_ARRAY_GET':
        return f"{self.sql(expression.expressions[0])}[{self.sql(expression.expressions[1])}]"
    elif expression.this.upper() == 'PARSE_DATETIME':
        return f"TO_TIMESTAMP({self.sql(expression.expressions[0])}, {self.sql(expression.expressions[1])})"
    elif expression.this.upper() == 'FROM_ISO8601_TIMESTAMP':
        return f"CAST({self.sql(expression.expressions[0])} AS TIMESTAMP)"
    elif expression.this.upper() == 'DOW':
        # dow in presto is an alias of day_of_week, which is equivalent to dayofweek_iso
        # https://prestodb.io/docs/current/functions/datetime.html#day_of_week-x-bigint
        # https://doc.clickzetta.com/en-US/sql_functions/scalar_functions/datetime_functions/dayofweek_iso
        return f"DAYOFWEEK_ISO({self.sql(expression.expressions[0])})"
    elif expression.this.upper() == 'DOY':
        return f"DAYOFYEAR({self.sql(expression.expressions[0])})"
    elif expression.this.upper() == 'YOW' or expression.this.upper() == 'YEAR_OF_WEEK':
        return f"YEAROFWEEK({self.sql(expression.expressions[0])})"
    # TODO: temporary workaround for presto 'select current_timezone()'
    elif expression.this.upper() == 'CURRENT_TIMEZONE':
        return f"'Asia/Shanghai'"
    elif expression.this.upper() == 'GROUPING':
        return f"GROUPING_ID({self.expressions(expression, flat=True)})"

    # return as it is
    args = ", ".join(self.sql(e) for e in expression.expressions)
    return f"{expression.this}({args})"

def nullif_to_if(self: ClickZetta.Generator, expression: exp.Nullif):
    cond = exp.EQ(this=expression.this, expression=expression.expression)
    ret = exp.If(this=cond, true=exp.Null(), false=expression.this)
    return self.sql(ret)

def unnest_to_values(self: ClickZetta.Generator, expression: exp.Unnest):
    if isinstance(expression.expressions, list) and len(expression.expressions) == 1 and isinstance(expression.expressions[0], exp.Array):
        array = expression.expressions[0].expressions
        alias = expression.args.get('alias')
        ret = exp.Values(expressions=array, alias=alias)
        return self.sql(ret)
    elif len(expression.expressions) == 1:
        ret = f"EXPLODE({self.sql(expression.expressions[0])})"
        alias = expression.args.get('alias')
        if alias:
            ret = f"{ret} AS {self.tablealias_sql(expression.args.get('alias'))}"
        return ret
    else:
        return f"UNNEST({self.sql(expression.expressions)})" # TODO: don't know what to do

def time_to_str(self: ClickZetta.Generator, expression: exp.TimeToStr):
    this = self.sql(expression, "this")
    if is_read_dialect('mysql'):
        return f"DATE_FORMAT_MYSQL({this}, {self.sql(expression, 'format')})"
    elif is_read_dialect('postgres'):
        return f"DATE_FORMAT_PG({this}, {self.sql(expression, 'format')})"

    # fallback to hive implementation
    time_format = self.format_time(expression)
    return f"DATE_FORMAT({this}, {time_format})"

def fill_tuple_with_column_name(self: ClickZetta.Generator, expression: exp.Tuple) -> str:
    if not isinstance(expression.parent, exp.Values) and not isinstance(expression.parent, exp.Group) and is_read_dialect('mysql'):
        elements = []
        for i, e in enumerate(expression.expressions):
            elements.append(f'{self.sql(e)} AS __c{i+1}')
        return f"({', '.join(elements)})"
    else:
        return f"({self.expressions(expression, flat=True)})"

def date_add_sql(self: ClickZetta.Generator, expression: exp.DateAdd) -> str:
    # this is a workaround since date_add in presto should be parsed as TsOrDsAdd
    # https://prestodb.io/docs/current/functions/datetime.html#date_add
    if is_read_dialect('presto'):
        unit = expression.args.get('unit')
        if isinstance(unit, exp.Var):
            unit_str = f"'{self.sql(unit)}'"
        else:
            unit_str = self.sql(unit)
        return f"TIMESTAMP_OR_DATE_ADD({unit_str}, {self.sql(expression.expression)}, {self.sql(expression.this)})"
    return f"DATEADD({self.sql(expression.args.get('unit'))}, {self.sql(expression.expression)}, {self.sql(expression.this)})"

def regexp_extract_sql(self: ClickZetta.Generator, expression: exp.RegexpExtract):
    bad_args = list(filter(expression.args.get, ("position", "occurrence", "parameters")))
    if bad_args:
        self.unsupported(f"REGEXP_EXTRACT does not support the following arg(s): {bad_args}")

    group = expression.args.get('group')
    if not group and is_read_dialect('presto'):
        return f"REGEXP_EXTRACT({self.sql(expression.this)}, {self.sql(expression.expression)}, 0)"

    return self.func(
        "REGEXP_EXTRACT", expression.this, expression.expression, expression.args.get('group')
    )

def adjust_day_of_week(self: ClickZetta.Generator, expression: exp.DayOfWeek):
    if is_read_dialect('presto'):
        return f'DAYOFWEEK_ISO({self.sql(expression.this)})'
    else:
        return f'DAYOFWEEK({self.sql(expression.this)})'

class ClickZetta(Spark):
    NULL_ORDERING = "nulls_are_small"
    LOG_BASE_FIRST = None

    class Tokenizer(Spark.Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "CREATE USER": TokenType.COMMAND,
            "DROP USER": TokenType.COMMAND,
            "SHOW USER": TokenType.COMMAND,
            "REVOKE": TokenType.COMMAND,
        }

    class Parser(Spark.Parser):
        pass

    class Generator(Spark.Generator):

        RESERVED_KEYWORDS = {'all', 'user', 'to', 'check', 'order'}

        TYPE_MAPPING = {
            **Spark.Generator.TYPE_MAPPING,
            exp.DataType.Type.MEDIUMTEXT: "STRING",
            exp.DataType.Type.LONGTEXT: "STRING",
            exp.DataType.Type.VARIANT: "STRING",
            exp.DataType.Type.ENUM: "STRING",
            exp.DataType.Type.ENUM16: "STRING",
            exp.DataType.Type.ENUM8: "STRING",
            # mysql unsigned types
            exp.DataType.Type.UINT: "INT",
            exp.DataType.Type.UTINYINT: "TINYINT",
            exp.DataType.Type.USMALLINT: "SMALLINT",
            exp.DataType.Type.UMEDIUMINT: "INT",
            exp.DataType.Type.UBIGINT: "BIGINT",
            exp.DataType.Type.UDECIMAL: "DECIMAL",
            # postgres serial types
            exp.DataType.Type.BIGSERIAL: "BIGINT",
            exp.DataType.Type.SERIAL: "INT",
            exp.DataType.Type.SMALLSERIAL: "SMALLINT",
            exp.DataType.Type.BIGDECIMAL: "DECIMAL",
        }

        PROPERTIES_LOCATION = {
            **Spark.Generator.PROPERTIES_LOCATION,
            # exp.DistributedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.PrimaryKey: exp.Properties.Location.POST_NAME,
            exp.EngineProperty: exp.Properties.Location.POST_SCHEMA,
        }

        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.DefaultColumnConstraint: lambda self, e: '',
            exp.OnUpdateColumnConstraint: lambda self, e: '',
            exp.AutoIncrementColumnConstraint: lambda self, e: '',
            exp.CollateColumnConstraint: lambda self, e: '',
            exp.CharacterSetColumnConstraint: lambda self, e: '',
            exp.Create: transforms.preprocess([_transform_create]),
            exp.GroupConcat: _groupconcat_to_wmconcat,
            exp.CurrentTime: lambda self, e: "DATE_FORMAT(NOW(),'HH:mm:ss')",
            exp.Anonymous: _anonymous_func,
            exp.AtTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", e.args.get("zone"), e.this
            ),
            exp.UnixToTime: lambda self, e: self.func(
                "CONVERT_TIMEZONE", "'UTC+0'", e.this
            ),
            # exp.DistributedByProperty: lambda self, e: self.distributedbyproperty_sql(e),
            exp.EngineProperty: lambda self, e: '',
            exp.TimeToStr: time_to_str,
            exp.Pow: rename_func("POW"),
            exp.ApproxQuantile: rename_func("APPROX_PERCENTILE"),
            exp.JSONFormat: rename_func("TO_JSON"),
            exp.ParseJSON: lambda self, e: f"JSON {self.sql(e.this)}",
            exp.Nullif: nullif_to_if,
            exp.If: if_sql(false_value=exp.Null()),
            exp.Unnest: unnest_to_values,
            exp.Try: lambda self, e: self.sql(e, "this"),
            exp.Tuple: fill_tuple_with_column_name,
            exp.GenerateSeries: rename_func("SEQUENCE"),
            exp.DateAdd: date_add_sql,
            exp.RegexpExtract: regexp_extract_sql,
            exp.DayOfWeek: adjust_day_of_week,
        }

        # def distributedbyproperty_sql(self, expression: exp.DistributedByProperty) -> str:
        #     expressions = self.expressions(expression, key="expressions", flat=True)
        #     sorted_by = self.expressions(expression, key="sorted_by", flat=True)
        #     sorted_by = f" SORTED BY ({sorted_by})" if sorted_by else ""
        #     buckets = self.sql(expression, "buckets")
        #     return f"HASH CLUSTERED BY ({expressions}){sorted_by} INTO {buckets} BUCKETS"

        def datatype_sql(self, expression: exp.DataType) -> str:
            """Remove unsupported type params from int types: eg. int(10) -> int
            Remove type param from enum series since it will be mapped as STRING."""
            type_value = expression.this
            type_sql = (
                self.TYPE_MAPPING.get(type_value, type_value.value)
                if isinstance(type_value, exp.DataType.Type)
                else type_value
            )
            if type_value in exp.DataType.INTEGER_TYPES or \
                type_value in {
                    exp.DataType.Type.UTINYINT,
                    exp.DataType.Type.USMALLINT,
                    exp.DataType.Type.UMEDIUMINT,
                    exp.DataType.Type.UINT,
                    exp.DataType.Type.UINT128,
                    exp.DataType.Type.UINT256,

                    exp.DataType.Type.ENUM,
               }:
                return type_sql
            return super().datatype_sql(expression)

        def tochar_sql(self, expression: exp.ToChar) -> str:
            this = expression.args.get('this')
            format = expression.args.get('format')
            if format:
                format_str = str(format).replace('mm', 'MM').replace('mi', 'mm')
                return f"DATE_FORMAT_PG({self.sql(this)}, {self.sql(format_str)})"

            return super().tochar_sql(expression)

        def preprocess(self, expression: exp.Expression) -> exp.Expression:
            """Apply generic preprocessing transformations to a given expression."""

            # do not move ctes to top levels

            if self.ENSURE_BOOLS:
                from sqlglot.transforms import ensure_bools

                expression = ensure_bools(expression)

            return expression