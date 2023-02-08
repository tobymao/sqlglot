from __future__ import annotations

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    approx_count_distinct_sql,
    arrow_json_extract_scalar_sql,
    arrow_json_extract_sql,
    datestrtodate_sql,
    format_time_lambda,
    no_pivot_sql,
    no_properties_sql,
    no_safe_divide_sql,
    no_tablesample_sql,
    rename_func,
    str_position_sql,
    timestrtotime_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _str_to_time_sql(self, expression):
    return f"STRPTIME({self.sql(expression, 'this')}, {self.format_time(expression)})"


def _ts_or_ds_add(self, expression):
    this = expression.args.get("this")
    unit = self.sql(expression, "unit").strip("'") or "DAY"
    return f"CAST({this} AS DATE) + {self.sql(exp.Interval(this=expression.expression, unit=unit))}"


def _ts_or_ds_to_date_sql(self, expression):
    time_format = self.format_time(expression)
    if time_format and time_format not in (DuckDB.time_format, DuckDB.date_format):
        return f"CAST({_str_to_time_sql(self, expression)} AS DATE)"
    return f"CAST({self.sql(expression, 'this')} AS DATE)"


def _date_add(self, expression):
    this = self.sql(expression, "this")
    unit = self.sql(expression, "unit").strip("'") or "DAY"
    return f"{this} + {self.sql(exp.Interval(this=expression.expression, unit=unit))}"


def _array_sort_sql(self, expression):
    if expression.expression:
        self.unsupported("DUCKDB ARRAY_SORT does not support a comparator")
    return f"ARRAY_SORT({self.sql(expression, 'this')})"


def _sort_array_sql(self, expression):
    this = self.sql(expression, "this")
    if expression.args.get("asc") == exp.false():
        return f"ARRAY_REVERSE_SORT({this})"
    return f"ARRAY_SORT({this})"


def _sort_array_reverse(args):
    return exp.SortArray(this=seq_get(args, 0), asc=exp.false())


def _struct_sql(self, expression):
    args = [
        f"'{e.name or e.this.name}': {self.sql(e, 'expression')}" for e in expression.expressions
    ]
    return f"{{{', '.join(args)}}}"


def _datatype_sql(self, expression):
    if expression.this == exp.DataType.Type.ARRAY:
        return f"{self.expressions(expression, flat=True)}[]"
    return self.datatype_sql(expression)


class DuckDB(Dialect):
    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            ":=": TokenType.EQ,
            "CHARACTER VARYING": TokenType.VARCHAR,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "APPROX_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "ARRAY_LENGTH": exp.ArraySize.from_arg_list,
            "ARRAY_SORT": exp.SortArray.from_arg_list,
            "ARRAY_REVERSE_SORT": _sort_array_reverse,
            "EPOCH": exp.TimeToUnix.from_arg_list,
            "EPOCH_MS": lambda args: exp.UnixToTime(
                this=exp.Div(
                    this=seq_get(args, 0),
                    expression=exp.Literal.number(1000),
                )
            ),
            "LIST_SORT": exp.SortArray.from_arg_list,
            "LIST_REVERSE_SORT": _sort_array_reverse,
            "LIST_VALUE": exp.Array.from_arg_list,
            "REGEXP_MATCHES": exp.RegexpLike.from_arg_list,
            "STRFTIME": format_time_lambda(exp.TimeToStr, "duckdb"),
            "STRPTIME": format_time_lambda(exp.StrToTime, "duckdb"),
            "STR_SPLIT": exp.Split.from_arg_list,
            "STRING_SPLIT": exp.Split.from_arg_list,
            "STRING_TO_ARRAY": exp.Split.from_arg_list,
            "STR_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
            "STRING_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
            "STRUCT_PACK": exp.Struct.from_arg_list,
            "TO_TIMESTAMP": exp.UnixToTime.from_arg_list,
            "UNNEST": exp.Explode.from_arg_list,
        }

    class Generator(generator.Generator):
        STRUCT_DELIMITER = ("(", ")")

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.Array: lambda self, e: f"{self.normalize_func('ARRAY')}({self.sql(e.expressions[0])})"
            if isinstance(seq_get(e.expressions, 0), exp.Select)
            else rename_func("LIST_VALUE")(self, e),
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.ArraySort: _array_sort_sql,
            exp.ArraySum: rename_func("LIST_SUM"),
            exp.DataType: _datatype_sql,
            exp.DateAdd: _date_add,
            exp.DateDiff: lambda self, e: f"""DATE_DIFF({self.format_args(e.args.get("unit") or "'day'", e.expression, e.this)})""",
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateToDi: lambda self, e: f"CAST(STRFTIME({self.sql(e, 'this')}, {DuckDB.dateint_format}) AS INT)",
            exp.DiToDate: lambda self, e: f"CAST(STRPTIME(CAST({self.sql(e, 'this')} AS TEXT), {DuckDB.dateint_format}) AS DATE)",
            exp.Explode: rename_func("UNNEST"),
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONBExtract: arrow_json_extract_sql,
            exp.JSONBExtractScalar: arrow_json_extract_scalar_sql,
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.Pivot: no_pivot_sql,
            exp.Properties: no_properties_sql,
            exp.RegexpLike: rename_func("REGEXP_MATCHES"),
            exp.RegexpSplit: rename_func("STR_SPLIT_REGEX"),
            exp.SafeDivide: no_safe_divide_sql,
            exp.Split: rename_func("STR_SPLIT"),
            exp.SortArray: _sort_array_sql,
            exp.StrPosition: str_position_sql,
            exp.StrToDate: lambda self, e: f"CAST({_str_to_time_sql(self, e)} AS DATE)",
            exp.StrToTime: _str_to_time_sql,
            exp.StrToUnix: lambda self, e: f"EPOCH(STRPTIME({self.sql(e, 'this')}, {self.format_time(e)}))",
            exp.Struct: _struct_sql,
            exp.TableSample: no_tablesample_sql,
            exp.TimeStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: lambda self, e: f"EPOCH(CAST({self.sql(e, 'this')} AS TIMESTAMP))",
            exp.TimeToStr: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToUnix: rename_func("EPOCH"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS TEXT), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: _ts_or_ds_add,
            exp.TsOrDsToDate: _ts_or_ds_to_date_sql,
            exp.UnixToStr: lambda self, e: f"STRFTIME(TO_TIMESTAMP({self.sql(e, 'this')}), {self.format_time(e)})",
            exp.UnixToTime: rename_func("TO_TIMESTAMP"),
            exp.UnixToTimeStr: lambda self, e: f"CAST(TO_TIMESTAMP({self.sql(e, 'this')}) AS TEXT)",
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.VARCHAR: "TEXT",
            exp.DataType.Type.NVARCHAR: "TEXT",
        }
