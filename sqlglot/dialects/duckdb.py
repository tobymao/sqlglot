from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    arrow_json_extract_scalar_sql,
    format_time_lambda,
    no_safe_divide_sql,
    no_tablesample_sql,
    rename_func,
)
from sqlglot.generator import Generator
from sqlglot.helper import list_get
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


def _unix_to_time(self, expression):
    return f"TO_TIMESTAMP(CAST({self.sql(expression, 'this')} AS BIGINT))"


def _ts_or_ds_add(self, expression):
    this = self.sql(expression, "this")
    e = self.sql(expression, "expression")
    unit = self.sql(expression, "unit").strip("'") or "DAY"
    return f"STRFTIME(CAST({this} AS DATE) + INTERVAL {e} {unit}, {DuckDB.date_format})"


def _date_add(self, expression):
    this = self.sql(expression, "this")
    e = self.sql(expression, "expression")
    unit = self.sql(expression, "unit").strip("'") or "DAY"
    return f"{this} + INTERVAL {e} {unit}"


def _struct_pack_sql(self, expression):
    args = [
        self.binary(e, ":=") if isinstance(e, exp.EQ) else self.sql(e)
        for e in expression.expressions
    ]
    return f"STRUCT_PACK({', '.join(args)})"


class DuckDB(Dialect):
    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            ":=": TokenType.EQ,
        }

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "APPROX_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "ARRAY_LENGTH": exp.ArraySize.from_arg_list,
            "EPOCH": exp.TimeToUnix.from_arg_list,
            "EPOCH_MS": lambda args: exp.UnixToTime(
                this=exp.Div(
                    this=list_get(args, 0),
                    expression=exp.Literal.number(1000),
                )
            ),
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
            "TO_TIMESTAMP": exp.TimeStrToTime.from_arg_list,
            "UNNEST": exp.Explode.from_arg_list,
        }

    class Generator(Generator):
        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.Array: lambda self, e: f"LIST_VALUE({self.expressions(e, flat=True)})",
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.ArraySum: rename_func("LIST_SUM"),
            exp.DateAdd: _date_add,
            exp.DateDiff: lambda self, e: f"""DATE_DIFF({self.sql(e, 'unit') or "'day'"}, {self.sql(e, 'expression')}, {self.sql(e, 'this')})""",
            exp.DateStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.DateToDateStr: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, {DuckDB.date_format})",
            exp.DateToDi: lambda self, e: f"CAST(STRFTIME({self.sql(e, 'this')}, {DuckDB.dateint_format}) AS INT)",
            exp.DiToDate: lambda self, e: f"CAST(STRPTIME(CAST({self.sql(e, 'this')} AS STRING), {DuckDB.dateint_format}) AS DATE)",
            exp.Explode: rename_func("UNNEST"),
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONBExtract: arrow_json_extract_sql,
            exp.JSONBExtractScalar: arrow_json_extract_scalar_sql,
            exp.RegexpLike: rename_func("REGEXP_MATCHES"),
            exp.RegexpSplit: rename_func("STR_SPLIT_REGEX"),
            exp.SafeDivide: no_safe_divide_sql,
            exp.Split: rename_func("STR_SPLIT"),
            exp.StrToTime: lambda self, e: f"STRPTIME({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.StrToUnix: lambda self, e: f"EPOCH(STRPTIME({self.sql(e, 'this')}, {self.format_time(e)}))",
            exp.Struct: _struct_pack_sql,
            exp.TableSample: no_tablesample_sql,
            exp.TimeStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.TimeStrToTime: lambda self, e: f"CAST({self.sql(e, 'this')} AS TIMESTAMP)",
            exp.TimeStrToUnix: lambda self, e: f"EPOCH(CAST({self.sql(e, 'this')} AS TIMESTAMP))",
            exp.TimeToStr: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToTimeStr: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, {DuckDB.time_format})",
            exp.TimeToUnix: rename_func("EPOCH"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS STRING), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: _ts_or_ds_add,
            exp.TsOrDsToDateStr: lambda self, e: f"STRFTIME(CAST({self.sql(e, 'this')} AS DATE), {DuckDB.date_format})",
            exp.TsOrDsToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.UnixToStr: lambda self, e: f"STRFTIME({_unix_to_time(self, e)}, {self.format_time(e)})",
            exp.UnixToTime: _unix_to_time,
            exp.UnixToTimeStr: lambda self, e: f"STRFTIME({_unix_to_time(self, e)}, {DuckDB.time_format})",
        }
