from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import (
    build_formatted_time,
    build_json_extract_path,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import mypyc_attr, seq_get
from sqlglot.parsers.mysql import MySQLParser, _show_parser
from sqlglot.tokens import TokenType


def cast_to_time6(
    expression: t.Optional[exp.Expr], time_type: exp.DType = exp.DType.TIME
) -> exp.Cast:
    return exp.Cast(
        this=expression,
        to=exp.DataType.build(
            time_type,
            expressions=[exp.DataTypeParam(this=exp.Literal.number(6))],
        ),
    )


@mypyc_attr(allow_interpreted_subclasses=True)
class SingleStoreParser(MySQLParser):
    FUNCTIONS = {
        **MySQLParser.FUNCTIONS,
        "TO_DATE": build_formatted_time(exp.TsOrDsToDate, "singlestore"),
        "TO_TIMESTAMP": build_formatted_time(exp.StrToTime, "singlestore"),
        "TO_CHAR": build_formatted_time(exp.ToChar, "singlestore"),
        "STR_TO_DATE": build_formatted_time(exp.StrToDate, "mysql"),
        "DATE_FORMAT": build_formatted_time(exp.TimeToStr, "mysql"),
        # The first argument of following functions is converted to TIME(6)
        # This is needed because exp.TimeToStr is converted to DATE_FORMAT
        # which interprets the first argument as DATETIME and fails to parse
        # string literals like '12:05:47' without a date part.
        "TIME_FORMAT": lambda args: exp.TimeToStr(
            this=cast_to_time6(seq_get(args, 0)),
            format=MySQL.format_time(seq_get(args, 1)),
        ),
        "HOUR": lambda args: exp.cast(
            exp.TimeToStr(
                this=cast_to_time6(seq_get(args, 0)),
                format=MySQL.format_time(exp.Literal.string("%k")),
            ),
            exp.DType.INT,
        ),
        "MICROSECOND": lambda args: exp.cast(
            exp.TimeToStr(
                this=cast_to_time6(seq_get(args, 0)),
                format=MySQL.format_time(exp.Literal.string("%f")),
            ),
            exp.DType.INT,
        ),
        "SECOND": lambda args: exp.cast(
            exp.TimeToStr(
                this=cast_to_time6(seq_get(args, 0)),
                format=MySQL.format_time(exp.Literal.string("%s")),
            ),
            exp.DType.INT,
        ),
        "MINUTE": lambda args: exp.cast(
            exp.TimeToStr(
                this=cast_to_time6(seq_get(args, 0)),
                format=MySQL.format_time(exp.Literal.string("%i")),
            ),
            exp.DType.INT,
        ),
        "MONTHNAME": lambda args: exp.TimeToStr(
            this=seq_get(args, 0),
            format=MySQL.format_time(exp.Literal.string("%M")),
        ),
        "WEEKDAY": lambda args: exp.paren(exp.DayOfWeek(this=seq_get(args, 0)) + 5, copy=False) % 7,
        "UNIX_TIMESTAMP": exp.StrToUnix.from_arg_list,
        "FROM_UNIXTIME": build_formatted_time(exp.UnixToTime, "mysql"),
        "TIME_BUCKET": lambda args: exp.DateBin(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            origin=seq_get(args, 2),
        ),
        "BSON_EXTRACT_BSON": build_json_extract_path(exp.JSONBExtract),
        "BSON_EXTRACT_STRING": build_json_extract_path(exp.JSONBExtractScalar, json_type="STRING"),
        "BSON_EXTRACT_DOUBLE": build_json_extract_path(exp.JSONBExtractScalar, json_type="DOUBLE"),
        "BSON_EXTRACT_BIGINT": build_json_extract_path(exp.JSONBExtractScalar, json_type="BIGINT"),
        "JSON_EXTRACT_JSON": build_json_extract_path(exp.JSONExtract),
        "JSON_EXTRACT_STRING": build_json_extract_path(exp.JSONExtractScalar, json_type="STRING"),
        "JSON_EXTRACT_DOUBLE": build_json_extract_path(exp.JSONExtractScalar, json_type="DOUBLE"),
        "JSON_EXTRACT_BIGINT": build_json_extract_path(exp.JSONExtractScalar, json_type="BIGINT"),
        "JSON_ARRAY_CONTAINS_STRING": lambda args: exp.JSONArrayContains(
            this=seq_get(args, 1),
            expression=seq_get(args, 0),
            json_type="STRING",
        ),
        "JSON_ARRAY_CONTAINS_DOUBLE": lambda args: exp.JSONArrayContains(
            this=seq_get(args, 1),
            expression=seq_get(args, 0),
            json_type="DOUBLE",
        ),
        "JSON_ARRAY_CONTAINS_JSON": lambda args: exp.JSONArrayContains(
            this=seq_get(args, 1),
            expression=seq_get(args, 0),
            json_type="JSON",
        ),
        "JSON_KEYS": lambda args: exp.JSONKeys(
            this=seq_get(args, 0),
            expressions=args[1:],
        ),
        "JSON_PRETTY": exp.JSONFormat.from_arg_list,
        "JSON_BUILD_ARRAY": lambda args: exp.JSONArray(expressions=args),
        "JSON_BUILD_OBJECT": lambda args: exp.JSONObject(expressions=args),
        "DATE": exp.Date.from_arg_list,
        "DAYNAME": lambda args: exp.TimeToStr(
            this=seq_get(args, 0),
            format=MySQL.format_time(exp.Literal.string("%W")),
        ),
        "TIMESTAMPDIFF": lambda args: exp.TimestampDiff(
            this=seq_get(args, 2),
            expression=seq_get(args, 1),
            unit=seq_get(args, 0),
        ),
        "APPROX_COUNT_DISTINCT": exp.Hll.from_arg_list,
        "APPROX_PERCENTILE": lambda args, dialect: exp.ApproxQuantile(
            this=seq_get(args, 0),
            quantile=seq_get(args, 1),
            error_tolerance=seq_get(args, 2),
        ),
        "VARIANCE": exp.VariancePop.from_arg_list,
        "INSTR": exp.Contains.from_arg_list,
        "REGEXP_MATCH": lambda args: exp.RegexpExtractAll(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            parameters=seq_get(args, 2),
        ),
        "REGEXP_SUBSTR": lambda args: exp.RegexpExtract(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            position=seq_get(args, 2),
            occurrence=seq_get(args, 3),
            parameters=seq_get(args, 4),
        ),
        "REDUCE": lambda args: exp.Reduce(
            initial=seq_get(args, 0),
            this=seq_get(args, 1),
            merge=seq_get(args, 2),
        ),
    }

    FUNCTION_PARSERS = {
        **MySQLParser.FUNCTION_PARSERS,
        "JSON_AGG": lambda self: exp.JSONArrayAgg(
            this=self._parse_term(),
            order=self._parse_order(),
        ),
    }

    NO_PAREN_FUNCTIONS = {
        **MySQLParser.NO_PAREN_FUNCTIONS,
        TokenType.UTC_DATE: exp.UtcDate,
        TokenType.UTC_TIME: exp.UtcTime,
        TokenType.UTC_TIMESTAMP: exp.UtcTimestamp,
    }

    CAST_COLUMN_OPERATORS = {TokenType.COLON_GT, TokenType.NCOLON_GT}

    COLUMN_OPERATORS = {
        **MySQLParser.COLUMN_OPERATORS,
        TokenType.COLON_GT: lambda self, this, to: self.expression(
            exp.Cast,
            this=this,
            to=to,
        ),
        TokenType.NCOLON_GT: lambda self, this, to: self.expression(
            exp.TryCast,
            this=this,
            to=to,
        ),
        TokenType.DCOLON: lambda self, this, path: build_json_extract_path(exp.JSONExtract)(
            [this, exp.Literal.string(path.name)]
        ),
        TokenType.DCOLONDOLLAR: lambda self, this, path: build_json_extract_path(
            exp.JSONExtractScalar, json_type="STRING"
        )([this, exp.Literal.string(path.name)]),
        TokenType.DCOLONPERCENT: lambda self, this, path: build_json_extract_path(
            exp.JSONExtractScalar, json_type="DOUBLE"
        )([this, exp.Literal.string(path.name)]),
        TokenType.DCOLONQMARK: lambda self, this, path: self.expression(
            exp.JSONExists,
            this=this,
            path=path.name,
            from_dcolonqmark=True,
        ),
    }
    COLUMN_OPERATORS = {
        k: v
        for k, v in COLUMN_OPERATORS.items()
        if k
        not in (
            TokenType.ARROW,
            TokenType.DARROW,
            TokenType.HASH_ARROW,
            TokenType.DHASH_ARROW,
            TokenType.PLACEHOLDER,
        )
    }

    SHOW_PARSERS = {
        **MySQLParser.SHOW_PARSERS,
        "AGGREGATES": _show_parser("AGGREGATES"),
        "CDC EXTRACTOR POOL": _show_parser("CDC EXTRACTOR POOL"),
        "CREATE AGGREGATE": _show_parser("CREATE AGGREGATE", target=True),
        "CREATE PIPELINE": _show_parser("CREATE PIPELINE", target=True),
        "CREATE PROJECTION": _show_parser("CREATE PROJECTION", target=True),
        "DATABASE STATUS": _show_parser("DATABASE STATUS"),
        "DISTRIBUTED_PLANCACHE STATUS": _show_parser("DISTRIBUTED_PLANCACHE STATUS"),
        "FULLTEXT SERVICE METRICS LOCAL": _show_parser("FULLTEXT SERVICE METRICS LOCAL"),
        "FULLTEXT SERVICE METRICS FOR NODE": _show_parser(
            "FULLTEXT SERVICE METRICS FOR NODE", target=True
        ),
        "FULLTEXT SERVICE STATUS": _show_parser("FULLTEXT SERVICE STATUS"),
        "FUNCTIONS": _show_parser("FUNCTIONS"),
        "GROUPS": _show_parser("GROUPS"),
        "GROUPS FOR ROLE": _show_parser("GROUPS FOR ROLE", target=True),
        "GROUPS FOR USER": _show_parser("GROUPS FOR USER", target=True),
        "INDEXES": _show_parser("INDEX", target="FROM"),
        "KEYS": _show_parser("INDEX", target="FROM"),
        "LINKS": _show_parser("LINKS", target="ON"),
        "LOAD ERRORS": _show_parser("LOAD ERRORS"),
        "LOAD WARNINGS": _show_parser("LOAD WARNINGS"),
        "PARTITIONS": _show_parser("PARTITIONS", target="ON"),
        "PIPELINES": _show_parser("PIPELINES"),
        "PLAN": _show_parser("PLAN", target=True),
        "PLANCACHE": _show_parser("PLANCACHE"),
        "PROCEDURES": _show_parser("PROCEDURES"),
        "PROJECTIONS": _show_parser("PROJECTIONS", target="ON TABLE"),
        "REPLICATION STATUS": _show_parser("REPLICATION STATUS"),
        "REPRODUCTION": _show_parser("REPRODUCTION"),
        "RESOURCE POOLS": _show_parser("RESOURCE POOLS"),
        "ROLES": _show_parser("ROLES"),
        "ROLES FOR USER": _show_parser("ROLES FOR USER", target=True),
        "ROLES FOR GROUP": _show_parser("ROLES FOR GROUP", target=True),
        "STATUS EXTENDED": _show_parser("STATUS EXTENDED"),
        "USERS": _show_parser("USERS"),
        "USERS FOR ROLE": _show_parser("USERS FOR ROLE", target=True),
        "USERS FOR GROUP": _show_parser("USERS FOR GROUP", target=True),
    }

    ALTER_PARSERS = {
        **MySQLParser.ALTER_PARSERS,
        "CHANGE": lambda self: self.expression(
            exp.RenameColumn, this=self._parse_column(), to=self._parse_column()
        ),
    }

    def _parse_vector_expressions(self, expressions: t.List[exp.Expr]) -> t.List[exp.Expr]:
        type_name = expressions[1].name.upper()
        if type_name in self.dialect.VECTOR_TYPE_ALIASES:
            type_name = self.dialect.VECTOR_TYPE_ALIASES[type_name]

        return [exp.DataType.build(type_name, dialect=self.dialect), expressions[0]]
