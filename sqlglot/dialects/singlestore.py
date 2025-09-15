import re

from sqlglot import TokenType
import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import (
    build_formatted_time,
    build_json_extract_path,
    json_extract_segments,
    json_path_key_only_name,
    rename_func,
    bool_xor_sql,
    count_if_to_sum,
    timestamptrunc_sql,
    date_add_interval_sql,
    timestampdiff_sql,
)
from sqlglot.dialects.mysql import MySQL, _remove_ts_or_ds_to_date, date_add_sql, _show_parser
from sqlglot.expressions import DataType
from sqlglot.generator import unsupported_args
from sqlglot.helper import seq_get


def cast_to_time6(
    expression: t.Optional[exp.Expression], time_type: DataType.Type = exp.DataType.Type.TIME
) -> exp.Cast:
    return exp.Cast(
        this=expression,
        to=exp.DataType.build(
            time_type,
            expressions=[exp.DataTypeParam(this=exp.Literal.number(6))],
        ),
    )


class SingleStore(MySQL):
    SUPPORTS_ORDER_BY_ALL = True

    TIME_MAPPING: t.Dict[str, str] = {
        "D": "%u",  # Day of week (1-7)
        "DD": "%d",  # day of month (01-31)
        "DY": "%a",  # abbreviated name of day
        "HH": "%I",  # Hour of day (01-12)
        "HH12": "%I",  # alias for HH
        "HH24": "%H",  # Hour of day (00-23)
        "MI": "%M",  # Minute (00-59)
        "MM": "%m",  # Month (01-12; January = 01)
        "MON": "%b",  # Abbreviated name of month
        "MONTH": "%B",  # Name of month
        "SS": "%S",  # Second (00-59)
        "RR": "%y",  # 15
        "YY": "%y",  # 15
        "YYYY": "%Y",  # 2015
        "FF6": "%f",  # only 6 digits are supported in python formats
    }

    VECTOR_TYPE_ALIASES = {
        "I8": "TINYINT",
        "I16": "SMALLINT",
        "I32": "INT",
        "I64": "BIGINT",
        "F32": "FLOAT",
        "F64": "DOUBLE",
    }

    INVERSE_VECTOR_TYPE_ALIASES = {v: k for k, v in VECTOR_TYPE_ALIASES.items()}

    class Tokenizer(MySQL.Tokenizer):
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]

        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            "BSON": TokenType.JSONB,
            "GEOGRAPHYPOINT": TokenType.GEOGRAPHYPOINT,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "UTC_DATE": TokenType.UTC_DATE,
            "UTC_TIME": TokenType.UTC_TIME,
            "UTC_TIMESTAMP": TokenType.UTC_TIMESTAMP,
            ":>": TokenType.COLON_GT,
            "!:>": TokenType.NCOLON_GT,
            "::$": TokenType.DCOLONDOLLAR,
            "::%": TokenType.DCOLONPERCENT,
        }

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
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
                DataType.Type.INT,
            ),
            "MICROSECOND": lambda args: exp.cast(
                exp.TimeToStr(
                    this=cast_to_time6(seq_get(args, 0)),
                    format=MySQL.format_time(exp.Literal.string("%f")),
                ),
                DataType.Type.INT,
            ),
            "SECOND": lambda args: exp.cast(
                exp.TimeToStr(
                    this=cast_to_time6(seq_get(args, 0)),
                    format=MySQL.format_time(exp.Literal.string("%s")),
                ),
                DataType.Type.INT,
            ),
            "MINUTE": lambda args: exp.cast(
                exp.TimeToStr(
                    this=cast_to_time6(seq_get(args, 0)),
                    format=MySQL.format_time(exp.Literal.string("%i")),
                ),
                DataType.Type.INT,
            ),
            "MONTHNAME": lambda args: exp.TimeToStr(
                this=seq_get(args, 0),
                format=MySQL.format_time(exp.Literal.string("%M")),
            ),
            "WEEKDAY": lambda args: exp.paren(exp.DayOfWeek(this=seq_get(args, 0)) + 5, copy=False)
            % 7,
            "UNIX_TIMESTAMP": exp.StrToUnix.from_arg_list,
            "FROM_UNIXTIME": build_formatted_time(exp.UnixToTime, "mysql"),
            "TIME_BUCKET": lambda args: exp.DateBin(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                origin=seq_get(args, 2),
            ),
            "BSON_EXTRACT_BSON": build_json_extract_path(exp.JSONBExtract),
            "BSON_EXTRACT_STRING": build_json_extract_path(
                exp.JSONBExtractScalar, json_type="STRING"
            ),
            "BSON_EXTRACT_DOUBLE": build_json_extract_path(
                exp.JSONBExtractScalar, json_type="DOUBLE"
            ),
            "BSON_EXTRACT_BIGINT": build_json_extract_path(
                exp.JSONBExtractScalar, json_type="BIGINT"
            ),
            "JSON_EXTRACT_JSON": build_json_extract_path(exp.JSONExtract),
            "JSON_EXTRACT_STRING": build_json_extract_path(
                exp.JSONExtractScalar, json_type="STRING"
            ),
            "JSON_EXTRACT_DOUBLE": build_json_extract_path(
                exp.JSONExtractScalar, json_type="DOUBLE"
            ),
            "JSON_EXTRACT_BIGINT": build_json_extract_path(
                exp.JSONExtractScalar, json_type="BIGINT"
            ),
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

        FUNCTION_PARSERS: t.Dict[str, t.Callable] = {
            **MySQL.Parser.FUNCTION_PARSERS,
            "JSON_AGG": lambda self: exp.JSONArrayAgg(
                this=self._parse_term(),
                order=self._parse_order(),
            ),
        }

        NO_PAREN_FUNCTIONS = {
            **MySQL.Parser.NO_PAREN_FUNCTIONS,
            TokenType.UTC_DATE: exp.UtcDate,
            TokenType.UTC_TIME: exp.UtcTime,
            TokenType.UTC_TIMESTAMP: exp.UtcTimestamp,
        }

        CAST_COLUMN_OPERATORS = {TokenType.COLON_GT, TokenType.NCOLON_GT}

        COLUMN_OPERATORS = {
            **MySQL.Parser.COLUMN_OPERATORS,
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
        }
        COLUMN_OPERATORS.pop(TokenType.ARROW)
        COLUMN_OPERATORS.pop(TokenType.DARROW)
        COLUMN_OPERATORS.pop(TokenType.HASH_ARROW)
        COLUMN_OPERATORS.pop(TokenType.DHASH_ARROW)
        COLUMN_OPERATORS.pop(TokenType.PLACEHOLDER)

        SHOW_PARSERS = {
            **MySQL.Parser.SHOW_PARSERS,
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
            **MySQL.Parser.ALTER_PARSERS,
            "CHANGE": lambda self: self.expression(
                exp.RenameColumn, this=self._parse_column(), to=self._parse_column()
            ),
        }

        def _parse_vector_expressions(
            self, expressions: t.List[exp.Expression]
        ) -> t.List[exp.Expression]:
            type_name = expressions[1].name.upper()
            if type_name in self.dialect.VECTOR_TYPE_ALIASES:
                type_name = self.dialect.VECTOR_TYPE_ALIASES[type_name]

            return [exp.DataType.build(type_name, dialect=self.dialect), expressions[0]]

    class Generator(MySQL.Generator):
        SUPPORTS_UESCAPE = False
        NULL_ORDERING_SUPPORTED = True
        MATCH_AGAINST_TABLE_PREFIX = "TABLE "

        @staticmethod
        def _unicode_substitute(m: re.Match[str]) -> str:
            # Interpret the number as hex and convert it to the Unicode string
            return chr(int(m.group(1), 16))

        UNICODE_SUBSTITUTE: t.Optional[t.Callable[[re.Match[str]], str]] = _unicode_substitute

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.TsOrDsToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e))
            if e.args.get("format")
            else self.func("DATE", e.this),
            exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            exp.ToChar: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.StrToDate: lambda self, e: self.func(
                "STR_TO_DATE",
                e.this,
                self.format_time(
                    e,
                    inverse_time_mapping=MySQL.INVERSE_TIME_MAPPING,
                    inverse_time_trie=MySQL.INVERSE_TIME_TRIE,
                ),
            ),
            exp.TimeToStr: lambda self, e: self.func(
                "DATE_FORMAT",
                e.this,
                self.format_time(
                    e,
                    inverse_time_mapping=MySQL.INVERSE_TIME_MAPPING,
                    inverse_time_trie=MySQL.INVERSE_TIME_TRIE,
                ),
            ),
            exp.Date: unsupported_args("zone", "expressions")(rename_func("DATE")),
            exp.Cast: unsupported_args("format", "action", "default")(
                lambda self, e: f"{self.sql(e, 'this')} :> {self.sql(e, 'to')}"
            ),
            exp.TryCast: unsupported_args("format", "action", "default")(
                lambda self, e: f"{self.sql(e, 'this')} !:> {self.sql(e, 'to')}"
            ),
            exp.CastToStrType: lambda self, e: self.sql(
                exp.cast(e.this, DataType.build(e.args["to"].name))
            ),
            exp.StrToUnix: unsupported_args("format")(rename_func("UNIX_TIMESTAMP")),
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.UnixSeconds: rename_func("UNIX_TIMESTAMP"),
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME",
                e.this,
                self.format_time(
                    e,
                    inverse_time_mapping=MySQL.INVERSE_TIME_MAPPING,
                    inverse_time_trie=MySQL.INVERSE_TIME_TRIE,
                ),
            ),
            exp.UnixToTime: unsupported_args("scale", "zone", "hours", "minutes")(
                lambda self, e: self.func(
                    "FROM_UNIXTIME",
                    e.this,
                    self.format_time(
                        e,
                        inverse_time_mapping=MySQL.INVERSE_TIME_MAPPING,
                        inverse_time_trie=MySQL.INVERSE_TIME_TRIE,
                    ),
                ),
            ),
            exp.UnixToTimeStr: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')}) :> TEXT",
            exp.DateBin: unsupported_args("unit", "zone")(
                lambda self, e: self.func("TIME_BUCKET", e.this, e.expression, e.args.get("origin"))
            ),
            exp.TimeStrToDate: lambda self, e: self.sql(exp.cast(e.this, exp.DataType.Type.DATE)),
            exp.FromTimeZone: lambda self, e: self.func(
                "CONVERT_TZ", e.this, e.args.get("zone"), "'UTC'"
            ),
            exp.DiToDate: lambda self,
            e: f"STR_TO_DATE({self.sql(e, 'this')}, {SingleStore.DATEINT_FORMAT})",
            exp.DateToDi: lambda self,
            e: f"(DATE_FORMAT({self.sql(e, 'this')}, {SingleStore.DATEINT_FORMAT}) :> INT)",
            exp.TsOrDiToDi: lambda self,
            e: f"(DATE_FORMAT({self.sql(e, 'this')}, {SingleStore.DATEINT_FORMAT}) :> INT)",
            exp.Time: unsupported_args("zone")(lambda self, e: f"{self.sql(e, 'this')} :> TIME"),
            exp.DatetimeAdd: _remove_ts_or_ds_to_date(date_add_sql("ADD")),
            exp.DatetimeTrunc: unsupported_args("zone")(timestamptrunc_sql()),
            exp.DatetimeSub: date_add_interval_sql("DATE", "SUB"),
            exp.DatetimeDiff: timestampdiff_sql,
            exp.DateTrunc: unsupported_args("zone")(timestamptrunc_sql()),
            exp.DateDiff: unsupported_args("zone")(
                lambda self, e: timestampdiff_sql(self, e)
                if e.unit is not None
                else self.func("DATEDIFF", e.this, e.expression)
            ),
            exp.TsOrDsDiff: lambda self, e: timestampdiff_sql(self, e)
            if e.unit is not None
            else self.func("DATEDIFF", e.this, e.expression),
            exp.TimestampTrunc: unsupported_args("zone")(timestamptrunc_sql()),
            exp.CurrentDatetime: lambda self, e: self.sql(
                cast_to_time6(
                    exp.CurrentTimestamp(this=exp.Literal.number(6)), exp.DataType.Type.DATETIME
                )
            ),
            exp.JSONExtract: unsupported_args(
                "only_json_types",
                "expressions",
                "variant_extract",
                "json_query",
                "option",
                "quote",
                "on_condition",
                "requires_json",
            )(json_extract_segments("JSON_EXTRACT_JSON")),
            exp.JSONBExtract: json_extract_segments("BSON_EXTRACT_BSON"),
            exp.JSONPathKey: json_path_key_only_name,
            exp.JSONPathSubscript: lambda self, e: self.json_path_part(e.this),
            exp.JSONPathRoot: lambda *_: "",
            exp.JSONFormat: unsupported_args("options", "is_json")(rename_func("JSON_PRETTY")),
            exp.JSONArrayAgg: unsupported_args("null_handling", "return_type", "strict")(
                lambda self, e: self.func("JSON_AGG", e.this, suffix=f"{self.sql(e, 'order')})")
            ),
            exp.JSONArray: unsupported_args("null_handling", "return_type", "strict")(
                rename_func("JSON_BUILD_ARRAY")
            ),
            exp.JSONBExists: lambda self, e: self.func(
                "BSON_MATCH_ANY_EXISTS", e.this, e.args.get("path")
            ),
            exp.JSONExists: unsupported_args("passing", "on_condition")(
                lambda self, e: self.func("JSON_MATCH_ANY_EXISTS", e.this, e.args.get("path"))
            ),
            exp.JSONObject: unsupported_args(
                "null_handling", "unique_keys", "return_type", "encoding"
            )(rename_func("JSON_BUILD_OBJECT")),
            exp.DayOfWeekIso: lambda self, e: f"(({self.func('DAYOFWEEK', e.this)} % 7) + 1)",
            exp.DayOfMonth: rename_func("DAY"),
            exp.Hll: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.CountIf: count_if_to_sum,
            exp.LogicalOr: lambda self, e: f"MAX(ABS({self.sql(e, 'this')}))",
            exp.LogicalAnd: lambda self, e: f"MIN(ABS({self.sql(e, 'this')}))",
            exp.ApproxQuantile: unsupported_args("accuracy", "weight")(
                lambda self, e: self.func(
                    "APPROX_PERCENTILE",
                    e.this,
                    e.args.get("quantile"),
                    e.args.get("error_tolerance"),
                )
            ),
            exp.Variance: rename_func("VAR_SAMP"),
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Xor: bool_xor_sql,
            exp.Cbrt: lambda self, e: self.sql(
                exp.Pow(this=e.this, expression=exp.Literal.number(1) / exp.Literal.number(3))
            ),
            exp.RegexpLike: lambda self, e: self.binary(e, "RLIKE"),
            exp.Repeat: lambda self, e: self.func(
                "LPAD",
                exp.Literal.string(""),
                exp.Mul(this=self.func("LENGTH", e.this), expression=e.args.get("times")),
                e.this,
            ),
            exp.IsAscii: lambda self, e: f"({self.sql(e, 'this')} RLIKE '^[\x00-\x7f]*$')",
            exp.MD5Digest: lambda self, e: self.func("UNHEX", self.func("MD5", e.this)),
            exp.Chr: rename_func("CHAR"),
            exp.Contains: rename_func("INSTR"),
            exp.RegexpExtractAll: unsupported_args("position", "occurrence", "group")(
                lambda self, e: self.func(
                    "REGEXP_MATCH",
                    e.this,
                    e.expression,
                    e.args.get("parameters"),
                )
            ),
            exp.RegexpExtract: unsupported_args("group")(
                lambda self, e: self.func(
                    "REGEXP_SUBSTR",
                    e.this,
                    e.expression,
                    e.args.get("position"),
                    e.args.get("occurrence"),
                    e.args.get("parameters"),
                )
            ),
            exp.StartsWith: lambda self, e: self.func(
                "REGEXP_INSTR", e.this, self.func("CONCAT", exp.Literal.string("^"), e.expression)
            ),
            exp.FromBase: lambda self, e: self.func(
                "CONV", e.this, e.expression, exp.Literal.number(10)
            ),
            exp.RegexpILike: lambda self, e: self.binary(
                exp.RegexpLike(
                    this=exp.Lower(this=e.this),
                    expression=exp.Lower(this=e.expression),
                ),
                "RLIKE",
            ),
            exp.Stuff: lambda self, e: self.func(
                "CONCAT",
                self.func("SUBSTRING", e.this, exp.Literal.number(1), e.args.get("start") - 1),
                e.expression,
                self.func("SUBSTRING", e.this, e.args.get("start") + e.args.get("length")),
            ),
            exp.National: lambda self, e: self.national_sql(e, prefix=""),
            exp.Reduce: unsupported_args("finish")(
                lambda self, e: self.func(
                    "REDUCE", e.args.get("initial"), e.this, e.args.get("merge")
                )
            ),
            exp.MatchAgainst: unsupported_args("modifier")(
                lambda self, e: super().matchagainst_sql(e)
            ),
            exp.Show: unsupported_args(
                "history",
                "terse",
                "offset",
                "starts_with",
                "limit",
                "from",
                "scope",
                "scope_kind",
                "mutex",
                "query",
                "channel",
                "log",
                "types",
                "privileges",
            )(lambda self, e: super().show_sql(e)),
            exp.Describe: unsupported_args(
                "style",
                "kind",
                "expressions",
                "partition",
                "format",
            )(lambda self, e: super().describe_sql(e)),
        }
        TRANSFORMS.pop(exp.JSONExtractScalar)
        TRANSFORMS.pop(exp.CurrentDate)

        UNSUPPORTED_TYPES = {
            exp.DataType.Type.ARRAY,
            exp.DataType.Type.AGGREGATEFUNCTION,
            exp.DataType.Type.SIMPLEAGGREGATEFUNCTION,
            exp.DataType.Type.BIGSERIAL,
            exp.DataType.Type.BPCHAR,
            exp.DataType.Type.DATEMULTIRANGE,
            exp.DataType.Type.DATERANGE,
            exp.DataType.Type.DYNAMIC,
            exp.DataType.Type.HLLSKETCH,
            exp.DataType.Type.HSTORE,
            exp.DataType.Type.IMAGE,
            exp.DataType.Type.INET,
            exp.DataType.Type.INT128,
            exp.DataType.Type.INT256,
            exp.DataType.Type.INT4MULTIRANGE,
            exp.DataType.Type.INT4RANGE,
            exp.DataType.Type.INT8MULTIRANGE,
            exp.DataType.Type.INT8RANGE,
            exp.DataType.Type.INTERVAL,
            exp.DataType.Type.IPADDRESS,
            exp.DataType.Type.IPPREFIX,
            exp.DataType.Type.IPV4,
            exp.DataType.Type.IPV6,
            exp.DataType.Type.LIST,
            exp.DataType.Type.MAP,
            exp.DataType.Type.LOWCARDINALITY,
            exp.DataType.Type.MONEY,
            exp.DataType.Type.MULTILINESTRING,
            exp.DataType.Type.NAME,
            exp.DataType.Type.NESTED,
            exp.DataType.Type.NOTHING,
            exp.DataType.Type.NULL,
            exp.DataType.Type.NUMMULTIRANGE,
            exp.DataType.Type.NUMRANGE,
            exp.DataType.Type.OBJECT,
            exp.DataType.Type.RANGE,
            exp.DataType.Type.ROWVERSION,
            exp.DataType.Type.SERIAL,
            exp.DataType.Type.SMALLSERIAL,
            exp.DataType.Type.SMALLMONEY,
            exp.DataType.Type.STRUCT,
            exp.DataType.Type.SUPER,
            exp.DataType.Type.TIMETZ,
            exp.DataType.Type.TIMESTAMPNTZ,
            exp.DataType.Type.TIMESTAMPLTZ,
            exp.DataType.Type.TIMESTAMPTZ,
            exp.DataType.Type.TIMESTAMP_NS,
            exp.DataType.Type.TSMULTIRANGE,
            exp.DataType.Type.TSRANGE,
            exp.DataType.Type.TSTZMULTIRANGE,
            exp.DataType.Type.TSTZRANGE,
            exp.DataType.Type.UINT128,
            exp.DataType.Type.UINT256,
            exp.DataType.Type.UNION,
            exp.DataType.Type.UNKNOWN,
            exp.DataType.Type.USERDEFINED,
            exp.DataType.Type.UUID,
            exp.DataType.Type.VARIANT,
            exp.DataType.Type.XML,
            exp.DataType.Type.TDIGEST,
        }

        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.BIGDECIMAL: "DECIMAL",
            exp.DataType.Type.BIT: "BOOLEAN",
            exp.DataType.Type.DATE32: "DATE",
            exp.DataType.Type.DATETIME64: "DATETIME",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.ENUM8: "ENUM",
            exp.DataType.Type.ENUM16: "ENUM",
            exp.DataType.Type.FIXEDSTRING: "TEXT",
            exp.DataType.Type.GEOMETRY: "GEOGRAPHY",
            exp.DataType.Type.POINT: "GEOGRAPHYPOINT",
            exp.DataType.Type.RING: "GEOGRAPHY",
            exp.DataType.Type.LINESTRING: "GEOGRAPHY",
            exp.DataType.Type.POLYGON: "GEOGRAPHY",
            exp.DataType.Type.MULTIPOLYGON: "GEOGRAPHY",
            exp.DataType.Type.JSONB: "BSON",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP_S: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP_MS: "TIMESTAMP(6)",
        }

        # https://docs.singlestore.com/cloud/reference/sql-reference/restricted-keywords/list-of-restricted-keywords/
        RESERVED_KEYWORDS = {
            "abs",
            "absolute",
            "access",
            "account",
            "acos",
            "action",
            "add",
            "adddate",
            "addtime",
            "admin",
            "aes_decrypt",
            "aes_encrypt",
            "after",
            "against",
            "aggregate",
            "aggregates",
            "aggregator",
            "aggregator_id",
            "aggregator_plan_hash",
            "aggregators",
            "algorithm",
            "all",
            "also",
            "alter",
            "always",
            "analyse",
            "analyze",
            "and",
            "anti_join",
            "any",
            "any_value",
            "approx_count_distinct",
            "approx_count_distinct_accumulate",
            "approx_count_distinct_combine",
            "approx_count_distinct_estimate",
            "approx_geography_intersects",
            "approx_percentile",
            "arghistory",
            "arrange",
            "arrangement",
            "array",
            "as",
            "asc",
            "ascii",
            "asensitive",
            "asin",
            "asm",
            "assertion",
            "assignment",
            "ast",
            "asymmetric",
            "async",
            "at",
            "atan",
            "atan2",
            "attach",
            "attribute",
            "authorization",
            "auto",
            "auto_increment",
            "auto_reprovision",
            "autostats",
            "autostats_cardinality_mode",
            "autostats_enabled",
            "autostats_histogram_mode",
            "autostats_sampling",
            "availability",
            "avg",
            "avg_row_length",
            "avro",
            "azure",
            "background",
            "_background_threads_for_cleanup",
            "backup",
            "backup_history",
            "backup_id",
            "backward",
            "batch",
            "batches",
            "batch_interval",
            "_batch_size_limit",
            "before",
            "begin",
            "between",
            "bigint",
            "bin",
            "binary",
            "_binary",
            "bit",
            "bit_and",
            "bit_count",
            "bit_or",
            "bit_xor",
            "blob",
            "bool",
            "boolean",
            "bootstrap",
            "both",
            "_bt",
            "btree",
            "bucket_count",
            "by",
            "byte",
            "byte_length",
            "cache",
            "call",
            "call_for_pipeline",
            "called",
            "capture",
            "cascade",
            "cascaded",
            "case",
            "cast",
            "catalog",
            "ceil",
            "ceiling",
            "chain",
            "change",
            "char",
            "character",
            "characteristics",
            "character_length",
            "char_length",
            "charset",
            "check",
            "checkpoint",
            "_check_can_connect",
            "_check_consistency",
            "checksum",
            "_checksum",
            "class",
            "clear",
            "client",
            "client_found_rows",
            "close",
            "cluster",
            "clustered",
            "cnf",
            "coalesce",
            "coercibility",
            "collate",
            "collation",
            "collect",
            "column",
            "columnar",
            "columns",
            "columnstore",
            "columnstore_segment_rows",
            "comment",
            "comments",
            "commit",
            "committed",
            "_commit_log_tail",
            "committed",
            "compact",
            "compile",
            "compressed",
            "compression",
            "concat",
            "concat_ws",
            "concurrent",
            "concurrently",
            "condition",
            "configuration",
            "connection",
            "connection_id",
            "connections",
            "config",
            "constraint",
            "constraints",
            "content",
            "continue",
            "_continue_replay",
            "conv",
            "conversion",
            "convert",
            "convert_tz",
            "copy",
            "_core",
            "cos",
            "cost",
            "cot",
            "count",
            "create",
            "credentials",
            "cross",
            "cube",
            "csv",
            "cume_dist",
            "curdate",
            "current",
            "current_catalog",
            "current_date",
            "current_role",
            "current_schema",
            "current_security_groups",
            "current_security_roles",
            "current_time",
            "current_timestamp",
            "current_user",
            "cursor",
            "curtime",
            "cycle",
            "data",
            "database",
            "databases",
            "date",
            "date_add",
            "datediff",
            "date_format",
            "date_sub",
            "date_trunc",
            "datetime",
            "day",
            "day_hour",
            "day_microsecond",
            "day_minute",
            "dayname",
            "dayofmonth",
            "dayofweek",
            "dayofyear",
            "day_second",
            "deallocate",
            "dec",
            "decimal",
            "declare",
            "decode",
            "default",
            "defaults",
            "deferrable",
            "deferred",
            "defined",
            "definer",
            "degrees",
            "delayed",
            "delay_key_write",
            "delete",
            "delimiter",
            "delimiters",
            "dense_rank",
            "desc",
            "describe",
            "detach",
            "deterministic",
            "dictionary",
            "differential",
            "directory",
            "disable",
            "discard",
            "_disconnect",
            "disk",
            "distinct",
            "distinctrow",
            "distributed_joins",
            "div",
            "do",
            "document",
            "domain",
            "dot_product",
            "double",
            "drop",
            "_drop_profile",
            "dual",
            "dump",
            "duplicate",
            "dynamic",
            "earliest",
            "each",
            "echo",
            "election",
            "else",
            "elseif",
            "elt",
            "enable",
            "enclosed",
            "encoding",
            "encrypted",
            "end",
            "engine",
            "engines",
            "enum",
            "errors",
            "escape",
            "escaped",
            "estimate",
            "euclidean_distance",
            "event",
            "events",
            "except",
            "exclude",
            "excluding",
            "exclusive",
            "execute",
            "exists",
            "exit",
            "exp",
            "explain",
            "extended",
            "extension",
            "external",
            "external_host",
            "external_port",
            "extract",
            "extractor",
            "extractors",
            "extra_join",
            "_failover",
            "failed_login_attempts",
            "failure",
            "false",
            "family",
            "fault",
            "fetch",
            "field",
            "fields",
            "file",
            "files",
            "fill",
            "first",
            "first_value",
            "fix_alter",
            "fixed",
            "float",
            "float4",
            "float8",
            "floor",
            "flush",
            "following",
            "for",
            "force",
            "force_compiled_mode",
            "force_interpreter_mode",
            "foreground",
            "foreign",
            "format",
            "forward",
            "found_rows",
            "freeze",
            "from",
            "from_base64",
            "from_days",
            "from_unixtime",
            "fs",
            "_fsync",
            "full",
            "fulltext",
            "function",
            "functions",
            "gc",
            "gcs",
            "get_format",
            "_gc",
            "_gcx",
            "generate",
            "geography",
            "geography_area",
            "geography_contains",
            "geography_distance",
            "geography_intersects",
            "geography_latitude",
            "geography_length",
            "geography_longitude",
            "geographypoint",
            "geography_point",
            "geography_within_distance",
            "geometry",
            "geometry_area",
            "geometry_contains",
            "geometry_distance",
            "geometry_filter",
            "geometry_intersects",
            "geometry_length",
            "geometrypoint",
            "geometry_point",
            "geometry_within_distance",
            "geometry_x",
            "geometry_y",
            "global",
            "_global_version_timestamp",
            "grant",
            "granted",
            "grants",
            "greatest",
            "group",
            "grouping",
            "groups",
            "group_concat",
            "gzip",
            "handle",
            "handler",
            "hard_cpu_limit_percentage",
            "hash",
            "has_temp_tables",
            "having",
            "hdfs",
            "header",
            "heartbeat_no_logging",
            "hex",
            "highlight",
            "high_priority",
            "hold",
            "holding",
            "host",
            "hosts",
            "hour",
            "hour_microsecond",
            "hour_minute",
            "hour_second",
            "identified",
            "identity",
            "if",
            "ifnull",
            "ignore",
            "ilike",
            "immediate",
            "immutable",
            "implicit",
            "import",
            "in",
            "including",
            "increment",
            "incremental",
            "index",
            "indexes",
            "inet_aton",
            "inet_ntoa",
            "inet6_aton",
            "inet6_ntoa",
            "infile",
            "inherit",
            "inherits",
            "_init_profile",
            "init",
            "initcap",
            "initialize",
            "initially",
            "inject",
            "inline",
            "inner",
            "inout",
            "input",
            "insensitive",
            "insert",
            "insert_method",
            "instance",
            "instead",
            "instr",
            "int",
            "int1",
            "int2",
            "int3",
            "int4",
            "int8",
            "integer",
            "_internal_dynamic_typecast",
            "interpreter_mode",
            "intersect",
            "interval",
            "into",
            "invoker",
            "is",
            "isnull",
            "isolation",
            "iterate",
            "join",
            "json",
            "json_agg",
            "json_array_contains_double",
            "json_array_contains_json",
            "json_array_contains_string",
            "json_array_push_double",
            "json_array_push_json",
            "json_array_push_string",
            "json_delete_key",
            "json_extract_double",
            "json_extract_json",
            "json_extract_string",
            "json_extract_bigint",
            "json_get_type",
            "json_length",
            "json_set_double",
            "json_set_json",
            "json_set_string",
            "json_splice_double",
            "json_splice_json",
            "json_splice_string",
            "kafka",
            "key",
            "key_block_size",
            "keys",
            "kill",
            "killall",
            "label",
            "lag",
            "language",
            "large",
            "last",
            "last_day",
            "last_insert_id",
            "last_value",
            "lateral",
            "latest",
            "lc_collate",
            "lc_ctype",
            "lcase",
            "lead",
            "leading",
            "leaf",
            "leakproof",
            "least",
            "leave",
            "leaves",
            "left",
            "length",
            "level",
            "license",
            "like",
            "limit",
            "lines",
            "listen",
            "llvm",
            "ln",
            "load",
            "loaddata_where",
            "_load",
            "local",
            "localtime",
            "localtimestamp",
            "locate",
            "location",
            "lock",
            "log",
            "log10",
            "log2",
            "long",
            "longblob",
            "longtext",
            "loop",
            "lower",
            "low_priority",
            "lpad",
            "_ls",
            "ltrim",
            "lz4",
            "management",
            "_management_thread",
            "mapping",
            "master",
            "match",
            "materialized",
            "max",
            "maxvalue",
            "max_concurrency",
            "max_errors",
            "max_partitions_per_batch",
            "max_queue_depth",
            "max_retries_per_batch_partition",
            "max_rows",
            "mbc",
            "md5",
            "mpl",
            "median",
            "mediumblob",
            "mediumint",
            "mediumtext",
            "member",
            "memory",
            "memory_percentage",
            "_memsql_table_id_lookup",
            "memsql",
            "memsql_deserialize",
            "memsql_imitating_kafka",
            "memsql_serialize",
            "merge",
            "metadata",
            "microsecond",
            "middleint",
            "min",
            "min_rows",
            "minus",
            "minute",
            "minute_microsecond",
            "minute_second",
            "minvalue",
            "mod",
            "mode",
            "model",
            "modifies",
            "modify",
            "month",
            "monthname",
            "months_between",
            "move",
            "mpl",
            "names",
            "named",
            "namespace",
            "national",
            "natural",
            "nchar",
            "next",
            "no",
            "node",
            "none",
            "no_query_rewrite",
            "noparam",
            "not",
            "nothing",
            "notify",
            "now",
            "nowait",
            "no_write_to_binlog",
            "no_query_rewrite",
            "norely",
            "nth_value",
            "ntile",
            "null",
            "nullcols",
            "nullif",
            "nulls",
            "numeric",
            "nvarchar",
            "object",
            "octet_length",
            "of",
            "off",
            "offline",
            "offset",
            "offsets",
            "oids",
            "on",
            "online",
            "only",
            "open",
            "operator",
            "optimization",
            "optimize",
            "optimizer",
            "optimizer_state",
            "option",
            "options",
            "optionally",
            "or",
            "order",
            "ordered_serialize",
            "orphan",
            "out",
            "out_of_order",
            "outer",
            "outfile",
            "over",
            "overlaps",
            "overlay",
            "owned",
            "owner",
            "pack_keys",
            "paired",
            "parser",
            "parquet",
            "partial",
            "partition",
            "partition_id",
            "partitioning",
            "partitions",
            "passing",
            "password",
            "password_lock_time",
            "parser",
            "pause",
            "_pause_replay",
            "percent_rank",
            "percentile_cont",
            "percentile_disc",
            "periodic",
            "persisted",
            "pi",
            "pipeline",
            "pipelines",
            "pivot",
            "placing",
            "plan",
            "plans",
            "plancache",
            "plugins",
            "pool",
            "pools",
            "port",
            "position",
            "pow",
            "power",
            "preceding",
            "precision",
            "prepare",
            "prepared",
            "preserve",
            "primary",
            "prior",
            "privileges",
            "procedural",
            "procedure",
            "procedures",
            "process",
            "processlist",
            "profile",
            "profiles",
            "program",
            "promote",
            "proxy",
            "purge",
            "quarter",
            "queries",
            "query",
            "query_timeout",
            "queue",
            "quote",
            "radians",
            "rand",
            "range",
            "rank",
            "read",
            "_read",
            "reads",
            "real",
            "reassign",
            "rebalance",
            "recheck",
            "record",
            "recursive",
            "redundancy",
            "redundant",
            "ref",
            "reference",
            "references",
            "refresh",
            "regexp",
            "reindex",
            "relative",
            "release",
            "reload",
            "rely",
            "remote",
            "remove",
            "rename",
            "repair",
            "_repair_table",
            "repeat",
            "repeatable",
            "_repl",
            "_reprovisioning",
            "replace",
            "replica",
            "replicate",
            "replicating",
            "replication",
            "durability",
            "require",
            "resource",
            "resource_pool",
            "reset",
            "restart",
            "restore",
            "restrict",
            "result",
            "_resurrect",
            "retry",
            "return",
            "returning",
            "returns",
            "reverse",
            "revoke",
            "rg_pool",
            "right",
            "right_anti_join",
            "right_semi_join",
            "right_straight_join",
            "rlike",
            "role",
            "roles",
            "rollback",
            "rollup",
            "round",
            "routine",
            "row",
            "row_count",
            "row_format",
            "row_number",
            "rows",
            "rowstore",
            "rule",
            "rpad",
            "_rpc",
            "rtrim",
            "running",
            "s3",
            "safe",
            "save",
            "savepoint",
            "scalar",
            "schema",
            "schemas",
            "schema_binding",
            "scroll",
            "search",
            "second",
            "second_microsecond",
            "sec_to_time",
            "security",
            "select",
            "semi_join",
            "_send_threads",
            "sensitive",
            "separator",
            "sequence",
            "sequences",
            "serial",
            "serializable",
            "series",
            "service_user",
            "server",
            "session",
            "session_user",
            "set",
            "setof",
            "security_lists_intersect",
            "sha",
            "sha1",
            "sha2",
            "shard",
            "sharded",
            "sharded_id",
            "share",
            "show",
            "shutdown",
            "sigmoid",
            "sign",
            "signal",
            "similar",
            "simple",
            "site",
            "signed",
            "sin",
            "skip",
            "skipped_batches",
            "sleep",
            "_sleep",
            "smallint",
            "snapshot",
            "_snapshot",
            "_snapshots",
            "soft_cpu_limit_percentage",
            "some",
            "soname",
            "sparse",
            "spatial",
            "spatial_check_index",
            "specific",
            "split",
            "sql",
            "sql_big_result",
            "sql_buffer_result",
            "sql_cache",
            "sql_calc_found_rows",
            "sqlexception",
            "sql_mode",
            "sql_no_cache",
            "sql_no_logging",
            "sql_small_result",
            "sqlstate",
            "sqlwarning",
            "sqrt",
            "ssl",
            "stable",
            "standalone",
            "start",
            "starting",
            "state",
            "statement",
            "statistics",
            "stats",
            "status",
            "std",
            "stddev",
            "stddev_pop",
            "stddev_samp",
            "stdin",
            "stdout",
            "stop",
            "storage",
            "str_to_date",
            "straight_join",
            "strict",
            "string",
            "strip",
            "subdate",
            "substr",
            "substring",
            "substring_index",
            "success",
            "sum",
            "super",
            "symmetric",
            "sync_snapshot",
            "sync",
            "_sync",
            "_sync2",
            "_sync_partitions",
            "_sync_snapshot",
            "synchronize",
            "sysid",
            "system",
            "table",
            "table_checksum",
            "tables",
            "tablespace",
            "tags",
            "tan",
            "target_size",
            "task",
            "temp",
            "template",
            "temporary",
            "temptable",
            "_term_bump",
            "terminate",
            "terminated",
            "test",
            "text",
            "then",
            "time",
            "timediff",
            "time_bucket",
            "time_format",
            "timeout",
            "timestamp",
            "timestampadd",
            "timestampdiff",
            "timezone",
            "time_to_sec",
            "tinyblob",
            "tinyint",
            "tinytext",
            "to",
            "to_base64",
            "to_char",
            "to_date",
            "to_days",
            "to_json",
            "to_number",
            "to_seconds",
            "to_timestamp",
            "tracelogs",
            "traditional",
            "trailing",
            "transform",
            "transaction",
            "_transactions_experimental",
            "treat",
            "trigger",
            "triggers",
            "trim",
            "true",
            "trunc",
            "truncate",
            "trusted",
            "two_phase",
            "_twopcid",
            "type",
            "types",
            "ucase",
            "unbounded",
            "uncommitted",
            "undefined",
            "undo",
            "unencrypted",
            "unenforced",
            "unhex",
            "unhold",
            "unicode",
            "union",
            "unique",
            "_unittest",
            "unix_timestamp",
            "unknown",
            "unlisten",
            "_unload",
            "unlock",
            "unlogged",
            "unpivot",
            "unsigned",
            "until",
            "update",
            "upgrade",
            "upper",
            "usage",
            "use",
            "user",
            "users",
            "using",
            "utc_date",
            "utc_time",
            "utc_timestamp",
            "_utf8",
            "vacuum",
            "valid",
            "validate",
            "validator",
            "value",
            "values",
            "varbinary",
            "varchar",
            "varcharacter",
            "variables",
            "variadic",
            "variance",
            "var_pop",
            "var_samp",
            "varying",
            "vector_sub",
            "verbose",
            "version",
            "view",
            "void",
            "volatile",
            "voting",
            "wait",
            "_wake",
            "warnings",
            "week",
            "weekday",
            "weekofyear",
            "when",
            "where",
            "while",
            "whitespace",
            "window",
            "with",
            "without",
            "within",
            "_wm_heartbeat",
            "work",
            "workload",
            "wrapper",
            "write",
            "xact_id",
            "xor",
            "year",
            "year_month",
            "yes",
            "zerofill",
            "zone",
        }

        def jsonextractscalar_sql(self, expression: exp.JSONExtractScalar) -> str:
            json_type = expression.args.get("json_type")
            func_name = "JSON_EXTRACT_JSON" if json_type is None else f"JSON_EXTRACT_{json_type}"
            return json_extract_segments(func_name)(self, expression)

        def jsonbextractscalar_sql(self, expression: exp.JSONBExtractScalar) -> str:
            json_type = expression.args.get("json_type")
            func_name = "BSON_EXTRACT_BSON" if json_type is None else f"BSON_EXTRACT_{json_type}"
            return json_extract_segments(func_name)(self, expression)

        def jsonextractarray_sql(self, expression: exp.JSONExtractArray) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        @unsupported_args("on_condition")
        def jsonvalue_sql(self, expression: exp.JSONValue) -> str:
            res: exp.Expression = exp.JSONExtractScalar(
                this=expression.this,
                expression=expression.args.get("path"),
                json_type="STRING",
            )

            returning = expression.args.get("returning")
            if returning is not None:
                res = exp.Cast(this=res, to=returning)

            return self.sql(res)

        def all_sql(self, expression: exp.All) -> str:
            self.unsupported("ALL subquery predicate is not supported in SingleStore")
            return super().all_sql(expression)

        def jsonarraycontains_sql(self, expression: exp.JSONArrayContains) -> str:
            json_type = expression.text("json_type").upper()

            if json_type:
                return self.func(
                    f"JSON_ARRAY_CONTAINS_{json_type}", expression.expression, expression.this
                )

            return self.func(
                "JSON_ARRAY_CONTAINS_JSON",
                expression.expression,
                self.func("TO_JSON", expression.this),
            )

        @unsupported_args("kind", "nested", "values")
        def datatype_sql(self, expression: exp.DataType) -> str:
            if expression.is_type(exp.DataType.Type.VARBINARY) and not expression.expressions:
                # `VARBINARY` must always have a size - if it doesn't, we always generate `BLOB`
                return "BLOB"
            if expression.is_type(
                exp.DataType.Type.DECIMAL32,
                exp.DataType.Type.DECIMAL64,
                exp.DataType.Type.DECIMAL128,
                exp.DataType.Type.DECIMAL256,
            ):
                scale = self.expressions(expression, flat=True)

                if expression.is_type(exp.DataType.Type.DECIMAL32):
                    precision = "9"
                elif expression.is_type(exp.DataType.Type.DECIMAL64):
                    precision = "18"
                elif expression.is_type(exp.DataType.Type.DECIMAL128):
                    precision = "38"
                else:
                    # 65 is a maximum precision supported in SingleStore
                    precision = "65"
                if scale is not None:
                    return f"DECIMAL({precision}, {scale[0]})"
                else:
                    return f"DECIMAL({precision})"
            if expression.is_type(exp.DataType.Type.VECTOR):
                expressions = expression.expressions
                if len(expressions) == 2:
                    type_name = self.sql(expressions[0])
                    if type_name in self.dialect.INVERSE_VECTOR_TYPE_ALIASES:
                        type_name = self.dialect.INVERSE_VECTOR_TYPE_ALIASES[type_name]

                    return f"VECTOR({self.sql(expressions[1])}, {type_name})"

            return super().datatype_sql(expression)

        def collate_sql(self, expression: exp.Collate) -> str:
            # SingleStore does not support setting a collation for column in the SELECT query,
            # so we cast column to a LONGTEXT type with specific collation
            return self.binary(expression, ":> LONGTEXT COLLATE")

        def currentdate_sql(self, expression: exp.CurrentDate) -> str:
            timezone = expression.this
            if timezone:
                if isinstance(timezone, exp.Literal) and timezone.name.lower() == "utc":
                    return self.func("UTC_DATE")
                self.unsupported("CurrentDate with timezone is not supported in SingleStore")

            return self.func("CURRENT_DATE")

        def currenttime_sql(self, expression: exp.CurrentTime) -> str:
            arg = expression.this
            if arg:
                if isinstance(arg, exp.Literal) and arg.name.lower() == "utc":
                    return self.func("UTC_TIME")
                if isinstance(arg, exp.Literal) and arg.is_number:
                    return self.func("CURRENT_TIME", arg)
                self.unsupported("CurrentTime with timezone is not supported in SingleStore")

            return self.func("CURRENT_TIME")

        def currenttimestamp_sql(self, expression: exp.CurrentTimestamp) -> str:
            arg = expression.this
            if arg:
                if isinstance(arg, exp.Literal) and arg.name.lower() == "utc":
                    return self.func("UTC_TIMESTAMP")
                if isinstance(arg, exp.Literal) and arg.is_number:
                    return self.func("CURRENT_TIMESTAMP", arg)
                self.unsupported("CurrentTimestamp with timezone is not supported in SingleStore")

            return self.func("CURRENT_TIMESTAMP")

        def standardhash_sql(self, expression: exp.StandardHash) -> str:
            hash_function = expression.expression
            if hash_function is None:
                return self.func("SHA", expression.this)
            if isinstance(hash_function, exp.Literal):
                if hash_function.name.lower() == "sha":
                    return self.func("SHA", expression.this)
                if hash_function.name.lower() == "md5":
                    return self.func("MD5", expression.this)

                self.unsupported(
                    f"{hash_function.this} hash method is not supported in SingleStore"
                )
                return self.func("SHA", expression.this)

            self.unsupported("STANDARD_HASH function is not supported in SingleStore")
            return self.func("SHA", expression.this)

        @unsupported_args("is_database", "exists", "cluster", "identity", "option", "partition")
        def truncatetable_sql(self, expression: exp.TruncateTable) -> str:
            statements = []
            for expression in expression.expressions:
                statements.append(f"TRUNCATE {self.sql(expression)}")

            return "; ".join(statements)

        @unsupported_args("exists")
        def renamecolumn_sql(self, expression: exp.RenameColumn) -> str:
            old_column = self.sql(expression, "this")
            new_column = self.sql(expression, "to")
            return f"CHANGE {old_column} {new_column}"

        @unsupported_args("drop", "comment", "allow_null", "visible", "using")
        def altercolumn_sql(self, expression: exp.AlterColumn) -> str:
            alter = super().altercolumn_sql(expression)

            collate = self.sql(expression, "collate")
            collate = f" COLLATE {collate}" if collate else ""
            return f"{alter}{collate}"

        def computedcolumnconstraint_sql(self, expression: exp.ComputedColumnConstraint) -> str:
            this = self.sql(expression, "this")
            not_null = " NOT NULL" if expression.args.get("not_null") else ""
            type = self.sql(expression, "data_type") or "AUTO"
            return f"AS {this} PERSISTED {type}{not_null}"
