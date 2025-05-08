from __future__ import annotations
import typing as t
import datetime
from sqlglot import exp, generator, parser, tokens, parse_one
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    arg_max_or_min_no_count,
    binary_from_function,
    build_date_delta,
    build_formatted_time,
    inline_array_sql,
    json_extract_segments,
    json_path_key_only_name,
    length_or_char_length_sql,
    no_pivot_sql,
    build_json_extract_path,
    rename_func,
    sha256_sql,
    strposition_sql,
    var_map_sql,
    timestamptrunc_sql,
    unit_to_var,
    trim_sql,
)
from sqlglot.generator import Generator
from sqlglot.helper import is_int, seq_get
from sqlglot.tokens import Token, TokenType
from sqlglot.generator import unsupported_args

DATEΤΙΜΕ_DELTA = t.Union[
    exp.DateAdd, exp.DateDiff, exp.DateSub, exp.TimestampSub, exp.TimestampAdd
]


def _build_date_format(args: t.List) -> exp.TimeToStr:
    expr = build_formatted_time(exp.TimeToStr, "exasol")(args)

    timezone = seq_get(args, 2)
    if timezone:
        expr.set("zone", timezone)

    return expr


def _unix_to_time_sql(self: Exasol.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func(
            "fromUnixTimestamp", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )
    if scale == exp.UnixToTime.MILLIS:
        return self.func(
            "fromUnixTimestamp64Milli", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )
    if scale == exp.UnixToTime.MICROS:
        return self.func(
            "fromUnixTimestamp64Micro", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )
    if scale == exp.UnixToTime.NANOS:
        return self.func(
            "fromUnixTimestamp64Nano", exp.cast(timestamp, exp.DataType.Type.BIGINT)
        )

    return self.func(
        "fromUnixTimestamp",
        exp.cast(
            exp.Div(this=timestamp, expression=exp.func("POW", 10, scale)),
            exp.DataType.Type.BIGINT,
        ),
    )


def _lower_func(sql: str) -> str:
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


def _quantile_sql(self: Exasol.Generator, expression: exp.Quantile) -> str:
    quantile = expression.args["quantile"]
    args = f"({self.sql(expression, 'this')})"

    if isinstance(quantile, exp.Array):
        func = self.func("quantiles", *quantile)
    else:
        func = self.func("quantile", quantile)

    return func + args


def _build_count_if(args: t.List) -> exp.CountIf | exp.CombinedAggFunc:
    if len(args) == 1:
        return exp.CountIf(this=seq_get(args, 0))

    return exp.CombinedAggFunc(this="countIf", expressions=args)


def _build_str_to_date(args: t.List) -> exp.Cast | exp.Anonymous:
    if len(args) == 3:
        return exp.Anonymous(this="STR_TO_DATE", expressions=args)

    strtodate = exp.StrToDate.from_arg_list(args)
    return exp.cast(strtodate, exp.DataType.build(exp.DataType.Type.DATETIME))


def _datetime_delta_sql(name: str) -> t.Callable[[Generator, DATEΤΙΜΕ_DELTA], str]:
    def _delta_sql(self: Generator, expression: DATEΤΙΜΕ_DELTA) -> str:
        if not expression.unit:
            return rename_func(name)(self, expression)

        return self.func(
            name,
            unit_to_var(expression),
            expression.expression,
            expression.this,
        )

    return _delta_sql


def _timestrtotime_sql(self: Exasol.Generator, expression: exp.TimeStrToTime):
    ts = expression.this

    tz = expression.args.get("zone")
    if tz and isinstance(ts, exp.Literal):
        # Clickhouse will not accept timestamps that include a UTC offset, so we must remove them.
        # The first step to removing is parsing the string with `datetime.datetime.fromisoformat`.
        #
        # In python <3.11, `fromisoformat()` can only parse timestamps of millisecond (3 digit)
        # or microsecond (6 digit) precision. It will error if passed any other number of fractional
        # digits, so we extract the fractional seconds and pad to 6 digits before parsing.
        ts_string = ts.name.strip()

        # separate [date and time] from [fractional seconds and UTC offset]
        ts_parts = ts_string.split(".")
        if len(ts_parts) == 2:
            # separate fractional seconds and UTC offset
            offset_sep = "+" if "+" in ts_parts[1] else "-"
            ts_frac_parts = ts_parts[1].split(offset_sep)
            num_frac_parts = len(ts_frac_parts)

            # pad to 6 digits if fractional seconds present
            ts_frac_parts[0] = ts_frac_parts[0].ljust(6, "0")
            ts_string = "".join(
                [
                    ts_parts[0],  # date and time
                    ".",
                    ts_frac_parts[0],  # fractional seconds
                    offset_sep if num_frac_parts > 1 else "",
                    ts_frac_parts[1]
                    if num_frac_parts > 1
                    else "",  # utc offset (if present)
                ]
            )

        # return literal with no timezone, eg turn '2020-01-01 12:13:14-08:00' into '2020-01-01 12:13:14'
        # this is because Clickhouse encodes the timezone as a data type parameter and throws an error if
        # it's part of the timestamp string
        ts_without_tz = (
            datetime.datetime.fromisoformat(ts_string)
            .replace(tzinfo=None)
            .isoformat(sep=" ")
        )
        ts = exp.Literal.string(ts_without_tz)

    # Non-nullable DateTime64 with microsecond precision
    expressions = [exp.DataTypeParam(this=tz)] if tz else []
    datatype = exp.DataType.build(
        exp.DataType.Type.DATETIME64,
        expressions=[exp.DataTypeParam(this=exp.Literal.number(6)), *expressions],
        nullable=False,
    )

    return self.sql(exp.cast(ts, datatype, dialect=self.dialect))


def _map_sql(self: Exasol.Generator, expression: exp.Map | exp.VarMap) -> str:
    if not (expression.parent and expression.parent.arg_key == "settings"):
        return _lower_func(var_map_sql(self, expression))

    keys = expression.args.get("keys")
    values = expression.args.get("values")

    if not isinstance(keys, exp.Array) or not isinstance(values, exp.Array):
        self.unsupported("Cannot convert array columns into map.")
        return ""

    args = []
    for key, value in zip(keys.expressions, values.expressions):
        args.append(f"{self.sql(key)}: {self.sql(value)}")

    csv_args = ", ".join(args)

    return f"{{{csv_args}}}"


class ExasolTokenType:
    WITH_LOCAL_TIME_ZONE = "WITH_LOCAL_TIME_ZONE"
    HASHTYPE = "HASHTYPE"
    BYTE = "BYTE"
    MONTH = "MONTH"
    DAY = "DAY"
    SECOND = "SECOND"
    TO = "TO"


class Tokenizer(tokens.Tokenizer):
    IDENTIFIER_ESCAPES = ['"']
    STRING_ESCAPES = ["'"]

    KEYWORDS = {
        **tokens.Tokenizer.KEYWORDS,
        "YEAR": TokenType.YEAR,
        "WITH LOCAL TIME ZONE": ExasolTokenType.WITH_LOCAL_TIME_ZONE,
        "MONTH": ExasolTokenType.MONTH,
        "DAY": ExasolTokenType.DAY,
        "SECOND": ExasolTokenType.SECOND,
        "TO": ExasolTokenType.TO,
        "HASHTYPE": ExasolTokenType.HASHTYPE,
        "BYTE": ExasolTokenType.BYTE,
    }


def _build_truncate(args: t.List) -> exp.Div:
    """
    exasol TRUNC[ATE] (number): https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/trunc[ate]%20(number).htm#TRUNC[ATE]_(number)
    example:
        exasol query: SELECT TRUNC(123.456,-2) -> 100
        generic query: SELECT FLOOR(123.456 * POWER(10,-2)) / POWER(10,-2) -> 100
    """
    number = seq_get(args, 0)
    truncate = seq_get(args, 1) or 0  # if no truncate arg then integer truncation
    sql = f"FLOOR({number} * POWER(10,{truncate})) / POWER(10,{truncate})"
    return parse_one(sql)
    # return exp.Div(
    #     this=exp.Floor(
    #         this=exp.Mul(
    #             this=exp.Literal(this=number,is_string=isinstance(number,str)),
    #             expression=exp.Pow(
    #                 this=exp.Literal(this=10,is_string=False),
    #                 expression=exp.Literal(this=truncate,is_string=isinstance(number,str)),
    #             ),
    #         )
    #     ),
    #     expression=exp.Pow(
    #         this=exp.Literal(this=10,is_string=False),
    #         expression=exp.Literal(this=truncate,is_string=isinstance(number,str)),
    #     ),
    #     # typed=???,
    #     # safe=???
    # )


class Exasol(Dialect):
    ANNOTATORS = {
        **Dialect.ANNOTATORS,
        # PI() has no args
    }

    class Tokenizer(tokens.Tokenizer):
        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
        }
        SINGLE_TOKENS.pop("%")  # "%": TokenType.MOD not supported in exasol

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }
        KEYWORDS.pop("DIV")

    class Parser(parser.Parser):
        # ADD_(DAYS | HOURS | MINUTES | MONTHS | SECONS | WEEKS | YEARS)
        # https://docs.exasol.com/db/latest/sql_references/functions/scalarfunctions.htm
        DATE_ADD_FUNCTIONS = {
            f"ADD_{unit}S": build_date_delta(exp.DateAdd, None, unit)
            for unit in ["DAY", "HOUR", "MINUTE", "MONTH", "SECOND", "WEEK", "YEAR"]
        }

        NUMERIC_FUNCTIONS = {
            ########### HANDLE IN GENERATOR ############
            # "MOD": a % b not allowed
            #       needs to be handled in generator
            #       see: bigquery line 1166 mod_sql
            # "TO_NUMBER": # in generator: SUPPORTS_TO_NUMBER = True
            #       but could handle boolean like CAST(TRUE AS DECIMAL(1,0))
            #       see: tonumber_sql in sqlglot/generator.py
            #       see: https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_number.htm#TO_NUMBER
            ########### HANDLE IN GENERATOR ############
            "DIV": binary_from_function(exp.IntDiv),
            "RANDOM": lambda args: exp.Rand(
                lower=seq_get(args, 0), upper=seq_get(args, 1)
            ),
            "TRUNCATE": _build_truncate,
            "TRUNC": _build_truncate,
            ########## NEED MORE RESEARCH ###########
            # "TO_CHAR": # exp.NumberToStr research formats
            # "MIN_SCALE": , # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/min_scale.htm
            # "WIDTH_BUCKET": , # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/width_bucket.htm
            # in general: how to handle exp.Anonymous ???
            ########## NEED MORE RESEARCH ###########
        }

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            **DATE_ADD_FUNCTIONS,
            **NUMERIC_FUNCTIONS,
        }

        ############################# CHECKED (changed) ##########################
        # Whether string aliases are supported `SELECT COUNT(*) 'count'`
        # Exasol: supports this, tested with exasol db
        STRING_ALIASES = True  # default False

        # Whether query modifiers such as LIMIT are attached to the UNION node (vs its right operand)
        # Exasol: only possible to attach to righ subquery (operand)
        MODIFIERS_ATTACHED_TO_SET_OP = False  # default True
        SET_OP_MODIFIERS = {"order", "limit", "offset"}

        ########################### CHECKED (not changed) ##############################

        # Whether or not a VALUES keyword needs to be followed by '(' to form a VALUES clause.
        # If this is True and '(' is not found, the keyword will be treated as an identifier
        # Exasol: generally true, however different when VALUES BETWEEN ... is used (https://docs.exasol.com/db/latest/sql/insert.htm)
        # Exasol also allows this (see example at end: https://docs.exasol.com/db/latest/sql_references/data_types/typeconversionrules.htm)
        VALUES_FOLLOWED_BY_PAREN = True

        # see: https://docs.exasol.com/db/latest/sql_references/data_types/typeconversionrules.htm
        # see: https://docs.exasol.com/db/latest/sql_references/sqlstandardcompliance.htm
        STRICT_CAST = True

        # see: https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/log.htm
        LOG_DEFAULTS_TO_LN = False

        # Whether ADD is present for each column added by ALTER TABLE
        # Exasol see: https://docs.exasol.com/db/latest/sql/alter_table(column).htm
        ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = True

        # Whether the table sample clause expects CSV syntax
        # Exasol does not seem to support TABLESAMPLE at all
        TABLESAMPLE_CSV = False

        # The default method used for table sampling
        # Exasol does not seem to support TABLESAMPLE at all
        DEFAULT_SAMPLING_METHOD: t.Optional[str] = None

        # Whether the TRIM function expects the characters to trim as its first argument
        # exasol: https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/trim.htm
        TRIM_PATTERN_FIRST = False

        # Whether to parse IF statements that aren't followed by a left parenthesis as commands
        # exasol: https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/if.htm
        NO_PAREN_IF_COMMANDS = True

        ############################ NOT SURE ##########################

        # Whether the SET command needs a delimiter (e.g. "=") for assignments
        # Exasol: https://docs.exasol.com/db/latest/search.htm?q=SET%20&f=aws
        SET_REQUIRES_ASSIGNMENT_DELIMITER = True

        ############################ DEFAULT ##########################

        PARTITION_KEYWORDS = {"PARTITION", "SUBPARTITION"}

        AMBIGUOUS_ALIAS_TOKENS = (TokenType.LIMIT, TokenType.OFFSET)

        OPERATION_MODIFIERS: t.Set[str] = set()

        RECURSIVE_CTE_SEARCH_KIND = {"BREADTH", "DEPTH", "CYCLE"}

        MODIFIABLES = (exp.Query, exp.Table, exp.TableFromRows)

        PREFIXED_PIVOT_COLUMNS = False
        IDENTIFY_PIVOT_STRINGS = False

        # Whether the -> and ->> operators expect documents of type JSON (e.g. Postgres)
        JSON_ARROWS_REQUIRE_JSON_TYPE = False

        # Whether the `:` operator is used to extract a value from a VARIANT column
        COLON_IS_VARIANT_EXTRACT = False

        # Whether implicit unnesting is supported, e.g. SELECT 1 FROM y.z AS z, z.a (Redshift)
        SUPPORTS_IMPLICIT_UNNEST = False

        # Whether or not interval spans are supported, INTERVAL 1 YEAR TO MONTHS
        INTERVAL_SPANS = True

        # Whether a PARTITION clause can follow a table reference
        SUPPORTS_PARTITION_SELECTION = False

        # Whether the `name AS expr` schema/column constraint requires parentheses around `expr`
        WRAPPED_TRANSFORM_COLUMN_CONSTRAINT = True

        # Whether the 'AS' keyword is optional in the CTE definition syntax
        OPTIONAL_ALIAS_TOKEN_CTE = True

    # class Generator(generator.Generator):
    #     QUERY_HINTS = False
    #     STRUCT_DELIMITER = ("(", ")")
    #     NVL2_SUPPORTED = False
    #     TABLESAMPLE_REQUIRES_PARENS = False
    #     TABLESAMPLE_SIZE_IS_ROWS = False
    #     TABLESAMPLE_KEYWORDS = "SAMPLE"
    #     LAST_DAY_SUPPORTS_DATE_PART = False
    #     CAN_IMPLEMENT_ARRAY_ANY = True
    #     SUPPORTS_TO_NUMBER = False
    #     JOIN_HINTS = False
    #     TABLE_HINTS = False
    #     GROUPINGS_SEP = ""
    #     SET_OP_MODIFIERS = False
    #     VALUES_AS_TABLE = False
    #     ARRAY_SIZE_NAME = "LENGTH"

    #     STRING_TYPE_MAPPING = {
    #         exp.DataType.Type.BLOB: "VARCHAR",
    #         exp.DataType.Type.CHAR: "CHAR",
    #         exp.DataType.Type.LONGBLOB: "VARCHAR",
    #         exp.DataType.Type.LONGTEXT: "VARCHAR",
    #         exp.DataType.Type.MEDIUMBLOB: "VARCHAR",
    #         exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
    #         exp.DataType.Type.TINYBLOB: "VARCHAR",
    #         exp.DataType.Type.TINYTEXT: "VARCHAR",
    #         exp.DataType.Type.TEXT: "VARCHAR",
    #         exp.DataType.Type.VARBINARY: "VARCHAR",
    #         exp.DataType.Type.VARCHAR: "VARCHAR",
    #     }

    #     SUPPORTED_JSON_PATH_PARTS = {
    #         exp.JSONPathKey,
    #         exp.JSONPathRoot,
    #         exp.JSONPathSubscript,
    #     }

    #     TYPE_MAPPING = {
    #         **generator.Generator.TYPE_MAPPING,
    #         **STRING_TYPE_MAPPING,
    #         exp.DataType.Type.ARRAY: "Array",
    #         exp.DataType.Type.BOOLEAN: "Bool",
    #         exp.DataType.Type.BIGINT: "Int64",
    #         exp.DataType.Type.DATE32: "Date32",
    #         exp.DataType.Type.DATETIME: "DateTime",
    #         exp.DataType.Type.DATETIME2: "DateTime",
    #         exp.DataType.Type.SMALLDATETIME: "DateTime",
    #         exp.DataType.Type.DATETIME64: "DateTime64",
    #         exp.DataType.Type.DECIMAL: "Decimal",
    #         exp.DataType.Type.DECIMAL32: "Decimal32",
    #         exp.DataType.Type.DECIMAL64: "Decimal64",
    #         exp.DataType.Type.DECIMAL128: "Decimal128",
    #         exp.DataType.Type.DECIMAL256: "Decimal256",
    #         exp.DataType.Type.TIMESTAMP: "DateTime",
    #         exp.DataType.Type.TIMESTAMPTZ: "DateTime",
    #         exp.DataType.Type.DOUBLE: "Float64",
    #         exp.DataType.Type.ENUM: "Enum",
    #         exp.DataType.Type.ENUM8: "Enum8",
    #         exp.DataType.Type.ENUM16: "Enum16",
    #         exp.DataType.Type.FIXEDSTRING: "FixedString",
    #         exp.DataType.Type.FLOAT: "Float32",
    #         exp.DataType.Type.INT: "Int32",
    #         exp.DataType.Type.MEDIUMINT: "Int32",
    #         exp.DataType.Type.INT128: "Int128",
    #         exp.DataType.Type.INT256: "Int256",
    #         exp.DataType.Type.LOWCARDINALITY: "LowCardinality",
    #         exp.DataType.Type.MAP: "Map",
    #         exp.DataType.Type.NESTED: "Nested",
    #         exp.DataType.Type.SMALLINT: "Int16",
    #         exp.DataType.Type.STRUCT: "Tuple",
    #         exp.DataType.Type.TINYINT: "Int8",
    #         exp.DataType.Type.UBIGINT: "UInt64",
    #         exp.DataType.Type.UINT: "UInt32",
    #         exp.DataType.Type.UINT128: "UInt128",
    #         exp.DataType.Type.UINT256: "UInt256",
    #         exp.DataType.Type.USMALLINT: "UInt16",
    #         exp.DataType.Type.UTINYINT: "UInt8",
    #         exp.DataType.Type.IPV4: "IPv4",
    #         exp.DataType.Type.IPV6: "IPv6",
    #         exp.DataType.Type.POINT: "Point",
    #         exp.DataType.Type.RING: "Ring",
    #         exp.DataType.Type.LINESTRING: "LineString",
    #         exp.DataType.Type.MULTILINESTRING: "MultiLineString",
    #         exp.DataType.Type.POLYGON: "Polygon",
    #         exp.DataType.Type.MULTIPOLYGON: "MultiPolygon",
    #         exp.DataType.Type.AGGREGATEFUNCTION: "AggregateFunction",
    #         exp.DataType.Type.SIMPLEAGGREGATEFUNCTION: "SimpleAggregateFunction",
    #         exp.DataType.Type.DYNAMIC: "Dynamic",
    #     }

    #     TRANSFORMS = {
    #         **generator.Generator.TRANSFORMS,
    #         exp.AnyValue: rename_func("any"),
    #         exp.ApproxDistinct: rename_func("uniq"),
    #         exp.ArrayConcat: rename_func("arrayConcat"),
    #         exp.ArrayFilter: lambda self, e: self.func(
    #             "arrayFilter", e.expression, e.this
    #         ),
    #         exp.ArraySum: rename_func("arraySum"),
    #         exp.ArgMax: arg_max_or_min_no_count("argMax"),
    #         exp.ArgMin: arg_max_or_min_no_count("argMin"),
    #         exp.Array: inline_array_sql,
    #         exp.CastToStrType: rename_func("CAST"),
    #         exp.CountIf: rename_func("countIf"),
    #         exp.CompressColumnConstraint: lambda self,
    #         e: f"CODEC({self.expressions(e, key='this', flat=True)})",
    #         exp.ComputedColumnConstraint: lambda self,
    #         e: f"{'MATERIALIZED' if e.args.get('persisted') else 'ALIAS'} {self.sql(e, 'this')}",
    #         exp.CurrentDate: lambda self, e: self.func("CURRENT_DATE"),
    #         exp.DateAdd: _datetime_delta_sql("DATE_ADD"),
    #         exp.DateDiff: _datetime_delta_sql("DATE_DIFF"),
    #         exp.DateStrToDate: rename_func("toDate"),
    #         exp.DateSub: _datetime_delta_sql("DATE_SUB"),
    #         exp.Explode: rename_func("arrayJoin"),
    #         exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
    #         exp.IsNan: rename_func("isNaN"),
    #         exp.JSONCast: lambda self, e: f"{self.sql(e, 'this')}.:{self.sql(e, 'to')}",
    #         exp.JSONExtract: json_extract_segments(
    #             "JSONExtractString", quoted_index=False
    #         ),
    #         exp.JSONExtractScalar: json_extract_segments(
    #             "JSONExtractString", quoted_index=False
    #         ),
    #         exp.JSONPathKey: json_path_key_only_name,
    #         exp.JSONPathRoot: lambda *_: "",
    #         exp.Length: length_or_char_length_sql,
    #         exp.Map: _map_sql,
    #         exp.Median: rename_func("median"),
    #         exp.Nullif: rename_func("nullIf"),
    #         exp.PartitionedByProperty: lambda self,
    #         e: f"PARTITION BY {self.sql(e, 'this')}",
    #         exp.Pivot: no_pivot_sql,
    #         exp.Quantile: _quantile_sql,
    #         exp.RegexpLike: lambda self, e: self.func("match", e.this, e.expression),
    #         exp.Rand: rename_func("randCanonical"),
    #         exp.StartsWith: rename_func("startsWith"),
    #         exp.StrPosition: lambda self, e: strposition_sql(
    #             self,
    #             e,
    #             func_name="POSITION",
    #             supports_position=True,
    #             use_ansi_position=False,
    #         ),
    #         exp.TimeToStr: lambda self, e: self.func(
    #             "formatDateTime", e.this, self.format_time(e), e.args.get("zone")
    #         ),
    #         exp.TimeStrToTime: _timestrtotime_sql,
    #         exp.TimestampAdd: _datetime_delta_sql("TIMESTAMP_ADD"),
    #         exp.TimestampSub: _datetime_delta_sql("TIMESTAMP_SUB"),
    #         exp.VarMap: _map_sql,
    #         exp.Xor: lambda self, e: self.func(
    #             "xor", e.this, e.expression, *e.expressions
    #         ),
    #         exp.MD5Digest: rename_func("MD5"),
    #         exp.MD5: lambda self, e: self.func(
    #             "LOWER", self.func("HEX", self.func("MD5", e.this))
    #         ),
    #         exp.SHA: rename_func("SHA1"),
    #         exp.SHA2: sha256_sql,
    #         exp.UnixToTime: _unix_to_time_sql,
    #         exp.TimestampTrunc: timestamptrunc_sql(zone=True),
    #         exp.Trim: lambda self, e: trim_sql(self, e, default_trim_type="BOTH"),
    #         exp.Variance: rename_func("varSamp"),
    #         exp.SchemaCommentProperty: lambda self, e: self.naked_property(e),
    #         exp.Stddev: rename_func("stddevSamp"),
    #         exp.Chr: rename_func("CHAR"),
    #         exp.Lag: lambda self, e: self.func(
    #             "lagInFrame", e.this, e.args.get("offset"), e.args.get("default")
    #         ),
    #         exp.Lead: lambda self, e: self.func(
    #             "leadInFrame", e.this, e.args.get("offset"), e.args.get("default")
    #         ),
    #         exp.Levenshtein: unsupported_args(
    #             "ins_cost", "del_cost", "sub_cost", "max_dist"
    #         )(rename_func("editDistance")),
    #     }

    #     PROPERTIES_LOCATION = {
    #         **generator.Generator.PROPERTIES_LOCATION,
    #         exp.OnCluster: exp.Properties.Location.POST_NAME,
    #         exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
    #         exp.ToTableProperty: exp.Properties.Location.POST_NAME,
    #         exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
    #     }

    #     # There's no list in docs, but it can be found in Clickhouse code
    #     # see `ClickHouse/src/Parsers/ParserCreate*.cpp`
    #     ON_CLUSTER_TARGETS = {
    #         "SCHEMA",  # Transpiled CREATE SCHEMA may have OnCluster property set
    #         "DATABASE",
    #         "TABLE",
    #         "VIEW",
    #         "DICTIONARY",
    #         "INDEX",
    #         "FUNCTION",
    #         "NAMED COLLECTION",
    #     }

    #     # https://clickhouse.com/docs/en/sql-reference/data-types/nullable
    #     NON_NULLABLE_TYPES = {
    #         exp.DataType.Type.ARRAY,
    #         exp.DataType.Type.MAP,
    #         exp.DataType.Type.STRUCT,
    #         exp.DataType.Type.POINT,
    #         exp.DataType.Type.RING,
    #         exp.DataType.Type.LINESTRING,
    #         exp.DataType.Type.MULTILINESTRING,
    #         exp.DataType.Type.POLYGON,
    #         exp.DataType.Type.MULTIPOLYGON,
    #     }

    #     def strtodate_sql(self, expression: exp.StrToDate) -> str:
    #         strtodate_sql = self.function_fallback_sql(expression)

    #         if not isinstance(expression.parent, exp.Cast):
    #             # StrToDate returns DATEs in other dialects (eg. postgres), so
    #             # this branch aims to improve the transpilation to clickhouse
    #             return self.cast_sql(exp.cast(expression, "DATE"))

    #         return strtodate_sql

    #     def cast_sql(
    #         self, expression: exp.Cast, safe_prefix: t.Optional[str] = None
    #     ) -> str:
    #         this = expression.this

    #         if isinstance(this, exp.StrToDate) and expression.to == exp.DataType.build(
    #             "datetime"
    #         ):
    #             return self.sql(this)

    #         return super().cast_sql(expression, safe_prefix=safe_prefix)

    #     def trycast_sql(self, expression: exp.TryCast) -> str:
    #         dtype = expression.to
    #         if not dtype.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True):
    #             # Casting x into Nullable(T) appears to behave similarly to TRY_CAST(x AS T)
    #             dtype.set("nullable", True)

    #         return super().cast_sql(expression)

    #     def _jsonpathsubscript_sql(self, expression: exp.JSONPathSubscript) -> str:
    #         this = self.json_path_part(expression.this)
    #         return str(int(this) + 1) if is_int(this) else this

    #     def likeproperty_sql(self, expression: exp.LikeProperty) -> str:
    #         return f"AS {self.sql(expression, 'this')}"

    #     def _any_to_has(
    #         self,
    #         expression: exp.EQ | exp.NEQ,
    #         default: t.Callable[[t.Any], str],
    #         prefix: str = "",
    #     ) -> str:
    #         if isinstance(expression.left, exp.Any):
    #             arr = expression.left
    #             this = expression.right
    #         elif isinstance(expression.right, exp.Any):
    #             arr = expression.right
    #             this = expression.left
    #         else:
    #             return default(expression)

    #         return prefix + self.func("has", arr.this.unnest(), this)

    #     def eq_sql(self, expression: exp.EQ) -> str:
    #         return self._any_to_has(expression, super().eq_sql)

    #     def neq_sql(self, expression: exp.NEQ) -> str:
    #         return self._any_to_has(expression, super().neq_sql, "NOT ")

    #     def regexpilike_sql(self, expression: exp.RegexpILike) -> str:
    #         # Manually add a flag to make the search case-insensitive
    #         regex = self.func("CONCAT", "'(?i)'", expression.expression)
    #         return self.func("match", expression.this, regex)

    #     def datatype_sql(self, expression: exp.DataType) -> str:
    #         # String is the standard ClickHouse type, every other variant is just an alias.
    #         # Additionally, any supplied length parameter will be ignored.
    #         #
    #         # https://clickhouse.com/docs/en/sql-reference/data-types/string
    #         if expression.this in self.STRING_TYPE_MAPPING:
    #             # print(self.STRING_TYPE_MAPPING[expression.this])
    #             dtype = self.STRING_TYPE_MAPPING[expression.this]
    #         else:
    #             dtype = super().datatype_sql(expression)

    #         # This section changes the type to `Nullable(...)` if the following conditions hold:
    #         # - It's marked as nullable - this ensures we won't wrap ClickHouse types with `Nullable`
    #         #   and change their semantics
    #         # - It's not the key type of a `Map`. This is because ClickHouse enforces the following
    #         #   constraint: "Type of Map key must be a type, that can be represented by integer or
    #         #   String or FixedString (possibly LowCardinality) or UUID or IPv6"
    #         # - It's not a composite type, e.g. `Nullable(Array(...))` is not a valid type
    #         # parent = expression.parent

    #         # nullable = expression.args.get("nullable")
    #         # if nullable is True or (
    #         #     nullable is None
    #         #     and not (
    #         #         isinstance(parent, exp.DataType)
    #         #         and parent.is_type(exp.DataType.Type.MAP, check_nullable=True)
    #         #         and expression.index in (None, 0)
    #         #     )
    #         #     and not expression.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True)
    #         # ):
    #         #     dtype = f"Nullable({dtype})"

    #         return dtype

    #     def cte_sql(self, expression: exp.CTE) -> str:
    #         if expression.args.get("scalar"):
    #             this = self.sql(expression, "this")
    #             alias = self.sql(expression, "alias")
    #             return f"{this} AS {alias}"

    #         return super().cte_sql(expression)

    #     def after_limit_modifiers(self, expression: exp.Expression) -> t.List[str]:
    #         return super().after_limit_modifiers(expression) + [
    #             (
    #                 self.seg("SETTINGS ")
    #                 + self.expressions(expression, key="settings", flat=True)
    #                 if expression.args.get("settings")
    #                 else ""
    #             ),
    #             (
    #                 self.seg("FORMAT ") + self.sql(expression, "format")
    #                 if expression.args.get("format")
    #                 else ""
    #             ),
    #         ]

    #     def placeholder_sql(self, expression: exp.Placeholder) -> str:
    #         return f"{{{expression.name}: {self.sql(expression, 'kind')}}}"

    #     def oncluster_sql(self, expression: exp.OnCluster) -> str:
    #         return f"ON CLUSTER {self.sql(expression, 'this')}"

    #     def createable_sql(
    #         self, expression: exp.Create, locations: t.DefaultDict
    #     ) -> str:
    #         if expression.kind in self.ON_CLUSTER_TARGETS and locations.get(
    #             exp.Properties.Location.POST_NAME
    #         ):
    #             this_name = self.sql(
    #                 expression.this
    #                 if isinstance(expression.this, exp.Schema)
    #                 else expression,
    #                 "this",
    #             )
    #             this_properties = " ".join(
    #                 [
    #                     self.sql(prop)
    #                     for prop in locations[exp.Properties.Location.POST_NAME]
    #                 ]
    #             )
    #             this_schema = self.schema_columns_sql(expression.this)
    #             this_schema = f"{self.sep()}{this_schema}" if this_schema else ""

    #             return f"{this_name}{self.sep()}{this_properties}{this_schema}"

    #         return super().createable_sql(expression, locations)

    #     def create_sql(self, expression: exp.Create) -> str:
    #         # The comment property comes last in CTAS statements, i.e. after the query
    #         query = expression.expression
    #         if isinstance(query, exp.Query):
    #             comment_prop = expression.find(exp.SchemaCommentProperty)
    #             if comment_prop:
    #                 comment_prop.pop()
    #                 query.replace(exp.paren(query))
    #         else:
    #             comment_prop = None

    #         create_sql = super().create_sql(expression)

    #         comment_sql = self.sql(comment_prop)
    #         comment_sql = f" {comment_sql}" if comment_sql else ""

    #         return f"{create_sql}{comment_sql}"

    #     def prewhere_sql(self, expression: exp.PreWhere) -> str:
    #         this = self.indent(self.sql(expression, "this"))
    #         return f"{self.seg('PREWHERE')}{self.sep()}{this}"

    #     def indexcolumnconstraint_sql(
    #         self, expression: exp.IndexColumnConstraint
    #     ) -> str:
    #         this = self.sql(expression, "this")
    #         this = f" {this}" if this else ""
    #         expr = self.sql(expression, "expression")
    #         expr = f" {expr}" if expr else ""
    #         index_type = self.sql(expression, "index_type")
    #         index_type = f" TYPE {index_type}" if index_type else ""
    #         granularity = self.sql(expression, "granularity")
    #         granularity = f" GRANULARITY {granularity}" if granularity else ""

    #         return f"INDEX{this}{expr}{index_type}{granularity}"

    #     def partition_sql(self, expression: exp.Partition) -> str:
    #         return f"PARTITION {self.expressions(expression, flat=True)}"

    #     def partitionid_sql(self, expression: exp.PartitionId) -> str:
    #         return f"ID {self.sql(expression.this)}"

    #     def replacepartition_sql(self, expression: exp.ReplacePartition) -> str:
    #         return f"REPLACE {self.sql(expression.expression)} FROM {self.sql(expression, 'source')}"

    #     def projectiondef_sql(self, expression: exp.ProjectionDef) -> str:
    #         return f"PROJECTION {self.sql(expression.this)} {self.wrap(expression.expression)}"

    #     def is_sql(self, expression: exp.Is) -> str:
    #         is_sql = super().is_sql(expression)

    #         if isinstance(expression.parent, exp.Not):
    #             # value IS NOT NULL -> NOT (value IS NULL)
    #             is_sql = self.wrap(is_sql)

    #         return is_sql

    #     def in_sql(self, expression: exp.In) -> str:
    #         in_sql = super().in_sql(expression)

    #         if isinstance(expression.parent, exp.Not) and expression.args.get(
    #             "is_global"
    #         ):
    #             in_sql = in_sql.replace("GLOBAL IN", "GLOBAL NOT IN", 1)

    #         return in_sql

    #     def not_sql(self, expression: exp.Not) -> str:
    #         if isinstance(expression.this, exp.In) and expression.this.args.get(
    #             "is_global"
    #         ):
    #             # let `GLOBAL IN` child interpose `NOT`
    #             return self.sql(expression, "this")

    #         return super().not_sql(expression)
