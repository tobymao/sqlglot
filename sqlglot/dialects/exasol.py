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


def _string_position_sql(self: Exasol.Generator, expression: exp.StrPosition) -> str:
    """
    - exasol uses:
        POSITION(substr IN this)
        LOCATE(this, substr, position)
        INSTR(this, substr, position, occurence)
    - sqlglot parses both as exp.StrPosition
    - depending on the number of args we return respectively
    """
    this = expression.args.get("this")
    substr = expression.args.get("substr")
    position = expression.args.get("position")
    occurrence = expression.args.get("occurrence")
    if position is None and occurrence is None:
        return f"POSITION({self.sql(expression, 'substr')} IN {self.sql(expression, 'this')})"
    elif occurrence is None:
        # this and substr swapped while parsing LOCATE
        # see: sqlglot/parser.py (line 157) build_locate_strposition
        return self.func("LOCATE", substr, this, position)
    else:
        return self.func("INSTR", this, substr, position, occurrence)


class Exasol(Dialect):
    ANNOTATORS = {
        **Dialect.ANNOTATORS,
        # PI() has no args
    }

    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    TIME_MAPPING = {
        # --- Year ---
        "YYYY": "%Y",  # 4-digit year (e.g., 2025)
        "YYY": "%Y",  # 3-digit year (often same as YYYY in practice for TO_CHAR)
        "YY": "%y",  # Last 2 digits of year (e.g., 25)
        "Y": "%y",  # Last digit of year
        # --- Month ---
        "MM": "%m",  # Month (01-12)
        "MON": "%b",  # Abbreviated month name (e.g., JAN)
        "MONTH": "%B",  # Full month name (e.g., JANUARY)
        "RM": "%m",  # Roman numeral month (no direct strftime, maps to decimal)
        # --- Day ---
        "DD": "%d",  # Day of month (01-31)
        "DDD": "%j",  # Day of year (001-366)
        "DY": "%a",  # Abbreviated weekday name (e.g., MON)
        "DAY": "%A",  # Full weekday name (e.g., MONDAY)
        "D": "%w",  # Day of week (1-7, 1=Sunday, matches %w; for ISO 8601 day, see ID)
        # --- Hour ---
        "HH24": "%H",  # Hour (00-23)
        "HH12": "%I",  # Hour (01-12)
        "HH": "%I",  # (alias for HH12)
        "AM": "%p",  # AM/PM meridian indicator
        "PM": "%p",  # AM/PM meridian indicator
        # --- Minute ---
        "MI": "%M",  # Minute (00-59)
        # --- Second ---
        "SS": "%S",  # Second (00-59)
        # Fractional Seconds: Exasol uses FF[1-9]. strftime uses %f (microseconds)
        "FF": "%f",  # Maps to microseconds. Precision (FF1-FF9) might need truncation/padding.
        "FF1": "%f",
        "FF2": "%f",
        "FF3": "%f",  # Often used for milliseconds
        "FF4": "%f",
        "FF5": "%f",
        "FF6": "%f",  # Microseconds
        "FF7": "%f",
        "FF8": "%f",
        "FF9": "%f",
        # --- Timezone ---
        "TZH": "%z",  # Timezone hour offset (+/-HH)
        "TZM": "%z",  # Timezone minute offset (often combined with TZH)
        "TZHTZM": "%z",  # Combined timezone offset (+/-HHMM). Note: strftime %z is +/-HHMM or +/-HH:MM
        # --- ISO 8601 Specific ---
        "IYYY": "%G",  # ISO Year (4-digit)
        "IW": "%V",  # ISO Week of year (01-53)
        "ID": "%u",  # ISO Day of week (1-7, 1=Monday)
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

        STRING_FUNCTIONS = {
            "ASCII": exp.Unicode.from_arg_list,
            "REGEXP_SUBSTR": exp.RegexpExtract.from_arg_list,
            ######### NEED MORE RESEARCH ##########
            # "OCTET_LENGTH": , # Anonymous
            # "REGEXP_INSTR": ,
            # "TO_CHAR": _build_to_char # sqlglot/dialects/presto.py line 175 - research formats
            ######### NEED MORE RESEARCH ##########
        }

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            **DATE_ADD_FUNCTIONS,
            **NUMERIC_FUNCTIONS,
            **STRING_FUNCTIONS,
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            # exasol allows: VARCHAR(200) [CHARACTER SET]? [UTF8 | ASCII]
            # stringtype def: https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm
            "UTF8": lambda self: self.expression(
                exp.CharacterSetColumnConstraint, this=exp.Var(this="UTF8")
            ),
            "ASCII": lambda self: self.expression(
                exp.CharacterSetColumnConstraint, this=exp.Var(this="ASCII")
            ),
        }

        ############################# CHECKED (changed) ##########################
        # Whether string aliases are supported `SELECT COUNT(*) 'count'`
        # Exasol: supports this, tested with exasol db
        STRING_ALIASES = True  # default False

        # Whether query modifiers such as LIMIT are attached to the UNION node (vs its right operand)
        # Exasol: only possible to attach to righ subquery (operand)
        MODIFIERS_ATTACHED_TO_SET_OP = False  # default True
        SET_OP_MODIFIERS = {"order", "limit", "offset"}

        # Whether the 'AS' keyword is optional in the CTE definition syntax
        # https://docs.exasol.com/db/latest/sql/select.htm
        OPTIONAL_ALIAS_TOKEN_CTE = False

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

        # Whether or not interval spans are supported, INTERVAL 1 YEAR TO MONTHS
        # https://docs.exasol.com/db/latest/sql_references/literals.htm#IntervalLiterals
        INTERVAL_SPANS = True

        # Whether implicit unnesting is supported, e.g. SELECT 1 FROM y.z AS z, z.a (Redshift)
        SUPPORTS_IMPLICIT_UNNEST = False

        # Whether a PARTITION clause can follow a table reference
        SUPPORTS_PARTITION_SELECTION = False

        ############################ NOT SURE ##########################
        # Whether the SET command needs a delimiter (e.g. "=") for assignments
        # Exasol: https://docs.exasol.com/db/latest/search.htm?q=SET%20&f=aws
        SET_REQUIRES_ASSIGNMENT_DELIMITER = True

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

        # Whether the `name AS expr` schema/column constraint requires parentheses around `expr`
        WRAPPED_TRANSFORM_COLUMN_CONSTRAINT = True

    class Generator(generator.Generator):
        QUERY_HINTS = False
        STRUCT_DELIMITER = ("(", ")")
        NVL2_SUPPORTED = False
        TABLESAMPLE_REQUIRES_PARENS = False
        TABLESAMPLE_SIZE_IS_ROWS = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        SUPPORTS_TO_NUMBER = False
        JOIN_HINTS = False
        TABLE_HINTS = False
        GROUPINGS_SEP = ""
        SET_OP_MODIFIERS = False
        VALUES_AS_TABLE = False
        ARRAY_SIZE_NAME = "LENGTH"

        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "VARCHAR",
            exp.DataType.Type.CHAR: "CHAR",
            exp.DataType.Type.LONGBLOB: "VARCHAR",
            exp.DataType.Type.LONGTEXT: "VARCHAR",
            exp.DataType.Type.MEDIUMBLOB: "VARCHAR",
            exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
            exp.DataType.Type.TINYBLOB: "VARCHAR",
            exp.DataType.Type.TINYTEXT: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.VARBINARY: "VARCHAR",
            exp.DataType.Type.VARCHAR: "VARCHAR",
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathRoot,  # $
            exp.JSONPathKey,  # .key or ['key']
            exp.JSONPathSubscript,  # ['key'] or [0]
            exp.JSONPathWildcard,  # [*]
            exp.JSONPathUnion,  # ['key1','key2']
            exp.JSONPathSlice,  # [start:end:step]
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.MEDIUMINT: "INTEGER",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.DATETIME2: "TIMESTAMP",
            exp.DataType.Type.SMALLDATETIME: "TIMESTAMP",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            # no need to introduce exp.Ascii
            # see e.g.: https://github.com/tobymao/sqlglot/blob/56da9629899e72ab1e15cfc45ede838c4c38c16e/sqlglot/dialects/postgres.py line 376 and 624
            exp.Unicode: rename_func("ASCII"),
            # exp.Abs : rename_func("ABS"),
            exp.Anonymous: lambda self, e: self._anonymous_func(e),
            exp.Acos: lambda self, e: f"ACOS({self.sql(e, 'this')})",
            exp.DateAdd: lambda self,
            e: f"ADD_DAYS({self._timestamp_literal(e.this, 'this')}, {self.sql(e, 'expression')})",
            exp.AddHours: lambda self,
            e: f"ADD_HOURS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddMinutes: lambda self,
            e: f"ADD_MINUTES({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddMonths: lambda self,
            e: f"ADD_MONTHS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddSeconds: lambda self,
            e: f"ADD_SECONDS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddWeeks: lambda self,
            e: f"ADD_WEEKS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AddYears: lambda self,
            e: f"ADD_YEARS({self._timestamp_literal(e, 'this')}, {self.sql(e, 'expression')})",
            exp.AnyValue: lambda self, e: self.windowed_func("ANY", e),
            exp.ApproxDistinct: rename_func("APPROXIMATE_COUNT_DISTINCT"),
            # exp.Ascii: lambda self, e: f"ASCII({self.sql(e, 'this')})",
            exp.Asin: lambda self, e: f"ASIN({self.sql(e, 'this')})",
            exp.Atan: lambda self, e: f"ATAN({self.sql(e, 'this')})",
            exp.Atan2: lambda self,
            e: f"ATAN2({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.Avg: lambda self, e: self.windowed_func("AVG", e),
            exp.BitwiseAnd: rename_func("BIT_AND"),
            exp.BitCheck: lambda self,
            e: f"BIT_CHECK({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitLength: lambda self, e: f"BIT_LENGTH({self.sql(e, 'this')})",
            exp.BitLRotate: lambda self,
            e: f"BIT_LROTATE({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseLeftShift: rename_func("BIT_LSHIFT"),
            exp.BitwiseNot: rename_func("BIT_NOT"),
            exp.BitwiseOr: rename_func("BIT_OR"),
            exp.BitRRotate: lambda self,
            e: f"BIT_RROTATE({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseRightShift: rename_func("BIT_RSHIFT"),
            exp.BitSet: lambda self,
            e: f"BIT_SET({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitToNum: lambda self,
            e: f"BIT_TO_NUM({', '.join(self.sql(arg) for arg in e.expressions)})",
            exp.BitwiseXor: rename_func("BIT_XOR"),
            # Case
            # Cast
            # exp.Ceil
            # exp.Chr: rename_func("CHR"),
            ###################################
            # already handled in class Length (expressions.py)
            # exp.CharacterLength: rename_func("CHARACTER_LENGTH"),
            ##################################
            # exp.Coalesce,
            exp.ColognePhonetic: lambda self,
            e: f"COLOGNE_PHONETIC({self.sql(e, 'this')})",
            exp.ConnectByIsCycle: lambda self, e: "CONNECT_BY_ISCYCLE",
            exp.ConnectByIsLeaf: lambda self, e: "CONNECT_BY_ISLEAF",
            exp.SysConnectByPath: lambda self,
            e: f"SYS_CONNECT_BY_PATH({self.sql(e, 'this')}, {', '.join(self.sql(x) for x in e.expressions)})",
            exp.Command: lambda self, e: " ".join(self.sql(x) for x in e.expressions),
            # exp.Concat
            # exp.Convert
            exp.ConvertTZ: lambda self, e: self.convert_tz_sql(e),
            # exp.Corr: lambda self, e: self.corr_sql(e), - already exist
            exp.Cos: lambda self, e: self.cos_sql(e),
            exp.CosH: lambda self, e: self.cosh_sql(e),
            exp.Cot: lambda self, e: self.cot_sql(e),
            exp.Count: lambda self, e: self.windowed_func("COUNT", e),
            exp.CovarPop: rename_func("COVAR_POP"),
            exp.CovarSamp: rename_func("COVAR_SAMP"),
            exp.CumeDist: lambda self, e: self.windowed_func("CUME_DIST"),
            exp.CurrentCluster: lambda self, e: "CURRENT_CLUSTER",
            exp.CurrentDate: rename_func("CURDATE"),
            exp.CurrentSchema: lambda self, e: "CURRENT_SCHEMA",
            exp.CurrentSession: lambda self, e: "CURRENT_SESSION",
            exp.CurrentStatement: lambda self, e: "CURRENT_STATEMENT",
            exp.CurrentUser: lambda self, e: "CURRENT_USER",
            # exp.CountDistinct: lambda self, e: self.windowed_func("COUNT_DISTINCT", e),
            # exp.CountStar: lambda self, e: "COUNT(*)",
            # exp.Covar: rename_func("COVAR
            exp.CurrentTimestamp: lambda self, e: (
                f"CURRENT_TIMESTAMP({self.sql(e, 'this')})"
                if e.args.get("this")
                else "CURRENT_TIMESTAMP"
            ),
            exp.DateTrunc: lambda self,
            e: f"DATE_TRUNC({self.sql(e, 'this')}, {self._timestamp_literal(e, 'expression')})",
            # exp.Day
            exp.DaysBetween: lambda self,
            e: f"DAYS_BETWEEN({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.Date: lambda self, e: f"DATE {self.sql(e, 'this')}",
            exp.Timestamp: lambda self, e: f"TIMESTAMP {self.sql(e, 'this')}",
            exp.DBTimezone: lambda self, e: "DBTIMEZONE",
            exp.Decode: lambda self,
            e: f"DECODE({', '.join(self.sql(arg) for arg in e.expressions)})",
            exp.Degrees: lambda self, e: f"DEGREES({self.sql(e, 'this')})",
            exp.DenseRank: lambda self, e: self.windowed_func("DENSE_RANK", e),
            # exp.Div
            exp.Dump: lambda self, e: f"DUMP({
                ', '.join(
                    filter(
                        None,
                        [
                            self.sql(e, 'this'),
                            self.sql(e, 'format'),
                            self.sql(e, 'start_position'),
                            self.sql(e, 'length'),
                        ],
                    )
                )
            })",
            exp.EditDistance: lambda self,
            e: f"EDIT_DISTANCE({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.Every: lambda self, e: f"EVERY({self.sql(e, 'this')})",
            exp.Extract: lambda self,
            e: f"EXTRACT({self.sql(e, 'expression')} FROM {self._timestamp_literal(e, 'this')})",
            # exp.Extract
            # exp.FirstValue: rename_func("FIRST_VALUE"),
            # exp.Exp: lambda self, e: f"EXP({self.sql(e, 'this')})",
            exp.FirstValue: lambda self, e: self.windowed_func("FIRST_VALUE", e),
            # exp.Floor: lambda self, e: f"FLOOR({self.sql(e, 'this')})",
            exp.FromPosixTime: lambda self,
            e: f"FROM_POSIX_TIME({self.sql(e, 'this')})",
            # exp.Greatest: lambda self, e: f"GREATEST({', '.join(self.sql(arg) for arg in e.expressions)})", //Todo:
            exp.GroupConcat: lambda self, e: self.group_concat_sql(e),
            exp.Grouping: lambda self,
            e: f"GROUPING({', '.join(self.sql(x) for x in e.expressions)})",
            # exp.GroupingId: lambda self, e: f"GROUPING_ID({self.sql(e, 'this')})",
            exp.MD5: lambda self,
            e: f"HASH_MD5({', '.join(self.sql(x) for x in e.expressions)})",
            exp.SHA: lambda self,
            e: f"HASH_SHA1({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashSha256: lambda self,
            e: f"HASH_SHA256({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashSha512: lambda self,
            e: f"HASH_SHA512({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTiger: lambda self,
            e: f"HASH_TIGER({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeMd5: lambda self,
            e: f"HASHTYPE_MD5({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeSha1: lambda self,
            e: f"HASHTYPE_SHA1({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeSha256: lambda self,
            e: f"HASHTYPE_SHA256({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeSha512: lambda self,
            e: f"HASHTYPE_SHA512({', '.join(self.sql(x) for x in e.expressions)})",
            exp.HashTypeTiger: lambda self,
            e: f"HASHTYPE_TIGER({', '.join(self.sql(x) for x in e.expressions)})",
            exp.Hour: lambda self, e: f"HOUR({self._timestamp_literal(e, 'this')})",
            exp.HoursBetween: lambda self,
            e: f"HOURS_BETWEEN({self._timestamp_literal(e, 'this')}, {self._timestamp_literal(e, 'expression')})",
            exp.If: lambda self, e: (
                f"IF {self.sql(e, 'this')} THEN {self.sql(e, 'true')} ELSE {self.sql(e, 'false')} ENDIF"
            ),
            exp.InitCap: lambda self, e: f"INITCAP({self.sql(e, 'this')})",
            # exp.Insert: lambda self,
            # e: f"INSERT({self.sql(e, 'this')}, {self.sql(e, 'start')}, {self.sql(e, 'length')}, {self.sql(e, 'expression')})",
            exp.Stuff: rename_func("INSERT"),
            # exp.Instr: lambda self, e: (
            #     "INSTR("
            #     + ", ".join(
            #         filter(
            #             None,
            #             [
            #                 self.sql(e, "this"),
            #                 self.sql(e, "substr"),
            #                 e.args.get("position") and self.sql(e, "position"),
            #                 e.args.get("occurrence") and self.sql(e, "occurrence"),
            #             ],
            #         )
            #     )
            #     + ")"
            # ),
            exp.Iproc: lambda self, e: f"IPROC({self.sql(e, 'this')})",
            exp.Is: lambda self, e: f"IS_{self.sql(e, 'type')}({self.sql(e, 'this')}"
            + (f", {self.sql(e, 'expression')}" if e.args.get("expression") else "")
            + ")",
            exp.JSONBExtract: lambda self, e: self.jsonb_extract_sql(e),
            exp.JSONValue: lambda self, e: (
                f"JSON_VALUE({self.sql(e, 'this')}, {self.sql(e, 'path')}"
                + (
                    f" NULL ON EMPTY DEFAULT {self.sql(e, 'default')} ON ERROR"
                    if e.args.get("null_on_empty")
                    and e.args.get("default")
                    and e.args.get("on_error")
                    else ""
                )
                + ")"
            ),
            exp.Lag: lambda self, e: (
                f"LAG({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                if e.args.get("expression") is not None
                else f"LAG({self.sql(e, 'this')})"
            ),
            # exp.LastValue
            # exp.LCase: rename_func("LCASE"),
            # LOWER and LCASE are aliases in exasol
            # sqlglot parses LCASE as exp.Lower (see: sqlglot/generator.py line 6439)
            exp.Lead: lambda self, e: (
                f"LEAD({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                if e.args.get("expression") is not None
                else f"LEAD({self.sql(e, 'this')})"
            ),
            # exp.Least
            # exp.Left: lambda self,
            # e: f"LEFT({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.Length
            exp.ConnectBy: lambda self,
            e: f"CONNECT BY {'PRIOR ' if e.args.get('prior') else ''}{self.sql(e, 'this')}",
            exp.StartWith: lambda self, e: f"START WITH {self.sql(e, 'this')}",
            exp.Level: lambda self, e: "LEVEL",
            exp.Log: lambda self,
            e: f"LOG({self.sql(e, 'this')}, {self.sql(e, 'base')})",
            exp.Log2: lambda self, e: f"LOG2({self.sql(e, 'this')})",
            exp.Log10: lambda self, e: f"LOG10({self.sql(e, 'this')})",
            exp.ListAgg: lambda self, e: (
                f"LISTAGG({self.sql(e, 'this')}"
                f"{', ' + self.sql(e, 'separator') if e.args.get('separator') else ''})"
                f" WITHIN GROUP (ORDER BY {self.sql(e, 'order')})"
                if e.args.get("order")
                else ""
            ),
            # exp.Ln: lambda self, e: f"LN({self.sql(e, 'this')})",
            exp.LocalTimestamp: lambda self, e: (
                f"LOCALTIMESTAMP({self.sql(e, 'precision')})"
                if e.args.get("precision")
                else "LOCALTIMESTAMP"
            ),
            # exp.Locate: lambda self, e: (
            #     f"LOCATE({self.sql(e, 'this')}, {self.sql(e, 'expression')}, {self.sql(e, 'start')})"
            #     if e.args.get("start")
            #     else f"LOCATE({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
            # ),
            # exp.Lower
            # exp.LPad: lambda self,
            # e: f"LPAD({self.sql(e, 'this')}, {self.sql(e, 'length')}, {self.sql(e, 'pad')})",
            # exp.LTrim: lambda self, e: (
            #     f"LTRIM({self.sql(e, 'this')}, {self.sql(e, 'trim_chars')})"
            #     if e.args.get("trim_chars")
            #     else f"LTRIM({self.sql(e, 'this')})"
            # ),
            # exp.Trim
            # exp.Max
            # exp.Median
            # exp.Min
            # exp.Mid: lambda self, e: (
            #     f"MID({self.sql(e, 'this')}, {self.sql(e, 'start')}, {self.sql(e, 'length')})"
            #     if e.args.get("length")
            #     else f"MID({self.sql(e, 'this')}, {self.sql(e, 'start')})"
            # ),
            exp.Mod: rename_func("MOD"),
            exp.MinScale: lambda self, e: f"MIN_SCALE({self.sql(e, 'this')})",
            # exp.MinutesBetween: lambda self, e: f"MINUTES_BETWEEN({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.Month: lambda self, e: f"MONTH({self.sql(e, 'this')})",
            # exp.MonthsBetween: lambda self, e: f"MONTHS_BETWEEN({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.Mul: lambda self, e: f"MUL({self.sql(e, 'this')})",
            # exp.Now: lambda self, e: "NOW()",
            # exp.NProc: lambda self, e: "NPROC()",
            # exp.NTile: lambda self, e: f"NTILE({self.sql(e, 'this')})",
            # exp.NullIf: lambda self, e: f"NULLIF({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.NullIfZero: lambda self, e: f"NULLIFZERO({self.sql(e, 'this')})",
            # exp.NumToDSInterval: lambda self, e: f"NUMTODSINTERVAL({self.sql(e, 'this')}, {self.sql(e, 'unit')})",
            # exp.NumToYMInterval: lambda self, e: f"NUMTOYMINTERVAL({self.sql(e, 'this')}, {self.sql(e, 'unit')})",
            # exp.NVL: lambda self, e: f"NVL({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            # exp.NVL2: lambda self, e: f"NVL2({self.sql(e, 'this')}, {self.sql(e, 'true_value')}, {self.sql(e, 'false_value')})",
            # exp.OctetLength: lambda self, e: f"OCTET_LENGTH({self.sql(e, 'this')})",
            # exp.PercentRank: lambda self, e: "PERCENT_RANK()",
            # exp.Pi: lambda self, e: "PI()",
            # exp.PosixTime: lambda self, e: f"POSIX_TIME({self.sql(e, 'this')})",
            # exp.Month: lambda self, e: f"MONTH({self.sql(e, 'this')})",
            # exp.MonthsBetween: lambda self, e
            # exp.Nullif
            exp.NthValue: lambda self, e: (
                f"NTH_VALUE({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                + (" FROM LAST" if e.args.get("from_last") else " FROM FIRST")
                + (" RESPECT NULLS" if e.args.get("respect_nulls") else " IGNORE NULLS")
                + self.sql(e, "over")
            ),
            exp.PercentileCont: lambda self, e: (
                f"PERCENTILE_CONT({self.sql(e, 'this')})"
                f" WITHIN GROUP ({self.sql(e, 'order').strip()})"
                f"{self.sql(e, 'over')}"
            ),
            exp.PercentileDisc: lambda self, e: (
                f"PERCENTILE_DISC({self.sql(e, 'this')})"
                f" WITHIN GROUP ({self.sql(e, 'order').strip()})"
                f"{self.sql(e, 'over')}"
            ),
            exp.Pow: rename_func("POWER"),
            exp.Rand: rename_func("RANDOM"),
            exp.RegexpExtract: rename_func("REGEXP_SUBSTR"),
            # exp.RegexpReplace: rename_func("REGEXP_REPLACE"),
            # exp.Repeat
            # exp.Right
            exp.Round: lambda self, e: self.round_sql(e),
            # exp.Round: lambda self, e: f"ADD_DAYS({self._timestamp_literal(e.this, 'this')}, {self.sql(e, 'expression')})",
            exp.StrPosition: _string_position_sql,
            exp.Substring: rename_func("SUBSTR"),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.OnCluster: exp.Properties.Location.POST_NAME,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.ToTableProperty: exp.Properties.Location.POST_NAME,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def _anonymous_func(self, e):
            func_name = e.this.upper()
            handlers = {
                "MIN_SCALE": lambda e: self._render_func_with_args(
                    e, expected_arg_count=1
                ),
                "MONTH": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "MINUTES_BETWEEN": lambda e: self._render_func_with_args(
                    e, expected_arg_count=2
                ),
                "MONTHS_BETWEEN": lambda e: self._render_func_with_args(
                    e, expected_arg_count=2
                ),
                "MUL": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "NOW": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "NPROC": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "NTILE": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "NULLIF": lambda e: self._render_func_with_args(
                    e, expected_arg_count=2
                ),
                "NULLIFZERO": lambda e: self._render_func_with_args(
                    e, expected_arg_count=1
                ),
                "NUMTODSINTERVAL": lambda e: self._render_func_with_args(
                    e, expected_arg_count=2
                ),
                "NUMTOYMINTERVAL": lambda e: self._render_func_with_args(
                    e, expected_arg_count=2
                ),
                "NVL": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "NVL2": lambda e: self._render_func_with_args(e, expected_arg_count=3),
                "OCTET_LENGTH": lambda e: self._render_func_with_args(
                    e, expected_arg_count=1
                ),
                "PERCENT_RANK": lambda e: self._render_func_with_args(
                    e, expected_arg_count=0
                ),
                "PI": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "POSIX_TIME": lambda e: self._render_func_with_args(
                    e, expected_arg_count=1
                ),
                "RADIANS": lambda e: self._render_func_with_args(
                    e, expected_arg_count=1
                ),
                "RANK": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "RATIO_TO_REPORT": lambda e: self._render_func_with_args(
                    e, expected_arg_count=1
                ),
                "REGEXP_INSTR": lambda e: self._render_func_with_args(
                    e, min_arg_count=2, max_arg_count=5
                ),
                "REPLACE": lambda e: self._render_func_with_args(
                    e, min_arg_count=2, max_arg_count=3
                ),
                "REVERSE": lambda e: self._render_func_with_args(
                    e, expected_arg_count=1
                ),
                "ROWNUM": lambda e: "ROWNUM",
                "ROW_NUMBER": lambda e: self._render_func_with_args(
                    e, expected_arg_count=0
                ),
                "ROWID": lambda e: "ROWID",
                # "RPAD": lambda e: self._render_func_with_args(e, expected_arg_count=3),
                # "RTRIM": lambda e: self._render_func_with_args(e, expected_arg_count=2),
            }

            regr_funcs = [
                "REGR_AVGX",
                "REGR_AVGY",
                "REGR_COUNT",
                "REGR_INTERCEPT",
                "REGR_R2",
                "REGR_SLOPE",
                "REGR_SXX",
                "REGR_SXY",
                "REGR_SYY",
            ]
            for name in regr_funcs:
                handlers[name] = lambda e: self._render_func_with_args(
                    e, expected_arg_count=2
                )

            if func_name in handlers:
                return handlers[func_name](e)
            return f"{func_name}({', '.join(self.sql(arg) for arg in e.expressions)})"

        def _render_func_with_args(
            self, e, expected_arg_count=None, min_arg_count=None, max_arg_count=None
        ):
            arg_count = len(e.expressions)

            if expected_arg_count is not None and arg_count != expected_arg_count:
                raise ValueError(
                    f"{e.this} expects exactly {expected_arg_count} arguments, got {arg_count}"
                )

            if min_arg_count is not None and arg_count < min_arg_count:
                raise ValueError(
                    f"{e.this} expects at least {min_arg_count} arguments, got {arg_count}"
                )

            if max_arg_count is not None and arg_count > max_arg_count:
                raise ValueError(
                    f"{e.this} expects at most {max_arg_count} arguments, got {arg_count}"
                )

            def format_arg(arg):
                if (
                    isinstance(arg, exp.Literal)
                    and isinstance(arg.this, str)
                    and arg.this.startswith("TIMESTAMP ")
                ):
                    return arg.this
                return self.sql(arg)

            formatted_args = ", ".join(format_arg(arg) for arg in e.expressions)
            return f"{e.this.upper()}({formatted_args})"

        def round_sql(self, expression):
            value = expression.this
            arg = expression.args.get("decimals")

            datetime_units = {
                "CC",
                "SCC",
                "YYYY",
                "SYYY",
                "YEAR",
                "SYEAR",
                "YYY",
                "YY",
                "Y",
                "IYYY",
                "IYY",
                "IY",
                "I",
                "Q",
                "MONTH",
                "MON",
                "MM",
                "RM",
                "WW",
                "IW",
                "W",
                "DDD",
                "DD",
                "J",
                "DAY",
                "DY",
                "D",
                "HH",
                "HH24",
                "HH12",
                "MI",
                "SS",
            }

            def is_datetime_literal(lit):
                return (
                    isinstance(lit, exp.Literal)
                    and lit.is_string
                    and (
                        lit.this.startswith("DATE ")
                        or lit.this.startswith("TIMESTAMP ")
                    )
                )

            # Render the first argument properly
            value_sql = value.this if is_datetime_literal(value) else self.sql(value)

            if arg is None:
                return f"ROUND({value_sql})"

            # If rounding a datetime, the second arg should be a valid datetime unit
            if is_datetime_literal(value):
                unit = (
                    arg.this.strip("'").upper()
                    if isinstance(arg, exp.Literal)
                    else self.sql(arg)
                )
                if unit not in datetime_units:
                    raise ValueError(f"Invalid ROUND datetime unit for Exasol: {unit}")
                return f"ROUND({value_sql}, '{unit}')"

            # Else, treat second argument as numeric (e.g., ROUND(42.123, 2))
            return f"ROUND({value_sql}, {self.sql(arg)})"

        def _timestamp_literal(self, e, key):
            arg = e.args.get(key)
            if isinstance(arg, exp.Literal) and arg.args.get("prefix"):
                # print(arg.args.get("prefix"))
                # print("The fame I waited for")
                return f"{arg.args['prefix']} '{arg.this}'"
            return self.sql(e, key)

        def alias_sql(self, expression):
            return f"{self.sql(expression, 'this')} {self.sql(expression, 'alias')}"

        def windowed_func(self, name, e):
            sql = f"{name}({self.sql(e, 'this')})"
            if e.args.get("over"):
                sql += f"{self.sql(e, 'over')}"
            return sql

        def convert_tz_sql(self, expression):
            this = self.sql(expression, "this")
            from_tz = self.sql(expression, "from_tz")
            to_tz = self.sql(expression, "to_tz")
            options = self.sql(expression, "options")
            if options:
                return f"CONVERT_TZ({this}, {from_tz}, {to_tz}, {options})"
            return f"CONVERT_TZ({this}, {from_tz}, {to_tz})"

        def convert_tz(self, e):
            args = [
                self.sql(e, "this"),
                self.sql(e, "timezone"),
                self.sql(e, "to_timezone"),
            ]
            if e.args.get("options"):
                args.append(self.sql(e, "options"))
            return f"CONVERT_TZ({', '.join(args)})"

        def cos_sql(self, expression):
            return f"COS({self.sql(expression, 'this')})"

        def cosh_sql(self, expression):
            return f"COSH({self.sql(expression, 'this')})"

        def cot_sql(self, expression):
            return f"COT({self.sql(expression, 'this')})"

        # def connect_by_sql(self, expression):
        #     prior = expression.args.get("prior")
        #     prefix = "CONNECT BY PRIOR " if prior else "CONNECT BY "
        #     return prefix + self.sql(expression, "this")

        # def start_with_sql(self, expression):
        #     return "START WITH " + self.sql(expression, "this")

        # def corr_sql(self, expression):
        #     return f"CORR({self.sql(expression, 'this')}, {self.sql(expression, 'expression')})"

        def group_concat_sql(self, e):
            order = e.args.get("order")
            order_sql = ""
            if order:
                # order is a list of Ordered expressions
                order_sql = " ORDER BY " + ", ".join(self.sql(o) for o in order)

            return (
                "GROUP_CONCAT("
                + ("DISTINCT " if e.args.get("distinct") else "")
                + self.sql(e, "this")
                + order_sql
                + (
                    f" SEPARATOR {self.sql(e, 'separator')}"
                    if e.args.get("separator")
                    else ""
                )
                + ")"
                + self.sql(e, "over")
            )

        def jsonb_extract_sql(self, e):
            args_sql = ", ".join(self.sql(arg) for arg in e.expressions)
            emits = e.args.get("emits")
            emits_sql = ""

            if emits:
                emits_sql = (
                    " EMITS("
                    + ", ".join(f"{name} {datatype}" for name, datatype in emits)
                    + ")"
                )

            return f"JSON_EXTRACT({self.sql(e, 'this')}, {args_sql}){emits_sql}"

        def convert_sql(self, expression):
            return f"CONVERT({self.sql(expression, 'this')}, {self.sql(expression, 'expression')})"

        def strtodate_sql(self, expression: exp.StrToDate) -> str:
            strtodate_sql = self.function_fallback_sql(expression)

            if not isinstance(expression.parent, exp.Cast):
                # StrToDate returns DATEs in other dialects (eg. postgres), so
                # this branch aims to improve the transpilation to clickhouse
                return self.cast_sql(exp.cast(expression, "DATE"))

            return strtodate_sql

        def cast_sql(
            self, expression: exp.Cast, safe_prefix: t.Optional[str] = None
        ) -> str:
            this = expression.this

            if isinstance(this, exp.StrToDate) and expression.to == exp.DataType.build(
                "datetime"
            ):
                return self.sql(this)

            return super().cast_sql(expression, safe_prefix=safe_prefix)

        # Important method
        def trycast_sql(self, expression: exp.TryCast) -> str:
            dtype = expression.to
            if not dtype.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True):
                # Casting x into Nullable(T) appears to behave similarly to TRY_CAST(x AS T)
                dtype.set("nullable", True)

            return super().cast_sql(expression)

        def _jsonpathsubscript_sql(self, expression: exp.JSONPathSubscript) -> str:
            this = self.json_path_part(expression.this)
            return str(int(this) + 1) if is_int(this) else this

        def likeproperty_sql(self, expression: exp.LikeProperty) -> str:
            return f"AS {self.sql(expression, 'this')}"

        def _any_to_has(
            self,
            expression: exp.EQ | exp.NEQ,
            default: t.Callable[[t.Any], str],
            prefix: str = "",
        ) -> str:
            if isinstance(expression.left, exp.Any):
                arr = expression.left
                this = expression.right
            elif isinstance(expression.right, exp.Any):
                arr = expression.right
                this = expression.left
            else:
                return default(expression)

            return prefix + self.func("has", arr.this.unnest(), this)

        def eq_sql(self, expression: exp.EQ) -> str:
            return self._any_to_has(expression, super().eq_sql)

        def neq_sql(self, expression: exp.NEQ) -> str:
            return self._any_to_has(expression, super().neq_sql, "NOT ")

        def regexpilike_sql(self, expression: exp.RegexpILike) -> str:
            # Manually add a flag to make the search case-insensitive
            regex = self.func("CONCAT", "'(?i)'", expression.expression)
            return self.func("match", expression.this, regex)

        def datatype_sql(self, expression: exp.DataType) -> str:
            # String is the standard ClickHouse type, every other variant is just an alias.
            # Additionally, any supplied length parameter will be ignored.
            #
            # https://clickhouse.com/docs/en/sql-reference/data-types/string
            if expression.this in self.STRING_TYPE_MAPPING:
                # print(self.STRING_TYPE_MAPPING[expression.this])
                dtype = self.STRING_TYPE_MAPPING[expression.this]
            else:
                dtype = super().datatype_sql(expression)

            # This section changes the type to `Nullable(...)` if the following conditions hold:
            # - It's marked as nullable - this ensures we won't wrap ClickHouse types with `Nullable`
            #   and change their semantics
            # - It's not the key type of a `Map`. This is because ClickHouse enforces the following
            #   constraint: "Type of Map key must be a type, that can be represented by integer or
            #   String or FixedString (possibly LowCardinality) or UUID or IPv6"
            # - It's not a composite type, e.g. `Nullable(Array(...))` is not a valid type
            # parent = expression.parent

            # nullable = expression.args.get("nullable")
            # if nullable is True or (
            #     nullable is None
            #     and not (
            #         isinstance(parent, exp.DataType)
            #         and parent.is_type(exp.DataType.Type.MAP, check_nullable=True)
            #         and expression.index in (None, 0)
            #     )
            #     and not expression.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True)
            # ):
            #     dtype = f"Nullable({dtype})"

            return dtype

        def cte_sql(self, expression: exp.CTE) -> str:
            if expression.args.get("scalar"):
                this = self.sql(expression, "this")
                alias = self.sql(expression, "alias")
                return f"{this} AS {alias}"

            return super().cte_sql(expression)

        def after_limit_modifiers(self, expression: exp.Expression) -> t.List[str]:
            return super().after_limit_modifiers(expression) + [
                (
                    self.seg("SETTINGS ")
                    + self.expressions(expression, key="settings", flat=True)
                    if expression.args.get("settings")
                    else ""
                ),
                (
                    self.seg("FORMAT ") + self.sql(expression, "format")
                    if expression.args.get("format")
                    else ""
                ),
            ]

        def placeholder_sql(self, expression: exp.Placeholder) -> str:
            return f"{{{expression.name}: {self.sql(expression, 'kind')}}}"

        def oncluster_sql(self, expression: exp.OnCluster) -> str:
            return f"ON CLUSTER {self.sql(expression, 'this')}"

        def createable_sql(
            self, expression: exp.Create, locations: t.DefaultDict
        ) -> str:
            if expression.kind in self.ON_CLUSTER_TARGETS and locations.get(
                exp.Properties.Location.POST_NAME
            ):
                this_name = self.sql(
                    (
                        expression.this
                        if isinstance(expression.this, exp.Schema)
                        else expression
                    ),
                    "this",
                )
                this_properties = " ".join(
                    [
                        self.sql(prop)
                        for prop in locations[exp.Properties.Location.POST_NAME]
                    ]
                )
                this_schema = self.schema_columns_sql(expression.this)
                this_schema = f"{self.sep()}{this_schema}" if this_schema else ""

                return f"{this_name}{self.sep()}{this_properties}{this_schema}"

            return super().createable_sql(expression, locations)

        def create_sql(self, expression: exp.Create) -> str:
            # The comment property comes last in CTAS statements, i.e. after the query
            query = expression.expression
            if isinstance(query, exp.Query):
                comment_prop = expression.find(exp.SchemaCommentProperty)
                if comment_prop:
                    comment_prop.pop()
                    query.replace(exp.paren(query))
            else:
                comment_prop = None

            create_sql = super().create_sql(expression)

            comment_sql = self.sql(comment_prop)
            comment_sql = f" {comment_sql}" if comment_sql else ""

            return f"{create_sql}{comment_sql}"

        def prewhere_sql(self, expression: exp.PreWhere) -> str:
            this = self.indent(self.sql(expression, "this"))
            return f"{self.seg('PREWHERE')}{self.sep()}{this}"

        def indexcolumnconstraint_sql(
            self, expression: exp.IndexColumnConstraint
        ) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            expr = self.sql(expression, "expression")
            expr = f" {expr}" if expr else ""
            index_type = self.sql(expression, "index_type")
            index_type = f" TYPE {index_type}" if index_type else ""
            granularity = self.sql(expression, "granularity")
            granularity = f" GRANULARITY {granularity}" if granularity else ""

            return f"INDEX{this}{expr}{index_type}{granularity}"

        def partition_sql(self, expression: exp.Partition) -> str:
            return f"PARTITION {self.expressions(expression, flat=True)}"

        def partitionid_sql(self, expression: exp.PartitionId) -> str:
            return f"ID {self.sql(expression.this)}"

        def replacepartition_sql(self, expression: exp.ReplacePartition) -> str:
            return f"REPLACE {self.sql(expression.expression)} FROM {self.sql(expression, 'source')}"

        def projectiondef_sql(self, expression: exp.ProjectionDef) -> str:
            return f"PROJECTION {self.sql(expression.this)} {self.wrap(expression.expression)}"

        def is_sql(self, expression: exp.Is) -> str:
            is_sql = super().is_sql(expression)

            if isinstance(expression.parent, exp.Not):
                # value IS NOT NULL -> NOT (value IS NULL)
                is_sql = self.wrap(is_sql)

            if isinstance(expression.parent, exp.Not) and expression.args.get(
                "is_global"
            ):
                in_sql = in_sql.replace("GLOBAL IN", "GLOBAL NOT IN", 1)

        def not_sql(self, expression: exp.Not) -> str:
            if isinstance(expression.this, exp.In) and expression.this.args.get(
                "is_global"
            ):
                # let `GLOBAL IN` child interpose `NOT`
                return self.sql(expression, "this")
