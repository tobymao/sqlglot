from __future__ import annotations

import re
import typing as t
from copy import deepcopy
from functools import partial
from collections import defaultdict

from sqlglot import exp, generator, jsonpath, tokens, transforms
from sqlglot.dialects.dialect import (
    DATE_ADD_OR_SUB,
    Dialect,
    NormalizationStrategy,
    approx_count_distinct_sql,
    arg_max_or_min_no_count,
    datestrtodate_sql,
    if_sql,
    is_parse_json,
    left_to_substring_sql,
    max_or_greatest,
    min_or_least,
    no_ilike_sql,
    no_recursive_cte_sql,
    no_trycast_sql,
    regexp_extract_sql,
    regexp_replace_sql,
    rename_func,
    right_to_substring_sql,
    strposition_sql,
    struct_extract_sql,
    time_format,
    timestrtotime_sql,
    trim_sql,
    unit_to_str,
    var_map_sql,
    sequence_sql,
    property_sql,
)
from sqlglot.transforms import (
    remove_unique_constraints,
    ctas_with_tmp_tables_to_create_tmp_view,
    preprocess,
    move_schema_columns_to_partitioned_by,
)
from sqlglot.parsers.hive import HiveParser
from sqlglot.tokens import TokenType
from sqlglot.generator import unsupported_args
from sqlglot.optimizer.annotate_types import TypeAnnotator
from sqlglot.typing.hive import EXPRESSION_METADATA

# (FuncType, Multiplier)
DATE_DELTA_INTERVAL = {
    "YEAR": ("ADD_MONTHS", 12),
    "MONTH": ("ADD_MONTHS", 1),
    "QUARTER": ("ADD_MONTHS", 3),
    "WEEK": ("DATE_ADD", 7),
    "DAY": ("DATE_ADD", 1),
}

TIME_DIFF_FACTOR = {
    "MILLISECOND": " * 1000",
    "SECOND": "",
    "MINUTE": " / 60",
    "HOUR": " / 3600",
}

DIFF_MONTH_SWITCH = ("YEAR", "QUARTER", "MONTH")


def _add_date_sql(self: Hive.Generator, expression: DATE_ADD_OR_SUB) -> str:
    if isinstance(expression, exp.TsOrDsAdd) and not expression.unit:
        return self.func("DATE_ADD", expression.this, expression.expression)

    unit = expression.text("unit").upper()
    func, multiplier = DATE_DELTA_INTERVAL.get(unit, ("DATE_ADD", 1))

    if isinstance(expression, exp.DateSub):
        multiplier *= -1

    increment = expression.expression
    if isinstance(increment, exp.Literal):
        value = increment.to_py() if increment.is_number else int(increment.name)
        increment = exp.Literal.number(value * multiplier)
    elif multiplier != 1:
        increment *= exp.Literal.number(multiplier)

    return self.func(func, expression.this, increment)


def _date_diff_sql(self: Hive.Generator, expression: exp.DateDiff | exp.TsOrDsDiff) -> str:
    unit = expression.text("unit").upper()

    factor = TIME_DIFF_FACTOR.get(unit)
    if factor is not None:
        left = self.sql(expression, "this")
        right = self.sql(expression, "expression")
        sec_diff = f"UNIX_TIMESTAMP({left}) - UNIX_TIMESTAMP({right})"
        return f"({sec_diff}){factor}" if factor else sec_diff

    months_between = unit in DIFF_MONTH_SWITCH
    sql_func = "MONTHS_BETWEEN" if months_between else "DATEDIFF"
    _, multiplier = DATE_DELTA_INTERVAL.get(unit, ("", 1))
    multiplier_sql = f" / {multiplier}" if multiplier > 1 else ""
    diff_sql = f"{sql_func}({self.format_args(expression.this, expression.expression)})"

    if months_between or multiplier_sql:
        # MONTHS_BETWEEN returns a float, so we need to truncate the fractional part.
        # For the same reason, we want to truncate if there's a divisor present.
        diff_sql = f"CAST({diff_sql}{multiplier_sql} AS INT)"

    return diff_sql


def _json_format_sql(self: Hive.Generator, expression: exp.JSONFormat) -> str:
    this = expression.this

    if is_parse_json(this):
        if this.this.is_string:
            # Since FROM_JSON requires a nested type, we always wrap the json string with
            # an array to ensure that "naked" strings like "'a'" will be handled correctly
            wrapped_json = exp.Literal.string(f"[{this.this.name}]")

            from_json = self.func(
                "FROM_JSON", wrapped_json, self.func("SCHEMA_OF_JSON", wrapped_json)
            )
            to_json = self.func("TO_JSON", from_json)

            # This strips the [, ] delimiters of the dummy array printed by TO_JSON
            return self.func("REGEXP_EXTRACT", to_json, "'^.(.*).$'", "1")
        return self.sql(this)

    return self.func("TO_JSON", this, expression.args.get("options"))


@generator.unsupported_args(("expression", "Hive's SORT_ARRAY does not support a comparator."))
def _array_sort_sql(self: Hive.Generator, expression: exp.ArraySort) -> str:
    return self.func("SORT_ARRAY", expression.this)


def _str_to_unix_sql(self: Hive.Generator, expression: exp.StrToUnix) -> str:
    return self.func("UNIX_TIMESTAMP", expression.this, time_format("hive")(self, expression))


def _unix_to_time_sql(self: Hive.Generator, expression: exp.UnixToTime) -> str:
    timestamp = self.sql(expression, "this")
    scale = expression.args.get("scale")
    if scale in (None, exp.UnixToTime.SECONDS):
        return rename_func("FROM_UNIXTIME")(self, expression)

    return f"FROM_UNIXTIME({timestamp} / POW(10, {scale}))"


def _str_to_date_sql(self: Hive.Generator, expression: exp.StrToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format not in (Hive.TIME_FORMAT, Hive.DATE_FORMAT):
        this = f"FROM_UNIXTIME(UNIX_TIMESTAMP({this}, {time_format}))"
    return f"CAST({this} AS DATE)"


def _str_to_time_sql(self: Hive.Generator, expression: exp.StrToTime) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format not in (Hive.TIME_FORMAT, Hive.DATE_FORMAT):
        this = f"FROM_UNIXTIME(UNIX_TIMESTAMP({this}, {time_format}))"
    return f"CAST({this} AS TIMESTAMP)"


def _to_date_sql(self: Hive.Generator, expression: exp.TsOrDsToDate) -> str:
    time_format = self.format_time(expression)
    if time_format and time_format not in (Hive.TIME_FORMAT, Hive.DATE_FORMAT):
        return self.func("TO_DATE", expression.this, time_format)

    if isinstance(expression.parent, self.TS_OR_DS_EXPRESSIONS):
        return self.sql(expression, "this")

    return self.func("TO_DATE", expression.this)


class Hive(Dialect):
    ALIAS_POST_TABLESAMPLE = True
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    ARRAY_AGG_INCLUDES_NULLS = None
    REGEXP_EXTRACT_DEFAULT_GROUP = 1
    ALTER_TABLE_SUPPORTS_CASCADE = True

    # https://spark.apache.org/docs/latest/sql-ref-identifier.html#description
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362046#LanguageManualUDF-StringFunctions
    # https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/exec/Utilities.java#L266-L269
    INITCAP_DEFAULT_DELIMITER_CHARS = " \t\n\r\f\u000b\u001c\u001d\u001e\u001f"

    # Support only the non-ANSI mode (default for Hive, Spark2, Spark)
    COERCES_TO = defaultdict(set, deepcopy(TypeAnnotator.COERCES_TO))
    for target_type in {
        *exp.DataType.NUMERIC_TYPES,
        *exp.DataType.TEMPORAL_TYPES,
        exp.DType.INTERVAL,
    }:
        COERCES_TO[target_type] |= exp.DataType.TEXT_TYPES

    TIME_MAPPING = {
        "y": "%Y",
        "Y": "%Y",
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "M": "%-m",
        "dd": "%d",
        "d": "%-d",
        "HH": "%H",
        "H": "%-H",
        "hh": "%I",
        "h": "%-I",
        "mm": "%M",
        "m": "%-M",
        "ss": "%S",
        "s": "%-S",
        "SSSSSS": "%f",
        "a": "%p",
        "DD": "%j",
        "D": "%-j",
        "E": "%a",
        "EE": "%a",
        "EEE": "%a",
        "EEEE": "%A",
        "z": "%Z",
        "Z": "%z",
    }

    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    class JSONPathTokenizer(jsonpath.JSONPathTokenizer):
        VAR_TOKENS = {
            *jsonpath.JSONPathTokenizer.VAR_TOKENS,
            TokenType.DASH,
        }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ADD ARCHIVE": TokenType.COMMAND,
            "ADD ARCHIVES": TokenType.COMMAND,
            "ADD FILE": TokenType.COMMAND,
            "ADD FILES": TokenType.COMMAND,
            "ADD JAR": TokenType.COMMAND,
            "ADD JARS": TokenType.COMMAND,
            "MINUS": TokenType.EXCEPT,
            "MSCK REPAIR": TokenType.COMMAND,
            "REFRESH": TokenType.REFRESH,
            "TIMESTAMP AS OF": TokenType.TIMESTAMP_SNAPSHOT,
            "VERSION AS OF": TokenType.VERSION_SNAPSHOT,
            "SERDEPROPERTIES": TokenType.SERDE_PROPERTIES,
        }

        NUMERIC_LITERALS = {
            "L": "BIGINT",
            "S": "SMALLINT",
            "Y": "TINYINT",
            "D": "DOUBLE",
            "F": "FLOAT",
            "BD": "DECIMAL",
        }

    Parser = HiveParser

    class Generator(generator.Generator):
        LIMIT_FETCH = "LIMIT"
        TABLESAMPLE_WITH_METHOD = False
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        INDEX_ON = "ON TABLE"
        EXTRACT_ALLOWS_QUOTES = False
        NVL2_SUPPORTED = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        JSON_PATH_SINGLE_QUOTE_ESCAPE = True
        SAFE_JSON_PATH_KEY_RE = re.compile(r"^[_\-a-zA-Z][\-\w]*$")
        SUPPORTS_TO_NUMBER = False
        WITH_PROPERTIES_PREFIX = "TBLPROPERTIES"
        PARSE_JSON_NAME: t.Optional[str] = None
        PAD_FILL_PATTERN_IS_REQUIRED = True
        SUPPORTS_MEDIAN = False
        ARRAY_SIZE_NAME = "SIZE"
        ALTER_SET_TYPE = ""

        EXPRESSIONS_WITHOUT_NESTED_CTES = {
            exp.Insert,
            exp.Select,
            exp.Subquery,
            exp.SetOperation,
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
            exp.JSONPathWildcard,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DType.BIT: "BOOLEAN",
            exp.DType.BLOB: "BINARY",
            exp.DType.DATETIME: "TIMESTAMP",
            exp.DType.ROWVERSION: "BINARY",
            exp.DType.TEXT: "STRING",
            exp.DType.TIME: "TIMESTAMP",
            exp.DType.TIMESTAMPNTZ: "TIMESTAMP",
            exp.DType.TIMESTAMPTZ: "TIMESTAMP",
            exp.DType.UTINYINT: "SMALLINT",
            exp.DType.VARBINARY: "BINARY",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Property: property_sql,
            exp.AnyValue: rename_func("FIRST"),
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ArgMax: arg_max_or_min_no_count("MAX_BY"),
            exp.ArgMin: arg_max_or_min_no_count("MIN_BY"),
            exp.Array: transforms.preprocess([transforms.inherit_struct_field_names]),
            exp.ArrayConcat: rename_func("CONCAT"),
            exp.ArrayToString: lambda self, e: self.func("CONCAT_WS", e.expression, e.this),
            exp.ArraySort: _array_sort_sql,
            exp.With: no_recursive_cte_sql,
            exp.DateAdd: _add_date_sql,
            exp.DateDiff: _date_diff_sql,
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateSub: _add_date_sql,
            exp.DateToDi: lambda self, e: (
                f"CAST(DATE_FORMAT({self.sql(e, 'this')}, {Hive.DATEINT_FORMAT}) AS INT)"
            ),
            exp.DiToDate: lambda self, e: (
                f"TO_DATE(CAST({self.sql(e, 'this')} AS STRING), {Hive.DATEINT_FORMAT})"
            ),
            exp.StorageHandlerProperty: lambda self, e: f"STORED BY {self.sql(e, 'this')}",
            exp.FromBase64: rename_func("UNBASE64"),
            exp.GenerateSeries: sequence_sql,
            exp.GenerateDateArray: sequence_sql,
            exp.If: if_sql(),
            exp.ILike: no_ilike_sql,
            exp.IntDiv: lambda self, e: self.binary(e, "DIV"),
            exp.IsNan: rename_func("ISNAN"),
            exp.JSONExtract: lambda self, e: self.func("GET_JSON_OBJECT", e.this, e.expression),
            exp.JSONExtractScalar: lambda self, e: self.func(
                "GET_JSON_OBJECT", e.this, e.expression
            ),
            exp.JSONFormat: _json_format_sql,
            exp.Left: left_to_substring_sql,
            exp.Map: var_map_sql,
            exp.Max: max_or_greatest,
            exp.MD5Digest: lambda self, e: self.func("UNHEX", self.func("MD5", e.this)),
            exp.Min: min_or_least,
            exp.MonthsBetween: lambda self, e: self.func("MONTHS_BETWEEN", e.this, e.expression),
            exp.NotNullColumnConstraint: lambda _, e: (
                "" if e.args.get("allow_null") else "NOT NULL"
            ),
            exp.VarMap: var_map_sql,
            exp.Create: preprocess(
                [
                    remove_unique_constraints,
                    ctas_with_tmp_tables_to_create_tmp_view,
                    move_schema_columns_to_partitioned_by,
                ]
            ),
            exp.Quantile: rename_func("PERCENTILE"),
            exp.ApproxQuantile: rename_func("PERCENTILE_APPROX"),
            exp.RegexpExtract: regexp_extract_sql,
            exp.RegexpExtractAll: regexp_extract_sql,
            exp.RegexpReplace: regexp_replace_sql,
            exp.RegexpLike: lambda self, e: self.binary(e, "RLIKE"),
            exp.RegexpSplit: rename_func("SPLIT"),
            exp.Right: right_to_substring_sql,
            exp.SchemaCommentProperty: lambda self, e: self.naked_property(e),
            exp.ArrayUniqueAgg: rename_func("COLLECT_SET"),
            exp.Split: lambda self, e: self.func(
                "SPLIT", e.this, self.func("CONCAT", "'\\\\Q'", e.expression, "'\\\\E'")
            ),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_qualify,
                    transforms.eliminate_distinct_on,
                    partial(transforms.unnest_to_explode, unnest_using_arrays_zip=False),
                    transforms.any_to_exists,
                ]
            ),
            exp.StrPosition: lambda self, e: strposition_sql(
                self, e, func_name="LOCATE", supports_position=True
            ),
            exp.StrToDate: _str_to_date_sql,
            exp.StrToTime: _str_to_time_sql,
            exp.StrToUnix: _str_to_unix_sql,
            exp.StructExtract: struct_extract_sql,
            exp.StarMap: rename_func("MAP"),
            exp.Table: transforms.preprocess([transforms.unnest_generate_series]),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimestampTrunc: lambda self, e: self.func("TRUNC", e.this, unit_to_str(e)),
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.ToBase64: rename_func("BASE64"),
            exp.TsOrDiToDi: lambda self, e: (
                f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS STRING), '-', ''), 1, 8) AS INT)"
            ),
            exp.TsOrDsAdd: _add_date_sql,
            exp.TsOrDsDiff: _date_diff_sql,
            exp.TsOrDsToDate: _to_date_sql,
            exp.TryCast: no_trycast_sql,
            exp.Trim: trim_sql,
            exp.Unicode: rename_func("ASCII"),
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, time_format("hive")(self, e)
            ),
            exp.UnixToTime: _unix_to_time_sql,
            exp.UnixToTimeStr: rename_func("FROM_UNIXTIME"),
            exp.Unnest: rename_func("EXPLODE"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITIONED BY {self.sql(e, 'this')}",
            exp.NumberToStr: rename_func("FORMAT_NUMBER"),
            exp.National: lambda self, e: self.national_sql(e, prefix=""),
            exp.ClusteredColumnConstraint: lambda self, e: (
                f"({self.expressions(e, 'this', indent=False)})"
            ),
            exp.NonClusteredColumnConstraint: lambda self, e: (
                f"({self.expressions(e, 'this', indent=False)})"
            ),
            exp.NotForReplicationColumnConstraint: lambda *_: "",
            exp.OnProperty: lambda *_: "",
            exp.PartitionedByBucket: lambda self, e: self.func("BUCKET", e.expression, e.this),
            exp.PartitionByTruncate: lambda self, e: self.func("TRUNCATE", e.expression, e.this),
            exp.PrimaryKeyColumnConstraint: lambda *_: "PRIMARY KEY",
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("LEVENSHTEIN")
            ),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.FileFormatProperty: exp.Properties.Location.POST_SCHEMA,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
            exp.WithDataProperty: exp.Properties.Location.UNSUPPORTED,
        }

        TS_OR_DS_EXPRESSIONS: t.Tuple[t.Type[exp.Expr], ...] = (
            exp.DateDiff,
            exp.Day,
            exp.Month,
            exp.Year,
        )

        IGNORE_NULLS_FUNCS = (exp.First, exp.Last, exp.FirstValue, exp.LastValue)

        def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
            this = expression.this
            if isinstance(this, self.IGNORE_NULLS_FUNCS):
                return self.func(this.sql_name(), this.this, exp.true())

            return super().ignorenulls_sql(expression)

        def unnest_sql(self, expression: exp.Unnest) -> str:
            return rename_func("EXPLODE")(self, expression)

        def _jsonpathkey_sql(self, expression: exp.JSONPathKey) -> str:
            if isinstance(expression.this, exp.JSONPathWildcard):
                self.unsupported("Unsupported wildcard in JSONPathKey expression")
                return ""

            return super()._jsonpathkey_sql(expression)

        def parameter_sql(self, expression: exp.Parameter) -> str:
            this = self.sql(expression, "this")
            expression_sql = self.sql(expression, "expression")

            parent = expression.parent
            this = f"{this}:{expression_sql}" if expression_sql else this

            if isinstance(parent, exp.EQ) and isinstance(parent.parent, exp.SetItem):
                # We need to produce SET key = value instead of SET ${key} = value
                return this

            return f"${{{this}}}"

        def schema_sql(self, expression: exp.Schema) -> str:
            for ordered in expression.find_all(exp.Ordered):
                if ordered.args.get("desc") is False:
                    ordered.set("desc", None)

            return super().schema_sql(expression)

        def constraint_sql(self, expression: exp.Constraint) -> str:
            for prop in list(expression.find_all(exp.Properties)):
                prop.pop()

            this = self.sql(expression, "this")
            expressions = self.expressions(expression, sep=" ", flat=True)
            return f"CONSTRAINT {this} {expressions}"

        def rowformatserdeproperty_sql(self, expression: exp.RowFormatSerdeProperty) -> str:
            serde_props = self.sql(expression, "serde_properties")
            serde_props = f" {serde_props}" if serde_props else ""
            return f"ROW FORMAT SERDE {self.sql(expression, 'this')}{serde_props}"

        def arrayagg_sql(self, expression: exp.ArrayAgg) -> str:
            return self.func(
                "COLLECT_LIST",
                expression.this.this if isinstance(expression.this, exp.Order) else expression.this,
            )

        # Hive/Spark lack native numeric TRUNC. CAST to BIGINT truncates toward zero (not rounds).
        # Potential enhancement: a TRUNC_TEMPLATE using FLOOR/CEIL with scale (Spark 3.3+)
        # could preserve decimals: CASE WHEN x >= 0 THEN FLOOR(x, d) ELSE CEIL(x, d) END
        @unsupported_args("decimals")
        def trunc_sql(self, expression: exp.Trunc) -> str:
            return self.sql(exp.cast(expression.this, exp.DType.BIGINT))

        def datatype_sql(self, expression: exp.DataType) -> str:
            if expression.this in self.PARAMETERIZABLE_TEXT_TYPES and (
                not expression.expressions or expression.expressions[0].name == "MAX"
            ):
                expression = exp.DataType.build("text")
            elif expression.is_type(exp.DType.TEXT) and expression.expressions:
                expression.set("this", exp.DType.VARCHAR)
            elif expression.this in exp.DataType.TEMPORAL_TYPES:
                expression = exp.DataType.build(expression.this)
            elif expression.is_type("float"):
                size_expression = expression.find(exp.DataTypeParam)
                if size_expression:
                    size = int(size_expression.name)
                    expression = (
                        exp.DataType.build("float") if size <= 32 else exp.DataType.build("double")
                    )

            return super().datatype_sql(expression)

        def version_sql(self, expression: exp.Version) -> str:
            sql = super().version_sql(expression)
            return sql.replace("FOR ", "", 1)

        def struct_sql(self, expression: exp.Struct) -> str:
            values = []

            for i, e in enumerate(expression.expressions):
                if isinstance(e, exp.PropertyEQ):
                    self.unsupported("Hive does not support named structs.")
                    values.append(e.expression)
                else:
                    values.append(e)

            return self.func("STRUCT", *values)

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            return super().columndef_sql(
                expression,
                sep=(
                    ": "
                    if isinstance(expression.parent, exp.DataType)
                    and expression.parent.is_type("struct")
                    else sep
                ),
            )

        def altercolumn_sql(self, expression: exp.AlterColumn) -> str:
            this = self.sql(expression, "this")
            new_name = self.sql(expression, "rename_to") or this
            dtype = self.sql(expression, "dtype")
            comment = (
                f" COMMENT {self.sql(expression, 'comment')}"
                if self.sql(expression, "comment")
                else ""
            )
            default = self.sql(expression, "default")
            visible = expression.args.get("visible")
            allow_null = expression.args.get("allow_null")
            drop = expression.args.get("drop")

            if any([default, drop, visible, allow_null, drop]):
                self.unsupported("Unsupported CHANGE COLUMN syntax")

            if not dtype:
                self.unsupported("CHANGE COLUMN without a type is not supported")

            return f"CHANGE COLUMN {this} {new_name} {dtype}{comment}"

        def renamecolumn_sql(self, expression: exp.RenameColumn) -> str:
            self.unsupported("Cannot rename columns without data type defined in Hive")
            return ""

        def alterset_sql(self, expression: exp.AlterSet) -> str:
            exprs = self.expressions(expression, flat=True)
            exprs = f" {exprs}" if exprs else ""
            location = self.sql(expression, "location")
            location = f" LOCATION {location}" if location else ""
            file_format = self.expressions(expression, key="file_format", flat=True, sep=" ")
            file_format = f" FILEFORMAT {file_format}" if file_format else ""
            serde = self.sql(expression, "serde")
            serde = f" SERDE {serde}" if serde else ""
            tags = self.expressions(expression, key="tag", flat=True, sep="")
            tags = f" TAGS {tags}" if tags else ""

            return f"SET{serde}{exprs}{location}{file_format}{tags}"

        def serdeproperties_sql(self, expression: exp.SerdeProperties) -> str:
            prefix = "WITH " if expression.args.get("with_") else ""
            exprs = self.expressions(expression, flat=True)

            return f"{prefix}SERDEPROPERTIES ({exprs})"

        def exists_sql(self, expression: exp.Exists) -> str:
            if expression.expression:
                return self.function_fallback_sql(expression)

            return super().exists_sql(expression)

        def timetostr_sql(self, expression: exp.TimeToStr) -> str:
            this = expression.this
            if isinstance(this, exp.TimeStrToTime):
                this = this.this

            return self.func("DATE_FORMAT", this, self.format_time(expression))

        def fileformatproperty_sql(self, expression: exp.FileFormatProperty) -> str:
            if isinstance(expression.this, exp.InputOutputFormat):
                this = self.sql(expression, "this")
            else:
                this = expression.name.upper()

            return f"STORED AS {this}"
