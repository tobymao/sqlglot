from __future__ import annotations

import typing as t

from sqlglot import exp, generator, transforms
from sqlglot.dialects.dialect import (
    arrow_json_extract_sql,
    build_date_delta,
    build_date_delta_with_interval,
    date_add_interval_sql,
    datestrtodate_sql,
    length_or_char_length_sql,
    max_or_greatest,
    min_or_least,
    no_ilike_sql,
    no_paren_current_date_sql,
    no_pivot_sql,
    no_tablesample_sql,
    no_trycast_sql,
    rename_func,
    strposition_sql,
    unit_to_var,
    trim_sql,
    timestrtotime_sql,
)
from sqlglot.generator import unsupported_args


def _date_trunc_sql(self: MySQLGenerator, expression: exp.DateTrunc) -> str:
    expr = self.sql(expression, "this")
    unit = expression.text("unit").upper()

    if unit == "WEEK":
        concat = f"CONCAT(YEAR({expr}), ' ', WEEK({expr}, 1), ' 1')"
        date_format = "%Y %u %w"
    elif unit == "MONTH":
        concat = f"CONCAT(YEAR({expr}), ' ', MONTH({expr}), ' 1')"
        date_format = "%Y %c %e"
    elif unit == "QUARTER":
        concat = f"CONCAT(YEAR({expr}), ' ', QUARTER({expr}) * 3 - 2, ' 1')"
        date_format = "%Y %c %e"
    elif unit == "YEAR":
        concat = f"CONCAT(YEAR({expr}), ' 1 1')"
        date_format = "%Y %c %e"
    else:
        if unit != "DAY":
            self.unsupported(f"Unexpected interval unit: {unit}")
        return self.func("DATE", expr)

    return self.func("STR_TO_DATE", concat, f"'{date_format}'")


def _str_to_date_sql(
    self: MySQLGenerator, expression: exp.StrToDate | exp.StrToTime | exp.TsOrDsToDate
) -> str:
    return self.func("STR_TO_DATE", expression.this, self.format_time(expression))


def _unix_to_time_sql(self: MySQLGenerator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func("FROM_UNIXTIME", timestamp, self.format_time(expression))

    return self.func(
        "FROM_UNIXTIME",
        exp.Div(this=timestamp, expression=exp.func("POW", 10, scale)),
        self.format_time(expression),
    )


def date_add_sql(
    kind: str,
) -> t.Callable[[generator.Generator, exp.Expr], str]:
    def func(self: generator.Generator, expression: exp.Expr) -> str:
        return self.func(
            f"DATE_{kind}",
            expression.this,
            exp.Interval(this=expression.expression, unit=unit_to_var(expression)),
        )

    return func


def _ts_or_ds_to_date_sql(self: MySQLGenerator, expression: exp.TsOrDsToDate) -> str:
    time_format = expression.args.get("format")
    return _str_to_date_sql(self, expression) if time_format else self.func("DATE", expression.this)


def _remove_ts_or_ds_to_date(
    to_sql: t.Optional[t.Callable[[MySQLGenerator, exp.Expr], str]] = None,
    args: t.Tuple[str, ...] = ("this",),
) -> t.Callable[[MySQLGenerator, exp.Func], str]:
    def func(self: MySQLGenerator, expression: exp.Func) -> str:
        for arg_key in args:
            arg = expression.args.get(arg_key)
            if isinstance(arg, (exp.TsOrDsToDate, exp.TsOrDsToTimestamp)) and not arg.args.get(
                "format"
            ):
                expression.set(arg_key, arg.this)

        return to_sql(self, expression) if to_sql else self.function_fallback_sql(expression)

    return func


class MySQLGenerator(generator.Generator):
    INTERVAL_ALLOWS_PLURAL_FORM = False
    LOCKING_READS_SUPPORTED = True
    NULL_ORDERING_SUPPORTED: t.Optional[bool] = None
    JOIN_HINTS = False
    TABLE_HINTS = True
    DUPLICATE_KEY_UPDATE_WITH_SET = False
    QUERY_HINT_SEP = " "
    VALUES_AS_TABLE = False
    NVL2_SUPPORTED = False
    LAST_DAY_SUPPORTS_DATE_PART = False
    JSON_TYPE_REQUIRED_FOR_EXTRACTION = True
    JSON_PATH_BRACKETED_KEY_SUPPORTED = False
    JSON_KEY_VALUE_PAIR_SEP = ","
    SUPPORTS_TO_NUMBER = False
    PARSE_JSON_NAME: t.Optional[str] = None
    PAD_FILL_PATTERN_IS_REQUIRED = True
    WRAP_DERIVED_VALUES = False
    VARCHAR_REQUIRES_SIZE = True
    SUPPORTS_MEDIAN = False
    UPDATE_STATEMENT_SUPPORTS_FROM = False

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.ArrayAgg: rename_func("GROUP_CONCAT"),
        exp.BitwiseAndAgg: rename_func("BIT_AND"),
        exp.BitwiseOrAgg: rename_func("BIT_OR"),
        exp.BitwiseXorAgg: rename_func("BIT_XOR"),
        exp.BitwiseCount: rename_func("BIT_COUNT"),
        exp.Chr: lambda self, e: self.chr_sql(e, "CHAR"),
        exp.CurrentDate: no_paren_current_date_sql,
        exp.CurrentVersion: rename_func("VERSION"),
        exp.DateDiff: _remove_ts_or_ds_to_date(
            lambda self, e: self.func("DATEDIFF", e.this, e.expression), ("this", "expression")
        ),
        exp.DateAdd: _remove_ts_or_ds_to_date(date_add_sql("ADD")),
        exp.DateStrToDate: datestrtodate_sql,
        exp.DateSub: _remove_ts_or_ds_to_date(date_add_sql("SUB")),
        exp.DateTrunc: _date_trunc_sql,
        exp.Day: _remove_ts_or_ds_to_date(),
        exp.DayOfMonth: _remove_ts_or_ds_to_date(rename_func("DAYOFMONTH")),
        exp.DayOfWeek: _remove_ts_or_ds_to_date(rename_func("DAYOFWEEK")),
        exp.DayOfYear: _remove_ts_or_ds_to_date(rename_func("DAYOFYEAR")),
        exp.GroupConcat: lambda self, e: (
            f"""GROUP_CONCAT({self.sql(e, "this")} SEPARATOR {self.sql(e, "separator") or "','"})"""
        ),
        exp.ILike: no_ilike_sql,
        exp.JSONExtractScalar: arrow_json_extract_sql,
        exp.Length: length_or_char_length_sql,
        exp.LogicalOr: rename_func("MAX"),
        exp.LogicalAnd: rename_func("MIN"),
        exp.Max: max_or_greatest,
        exp.Min: min_or_least,
        exp.Month: _remove_ts_or_ds_to_date(),
        exp.NullSafeEQ: lambda self, e: self.binary(e, "<=>"),
        exp.NullSafeNEQ: lambda self, e: f"NOT {self.binary(e, '<=>')}",
        exp.NumberToStr: rename_func("FORMAT"),
        exp.Pivot: no_pivot_sql,
        exp.Select: transforms.preprocess(
            [
                transforms.eliminate_distinct_on,
                transforms.eliminate_semi_and_anti_joins,
                transforms.eliminate_qualify,
                transforms.eliminate_full_outer_join,
                transforms.unnest_generate_date_array_using_recursive_cte,
            ]
        ),
        exp.StrPosition: lambda self, e: strposition_sql(
            self, e, func_name="LOCATE", supports_position=True
        ),
        exp.StrToDate: _str_to_date_sql,
        exp.StrToTime: _str_to_date_sql,
        exp.Stuff: rename_func("INSERT"),
        exp.SessionUser: lambda *_: "SESSION_USER()",
        exp.TableSample: no_tablesample_sql,
        exp.TimeFromParts: rename_func("MAKETIME"),
        exp.TimestampAdd: date_add_interval_sql("DATE", "ADD"),
        exp.TimestampDiff: lambda self, e: self.func(
            "TIMESTAMPDIFF", unit_to_var(e), e.expression, e.this
        ),
        exp.TimestampSub: date_add_interval_sql("DATE", "SUB"),
        exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
        exp.TimeStrToTime: lambda self, e: timestrtotime_sql(
            self,
            e,
            include_precision=not e.args.get("zone"),
        ),
        exp.TimeToStr: _remove_ts_or_ds_to_date(
            lambda self, e: self.func("DATE_FORMAT", e.this, self.format_time(e))
        ),
        exp.Trim: trim_sql,
        exp.Trunc: rename_func("TRUNCATE"),
        exp.TryCast: no_trycast_sql,
        exp.TsOrDsAdd: date_add_sql("ADD"),
        exp.TsOrDsDiff: lambda self, e: self.func("DATEDIFF", e.this, e.expression),
        exp.TsOrDsToDate: _ts_or_ds_to_date_sql,
        exp.Unicode: lambda self, e: f"ORD(CONVERT({self.sql(e.this)} USING utf32))",
        exp.UnixToTime: _unix_to_time_sql,
        exp.Week: _remove_ts_or_ds_to_date(),
        exp.WeekOfYear: _remove_ts_or_ds_to_date(rename_func("WEEKOFYEAR")),
        exp.Year: _remove_ts_or_ds_to_date(),
        exp.UtcTimestamp: rename_func("UTC_TIMESTAMP"),
        exp.UtcTime: rename_func("UTC_TIME"),
    }

    UNSIGNED_TYPE_MAPPING = {
        exp.DType.UBIGINT: "BIGINT",
        exp.DType.UINT: "INT",
        exp.DType.UMEDIUMINT: "MEDIUMINT",
        exp.DType.USMALLINT: "SMALLINT",
        exp.DType.UTINYINT: "TINYINT",
        exp.DType.UDECIMAL: "DECIMAL",
        exp.DType.UDOUBLE: "DOUBLE",
    }

    TIMESTAMP_TYPE_MAPPING = {
        exp.DType.DATETIME2: "DATETIME",
        exp.DType.SMALLDATETIME: "DATETIME",
        exp.DType.TIMESTAMP: "DATETIME",
        exp.DType.TIMESTAMPNTZ: "DATETIME",
        exp.DType.TIMESTAMPTZ: "TIMESTAMP",
        exp.DType.TIMESTAMPLTZ: "TIMESTAMP",
    }

    TYPE_MAPPING: t.ClassVar = {
        exp.DType.NCHAR: "CHAR",
        exp.DType.NVARCHAR: "VARCHAR",
        exp.DType.INET: "INET",
        exp.DType.ROWVERSION: "VARBINARY",
        exp.DType.UBIGINT: "BIGINT",
        exp.DType.UINT: "INT",
        exp.DType.UMEDIUMINT: "MEDIUMINT",
        exp.DType.USMALLINT: "SMALLINT",
        exp.DType.UTINYINT: "TINYINT",
        exp.DType.UDECIMAL: "DECIMAL",
        exp.DType.UDOUBLE: "DOUBLE",
        exp.DType.DATETIME2: "DATETIME",
        exp.DType.SMALLDATETIME: "DATETIME",
        exp.DType.TIMESTAMP: "DATETIME",
        exp.DType.TIMESTAMPNTZ: "DATETIME",
        exp.DType.TIMESTAMPTZ: "TIMESTAMP",
        exp.DType.TIMESTAMPLTZ: "TIMESTAMP",
    }

    PROPERTIES_LOCATION: t.ClassVar = {
        **generator.Generator.PROPERTIES_LOCATION,
        exp.TransientProperty: exp.Properties.Location.UNSUPPORTED,
        exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        exp.PartitionedByProperty: exp.Properties.Location.UNSUPPORTED,
        exp.PartitionByRangeProperty: exp.Properties.Location.POST_SCHEMA,
        exp.PartitionByListProperty: exp.Properties.Location.POST_SCHEMA,
    }

    LIMIT_FETCH = "LIMIT"

    LIMIT_ONLY_LITERALS = True

    CHAR_CAST_MAPPING = dict.fromkeys(
        (
            exp.DType.LONGTEXT,
            exp.DType.LONGBLOB,
            exp.DType.MEDIUMBLOB,
            exp.DType.MEDIUMTEXT,
            exp.DType.TEXT,
            exp.DType.TINYBLOB,
            exp.DType.TINYTEXT,
            exp.DType.VARCHAR,
        ),
        "CHAR",
    )
    SIGNED_CAST_MAPPING = dict.fromkeys(
        (
            exp.DType.BIGINT,
            exp.DType.BOOLEAN,
            exp.DType.INT,
            exp.DType.SMALLINT,
            exp.DType.TINYINT,
            exp.DType.MEDIUMINT,
        ),
        "SIGNED",
    )

    # MySQL doesn't support many datatypes in cast.
    # https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#function_cast
    CAST_MAPPING = {
        exp.DType.LONGTEXT: "CHAR",
        exp.DType.LONGBLOB: "CHAR",
        exp.DType.MEDIUMBLOB: "CHAR",
        exp.DType.MEDIUMTEXT: "CHAR",
        exp.DType.TEXT: "CHAR",
        exp.DType.TINYBLOB: "CHAR",
        exp.DType.TINYTEXT: "CHAR",
        exp.DType.VARCHAR: "CHAR",
        exp.DType.BIGINT: "SIGNED",
        exp.DType.BOOLEAN: "SIGNED",
        exp.DType.INT: "SIGNED",
        exp.DType.SMALLINT: "SIGNED",
        exp.DType.TINYINT: "SIGNED",
        exp.DType.MEDIUMINT: "SIGNED",
        exp.DType.UBIGINT: "UNSIGNED",
    }

    TIMESTAMP_FUNC_TYPES = {
        exp.DType.TIMESTAMPTZ,
        exp.DType.TIMESTAMPLTZ,
    }

    # https://dev.mysql.com/doc/refman/8.0/en/keywords.html
    RESERVED_KEYWORDS = {
        "accessible",
        "add",
        "all",
        "alter",
        "analyze",
        "and",
        "as",
        "asc",
        "asensitive",
        "before",
        "between",
        "bigint",
        "binary",
        "blob",
        "both",
        "by",
        "call",
        "cascade",
        "case",
        "change",
        "char",
        "character",
        "check",
        "collate",
        "column",
        "condition",
        "constraint",
        "continue",
        "convert",
        "create",
        "cross",
        "cube",
        "cume_dist",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "cursor",
        "database",
        "databases",
        "day_hour",
        "day_microsecond",
        "day_minute",
        "day_second",
        "dec",
        "decimal",
        "declare",
        "default",
        "delayed",
        "delete",
        "dense_rank",
        "desc",
        "describe",
        "deterministic",
        "distinct",
        "distinctrow",
        "div",
        "double",
        "drop",
        "dual",
        "each",
        "else",
        "elseif",
        "empty",
        "enclosed",
        "escaped",
        "except",
        "exists",
        "exit",
        "explain",
        "false",
        "fetch",
        "first_value",
        "float",
        "float4",
        "float8",
        "for",
        "force",
        "foreign",
        "from",
        "fulltext",
        "function",
        "generated",
        "get",
        "grant",
        "group",
        "grouping",
        "groups",
        "having",
        "high_priority",
        "hour_microsecond",
        "hour_minute",
        "hour_second",
        "if",
        "ignore",
        "in",
        "index",
        "infile",
        "inner",
        "inout",
        "insensitive",
        "insert",
        "int",
        "int1",
        "int2",
        "int3",
        "int4",
        "int8",
        "integer",
        "intersect",
        "interval",
        "into",
        "io_after_gtids",
        "io_before_gtids",
        "is",
        "iterate",
        "join",
        "json_table",
        "key",
        "keys",
        "kill",
        "lag",
        "last_value",
        "lateral",
        "lead",
        "leading",
        "leave",
        "left",
        "like",
        "limit",
        "linear",
        "lines",
        "load",
        "localtime",
        "localtimestamp",
        "lock",
        "long",
        "longblob",
        "longtext",
        "loop",
        "low_priority",
        "master_bind",
        "master_ssl_verify_server_cert",
        "match",
        "maxvalue",
        "mediumblob",
        "mediumint",
        "mediumtext",
        "middleint",
        "minute_microsecond",
        "minute_second",
        "mod",
        "modifies",
        "natural",
        "not",
        "no_write_to_binlog",
        "nth_value",
        "ntile",
        "null",
        "numeric",
        "of",
        "on",
        "optimize",
        "optimizer_costs",
        "option",
        "optionally",
        "or",
        "order",
        "out",
        "outer",
        "outfile",
        "over",
        "partition",
        "percent_rank",
        "precision",
        "primary",
        "procedure",
        "purge",
        "range",
        "rank",
        "read",
        "reads",
        "read_write",
        "real",
        "recursive",
        "references",
        "regexp",
        "release",
        "rename",
        "repeat",
        "replace",
        "require",
        "resignal",
        "restrict",
        "return",
        "revoke",
        "right",
        "rlike",
        "row",
        "rows",
        "row_number",
        "schema",
        "schemas",
        "second_microsecond",
        "select",
        "sensitive",
        "separator",
        "set",
        "show",
        "signal",
        "smallint",
        "spatial",
        "specific",
        "sql",
        "sqlexception",
        "sqlstate",
        "sqlwarning",
        "sql_big_result",
        "sql_calc_found_rows",
        "sql_small_result",
        "ssl",
        "starting",
        "stored",
        "straight_join",
        "system",
        "table",
        "terminated",
        "then",
        "tinyblob",
        "tinyint",
        "tinytext",
        "to",
        "trailing",
        "trigger",
        "true",
        "undo",
        "union",
        "unique",
        "unlock",
        "unsigned",
        "update",
        "usage",
        "use",
        "using",
        "utc_date",
        "utc_time",
        "utc_timestamp",
        "values",
        "varbinary",
        "varchar",
        "varcharacter",
        "varying",
        "virtual",
        "when",
        "where",
        "while",
        "window",
        "with",
        "write",
        "xor",
        "year_month",
        "zerofill",
    }

    SQL_SECURITY_VIEW_LOCATION = exp.Properties.Location.POST_CREATE

    def locate_properties(self, properties: exp.Properties) -> t.DefaultDict:
        locations = super().locate_properties(properties)

        # MySQL puts SQL SECURITY before VIEW but after the schema for functions/procedures
        if isinstance(create := properties.parent, exp.Create) and create.kind == "VIEW":
            post_schema = locations[exp.Properties.Location.POST_SCHEMA]
            for i, p in enumerate(post_schema):
                if isinstance(p, exp.SqlSecurityProperty):
                    post_schema.pop(i)
                    locations[self.SQL_SECURITY_VIEW_LOCATION].append(p)
                    break

        return locations

    def computedcolumnconstraint_sql(self, expression: exp.ComputedColumnConstraint) -> str:
        persisted = "STORED" if expression.args.get("persisted") else "VIRTUAL"
        return f"GENERATED ALWAYS AS ({self.sql(expression.this.unnest())}) {persisted}"

    def array_sql(self, expression: exp.Array) -> str:
        self.unsupported("Arrays are not supported by MySQL")
        return self.function_fallback_sql(expression)

    def arraycontainsall_sql(self, expression: exp.ArrayContainsAll) -> str:
        self.unsupported("Array operations are not supported by MySQL")
        return self.function_fallback_sql(expression)

    def dpipe_sql(self, expression: exp.DPipe) -> str:
        return self.func("CONCAT", *expression.flatten())

    def extract_sql(self, expression: exp.Extract) -> str:
        unit = expression.name
        if unit and unit.lower() == "epoch":
            return self.func("UNIX_TIMESTAMP", expression.expression)

        return super().extract_sql(expression)

    def datatype_sql(self, expression: exp.DataType) -> str:
        if (
            self.VARCHAR_REQUIRES_SIZE
            and expression.is_type(exp.DType.VARCHAR)
            and not expression.expressions
        ):
            # `VARCHAR` must always have a size - if it doesn't, we always generate `TEXT`
            return "TEXT"

        # https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html
        result = super().datatype_sql(expression)
        if expression.this in self.UNSIGNED_TYPE_MAPPING:
            result = f"{result} UNSIGNED"

        return result

    def jsonarraycontains_sql(self, expression: exp.JSONArrayContains) -> str:
        return f"{self.sql(expression, 'this')} MEMBER OF({self.sql(expression, 'expression')})"

    def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
        if expression.to.this in self.TIMESTAMP_FUNC_TYPES:
            return self.func("TIMESTAMP", expression.this)

        to = self.CAST_MAPPING.get(expression.to.this)

        if to:
            expression.to.set("this", to)
        return super().cast_sql(expression)

    def show_sql(self, expression: exp.Show) -> str:
        this = f" {expression.name}"
        full = " FULL" if expression.args.get("full") else ""
        global_ = " GLOBAL" if expression.args.get("global_") else ""

        target = self.sql(expression, "target")
        target = f" {target}" if target else ""
        if expression.name in ("COLUMNS", "INDEX"):
            target = f" FROM{target}"
        elif expression.name == "GRANTS":
            target = f" FOR{target}"
        elif expression.name in ("LINKS", "PARTITIONS"):
            target = f" ON{target}" if target else ""
        elif expression.name == "PROJECTIONS":
            target = f" ON TABLE{target}" if target else ""

        db = self._prefixed_sql("FROM", expression, "db")

        like = self._prefixed_sql("LIKE", expression, "like")
        where = self.sql(expression, "where")

        types = self.expressions(expression, key="types")
        types = f" {types}" if types else types
        query = self._prefixed_sql("FOR QUERY", expression, "query")

        if expression.name == "PROFILE":
            offset = self._prefixed_sql("OFFSET", expression, "offset")
            limit = self._prefixed_sql("LIMIT", expression, "limit")
        else:
            offset = ""
            limit = self._oldstyle_limit_sql(expression)

        log = self._prefixed_sql("IN", expression, "log")
        position = self._prefixed_sql("FROM", expression, "position")

        channel = self._prefixed_sql("FOR CHANNEL", expression, "channel")

        if expression.name == "ENGINE":
            mutex_or_status = " MUTEX" if expression.args.get("mutex") else " STATUS"
        else:
            mutex_or_status = ""

        for_table = self._prefixed_sql("FOR TABLE", expression, "for_table")
        for_group = self._prefixed_sql("FOR GROUP", expression, "for_group")
        for_user = self._prefixed_sql("FOR USER", expression, "for_user")
        for_role = self._prefixed_sql("FOR ROLE", expression, "for_role")
        into_outfile = self._prefixed_sql("INTO OUTFILE", expression, "into_outfile")
        json = " JSON" if expression.args.get("json") else ""

        return f"SHOW{full}{global_}{this}{json}{target}{for_table}{types}{db}{query}{log}{position}{channel}{mutex_or_status}{like}{where}{offset}{limit}{for_group}{for_user}{for_role}{into_outfile}"

    def alterrename_sql(self, expression: exp.AlterRename, include_to: bool = True) -> str:
        """To avoid TO keyword in ALTER ... RENAME statements.
        It's moved from Doris, because it's the same for all MySQL, Doris, and StarRocks.
        """
        return super().alterrename_sql(expression, include_to=False)

    def altercolumn_sql(self, expression: exp.AlterColumn) -> str:
        dtype = self.sql(expression, "dtype")
        if not dtype:
            return super().altercolumn_sql(expression)

        this = self.sql(expression, "this")
        return f"MODIFY COLUMN {this} {dtype}"

    def _prefixed_sql(self, prefix: str, expression: exp.Expr, arg: str) -> str:
        sql = self.sql(expression, arg)
        return f" {prefix} {sql}" if sql else ""

    def _oldstyle_limit_sql(self, expression: exp.Show) -> str:
        limit = self.sql(expression, "limit")
        offset = self.sql(expression, "offset")
        if limit:
            limit_offset = f"{offset}, {limit}" if offset else limit
            return f" LIMIT {limit_offset}"
        return ""

    def timestamptrunc_sql(self, expression: exp.TimestampTrunc) -> str:
        unit = expression.args.get("unit")

        # Pick an old-enough date to avoid negative timestamp diffs
        start_ts = "'0000-01-01 00:00:00'"

        # Source: https://stackoverflow.com/a/32955740
        timestamp_diff = build_date_delta(exp.TimestampDiff)([unit, start_ts, expression.this])
        interval = exp.Interval(this=timestamp_diff, unit=unit)
        dateadd = build_date_delta_with_interval(exp.DateAdd)([start_ts, interval])

        return self.sql(dateadd)

    def converttimezone_sql(self, expression: exp.ConvertTimezone) -> str:
        from_tz = expression.args.get("source_tz")
        to_tz = expression.args.get("target_tz")
        dt = expression.args.get("timestamp")

        return self.func("CONVERT_TZ", dt, from_tz, to_tz)

    def attimezone_sql(self, expression: exp.AtTimeZone) -> str:
        self.unsupported("AT TIME ZONE is not supported by MySQL")
        return self.sql(expression.this)

    def isascii_sql(self, expression: exp.IsAscii) -> str:
        return f"REGEXP_LIKE({self.sql(expression.this)}, '^[[:ascii:]]*$')"

    def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
        # https://dev.mysql.com/doc/refman/8.4/en/window-function-descriptions.html
        self.unsupported("MySQL does not support IGNORE NULLS.")
        return self.sql(expression.this)

    @unsupported_args("this")
    def currentschema_sql(self, expression: exp.CurrentSchema) -> str:
        return self.func("SCHEMA")

    def partition_sql(self, expression: exp.Partition) -> str:
        parent = expression.parent
        if isinstance(parent, (exp.PartitionByRangeProperty, exp.PartitionByListProperty)):
            return self.expressions(expression, flat=True)
        return super().partition_sql(expression)

    def _partition_by_sql(
        self, expression: exp.PartitionByRangeProperty | exp.PartitionByListProperty, kind: str
    ) -> str:
        partitions = self.expressions(expression, key="partition_expressions", flat=True)
        create = self.expressions(expression, key="create_expressions", flat=True)
        return f"PARTITION BY {kind} ({partitions}) ({create})"

    def partitionbyrangeproperty_sql(self, expression: exp.PartitionByRangeProperty) -> str:
        return self._partition_by_sql(expression, "RANGE")

    def partitionbylistproperty_sql(self, expression: exp.PartitionByListProperty) -> str:
        return self._partition_by_sql(expression, "LIST")

    def partitionlist_sql(self, expression: exp.PartitionList) -> str:
        name = self.sql(expression, "this")
        values = self.expressions(expression, flat=True)
        return f"PARTITION {name} VALUES IN ({values})"

    def partitionrange_sql(self, expression: exp.PartitionRange) -> str:
        name = self.sql(expression, "this")
        values = self.expressions(expression, flat=True)
        return f"PARTITION {name} VALUES LESS THAN ({values})"
