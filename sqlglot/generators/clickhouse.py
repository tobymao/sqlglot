from __future__ import annotations

import datetime
import typing as t

from sqlglot import exp, generator
from sqlglot.dialects.dialect import (
    arg_max_or_min_no_count,
    inline_array_sql,
    jarowinkler_similarity,
    json_extract_segments,
    json_path_key_only_name,
    length_or_char_length_sql,
    no_pivot_sql,
    rename_func,
    remove_from_array_using_filter,
    sha256_sql,
    strposition_sql,
    var_map_sql,
    unit_to_str,
    unit_to_var,
    trim_sql,
    sha2_digest_sql,
)
from sqlglot.generator import unsupported_args
from sqlglot.helper import is_int

DATETIME_DELTA = t.Union[exp.DateAdd, exp.DateDiff, exp.DateSub, exp.TimestampSub, exp.TimestampAdd]


def _unix_to_time_sql(self: ClickHouseGenerator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func("fromUnixTimestamp", exp.cast(timestamp, exp.DType.BIGINT))
    if scale == exp.UnixToTime.MILLIS:
        return self.func("fromUnixTimestamp64Milli", exp.cast(timestamp, exp.DType.BIGINT))
    if scale == exp.UnixToTime.MICROS:
        return self.func("fromUnixTimestamp64Micro", exp.cast(timestamp, exp.DType.BIGINT))
    if scale == exp.UnixToTime.NANOS:
        return self.func("fromUnixTimestamp64Nano", exp.cast(timestamp, exp.DType.BIGINT))

    return self.func(
        "fromUnixTimestamp",
        exp.cast(exp.Div(this=timestamp, expression=exp.func("POW", 10, scale)), exp.DType.BIGINT),
    )


def _lower_func(sql: str) -> str:
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


def _quantile_sql(self: ClickHouseGenerator, expression: exp.Quantile) -> str:
    quantile = expression.args["quantile"]
    args = f"({self.sql(expression, 'this')})"

    if isinstance(quantile, exp.Array):
        func = self.func("quantiles", *quantile)
    else:
        func = self.func("quantile", quantile)

    return func + args


def _datetime_delta_sql(name: str) -> t.Callable[[generator.Generator, DATETIME_DELTA], str]:
    def _delta_sql(self: generator.Generator, expression: DATETIME_DELTA) -> str:
        if not expression.unit:
            return rename_func(name)(self, expression)

        return self.func(
            name,
            unit_to_var(expression),
            expression.expression,
            expression.this,
            expression.args.get("zone"),
        )

    return _delta_sql


def _timestrtotime_sql(self: ClickHouseGenerator, expression: exp.TimeStrToTime):
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
                    ts_frac_parts[1] if num_frac_parts > 1 else "",  # utc offset (if present)
                ]
            )

        # return literal with no timezone, eg turn '2020-01-01 12:13:14-08:00' into '2020-01-01 12:13:14'
        # this is because Clickhouse encodes the timezone as a data type parameter and throws an error if
        # it's part of the timestamp string
        ts_without_tz = (
            datetime.datetime.fromisoformat(ts_string).replace(tzinfo=None).isoformat(sep=" ")
        )
        ts = exp.Literal.string(ts_without_tz)

    # Non-nullable DateTime64 with microsecond precision
    expressions = [exp.DataTypeParam(this=tz)] if tz else []
    datatype = exp.DataType.build(
        exp.DType.DATETIME64,
        expressions=[exp.DataTypeParam(this=exp.Literal.number(6)), *expressions],
        nullable=False,
    )

    return self.sql(exp.cast(ts, datatype, dialect=self.dialect))


def _map_sql(self: ClickHouseGenerator, expression: exp.Map | exp.VarMap) -> str:
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


def _json_cast_sql(self: ClickHouseGenerator, expression: exp.JSONCast) -> str:
    this = self.sql(expression, "this")
    to = expression.to
    to_sql = self.sql(to)

    if to.expressions:
        to_sql = self.sql(exp.to_identifier(to_sql))

    return f"{this}.:{to_sql}"


class ClickHouseGenerator(generator.Generator):
    SELECT_KINDS: t.Tuple[str, ...] = ()
    TRY_SUPPORTED = False
    SUPPORTS_UESCAPE = False
    SUPPORTS_DECODE_CASE = False

    AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS

    QUERY_HINTS = False
    STRUCT_DELIMITER = ("(", ")")
    NVL2_SUPPORTED = False
    TABLESAMPLE_REQUIRES_PARENS = False
    TABLESAMPLE_SIZE_IS_ROWS = False
    TABLESAMPLE_KEYWORDS = "SAMPLE"
    LAST_DAY_SUPPORTS_DATE_PART = False
    CAN_IMPLEMENT_ARRAY_ANY = True
    SUPPORTS_TO_NUMBER = False
    JOIN_HINTS = False
    TABLE_HINTS = False
    GROUPINGS_SEP = ""
    SET_OP_MODIFIERS = False
    ARRAY_SIZE_NAME = "LENGTH"
    WRAP_DERIVED_VALUES = False

    STRING_TYPE_MAPPING: t.ClassVar = {
        exp.DType.BLOB: "String",
        exp.DType.CHAR: "String",
        exp.DType.LONGBLOB: "String",
        exp.DType.LONGTEXT: "String",
        exp.DType.MEDIUMBLOB: "String",
        exp.DType.MEDIUMTEXT: "String",
        exp.DType.TINYBLOB: "String",
        exp.DType.TINYTEXT: "String",
        exp.DType.TEXT: "String",
        exp.DType.VARBINARY: "String",
        exp.DType.VARCHAR: "String",
    }

    SUPPORTED_JSON_PATH_PARTS = {
        exp.JSONPathKey,
        exp.JSONPathRoot,
        exp.JSONPathSubscript,
    }

    TYPE_MAPPING = {
        **generator.Generator.TYPE_MAPPING,
        exp.DType.BLOB: "String",
        exp.DType.CHAR: "String",
        exp.DType.LONGBLOB: "String",
        exp.DType.LONGTEXT: "String",
        exp.DType.MEDIUMBLOB: "String",
        exp.DType.MEDIUMTEXT: "String",
        exp.DType.TINYBLOB: "String",
        exp.DType.TINYTEXT: "String",
        exp.DType.TEXT: "String",
        exp.DType.VARBINARY: "String",
        exp.DType.VARCHAR: "String",
        exp.DType.ARRAY: "Array",
        exp.DType.BOOLEAN: "Bool",
        exp.DType.BIGINT: "Int64",
        exp.DType.DATE32: "Date32",
        exp.DType.DATETIME: "DateTime",
        exp.DType.DATETIME2: "DateTime",
        exp.DType.SMALLDATETIME: "DateTime",
        exp.DType.DATETIME64: "DateTime64",
        exp.DType.DECIMAL: "Decimal",
        exp.DType.DECIMAL32: "Decimal32",
        exp.DType.DECIMAL64: "Decimal64",
        exp.DType.DECIMAL128: "Decimal128",
        exp.DType.DECIMAL256: "Decimal256",
        exp.DType.TIMESTAMP: "DateTime",
        exp.DType.TIMESTAMPNTZ: "DateTime",
        exp.DType.TIMESTAMPTZ: "DateTime",
        exp.DType.DOUBLE: "Float64",
        exp.DType.ENUM: "Enum",
        exp.DType.ENUM8: "Enum8",
        exp.DType.ENUM16: "Enum16",
        exp.DType.FIXEDSTRING: "FixedString",
        exp.DType.FLOAT: "Float32",
        exp.DType.INT: "Int32",
        exp.DType.MEDIUMINT: "Int32",
        exp.DType.INT128: "Int128",
        exp.DType.INT256: "Int256",
        exp.DType.LOWCARDINALITY: "LowCardinality",
        exp.DType.MAP: "Map",
        exp.DType.NESTED: "Nested",
        exp.DType.NOTHING: "Nothing",
        exp.DType.SMALLINT: "Int16",
        exp.DType.STRUCT: "Tuple",
        exp.DType.TINYINT: "Int8",
        exp.DType.UBIGINT: "UInt64",
        exp.DType.UINT: "UInt32",
        exp.DType.UINT128: "UInt128",
        exp.DType.UINT256: "UInt256",
        exp.DType.USMALLINT: "UInt16",
        exp.DType.UTINYINT: "UInt8",
        exp.DType.IPV4: "IPv4",
        exp.DType.IPV6: "IPv6",
        exp.DType.POINT: "Point",
        exp.DType.RING: "Ring",
        exp.DType.LINESTRING: "LineString",
        exp.DType.MULTILINESTRING: "MultiLineString",
        exp.DType.POLYGON: "Polygon",
        exp.DType.MULTIPOLYGON: "MultiPolygon",
        exp.DType.AGGREGATEFUNCTION: "AggregateFunction",
        exp.DType.SIMPLEAGGREGATEFUNCTION: "SimpleAggregateFunction",
        exp.DType.DYNAMIC: "Dynamic",
    }

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.AnyValue: rename_func("any"),
        exp.ApproxDistinct: rename_func("uniq"),
        exp.ArrayDistinct: rename_func("arrayDistinct"),
        exp.ArrayConcat: rename_func("arrayConcat"),
        exp.ArrayContains: rename_func("has"),
        exp.ArrayFilter: lambda self, e: self.func("arrayFilter", e.expression, e.this),
        exp.ArrayRemove: remove_from_array_using_filter,
        exp.ArrayReverse: rename_func("arrayReverse"),
        exp.ArraySlice: rename_func("arraySlice"),
        exp.ArraySum: rename_func("arraySum"),
        exp.ArrayMax: rename_func("arrayMax"),
        exp.ArrayMin: rename_func("arrayMin"),
        exp.ArgMax: arg_max_or_min_no_count("argMax"),
        exp.ArgMin: arg_max_or_min_no_count("argMin"),
        exp.Array: inline_array_sql,
        exp.CityHash64: rename_func("cityHash64"),
        exp.CastToStrType: rename_func("CAST"),
        exp.CurrentDatabase: rename_func("CURRENT_DATABASE"),
        exp.CurrentSchemas: rename_func("CURRENT_SCHEMAS"),
        exp.CountIf: rename_func("countIf"),
        exp.CosineDistance: rename_func("cosineDistance"),
        exp.CompressColumnConstraint: lambda self, e: (
            f"CODEC({self.expressions(e, key='this', flat=True)})"
        ),
        exp.ComputedColumnConstraint: lambda self, e: (
            f"{'MATERIALIZED' if e.args.get('persisted') else 'ALIAS'} {self.sql(e, 'this')}"
        ),
        exp.CurrentDate: lambda self, e: self.func("CURRENT_DATE"),
        exp.CurrentVersion: rename_func("VERSION"),
        exp.DateAdd: _datetime_delta_sql("DATE_ADD"),
        exp.DateDiff: _datetime_delta_sql("DATE_DIFF"),
        exp.DateStrToDate: rename_func("toDate"),
        exp.DateSub: _datetime_delta_sql("DATE_SUB"),
        exp.Explode: rename_func("arrayJoin"),
        exp.FarmFingerprint: rename_func("farmFingerprint64"),
        exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
        exp.IsNan: rename_func("isNaN"),
        exp.JarowinklerSimilarity: jarowinkler_similarity("jaroWinklerSimilarity"),
        exp.JSONCast: _json_cast_sql,
        exp.JSONExtract: json_extract_segments("JSONExtractString", quoted_index=False),
        exp.JSONExtractScalar: json_extract_segments("JSONExtractString", quoted_index=False),
        exp.JSONPathKey: json_path_key_only_name,
        exp.JSONPathRoot: lambda *_: "",
        exp.Length: length_or_char_length_sql,
        exp.Map: _map_sql,
        exp.Median: rename_func("median"),
        exp.Nullif: rename_func("nullIf"),
        exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
        exp.Pivot: no_pivot_sql,
        exp.Quantile: _quantile_sql,
        exp.RegexpLike: lambda self, e: self.func("match", e.this, e.expression),
        exp.Rand: rename_func("randCanonical"),
        exp.StartsWith: rename_func("startsWith"),
        exp.Struct: rename_func("tuple"),
        exp.Trunc: rename_func("trunc"),
        exp.EndsWith: rename_func("endsWith"),
        exp.EuclideanDistance: rename_func("L2Distance"),
        exp.StrPosition: lambda self, e: strposition_sql(
            self,
            e,
            func_name="POSITION",
            supports_position=True,
            use_ansi_position=False,
        ),
        exp.TimeToStr: lambda self, e: self.func(
            "formatDateTime",
            e.this.this if isinstance(e.this, exp.TsOrDsToTimestamp) else e.this,
            self.format_time(e),
            e.args.get("zone"),
        ),
        exp.TimeStrToTime: _timestrtotime_sql,
        exp.TimestampAdd: _datetime_delta_sql("TIMESTAMP_ADD"),
        exp.TimestampSub: _datetime_delta_sql("TIMESTAMP_SUB"),
        exp.Typeof: rename_func("toTypeName"),
        exp.VarMap: _map_sql,
        exp.Xor: lambda self, e: self.func("xor", e.this, e.expression, *e.expressions),
        exp.MD5Digest: rename_func("MD5"),
        exp.MD5: lambda self, e: self.func("LOWER", self.func("HEX", self.func("MD5", e.this))),
        exp.SHA: rename_func("SHA1"),
        exp.SHA1Digest: rename_func("SHA1"),
        exp.SHA2: sha256_sql,
        exp.SHA2Digest: sha2_digest_sql,
        exp.Split: lambda self, e: self.func(
            "splitByString", e.args.get("expression"), e.this, e.args.get("limit")
        ),
        exp.RegexpSplit: lambda self, e: self.func(
            "splitByRegexp", e.args.get("expression"), e.this, e.args.get("limit")
        ),
        exp.UnixToTime: _unix_to_time_sql,
        exp.Trim: lambda self, e: trim_sql(self, e, default_trim_type="BOTH"),
        exp.Variance: rename_func("varSamp"),
        exp.SchemaCommentProperty: lambda self, e: self.naked_property(e),
        exp.Stddev: rename_func("stddevSamp"),
        exp.Chr: rename_func("CHAR"),
        exp.Lag: lambda self, e: self.func(
            "lagInFrame", e.this, e.args.get("offset"), e.args.get("default")
        ),
        exp.Lead: lambda self, e: self.func(
            "leadInFrame", e.this, e.args.get("offset"), e.args.get("default")
        ),
        exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
            rename_func("editDistance")
        ),
        exp.ParseDatetime: rename_func("parseDateTime"),
    }

    PROPERTIES_LOCATION = {
        **generator.Generator.PROPERTIES_LOCATION,
        exp.DefinerProperty: exp.Properties.Location.POST_SCHEMA,
        exp.OnCluster: exp.Properties.Location.POST_NAME,
        exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
        exp.ToTableProperty: exp.Properties.Location.POST_NAME,
        exp.UuidProperty: exp.Properties.Location.POST_NAME,
        exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
    }

    # There's no list in docs, but it can be found in Clickhouse code
    # see `ClickHouse/src/Parsers/ParserCreate*.cpp`
    ON_CLUSTER_TARGETS = {
        "SCHEMA",  # Transpiled CREATE SCHEMA may have OnCluster property set
        "DATABASE",
        "TABLE",
        "VIEW",
        "DICTIONARY",
        "INDEX",
        "FUNCTION",
        "NAMED COLLECTION",
    }

    # https://clickhouse.com/docs/en/sql-reference/data-types/nullable
    NON_NULLABLE_TYPES = {
        exp.DType.ARRAY,
        exp.DType.MAP,
        exp.DType.STRUCT,
        exp.DType.POINT,
        exp.DType.RING,
        exp.DType.LINESTRING,
        exp.DType.MULTILINESTRING,
        exp.DType.POLYGON,
        exp.DType.MULTIPOLYGON,
    }

    def offset_sql(self, expression: exp.Offset) -> str:
        offset = super().offset_sql(expression)

        # OFFSET ... FETCH syntax requires a "ROW" or "ROWS" keyword
        # https://clickhouse.com/docs/sql-reference/statements/select/offset
        parent = expression.parent
        if isinstance(parent, exp.Select) and isinstance(parent.args.get("limit"), exp.Fetch):
            offset = f"{offset} ROWS"

        return offset

    def strtodate_sql(self, expression: exp.StrToDate) -> str:
        strtodate_sql = self.function_fallback_sql(expression)

        if not isinstance(expression.parent, exp.Cast):
            # StrToDate returns DATEs in other dialects (eg. postgres), so
            # this branch aims to improve the transpilation to clickhouse
            return self.cast_sql(exp.cast(expression, "DATE"))

        return strtodate_sql

    def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
        this = expression.this

        if isinstance(this, exp.StrToDate) and expression.to == exp.DataType.build("datetime"):
            return self.sql(this)

        return super().cast_sql(expression, safe_prefix=safe_prefix)

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
            dtype = "String"
        else:
            dtype = super().datatype_sql(expression)

        # This section changes the type to `Nullable(...)` if the following conditions hold:
        # - It's marked as nullable - this ensures we won't wrap ClickHouse types with `Nullable`
        #   and change their semantics
        # - It's not the key type of a `Map`. This is because ClickHouse enforces the following
        #   constraint: "Type of Map key must be a type, that can be represented by integer or
        #   String or FixedString (possibly LowCardinality) or UUID or IPv6"
        # - It's not a composite type, e.g. `Nullable(Array(...))` is not a valid type
        parent = expression.parent
        nullable = expression.args.get("nullable")
        if nullable is True or (
            nullable is None
            and not (
                isinstance(parent, exp.DataType)
                and parent.is_type(exp.DType.MAP, check_nullable=True)
                and expression.index in (None, 0)
            )
            and not expression.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True)
        ):
            dtype = f"Nullable({dtype})"

        return dtype

    def cte_sql(self, expression: exp.CTE) -> str:
        if expression.args.get("scalar"):
            this = self.sql(expression, "this")
            alias = self.sql(expression, "alias")
            return f"{this} AS {alias}"

        return super().cte_sql(expression)

    def after_limit_modifiers(self, expression: exp.Expr) -> t.List[str]:
        return super().after_limit_modifiers(expression) + [
            (
                self.seg("SETTINGS ") + self.expressions(expression, key="settings", flat=True)
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

    def createable_sql(self, expression: exp.Create, locations: t.DefaultDict) -> str:
        if expression.kind in self.ON_CLUSTER_TARGETS and locations.get(
            exp.Properties.Location.POST_NAME
        ):
            this_name = self.sql(
                expression.this if isinstance(expression.this, exp.Schema) else expression,
                "this",
            )
            this_properties = " ".join(
                [self.sql(prop) for prop in locations[exp.Properties.Location.POST_NAME]]
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

    def indexcolumnconstraint_sql(self, expression: exp.IndexColumnConstraint) -> str:
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

    def nestedjsonselect_sql(self, expression: exp.NestedJSONSelect) -> str:
        return f"{self.sql(expression, 'this')}.^{self.sql(expression, 'expression')}"

    def is_sql(self, expression: exp.Is) -> str:
        is_sql = super().is_sql(expression)

        if isinstance(expression.parent, exp.Not):
            # value IS NOT NULL -> NOT (value IS NULL)
            is_sql = self.wrap(is_sql)

        return is_sql

    def in_sql(self, expression: exp.In) -> str:
        in_sql = super().in_sql(expression)

        if isinstance(expression.parent, exp.Not) and expression.args.get("is_global"):
            in_sql = in_sql.replace("GLOBAL IN", "GLOBAL NOT IN", 1)

        return in_sql

    def not_sql(self, expression: exp.Not) -> str:
        if isinstance(expression.this, exp.In):
            if expression.this.args.get("is_global"):
                # let `GLOBAL IN` child interpose `NOT`
                return self.sql(expression, "this")

            expression.set("this", exp.paren(expression.this, copy=False))

        return super().not_sql(expression)

    def values_sql(self, expression: exp.Values, values_as_table: bool = True) -> str:
        # If the VALUES clause contains tuples of expressions, we need to treat it
        # as a table since Clickhouse will automatically alias it as such.
        alias = expression.args.get("alias")

        if alias and alias.args.get("columns") and expression.expressions:
            values = expression.expressions[0].expressions
            values_as_table = any(isinstance(value, exp.Tuple) for value in values)
        else:
            values_as_table = True

        return super().values_sql(expression, values_as_table=values_as_table)

    def timestamptrunc_sql(self, expression: exp.TimestampTrunc) -> str:
        unit = unit_to_str(expression)
        # https://clickhouse.com/docs/whats-new/changelog/2023#improvement
        if self.dialect.version < (23, 12) and unit and unit.is_string:
            unit = exp.Literal.string(unit.name.lower())
        return self.func("dateTrunc", unit, expression.this, expression.args.get("zone"))
