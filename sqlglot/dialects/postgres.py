from __future__ import annotations

import typing as t

from sqlglot import exp, generator, tokens, transforms
from sqlglot.dialects.dialect import (
    DATE_ADD_OR_SUB,
    Dialect,
    JSON_EXTRACT_TYPE,
    any_value_to_max_sql,
    array_append_sql,
    array_concat_sql,
    bool_xor_sql,
    datestrtodate_sql,
    filter_array_using_unnest,
    generate_series_sql,
    getbit_sql,
    inline_array_sql,
    json_extract_segments,
    json_path_key_only_name,
    max_or_greatest,
    merge_without_target_sql,
    min_or_least,
    no_last_day_sql,
    no_map_from_entries_sql,
    no_paren_current_date_sql,
    no_pivot_sql,
    no_trycast_sql,
    rename_func,
    sha256_sql,
    struct_extract_sql,
    timestamptrunc_sql,
    timestrtotime_sql,
    trim_sql,
    ts_or_ds_add_cast,
    strposition_sql,
    count_if_to_sum,
    groupconcat_sql,
    regexp_replace_global_modifier,
    sha2_digest_sql,
)
from sqlglot.generator import unsupported_args
from sqlglot.helper import seq_get
from sqlglot.parsers.postgres import Parser as PostgresParser
from sqlglot.tokens import TokenType


DATE_DIFF_FACTOR = {
    "MICROSECOND": " * 1000000",
    "MILLISECOND": " * 1000",
    "SECOND": "",
    "MINUTE": " / 60",
    "HOUR": " / 3600",
    "DAY": " / 86400",
}


def _date_add_sql(kind: str) -> t.Callable[[Postgres.Generator, DATE_ADD_OR_SUB], str]:
    def func(self: Postgres.Generator, expression: DATE_ADD_OR_SUB) -> str:
        if isinstance(expression, exp.TsOrDsAdd):
            expression = ts_or_ds_add_cast(expression)

        this = self.sql(expression, "this")
        unit = expression.args.get("unit")

        e = self._simplify_unless_literal(expression.expression)
        if isinstance(e, exp.Literal):
            e.set("is_string", True)
        elif e.is_number:
            e = exp.Literal.string(e.to_py())
        else:
            self.unsupported("Cannot add non literal")

        return f"{this} {kind} {self.sql(exp.Interval(this=e, unit=unit))}"

    return func


def _date_diff_sql(self: Postgres.Generator, expression: exp.DateDiff) -> str:
    unit = expression.text("unit").upper()
    factor = DATE_DIFF_FACTOR.get(unit)

    end = f"CAST({self.sql(expression, 'this')} AS TIMESTAMP)"
    start = f"CAST({self.sql(expression, 'expression')} AS TIMESTAMP)"

    if factor is not None:
        return f"CAST(EXTRACT(epoch FROM {end} - {start}){factor} AS BIGINT)"

    age = f"AGE({end}, {start})"

    if unit == "WEEK":
        unit = f"EXTRACT(days FROM ({end} - {start})) / 7"
    elif unit == "MONTH":
        unit = f"EXTRACT(year FROM {age}) * 12 + EXTRACT(month FROM {age})"
    elif unit == "QUARTER":
        unit = f"EXTRACT(year FROM {age}) * 4 + EXTRACT(month FROM {age}) / 3"
    elif unit == "YEAR":
        unit = f"EXTRACT(year FROM {age})"
    else:
        unit = age

    return f"CAST({unit} AS BIGINT)"


def _substring_sql(self: Postgres.Generator, expression: exp.Substring) -> str:
    this = self.sql(expression, "this")
    start = self.sql(expression, "start")
    length = self.sql(expression, "length")

    from_part = f" FROM {start}" if start else ""
    for_part = f" FOR {length}" if length else ""

    return f"SUBSTRING({this}{from_part}{for_part})"


def _auto_increment_to_serial(expression: exp.Expr) -> exp.Expr:
    auto = expression.find(exp.AutoIncrementColumnConstraint)

    if auto:
        expression.args["constraints"].remove(auto.parent)
        kind = expression.args["kind"]

        if kind.this == exp.DType.INT:
            kind.replace(exp.DataType(this=exp.DType.SERIAL))
        elif kind.this == exp.DType.SMALLINT:
            kind.replace(exp.DataType(this=exp.DType.SMALLSERIAL))
        elif kind.this == exp.DType.BIGINT:
            kind.replace(exp.DataType(this=exp.DType.BIGSERIAL))

    return expression


def _serial_to_generated(expression: exp.Expr) -> exp.Expr:
    if not isinstance(expression, exp.ColumnDef):
        return expression
    kind = expression.kind
    if not kind:
        return expression

    if kind.this == exp.DType.SERIAL:
        data_type = exp.DataType(this=exp.DType.INT)
    elif kind.this == exp.DType.SMALLSERIAL:
        data_type = exp.DataType(this=exp.DType.SMALLINT)
    elif kind.this == exp.DType.BIGSERIAL:
        data_type = exp.DataType(this=exp.DType.BIGINT)
    else:
        data_type = None

    if data_type:
        expression.args["kind"].replace(data_type)
        constraints = expression.args["constraints"]
        generated = exp.ColumnConstraint(kind=exp.GeneratedAsIdentityColumnConstraint(this=False))
        notnull = exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())

        if notnull not in constraints:
            constraints.insert(0, notnull)
        if generated not in constraints:
            constraints.insert(0, generated)

    return expression


def _json_extract_sql(
    name: str, op: str
) -> t.Callable[[Postgres.Generator, JSON_EXTRACT_TYPE], str]:
    def _generate(self: Postgres.Generator, expression: JSON_EXTRACT_TYPE) -> str:
        if expression.args.get("only_json_types"):
            return json_extract_segments(name, quoted_index=False, op=op)(self, expression)
        return json_extract_segments(name)(self, expression)

    return _generate


def _unix_to_time_sql(self: Postgres.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func("TO_TIMESTAMP", timestamp, self.format_time(expression))

    return self.func(
        "TO_TIMESTAMP",
        exp.Div(this=timestamp, expression=exp.func("POW", 10, scale)),
        self.format_time(expression),
    )


def _levenshtein_sql(self: Postgres.Generator, expression: exp.Levenshtein) -> str:
    name = "LEVENSHTEIN_LESS_EQUAL" if expression.args.get("max_dist") else "LEVENSHTEIN"

    return rename_func(name)(self, expression)


def _versioned_anyvalue_sql(self: Postgres.Generator, expression: exp.AnyValue) -> str:
    # https://www.postgresql.org/docs/16/functions-aggregate.html
    # https://www.postgresql.org/about/featurematrix/
    if self.dialect.version < (16,):
        return any_value_to_max_sql(self, expression)

    return rename_func("ANY_VALUE")(self, expression)


def _round_sql(self: Postgres.Generator, expression: exp.Round) -> str:
    this = self.sql(expression, "this")
    decimals = self.sql(expression, "decimals")

    if not decimals:
        return self.func("ROUND", this)

    if not expression.type:
        from sqlglot.optimizer.annotate_types import annotate_types

        expression = annotate_types(expression, dialect=self.dialect)

    # ROUND(double precision, integer) is not permitted in Postgres
    # so it's necessary to cast to decimal before rounding.
    if expression.this.is_type(exp.DType.DOUBLE):
        decimal_type = exp.DataType.build(exp.DType.DECIMAL, expressions=expression.expressions)
        this = self.sql(exp.Cast(this=this, to=decimal_type))

    return self.func("ROUND", this, decimals)


class Postgres(Dialect):
    INDEX_OFFSET = 1
    TYPED_DIVISION = True
    CONCAT_COALESCE = True
    NULL_ORDERING = "nulls_are_large"
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    TABLESAMPLE_SIZE_IS_PERCENT = True
    TABLES_REFERENCEABLE_AS_COLUMNS = True

    DEFAULT_FUNCTIONS_COLUMN_NAMES = {
        exp.ExplodingGenerateSeries: "generate_series",
    }

    TIME_MAPPING = {
        "d": "%u",  # 1-based day of week
        "D": "%u",  # 1-based day of week
        "dd": "%d",  # day of month
        "DD": "%d",  # day of month
        "ddd": "%j",  # zero padded day of year
        "DDD": "%j",  # zero padded day of year
        "FMDD": "%-d",  # - is no leading zero for Python; same for FM in postgres
        "FMDDD": "%-j",  # day of year
        "FMHH12": "%-I",  # 9
        "FMHH24": "%-H",  # 9
        "FMMI": "%-M",  # Minute
        "FMMM": "%-m",  # 1
        "FMSS": "%-S",  # Second
        "HH12": "%I",  # 09
        "HH24": "%H",  # 09
        "mi": "%M",  # zero padded minute
        "MI": "%M",  # zero padded minute
        "mm": "%m",  # 01
        "MM": "%m",  # 01
        "OF": "%z",  # utc offset
        "ss": "%S",  # zero padded second
        "SS": "%S",  # zero padded second
        "TMDay": "%A",  # TM is locale dependent
        "TMDy": "%a",
        "TMMon": "%b",  # Sep
        "TMMonth": "%B",  # September
        "TZ": "%Z",  # uppercase timezone name
        "US": "%f",  # zero padded microsecond
        "ww": "%U",  # 1-based week of year
        "WW": "%U",  # 1-based week of year
        "yy": "%y",  # 15
        "YY": "%y",  # 15
        "yyyy": "%Y",  # 2015
        "YYYY": "%Y",  # 2015
    }

    class Tokenizer(tokens.Tokenizer):
        BIT_STRINGS = [("b'", "'"), ("B'", "'")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'")]
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]
        BYTE_STRING_ESCAPES = ["'", "\\"]
        HEREDOC_STRINGS = ["$"]

        HEREDOC_TAG_IS_IDENTIFIER = True
        HEREDOC_STRING_ALTERNATIVE = TokenType.PARAMETER

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "~": TokenType.RLIKE,
            "@@": TokenType.DAT,
            "@>": TokenType.AT_GT,
            "<@": TokenType.LT_AT,
            "?&": TokenType.QMARK_AMP,
            "?|": TokenType.QMARK_PIPE,
            "#-": TokenType.HASH_DASH,
            "|/": TokenType.PIPE_SLASH,
            "||/": TokenType.DPIPE_SLASH,
            "BEGIN": TokenType.BEGIN,
            "BIGSERIAL": TokenType.BIGSERIAL,
            "CSTRING": TokenType.PSEUDO_TYPE,
            "DECLARE": TokenType.COMMAND,
            "DO": TokenType.COMMAND,
            "EXEC": TokenType.COMMAND,
            "HSTORE": TokenType.HSTORE,
            "INT8": TokenType.BIGINT,
            "MONEY": TokenType.MONEY,
            "NAME": TokenType.NAME,
            "OID": TokenType.OBJECT_IDENTIFIER,
            "ONLY": TokenType.ONLY,
            "POINT": TokenType.POINT,
            "REFRESH": TokenType.COMMAND,
            "REINDEX": TokenType.COMMAND,
            "RESET": TokenType.COMMAND,
            "SERIAL": TokenType.SERIAL,
            "SMALLSERIAL": TokenType.SMALLSERIAL,
            "TEMP": TokenType.TEMPORARY,
            "REGCLASS": TokenType.OBJECT_IDENTIFIER,
            "REGCOLLATION": TokenType.OBJECT_IDENTIFIER,
            "REGCONFIG": TokenType.OBJECT_IDENTIFIER,
            "REGDICTIONARY": TokenType.OBJECT_IDENTIFIER,
            "REGNAMESPACE": TokenType.OBJECT_IDENTIFIER,
            "REGOPER": TokenType.OBJECT_IDENTIFIER,
            "REGOPERATOR": TokenType.OBJECT_IDENTIFIER,
            "REGPROC": TokenType.OBJECT_IDENTIFIER,
            "REGPROCEDURE": TokenType.OBJECT_IDENTIFIER,
            "REGROLE": TokenType.OBJECT_IDENTIFIER,
            "REGTYPE": TokenType.OBJECT_IDENTIFIER,
            "FLOAT": TokenType.DOUBLE,
            "XML": TokenType.XML,
            "VARIADIC": TokenType.VARIADIC,
            "INOUT": TokenType.INOUT,
        }
        KEYWORDS.pop("/*+")
        KEYWORDS.pop("DIV")

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.HEREDOC_STRING,
        }

        VAR_SINGLE_TOKENS = {"$"}

    Parser = PostgresParser

    class Generator(generator.Generator):
        SINGLE_STRING_INTERVAL = True
        RENAME_TABLE_WITH_DB = False
        LOCKING_READS_SUPPORTED = True
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        NVL2_SUPPORTED = False
        PARAMETER_TOKEN = "$"
        NAMED_PLACEHOLDER_TOKEN = "%"
        TABLESAMPLE_SIZE_IS_ROWS = False
        TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"
        SUPPORTS_SELECT_INTO = True
        JSON_TYPE_REQUIRED_FOR_EXTRACTION = True
        SUPPORTS_UNLOGGED_TABLES = True
        LIKE_PROPERTY_INSIDE_SCHEMA = True
        MULTI_ARG_DISTINCT = False
        CAN_IMPLEMENT_ARRAY_ANY = True
        SUPPORTS_WINDOW_EXCLUDE = True
        COPY_HAS_INTO_KEYWORD = False
        ARRAY_CONCAT_IS_VAR_LEN = False
        SUPPORTS_MEDIAN = False
        ARRAY_SIZE_DIM_REQUIRED = True
        SUPPORTS_BETWEEN_FLAGS = True
        INOUT_SEPARATOR = ""  # PostgreSQL uses "INOUT" (no space)

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        def lateral_sql(self, expression: exp.Lateral) -> str:
            sql = super().lateral_sql(expression)

            if expression.args.get("cross_apply") is not None:
                sql = f"{sql} ON TRUE"

            return sql

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DType.TINYINT: "SMALLINT",
            exp.DType.FLOAT: "REAL",
            exp.DType.DOUBLE: "DOUBLE PRECISION",
            exp.DType.BINARY: "BYTEA",
            exp.DType.VARBINARY: "BYTEA",
            exp.DType.ROWVERSION: "BYTEA",
            exp.DType.DATETIME: "TIMESTAMP",
            exp.DType.TIMESTAMPNTZ: "TIMESTAMP",
            exp.DType.BLOB: "BYTEA",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: _versioned_anyvalue_sql,
            exp.ArrayConcat: array_concat_sql("ARRAY_CAT"),
            exp.ArrayFilter: filter_array_using_unnest,
            exp.ArrayAppend: array_append_sql("ARRAY_APPEND"),
            exp.ArrayPrepend: array_append_sql("ARRAY_PREPEND", swap_params=True),
            exp.BitwiseAndAgg: rename_func("BIT_AND"),
            exp.BitwiseOrAgg: rename_func("BIT_OR"),
            exp.BitwiseXor: lambda self, e: self.binary(e, "#"),
            exp.BitwiseXorAgg: rename_func("BIT_XOR"),
            exp.ColumnDef: transforms.preprocess([_auto_increment_to_serial, _serial_to_generated]),
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.CurrentUser: lambda *_: "CURRENT_USER",
            exp.CurrentVersion: rename_func("VERSION"),
            exp.DateAdd: _date_add_sql("+"),
            exp.DateDiff: _date_diff_sql,
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateSub: _date_add_sql("-"),
            exp.Explode: rename_func("UNNEST"),
            exp.ExplodingGenerateSeries: rename_func("GENERATE_SERIES"),
            exp.GenerateSeries: generate_series_sql("GENERATE_SERIES"),
            exp.Getbit: getbit_sql,
            exp.GroupConcat: lambda self, e: groupconcat_sql(
                self, e, func_name="STRING_AGG", within_group=False
            ),
            exp.IntDiv: rename_func("DIV"),
            exp.JSONArrayAgg: lambda self, e: self.func(
                "JSON_AGG",
                self.sql(e, "this"),
                suffix=f"{self.sql(e, 'order')})",
            ),
            exp.JSONExtract: _json_extract_sql("JSON_EXTRACT_PATH", "->"),
            exp.JSONExtractScalar: _json_extract_sql("JSON_EXTRACT_PATH_TEXT", "->>"),
            exp.JSONBExtract: lambda self, e: self.binary(e, "#>"),
            exp.JSONBExtractScalar: lambda self, e: self.binary(e, "#>>"),
            exp.JSONBContains: lambda self, e: self.binary(e, "?"),
            exp.ParseJSON: lambda self, e: self.sql(exp.cast(e.this, exp.DType.JSON)),
            exp.JSONPathKey: json_path_key_only_name,
            exp.JSONPathRoot: lambda *_: "",
            exp.JSONPathSubscript: lambda self, e: self.json_path_part(e.this),
            exp.LastDay: no_last_day_sql,
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.Max: max_or_greatest,
            exp.MapFromEntries: no_map_from_entries_sql,
            exp.Min: min_or_least,
            exp.Merge: merge_without_target_sql,
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.PercentileCont: transforms.preprocess(
                [transforms.add_within_group_for_percentiles]
            ),
            exp.PercentileDisc: transforms.preprocess(
                [transforms.add_within_group_for_percentiles]
            ),
            exp.Pivot: no_pivot_sql,
            exp.Rand: rename_func("RANDOM"),
            exp.RegexpLike: lambda self, e: self.binary(e, "~"),
            exp.RegexpILike: lambda self, e: self.binary(e, "~*"),
            exp.RegexpReplace: lambda self, e: self.func(
                "REGEXP_REPLACE",
                e.this,
                e.expression,
                e.args.get("replacement"),
                e.args.get("position"),
                e.args.get("occurrence"),
                regexp_replace_global_modifier(e),
            ),
            exp.Round: _round_sql,
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_semi_and_anti_joins,
                    transforms.eliminate_qualify,
                ]
            ),
            exp.SHA2: sha256_sql,
            exp.SHA2Digest: sha2_digest_sql,
            exp.StrPosition: lambda self, e: strposition_sql(self, e, func_name="POSITION"),
            exp.StrToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            exp.StructExtract: struct_extract_sql,
            exp.Substring: _substring_sql,
            exp.TimeFromParts: rename_func("MAKE_TIME"),
            exp.TimestampFromParts: rename_func("MAKE_TIMESTAMP"),
            exp.TimestampTrunc: timestamptrunc_sql(zone=True),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e)
            if e.args.get("format")
            else self.tochar_sql(e),
            exp.Trim: trim_sql,
            exp.TryCast: no_trycast_sql,
            exp.TsOrDsAdd: _date_add_sql("+"),
            exp.TsOrDsDiff: _date_diff_sql,
            exp.UnixToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this),
            exp.Uuid: lambda *_: "GEN_RANDOM_UUID()",
            exp.TimeToUnix: lambda self, e: self.func(
                "DATE_PART", exp.Literal.string("epoch"), e.this
            ),
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Variance: rename_func("VAR_SAMP"),
            exp.Xor: bool_xor_sql,
            exp.Unicode: rename_func("ASCII"),
            exp.UnixToTime: _unix_to_time_sql,
            exp.Levenshtein: _levenshtein_sql,
            exp.JSONObjectAgg: rename_func("JSON_OBJECT_AGG"),
            exp.JSONBObjectAgg: rename_func("JSONB_OBJECT_AGG"),
            exp.CountIf: count_if_to_sum,
        }

        TRANSFORMS.pop(exp.CommentColumnConstraint)

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.TransientProperty: exp.Properties.Location.UNSUPPORTED,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def schemacommentproperty_sql(self, expression: exp.SchemaCommentProperty) -> str:
            self.unsupported("Table comments are not supported in the CREATE statement")
            return ""

        def commentcolumnconstraint_sql(self, expression: exp.CommentColumnConstraint) -> str:
            self.unsupported("Column comments are not supported in the CREATE statement")
            return ""

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            # PostgreSQL places parameter modes BEFORE parameter name
            param_constraint = expression.find(exp.InOutColumnConstraint)

            if param_constraint:
                mode_sql = self.sql(param_constraint)
                param_constraint.pop()  # Remove to prevent double-rendering
                base_sql = super().columndef_sql(expression, sep)
                return f"{mode_sql} {base_sql}"

            return super().columndef_sql(expression, sep)

        def unnest_sql(self, expression: exp.Unnest) -> str:
            if len(expression.expressions) == 1:
                arg = expression.expressions[0]
                if isinstance(arg, exp.GenerateDateArray):
                    generate_series: exp.Expr = exp.GenerateSeries(**arg.args)
                    if isinstance(expression.parent, (exp.From, exp.Join)):
                        generate_series = (
                            exp.select("value::date")
                            .from_(exp.Table(this=generate_series).as_("_t", table=["value"]))
                            .subquery(expression.args.get("alias") or "_unnested_generate_series")
                        )
                    return self.sql(generate_series)

                from sqlglot.optimizer.annotate_types import annotate_types

                this = annotate_types(arg, dialect=self.dialect)
                if this.is_type("array<json>"):
                    while isinstance(this, exp.Cast):
                        this = this.this

                    arg_as_json = self.sql(exp.cast(this, exp.DType.JSON))
                    alias = self.sql(expression, "alias")
                    alias = f" AS {alias}" if alias else ""

                    if expression.args.get("offset"):
                        self.unsupported("Unsupported JSON_ARRAY_ELEMENTS with offset")

                    return f"JSON_ARRAY_ELEMENTS({arg_as_json}){alias}"

            return super().unnest_sql(expression)

        def bracket_sql(self, expression: exp.Bracket) -> str:
            """Forms like ARRAY[1, 2, 3][3] aren't allowed; we need to wrap the ARRAY."""
            if isinstance(expression.this, exp.Array):
                expression.set("this", exp.paren(expression.this, copy=False))

            return super().bracket_sql(expression)

        def matchagainst_sql(self, expression: exp.MatchAgainst) -> str:
            this = self.sql(expression, "this")
            expressions = [f"{self.sql(e)} @@ {this}" for e in expression.expressions]
            sql = " OR ".join(expressions)
            return f"({sql})" if len(expressions) > 1 else sql

        def alterset_sql(self, expression: exp.AlterSet) -> str:
            exprs = self.expressions(expression, flat=True)
            exprs = f"({exprs})" if exprs else ""

            access_method = self.sql(expression, "access_method")
            access_method = f"ACCESS METHOD {access_method}" if access_method else ""
            tablespace = self.sql(expression, "tablespace")
            tablespace = f"TABLESPACE {tablespace}" if tablespace else ""
            option = self.sql(expression, "option")

            return f"SET {exprs}{access_method}{tablespace}{option}"

        def datatype_sql(self, expression: exp.DataType) -> str:
            if expression.is_type(exp.DType.ARRAY):
                if expression.expressions:
                    values = self.expressions(expression, key="values", flat=True)
                    return f"{self.expressions(expression, flat=True)}[{values}]"
                return "ARRAY"

            if expression.is_type(exp.DType.DOUBLE, exp.DType.FLOAT) and expression.expressions:
                # Postgres doesn't support precision for REAL and DOUBLE PRECISION types
                return f"FLOAT({self.expressions(expression, flat=True)})"

            return super().datatype_sql(expression)

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            this = expression.this

            # Postgres casts DIV() to decimal for transpilation but when roundtripping it's superfluous
            if isinstance(this, exp.IntDiv) and expression.to == exp.DataType.build("decimal"):
                return self.sql(this)

            return super().cast_sql(expression, safe_prefix=safe_prefix)

        def array_sql(self, expression: exp.Array) -> str:
            exprs = expression.expressions
            func_name = self.normalize_func("ARRAY")

            if isinstance(seq_get(exprs, 0), exp.Select):
                return f"{func_name}({self.sql(exprs[0])})"

            return f"{func_name}{inline_array_sql(self, expression)}"

        def computedcolumnconstraint_sql(self, expression: exp.ComputedColumnConstraint) -> str:
            return f"GENERATED ALWAYS AS ({self.sql(expression, 'this')}) STORED"

        def isascii_sql(self, expression: exp.IsAscii) -> str:
            return f"({self.sql(expression.this)} ~ '^[[:ascii:]]*$')"

        def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
            # https://www.postgresql.org/docs/current/functions-window.html
            self.unsupported("PostgreSQL does not support IGNORE NULLS.")
            return self.sql(expression.this)

        def respectnulls_sql(self, expression: exp.RespectNulls) -> str:
            # https://www.postgresql.org/docs/current/functions-window.html
            self.unsupported("PostgreSQL does not support RESPECT NULLS.")
            return self.sql(expression.this)

        @unsupported_args("this")
        def currentschema_sql(self, expression: exp.CurrentSchema) -> str:
            return "CURRENT_SCHEMA"

        def interval_sql(self, expression: exp.Interval) -> str:
            unit = expression.text("unit").lower()

            this = expression.this
            if unit.startswith("quarter") and isinstance(this, exp.Literal):
                this.replace(exp.Literal.string(int(this.to_py()) * 3))
                expression.args["unit"].replace(exp.var("MONTH"))

            return super().interval_sql(expression)

        def placeholder_sql(self, expression: exp.Placeholder) -> str:
            if expression.args.get("jdbc"):
                return "?"

            this = f"({expression.name})" if expression.this else ""
            return f"{self.NAMED_PLACEHOLDER_TOKEN}{this}s"

        def arraycontains_sql(self, expression: exp.ArrayContains) -> str:
            # Convert DuckDB's LIST_CONTAINS(array, value) to PostgreSQL
            # DuckDB behavior:
            #   - LIST_CONTAINS([1,2,3], 2) -> true
            #   - LIST_CONTAINS([1,2,3], 4) -> false
            #   - LIST_CONTAINS([1,2,NULL], 4) -> false (not NULL)
            #   - LIST_CONTAINS([1,2,3], NULL) -> NULL
            #
            # PostgreSQL equivalent: CASE WHEN value IS NULL THEN NULL
            #                            ELSE COALESCE(value = ANY(array), FALSE) END
            value = expression.expression
            array = expression.this

            coalesce_expr = exp.Coalesce(
                this=value.eq(exp.Any(this=exp.paren(expression=array, copy=False))),
                expressions=[exp.false()],
            )

            case_expr = (
                exp.Case()
                .when(exp.Is(this=value, expression=exp.null()), exp.null(), copy=False)
                .else_(coalesce_expr, copy=False)
            )

            return self.sql(case_expr)
