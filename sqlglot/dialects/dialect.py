from __future__ import annotations

import typing as t
from enum import Enum
from functools import reduce

from sqlglot import exp
from sqlglot._typing import E
from sqlglot.errors import ParseError
from sqlglot.generator import Generator
from sqlglot.helper import flatten, seq_get
from sqlglot.parser import Parser
from sqlglot.time import TIMEZONES, format_time
from sqlglot.tokens import Token, Tokenizer, TokenType
from sqlglot.trie import new_trie

B = t.TypeVar("B", bound=exp.Binary)


class Dialects(str, Enum):
    DIALECT = ""

    BIGQUERY = "bigquery"
    CLICKHOUSE = "clickhouse"
    DATABRICKS = "databricks"
    DRILL = "drill"
    DUCKDB = "duckdb"
    HIVE = "hive"
    MYSQL = "mysql"
    ORACLE = "oracle"
    POSTGRES = "postgres"
    PRESTO = "presto"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    SPARK = "spark"
    SPARK2 = "spark2"
    SQLITE = "sqlite"
    STARROCKS = "starrocks"
    TABLEAU = "tableau"
    TERADATA = "teradata"
    TRINO = "trino"
    TSQL = "tsql"
    Doris = "doris"


class _Dialect(type):
    classes: t.Dict[str, t.Type[Dialect]] = {}

    def __eq__(cls, other: t.Any) -> bool:
        if cls is other:
            return True
        if isinstance(other, str):
            return cls is cls.get(other)
        if isinstance(other, Dialect):
            return cls is type(other)

        return False

    def __hash__(cls) -> int:
        return hash(cls.__name__.lower())

    @classmethod
    def __getitem__(cls, key: str) -> t.Type[Dialect]:
        return cls.classes[key]

    @classmethod
    def get(
        cls, key: str, default: t.Optional[t.Type[Dialect]] = None
    ) -> t.Optional[t.Type[Dialect]]:
        return cls.classes.get(key, default)

    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)
        enum = Dialects.__members__.get(clsname.upper())
        cls.classes[enum.value if enum is not None else clsname.lower()] = klass

        klass.TIME_TRIE = new_trie(klass.TIME_MAPPING)
        klass.FORMAT_TRIE = (
            new_trie(klass.FORMAT_MAPPING) if klass.FORMAT_MAPPING else klass.TIME_TRIE
        )
        klass.INVERSE_TIME_MAPPING = {v: k for k, v in klass.TIME_MAPPING.items()}
        klass.INVERSE_TIME_TRIE = new_trie(klass.INVERSE_TIME_MAPPING)

        klass.INVERSE_ESCAPE_SEQUENCES = {v: k for k, v in klass.ESCAPE_SEQUENCES.items()}

        klass.tokenizer_class = getattr(klass, "Tokenizer", Tokenizer)
        klass.parser_class = getattr(klass, "Parser", Parser)
        klass.generator_class = getattr(klass, "Generator", Generator)

        klass.QUOTE_START, klass.QUOTE_END = list(klass.tokenizer_class._QUOTES.items())[0]
        klass.IDENTIFIER_START, klass.IDENTIFIER_END = list(
            klass.tokenizer_class._IDENTIFIERS.items()
        )[0]

        def get_start_end(token_type: TokenType) -> t.Tuple[t.Optional[str], t.Optional[str]]:
            return next(
                (
                    (s, e)
                    for s, (e, t) in klass.tokenizer_class._FORMAT_STRINGS.items()
                    if t == token_type
                ),
                (None, None),
            )

        klass.BIT_START, klass.BIT_END = get_start_end(TokenType.BIT_STRING)
        klass.HEX_START, klass.HEX_END = get_start_end(TokenType.HEX_STRING)
        klass.BYTE_START, klass.BYTE_END = get_start_end(TokenType.BYTE_STRING)

        dialect_properties = {
            **{
                k: v
                for k, v in vars(klass).items()
                if not callable(v) and not isinstance(v, classmethod) and not k.startswith("__")
            },
            "TOKENIZER_CLASS": klass.tokenizer_class,
        }

        if enum not in ("", "bigquery"):
            dialect_properties["SELECT_KINDS"] = ()

        # Pass required dialect properties to the tokenizer, parser and generator classes
        for subclass in (klass.tokenizer_class, klass.parser_class, klass.generator_class):
            for name, value in dialect_properties.items():
                if hasattr(subclass, name):
                    setattr(subclass, name, value)

        if not klass.STRICT_STRING_CONCAT and klass.DPIPE_IS_STRING_CONCAT:
            klass.parser_class.BITWISE[TokenType.DPIPE] = exp.SafeDPipe

        if not klass.SUPPORTS_SEMI_ANTI_JOIN:
            klass.parser_class.TABLE_ALIAS_TOKENS = klass.parser_class.TABLE_ALIAS_TOKENS | {
                TokenType.ANTI,
                TokenType.SEMI,
            }

        klass.generator_class.can_identify = klass.can_identify

        return klass


class Dialect(metaclass=_Dialect):
    # Determines the base index offset for arrays
    INDEX_OFFSET = 0

    # If true unnest table aliases are considered only as column aliases
    UNNEST_COLUMN_ONLY = False

    # Determines whether or not the table alias comes after tablesample
    ALIAS_POST_TABLESAMPLE = False

    # Determines whether or not unquoted identifiers are resolved as uppercase
    # When set to None, it means that the dialect treats all identifiers as case-insensitive
    RESOLVES_IDENTIFIERS_AS_UPPERCASE: t.Optional[bool] = False

    # Determines whether or not an unquoted identifier can start with a digit
    IDENTIFIERS_CAN_START_WITH_DIGIT = False

    # Determines whether or not the DPIPE token ('||') is a string concatenation operator
    DPIPE_IS_STRING_CONCAT = True

    # Determines whether or not CONCAT's arguments must be strings
    STRICT_STRING_CONCAT = False

    # Determines whether or not user-defined data types are supported
    SUPPORTS_USER_DEFINED_TYPES = True

    # Determines whether or not SEMI/ANTI JOINs are supported
    SUPPORTS_SEMI_ANTI_JOIN = True

    # Determines how function names are going to be normalized
    NORMALIZE_FUNCTIONS: bool | str = "upper"

    # Determines whether the base comes first in the LOG function
    LOG_BASE_FIRST = True

    # Indicates the default null ordering method to use if not explicitly set
    # Options are: "nulls_are_small", "nulls_are_large", "nulls_are_last"
    NULL_ORDERING = "nulls_are_small"

    DATE_FORMAT = "'%Y-%m-%d'"
    DATEINT_FORMAT = "'%Y%m%d'"
    TIME_FORMAT = "'%Y-%m-%d %H:%M:%S'"

    # Custom time mappings in which the key represents dialect time format
    # and the value represents a python time format
    TIME_MAPPING: t.Dict[str, str] = {}

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_model_rules_date_time
    # https://docs.teradata.com/r/Teradata-Database-SQL-Functions-Operators-Expressions-and-Predicates/March-2017/Data-Type-Conversions/Character-to-DATE-Conversion/Forcing-a-FORMAT-on-CAST-for-Converting-Character-to-DATE
    # special syntax cast(x as date format 'yyyy') defaults to time_mapping
    FORMAT_MAPPING: t.Dict[str, str] = {}

    # Mapping of an unescaped escape sequence to the corresponding character
    ESCAPE_SEQUENCES: t.Dict[str, str] = {}

    # Columns that are auto-generated by the engine corresponding to this dialect
    # Such columns may be excluded from SELECT * queries, for example
    PSEUDOCOLUMNS: t.Set[str] = set()

    # Autofilled
    tokenizer_class = Tokenizer
    parser_class = Parser
    generator_class = Generator

    # A trie of the time_mapping keys
    TIME_TRIE: t.Dict = {}
    FORMAT_TRIE: t.Dict = {}

    INVERSE_TIME_MAPPING: t.Dict[str, str] = {}
    INVERSE_TIME_TRIE: t.Dict = {}

    INVERSE_ESCAPE_SEQUENCES: t.Dict[str, str] = {}

    def __eq__(self, other: t.Any) -> bool:
        return type(self) == other

    def __hash__(self) -> int:
        return hash(type(self))

    @classmethod
    def get_or_raise(cls, dialect: DialectType) -> t.Type[Dialect]:
        if not dialect:
            return cls
        if isinstance(dialect, _Dialect):
            return dialect
        if isinstance(dialect, Dialect):
            return dialect.__class__

        result = cls.get(dialect)
        if not result:
            raise ValueError(f"Unknown dialect '{dialect}'")

        return result

    @classmethod
    def format_time(
        cls, expression: t.Optional[str | exp.Expression]
    ) -> t.Optional[exp.Expression]:
        if isinstance(expression, str):
            return exp.Literal.string(
                # the time formats are quoted
                format_time(expression[1:-1], cls.TIME_MAPPING, cls.TIME_TRIE)
            )

        if expression and expression.is_string:
            return exp.Literal.string(format_time(expression.this, cls.TIME_MAPPING, cls.TIME_TRIE))

        return expression

    @classmethod
    def normalize_identifier(cls, expression: E) -> E:
        """
        Normalizes an unquoted identifier to either lower or upper case, thus essentially
        making it case-insensitive. If a dialect treats all identifiers as case-insensitive,
        they will be normalized to lowercase regardless of being quoted or not.
        """
        if isinstance(expression, exp.Identifier) and (
            not expression.quoted or cls.RESOLVES_IDENTIFIERS_AS_UPPERCASE is None
        ):
            expression.set(
                "this",
                expression.this.upper()
                if cls.RESOLVES_IDENTIFIERS_AS_UPPERCASE
                else expression.this.lower(),
            )

        return expression

    @classmethod
    def case_sensitive(cls, text: str) -> bool:
        """Checks if text contains any case sensitive characters, based on the dialect's rules."""
        if cls.RESOLVES_IDENTIFIERS_AS_UPPERCASE is None:
            return False

        unsafe = str.islower if cls.RESOLVES_IDENTIFIERS_AS_UPPERCASE else str.isupper
        return any(unsafe(char) for char in text)

    @classmethod
    def can_identify(cls, text: str, identify: str | bool = "safe") -> bool:
        """Checks if text can be identified given an identify option.

        Args:
            text: The text to check.
            identify:
                "always" or `True`: Always returns true.
                "safe": True if the identifier is case-insensitive.

        Returns:
            Whether or not the given text can be identified.
        """
        if identify is True or identify == "always":
            return True

        if identify == "safe":
            return not cls.case_sensitive(text)

        return False

    @classmethod
    def quote_identifier(cls, expression: E, identify: bool = True) -> E:
        if isinstance(expression, exp.Identifier):
            name = expression.this
            expression.set(
                "quoted",
                identify or cls.case_sensitive(name) or not exp.SAFE_IDENTIFIER_RE.match(name),
            )

        return expression

    def parse(self, sql: str, **opts) -> t.List[t.Optional[exp.Expression]]:
        return self.parser(**opts).parse(self.tokenize(sql), sql)

    def parse_into(
        self, expression_type: exp.IntoType, sql: str, **opts
    ) -> t.List[t.Optional[exp.Expression]]:
        return self.parser(**opts).parse_into(expression_type, self.tokenize(sql), sql)

    def generate(self, expression: exp.Expression, copy: bool = True, **opts) -> str:
        return self.generator(**opts).generate(expression, copy=copy)

    def transpile(self, sql: str, **opts) -> t.List[str]:
        return [
            self.generate(expression, copy=False, **opts) if expression else ""
            for expression in self.parse(sql)
        ]

    def tokenize(self, sql: str) -> t.List[Token]:
        return self.tokenizer.tokenize(sql)

    @property
    def tokenizer(self) -> Tokenizer:
        if not hasattr(self, "_tokenizer"):
            self._tokenizer = self.tokenizer_class()
        return self._tokenizer

    def parser(self, **opts) -> Parser:
        return self.parser_class(**opts)

    def generator(self, **opts) -> Generator:
        return self.generator_class(**opts)


DialectType = t.Union[str, Dialect, t.Type[Dialect], None]


def rename_func(name: str) -> t.Callable[[Generator, exp.Expression], str]:
    return lambda self, expression: self.func(name, *flatten(expression.args.values()))


def approx_count_distinct_sql(self: Generator, expression: exp.ApproxDistinct) -> str:
    if expression.args.get("accuracy"):
        self.unsupported("APPROX_COUNT_DISTINCT does not support accuracy")
    return self.func("APPROX_COUNT_DISTINCT", expression.this)


def if_sql(
    name: str = "IF", false_value: t.Optional[exp.Expression | str] = None
) -> t.Callable[[Generator, exp.If], str]:
    def _if_sql(self: Generator, expression: exp.If) -> str:
        return self.func(
            name,
            expression.this,
            expression.args.get("true"),
            expression.args.get("false") or false_value,
        )

    return _if_sql


def arrow_json_extract_sql(self: Generator, expression: exp.JSONExtract | exp.JSONBExtract) -> str:
    return self.binary(expression, "->")


def arrow_json_extract_scalar_sql(
    self: Generator, expression: exp.JSONExtractScalar | exp.JSONBExtractScalar
) -> str:
    return self.binary(expression, "->>")


def inline_array_sql(self: Generator, expression: exp.Array) -> str:
    return f"[{self.expressions(expression, flat=True)}]"


def no_ilike_sql(self: Generator, expression: exp.ILike) -> str:
    return self.like_sql(
        exp.Like(this=exp.Lower(this=expression.this), expression=expression.expression)
    )


def no_paren_current_date_sql(self: Generator, expression: exp.CurrentDate) -> str:
    zone = self.sql(expression, "this")
    return f"CURRENT_DATE AT TIME ZONE {zone}" if zone else "CURRENT_DATE"


def no_recursive_cte_sql(self: Generator, expression: exp.With) -> str:
    if expression.args.get("recursive"):
        self.unsupported("Recursive CTEs are unsupported")
        expression.args["recursive"] = False
    return self.with_sql(expression)


def no_safe_divide_sql(self: Generator, expression: exp.SafeDivide) -> str:
    n = self.sql(expression, "this")
    d = self.sql(expression, "expression")
    return f"IF({d} <> 0, {n} / {d}, NULL)"


def no_tablesample_sql(self: Generator, expression: exp.TableSample) -> str:
    self.unsupported("TABLESAMPLE unsupported")
    return self.sql(expression.this)


def no_pivot_sql(self: Generator, expression: exp.Pivot) -> str:
    self.unsupported("PIVOT unsupported")
    return ""


def no_trycast_sql(self: Generator, expression: exp.TryCast) -> str:
    return self.cast_sql(expression)


def no_properties_sql(self: Generator, expression: exp.Properties) -> str:
    self.unsupported("Properties unsupported")
    return ""


def no_comment_column_constraint_sql(
    self: Generator, expression: exp.CommentColumnConstraint
) -> str:
    self.unsupported("CommentColumnConstraint unsupported")
    return ""


def no_map_from_entries_sql(self: Generator, expression: exp.MapFromEntries) -> str:
    self.unsupported("MAP_FROM_ENTRIES unsupported")
    return ""


def str_position_sql(self: Generator, expression: exp.StrPosition) -> str:
    this = self.sql(expression, "this")
    substr = self.sql(expression, "substr")
    position = self.sql(expression, "position")
    if position:
        return f"STRPOS(SUBSTR({this}, {position}), {substr}) + {position} - 1"
    return f"STRPOS({this}, {substr})"


def struct_extract_sql(self: Generator, expression: exp.StructExtract) -> str:
    return (
        f"{self.sql(expression, 'this')}.{self.sql(exp.to_identifier(expression.expression.name))}"
    )


def var_map_sql(
    self: Generator, expression: exp.Map | exp.VarMap, map_func_name: str = "MAP"
) -> str:
    keys = expression.args["keys"]
    values = expression.args["values"]

    if not isinstance(keys, exp.Array) or not isinstance(values, exp.Array):
        self.unsupported("Cannot convert array columns into map.")
        return self.func(map_func_name, keys, values)

    args = []
    for key, value in zip(keys.expressions, values.expressions):
        args.append(self.sql(key))
        args.append(self.sql(value))

    return self.func(map_func_name, *args)


def format_time_lambda(
    exp_class: t.Type[E], dialect: str, default: t.Optional[bool | str] = None
) -> t.Callable[[t.List], E]:
    """Helper used for time expressions.

    Args:
        exp_class: the expression class to instantiate.
        dialect: target sql dialect.
        default: the default format, True being time.

    Returns:
        A callable that can be used to return the appropriately formatted time expression.
    """

    def _format_time(args: t.List):
        return exp_class(
            this=seq_get(args, 0),
            format=Dialect[dialect].format_time(
                seq_get(args, 1)
                or (Dialect[dialect].TIME_FORMAT if default is True else default or None)
            ),
        )

    return _format_time


def time_format(
    dialect: DialectType = None,
) -> t.Callable[[Generator, exp.UnixToStr | exp.StrToUnix], t.Optional[str]]:
    def _time_format(self: Generator, expression: exp.UnixToStr | exp.StrToUnix) -> t.Optional[str]:
        """
        Returns the time format for a given expression, unless it's equivalent
        to the default time format of the dialect of interest.
        """
        time_format = self.format_time(expression)
        return time_format if time_format != Dialect.get_or_raise(dialect).TIME_FORMAT else None

    return _time_format


def create_with_partitions_sql(self: Generator, expression: exp.Create) -> str:
    """
    In Hive and Spark, the PARTITIONED BY property acts as an extension of a table's schema. When the
    PARTITIONED BY value is an array of column names, they are transformed into a schema. The corresponding
    columns are removed from the create statement.
    """
    has_schema = isinstance(expression.this, exp.Schema)
    is_partitionable = expression.args.get("kind") in ("TABLE", "VIEW")

    if has_schema and is_partitionable:
        prop = expression.find(exp.PartitionedByProperty)
        if prop and prop.this and not isinstance(prop.this, exp.Schema):
            schema = expression.this
            columns = {v.name.upper() for v in prop.this.expressions}
            partitions = [col for col in schema.expressions if col.name.upper() in columns]
            schema.set("expressions", [e for e in schema.expressions if e not in partitions])
            prop.replace(exp.PartitionedByProperty(this=exp.Schema(expressions=partitions)))
            expression.set("this", schema)

    return self.create_sql(expression)


def parse_date_delta(
    exp_class: t.Type[E], unit_mapping: t.Optional[t.Dict[str, str]] = None
) -> t.Callable[[t.List], E]:
    def inner_func(args: t.List) -> E:
        unit_based = len(args) == 3
        this = args[2] if unit_based else seq_get(args, 0)
        unit = args[0] if unit_based else exp.Literal.string("DAY")
        unit = exp.var(unit_mapping.get(unit.name.lower(), unit.name)) if unit_mapping else unit
        return exp_class(this=this, expression=seq_get(args, 1), unit=unit)

    return inner_func


def parse_date_delta_with_interval(
    expression_class: t.Type[E],
) -> t.Callable[[t.List], t.Optional[E]]:
    def func(args: t.List) -> t.Optional[E]:
        if len(args) < 2:
            return None

        interval = args[1]

        if not isinstance(interval, exp.Interval):
            raise ParseError(f"INTERVAL expression expected but got '{interval}'")

        expression = interval.this
        if expression and expression.is_string:
            expression = exp.Literal.number(expression.this)

        return expression_class(
            this=args[0], expression=expression, unit=exp.Literal.string(interval.text("unit"))
        )

    return func


def date_trunc_to_time(args: t.List) -> exp.DateTrunc | exp.TimestampTrunc:
    unit = seq_get(args, 0)
    this = seq_get(args, 1)

    if isinstance(this, exp.Cast) and this.is_type("date"):
        return exp.DateTrunc(unit=unit, this=this)
    return exp.TimestampTrunc(this=this, unit=unit)


def date_add_interval_sql(
    data_type: str, kind: str
) -> t.Callable[[Generator, exp.Expression], str]:
    def func(self: Generator, expression: exp.Expression) -> str:
        this = self.sql(expression, "this")
        unit = expression.args.get("unit")
        unit = exp.var(unit.name.upper() if unit else "DAY")
        interval = exp.Interval(this=expression.expression, unit=unit)
        return f"{data_type}_{kind}({this}, {self.sql(interval)})"

    return func


def timestamptrunc_sql(self: Generator, expression: exp.TimestampTrunc) -> str:
    return self.func(
        "DATE_TRUNC", exp.Literal.string(expression.text("unit") or "day"), expression.this
    )


def no_timestamp_sql(self: Generator, expression: exp.Timestamp) -> str:
    if not expression.expression:
        return self.sql(exp.cast(expression.this, to=exp.DataType.Type.TIMESTAMP))
    if expression.text("expression").lower() in TIMEZONES:
        return self.sql(
            exp.AtTimeZone(
                this=exp.cast(expression.this, to=exp.DataType.Type.TIMESTAMP),
                zone=expression.expression,
            )
        )
    return self.function_fallback_sql(expression)


def locate_to_strposition(args: t.List) -> exp.Expression:
    return exp.StrPosition(
        this=seq_get(args, 1), substr=seq_get(args, 0), position=seq_get(args, 2)
    )


def strposition_to_locate_sql(self: Generator, expression: exp.StrPosition) -> str:
    return self.func(
        "LOCATE", expression.args.get("substr"), expression.this, expression.args.get("position")
    )


def left_to_substring_sql(self: Generator, expression: exp.Left) -> str:
    return self.sql(
        exp.Substring(
            this=expression.this, start=exp.Literal.number(1), length=expression.expression
        )
    )


def right_to_substring_sql(self: Generator, expression: exp.Left) -> str:
    return self.sql(
        exp.Substring(
            this=expression.this,
            start=exp.Length(this=expression.this) - exp.paren(expression.expression - 1),
        )
    )


def timestrtotime_sql(self: Generator, expression: exp.TimeStrToTime) -> str:
    return self.sql(exp.cast(expression.this, "timestamp"))


def datestrtodate_sql(self: Generator, expression: exp.DateStrToDate) -> str:
    return self.sql(exp.cast(expression.this, "date"))


# Used for Presto and Duckdb which use functions that don't support charset, and assume utf-8
def encode_decode_sql(
    self: Generator, expression: exp.Expression, name: str, replace: bool = True
) -> str:
    charset = expression.args.get("charset")
    if charset and charset.name.lower() != "utf-8":
        self.unsupported(f"Expected utf-8 character set, got {charset}.")

    return self.func(name, expression.this, expression.args.get("replace") if replace else None)


def min_or_least(self: Generator, expression: exp.Min) -> str:
    name = "LEAST" if expression.expressions else "MIN"
    return rename_func(name)(self, expression)


def max_or_greatest(self: Generator, expression: exp.Max) -> str:
    name = "GREATEST" if expression.expressions else "MAX"
    return rename_func(name)(self, expression)


def count_if_to_sum(self: Generator, expression: exp.CountIf) -> str:
    cond = expression.this

    if isinstance(expression.this, exp.Distinct):
        cond = expression.this.expressions[0]
        self.unsupported("DISTINCT is not supported when converting COUNT_IF to SUM")

    return self.func("sum", exp.func("if", cond, 1, 0))


def trim_sql(self: Generator, expression: exp.Trim) -> str:
    target = self.sql(expression, "this")
    trim_type = self.sql(expression, "position")
    remove_chars = self.sql(expression, "expression")
    collation = self.sql(expression, "collation")

    # Use TRIM/LTRIM/RTRIM syntax if the expression isn't database-specific
    if not remove_chars and not collation:
        return self.trim_sql(expression)

    trim_type = f"{trim_type} " if trim_type else ""
    remove_chars = f"{remove_chars} " if remove_chars else ""
    from_part = "FROM " if trim_type or remove_chars else ""
    collation = f" COLLATE {collation}" if collation else ""
    return f"TRIM({trim_type}{remove_chars}{from_part}{target}{collation})"


def str_to_time_sql(self: Generator, expression: exp.Expression) -> str:
    return self.func("STRPTIME", expression.this, self.format_time(expression))


def ts_or_ds_to_date_sql(dialect: str) -> t.Callable:
    def _ts_or_ds_to_date_sql(self: Generator, expression: exp.TsOrDsToDate) -> str:
        _dialect = Dialect.get_or_raise(dialect)
        time_format = self.format_time(expression)
        if time_format and time_format not in (_dialect.TIME_FORMAT, _dialect.DATE_FORMAT):
            return self.sql(
                exp.cast(
                    exp.StrToTime(this=expression.this, format=expression.args["format"]),
                    "date",
                )
            )
        return self.sql(exp.cast(expression.this, "date"))

    return _ts_or_ds_to_date_sql


def concat_to_dpipe_sql(self: Generator, expression: exp.Concat | exp.SafeConcat) -> str:
    return self.sql(reduce(lambda x, y: exp.DPipe(this=x, expression=y), expression.expressions))


def concat_ws_to_dpipe_sql(self: Generator, expression: exp.ConcatWs) -> str:
    delim, *rest_args = expression.expressions
    return self.sql(
        reduce(
            lambda x, y: exp.DPipe(this=x, expression=exp.DPipe(this=delim, expression=y)),
            rest_args,
        )
    )


def regexp_extract_sql(self: Generator, expression: exp.RegexpExtract) -> str:
    bad_args = list(filter(expression.args.get, ("position", "occurrence", "parameters")))
    if bad_args:
        self.unsupported(f"REGEXP_EXTRACT does not support the following arg(s): {bad_args}")

    return self.func(
        "REGEXP_EXTRACT", expression.this, expression.expression, expression.args.get("group")
    )


def regexp_replace_sql(self: Generator, expression: exp.RegexpReplace) -> str:
    bad_args = list(
        filter(expression.args.get, ("position", "occurrence", "parameters", "modifiers"))
    )
    if bad_args:
        self.unsupported(f"REGEXP_REPLACE does not support the following arg(s): {bad_args}")

    return self.func(
        "REGEXP_REPLACE", expression.this, expression.expression, expression.args["replacement"]
    )


def pivot_column_names(aggregations: t.List[exp.Expression], dialect: DialectType) -> t.List[str]:
    names = []
    for agg in aggregations:
        if isinstance(agg, exp.Alias):
            names.append(agg.alias)
        else:
            """
            This case corresponds to aggregations without aliases being used as suffixes
            (e.g. col_avg(foo)). We need to unquote identifiers because they're going to
            be quoted in the base parser's `_parse_pivot` method, due to `to_identifier`.
            Otherwise, we'd end up with `col_avg(`foo`)` (notice the double quotes).
            """
            agg_all_unquoted = agg.transform(
                lambda node: exp.Identifier(this=node.name, quoted=False)
                if isinstance(node, exp.Identifier)
                else node
            )
            names.append(agg_all_unquoted.sql(dialect=dialect, normalize_functions="lower"))

    return names


def binary_from_function(expr_type: t.Type[B]) -> t.Callable[[t.List], B]:
    return lambda args: expr_type(this=seq_get(args, 0), expression=seq_get(args, 1))


# Used to represent DATE_TRUNC in Doris, Postgres and Starrocks dialects
def parse_timestamp_trunc(args: t.List) -> exp.TimestampTrunc:
    return exp.TimestampTrunc(this=seq_get(args, 1), unit=seq_get(args, 0))


def any_value_to_max_sql(self: Generator, expression: exp.AnyValue) -> str:
    return self.func("MAX", expression.this)


def bool_xor_sql(self: Generator, expression: exp.Xor) -> str:
    a = self.sql(expression.left)
    b = self.sql(expression.right)
    return f"({a} AND (NOT {b})) OR ((NOT {a}) AND {b})"


# Used to generate JSON_OBJECT with a comma in BigQuery and MySQL instead of colon
def json_keyvalue_comma_sql(self: Generator, expression: exp.JSONKeyValue) -> str:
    return f"{self.sql(expression, 'this')}, {self.sql(expression, 'expression')}"


def is_parse_json(expression: exp.Expression) -> bool:
    return isinstance(expression, exp.ParseJSON) or (
        isinstance(expression, exp.Cast) and expression.is_type("json")
    )


def isnull_to_is_null(args: t.List) -> exp.Expression:
    return exp.Paren(this=exp.Is(this=seq_get(args, 0), expression=exp.null()))


def generatedasidentitycolumnconstraint_sql(
    self: Generator, expression: exp.GeneratedAsIdentityColumnConstraint
) -> str:
    start = self.sql(expression, "start") or "1"
    increment = self.sql(expression, "increment") or "1"
    return f"IDENTITY({start}, {increment})"


def arg_max_or_min_no_count(name: str) -> t.Callable[[Generator, exp.ArgMax | exp.ArgMin], str]:
    def _arg_max_or_min_sql(self: Generator, expression: exp.ArgMax | exp.ArgMin) -> str:
        if expression.args.get("count"):
            self.unsupported(f"Only two arguments are supported in function {name}.")

        return self.func(name, expression.this, expression.expression)

    return _arg_max_or_min_sql
