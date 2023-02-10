from __future__ import annotations

import typing as t
from enum import Enum

from sqlglot import exp
from sqlglot.generator import Generator
from sqlglot.helper import flatten, seq_get
from sqlglot.parser import Parser
from sqlglot.time import format_time
from sqlglot.tokens import Tokenizer
from sqlglot.trie import new_trie

E = t.TypeVar("E", bound=exp.Expression)


class Dialects(str, Enum):
    DIALECT = ""

    BIGQUERY = "bigquery"
    CLICKHOUSE = "clickhouse"
    DUCKDB = "duckdb"
    HIVE = "hive"
    MYSQL = "mysql"
    ORACLE = "oracle"
    POSTGRES = "postgres"
    PRESTO = "presto"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    SPARK = "spark"
    SQLITE = "sqlite"
    STARROCKS = "starrocks"
    TABLEAU = "tableau"
    TRINO = "trino"
    TSQL = "tsql"
    DATABRICKS = "databricks"
    DRILL = "drill"
    TERADATA = "teradata"


class _Dialect(type):
    classes: t.Dict[str, t.Type[Dialect]] = {}

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

        klass.time_trie = new_trie(klass.time_mapping)
        klass.inverse_time_mapping = {v: k for k, v in klass.time_mapping.items()}
        klass.inverse_time_trie = new_trie(klass.inverse_time_mapping)

        klass.tokenizer_class = getattr(klass, "Tokenizer", Tokenizer)
        klass.parser_class = getattr(klass, "Parser", Parser)
        klass.generator_class = getattr(klass, "Generator", Generator)

        klass.quote_start, klass.quote_end = list(klass.tokenizer_class._QUOTES.items())[0]
        klass.identifier_start, klass.identifier_end = list(
            klass.tokenizer_class._IDENTIFIERS.items()
        )[0]

        if (
            klass.tokenizer_class._BIT_STRINGS
            and exp.BitString not in klass.generator_class.TRANSFORMS
        ):
            bs_start, bs_end = list(klass.tokenizer_class._BIT_STRINGS.items())[0]
            klass.generator_class.TRANSFORMS[
                exp.BitString
            ] = lambda self, e: f"{bs_start}{int(self.sql(e, 'this')):b}{bs_end}"
        if (
            klass.tokenizer_class._HEX_STRINGS
            and exp.HexString not in klass.generator_class.TRANSFORMS
        ):
            hs_start, hs_end = list(klass.tokenizer_class._HEX_STRINGS.items())[0]
            klass.generator_class.TRANSFORMS[
                exp.HexString
            ] = lambda self, e: f"{hs_start}{int(self.sql(e, 'this')):X}{hs_end}"
        if (
            klass.tokenizer_class._BYTE_STRINGS
            and exp.ByteString not in klass.generator_class.TRANSFORMS
        ):
            be_start, be_end = list(klass.tokenizer_class._BYTE_STRINGS.items())[0]
            klass.generator_class.TRANSFORMS[
                exp.ByteString
            ] = lambda self, e: f"{be_start}{self.sql(e, 'this')}{be_end}"

        return klass


class Dialect(metaclass=_Dialect):
    index_offset = 0
    unnest_column_only = False
    alias_post_tablesample = False
    normalize_functions: t.Optional[str] = "upper"
    null_ordering = "nulls_are_small"

    date_format = "'%Y-%m-%d'"
    dateint_format = "'%Y%m%d'"
    time_format = "'%Y-%m-%d %H:%M:%S'"
    time_mapping: t.Dict[str, str] = {}

    # autofilled
    quote_start = None
    quote_end = None
    identifier_start = None
    identifier_end = None

    time_trie = None
    inverse_time_mapping = None
    inverse_time_trie = None
    tokenizer_class = None
    parser_class = None
    generator_class = None

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
                format_time(
                    expression[1:-1],  # the time formats are quoted
                    cls.time_mapping,
                    cls.time_trie,
                )
            )
        if expression and expression.is_string:
            return exp.Literal.string(
                format_time(
                    expression.this,
                    cls.time_mapping,
                    cls.time_trie,
                )
            )
        return expression

    def parse(self, sql: str, **opts) -> t.List[t.Optional[exp.Expression]]:
        return self.parser(**opts).parse(self.tokenizer.tokenize(sql), sql)

    def parse_into(
        self, expression_type: exp.IntoType, sql: str, **opts
    ) -> t.List[t.Optional[exp.Expression]]:
        return self.parser(**opts).parse_into(expression_type, self.tokenizer.tokenize(sql), sql)

    def generate(self, expression: t.Optional[exp.Expression], **opts) -> str:
        return self.generator(**opts).generate(expression)

    def transpile(self, sql: str, **opts) -> t.List[str]:
        return [self.generate(expression, **opts) for expression in self.parse(sql)]

    @property
    def tokenizer(self) -> Tokenizer:
        if not hasattr(self, "_tokenizer"):
            self._tokenizer = self.tokenizer_class()  # type: ignore
        return self._tokenizer

    def parser(self, **opts) -> Parser:
        return self.parser_class(  # type: ignore
            **{
                "index_offset": self.index_offset,
                "unnest_column_only": self.unnest_column_only,
                "alias_post_tablesample": self.alias_post_tablesample,
                "null_ordering": self.null_ordering,
                **opts,
            },
        )

    def generator(self, **opts) -> Generator:
        return self.generator_class(  # type: ignore
            **{
                "quote_start": self.quote_start,
                "quote_end": self.quote_end,
                "identifier_start": self.identifier_start,
                "identifier_end": self.identifier_end,
                "string_escape": self.tokenizer_class.STRING_ESCAPES[0],
                "identifier_escape": self.tokenizer_class.IDENTIFIER_ESCAPES[0],
                "index_offset": self.index_offset,
                "time_mapping": self.inverse_time_mapping,
                "time_trie": self.inverse_time_trie,
                "unnest_column_only": self.unnest_column_only,
                "alias_post_tablesample": self.alias_post_tablesample,
                "normalize_functions": self.normalize_functions,
                "null_ordering": self.null_ordering,
                **opts,
            }
        )


DialectType = t.Union[str, Dialect, t.Type[Dialect], None]


def rename_func(name: str) -> t.Callable[[Generator, exp.Expression], str]:
    def _rename(self, expression):
        args = flatten(expression.args.values())
        return f"{self.normalize_func(name)}({self.format_args(*args)})"

    return _rename


def approx_count_distinct_sql(self: Generator, expression: exp.ApproxDistinct) -> str:
    if expression.args.get("accuracy"):
        self.unsupported("APPROX_COUNT_DISTINCT does not support accuracy")
    return f"APPROX_COUNT_DISTINCT({self.format_args(expression.this)})"


def if_sql(self: Generator, expression: exp.If) -> str:
    expressions = self.format_args(
        expression.this, expression.args.get("true"), expression.args.get("false")
    )
    return f"IF({expressions})"


def arrow_json_extract_sql(self: Generator, expression: exp.JSONExtract | exp.JSONBExtract) -> str:
    return self.binary(expression, "->")


def arrow_json_extract_scalar_sql(
    self: Generator, expression: exp.JSONExtractScalar | exp.JSONBExtractScalar
) -> str:
    return self.binary(expression, "->>")


def inline_array_sql(self: Generator, expression: exp.Array) -> str:
    return f"[{self.expressions(expression)}]"


def no_ilike_sql(self: Generator, expression: exp.ILike) -> str:
    return self.like_sql(
        exp.Like(
            this=exp.Lower(this=expression.this),
            expression=expression.args["expression"],
        )
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
    return self.sql(expression)


def no_trycast_sql(self: Generator, expression: exp.TryCast) -> str:
    return self.cast_sql(expression)


def no_properties_sql(self: Generator, expression: exp.Properties) -> str:
    self.unsupported("Properties unsupported")
    return ""


def str_position_sql(self: Generator, expression: exp.StrPosition) -> str:
    this = self.sql(expression, "this")
    substr = self.sql(expression, "substr")
    position = self.sql(expression, "position")
    if position:
        return f"STRPOS(SUBSTR({this}, {position}), {substr}) + {position} - 1"
    return f"STRPOS({this}, {substr})"


def struct_extract_sql(self: Generator, expression: exp.StructExtract) -> str:
    this = self.sql(expression, "this")
    struct_key = self.sql(exp.Identifier(this=expression.expression, quoted=True))
    return f"{this}.{struct_key}"


def var_map_sql(
    self: Generator, expression: exp.Map | exp.VarMap, map_func_name: str = "MAP"
) -> str:
    keys = expression.args["keys"]
    values = expression.args["values"]

    if not isinstance(keys, exp.Array) or not isinstance(values, exp.Array):
        self.unsupported("Cannot convert array columns into map.")
        return f"{map_func_name}({self.format_args(keys, values)})"

    args = []
    for key, value in zip(keys.expressions, values.expressions):
        args.append(self.sql(key))
        args.append(self.sql(value))
    return f"{map_func_name}({self.format_args(*args)})"


def format_time_lambda(
    exp_class: t.Type[E], dialect: str, default: t.Optional[bool | str] = None
) -> t.Callable[[t.Sequence], E]:
    """Helper used for time expressions.

    Args:
        exp_class: the expression class to instantiate.
        dialect: target sql dialect.
        default: the default format, True being time.

    Returns:
        A callable that can be used to return the appropriately formatted time expression.
    """

    def _format_time(args: t.Sequence):
        return exp_class(
            this=seq_get(args, 0),
            format=Dialect[dialect].format_time(
                seq_get(args, 1)
                or (Dialect[dialect].time_format if default is True else default or None)
            ),
        )

    return _format_time


def create_with_partitions_sql(self: Generator, expression: exp.Create) -> str:
    """
    In Hive and Spark, the PARTITIONED BY property acts as an extension of a table's schema. When the
    PARTITIONED BY value is an array of column names, they are transformed into a schema. The corresponding
    columns are removed from the create statement.
    """
    has_schema = isinstance(expression.this, exp.Schema)
    is_partitionable = expression.args.get("kind") in ("TABLE", "VIEW")

    if has_schema and is_partitionable:
        expression = expression.copy()
        prop = expression.find(exp.PartitionedByProperty)
        this = prop and prop.this
        if prop and not isinstance(this, exp.Schema):
            schema = expression.this
            columns = {v.name.upper() for v in this.expressions}
            partitions = [col for col in schema.expressions if col.name.upper() in columns]
            schema.set("expressions", [e for e in schema.expressions if e not in partitions])
            prop.replace(exp.PartitionedByProperty(this=exp.Schema(expressions=partitions)))
            expression.set("this", schema)

    return self.create_sql(expression)


def parse_date_delta(
    exp_class: t.Type[E], unit_mapping: t.Optional[t.Dict[str, str]] = None
) -> t.Callable[[t.Sequence], E]:
    def inner_func(args: t.Sequence) -> E:
        unit_based = len(args) == 3
        this = seq_get(args, 2) if unit_based else seq_get(args, 0)
        expression = seq_get(args, 1) if unit_based else seq_get(args, 1)
        unit = seq_get(args, 0) if unit_based else exp.Literal.string("DAY")
        unit = unit_mapping.get(unit.name.lower(), unit) if unit_mapping else unit  # type: ignore
        return exp_class(this=this, expression=expression, unit=unit)

    return inner_func


def locate_to_strposition(args: t.Sequence) -> exp.Expression:
    return exp.StrPosition(
        this=seq_get(args, 1),
        substr=seq_get(args, 0),
        position=seq_get(args, 2),
    )


def strposition_to_locate_sql(self: Generator, expression: exp.StrPosition) -> str:
    args = self.format_args(
        expression.args.get("substr"), expression.this, expression.args.get("position")
    )
    return f"LOCATE({args})"


def timestrtotime_sql(self: Generator, expression: exp.TimeStrToTime) -> str:
    return f"CAST({self.sql(expression, 'this')} AS TIMESTAMP)"


def datestrtodate_sql(self: Generator, expression: exp.DateStrToDate) -> str:
    return f"CAST({self.sql(expression, 'this')} AS DATE)"


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
