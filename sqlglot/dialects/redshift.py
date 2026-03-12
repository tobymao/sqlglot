from __future__ import annotations

import typing as t

from sqlglot import exp, transforms
from sqlglot.typing.redshift import EXPRESSION_METADATA
from sqlglot.dialects.dialect import (
    NormalizationStrategy,
    array_concat_sql,
    concat_to_dpipe_sql,
    concat_ws_to_dpipe_sql,
    date_delta_sql,
    generatedasidentitycolumnconstraint_sql,
    json_extract_segments,
    no_tablesample_sql,
    rename_func,
)
from sqlglot.dialects.postgres import Postgres
from sqlglot.generator import Generator
from sqlglot.helper import seq_get
from sqlglot.parsers.redshift import RedshiftParser
from sqlglot.tokens import TokenType


class Redshift(Postgres):
    # https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()
    SUPPORTS_USER_DEFINED_TYPES = False
    INDEX_OFFSET = 0
    COPY_PARAMS_ARE_CSV = False
    HEX_LOWERCASE = True
    HAS_DISTINCT_ARRAY_CONSTRUCTORS = True
    COALESCE_COMPARISON_NON_STANDARD = True
    REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL = False
    ARRAY_FUNCS_PROPAGATES_NULLS = True

    # ref: https://docs.aws.amazon.com/redshift/latest/dg/r_FORMAT_strings.html
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    TIME_MAPPING = {**Postgres.TIME_MAPPING, "MON": "%b", "HH24": "%H", "HH": "%I"}

    Parser = RedshiftParser

    class Tokenizer(Postgres.Tokenizer):
        BIT_STRINGS = []
        HEX_STRINGS = []
        STRING_ESCAPES = ["\\", "'"]

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "(+)": TokenType.JOIN_MARKER,
            "HLLSKETCH": TokenType.HLLSKETCH,
            "MINUS": TokenType.EXCEPT,
            "SUPER": TokenType.SUPER,
            "TOP": TokenType.TOP,
            "UNLOAD": TokenType.COMMAND,
            "VARBYTE": TokenType.VARBINARY,
            "BINARY VARYING": TokenType.VARBINARY,
        }
        KEYWORDS.pop("VALUES")

        # Redshift allows # to appear as a table identifier prefix
        SINGLE_TOKENS = Postgres.Tokenizer.SINGLE_TOKENS.copy()
        SINGLE_TOKENS.pop("#")

    class Generator(Postgres.Generator):
        LOCKING_READS_SUPPORTED = False
        QUERY_HINTS = False
        VALUES_AS_TABLE = False
        TZ_TO_WITH_TIME_ZONE = True
        NVL2_SUPPORTED = True
        LAST_DAY_SUPPORTS_DATE_PART = False
        CAN_IMPLEMENT_ARRAY_ANY = False
        MULTI_ARG_DISTINCT = True
        COPY_PARAMS_ARE_WRAPPED = False
        HEX_FUNC = "TO_HEX"
        PARSE_JSON_NAME = "JSON_PARSE"
        ARRAY_CONCAT_IS_VAR_LEN = False
        SUPPORTS_CONVERT_TIMEZONE = True
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        SUPPORTS_MEDIAN = True
        ALTER_SET_TYPE = "TYPE"
        SUPPORTS_DECODE_CASE = True
        SUPPORTS_BETWEEN_FLAGS = False
        LIMIT_FETCH = "LIMIT"
        STAR_EXCEPT = "EXCLUDE"
        STAR_EXCLUDE_REQUIRES_DERIVED_TABLE = False

        # Redshift doesn't have `WITH` as part of their with_properties so we remove it
        WITH_PROPERTIES_PREFIX = " "

        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,
            exp.DType.BINARY: "VARBYTE",
            exp.DType.BLOB: "VARBYTE",
            exp.DType.INT: "INTEGER",
            exp.DType.TIMETZ: "TIME",
            exp.DType.TIMESTAMPTZ: "TIMESTAMP",
            exp.DType.VARBINARY: "VARBYTE",
            exp.DType.ROWVERSION: "VARBYTE",
        }

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.ArrayConcat: array_concat_sql("ARRAY_CONCAT"),
            exp.Concat: concat_to_dpipe_sql,
            exp.ConcatWs: concat_ws_to_dpipe_sql,
            exp.ApproxDistinct: lambda self,
            e: f"APPROXIMATE COUNT(DISTINCT {self.sql(e, 'this')})",
            exp.CurrentTimestamp: lambda self, e: (
                "SYSDATE" if e.args.get("sysdate") else "GETDATE()"
            ),
            exp.DateAdd: date_delta_sql("DATEADD"),
            exp.DateDiff: date_delta_sql("DATEDIFF"),
            exp.DistKeyProperty: lambda self, e: self.func("DISTKEY", e.this),
            exp.DistStyleProperty: lambda self, e: self.naked_property(e),
            exp.Explode: lambda self, e: self.explode_sql(e),
            exp.FarmFingerprint: rename_func("FARMFINGERPRINT64"),
            exp.FromBase: rename_func("STRTOL"),
            exp.GeneratedAsIdentityColumnConstraint: generatedasidentitycolumnconstraint_sql,
            exp.JSONExtract: json_extract_segments("JSON_EXTRACT_PATH_TEXT"),
            exp.JSONExtractScalar: json_extract_segments("JSON_EXTRACT_PATH_TEXT"),
            exp.GroupConcat: rename_func("LISTAGG"),
            exp.Hex: lambda self, e: self.func("UPPER", self.func("TO_HEX", self.sql(e, "this"))),
            exp.RegexpExtract: rename_func("REGEXP_SUBSTR"),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_window_clause,
                    transforms.eliminate_distinct_on,
                    transforms.eliminate_semi_and_anti_joins,
                    transforms.unqualify_unnest,
                    transforms.unnest_generate_date_array_using_recursive_cte,
                ]
            ),
            exp.SortKeyProperty: lambda self,
            e: f"{'COMPOUND ' if e.args['compound'] else ''}SORTKEY({self.format_args(*e.this)})",
            exp.StartsWith: lambda self,
            e: f"{self.sql(e.this)} LIKE {self.sql(e.expression)} || '%'",
            exp.StringToArray: rename_func("SPLIT_TO_ARRAY"),
            exp.TableSample: no_tablesample_sql,
            exp.TsOrDsAdd: date_delta_sql("DATEADD"),
            exp.TsOrDsDiff: date_delta_sql("DATEDIFF"),
            exp.UnixToTime: lambda self, e: self._unix_to_time_sql(e),
            exp.SHA2Digest: lambda self, e: self.func(
                "SHA2", e.this, e.args.get("length") or exp.Literal.number(256)
            ),
        }

        # Postgres maps exp.Pivot to no_pivot_sql, but Redshift support pivots
        TRANSFORMS.pop(exp.Pivot)

        # Postgres doesn't support JSON_PARSE, but Redshift does
        TRANSFORMS.pop(exp.ParseJSON)

        # Redshift supports these functions
        TRANSFORMS.pop(exp.AnyValue)
        TRANSFORMS.pop(exp.LastDay)
        TRANSFORMS.pop(exp.SHA2)

        # Postgres and Redshift have different semantics for Getbit
        TRANSFORMS.pop(exp.Getbit)

        # Postgres does not permit a double precision argument in ROUND; Redshift does
        TRANSFORMS.pop(exp.Round)

        RESERVED_KEYWORDS = {
            "aes128",
            "aes256",
            "all",
            "allowoverwrite",
            "analyse",
            "analyze",
            "and",
            "any",
            "array",
            "as",
            "asc",
            "authorization",
            "az64",
            "backup",
            "between",
            "binary",
            "blanksasnull",
            "both",
            "bytedict",
            "bzip2",
            "case",
            "cast",
            "check",
            "collate",
            "column",
            "constraint",
            "create",
            "credentials",
            "cross",
            "current_date",
            "current_time",
            "current_timestamp",
            "current_user",
            "current_user_id",
            "default",
            "deferrable",
            "deflate",
            "defrag",
            "delta",
            "delta32k",
            "desc",
            "disable",
            "distinct",
            "do",
            "else",
            "emptyasnull",
            "enable",
            "encode",
            "encrypt     ",
            "encryption",
            "end",
            "except",
            "explicit",
            "false",
            "for",
            "foreign",
            "freeze",
            "from",
            "full",
            "globaldict256",
            "globaldict64k",
            "grant",
            "group",
            "gzip",
            "having",
            "identity",
            "ignore",
            "ilike",
            "in",
            "initially",
            "inner",
            "intersect",
            "interval",
            "into",
            "is",
            "isnull",
            "join",
            "leading",
            "left",
            "like",
            "limit",
            "localtime",
            "localtimestamp",
            "lun",
            "luns",
            "lzo",
            "lzop",
            "minus",
            "mostly16",
            "mostly32",
            "mostly8",
            "natural",
            "new",
            "not",
            "notnull",
            "null",
            "nulls",
            "off",
            "offline",
            "offset",
            "oid",
            "old",
            "on",
            "only",
            "open",
            "or",
            "order",
            "outer",
            "overlaps",
            "parallel",
            "partition",
            "percent",
            "permissions",
            "pivot",
            "placing",
            "primary",
            "raw",
            "readratio",
            "recover",
            "references",
            "rejectlog",
            "resort",
            "respect",
            "restore",
            "right",
            "select",
            "session_user",
            "similar",
            "snapshot",
            "some",
            "sysdate",
            "system",
            "table",
            "tag",
            "tdes",
            "text255",
            "text32k",
            "then",
            "timestamp",
            "to",
            "top",
            "trailing",
            "true",
            "truncatecolumns",
            "type",
            "union",
            "unique",
            "unnest",
            "unpivot",
            "user",
            "using",
            "verbose",
            "wallet",
            "when",
            "where",
            "with",
            "without",
        }

        def unnest_sql(self, expression: exp.Unnest) -> str:
            args = expression.expressions
            num_args = len(args)

            if num_args != 1:
                self.unsupported(f"Unsupported number of arguments in UNNEST: {num_args}")
                return ""

            if isinstance(expression.find_ancestor(exp.From, exp.Join, exp.Select), exp.Select):
                self.unsupported("Unsupported UNNEST when not used in FROM/JOIN clauses")
                return ""

            arg = self.sql(seq_get(args, 0))

            alias = self.expressions(expression.args.get("alias"), key="columns", flat=True)
            return f"{arg} AS {alias}" if alias else arg

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            if expression.is_type(exp.DType.JSON):
                # Redshift doesn't support a JSON type, so casting to it is treated as a noop
                return self.sql(expression, "this")

            return super().cast_sql(expression, safe_prefix=safe_prefix)

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Redshift converts the `TEXT` data type to `VARCHAR(255)` by default when people more generally mean
            VARCHAR of max length which is `VARCHAR(max)` in Redshift. Therefore if we get a `TEXT` data type
            without precision we convert it to `VARCHAR(max)` and if it does have precision then we just convert
            `TEXT` to `VARCHAR`.
            """
            if expression.is_type("text"):
                expression.set("this", exp.DType.VARCHAR)
                precision = expression.args.get("expressions")

                if not precision:
                    expression.append("expressions", exp.var("MAX"))

            return super().datatype_sql(expression)

        def alterset_sql(self, expression: exp.AlterSet) -> str:
            exprs = self.expressions(expression, flat=True)
            exprs = f" TABLE PROPERTIES ({exprs})" if exprs else ""
            location = self.sql(expression, "location")
            location = f" LOCATION {location}" if location else ""
            file_format = self.expressions(expression, key="file_format", flat=True, sep=" ")
            file_format = f" FILE FORMAT {file_format}" if file_format else ""

            return f"SET{exprs}{location}{file_format}"

        def array_sql(self, expression: exp.Array) -> str:
            if expression.args.get("bracket_notation"):
                return super().array_sql(expression)

            return rename_func("ARRAY")(self, expression)

        def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
            return Generator.ignorenulls_sql(self, expression)

        def respectnulls_sql(self, expression: exp.RespectNulls) -> str:
            return Generator.respectnulls_sql(self, expression)

        def explode_sql(self, expression: exp.Explode) -> str:
            self.unsupported("Unsupported EXPLODE() function")
            return ""

        def _unix_to_time_sql(self, expression: exp.UnixToTime) -> str:
            scale = expression.args.get("scale")
            this = self.sql(expression.this)

            if scale is not None and scale != exp.UnixToTime.SECONDS and scale.is_int:
                this = f"({this} / POWER(10, {scale.to_py()}))"

            return f"(TIMESTAMP 'epoch' + {this} * INTERVAL '1 SECOND')"
