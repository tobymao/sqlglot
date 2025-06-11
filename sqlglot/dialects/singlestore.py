from collections import defaultdict

from sqlglot import Dialect, generator, Tokenizer, TokenType, tokens
from sqlglot.dialects.dialect import NormalizationStrategy, no_ilike_sql, \
    bool_xor_sql, rename_func, count_if_to_sum, \
    time_format
import typing as t
import re
from sqlglot import exp
from sqlglot.generator import ESCAPED_UNICODE_RE, unsupported_args
from sqlglot.helper import csv


class SingleStore(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    DPIPE_IS_STRING_CONCAT = False
    SUPPORTS_USER_DEFINED_TYPES = False
    SUPPORTS_SEMI_ANTI_JOIN = False
    SAFE_DIVISION = True
    TIME_FORMAT = "'%Y-%m-%d %T'"

    TIME_MAPPING: t.Dict[str, str] = {
        "%Y": "%Y",
        "%y": "%-y",
        "%j": "%j",
        "%b": "%b",
        "%M": "%B",
        "%m": "%m",
        "%c": "%-m",
        "%d": "%d",
        "%e": "%-d",
        "%H": "%H",
        "%h": "%I",
        "%I": "%I",
        "%k": "%-H",
        "%l": "%-I",
        "%i": "%M",
        "%S": "%S",
        "%s": "%S",
        "%f": "%f",
        "%p": "%p",
        "%r": "%H:%i:%S %p",
        "%T": "%H:%i:%S",
        "%U": "%U",
        "%u": "%W",
        "%W": "%A",
        "%w": "%w",
        "%a": "%a",
        "%%": "%%"
    }

    FORCE_EARLY_ALIAS_REF_EXPANSION = True
    SUPPORTS_ORDER_BY_ALL = True
    PROMOTE_TO_INFERRED_DATETIME_TYPE = True

    CREATABLE_KIND_MAPPING: dict[str, str] = {
        "DATABASE": "SCHEMA"
    }

    class Tokenizer(tokens.Tokenizer):
        BIT_STRINGS = [("b'", "'"), ("B'", "'"), ("0b", "")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", "")]
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]
        IDENTIFIERS = ['`', '"']
        QUOTES = ["'", '"']
        STRING_ESCAPES = ["'", '"', "\\"]
        COMMENTS = ["--", "#", ("/*", "*/")]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "@@": TokenType.SESSION_PARAMETER,
            "YEAR": TokenType.YEAR,
            "BSON": TokenType.JSONB,
            "GEOGRAPHYPOINT": TokenType.GEOGRAPHY,
            "IGNORE": TokenType.IGNORE,
            "KEY": TokenType.KEY,
            "START": TokenType.BEGIN
        }

        COMMANDS = {*tokens.Tokenizer.COMMANDS, TokenType.REPLACE} - {
            TokenType.SHOW}

    # TODO: implement
    # class Parser(parser.Parser):

    class Generator(generator.Generator):
        LOCKING_READS_SUPPORTED = True
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        MATCHED_BY_SOURCE = False
        INTERVAL_ALLOWS_PLURAL_FORM = False
        LIMIT_FETCH = "LIMIT"
        LIMIT_ONLY_LITERALS = True
        JOIN_HINTS = False
        DUPLICATE_KEY_UPDATE_WITH_SET = False
        NVL2_SUPPORTED = False
        VALUES_AS_TABLE = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        PAD_FILL_PATTERN_IS_REQUIRED = True
        SUPPORTS_TABLE_ALIAS_COLUMNS = False
        SUPPORTED_JSON_PATH_PARTS = {exp.JSONPathKey}
        SET_OP_MODIFIERS = False
        TRY_SUPPORTED = False
        SUPPORTS_UESCAPE = False
        WITH_PROPERTIES_PREFIX = " "
        SUPPORTS_CONVERT_TIMEZONE = True
        SUPPORTS_UNIX_SECONDS = True
        JSON_KEY_VALUE_PAIR_SEP = ","
        AGGREGATE_FILTER_SUPPORTED = False
        QUERY_HINTS = False

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.NullSafeEQ: lambda self, e: self.binary(e, "<=>"),
            exp.NullSafeNEQ: lambda self, e: f"NOT {self.binary(e, '<=>')}",
            exp.JSONArrayContains: lambda self, e: self.func(
                "JSON_ARRAY_CONTAINS_JSON", e.expression, e.this),
            exp.ILike: no_ilike_sql,
            exp.Xor: bool_xor_sql,
            exp.IntDiv: lambda self, e: f"{self.binary(e, 'DIV')}",
            exp.RegexpLike: lambda self, e: self.binary(e, "RLIKE"),
            exp.Hll: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.CountIf: count_if_to_sum,
            exp.LogicalOr: lambda self, e: f"MAX(ABS({self.sql(e, 'this')}))",
            exp.LogicalAnd: lambda self, e: f"MIN(ABS({self.sql(e, 'this')}))",
            exp.ApproxQuantile: rename_func("APPROX_PERCENTILE"),
            exp.Variance: rename_func("VAR_SAMP"),
            exp.VariancePop: rename_func("VAR_POP"),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.Chr: rename_func("CHAR"),
            exp.Contains: rename_func("INSTR"),
            exp.CurrentSchema: rename_func("SCHEMA"),
            exp.DateBin: rename_func("TIME_BUCKET"),
            exp.DatetimeAdd: rename_func("DATE_ADD"),
            exp.DatetimeSub: rename_func("DATE_SUB"),
            exp.DatetimeDiff: rename_func("TIMESTAMPDIFF"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfWeekIso: lambda self,
                                     e: f"(({self.func('DAYOFWEEK', e.this)} % 7) + 1)",
            exp.DayOfMonth: rename_func("DAY"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
            exp.TimestampAdd: rename_func("DATE_ADD"),
            exp.TimestampSub: rename_func("DATE_SUB"),
            exp.TimeAdd: rename_func("DATE_ADD"),
            exp.TimeSub: rename_func("DATE_SUB"),
            exp.TimeDiff: rename_func("TIMESTAMPDIFF"),
            exp.DateToDi: lambda self,
                                 e: f"(DATE_FORMAT({self.sql(e, 'this')}, {SingleStore.DATEINT_FORMAT}) :> INT)",
            exp.DiToDate: lambda self,
                                 e: f"STR_TO_DATE({self.sql(e, 'this')}, {SingleStore.DATEINT_FORMAT})",
            exp.LowerHex: lambda self, e: f"LOWER(HEX({self.sql(e, 'this')}))",
            exp.IsAscii: lambda self,
                                e: f"({self.sql(e, 'this')} RLIKE '^[\x00-\x7F]*$')",
            exp.Int64: lambda self, e: f"{self.sql(e, 'this')} :> BIGINT",
            exp.JSONFormat: rename_func("JSON_PRETTY"),
            exp.MD5Digest: lambda self, e: self.func("UNHEX",
                                                     self.func("MD5", e.this)),
            exp.AddMonths: lambda self,
                                  e: f"TIMESTAMPADD(MONTH, {self.sql(e, 'expression')}, {self.sql(e, 'this')})",
            exp.RegexpExtract: unsupported_args("group")(
                rename_func("REGEXP_SUBSTR")),
            exp.RegexpExtractAll: unsupported_args("position", "occurrence",
                                                   "group")(
                rename_func("REGEXP_MATCH")),
            exp.Repeat: lambda self,
                               e: f"LPAD('', LENGTH({self.sql(e, 'this')}) * {self.sql(e, 'times')}, {self.sql(e, 'this')})",
            exp.StartsWith: lambda self,
                                   e: f"REGEXP_INSTR({self.sql(e, 'this')}, CONCAT('^', {self.sql(e, 'expression')}))",
            exp.StrToDate: unsupported_args("safe")(rename_func("STR_TO_DATE")),
            exp.StrToTime: unsupported_args("safe", "zone")(
                rename_func("STR_TO_DATE")),
            exp.StrToUnix: unsupported_args("format")(
                rename_func("UNIX_TIMESTAMP")),
            exp.NumberToStr: unsupported_args("culture")(rename_func("FORMAT")),
            exp.FromBase: lambda self, e: self.func("CONV", e.this,
                                                    e.expression,
                                                    exp.Literal.number(10)),
            exp.Time: unsupported_args("zone")(
                lambda self, e: f"{self.sql(e, 'this')} :> TIME"),
            exp.TimeToStr: unsupported_args("zone", "culture")
            (lambda self,
                    e: f"DATE_FORMAT({self.sql(e, 'this')} :> TIME, {self.sql(e, 'format')})"),
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeStrToDate: lambda self, e: self.sql(
                exp.cast(e.this, exp.DataType.Type.DATE)),
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TsOrDsAdd: lambda self, e: self.func("DATE_ADD", e.this,
                                                     e.expression),
            exp.TsOrDsDiff: rename_func("TIMESTAMPDIFF"),
            exp.TsOrDsToDate: unsupported_args("format", "safe")
            (lambda self, e: self.sql(
                exp.cast(e.this, exp.DataType.Type.DATE))),
            exp.TsOrDsToDatetime: lambda self, e: self.sql(
                exp.cast(e.this, exp.DataType.Type.DATETIME)),
            exp.TsOrDsToTime: unsupported_args("format", "safe")(
                lambda self, e: self.sql(
                    exp.cast(e.this, exp.DataType.Type.TIME))),
            exp.TsOrDsToTimestamp: lambda self, e: self.sql(
                exp.cast(e.this, exp.DataType.Type.TIMESTAMP)),
            exp.TsOrDiToDi: lambda self,
                                   e: f"(DATE_FORMAT({self.sql(e, 'this')}, {SingleStore.DATEINT_FORMAT}) :> INT)",
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, time_format("singlestore")(self, e)
            ),
            exp.UnixToTime: unsupported_args("scale", "zone", "hours",
                                             "minutes", "format")(
                rename_func("FROM_UNIXTIME")),
            exp.UnixToTimeStr: lambda self,
                                      e: f"FROM_UNIXTIME({self.sql(e, 'this')}) :> TEXT",
            exp.UnixSeconds: rename_func("UNIX_TIMESTAMP"),
            exp.FromTimeZone: lambda self, e: self.func(
                "CONVERT_TZ", e.this, e.args.get("zone"), "'UTC'"
            ),
        }

        TRANSFORMS.pop(exp.Operator)
        TRANSFORMS.pop(exp.ArrayContainsAll)
        TRANSFORMS.pop(exp.ArrayOverlaps)
        TRANSFORMS.pop(exp.ConnectByRoot)
        TRANSFORMS.pop(exp.JSONPathFilter)
        TRANSFORMS.pop(exp.JSONPathKey)
        TRANSFORMS.pop(exp.JSONPathRecursive)
        TRANSFORMS.pop(exp.JSONPathRoot)
        TRANSFORMS.pop(exp.JSONPathScript)
        TRANSFORMS.pop(exp.JSONPathSelector)
        TRANSFORMS.pop(exp.JSONPathSlice)
        TRANSFORMS.pop(exp.JSONPathSubscript)
        TRANSFORMS.pop(exp.JSONPathUnion)
        TRANSFORMS.pop(exp.JSONPathWildcard)
        TRANSFORMS.pop(exp.ToMap)
        TRANSFORMS.pop(exp.VarMap)
        TRANSFORMS.pop(exp.SwapTable)
        TRANSFORMS.pop(exp.CaseSpecificColumnConstraint)
        TRANSFORMS.pop(exp.ClusteredColumnConstraint)
        TRANSFORMS.pop(exp.DateFormatColumnConstraint)
        TRANSFORMS.pop(exp.EncodeColumnConstraint)
        TRANSFORMS.pop(exp.ExcludeColumnConstraint)
        TRANSFORMS.pop(exp.EphemeralColumnConstraint)
        TRANSFORMS.pop(exp.UppercaseColumnConstraint)
        TRANSFORMS.pop(exp.PathColumnConstraint)
        TRANSFORMS.pop(exp.ProjectionPolicyColumnConstraint)
        TRANSFORMS.pop(exp.InlineLengthColumnConstraint)
        TRANSFORMS.pop(exp.NonClusteredColumnConstraint)
        TRANSFORMS.pop(exp.NotForReplicationColumnConstraint)
        TRANSFORMS.pop(exp.OnUpdateColumnConstraint)
        TRANSFORMS.pop(exp.TitleColumnConstraint)
        TRANSFORMS.pop(exp.Tags)
        TRANSFORMS.pop(exp.WithOperator)
        TRANSFORMS.pop(exp.AllowedValuesProperty)
        TRANSFORMS.pop(exp.IntervalSpan)
        TRANSFORMS.pop(exp.PivotAny)
        TRANSFORMS.pop(exp.Stream)
        TRANSFORMS.pop(exp.AnalyzeColumns)
        TRANSFORMS.pop(exp.WithSchemaBindingProperty)
        TRANSFORMS.pop(exp.ViewAttributeProperty)

        UNSIGNED_TYPE_MAPPING = {
            exp.DataType.Type.UBIGINT: "BIGINT",
            exp.DataType.Type.UINT: "INT",
            exp.DataType.Type.UMEDIUMINT: "MEDIUMINT",
            exp.DataType.Type.USMALLINT: "SMALLINT",
            exp.DataType.Type.UTINYINT: "TINYINT",
            exp.DataType.Type.UDECIMAL: "DECIMAL",
            exp.DataType.Type.UDOUBLE: "DOUBLE",
        }

        UNSUPPORTED_TYPE_MAPPING = {
            exp.DataType.Type.ARRAY: "TEXT",
            exp.DataType.Type.AGGREGATEFUNCTION: "TEXT",
            exp.DataType.Type.SIMPLEAGGREGATEFUNCTION: "TEXT",
            exp.DataType.Type.BIGSERIAL: "BIGINT AUTO_INCREMENT KEY",
            exp.DataType.Type.BPCHAR: "TEXT",
            exp.DataType.Type.DATEMULTIRANGE: "TEXT",
            exp.DataType.Type.DATERANGE: "TEXT",
            exp.DataType.Type.DYNAMIC: "TEXT",
            exp.DataType.Type.HLLSKETCH: "TEXT",
            exp.DataType.Type.HSTORE: "TEXT",
            exp.DataType.Type.IMAGE: "LONGBLOB",
            exp.DataType.Type.INET: "TEXT",
            exp.DataType.Type.INT128: "DECIMAL(39,0)",
            exp.DataType.Type.INT256: "DECIMAL(65,0)",
            exp.DataType.Type.INT4MULTIRANGE: "TEXT",
            exp.DataType.Type.INT4RANGE: "TEXT",
            exp.DataType.Type.INT8MULTIRANGE: "TEXT",
            exp.DataType.Type.INT8RANGE: "TEXT",
            exp.DataType.Type.INTERVAL: "TEXT",
            exp.DataType.Type.IPADDRESS: "TEXT",
            exp.DataType.Type.IPPREFIX: "TEXT",
            exp.DataType.Type.IPV4: "TEXT",
            exp.DataType.Type.IPV6: "TEXT",
            exp.DataType.Type.LIST: "TEXT",
            exp.DataType.Type.MAP: "TEXT",
            exp.DataType.Type.LOWCARDINALITY: "TEXT",  # TODO: replace LOWCARDINALITY with underlying data type
            exp.DataType.Type.MONEY: "DECIMAL(15,4)",
            exp.DataType.Type.NAME: "TEXT",
            exp.DataType.Type.NESTED: "TEXT",
            exp.DataType.Type.NOTHING: "INT",
            exp.DataType.Type.NULL: "INT",
            exp.DataType.Type.NUMMULTIRANGE: "TEXT",
            exp.DataType.Type.NUMRANGE: "TEXT",
            exp.DataType.Type.OBJECT: "BLOB",
            exp.DataType.Type.RANGE: "TEXT",
            exp.DataType.Type.ROWVERSION: "BIGINT AUTO_INCREMENT KEY",
            exp.DataType.Type.SERIAL: "BIGINT AUTO_INCREMENT KEY",
            exp.DataType.Type.SMALLSERIAL: "BIGINT AUTO_INCREMENT KEY",
            exp.DataType.Type.SMALLMONEY: "DECIMAL(6,4)",
            exp.DataType.Type.STRUCT: "TEXT",
            exp.DataType.Type.SUPER: "TEXT",
            exp.DataType.Type.TIMETZ: "TIME",
            exp.DataType.Type.TIMESTAMPNTZ: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP_NS: "TIMESTAMP(6)",
            exp.DataType.Type.TSMULTIRANGE: "TEXT",
            exp.DataType.Type.TSRANGE: "TEXT",
            exp.DataType.Type.TSTZMULTIRANGE: "TEXT",
            exp.DataType.Type.TSTZRANGE: "TEXT",
            exp.DataType.Type.UINT128: "DECIMAL(39,0)",
            exp.DataType.Type.UINT256: "DECIMAL(65,0)",
            exp.DataType.Type.UNION: "TEXT",
            exp.DataType.Type.UNKNOWN: "TEXT",
            exp.DataType.Type.USERDEFINED: "TEXT",
            exp.DataType.Type.UUID: "TEXT",
            exp.DataType.Type.VARIANT: "TEXT",
            exp.DataType.Type.XML: "TEXT",
            exp.DataType.Type.TDIGEST: "BLOB",
        }

        TYPE_MAPPING = {
            **UNSIGNED_TYPE_MAPPING,
            exp.DataType.Type.BIGDECIMAL: "DECIMAL",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.BINARY: "BINARY",
            exp.DataType.Type.BIT: "BOOLEAN",
            exp.DataType.Type.BLOB: "BLOB",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.CHAR: "CHAR",
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.DATE32: "DATE",
            exp.DataType.Type.DATETIME: "DATETIME",
            exp.DataType.Type.DATETIME2: "DATETIME",
            exp.DataType.Type.DATETIME64: "DATETIME",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.ENUM: "ENUM",
            exp.DataType.Type.ENUM8: "ENUM",
            exp.DataType.Type.ENUM16: "ENUM",
            exp.DataType.Type.FIXEDSTRING: "TEXT",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.GEOGRAPHY: "GEOGRAPHY",
            exp.DataType.Type.GEOMETRY: "GEOGRAPHY",
            exp.DataType.Type.POINT: "GEOGRAPHYPOINT",
            exp.DataType.Type.RING: "GEOGRAPHY",
            exp.DataType.Type.LINESTRING: "GEOGRAPHY",
            exp.DataType.Type.MULTILINESTRING: "GEOGRAPHY",
            exp.DataType.Type.POLYGON: "GEOGRAPHY",
            exp.DataType.Type.MULTIPOLYGON: "GEOGRAPHY",
            exp.DataType.Type.INT: "INT",
            exp.DataType.Type.JSON: "JSON",
            exp.DataType.Type.JSONB: "BSON",
            exp.DataType.Type.LONGBLOB: "LONGBLOB",
            exp.DataType.Type.LONGTEXT: "LONGTEXT",
            exp.DataType.Type.MEDIUMBLOB: "MEDIUMBLOB",
            exp.DataType.Type.MEDIUMINT: "MEDIUMINT",
            exp.DataType.Type.MEDIUMTEXT: "MEDIUMTEXT",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.SET: "SET",
            exp.DataType.Type.SMALLDATETIME: "DATETIME",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.TEXT: "TEXT",
            exp.DataType.Type.TINYBLOB: "TINYBLOB",
            exp.DataType.Type.TINYTEXT: "TINYTEXT",
            exp.DataType.Type.TIME: "TIME",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP_S: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP_MS: "TIMESTAMP(6)",
            exp.DataType.Type.TINYINT: "TINYINT",
            exp.DataType.Type.UNICHAR: "CHAR",
            exp.DataType.Type.UNIVARCHAR: "VARCHAR",
            exp.DataType.Type.VARBINARY: "VARBINARY",
            exp.DataType.Type.VARCHAR: "VARCHAR",
            exp.DataType.Type.VECTOR: "VECTOR",
            exp.DataType.Type.YEAR: "YEAR",
        }

        PROPERTIES_LOCATION = {
            exp.AllowedValuesProperty: exp.Properties.Location.UNSUPPORTED,
            exp.AlgorithmProperty: exp.Properties.Location.UNSUPPORTED,
            exp.AutoIncrementProperty: exp.Properties.Location.POST_SCHEMA,
            exp.AutoRefreshProperty: exp.Properties.Location.UNSUPPORTED,
            exp.BackupProperty: exp.Properties.Location.UNSUPPORTED,
            exp.BlockCompressionProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CharacterSetProperty: exp.Properties.Location.POST_SCHEMA,
            exp.ChecksumProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CollateProperty: exp.Properties.Location.POST_SCHEMA,
            exp.CopyGrantsProperty: exp.Properties.Location.UNSUPPORTED,
            exp.Cluster: exp.Properties.Location.UNSUPPORTED,
            exp.ClusteredByProperty: exp.Properties.Location.UNSUPPORTED,
            exp.DistributedByProperty: exp.Properties.Location.UNSUPPORTED,
            exp.DuplicateKeyProperty: exp.Properties.Location.UNSUPPORTED,
            exp.DataBlocksizeProperty: exp.Properties.Location.UNSUPPORTED,
            exp.DataDeletionProperty: exp.Properties.Location.UNSUPPORTED,
            exp.DefinerProperty: exp.Properties.Location.POST_SCHEMA,
            exp.DictRange: exp.Properties.Location.UNSUPPORTED,
            exp.DictProperty: exp.Properties.Location.UNSUPPORTED,
            exp.DynamicProperty: exp.Properties.Location.UNSUPPORTED,
            # TODO: replace with SHARD KEY
            exp.DistKeyProperty: exp.Properties.Location.UNSUPPORTED,
            exp.DistStyleProperty: exp.Properties.Location.UNSUPPORTED,
            exp.EmptyProperty: exp.Properties.Location.UNSUPPORTED,
            exp.EncodeProperty: exp.Properties.Location.UNSUPPORTED,
            exp.EngineProperty: exp.Properties.Location.UNSUPPORTED,
            exp.ExecuteAsProperty: exp.Properties.Location.UNSUPPORTED,
            exp.ExternalProperty: exp.Properties.Location.POST_CREATE,
            exp.FallbackProperty: exp.Properties.Location.UNSUPPORTED,
            exp.FileFormatProperty: exp.Properties.Location.UNSUPPORTED,
            exp.FreespaceProperty: exp.Properties.Location.UNSUPPORTED,
            exp.GlobalProperty: exp.Properties.Location.UNSUPPORTED,
            exp.HeapProperty: exp.Properties.Location.UNSUPPORTED,
            exp.InheritsProperty: exp.Properties.Location.UNSUPPORTED,
            exp.IcebergProperty: exp.Properties.Location.UNSUPPORTED,
            exp.IncludeProperty: exp.Properties.Location.UNSUPPORTED,
            exp.InputModelProperty: exp.Properties.Location.UNSUPPORTED,
            exp.IsolatedLoadingProperty: exp.Properties.Location.UNSUPPORTED,
            exp.JournalProperty: exp.Properties.Location.UNSUPPORTED,
            exp.LanguageProperty: exp.Properties.Location.UNSUPPORTED,
            exp.LikeProperty: exp.Properties.Location.POST_SCHEMA,
            exp.LocationProperty: exp.Properties.Location.UNSUPPORTED,
            exp.LockProperty: exp.Properties.Location.UNSUPPORTED,
            exp.LockingProperty: exp.Properties.Location.UNSUPPORTED,
            exp.LogProperty: exp.Properties.Location.UNSUPPORTED,
            exp.MaterializedProperty: exp.Properties.Location.UNSUPPORTED,
            exp.MergeBlockRatioProperty: exp.Properties.Location.UNSUPPORTED,
            exp.NoPrimaryIndexProperty: exp.Properties.Location.UNSUPPORTED,
            exp.OnProperty: exp.Properties.Location.UNSUPPORTED,
            exp.OnCommitProperty: exp.Properties.Location.UNSUPPORTED,
            exp.Order: exp.Properties.Location.UNSUPPORTED,
            exp.OutputModelProperty: exp.Properties.Location.UNSUPPORTED,
            exp.PartitionedByProperty: exp.Properties.Location.UNSUPPORTED,
            exp.PartitionedOfProperty: exp.Properties.Location.UNSUPPORTED,
            # TODO: Move PK into Schema
            exp.PrimaryKey: exp.Properties.Location.UNSUPPORTED,
            exp.Property: exp.Properties.Location.UNSUPPORTED,
            exp.RemoteWithConnectionModelProperty: exp.Properties.Location.UNSUPPORTED,
            exp.ReturnsProperty: exp.Properties.Location.POST_SCHEMA,
            exp.RowFormatProperty: exp.Properties.Location.UNSUPPORTED,
            exp.RowFormatDelimitedProperty: exp.Properties.Location.UNSUPPORTED,
            exp.RowFormatSerdeProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SampleProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SchemaCommentProperty: exp.Properties.Location.POST_SCHEMA,
            exp.SecureProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SecurityProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SerdeProperties: exp.Properties.Location.UNSUPPORTED,
            exp.Set: exp.Properties.Location.UNSUPPORTED,
            exp.SettingsProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SetProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SetConfigProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SharingProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SequenceProperties: exp.Properties.Location.UNSUPPORTED,
            # TODO: Move into schema
            exp.SortKeyProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SqlReadWriteProperty: exp.Properties.Location.UNSUPPORTED,
            exp.SqlSecurityProperty: exp.Properties.Location.UNSUPPORTED,
            exp.StabilityProperty: exp.Properties.Location.UNSUPPORTED,
            exp.StorageHandlerProperty: exp.Properties.Location.UNSUPPORTED,
            exp.StreamingTableProperty: exp.Properties.Location.UNSUPPORTED,
            exp.StrictProperty: exp.Properties.Location.UNSUPPORTED,
            exp.Tags: exp.Properties.Location.UNSUPPORTED,
            exp.TemporaryProperty: exp.Properties.Location.POST_CREATE,
            exp.ToTableProperty: exp.Properties.Location.UNSUPPORTED,
            exp.TransientProperty: exp.Properties.Location.UNSUPPORTED,
            exp.TransformModelProperty: exp.Properties.Location.UNSUPPORTED,
            exp.MergeTreeTTL: exp.Properties.Location.UNSUPPORTED,
            exp.UnloggedProperty: exp.Properties.Location.UNSUPPORTED,
            exp.UsingTemplateProperty: exp.Properties.Location.UNSUPPORTED,
            exp.ViewAttributeProperty: exp.Properties.Location.POST_CREATE,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
            exp.WithDataProperty: exp.Properties.Location.UNSUPPORTED,
            exp.WithJournalTableProperty: exp.Properties.Location.UNSUPPORTED,
            exp.WithProcedureOptions: exp.Properties.Location.UNSUPPORTED,
            exp.WithSchemaBindingProperty: exp.Properties.Location.POST_CREATE,
            exp.WithSystemVersioningProperty: exp.Properties.Location.UNSUPPORTED,
            exp.ForceProperty: exp.Properties.Location.UNSUPPORTED,
        }

        # https://docs.singlestore.com/cloud/reference/sql-reference/restricted-keywords/list-of-restricted-keywords/
        RESERVED_KEYWORDS = {
            "ABORT",
            "ABS",
            "ABSOLUTE",
            "ACCESS",
            "ACCOUNT",
            "ACOS",
            "ACTION",
            "ADD",
            "ADDDATE",
            "ADDTIME",
            "ADMIN",
            "AES_DECRYPT",
            "AES_ENCRYPT",
            "AFTER",
            "AGAINST",
            "AGGREGATE",
            "AGGREGATES",
            "AGGREGATOR",
            "AGGREGATOR_ID",
            "AGGREGATOR_PLAN_HASH",
            "AGGREGATORS",
            "ALGORITHM",
            "ALL",
            "ALSO",
            "ALTER",
            "ALWAYS",
            "ANALYSE",
            "ANALYZE",
            "AND",
            "ANTI_JOIN",
            "ANY",
            "ANY_VALUE",
            "APPROX_COUNT_DISTINCT",
            "APPROX_COUNT_DISTINCT_ACCUMULATE",
            "APPROX_COUNT_DISTINCT_COMBINE",
            "APPROX_COUNT_DISTINCT_ESTIMATE",
            "APPROX_GEOGRAPHY_INTERSECTS",
            "APPROX_PERCENTILE",
            "ARGHISTORY",
            "ARRANGE",
            "ARRANGEMENT",
            "ARRAY",
            "AS",
            "ASC",
            "ASCII",
            "ASENSITIVE",
            "ASIN",
            "ASM",
            "ASSERTION",
            "ASSIGNMENT",
            "AST",
            "ASYMMETRIC",
            "ASYNC",
            "AT",
            "ATAN",
            "ATAN2",
            "ATTACH",
            "ATTRIBUTE",
            "AUTHORIZATION",
            "AUTO",
            "AUTO_INCREMENT",
            "AUTO_REPROVISION",
            "AUTOSTATS",
            "AUTOSTATS_CARDINALITY_MODE",
            "AUTOSTATS_ENABLED",
            "AUTOSTATS_HISTOGRAM_MODE",
            "AUTOSTATS_SAMPLING",
            "AVAILABILITY",
            "AVG",
            "AVG_ROW_LENGTH",
            "AVRO",
            "AZURE",
            "BACKGROUND",
            "_BACKGROUND_THREADS_FOR_CLEANUP",
            "BACKUP",
            "BACKUP_HISTORY",
            "BACKUP_ID",
            "BACKWARD",
            "BATCH",
            "BATCHES",
            "BATCH_INTERVAL",
            "_BATCH_SIZE_LIMIT",
            "BEFORE",
            "BEGIN",
            "BETWEEN",
            "BIGINT",
            "BIN",
            "BINARY",
            "_BINARY",
            "BIT",
            "BIT_AND",
            "BIT_COUNT",
            "BIT_OR",
            "BIT_XOR",
            "BLOB",
            "BOOL",
            "BOOLEAN",
            "BOOTSTRAP",
            "BOTH",
            "_BT",
            "BTREE",
            "BUCKET_COUNT",
            "BY",
            "BYTE",
            "BYTE_LENGTH",
            "CACHE",
            "CALL",
            "CALL_FOR_PIPELINE",
            "CALLED",
            "CAPTURE",
            "CASCADE",
            "CASCADED",
            "CASE",
            "CAST",
            "CATALOG",
            "CEIL",
            "CEILING",
            "CHAIN",
            "CHANGE",
            "CHAR",
            "CHARACTER",
            "CHARACTERISTICS",
            "CHARACTER_LENGTH",
            "CHAR_LENGTH",
            "CHARSET",
            "CHECK",
            "CHECKPOINT",
            "_CHECK_CAN_CONNECT",
            "_CHECK_CONSISTENCY",
            "CHECKSUM",
            "_CHECKSUM",
            "CLASS",
            "CLEAR",
            "CLIENT",
            "CLIENT_FOUND_ROWS",
            "CLOSE",
            "CLUSTER",
            "CLUSTERED",
            "CNF",
            "COALESCE",
            "COERCIBILITY",
            "COLLATE",
            "COLLATION",
            "COLLECT",
            "COLUMN",
            "COLUMNAR",
            "COLUMNS",
            "COLUMNSTORE",
            "COLUMNSTORE_SEGMENT_ROWS",
            "COMMENT",
            "COMMENTS",
            "COMMIT",
            "COMMITTED",
            "_COMMIT_LOG_TAIL",
            "COMMITTED",
            "COMPACT",
            "COMPILE",
            "COMPRESSED",
            "COMPRESSION",
            "CONCAT",
            "CONCAT_WS",
            "CONCURRENT",
            "CONCURRENTLY",
            "CONDITION",
            "CONFIGURATION",
            "CONNECTION",
            "CONNECTION_ID",
            "CONNECTIONS",
            "CONFIG",
            "CONSTRAINT",
            "CONSTRAINTS",
            "CONTENT",
            "CONTINUE",
            "_CONTINUE_REPLAY",
            "CONV",
            "CONVERSION",
            "CONVERT",
            "CONVERT_TZ",
            "COPY",
            "_CORE",
            "COS",
            "COST",
            "COT",
            "COUNT",
            "CREATE",
            "CREDENTIALS",
            "CROSS",
            "CUBE",
            "CSV",
            "CUME_DIST",
            "CURDATE",
            "CURRENT",
            "CURRENT_CATALOG",
            "CURRENT_DATE",
            "CURRENT_ROLE",
            "CURRENT_SCHEMA",
            "CURRENT_SECURITY_GROUPS",
            "CURRENT_SECURITY_ROLES",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_USER",
            "CURSOR",
            "CURTIME",
            "CYCLE",
            "DATA",
            "DATABASE",
            "DATABASES",
            "DATE",
            "DATE_ADD",
            "DATEDIFF",
            "DATE_FORMAT",
            "DATE_SUB",
            "DATE_TRUNC",
            "DATETIME",
            "DAY",
            "DAY_HOUR",
            "DAY_MICROSECOND",
            "DAY_MINUTE",
            "DAYNAME",
            "DAYOFMONTH",
            "DAYOFWEEK",
            "DAYOFYEAR",
            "DAY_SECOND",
            "DEALLOCATE",
            "DEC",
            "DECIMAL",
            "DECLARE",
            "DECODE",
            "DEFAULT",
            "DEFAULTS",
            "DEFERRABLE",
            "DEFERRED",
            "DEFINED",
            "DEFINER",
            "DEGREES",
            "DELAYED",
            "DELAY_KEY_WRITE",
            "DELETE",
            "DELIMITER",
            "DELIMITERS",
            "DENSE_RANK",
            "DESC",
            "DESCRIBE",
            "DETACH",
            "DETERMINISTIC",
            "DICTIONARY",
            "DIFFERENTIAL",
            "DIRECTORY",
            "DISABLE",
            "DISCARD",
            "_DISCONNECT",
            "DISK",
            "DISTINCT",
            "DISTINCTROW",
            "DISTRIBUTED_JOINS",
            "DIV",
            "DO",
            "DOCUMENT",
            "DOMAIN",
            "DOT_PRODUCT",
            "DOUBLE",
            "DROP",
            "_DROP_PROFILE",
            "DUAL",
            "DUMP",
            "DUPLICATE",
            "DYNAMIC",
            "EARLIEST",
            "EACH",
            "ECHO",
            "ELECTION",
            "ELSE",
            "ELSEIF",
            "ELT",
            "ENABLE",
            "ENCLOSED",
            "ENCODING",
            "ENCRYPTED",
            "END",
            "ENGINE",
            "ENGINES",
            "ENUM",
            "ERRORS",
            "ESCAPE",
            "ESCAPED",
            "ESTIMATE",
            "EUCLIDEAN_DISTANCE",
            "EVENT",
            "EVENTS",
            "EXCEPT",
            "EXCLUDE",
            "EXCLUDING",
            "EXCLUSIVE",
            "EXECUTE",
            "EXISTS",
            "EXIT",
            "EXP",
            "EXPLAIN",
            "EXTENDED",
            "EXTENSION",
            "EXTERNAL",
            "EXTERNAL_HOST",
            "EXTERNAL_PORT",
            "EXTRACT",
            "EXTRACTOR",
            "EXTRACTORS",
            "EXTRA_JOIN",
            "_FAILOVER",
            "FAILED_LOGIN_ATTEMPTS",
            "FAILURE",
            "FALSE",
            "FAMILY",
            "FAULT",
            "FETCH",
            "FIELD",
            "FIELDS",
            "FILE",
            "FILES",
            "FILL",
            "FIRST",
            "FIRST_VALUE",
            "FIX_ALTER",
            "FIXED",
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
            "FLOOR",
            "FLUSH",
            "FOLLOWING",
            "FOR",
            "FORCE",
            "FORCE_COMPILED_MODE",
            "FORCE_INTERPRETER_MODE",
            "FOREGROUND",
            "FOREIGN",
            "FORMAT",
            "FORWARD",
            "FOUND_ROWS",
            "FREEZE",
            "FROM",
            "FROM_BASE64",
            "FROM_DAYS",
            "FROM_UNIXTIME",
            "FS",
            "_FSYNC",
            "FULL",
            "FULLTEXT",
            "FUNCTION",
            "FUNCTIONS",
            "GC",
            "GCS",
            "GET_FORMAT",
            "_GC",
            "_GCX",
            "GENERATE",
            "GEOGRAPHY",
            "GEOGRAPHY_AREA",
            "GEOGRAPHY_CONTAINS",
            "GEOGRAPHY_DISTANCE",
            "GEOGRAPHY_INTERSECTS",
            "GEOGRAPHY_LATITUDE",
            "GEOGRAPHY_LENGTH",
            "GEOGRAPHY_LONGITUDE",
            "GEOGRAPHYPOINT",
            "GEOGRAPHY_POINT",
            "GEOGRAPHY_WITHIN_DISTANCE",
            "GEOMETRY",
            "GEOMETRY_AREA",
            "GEOMETRY_CONTAINS",
            "GEOMETRY_DISTANCE",
            "GEOMETRY_FILTER",
            "GEOMETRY_INTERSECTS",
            "GEOMETRY_LENGTH",
            "GEOMETRYPOINT",
            "GEOMETRY_POINT",
            "GEOMETRY_WITHIN_DISTANCE",
            "GEOMETRY_X",
            "GEOMETRY_Y",
            "GLOBAL",
            "_GLOBAL_VERSION_TIMESTAMP",
            "GRANT",
            "GRANTED",
            "GRANTS",
            "GREATEST",
            "GROUP",
            "GROUPING",
            "GROUPS",
            "GROUP_CONCAT",
            "GZIP",
            "HANDLE",
            "HANDLER",
            "HARD_CPU_LIMIT_PERCENTAGE",
            "HASH",
            "HAS_TEMP_TABLES",
            "HAVING",
            "HDFS",
            "HEADER",
            "HEARTBEAT_NO_LOGGING",
            "HEX",
            "HIGHLIGHT",
            "HIGH_PRIORITY",
            "HOLD",
            "HOLDING",
            "HOST",
            "HOSTS",
            "HOUR",
            "HOUR_MICROSECOND",
            "HOUR_MINUTE",
            "HOUR_SECOND",
            "IDENTIFIED",
            "IDENTITY",
            "IF",
            "IFNULL",
            "IGNORE",
            "ILIKE",
            "IMMEDIATE",
            "IMMUTABLE",
            "IMPLICIT",
            "IMPORT",
            "IN",
            "INCLUDING",
            "INCREMENT",
            "INCREMENTAL",
            "INDEX",
            "INDEXES",
            "INET_ATON",
            "INET_NTOA",
            "INET6_ATON",
            "INET6_NTOA",
            "INFILE",
            "INHERIT",
            "INHERITS",
            "_INIT_PROFILE",
            "INIT",
            "INITCAP",
            "INITIALIZE",
            "INITIALLY",
            "INJECT",
            "INLINE",
            "INNER",
            "INOUT",
            "INPUT",
            "INSENSITIVE",
            "INSERT",
            "INSERT_METHOD",
            "INSTANCE",
            "INSTEAD",
            "INSTR",
            "INT",
            "INT1",
            "INT2",
            "INT3",
            "INT4",
            "INT8",
            "INTEGER",
            "_INTERNAL_DYNAMIC_TYPECAST",
            "INTERPRETER_MODE",
            "INTERSECT",
            "INTERVAL",
            "INTO",
            "INVOKER",
            "IS",
            "ISNULL",
            "ISOLATION",
            "ITERATE",
            "JOIN",
            "JSON",
            "JSON_AGG",
            "JSON_ARRAY_CONTAINS_DOUBLE",
            "JSON_ARRAY_CONTAINS_JSON",
            "JSON_ARRAY_CONTAINS_STRING",
            "JSON_ARRAY_PUSH_DOUBLE",
            "JSON_ARRAY_PUSH_JSON",
            "JSON_ARRAY_PUSH_STRING",
            "JSON_DELETE_KEY",
            "JSON_EXTRACT_DOUBLE",
            "JSON_EXTRACT_JSON",
            "JSON_EXTRACT_STRING",
            "JSON_EXTRACT_BIGINT",
            "JSON_GET_TYPE",
            "JSON_LENGTH",
            "JSON_SET_DOUBLE",
            "JSON_SET_JSON",
            "JSON_SET_STRING",
            "JSON_SPLICE_DOUBLE",
            "JSON_SPLICE_JSON",
            "JSON_SPLICE_STRING",
            "KAFKA",
            "KEY",
            "KEY_BLOCK_SIZE",
            "KEYS",
            "KILL",
            "KILLALL",
            "LABEL",
            "LAG",
            "LANGUAGE",
            "LARGE",
            "LAST",
            "LAST_DAY",
            "LAST_INSERT_ID",
            "LAST_VALUE",
            "LATERAL",
            "LATEST",
            "LC_COLLATE",
            "LC_CTYPE",
            "LCASE",
            "LEAD",
            "LEADING",
            "LEAF",
            "LEAKPROOF",
            "LEAST",
            "LEAVE",
            "LEAVES",
            "LEFT",
            "LENGTH",
            "LEVEL",
            "LICENSE",
            "LIKE",
            "LIMIT",
            "LINES",
            "LISTEN",
            "LLVM",
            "LN",
            "LOAD",
            "LOADDATA_WHERE",
            "_LOAD",
            "LOCAL",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "LOCATE",
            "LOCATION",
            "LOCK",
            "LOG",
            "LOG10",
            "LOG2",
            "LONG",
            "LONGBLOB",
            "LONGTEXT",
            "LOOP",
            "LOWER",
            "LOW_PRIORITY",
            "LPAD",
            "_LS",
            "LTRIM",
            "LZ4",
            "MANAGEMENT",
            "_MANAGEMENT_THREAD",
            "MAPPING",
            "MASTER",
            "MATCH",
            "MATERIALIZED",
            "MAX",
            "MAXVALUE",
            "MAX_CONCURRENCY",
            "MAX_ERRORS",
            "MAX_PARTITIONS_PER_BATCH",
            "MAX_QUEUE_DEPTH",
            "MAX_RETRIES_PER_BATCH_PARTITION",
            "MAX_ROWS",
            "MBC",
            "MD5",
            "MPL",
            "MEDIAN",
            "MEDIUMBLOB",
            "MEDIUMINT",
            "MEDIUMTEXT",
            "MEMBER",
            "MEMORY",
            "MEMORY_PERCENTAGE",
            "_MEMSQL_TABLE_ID_LOOKUP",
            "MEMSQL",
            "MEMSQL_DESERIALIZE",
            "MEMSQL_IMITATING_KAFKA",
            "MEMSQL_SERIALIZE",
            "MERGE",
            "METADATA",
            "MICROSECOND",
            "MIDDLEINT",
            "MIN",
            "MIN_ROWS",
            "MINUS",
            "MINUTE",
            "MINUTE_MICROSECOND",
            "MINUTE_SECOND",
            "MINVALUE",
            "MOD",
            "MODE",
            "MODEL",
            "MODIFIES",
            "MODIFY",
            "MONTH",
            "MONTHNAME",
            "MONTHS_BETWEEN",
            "MOVE",
            "MPL",
            "NAMES",
            "NAMED",
            "NAMESPACE",
            "NATIONAL",
            "NATURAL",
            "NCHAR",
            "NEXT",
            "NO",
            "NODE",
            "NONE",
            "NO_QUERY_REWRITE",
            "NOPARAM",
            "NOT",
            "NOTHING",
            "NOTIFY",
            "NOW",
            "NOWAIT",
            "NO_WRITE_TO_BINLOG",
            "NO_QUERY_REWRITE",
            "NORELY",
            "NTH_VALUE",
            "NTILE",
            "NULL",
            "NULLCOLS",
            "NULLIF",
            "NULLS",
            "NUMERIC",
            "NVARCHAR",
            "OBJECT",
            "OCTET_LENGTH",
            "OF",
            "OFF",
            "OFFLINE",
            "OFFSET",
            "OFFSETS",
            "OIDS",
            "ON",
            "ONLINE",
            "ONLY",
            "OPEN",
            "OPERATOR",
            "OPTIMIZATION",
            "OPTIMIZE",
            "OPTIMIZER",
            "OPTIMIZER_STATE",
            "OPTION",
            "OPTIONS",
            "OPTIONALLY",
            "OR",
            "ORDER",
            "ORDERED_SERIALIZE",
            "ORPHAN",
            "OUT",
            "OUT_OF_ORDER",
            "OUTER",
            "OUTFILE",
            "OVER",
            "OVERLAPS",
            "OVERLAY",
            "OWNED",
            "OWNER",
            "PACK_KEYS",
            "PAIRED",
            "PARSER",
            "PARQUET",
            "PARTIAL",
            "PARTITION",
            "PARTITION_ID",
            "PARTITIONING",
            "PARTITIONS",
            "PASSING",
            "PASSWORD",
            "PASSWORD_LOCK_TIME",
            "PARSER",
            "PAUSE",
            "_PAUSE_REPLAY",
            "PERCENT_RANK",
            "PERCENTILE_CONT",
            "PERCENTILE_DISC",
            "PERIODIC",
            "PERSISTED",
            "PI",
            "PIPELINE",
            "PIPELINES",
            "PIVOT",
            "PLACING",
            "PLAN",
            "PLANS",
            "PLANCACHE",
            "PLUGINS",
            "POOL",
            "POOLS",
            "PORT",
            "POSITION",
            "POW",
            "POWER",
            "PRECEDING",
            "PRECISION",
            "PREPARE",
            "PREPARED",
            "PRESERVE",
            "PRIMARY",
            "PRIOR",
            "PRIVILEGES",
            "PROCEDURAL",
            "PROCEDURE",
            "PROCEDURES",
            "PROCESS",
            "PROCESSLIST",
            "PROFILE",
            "PROFILES",
            "PROGRAM",
            "PROMOTE",
            "PROXY",
            "PURGE",
            "QUARTER",
            "QUERIES",
            "QUERY",
            "QUERY_TIMEOUT",
            "QUEUE",
            "QUOTE",
            "RADIANS",
            "RAND",
            "RANGE",
            "RANK",
            "READ",
            "_READ",
            "READS",
            "REAL",
            "REASSIGN",
            "REBALANCE",
            "RECHECK",
            "RECORD",
            "RECURSIVE",
            "REDUNDANCY",
            "REDUNDANT",
            "REF",
            "REFERENCE",
            "REFERENCES",
            "REFRESH",
            "REGEXP",
            "REINDEX",
            "RELATIVE",
            "RELEASE",
            "RELOAD",
            "RELY",
            "REMOTE",
            "REMOVE",
            "RENAME",
            "REPAIR",
            "_REPAIR_TABLE",
            "REPEAT",
            "REPEATABLE",
            "_REPL",
            "_REPROVISIONING",
            "REPLACE",
            "REPLICA",
            "REPLICATE",
            "REPLICATING",
            "REPLICATION",
            "DURABILITY",
            "REQUIRE",
            "RESOURCE",
            "RESOURCE_POOL",
            "RESET",
            "RESTART",
            "RESTORE",
            "RESTRICT",
            "RESULT",
            "_RESURRECT",
            "RETRY",
            "RETURN",
            "RETURNING",
            "RETURNS",
            "REVERSE",
            "REVOKE",
            "RG_POOL",
            "RIGHT",
            "RIGHT_ANTI_JOIN",
            "RIGHT_SEMI_JOIN",
            "RIGHT_STRAIGHT_JOIN",
            "RLIKE",
            "ROLE",
            "ROLES",
            "ROLLBACK",
            "ROLLUP",
            "ROUND",
            "ROUTINE",
            "ROW",
            "ROW_COUNT",
            "ROW_FORMAT",
            "ROW_NUMBER",
            "ROWS",
            "ROWSTORE",
            "RULE",
            "RPAD",
            "_RPC",
            "RTRIM",
            "RUNNING",
            "S3",
            "SAFE",
            "SAVE",
            "SAVEPOINT",
            "SCALAR",
            "SCHEMA",
            "SCHEMAS",
            "SCHEMA_BINDING",
            "SCROLL",
            "SEARCH",
            "SECOND",
            "SECOND_MICROSECOND",
            "SEC_TO_TIME",
            "SECURITY",
            "SELECT",
            "SEMI_JOIN",
            "_SEND_THREADS",
            "SENSITIVE",
            "SEPARATOR",
            "SEQUENCE",
            "SEQUENCES",
            "SERIAL",
            "SERIALIZABLE",
            "SERIES",
            "SERVICE_USER",
            "SERVER",
            "SESSION",
            "SESSION_USER",
            "SET",
            "SETOF",
            "SECURITY_LISTS_INTERSECT",
            "SHA",
            "SHA1",
            "SHA2",
            "SHARD",
            "SHARDED",
            "SHARDED_ID",
            "SHARE",
            "SHOW",
            "SHUTDOWN",
            "SIGMOID",
            "SIGN",
            "SIGNAL",
            "SIMILAR",
            "SIMPLE",
            "SITE",
            "SIGNED",
            "SIN",
            "SKIP",
            "SKIPPED_BATCHES",
            "SLEEP",
            "_SLEEP",
            "SMALLINT",
            "SNAPSHOT",
            "_SNAPSHOT",
            "_SNAPSHOTS",
            "SOFT_CPU_LIMIT_PERCENTAGE",
            "SOME",
            "SONAME",
            "SPARSE",
            "SPATIAL",
            "SPATIAL_CHECK_INDEX",
            "SPECIFIC",
            "SPLIT",
            "SQL",
            "SQL_BIG_RESULT",
            "SQL_BUFFER_RESULT",
            "SQL_CACHE",
            "SQL_CALC_FOUND_ROWS",
            "SQLEXCEPTION",
            "SQL_MODE",
            "SQL_NO_CACHE",
            "SQL_NO_LOGGING",
            "SQL_SMALL_RESULT",
            "SQLSTATE",
            "SQLWARNING",
            "SQRT",
            "SSL",
            "STABLE",
            "STANDALONE",
            "START",
            "STARTING",
            "STATE",
            "STATEMENT",
            "STATISTICS",
            "STATS",
            "STATUS",
            "STD",
            "STDDEV",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "STDIN",
            "STDOUT",
            "STOP",
            "STORAGE",
            "STR_TO_DATE",
            "STRAIGHT_JOIN",
            "STRICT",
            "STRING",
            "STRIP",
            "SUBDATE",
            "SUBSTR",
            "SUBSTRING",
            "SUBSTRING_INDEX",
            "SUCCESS",
            "SUM",
            "SUPER",
            "SYMMETRIC",
            "SYNC_SNAPSHOT",
            "SYNC",
            "_SYNC",
            "_SYNC2",
            "_SYNC_PARTITIONS",
            "_SYNC_SNAPSHOT",
            "SYNCHRONIZE",
            "SYSID",
            "SYSTEM",
            "TABLE",
            "TABLE_CHECKSUM",
            "TABLES",
            "TABLESPACE",
            "TAGS",
            "TAN",
            "TARGET_SIZE",
            "TASK",
            "TEMP",
            "TEMPLATE",
            "TEMPORARY",
            "TEMPTABLE",
            "_TERM_BUMP",
            "TERMINATE",
            "TERMINATED",
            "TEST",
            "TEXT",
            "THEN",
            "TIME",
            "TIMEDIFF",
            "TIME_BUCKET",
            "TIME_FORMAT",
            "TIMEOUT",
            "TIMESTAMP",
            "TIMESTAMPADD",
            "TIMESTAMPDIFF",
            "TIMEZONE",
            "TIME_TO_SEC",
            "TINYBLOB",
            "TINYINT",
            "TINYTEXT",
            "TO",
            "TO_BASE64",
            "TO_CHAR",
            "TO_DATE",
            "TO_DAYS",
            "TO_JSON",
            "TO_NUMBER",
            "TO_SECONDS",
            "TO_TIMESTAMP",
            "TRACELOGS",
            "TRADITIONAL",
            "TRAILING",
            "TRANSFORM",
            "TRANSACTION",
            "_TRANSACTIONS_EXPERIMENTAL",
            "TREAT",
            "TRIGGER",
            "TRIGGERS",
            "TRIM",
            "TRUE",
            "TRUNC",
            "TRUNCATE",
            "TRUSTED",
            "TWO_PHASE",
            "_TWOPCID",
            "TYPE",
            "TYPES",
            "UCASE",
            "UNBOUNDED",
            "UNCOMMITTED",
            "UNDEFINED",
            "UNDO",
            "UNENCRYPTED",
            "UNENFORCED",
            "UNHEX",
            "UNHOLD",
            "UNICODE",
            "UNION",
            "UNIQUE",
            "_UNITTEST",
            "UNIX_TIMESTAMP",
            "UNKNOWN",
            "UNLISTEN",
            "_UNLOAD",
            "UNLOCK",
            "UNLOGGED",
            "UNPIVOT",
            "UNSIGNED",
            "UNTIL",
            "UPDATE",
            "UPGRADE",
            "UPPER",
            "USAGE",
            "USE",
            "USER",
            "USERS",
            "USING",
            "UTC_DATE",
            "UTC_TIME",
            "UTC_TIMESTAMP",
            "_UTF8",
            "VACUUM",
            "VALID",
            "VALIDATE",
            "VALIDATOR",
            "VALUE",
            "VALUES",
            "VARBINARY",
            "VARCHAR",
            "VARCHARACTER",
            "VARIABLES",
            "VARIADIC",
            "VARIANCE",
            "VAR_POP",
            "VAR_SAMP",
            "VARYING",
            "VECTOR_SUB",
            "VERBOSE",
            "VERSION",
            "VIEW",
            "VOID",
            "VOLATILE",
            "VOTING",
            "WAIT",
            "_WAKE",
            "WARNINGS",
            "WEEK",
            "WEEKDAY",
            "WEEKOFYEAR",
            "WHEN",
            "WHERE",
            "WHILE",
            "WHITESPACE",
            "WINDOW",
            "WITH",
            "WITHOUT",
            "WITHIN",
            "_WM_HEARTBEAT",
            "WORK",
            "WORKLOAD",
            "WRAPPER",
            "WRITE",
            "XACT_ID",
            "XOR",
            "YEAR",
            "YEAR_MONTH",
            "YES",
            "ZEROFILL",
            "ZONE"
        }

        def all_sql(self, expression: exp.All) -> str:
            self.unsupported(
                "ALL subquery predicate is not supported in SingleStore")
            return super().all_sql(expression)

        def any_sql(self, expression: exp.Any) -> str:
            self.unsupported(
                "ANY subquery predicate is not supported in SingleStore")
            return super().any_sql(expression)

        def glob_sql(self, expression: exp.Glob) -> str:
            self.unsupported("GLOB predicate is not supported in SingleStore")
            return super().glob_sql(expression)

        def ilikeany_sql(self, expression: exp.ILikeAny) -> str:
            self.unsupported(
                "ILIKE ANY predicate is not supported in SingleStore")
            return super().ilikeany_sql(expression)

        def likeany_sql(self, expression: exp.LikeAny) -> str:
            self.unsupported(
                "LIKE ANY predicate is not supported in SingleStore")
            return super().likeany_sql(expression)

        def similarto_sql(self, expression: exp.SimilarTo) -> str:
            self.unsupported(
                "SIMILAR TO predicate is not supported in SingleStore")
            return super().similarto_sql(expression)

        def unicodestring_sql(self, expression: exp.UnicodeString) -> str:
            this = self.sql(expression, "this")
            escape = expression.args.get("escape")

            left_quote, right_quote = self.dialect.QUOTE_START, self.dialect.QUOTE_END

            if escape:
                escape_pattern = re.compile(rf"{escape.name}(\d+)")
            else:
                escape_pattern = ESCAPED_UNICODE_RE

            this = re.sub(escape_pattern, lambda m: chr(int(m.group(1), 16)),
                          this)

            return f"{left_quote}{this}{right_quote}"

        def placeholder_sql(self, expression: exp.Placeholder) -> str:
            # Named parameters are query parameters that are prefixed with a colon (:).
            # https://docs.oracle.com/cd/E19798-01/821-1841/bnbrh/index.html
            if expression.this:
                self.unsupported(
                    "Named placeholders are not supported in SingleStore")
            return super().placeholder_sql(expression)

        # TODO: implement using comparison operators
        def overlaps_sql(self, expression: exp.Overlaps) -> str:
            self.unsupported(
                "OVERLAPS is not supported in SingleStore")
            return super().overlaps_sql(expression)

        def dot_sql(self, expression: exp.Dot) -> str:
            self.unsupported(
                "Dot condition (.) is not supported in SingleStore")
            return super().dot_sql(expression)

        def dpipe_sql(self, expression: exp.DPipe) -> str:
            return self.func("CONCAT", *expression.flatten())

        # TODO: implement using REPLACE
        def escape_sql(self, expression: exp.Escape) -> str:
            self.unsupported(
                "ESCAPE condition in LIKE is not supported in SingleStore")
            return super().escape_sql(expression)

        def kwarg_sql(self, expression: exp.Kwarg) -> str:
            self.unsupported(
                "Kwarg condition (=>) is not supported in SingleStore")
            return super().kwarg_sql(expression)

        def operator_sql(self, expression: exp.Operator) -> str:
            self.unsupported(
                "Custom operators are not supported in SingleStore")
            return self.binary(expression, "")

        def slice_sql(self, expression: exp.Slice) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return super().slice_sql(expression)

        def arraycontains_sql(self, expression: exp.ArrayContains) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arraycontainsall_sql(self, expression: exp.ArrayContainsAll) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arrayoverlaps_sql(self, expression: exp.ArrayOverlaps) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def collate_sql(self, expression: exp.Collate) -> str:
            return self.binary(expression, ":> LONGTEXT COLLATE")

        def jsonpathkey_sql(self, expression: exp.JSONPathKey) -> str:
            return self.sql(exp.Literal.string(expression.this))

        def jsonpathsubscript_sql(self,
                                  expression: exp.JSONPathSubscript) -> str:
            return self.sql(exp.Literal.number(expression.this))

        def jsonpathfilter_sql(self, expression: exp.JSONPathFilter) -> str:
            self.unsupported("JSONPathFilter is not supported in SingleStore")
            return f"?{expression.this}"

        def jsonpathrecursive_sql(self,
                                  expression: exp.JSONPathRecursive) -> str:
            self.unsupported(
                "JSONPathRecursive is not supported in SingleStore")
            return f"..{expression.this or ''}"

        def jsonpathroot_sql(self, expression: exp.JSONPathRoot) -> str:
            self.unsupported("JSONPathRoot is not supported in SingleStore")
            return "$"

        def jsonpathscript_sql(self, expression: exp.JSONPathScript) -> str:
            self.unsupported("JSONPathScript is not supported in SingleStore")
            return f"({expression.this}"

        def jsonpathselector_sql(self, expression: exp.JSONPathSelector) -> str:
            self.unsupported("JSONPathSelector is not supported in SingleStore")
            return f"[{self.json_path_part(expression.this)}]"

        def jsonpathslice_sql(self, expression: exp.JSONPathSlice) -> str:
            self.unsupported("JSONPathSlice is not supported in SingleStore")
            return ":".join(
                "" if p is False else self.json_path_part(p)
                for p in
                [expression.args.get("start"), expression.args.get("end"),
                 expression.args.get("step")]
                if p is not None
            )

        def jsonpathunion_sql(self, expression: exp.JSONPathUnion) -> str:
            self.unsupported("JSONPathUnion is not supported in SingleStore")
            return f"[{','.join(self.json_path_part(p) for p in expression.expressions)}]"

        def jsonpathwildcard_sql(self, expression: exp.JSONPathWildcard) -> str:
            self.unsupported("JSONPathWildcard is not supported in SingleStore")
            return "*"

        def jsonpath_sql(self, expression: exp.JSONPath) -> str:
            args = [e for e in expression.expressions if
                    not isinstance(e, exp.JSONPathRoot)]

            return self.format_args(*args)

        @unsupported_args("quote")
        def jsonextract_sql(self, expression: exp.JSONExtract) -> str:
            return self.func("JSON_EXTRACT_JSON", expression.this,
                             expression.expression)

        def jsonextractscalar_sql(self, expression: exp.JSONExtract) -> str:
            return self.func("JSON_EXTRACT_STRING", expression.this,
                             expression.expression)

        def jsonbextract_sql(self, expression: exp.JSONBExtract) -> str:
            return self.func("BSON_EXTRACT_BSON", expression.this,
                             expression.expression)

        def jsonbextractscalar_sql(self, expression: exp.JSONBExtract) -> str:
            return self.func("BSON_EXTRACT_STRING", expression.this,
                             expression.expression)

        # TODO: handle partial case using BSON_ARRAY_CONTAINS_BSON
        def jsonbcontains_sql(self, expression: exp.JSONBContains) -> str:
            self.unsupported("JSONBContains is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def regexpilike_sql(self, expression: exp.RegexpILike) -> str:
            return self.binary(
                exp.RegexpLike(
                    this=exp.Lower(this=expression.this),
                    expression=exp.Lower(this=expression.expression)
                ), "RLIKE")

        def bracket_sql(self, expression: exp.Bracket) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return super().bracket_sql(expression)

        # TODO: investigate which Clickhouse parametrized/combined functions can be translated to SingleStore
        def combinedparameterizedagg_sql(self,
                                         expression: exp.CombinedParameterizedAgg) -> str:
            # https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/quantileGK
            # https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators
            self.unsupported(
                "Parametrized aggregate functions are not supported in SingleStore")
            return super().combinedparameterizedagg_sql(expression)

        def parameterizedagg_sql(self, expression: exp.ParameterizedAgg) -> str:
            # https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/quantileGK
            self.unsupported(
                "Parametrized aggregate functions are not supported in SingleStore")
            return super().parameterizedagg_sql(expression)

        def anonymousaggfunc_sql(self, expression: exp.AnonymousAggFunc) -> str:
            # https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/quantileGK
            self.unsupported(
                "Anonymous aggregate functions are not supported in SingleStore")
            return super().anonymousaggfunc_sql(expression)

        def combinedaggfunc_sql(self, expression: exp.CombinedAggFunc) -> str:
            # https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators
            self.unsupported(
                "Aggregate function combinators are not supported in SingleStore")
            return super().anonymousaggfunc_sql(expression)

        def argmin_sql(self, expression: exp.ArgMin) -> str:
            self.unsupported("ARG_MIN function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def argmax_sql(self, expression: exp.ArgMax) -> str:
            self.unsupported("ARG_MAX function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def approxtopk_sql(self, expression: exp.ApproxTopK) -> str:
            self.unsupported(
                "APPROX_TOP_K function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arrayagg_sql(self, expression: exp.ArrayAgg) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return super().arrayagg_sql(expression)

        def arrayuniqueagg_sql(self, expression: exp.ArrayAgg) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arrayunionagg_sql(self, expression: exp.ArrayAgg) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def jsonobjectagg_sql(self, expression: exp.JSONObjectAgg) -> str:
            self.unsupported(
                "JSON_OBJECT_AGG function is not supported in SingleStore")
            return super().jsonobjectagg_sql(expression)

        def jsonbobjectagg_sql(self, expression: exp.JSONBObjectAgg) -> str:
            self.unsupported(
                "JSONB_OBJECT_AGG function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def quantile_sql(self, expression: exp.Quantile) -> str:
            self.unsupported(
                "QUANTILE function is not supported in SingleStore")
            return self.func("APPROX_PERCENTILE", expression.this,
                             expression.args.get("quantile"))

        def corr_sql(self, expression: exp.Corr) -> str:
            self.unsupported("CORR function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def covarsamp_sql(self, expression: exp.CovarSamp) -> str:
            self.unsupported(
                "COVAR_SAMP function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def covarpop_sql(self, expression: exp.CovarPop) -> str:
            self.unsupported(
                "COVAR_POP function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def flatten_sql(self, expression: exp.Flatten) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def transform_sql(self, expression: exp.Transform) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        # TODO: rewrite APPLY to call function for each column
        def apply_sql(self, expression: exp.Apply) -> str:
            # https://clickhouse.com/docs/ru/sql-reference/statements/select#apply
            self.unsupported("APPLY function is not supported in SingleStore")
            return super().apply_sql(expression)

        def array_sql(self, expression: exp.Array) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def toarray_sql(self, expression: exp.ToArray) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return super().toarray_sql(expression)

        def list_sql(self, expression: exp.List) -> str:
            self.unsupported("LIST function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        @unsupported_args("format")
        @unsupported_args("action")
        @unsupported_args("default")
        def cast_sql(self, expression: exp.Cast) -> str:
            return f"{self.sql(expression, 'this')} :> {self.sql(expression, 'to')}"

        @unsupported_args("format")
        @unsupported_args("action")
        @unsupported_args("default")
        def trycast_sql(self, expression: exp.TryCast) -> str:
            return f"{self.sql(expression, 'this')} !:> {self.sql(expression, 'to')}"

        def columns_sql(self, expression: exp.Columns) -> str:
            # https://clickhouse.com/docs/ru/sql-reference/statements/select#dynamic-column-selection
            self.unsupported(
                "Dynamic column selection is not supported in SingleStore")
            return super().columns_sql(expression)

        def converttimezone_sql(self, expression: exp.ConvertTimezone) -> str:
            from_tz = expression.args.get("source_tz")
            to_tz = expression.args.get("target_tz")
            dt = expression.args.get("timestamp")

            return self.func("CONVERT_TZ", dt, from_tz, to_tz)

        def generateseries_sql(self, expression: exp.GenerateSeries) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def explodinggenerateseries_sql(self,
                                        expression: exp.ExplodingGenerateSeries) -> str:
            self.unsupported(
                "EXPLODING_GENERATE_SERIES function is not supported in SingleStore")
            return super().explodinggenerateseries_sql(expression)

        def arrayall_sql(self, expression: exp.ArrayAll) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arrayany_sql(self, expression: exp.ArrayAny) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return super().arrayany_sql(expression)

        def arrayconstructcompact_sql(self,
                                      expression: exp.ArrayConstructCompact):
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arrayfilter_sql(self, expression: exp.ArrayFilter) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arraytostring_sql(self, expression: exp.ArrayToString) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def stringtoarray_sql(self, expression: exp.StringToArray) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arraysize_sql(self, expression: exp.ArraySize) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return super().arraysize_sql(expression)

        def arraysort_sql(self, expression: exp.ArraySort) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arraysum_sql(self, expression: exp.ArraySum) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def arrayconcat_sql(self, expression: exp.ArrayConcat):
            self.unsupported("Arrays are not supported in SingleStore")
            return super().arrayconcat_sql(expression)

        def string_sql(self, expression: exp.String) -> str:
            this = expression.this
            zone = expression.args.get("zone")

            if zone:
                # This is a BigQuery specific argument for STRING(<timestamp_expr>, <time_zone>)
                # BigQuery stores timestamps internally as UTC, so ConvertTimezone is used with UTC
                # set for source_tz to transpile the time conversion before the STRING cast
                this = exp.ConvertTimezone(
                    source_tz=exp.Literal.string("UTC"), target_tz=zone,
                    timestamp=this
                )

            return self.sql(exp.cast(this, exp.DataType.Type.TEXT))

        def casttostrtype_sql(self, expression: exp.CastToStrType) -> str:
            to = expression.args.get("to")
            if not to or not isinstance(to, exp.Literal) or not to.is_string:
                self.unsupported("Invalid type for CAST")
            return self.sql(
                exp.cast(expression.this, expression.args.get("to").this))

        def connectbyroot_sql(self, expression: exp.ConnectByRoot) -> str:
            self.unsupported(
                "CONNECT_BY_ROOT function is not supported in SingleStore")
            return f"CONNECT_BY_ROOT {self.sql(expression, 'this')}"

        def cbrt_sql(self, expression: exp.Cbrt) -> str:
            return self.sql(exp.Pow(this=expression.this,
                                    expression=exp.Literal.number(1 / 3)))

        def currentdatetime_sql(self, expression: exp.CurrentDatetime) -> str:
            return self.sql(exp.cast(exp.CurrentTimestamp(),
                                     exp.DataType.Type.DATETIME))

        def dateadd_sql(self, expression: exp.DateAdd) -> str:
            date = self.sql(expression, "this")
            interval = self.sql(
                exp.Interval(this=expression.expression, unit=expression.unit))

            return f"DATE_ADD({date}, {interval})"

        def datesub_sql(self, expression: exp.DateSub) -> str:
            date = self.sql(expression, "this")
            interval = self.sql(
                exp.Interval(this=expression.expression, unit=expression.unit))

            return f"DATE_SUB({date}, {interval})"

        @unsupported_args("zone")
        def datediff_sql(self, expression: exp.DateDiff) -> str:
            return self.func("TIMESTAMPDIFF", expression.unit, expression.this,
                             expression.expression)

        @unsupported_args("zone")
        def datetrunc_sql(self, expression: exp.DateTrunc) -> str:
            return self.function_fallback_sql(expression)

        @unsupported_args("zone")
        @unsupported_args("expressions")
        def datetime_sql(self, expression: exp.Datetime) -> str:
            return self.sql(
                exp.cast(expression.this, exp.DataType.Type.DATETIME))

        @unsupported_args("zone")
        def datetimetrunc_sql(self, expression: exp.DateTrunc) -> str:
            unit = self.sql(exp.Literal.string(expression.unit))
            datetime = self.sql(expression, "this")
            return f"DATE_TRUNC({unit}, {datetime})"

        def makeinterval_sql(self, expression: exp.MakeInterval) -> str:
            self.unsupported(
                "INTERVAL data type is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        @unsupported_args("zone")
        def timestamptrunc_sql(self, expression: exp.TimestampTrunc) -> str:
            unit = self.sql(exp.Literal.string(expression.unit))
            datetime = self.sql(expression, "this")
            return f"DATE_TRUNC({unit}, {datetime})"

        @unsupported_args("zone")
        def timetrunc_sql(self, expression: exp.TimeTrunc) -> str:
            unit = self.sql(exp.Literal.string(expression.unit))
            datetime = self.sql(expression, "this")
            return f"DATE_TRUNC({unit}, {datetime})"

        def datestrtodate_sql(self, expression: exp.DateStrToDate) -> str:
            return self.sql(
                exp.cast(expression.this, exp.DataType.Type.DATE))

        def datetodatestr_sql(self, expression: exp.DateToDateStr) -> str:
            return self.sql(
                exp.cast(expression.this, exp.DataType.Type.TEXT))

        def datefromparts_sql(self, expression: exp.DateFromParts) -> str:
            self.unsupported(
                "DATE_FROM_PARTS function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def timefromparts_sql(self, expression: exp.TimeFromParts) -> str:
            self.unsupported(
                "TIME_FROM_PARTS function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        @unsupported_args("zone")
        @unsupported_args("expressions")
        def date_sql(self, expression: exp.Date) -> str:
            return self.sql(exp.cast(expression.this, exp.DataType.Type.DATE))

        def decode_sql(self, expression: exp.Decode) -> str:
            self.unsupported(
                "DECODE function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def encode_sql(self, expression: exp.Encode) -> str:
            self.unsupported(
                "ENCODE function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def explode_sql(self, expression: exp.Explode) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def posexplodeouter_sql(self, expression: exp.PosexplodeOuter) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def posexplode_sql(self, expression: exp.Posexplode) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def inline_sql(self, expression: exp.Inline) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def unnest_sql(self, expression: exp.Unnest) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return super().unnest_sql(expression)

        def featuresattime_sql(self, expression: exp.FeaturesAtTime) -> str:
            self.unsupported(
                "FEATURES_AT_TIME function is not supported in SingleStore")
            return super().featuresattime_sql(expression)

        def fromiso8601timestamp_sql(self,
                                     expression: exp.FromISO8601Timestamp):
            self.unsupported(
                "FROM_ISO8601_TIMESTAMP function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def gapfill_sql(self, expression: exp.GapFill) -> str:
            self.unsupported(
                "GAP_FILL function is not supported in SingleStore")
            return super().gapfill_sql(expression)

        def generatedatearray_sql(self,
                                  expression: exp.GenerateDateArray) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def generatetimestamparray_sql(self,
                                       expression: exp.GenerateTimestampArray) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def isinf_sql(self, expression: exp.IsInf) -> str:
            self.unsupported("IS_INF function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def isnan_sql(self, expression: exp.IsNan) -> str:
            self.unsupported("IS_NAN function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        @unsupported_args("null_handling")
        @unsupported_args("unique_keys")
        @unsupported_args("return_type")
        @unsupported_args("encoding")
        def jsonobject_sql(self, expression: exp.JSONObject) -> str:
            return self.func("JSON_BUILD_OBJECT", *expression.expressions)

        @unsupported_args("null_handling")
        @unsupported_args("return_type")
        @unsupported_args("strict")
        def jsonarray_sql(self, expression: exp.JSONArray) -> str:
            return self.func("JSON_BUILD_ARRAY", *expression.expressions)

        @unsupported_args("null_handling")
        @unsupported_args("return_type")
        @unsupported_args("strict")
        def jsonarrayagg_sql(self, expression: exp.JSONArrayAgg) -> str:
            this = self.sql(expression, "this")
            order = self.sql(expression, "order")

            return self.func("JSON_AGG", this, suffix=f"{order})")

        @unsupported_args("passing")
        @unsupported_args("on_condition")
        def jsonexists_sql(self, expression: exp.JSONExists) -> str:
            this = self.sql(expression, "this")
            path = self.sql(expression, "path")
            return self.func("JSON_MATCH_ANY_EXISTS", this, path)

        def jsonvaluearray_sql(self, expression: exp.JSONValueArray) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def jsontable_sql(self, expression: exp.JSONTable) -> str:
            self.unsupported(
                "JSON_TABLE function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def objectinsert_sql(self, expression: exp.ObjectInsert) -> str:
            return self.func("JSON_SET_JSON", expression.this,
                             expression.args.get("key"),
                             expression.args.get("value"))

        def openjson_sql(self, expression: exp.OpenJSON) -> str:
            self.unsupported(
                "OPENJSON function is not supported in SingleStore")
            return super().openjson_sql(expression)

        def parsejson_sql(self, expression: exp.ParseJSON) -> str:
            self.unsupported(
                "PARSE_JSON function is not supported in SingleStore")
            return super().parsejson_sql(expression)

        def jsonbexists_sql(self, expression: exp.JSONBExists) -> str:
            this = self.sql(expression, "this")
            path = self.sql(expression, "path")
            return self.func("BSON_MATCH_ANY_EXISTS", this, path)

        def jsonextractarray_sql(self, expression: exp.JSONExtractArray) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def levenshtein_sql(self, expression: exp.Levenshtein) -> str:
            self.unsupported(
                "LEVENSHTEIN function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def map_sql(self, expression: exp.Map) -> str:
            self.unsupported("Maps are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def tomap_sql(self, expression: exp.ToMap) -> str:
            self.unsupported("Maps are not supported in SingleStore")
            return f"MAP {self.sql(expression, 'this')}"

        def mapfromentries_sql(self, expression: exp.MapFromEntries) -> str:
            self.unsupported("Maps are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def starmap_sql(self, expression: exp.StarMap) -> str:
            self.unsupported("Maps are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def varmap_sql(self, expression: exp.VarMap) -> str:
            self.unsupported("Maps are not supported in SingleStore")
            return self.func("MAP", expression.args["keys"],
                             expression.args["values"])

        def matchagainst_sql(self, expression: exp.MatchAgainst) -> str:
            self.unsupported(
                "MATCH_AGAINST function is not supported in SingleStore")
            return super().matchagainst_sql(expression)

        def normalize_sql(self, expression: exp.Normalize) -> str:
            self.unsupported(
                "NORMALIZE function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def overlay_sql(self, expression: exp.Overlay) -> str:
            self.unsupported(
                "OVERLAY function is not supported in SingleStore")
            return super().overlay_sql(expression)

        def predict_sql(self, expression: exp.Predict) -> str:
            self.unsupported(
                "PREDICT function is not supported in SingleStore")
            return super().predict_sql(expression)

        def randn_sql(self, expression: exp.Randn) -> str:
            self.unsupported(
                "RANDN function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def rangen_sql(self, expression: exp.RangeN) -> str:
            self.unsupported(
                "RANGE_N function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def readcsv_sql(self, expression: exp.ReadCSV) -> str:
            self.unsupported(
                "READ_CSV function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def reduce_sql(self, expression: exp.Reduce) -> str:
            self.unsupported("REDUCE function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def regexpsplit_sql(self, expression: exp.RegexpSplit) -> str:
            self.unsupported(
                "REGEXP_SPLIT function is not supported in SingleStore")
            return self.func("SPLIT", expression.this, expression.expression)

        def sortarray_sql(self, expression: exp.SortArray) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def splitpart_sql(self, expression: exp.SplitPart) -> str:
            self.unsupported(
                "SPLIT_PART function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def standardhash_sql(self, expression: exp.StandardHash) -> str:
            hash_function = expression.expression
            if hash_function is None:
                return self.func("SHA", expression.this)
            if isinstance(hash_function, exp.Literal):
                if hash_function.is_string and hash_function.this.lower() == "sha":
                    return self.func("SHA", expression.this)
                if hash_function.is_string and hash_function.this.lower() == "md5":
                    return self.func("MD5", expression.this)

                self.unsupported(
                    f"{hash_function.this} hash method is not supported in SingleStore")

            self.unsupported(
                f"STANDARD_HASH function is not supported in SingleStore")
            return self.func("SHA", expression.this)

        @unsupported_args("occurrence")
        def strposition_sql(self, expression: exp.StrPosition) -> str:
            haystack = self.sql(expression, "this")
            needle = self.sql(expression, "substr")
            if expression.args.get("position") is not None:
                position = self.sql(expression, "position")
                return self.func("LOCATE", needle, haystack, position)

            return self.func("LOCATE", needle, haystack)

        def strtomap_sql(self, expression: exp.StrToMap) -> str:
            self.unsupported("Maps are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def struct_sql(self, expression: exp.Struct) -> str:
            self.unsupported("Structs are not supported in SingleStore")
            return super().struct_sql(expression)

        def structextract_sql(self, expression: exp.StructExtract) -> str:
            self.unsupported("Structs are not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def stuff_sql(self, expression: exp.Stuff) -> str:
            text = self.sql(expression, "this")
            start = self.sql(expression, "start")
            length = self.sql(expression, "length")
            value = self.sql(expression, "expression")

            return f"CONCAT(SUBSTRING({text}, 1, {start}-1), {value}, SUBSTRING({text}, {start}+{length}))"

        def unicode_sql(self, expression: exp.Unicode) -> str:
            self.unsupported("UNICODE function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def timestampfromparts_sql(self,
                                   expression: exp.TimestampFromParts) -> str:
            self.unsupported(
                "TIMESTAMP_FROM_PARTS function is not supported in SingleStore")
            return self.function_fallback_sql(expression)

        def xmlelement_sql(self,
                           expression: exp.XMLElement) -> str:
            self.unsupported(
                "XMLELEMENT function is not supported in SingleStore")
            return super().xmlelement_sql(expression)

        def xmltable_sql(self,
                         expression: exp.XMLTable) -> str:
            self.unsupported(
                "XMLTABLE function is not supported in SingleStore")
            return super().xmltable_sql(expression)

        def nextvaluefor_sql(self, expression: exp.NextValueFor) -> str:
            self.unsupported(
                "NEXT_VALUE_FOR function is not supported in SingleStore")
            return super().nextvaluefor_sql(expression)

        @unsupported_args("kind", "cluster", "distribute", "sort")
        def select_sql(self, expression: exp.Select) -> str:
            return super().select_sql(expression)

        def cache_sql(self, expression: exp.Cache) -> str:
            self.unsupported(
                "CACHE query is not supported in SingleStore")
            return super().cache_sql(expression)

        def uncache_sql(self, expression: exp.Uncache) -> str:
            self.unsupported(
                "UNCACHE query is not supported in SingleStore")
            return super().uncache_sql(expression)

        def refresh_sql(self, expression: exp.Refresh) -> str:
            self.unsupported(
                "REFRESH query is not supported in SingleStore")
            return super().refresh_sql(expression)

        def sequenceproperties_sql(self,
                                   expression: exp.SequenceProperties) -> str:
            self.unsupported(
                "Sequences are not supported in SingleStore")
            return super().sequenceproperties_sql(expression)

        @unsupported_args("history")
        @unsupported_args("terse")
        @unsupported_args("offset")
        @unsupported_args("starts_with")
        @unsupported_args("limit")
        @unsupported_args("from")
        @unsupported_args("scope")
        @unsupported_args("scope_kind")
        @unsupported_args("mutex")
        @unsupported_args("query")
        @unsupported_args("channel")
        @unsupported_args("log")
        @unsupported_args("types")
        @unsupported_args("privileges")
        # TODO: add support of SingleStore specific SHOW command parts
        # TODO: translate some SHOW commands to selects from information_schema
        def show_sql(self, expression: exp.Show) -> str:
            this = f" {expression.name}"
            full = " FULL" if expression.args.get("full") else ""
            global_ = " GLOBAL" if expression.args.get("global") else ""

            target = self.sql(expression, "target")
            target = f" {target}" if target else ""
            if expression.name in (
                    "COLUMNS", "INDEX", "INDEXES", "KEYS"):
                target = f" FROM{target}"
            elif expression.name == "GRANTS":
                target = f" FOR{target}"

            db = self._prefixed_sql("FROM", expression, "db")

            like = self._prefixed_sql("LIKE", expression, "like")
            where = self.sql(expression, "where")

            return f"SHOW{full}{global_}{this}{target}{db}{like}{where}"

        def _prefixed_sql(self, prefix: str, expression: exp.Expression,
                          arg: str) -> str:
            sql = self.sql(expression, arg)
            return f" {prefix} {sql}" if sql else ""

        @unsupported_args("is_database")
        @unsupported_args("exists")
        @unsupported_args("cluster")
        @unsupported_args("identity")
        @unsupported_args("option")
        @unsupported_args("partition")
        def truncatetable_sql(self, expression: exp.TruncateTable) -> str:
            statements = []
            for expression in expression.expressions:
                statements.append(f"TRUNCATE {self.sql(expression)}")

            return "; ".join(statements)

        def clone_sql(self, expression: exp.Clone) -> str:
            this = self.sql(expression, "this")

            if expression.args.get("copy"):
                shallow = "WITH SHALLOW" if expression.args.get(
                    "shallow") else "WITH DEEP"
                return f"LIKE {this} {shallow} COPY"

            return f"LIKE {this}"

        @unsupported_args("style")
        @unsupported_args("kind")
        @unsupported_args("expressions")
        @unsupported_args("partition")
        @unsupported_args("format")
        def describe_sql(self, expression: exp.Describe) -> str:
            return f"DESCRIBE {self.sql(expression, 'this')}"

        # TODO: parse credentials
        @unsupported_args("exists")
        def attach_sql(self, expression: exp.Attach) -> str:
            this = self.sql(expression, "this")
            expressions = self.expressions(expression)

            return f"ATTACH DATABASE {this} {expressions}"

        @unsupported_args("exists")
        def detach_sql(self, expression: exp.Detach) -> str:
            this = self.sql(expression, "this")

            return f"DETACH DATABASE {this}"

        def summarize_sql(self, expression: exp.Summarize) -> str:
            self.unsupported("SUMMARIZE query is not supported in SingleStore")
            return super().summarize_sql(expression)

        def pragma_sql(self, expression: exp.Pragma) -> str:
            self.unsupported("PRAGMA query is not supported in SingleStore")
            return super().pragma_sql(expression)

        def declareitem_sql(self, expression: exp.DeclareItem) -> str:
            if isinstance(expression.this, exp.Parameter):
                variable = self.sql(expression.this, "this")
            else:
                variable = self.sql(expression, "this")
            default = self.sql(expression, "default")
            default = f" = {default}" if default else ""

            kind = self.sql(expression, "kind")
            if isinstance(expression.args.get("kind"), exp.Schema):
                kind = f"TABLE {kind}"

            return f"{variable} {kind}{default}"

        def userdefinedfunction_sql(self,
                                    expression: exp.UserDefinedFunction) -> str:
            this = self.sql(expression, "this")
            expressions = self.no_identify(self.expressions, expression)
            expressions = self.wrap(expressions)
            return f"{this}{expressions}"

        def recursivewithsearch_sql(self,
                                    expression: exp.RecursiveWithSearch) -> str:
            self.unsupported(
                "RecursiveWithSearch expression is not supported in SingleStore")
            return ""

        def projectiondef_sql(self, expression: exp.ProjectionDef) -> str:
            self.unsupported(
                "PROJECTION definition is not supported in SingleStore")
            return ""

        @unsupported_args("exists")
        def columndef_sql(self, expression: exp.ColumnDef,
                          sep: str = " ") -> str:
            return super().columndef_sql(expression, sep)

        @unsupported_args("drop", "comment", "allow_null", "visible", "using")
        def altercolumn_sql(self, expression: exp.AlterColumn) -> str:
            dtype = self.sql(expression, "dtype")
            if not dtype:
                return super().altercolumn_sql(expression)

            collate = self.sql(expression, "collate")
            collate = f" COLLATE {collate}" if collate else ""
            this = self.sql(expression, "this")

            return f"MODIFY COLUMN {this} {dtype}{collate}"

        def alterindex_sql(self, expression: exp.AlterIndex) -> str:
            self.unsupported(
                "INVISIBLE INDEXES are not supported in SingleStore")
            return super().alterindex_sql(expression)

        def alterdiststyle_sql(self, expression: exp.AlterDistStyle) -> str:
            self.unsupported("ALTER DYSTSTILE is not supported in SingleStore")
            return super().alterdiststyle_sql(expression)

        def altersortkey_sql(self, expression: exp.AlterSortKey) -> str:
            self.unsupported("ALTER SORTKEY is not supported in SingleStore")
            return super().altersortkey_sql(expression)

        @unsupported_args("exists")
        def renamecolumn_sql(self, expression: exp.RenameColumn) -> str:
            old_column = self.sql(expression, "this")
            new_column = self.sql(expression, "to")
            return f"CHANGE {old_column} {new_column}"

        def swaptable_sql(self, expression: exp.SwapTable) -> str:
            self.unsupported("ALTER TABLE SWAP is not supported in SingleStore")
            return f"SWAP WITH {self.sql(expression, 'this')}"

        def comment_sql(self, expression: exp.Comment) -> str:
            self.unsupported("COMMENT query is not supported in SingleStore")
            return super().comment_sql(expression)

        def comprehension_sql(self, expression: exp.Comprehension) -> str:
            self.unsupported("Comprehension is not supported in SingleStore")
            return super().comprehension_sql(expression)

        def mergetreettlaction_sql(self,
                                   expression: exp.MergeTreeTTLAction) -> str:
            self.unsupported("TTLs are not supported in SingleStore")
            return super().mergetreettlaction_sql(expression)

        def mergetreettl_sql(self, expression: exp.MergeTreeTTL) -> str:
            self.unsupported("TTLs are not supported in SingleStore")
            return super().mergetreettl_sql(expression)

        @unsupported_args("parser", "visible", "engine_attr",
                          "secondary_engine_attr")
        def indexconstraintoption_sql(self,
                                      expression: exp.IndexConstraintOption) -> str:
            key_block_size = self.sql(expression, "key_block_size")
            if key_block_size:
                return f"KEY_BLOCK_SIZE = {key_block_size}"

            using = self.sql(expression, "using")
            if using:
                return f"USING {using}"

            comment = self.sql(expression, "comment")
            if comment:
                return f"COMMENT {comment}"

            self.unsupported("Unsupported index constraint option.")
            return ""

        def alterset_sql(self, expression: exp.AlterSet) -> str:
            self.unsupported("ALTER SET query is not supported in SingleStore")
            return super().alterset_sql(expression)

        def periodforsystemtimeconstraint_sql(self,
                                              expression: exp.PeriodForSystemTimeConstraint) -> str:
            self.unsupported(
                "PERIOD FOR SYSTEM TIME column constraint is not supported in SingleStore")
            return ""

        def casespecificcolumnconstraint_sql(self,
                                             expression: exp.CaseSpecificColumnConstraint) -> str:
            self.unsupported(
                "CASE SPECIFIC column constraint is not supported in SingleStore")
            return ""

        def checkcolumnconstraint_sql(self,
                                      expression: exp.CheckColumnConstraint) -> str:
            self.unsupported(
                "CHECK column constraint is not supported in SingleStore")
            return ""

        def clusteredcolumnconstraint_sql(self,
                                          expression: exp.ClusteredColumnConstraint) -> str:
            self.unsupported(
                "CLUSTERED column constraint is not supported in SingleStore")
            return ""

        def compresscolumnconstraint_sql(self,
                                         expression: exp.CompressColumnConstraint) -> str:
            self.unsupported(
                "COMPRESS column constraint is not supported in SingleStore")
            return ""

        def dateformatcolumnconstraint_sql(self,
                                           expression: exp.DateFormatColumnConstraint) -> str:
            self.unsupported(
                "FORMAT column constraint is not supported in SingleStore")
            return ""

        def encodecolumnconstraint_sql(self,
                                       expression: exp.EncodeColumnConstraint) -> str:
            self.unsupported(
                "ENCODE column constraint is not supported in SingleStore")
            return ""

        def excludecolumnconstraint_sql(self,
                                        expression: exp.ExcludeColumnConstraint) -> str:
            self.unsupported(
                "EXCLUDE column constraint is not supported in SingleStore")
            return ""

        def ephemeralcolumnconstraint_sql(self,
                                          expression: exp.EphemeralColumnConstraint) -> str:
            self.unsupported(
                "EPHEMERAL column constraint is not supported in SingleStore")
            return ""

        def generatedasidentitycolumnconstraint_sql(self,
                                                    expression: exp.GeneratedAsIdentityColumnConstraint) -> str:
            self.unsupported(
                "GENERATED AS column constraint is not supported in SingleStore")
            return ""

        def generatedasrowcolumnconstraint_sql(self,
                                               expression: exp.GeneratedAsRowColumnConstraint) -> str:
            self.unsupported(
                "GENERATED AS column constraint is not supported in SingleStore")
            return ""

        def uppercasecolumnconstraint_sql(self,
                                          expression: exp.UppercaseColumnConstraint) -> str:
            self.unsupported(
                "UPPERCASE column constraint is not supported in SingleStore")
            return ""

        def pathcolumnconstraint_sql(self,
                                     expression: exp.PathColumnConstraint) -> str:
            self.unsupported(
                "PATH column constraint is not supported in SingleStore")
            return ""

        def projectionpolicycolumnconstraint_sql(self,
                                                 expression: exp.ProjectionPolicyColumnConstraint) -> str:
            self.unsupported(
                "PROJECTION POLICY constraint is not supported in SingleStore")
            return ""

        def inlinelengthcolumnconstraint_sql(self,
                                             expression: exp.InlineLengthColumnConstraint) -> str:
            self.unsupported(
                "INLINE LENGTH column constraint is not supported in SingleStore")
            return ""

        def nonclusteredcolumnconstraint_sql(self,
                                             expression: exp.NonClusteredColumnConstraint) -> str:
            self.unsupported(
                "NONCLUSTERED column constraint is not supported in SingleStore")
            return ""

        def notforreplicationcolumnconstraint_sql(self,
                                                  expression: exp.NotForReplicationColumnConstraint) -> str:
            self.unsupported(
                "NOT FOR REPLICATION column constraint is not supported in SingleStore")
            return ""

        def maskingpolicycolumnconstraint_sql(self,
                                              expression: exp.MaskingPolicyColumnConstraint) -> str:
            self.unsupported(
                "MASKING POLICY column constraint is not supported in SingleStore")
            return ""

        def onupdatecolumnconstraint_sql(self,
                                         expression: exp.OnUpdateColumnConstraint) -> str:
            self.unsupported(
                "ON UPDATE column constraint is not supported in SingleStore")
            return ""

        def titlecolumnconstraint_sql(self,
                                      expression: exp.TitleColumnConstraint) -> str:
            self.unsupported(
                "TITLE column constraint is not supported in SingleStore")
            return ""

        def transformcolumnconstraint_sql(self,
                                          expression: exp.TransformColumnConstraint) -> str:
            self.unsupported(
                "TRANSFORM column constraint is not supported in SingleStore")
            return ""

        def computedcolumnconstraint_sql(self,
                                         expression: exp.ComputedColumnConstraint) -> str:
            this = self.sql(expression, "this")
            not_null = ""
            if expression.args.get("not_null"):
                not_null = " NOT NULL"
            return f"AS {this} PERSISTED AUTO{not_null}"

        @unsupported_args("desc", "options")
        def primarykeycolumnconstraint_sql(self,
                                           expression: exp.PrimaryKeyColumnConstraint) -> str:
            return f"PRIMARY KEY"

        @unsupported_args("this", "nulls_sql", "on_conflict", "index_type",
                          "options")
        def uniquecolumnconstraint_sql(self,
                                       expression: exp.UniqueColumnConstraint) -> str:
            return f"UNIQUE"

        def tags_sql(self, expression: exp.Tags) -> str:
            self.unsupported(
                "TAG column constraint is not supported in SingleStore")
            return ""

        def watermarkcolumnconstraint_sql(self,
                                          expression: exp.WatermarkColumnConstraint) -> str:
            self.unsupported(
                "WATERMARK column constraint is not supported in SingleStore")
            return ""

        @unsupported_args("materialized", "concurrently", "cascade",
                          "expressions", "constraints", "purge")
        def drop_sql(self, expression: exp.Drop) -> str:
            this = self.sql(expression, "this")
            kind = expression.args["kind"]
            kind = self.dialect.INVERSE_CREATABLE_KIND_MAPPING.get(kind) or kind
            exists_sql = " IF EXISTS " if expression.args.get("exists") else " "
            on_cluster = self.sql(expression, "cluster")
            on_cluster = f" {on_cluster}" if on_cluster else ""
            temporary = " TEMPORARY" if expression.args.get("temporary") else ""
            return f"DROP{temporary} {kind}{exists_sql}{this}{on_cluster}"

        # TODO: implement using INSERT INTO
        # https://docs.singlestore.com/db/v8.9/reference/sql-reference/data-manipulation-language-dml/select/#select-into-gcs
        def export_sql(self, expression: exp.Export) -> str:
            self.unsupported(
                "EXPORT query is not supported in SingleStore")
            return super().export_sql(expression)

        def check_sql(self, expression: exp.Check) -> str:
            self.unsupported(
                "CHECK column constraint is not supported in SingleStore")
            return super().check_sql(expression)

        def changes_sql(self, expression: exp.Changes) -> str:
            self.unsupported(
                "CHANGES clause is not supported in SingleStore")
            return super().changes_sql(expression)

        def connect_sql(self, expression: exp.Connect) -> str:
            self.unsupported(
                "CONNECT BY clause is not supported in SingleStore")
            return super().connect_sql(expression)

        # TODO: implement using LOAD DATA and INSERT INTO
        def copyparameter_sql(self, expression: exp.CopyParameter) -> str:
            self.unsupported(
                "COPY query is not supported in SingleStore")
            return super().copyparameter_sql(expression)

        def credentials_sql(self, expression: exp.Credentials) -> str:
            self.unsupported(
                "COPY query is not supported in SingleStore")
            return super().credentials_sql(expression)

        def prior_sql(self, expression: exp.Prior) -> str:
            self.unsupported(
                "CONNECT BY clause is not supported in SingleStore")
            return super().prior_sql(expression)

        # TODO: implement using INSERT INTO
        def directory_sql(self, expression: exp.Directory) -> str:
            self.unsupported(
                "INSERT OVERWRITE DIRECTORY query is not supported in SingleStore")
            return super().directory_sql(expression)

        def foreignkey_sql(self, expression: exp.ForeignKey) -> str:
            self.unsupported(
                "Foreign keys are not supported in SingleStore")
            return super().foreignkey_sql(expression)

        def columnprefix_sql(self, expression: exp.ColumnPrefix) -> str:
            self.unsupported(
                "Using column prefix for PK is not supported in SingleStore")
            return super().columnprefix_sql(expression)

        @unsupported_args("options")
        def primarykey_sql(self, expression: exp.PrimaryKey) -> str:
            expressions = self.expressions(expression, flat=True)
            return f"PRIMARY KEY ({expressions})"

        def opclass_sql(self, expression: exp.Opclass) -> str:
            self.unsupported(
                "Operator classes are not supported in SingleStore")
            return f"{self.sql(expression, 'this')}"

        @unsupported_args("amp")
        @unsupported_args("primary")
        def index_sql(self, expression: exp.Index) -> str:
            unique = "UNIQUE " if expression.args.get("unique") else ""
            name = self.sql(expression, "this")
            name = f"{name} " if name else ""
            table = self.sql(expression, "table")
            table = f"{self.INDEX_ON} {table}" if table else ""

            index = "INDEX " if not table else ""

            params = self.sql(expression, "params")
            return f"{unique}{index}{name}{table}{params}"

        def withoperator_sql(self, expression: exp.WithOperator) -> str:
            self.unsupported(
                "Indexes with operator are not supported in SingleStore")
            return self.sql(expression, 'this')

        @unsupported_args("include", "with_storage", "tablespace",
                          "partition_by", "where", "on")
        def indexparameters_sql(self, expression: exp.IndexParameters) -> str:
            using = self.sql(expression, "using")
            using = f" USING {using}" if using else ""
            columns = self.expressions(expression, key="columns", flat=True)
            columns = f"({columns})" if columns else ""

            return f"{columns}{using}"

        def conditionalinsert_sql(self,
                                  expression: exp.ConditionalInsert) -> str:
            self.unsupported(
                "Conditional insert is not supported in SingleStore")
            return super().conditionalinsert_sql(expression)

        def multitableinserts_sql(self,
                                  expression: exp.MultitableInserts) -> str:
            self.unsupported(
                "Multitable insert is not supported in SingleStore")
            return super().multitableinserts_sql(expression)

        @unsupported_args("constraint", "conflict_keys", "where")
        def onconflict_sql(self, expression: exp.OnConflict) -> str:
            conflict = "ON DUPLICATE KEY" if expression.args.get(
                "duplicate") else "ON CONFLICT"

            action = self.sql(expression, "action")

            expressions = self.expressions(expression, flat=True)
            expressions = f" {expressions}" if expressions else ""

            return f"{conflict} {action}{expressions}"

        def oncondition_sql(self, expression: exp.OnCondition) -> str:
            self.unsupported(
                "Setting on empty or on error behaviour for JSON functions is not supported in SingleStore")
            return ""

        def returning_sql(self, expression: exp.Returning) -> str:
            self.unsupported(
                "RETURNING is not supported in SingleStore")
            return ""

        def introducer_sql(self, expression: exp.Introducer) -> str:
            self.unsupported(
                "Character set introducers are not supported in SingleStore")
            return f"{self.sql(expression, 'expression')}"

        def national_sql(self, expression: exp.National,
                         prefix: str = "N") -> str:
            return self.sql(exp.Literal.string(expression.name))

        @unsupported_args("partition", "serde")
        def loaddata_sql(self, expression: exp.LoadData) -> str:
            local = " LOCAL" if expression.args.get("local") else ""
            inpath = f" INFILE {self.sql(expression, 'inpath')}"
            overwrite = " REPLACE" if expression.args.get("overwrite") else ""
            this = f" INTO TABLE {self.sql(expression, 'this')}"
            input_format = expression.args.get("input_format")
            input_format = f" FORMAT {input_format.this}" if input_format else ""
            return f"LOAD DATA{local}{inpath}{overwrite}{this}{input_format}"

        @unsupported_args("kind")
        def grant_sql(self, expression: exp.Grant) -> str:
            privileges_sql = self.expressions(expression, key="privileges",
                                              flat=True)

            securable = self.sql(expression, "securable")
            securable = f" {securable}" if securable else ""

            principals = self.expressions(expression, key="principals",
                                          flat=True)

            grant_option = " WITH GRANT OPTION" if expression.args.get(
                "grant_option") else ""

            return f"GRANT {privileges_sql} ON{securable} TO {principals}{grant_option}"

        @unsupported_args("view", "ordinality")
        def lateral_sql(self, expression: exp.Lateral) -> str:
            this = self.sql(expression, "this")

            alias = self.sql(expression, "alias")
            alias = f" AS {alias}" if alias else ""
            condition = " ON TRUE" if expression.args.get(
                "cross_apply") is False else ""
            op = self.lateral_op(expression)

            return f"{op} {this}{alias}{condition}"

        @unsupported_args("sample", "joins")
        def tablefromrows_sql(self, expression: exp.TableFromRows) -> str:
            # Copy alias to values
            # This is done to ensure that column names are correctly propagated
            if expression.args.get("alias") and isinstance(expression.this,
                                                           exp.Values):
                expression.this.set("alias",
                                    expression.args.get("alias").copy())

            table = self.sql(expression.this)
            pivots = self.expressions(expression, key="pivots", sep="",
                                      flat=True)
            return f"{table}{pivots}"

        @unsupported_args("materialized")
        def cte_sql(self, expression: exp.CTE) -> str:
            alias = expression.args.get("alias")
            if alias:
                alias.add_comments(expression.pop_comments())

            alias_sql = self.sql(expression, "alias")

            return f"{alias_sql} AS {self.wrap(expression)}"

        def tablealias_sql(self, expression: exp.TableAlias) -> str:
            alias = self.sql(expression, "this")
            columns = self.expressions(expression, key="columns", flat=True)
            columns = f"({columns})" if columns else ""

            if columns and not isinstance(expression.parent, exp.CTE):
                columns = ""
                self.unsupported(
                    "Named columns are not supported in table alias.")

            if not alias and not self.dialect.UNNEST_COLUMN_ONLY:
                alias = self._next_name()

            return f"{alias}{columns}"

        def subquery_sql(self, expression: exp.Subquery,
                         sep: str = " AS ", wrap: bool = True) -> str:
            if expression.args.get("sample") is not None:
                self.unsupported(
                    "Argument 'sample' is not supported for expression 'subquery' when targeting SingleStore.")

            alias = self.sql(expression, "alias")
            alias = f"{sep}{alias}" if alias else ""

            pivots = self.expressions(expression, key="pivots", sep="",
                                      flat=True)
            sql = self.query_modifiers(expression, self.wrap(expression) if wrap else self.sql(expression, "this"),
                                       alias, pivots)
            return self.prepend_ctes(expression, sql)

        @unsupported_args("returning", "overwrite", "alternative",
                          "is_function", "exists", "by_name", "where", "stored",
                          "partition", "source", "settings")
        def insert_sql(self, expression: exp.Insert) -> str:
            hint = self.sql(expression, "hint")
            this = " INTO"

            ignore = " IGNORE" if expression.args.get("ignore") else ""
            this = f"{this} {self.sql(expression, 'this')}"

            expression_sql = f"{self.sep()}{self.sql(expression, 'expression')}"
            on_conflict = self.sql(expression, "conflict")
            on_conflict = f" {on_conflict}" if on_conflict else ""
            expression_sql = f"{expression_sql}{on_conflict}"

            sql = f"INSERT{hint}{ignore}{this}{expression_sql}"
            return self.prepend_ctes(expression, sql)

        @unsupported_args("returning", "using", "cluster")
        def delete_sql(self, expression: exp.Delete) -> str:
            this = self.sql(expression, "this")
            this = f" FROM {this}" if this else ""
            where = self.sql(expression, "where")
            limit = self.sql(expression, "limit")
            tables = self.expressions(expression, key="tables")
            tables = f" {tables}" if tables else ""

            return self.prepend_ctes(expression,
                                     f"DELETE{tables}{this}{where}{limit}")

        @unsupported_args("order", "returning", "from")
        def update_sql(self, expression: exp.Update) -> str:
            this = self.sql(expression, "this")
            set_sql = self.expressions(expression, flat=True)
            where_sql = self.sql(expression, "where")
            limit = self.sql(expression, "limit")

            sql = f"UPDATE {this} SET {set_sql}{where_sql}{limit}"
            return self.prepend_ctes(expression, sql)

        # TODO: implement using LOAD DATA or SELECT INTO
        def copy_sql(self, expression: exp.Copy) -> str:
            self.unsupported("COPY query is not supported in SingleStore")
            return super().copy_sql(expression)

        def merge_sql(self, expression: exp.Merge) -> str:
            self.unsupported("MERGE query is not supported in SingleStore")
            return super().merge_sql(expression)

        @unsupported_args("totals")
        def group_sql(self, expression: exp.Group) -> str:
            group_by_all = expression.args.get("all")
            if group_by_all is True:
                modifier = "ALL"
            else:
                modifier = ""

            expressions = expression.expressions or []
            cubes = expression.args.get("cube") or []
            rollups = expression.args.get("rollup") or []
            grouping_sets = expression.args.get("grouping_sets") or []
            grouping_sets = [exprs for grouping_set in grouping_sets for exprs
                             in
                             grouping_set.expressions]

            grouping_sets_num = (1 if len(expressions) > 0 else 0) + len(
                cubes) + len(rollups) + len(grouping_sets) + (
                                    1 if modifier else 0)
            if grouping_sets_num > 1:
                self.unsupported(
                    "Multiple grouping sets are not supported in SingleStore")
            if grouping_sets_num == 0:
                self.unsupported(
                    "Empty GROUP BY is not supported in SingleStore")

            grouping = modifier
            if len(expressions) > 0:
                grouping = self.expressions(expression)
            elif len(cubes) > 0:
                grouping = self.sql(cubes[0])
            elif len(rollups) > 0:
                grouping = self.sql(rollups[0])
            elif len(grouping_sets) > 0:
                grouping = self.expressions(grouping_sets[0])

            return f" GROUP BY {grouping}"

        def cube_sql(self, expression: exp.Cube) -> str:
            expressions = self.expressions(expression, indent=False)
            if not expressions:
                self.unsupported("Empty CUBE is not supported in SingleStore")
            return f"CUBE {self.wrap(expressions)}"

        def rollup_sql(self, expression: exp.Rollup) -> str:
            expressions = self.expressions(expression, indent=False)
            if not expressions:
                self.unsupported("Empty ROLLUP is not supported in SingleStore")
            return f"ROLLUP {self.wrap(expressions)}"

        def groupingsets_sql(self, expression: exp.GroupingSets) -> str:
            self.unsupported(
                "GROUPING SETS clause is not supported in SingleStore")
            return super().groupingsets_sql(expression)

        def lambda_sql(self, expression: exp.Lambda,
                       arrow_sep: str = "->") -> str:
            self.unsupported(
                "Lambda functions are not supported in SingleStore")
            return super().lambda_sql(expression, arrow_sep)

        @unsupported_args("limit_options", "expressions")
        def limit_sql(self, expression: exp.Limit, top: bool = False) -> str:
            this = self.sql(expression, "this")

            args = [
                self._simplify_unless_literal(
                    e) if self.LIMIT_ONLY_LITERALS else e
                for e in
                (expression.args.get(k) for k in ("offset", "expression"))
                if e
            ]

            args_sql = ", ".join(self.sql(e) for e in args)

            return f"{this}{self.seg('LIMIT')} {args_sql}"

        def limitoptions_sql(self, expression: exp.LimitOptions) -> str:
            self.unsupported("LIMIT options are not supported in SingleStore")
            return ""

        def matchrecognize_sql(self, expression: exp.MatchRecognize) -> str:
            self.unsupported("MATCH_RECOGNIZE is not supported in SingleStore")
            return super().matchrecognize_sql(expression)

        def matchrecognizemeasure_sql(self,
                                      expression: exp.MatchRecognizeMeasure) -> str:
            self.unsupported("MATCH_RECOGNIZE is not supported in SingleStore")
            return super().matchrecognizemeasure_sql(expression)

        def final_sql(self, expression: exp.Final) -> str:
            self.unsupported("FINAL clause is not supported in SingleStore")
            return self.sql(expression, 'this')

        def order_sql(self, expression: exp.Order, flat: bool = False) -> str:
            this = self.sql(expression, "this")
            this = f"{this} " if this else this
            return self.op_expressions(f"{this}ORDER BY", expression,
                                       flat=this or flat)  # type: ignore

        def cluster_sql(self, expression: exp.Cluster) -> str:
            self.unsupported(
                "CLUSTER BY clause is not supported in SingleStore")
            return ""

        def distribute_sql(self, expression: exp.Distribute) -> str:
            self.unsupported(
                "DISTRIBUTE BY clause is not supported in SingleStore")
            return ""

        def sort_sql(self, expression: exp.Sort) -> str:
            self.unsupported(
                "SORT BY clause is not supported in SingleStore")
            return ""

        def withfill_sql(self, expression: exp.WithFill) -> str:
            self.unsupported(
                "WITH FILL clause is not supported in SingleStore")
            return ""

        def allowedvaluesproperty_sql(self,
                                      expression: exp.AllowedValuesProperty) -> str:
            self.unsupported("TAGs are not supported in SingleStore")
            return f"ALLOWED_VALUES {self.expressions(expression, flat=True)}"

        def partitionbyrangepropertydynamic_sql(self,
                                                expression: exp.PartitionByRangePropertyDynamic) -> str:
            self.unsupported(
                "PARTITION BY RANGE clause is not supported in SingleStore")
            return ""

        def partitionboundspec_sql(self,
                                   expression: exp.PartitionBoundSpec) -> str:
            self.unsupported(
                "PARTITION OF clause is not supported in SingleStore")
            return super().partitionboundspec_sql(expression)

        def querytransform_sql(self, expression: exp.QueryTransform) -> str:
            self.unsupported("TRANSFORM clause is not supported in SingleStore")
            return super().querytransform_sql(expression)

        def qualify_sql(self, expression: exp.Qualify) -> str:
            self.unsupported("QUALIFY clause is not supported in SingleStore")
            return super().qualify_sql(expression)

        def inputoutputformat_sql(self,
                                  expression: exp.InputOutputFormat) -> str:
            self.unsupported(
                "INPUTFORMAT and OUTPUTFORMAT clauses are not supported in SingleStore")
            return super().inputoutputformat_sql(expression)

        def reference_sql(self, expression: exp.Reference) -> str:
            self.unsupported(
                "Foreign keys are not supported in SingleStore")
            return super().reference_sql(expression)

        def withtablehint_sql(self, expression: exp.WithTableHint) -> str:
            self.unsupported(
                "Table hints are not supported in SingleStore")
            return ""

        @unsupported_args("target")
        def indextablehint_sql(self, expression: exp.IndexTableHint) -> str:
            this = f"{self.sql(expression, 'this')} INDEX"
            return f"{this} ({self.expressions(expression, flat=True)})"

        def historicaldata_sql(self, expression: exp.HistoricalData) -> str:
            self.unsupported("Historical data is not supported in SingleStore")
            return ""

        def put_sql(self, expression: exp.Put) -> str:
            self.unsupported("PUT query is not supported in SingleStore")
            return super().put_sql(expression)

        @unsupported_args("only", "partition", "version", "sample", "ordinality", "format", "pattern", "rows_from",
                          "changes")
        def table_sql(self, expression: exp.Table, sep: str = " AS ") -> str:
            table = self.table_parts(expression)
            alias = self.sql(expression, "alias")
            alias = f"{sep}{alias}" if alias else ""

            hints = self.expressions(expression, key="hints", sep=" ")
            hints = f" {hints}" if hints and self.TABLE_HINTS else ""
            pivots = self.expressions(expression, key="pivots", sep="", flat=True)
            joins = self.indent(
                self.expressions(expression, key="joins", sep="", flat=True), skip_first=True
            )
            laterals = self.expressions(expression, key="laterals", sep="")

            when = self.sql(expression, "when")
            if when:
                table = f"{table} {when}"

            return f"{table}{alias}{hints}{pivots}{joins}{laterals}"

        def version_sql(self, expression: exp.Version) -> str:
            self.unsupported("Versioned tables are not supported in SingleStore")
            return ""

        @unsupported_args("expressions", "wait")
        def lock_sql(self, expression: exp.Lock) -> str:
            lock_type = "FOR UPDATE" if expression.args["update"] else "FOR SHARE"
            if lock_type == "FOR SHARE":
                self.unsupported("Locking reads using 'FOR SHARE' is not supported in SingleStore")
                return ""

            return lock_type

        def tablesample_sql(
                self,
                expression: exp.TableSample,
                tablesample_keyword: t.Optional[str] = None) -> str:
            self.unsupported("TABLESAMPLE is not supported in SingleStore")
            return ""

        @unsupported_args("unpivot", "this", "include_nulls", "default_on_null")
        def pivot_sql(self, expression: exp.Pivot) -> str:
            if expression.unpivot:
                return super().pivot_sql(expression)

            expressions = self.expressions(expression, flat=True)
            if len(expression.expressions) > 1:
                self.unsupported("Multiple aggregations in PIVOT are not supported in SingleStore")

            group = self.sql(expression, "group")

            if expression.this:
                self.unsupported("Simplified PIVOT is not supported in SingleStore")
                return super().pivot_sql(expression)

            # PIVOT alias is required in SingleStore
            alias = self.sql(expression, "alias")
            alias = f" AS {alias}" if alias else self._next_name()

            fields = self.expressions(
                expression,
                "fields",
                sep=" ",
                dynamic=True,
                new_line=True,
                skip_first=True,
                skip_last=True,
            )

            return f"{self.seg('PIVOT')}({expressions} FOR {fields}{group}){alias}"

        def unpivotcolumns_sql(self, expression: exp.UnpivotColumns) -> str:
            self.unsupported("UNPIVOT query is not supported in SingleStore")
            return super().unpivotcolumns_sql(expression)

        @unsupported_args("except", "replace", "rename")
        def star_sql(self, expression: exp.Star) -> str:
            return "*"

        @unsupported_args("kind", "nested", "values")
        def datatype_sql(self, expression: exp.DataType) -> str:
            type_value = expression.this

            if type_value in self.UNSUPPORTED_TYPE_MAPPING:
                self.unsupported(f"Data type {type_value.value} is not supported in SingleStore")
                return self.UNSUPPORTED_TYPE_MAPPING.get(type_value)

            if (expression.is_type(exp.DataType.Type.VARCHAR)
                    and not expression.expressions
            ):
                # `VARCHAR` must always have a size - if it doesn't, we always generate `TEXT`
                return "TEXT"
            if (expression.is_type(exp.DataType.Type.VARBINARY)
                    and not expression.expressions
            ):
                # `VARBINARY` must always have a size - if it doesn't, we always generate `BLOB`
                return "BLOB"

            interior = self.expressions(expression, flat=True)
            interior = f"({interior})" if interior else ""

            type_sql = (
                self.TYPE_MAPPING.get(type_value, type_value.value)
                if isinstance(type_value, exp.DataType.Type)
                else type_value
            )

            unsigned = " UNSIGNED" if expression.this in self.UNSIGNED_TYPE_MAPPING else ""

            return f"{type_sql}{interior}{unsigned}"

        def pseudotype_sql(self, expression: exp.PseudoType) -> str:
            self.unsupported("Pseudo-Types are not supported in SingleStore")
            return "TEXT"

        def objectidentifier_sql(self, expression: exp.ObjectIdentifier) -> str:
            self.unsupported("Object Identifiers Types are not supported in SingleStore")
            return "INT"

        def intervalspan_sql(self, expression: exp.IntervalSpan) -> str:
            self.unsupported("INTERVAL spans are not supported in SingleStore")
            return f"{self.sql(expression, 'this')} TO {self.sql(expression, 'expression')}"

        @unsupported_args("chain")
        def commit_sql(self, expression: exp.Commit) -> str:
            return f"COMMIT"

        @unsupported_args("savepoint")
        def rollback_sql(self, expression: exp.Rollback) -> str:
            return f"ROLLBACK"

        @unsupported_args("exists", "only", "on_cluster", "not_valid", "options")
        def alter_sql(self, expression: exp.Alter) -> str:
            actions = expression.args["actions"]

            if isinstance(actions[0], exp.ColumnDef):
                actions = self.add_column_sql(expression)
            elif isinstance(actions[0], exp.Schema):
                actions = self.expressions(expression, key="actions", prefix="ADD COLUMN ")
            else:
                actions = self.expressions(expression, key="actions", flat=True)

            kind = self.sql(expression, "kind")

            return f"ALTER {kind} {self.sql(expression, 'this')} {actions}"

        def attachoption_sql(self, expression: exp.AttachOption) -> str:
            self.unsupported("ATTACH options are not supported in SingleStore")
            return ""

        def droppartition_sql(self, expression: exp.DropPartition) -> str:
            self.unsupported("ALTER TABLE DROP PARTITION is not supported in SingleStore")
            return super().droppartition_sql(expression)

        def replacepartition_sql(self, expression: exp.ReplacePartition) -> str:
            self.unsupported("ALTER TABLE REPLACE PARTITION is not supported in SingleStore")
            return (
                f"REPLACE {self.sql(expression.expression)} FROM {self.sql(expression, 'source')}"
            )

        def pivotany_sql(self, expression: exp.PivotAny) -> str:
            self.unsupported("PIVOT ANY [ ORDER BY ... ] is not supported in SingleStore")
            return f"ANY{self.sql(expression, 'this')}",

        # TODO: I didn't find this syntax in other databases. This should be investigated deeper
        def aliases_sql(self, expression: exp.Aliases) -> str:
            self.unsupported("Specifying multiple aliases in parrents is not supported in SingleStore")
            return super().aliases_sql(expression)

        def atindex_sql(self, expression: exp.AtIndex) -> str:
            self.unsupported("Arrays are not supported in SingleStore")
            return super().atindex_sql(expression)

        def attimezone_sql(self, expression: exp.AtTimeZone) -> str:
            self.unsupported("AT TIME ZONE is not supported in SingleStore")
            return self.sql(expression.this)

        @unsupported_args("on")
        def distinct_sql(self, expression: exp.Distinct) -> str:
            this = self.expressions(expression, flat=True)
            this = f" {this}" if this else ""

            return f"DISTINCT{this}"

        def forin_sql(self, expression: exp.ForIn) -> str:
            this = self.sql(expression, "this")
            expression_sql = self.sql(expression, "expression")
            return f"FOR {this} LOOP {expression_sql}"

        def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
            self.unsupported("IGNORE NULLS clause is not supported in SingleStore")
            return self.sql(expression, "this")

        def respectnulls_sql(self, expression: exp.IgnoreNulls) -> str:
            self.unsupported("RESPECT NULLS clause is not supported in SingleStore")
            return self.sql(expression, "this")

        def havingmax_sql(self, expression: exp.HavingMax) -> str:
            self.unsupported("HAVING NULL clause is not supported in SingleStore")
            return self.sql(expression, "this")

        @unsupported_args("kind")
        def use_sql(self, expression: exp.Use) -> str:
            this = self.sql(expression, "this") or self.expressions(expression, flat=True)
            this = f" {this}" if this else ""
            return f"USE{this}"

        def json_sql(self, expression: exp.JSON) -> str:
            self.unsupported("JSON testing functions are not supported in SingleStore")
            return super().json_sql(expression)

        def formatjson_sql(self, expression: exp.FormatJson) -> str:
            self.unsupported("FORMAT JSON clause is not supported in SingleStore")
            return self.sql(expression, "this")

        def jsoncolumndef_sql(self, expression: exp.JSONColumnDef) -> str:
            self.unsupported("JSON_TABLE function is not supported in SingleStore")
            return super().jsoncolumndef_sql(expression)

        def jsonschema_sql(self, expression: exp.JSONSchema) -> str:
            self.unsupported("JSON_TABLE function is not supported in SingleStore")
            return super().jsonschema_sql(expression)

        def openjsoncolumndef_sql(self, expression: exp.OpenJSONColumnDef) -> str:
            self.unsupported("OPENJSON function is not supported in SingleStore")
            return super().openjsoncolumndef_sql(expression)

        def xmlnamespace_sql(self, expression: exp.XMLNamespace) -> str:
            self.unsupported("XMLTABLE function is not supported in SingleStore")
            return super().xmlnamespace_sql(expression)

        @unsupported_args("on_condition")
        def jsonvalue_sql(self, expression: exp.JSONValue) -> str:
            path = self.sql(expression, "path")

            res = self.func("JSON_EXTRACT_STRING", expression.this, f"{path}")

            returning = self.sql(expression, "returning")
            if returning:
                return f"{res} :> {returning}"
            else:
                return res

        def jsonextractquote_sql(self, expression: exp.JSONExtractQuote) -> str:
            self.unsupported("QUOTES clause is not supported in SingleStore")
            return ""

        def scoperesolution_sql(self, expression: exp.ScopeResolution) -> str:
            self.unsupported("SCOPE_RESOLUTION is not supported in SingleStore")
            return super().scoperesolution_sql(expression)

        def stream_sql(self, expression: exp.Stream) -> str:
            self.unsupported("STREAM is not supported in SingleStore")
            return f"STREAM {self.sql(expression, 'this')}"

        def whens_sql(self, expression: exp.Whens) -> str:
            self.unsupported("WHEN MATCHED clause is not supported in SingleStore")
            return super().whens_sql(expression)

        @unsupported_args("options", "partition", "mode", "properties")
        def analyze_sql(self, expression: exp.Analyze) -> str:
            kind = self.sql(expression, "kind")
            kind = f" {kind}" if kind else ""
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            inner_expression = self.sql(expression, "expression")
            inner_expression = f" {inner_expression}" if inner_expression else ""
            return f"ANALYZE{kind}{this}{inner_expression}"

        def analyzestatistics_sql(self, expression: exp.AnalyzeStatistics) -> str:
            # SingleStore always updates statistic of all columns when running ANALYZE
            return ""

        def analyzehistogram_sql(self, expression: exp.AnalyzeHistogram) -> str:
            columns = self.expressions(expression)
            return f"COLUMNS {columns} ENABLE"

        def analyzelistchainedrows_sql(self, expression: exp.AnalyzeListChainedRows) -> str:
            self.unsupported("LIST CHAINED ROWS clause is not supported in SingleStore")
            return ""

        def analyzedelete_sql(self, expression: exp.AnalyzeDelete) -> str:
            return "DROP"

        def analyzevalidate_sql(self, expression: exp.AnalyzeValidate) -> str:
            self.unsupported("VALIDATE STRUCTURE clause is not supported in SingleStore")
            return ""

        def analyzecolumns_sql(self, expression: exp.AnalyzeColumns):
            return "COLUMNS ALL ENABLE"

        # TODO: investigate if MATCH_CONDITION can be translated into SingleStore syntax
        @unsupported_args("global", "match_condition")
        def join_sql(self, expression: exp.Join) -> str:
            op_sql = " ".join(
                op
                for op in (
                    expression.method,
                    expression.side,
                    expression.kind
                )
                if op
            )
            on_sql = self.sql(expression, "on")
            using = expression.args.get("using")

            if not on_sql and using:
                on_sql = csv(*(self.sql(column) for column in using))

            this = expression.this
            this_sql = self.sql(this)

            exprs = self.expressions(expression)
            if exprs:
                this_sql = f"{this_sql},{self.seg(exprs)}"

            if on_sql:
                on_sql = self.indent(on_sql, skip_first=True)
                space = self.seg(" " * self.pad) if self.pretty else " "
                if using:
                    on_sql = f"{space}USING ({on_sql})"
                else:
                    on_sql = f"{space}ON {on_sql}"
            elif not op_sql:
                if isinstance(this, exp.Lateral) and this.args.get("cross_apply") is not None:
                    return f" {this_sql}"

                return f", {this_sql}"

            if op_sql != "STRAIGHT_JOIN":
                op_sql = f"{op_sql} JOIN" if op_sql else "JOIN"

            return f"{self.seg(op_sql)} {this_sql}{on_sql}"

        def withschemabindingproperty_sql(self, expression: exp.WithSchemaBindingProperty) -> str:
            if isinstance(expression.this, exp.Var) and expression.this.this == "BINDING":
                return "SCHEMA_BINDING=ON"
            self.unsupported("Unsupported property withschemabinding")
            return ""

        def viewattributeproperty_sql(self, expression: exp.ViewAttributeProperty) -> str:
            if expression.this == "SCHEMABINDING":
                return "SCHEMA_BINDING=ON"
            self.unsupported("Unsupported property viewattribute")
            return ""

        @unsupported_args("concurrently", "refresh", "unique", "clustered")
        def create_sql(self, expression: exp.Create) -> str:
            kind = self.sql(expression, "kind")
            kind = self.dialect.INVERSE_CREATABLE_KIND_MAPPING.get(kind) or kind
            properties = expression.args.get("properties")
            properties_locs = self.locate_properties(properties) if properties else defaultdict()

            if isinstance(expression.this, exp.Schema):
                indexes = self.expressions(expression, key="indexes", indent=False, sep=" ")
                this = self.schema_sql(expression.this, indexes)
            else:
                this = self.sql(expression, "this")

            properties_sql = ""
            if properties_locs.get(exp.Properties.Location.POST_SCHEMA) or properties_locs.get(
                    exp.Properties.Location.POST_WITH
            ):
                properties_sql = self.sql(
                    exp.Properties(
                        expressions=[
                            *properties_locs[exp.Properties.Location.POST_SCHEMA],
                            *properties_locs[exp.Properties.Location.POST_WITH],
                        ]
                    )
                )

                if properties_locs.get(exp.Properties.Location.POST_SCHEMA):
                    properties_sql = self.sep() + properties_sql
                elif not self.pretty:
                    # Standalone POST_WITH properties need a leading whitespace in non-pretty mode
                    properties_sql = f" {properties_sql}"

            begin = " BEGIN" if expression.args.get("begin") else ""
            end = " END" if expression.args.get("end") else ""

            if isinstance(expression.expression, exp.Subquery):
                expression_sql = self.subquery_sql(expression.expression, " AS ", False)
            else:
                expression_sql = self.sql(expression, "expression")
            if expression_sql:
                expression_sql = f"{begin}{self.sep()}{expression_sql}{end}"

                if self.CREATE_FUNCTION_RETURN_AS or not isinstance(expression.expression, exp.Return):
                    postalias_props_sql = ""
                    if properties_locs.get(exp.Properties.Location.POST_ALIAS):
                        postalias_props_sql = self.properties(
                            exp.Properties(
                                expressions=properties_locs[exp.Properties.Location.POST_ALIAS]
                            ),
                            wrapped=False,
                        )
                    postalias_props_sql = f" {postalias_props_sql}" if postalias_props_sql else ""
                    expression_sql = f" AS{postalias_props_sql}{expression_sql}"

            replace = " OR REPLACE" if expression.args.get("replace") else ""

            postcreate_props_sql = ""
            if properties_locs.get(exp.Properties.Location.POST_CREATE):
                postcreate_props_sql = self.properties(
                    exp.Properties(expressions=properties_locs[exp.Properties.Location.POST_CREATE]),
                    sep=" ",
                    prefix=" ",
                    wrapped=False,
                )

            modifiers = "".join((replace, postcreate_props_sql))

            postexpression_props_sql = ""
            if properties_locs.get(exp.Properties.Location.POST_EXPRESSION):
                postexpression_props_sql = self.properties(
                    exp.Properties(
                        expressions=properties_locs[exp.Properties.Location.POST_EXPRESSION]
                    ),
                    sep=" ",
                    prefix=" ",
                    wrapped=False,
                )

            exists_sql = " IF NOT EXISTS" if expression.args.get("exists") else ""
            no_schema_binding = (
                " SCHEMA_BINDING=OFF" if expression.args.get("no_schema_binding") else ""
            )

            clone = self.sql(expression, "clone")
            clone = f" {clone}" if clone else ""

            properties_expression = f"{properties_sql}{expression_sql}"

            expression_sql = f"CREATE{modifiers}{no_schema_binding} {kind}{exists_sql} {this}{properties_expression}{postexpression_props_sql}{clone}"
            return self.prepend_ctes(expression, expression_sql)

        def schema_sql(self, expression: exp.Schema, indexes: str = None) -> str:
            this = self.sql(expression, "this")
            sql = self.schema_columns_sql(expression, indexes)
            return f"{this} {sql}" if this and sql else this or sql

        def schema_columns_sql(self, expression: exp.Schema, indexes: str = None) -> str:
            if expression.expressions:
                indexes = f"{self.sep(', ')}{indexes}" if indexes else ""
                return f"({self.sep('')}{self.expressions(expression)}{indexes}{self.seg(')', sep='')}"
            return ""

        @unsupported_args("zone")
        def timestrtotime_sql(self, expression: exp.TimeStrToTime)  -> str:
            datatype = exp.DataType.build(
                exp.DataType.Type.TIMESTAMP, expressions=[exp.DataTypeParam(this=exp.Literal.number(6))]
            )

            return self.sql(exp.cast(expression.this, datatype, dialect=self.dialect))
