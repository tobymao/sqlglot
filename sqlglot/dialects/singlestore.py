import this

from sqlglot import Dialect, generator, Tokenizer, TokenType, tokens
from sqlglot.dialects.dialect import NormalizationStrategy, no_ilike_sql, \
    bool_xor_sql, rename_func, count_if_to_sum, unit_to_str
import typing as t
import re
from sqlglot import exp
from sqlglot.generator import ESCAPED_UNICODE_RE, unsupported_args


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
        IDENTIFIERS = ['"', '`']
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
            return rename_func("TIMESTAMPDIFF")(self, expression)

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
                "OpenJSON function is not supported in SingleStore")
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
