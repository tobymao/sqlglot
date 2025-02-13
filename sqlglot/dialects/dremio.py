from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    rename_func,
    date_trunc_to_time,
    left_to_substring_sql,
    right_to_substring_sql,
    timestamptrunc_sql,
    timestrtotime_sql,
    explode_to_unnest_sql,
    regexp_extract_sql,
    strposition_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

class Dremio(Dialect):
    INDEX_OFFSET = 0
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    RESERVED_KEYWORDS = {
        "ABS", "ACCESS", "ACOS", "AES_DECRYPT", "AGGREGATE", "ALL", "ALLOCATE", "ALLOW", "ALTER",
        "ANALYZE", "AND", "ANY", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE", "ARE", "ARRAY",
        "ARRAY_AVG", "ARRAY_CAT", "ARRAY_COMPACT", "ARRAY_CONTAINS", "ARRAY_GENERATE_RANGE",
        "ARRAY_MAX", "ARRAY_MIN", "ARRAY_POSITION", "ARRAY_REMOVE", "ARRAY_SIZE", "ARRAY_SUM",
        "ARRAY_TO_STRING", "AS", "ASCII", "ASENSITIVE", "ASIN", "ASSIGN", "ASYMMETRIC", "AT",
        "ATAN", "ATAN2", "ATOMIC", "AUTHORIZATION", "AUTO", "AVG", "AVOID", "BASE64", "BATCH",
        "BEGIN", "BETWEEN", "BIGINT", "BIN", "BINARY_STRING", "BINARY", "BIT_AND", "BIT_LENGTH",
        "BIT_OR", "BITWISE_AND", "BITWISE_NOT", "BITWISE_OR", "BITWISE_XOR", "BLOB", "BOOL_AND",
        "BOOL_OR", "BOOLEAN", "BOTH", "BRANCH", "BY", "CACHE", "CALL", "CARDINALITY", "CASCADE",
        "CASE", "CAST", "CATALOG", "CBRT", "CEIL", "CEILING", "CHANGE", "CHAR", "CHAR_LENGTH",
        "CHARACTER", "CHARACTER_LENGTH", "CHECK", "CHR", "CLASSIFIER", "CLOB", "CLOSE", "CLOUD",
        "COALESCE", "COLUMN", "COLUMNS", "COMMIT", "COMPACT", "CONCAT", "CONDITION", "CONNECT",
        "CONSTRAINT", "CONTAINS", "CONVERT_FROM", "CONVERT_TO", "COPY", "CORR", "COS", "COSH",
        "COT", "COUNT", "COVAR_POP", "COVAR_SAMP", "CRC32", "CREATE", "CROSS", "CUBE",
        "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH",
        "CURRENT_ROLE", "CURRENT_ROW", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURRENT", "CURSOR", "CYCLE",
        "DATA", "DATABASES", "DATASETS", "DATE_ADD", "DATE_DIFF", "DATE_FORMAT", "DATE_PART",
        "DATE_SUB", "DATE_TRUNC", "DATE", "DATEDIFF", "DAY", "DAYOFMONTH", "DAYOFWEEK",
        "DAYOFYEAR", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFINE", "DEGREES",
        "DELETE", "DENSE_RANK", "DEREF", "DESCRIBE", "DETERMINISTIC", "DIMENSIONS", "DISALLOW",
        "DISCONNECT", "DISPLAY", "DISTINCT", "DOUBLE", "DROP", "DYNAMIC", "EACH", "ELEMENT",
        "ELSE", "EMPTY", "ENCODE", "END", "ENGINE", "EQUALS", "ESCAPE", "EVERY", "EXCEPT",
        "EXECUTE", "EXISTS", "EXP", "EXPLAIN", "EXTEND", "EXTERNAL", "EXTRACT", "FALSE",
        "FETCH", "FIELD", "FILTER", "FIRST_VALUE", "FLATTEN", "FLOAT", "FLOOR", "FOR",
        "FOREIGN", "FREE", "FROM", "FULL", "FUNCTION", "FUSION", "GEO_BEYOND", "GEO_DISTANCE",
        "GET", "GLOBAL", "GRANT", "GRANTS", "GREATEST", "GROUP", "GROUPING", "GROUPS",
        "HAVING", "HEX", "HISTORY", "HOUR", "IDENTITY", "IF", "ILIKE", "IMPORT", "IN",
        "INCLUDE", "INDEX", "INDICATOR", "INITCAP", "INNER", "INOUT", "INSERT", "INSTR",
        "INT", "INTEGER", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "IS", "ISDATE",
        "ISNUMERIC", "JOIN", "JSON_ARRAY", "JSON_OBJECT", "JSON_VALUE", "LAG", "LAST_DAY",
        "LAST_QUERY_ID", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEAST", "LEFT",
        "LENGTH", "LIKE", "LIMIT", "LISTAGG", "LN", "LOCAL", "LOCATE", "LOG", "LOWER",
        "LPAD", "LTRIM", "MANIFESTS", "MAP", "MASK", "MATCH", "MAX", "MD5", "MEDIAN",
        "MERGE", "MIN", "MINUTE", "MOD", "MONTH", "MUL", "NATURAL", "NDV", "NEW", "NEXT",
        "NO", "NONE", "NORMALIZE", "NOT", "NOW", "NTH_VALUE", "NTILE", "NULL", "NULLIF",
        "NUMERIC", "NVL", "OCCURRENCES_REGEX", "OFFSET", "ON", "ONE", "ONLY", "OPEN",
        "OPERATE", "OPTIMIZE", "OR", "ORDER", "OUT", "OUTER", "OVER", "OVERLAY", "OWNER",
        "PARSE_URL", "PARTITION", "PERCENT", "PERCENTILE_CONT", "PERCENTILE_DISC", "PI",
        "PIVOT", "POSITION", "POW", "POWER", "PRECEDES", "PRECISION", "PREPARE", "PRIMARY",
        "QUALIFY", "QUERY", "RADIANS", "RANDOM", "RANK", "RAW", "REAL", "RECORD_DELIMITER",
        "RECURSIVE", "REF", "REGEXP_EXTRACT", "RELEASE", "REMOVE", "RENAME", "REPEAT",
        "REPLACE", "RESET", "RESULT", "REVERSE", "REVOKE", "RIGHT", "ROLE", "ROLLBACK",
        "ROUND", "ROW", "ROW_NUMBER", "ROWS", "RPAD", "RTRIM", "SAVEPOINT", "SCHEMA",
        "SEARCH", "SECOND", "SELECT", "SEMI", "SENSITIVE", "SESSION_USER", "SET", "SHA",
        "SHA1", "SHA256", "SHA512", "SHOW", "SIMILAR", "SIN", "SIZE", "SMALLINT", "SOME",
        "SORT", "SOUNDEX", "SPLIT_PART", "SQRT", "START", "STATIC", "STATISTICS",
        "MERGE", "ALTER", "UPDATE", "DROP", "INSERT"}
    
    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "CONVERT_FROM": exp.Unhex.from_arg_list,
            "CONVERT_TO": exp.Hex.from_arg_list,
            "FLATTEN": lambda args: exp.Explode(this=seq_get(args, 0)),
        }