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
    """
    The Dremio SQL dialect for SQLGlot.

    This class defines the necessary components to parse, generate, and tokenize
    SQL statements specific to Dremio, ensuring compatibility with SQLGlot’s
    framework for SQL translation.

    Key Responsibilities:
    - Define Dremio's reserved keywords to prevent conflicts during parsing.
    - Implement custom parsing rules for Dremio-specific SQL functions and expressions.
    - Configure the SQL generator to produce valid Dremio SQL syntax.
    - Set up a tokenizer to correctly recognize SQL tokens used in Dremio.

    The Dremio dialect consists of the following required subclasses:

    1. **Tokenizer (class Tokenizer)**:
        - Responsible for breaking down SQL statements into tokens.
        - Ensures Dremio-specific keywords (e.g., QUALIFY, ILIKE, ARRAY) are recognized.
        - Overrides default token mappings where necessary.

    2. **Parser (class Parser)**:
        - Defines how Dremio SQL constructs are mapped to SQLGlot expressions.
        - Maps built-in functions (e.g., DATE_ADD, FLATTEN, NDV) to SQLGlot AST nodes.
        - Ensures correct parsing of Dremio-specific syntax, such as QUALIFY and LISTAGG.

    3. **Generator (class Generator)**:
        - Converts parsed SQLGlot expressions back into valid Dremio SQL syntax.
        - Implements transformation rules for expressions that differ from standard SQL.
        - Ensures functions and operators are formatted correctly according to Dremio's syntax.

    Example:
        - Parsing: Converts `DATE_ADD(start_date, INTERVAL 5 DAY)` into SQLGlot’s `exp.DateAdd`
        - Generation: Translates `exp.DateAdd` back into `DATE_ADD(start_date, INTERVAL 5 DAY)`

    By implementing these components, the Dremio dialect enables seamless SQL parsing,
    translation, and generation within SQLGlot.
    """
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
        """
    The Dremio-specific SQL parser for SQLGlot.

    This class extends the base SQLGlot parser to handle Dremio's SQL syntax and functions.
    It defines a mapping between Dremio's built-in functions and SQLGlot's expression classes,
    ensuring proper parsing and transformation of SQL queries.

    Key Responsibilities:
    - Map Dremio functions to SQLGlot expressions (e.g., DATE_ADD → exp.DateAdd).
    - Support Dremio-specific syntax (e.g., QUALIFY, FLATTEN, CONVERT_FROM).
    - Enable conversion between Dremio SQL and other SQL dialects via SQLGlot.

    The FUNCTIONS dictionary maps function names to SQLGlot expressions, allowing
    SQLGlot to recognize and convert them accordingly.

    Example:
        "DATE_ADD": lambda args: exp.DateAdd(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0))

    This ensures that SQL using DATE_ADD in Dremio is correctly parsed and transformed
    when working with different SQL dialects.
        """
        FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "ABS": exp.Abs.from_arg_list,
        "ACOS": exp.Acos.from_arg_list,
        "ASIN": exp.Asin.from_arg_list,
        "ATAN": exp.Atan.from_arg_list,
        "ATAN2": exp.Atan2.from_arg_list,
        "CEIL": exp.Ceil.from_arg_list,
        "CEILING": exp.Ceil.from_arg_list,
        "CONVERT_FROM": exp.Unhex.from_arg_list,
        "CONVERT_TO": exp.Hex.from_arg_list,
        "COS": exp.Cos.from_arg_list,
        "COT": exp.Cot.from_arg_list,
        "DEGREES": exp.Degrees.from_arg_list,
        "EXP": exp.Exp.from_arg_list,
        "FLOOR": exp.Floor.from_arg_list,
        "GREATEST": lambda args: exp.Greatest(expressions=args),
        "LEAST": lambda args: exp.Least(expressions=args),
        "LOG": exp.Log.from_arg_list,
        "LOG10": lambda args: exp.Log(base=exp.Literal.number(10), this=seq_get(args, 0)),
        "LN": exp.Ln.from_arg_list,
        "MOD": exp.Mod.from_arg_list,
        "PI": exp.Pi.from_arg_list,
        "POWER": exp.Pow.from_arg_list,
        "POW": exp.Pow.from_arg_list,
        "RADIANS": exp.Radians.from_arg_list,
        "ROUND": exp.Round.from_arg_list,
        "SIGN": exp.Sign.from_arg_list,
        "SIN": exp.Sin.from_arg_list,
        "SQRT": exp.Sqrt.from_arg_list,
        "TAN": exp.Tan.from_arg_list,
        "TRUNC": exp.Trunc.from_arg_list,
        "TRUNCATE": exp.Trunc.from_arg_list,
        "EXTRACT": date_trunc_to_time,
        "DATE_ADD": lambda args: exp.DateAdd(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)),
        "DATE_SUB": lambda args: exp.DateSub(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)),
        "DATE_TRUNC": timestamptrunc_sql(),
        "STRPOS": lambda args: strposition_sql(None, args),
        "CHAR_LENGTH": exp.Length.from_arg_list,
        "LENGTH": exp.Length.from_arg_list,
        "LOWER": exp.Lower.from_arg_list,
        "UPPER": exp.Upper.from_arg_list,
        "LTRIM": exp.Ltrim.from_arg_list,
        "RTRIM": exp.Rtrim.from_arg_list,
        "TRIM": exp.Trim.from_arg_list,
        "CONCAT": exp.Concat.from_arg_list,
        "REPLACE": exp.Replace.from_arg_list,
        "SPLIT_PART": lambda args: exp.Split(this=seq_get(args, 0), delimiter=seq_get(args, 1), index=seq_get(args, 2)),
        "SUBSTRING": exp.Substring.from_arg_list,
        "LEFT": left_to_substring_sql,
        "RIGHT": right_to_substring_sql,
        "TRANSLATE": exp.Translate.from_arg_list,
        "REGEXP_EXTRACT": regexp_extract_sql,
        "FLATTEN": lambda args: exp.Explode(this=seq_get(args, 0)),
        "NDV": exp.ApproxDistinct.from_arg_list,
        "PERCENTILE_CONT": exp.PercentileCont.from_arg_list,
        "PERCENTILE_DISC": exp.PercentileDisc.from_arg_list,
        "LISTAGG": exp.GroupConcat.from_arg_list,
        "QUALIFY": lambda args: exp.Qualify(this=seq_get(args, 0)),
        # REGEXP_EXTRACT
        "REGEXP_EXTRACT": lambda args: exp.RegexpExtract(
            this=seq_get(args, 0),  # Input string
            expression=seq_get(args, 1),  # Regex pattern
            group=seq_get(args, 2),  # Group index (optional)
        ),

        # MAP_KEYS
        "MAP_KEYS": lambda args: exp.MapKeys(this=seq_get(args, 0)),

        # MAP_VALUES
        "MAP_VALUES": lambda args: exp.MapValues(this=seq_get(args, 0)),
    }

    class Tokenizer(tokens.Tokenizer):
    """
    The Dremio-specific SQL tokenizer.

    This class is responsible for breaking down SQL statements into individual tokens
    for parsing. It ensures that Dremio's specific SQL keywords, operators, and 
    function names are correctly identified and processed.

    Key Responsibilities:
    - Recognize Dremio-specific keywords such as QUALIFY, ILIKE, and ARRAY.
    - Properly tokenize data types like STRUCT, LIST, and MAP.
    - Handle special operators and syntax unique to Dremio.
    - Ensure all Dremio functions and expressions are correctly tokenized.

    This tokenizer extends the base SQLGlot tokenizer and overrides the KEYWORDS
    dictionary to include Dremio-specific SQL elements.
    """

    KEYWORDS = {
        **tokens.Tokenizer.KEYWORDS,
        
        # Comparison & Conditional
        "ILIKE": TokenType.ILIKE,
        "QUALIFY": TokenType.QUALIFY,
        "IS DISTINCT FROM": TokenType.IS_DISTINCT_FROM,
        "IS NOT DISTINCT FROM": TokenType.IS_NOT_DISTINCT_FROM,

        # Data Types
        "ARRAY": TokenType.ARRAY,
        "STRUCT": TokenType.STRUCT,
        "MAP": TokenType.MAP,
        "LIST": TokenType.ARRAY,  # Dremio treats LIST similar to ARRAY

        # Functions
        "FLATTEN": TokenType.EXPLODE,  # Tokenize FLATTEN as EXPLODE for handling nested arrays
        "EXTRACT": TokenType.EXTRACT,
        "PERCENTILE_CONT": TokenType.FUNCTION,
        "PERCENTILE_DISC": TokenType.FUNCTION,
        "NDV": TokenType.FUNCTION,
        "CONVERT_FROM": TokenType.FUNCTION,
        "CONVERT_TO": TokenType.FUNCTION,
        "DATE_ADD": TokenType.FUNCTION,
        "DATE_SUB": TokenType.FUNCTION,
        "DATE_TRUNC": TokenType.FUNCTION,
        "GREATEST": TokenType.FUNCTION,
        "LEAST": TokenType.FUNCTION,
        "STRPOS": TokenType.FUNCTION,
        "SPLIT_PART": TokenType.FUNCTION,
        "LISTAGG": TokenType.FUNCTION,
        "TRIM": TokenType.FUNCTION,
        "LTRIM": TokenType.FUNCTION,
        "RTRIM": TokenType.FUNCTION,
        "REGEXP_EXTRACT": TokenType.FUNCTION,
        "REGEXP_REPLACE": TokenType.FUNCTION,
        "TO_DATE": TokenType.FUNCTION,
        "TO_TIMESTAMP": TokenType.FUNCTION,

        # Joins
        "LEFT JOIN": TokenType.LEFT_JOIN,
        "RIGHT JOIN": TokenType.RIGHT_JOIN,
        "FULL JOIN": TokenType.FULL_JOIN,
        "INNER JOIN": TokenType.INNER_JOIN,
        "CROSS JOIN": TokenType.CROSS_JOIN,
        "LATERAL": TokenType.LATERAL,

        # Miscellaneous
        "IF": TokenType.IF,
        "CASE": TokenType.CASE,
        "WHEN": TokenType.WHEN,
        "THEN": TokenType.THEN,
        "ELSE": TokenType.ELSE,
        "END": TokenType.END,

                # Additional Data Types  
        "DECIMAL": TokenType.DECIMAL,  
        "NUMERIC": TokenType.NUMERIC,  
        "DOUBLE": TokenType.DOUBLE,  
        "FLOAT": TokenType.FLOAT,  
        "BOOLEAN": TokenType.BOOLEAN,  
        "BINARY": TokenType.BINARY,  
        "VARBINARY": TokenType.BINARY,  

        # String Functions  
        "CHAR_LENGTH": TokenType.FUNCTION,  
        "CHARACTER_LENGTH": TokenType.FUNCTION,  
        "POSITION": TokenType.FUNCTION,  
        "ASCII": TokenType.FUNCTION,  
        "REPEAT": TokenType.FUNCTION,  
        "REVERSE": TokenType.FUNCTION,  
        "INITCAP": TokenType.FUNCTION,  

        # Date/Time Functions  
        "NOW": TokenType.FUNCTION,  
        "CURRENT_DATE": TokenType.FUNCTION,  
        "CURRENT_TIME": TokenType.FUNCTION,  
        "CURRENT_TIMESTAMP": TokenType.FUNCTION,  
        "DAYOFWEEK": TokenType.FUNCTION,  
        "DAYOFMONTH": TokenType.FUNCTION,  
        "DAYOFYEAR": TokenType.FUNCTION,  
        "MONTH": TokenType.FUNCTION,  
        "YEAR": TokenType.FUNCTION,  
        "HOUR": TokenType.FUNCTION,  
        "MINUTE": TokenType.FUNCTION,  
        "SECOND": TokenType.FUNCTION,  

        # Aggregation Functions  
        "APPROX_COUNT_DISTINCT": TokenType.FUNCTION,  
        "CORR": TokenType.FUNCTION,  
        "COVAR_POP": TokenType.FUNCTION,  
        "COVAR_SAMP": TokenType.FUNCTION,  
        "VAR_POP": TokenType.FUNCTION,  
        "VAR_SAMP": TokenType.FUNCTION,  

        # Bitwise Functions  
        "BIT_AND": TokenType.FUNCTION,  
        "BIT_OR": TokenType.FUNCTION,  
        "BIT_XOR": TokenType.FUNCTION,  
        "BITWISE_AND": TokenType.FUNCTION,  
        "BITWISE_OR": TokenType.FUNCTION,  
        "BITWISE_XOR": TokenType.FUNCTION,  
        "BITWISE_NOT": TokenType.FUNCTION,  

        # JSON Functions  
        "JSON_ARRAY": TokenType.FUNCTION,  
        "JSON_OBJECT": TokenType.FUNCTION,  
        "JSON_VALUE": TokenType.FUNCTION,  

        # Table Functions  
        "UNNEST": TokenType.UNNEST,  
        "PIVOT": TokenType.PIVOT,  
        "UNPIVOT": TokenType.UNPIVOT,  

        # Miscellaneous  
        "QUALIFY": TokenType.QUALIFY,  
        "LAG": TokenType.FUNCTION,  
        "LEAD": TokenType.FUNCTION,  
        "FIRST_VALUE": TokenType.FUNCTION,  
        "LAST_VALUE": TokenType.FUNCTION,  
        "NTH_VALUE": TokenType.FUNCTION,  

        # REGEXP functions
        "REGEXP_EXTRACT": TokenType.FUNCTION,

        # MAP functions
        "MAP_KEYS": TokenType.FUNCTION,
        "MAP_VALUES": TokenType.FUNCTION,

    }

    class Generator(generator.Generator):
    """
    The Dremio-specific SQL generator.

    This class is responsible for converting SQLGlot expressions into properly formatted
    Dremio SQL syntax. It defines transformation rules for functions, expressions, and
    special syntax constructs that differ from standard SQL.

    Key Responsibilities:
    - Ensure Dremio SQL syntax is correctly formatted.
    - Define function translations where Dremio’s implementation differs from ANSI SQL.
    - Handle special expressions like DATE_ADD, QUALIFY, FLATTEN, and LISTAGG.

    This generator extends SQLGlot’s base generator and overrides necessary transformations.
    """

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,

        # Date/Time Functions
        exp.DateAdd: lambda self, e: f"DATE_ADD({self.sql(e.this)}, INTERVAL {self.sql(e.expression)} {self.sql(e.unit)})",
        exp.DateSub: lambda self, e: f"DATE_SUB({self.sql(e.this)}, INTERVAL {self.sql(e.expression)} {self.sql(e.unit)})",
        exp.DateTrunc: timestamptrunc_sql(),

        # String Functions
        exp.StrToDate: lambda self, e: f"CAST({self.sql(e.this)} AS DATE)",
        exp.StrToTime: timestrtotime_sql,
        exp.Length: lambda self, e: f"LENGTH({self.sql(e.this)})",
        exp.Lower: lambda self, e: f"LOWER({self.sql(e.this)})",
        exp.Upper: lambda self, e: f"UPPER({self.sql(e.this)})",
        exp.Trim: lambda self, e: f"TRIM({self.sql(e.this)})",
        exp.Ltrim: lambda self, e: f"LTRIM({self.sql(e.this)})",
        exp.Rtrim: lambda self, e: f"RTRIM({self.sql(e.this)})",
        exp.Concat: lambda self, e: f"CONCAT({self.expressions(e)})",
        exp.Replace: lambda self, e: f"REPLACE({self.sql(e.this)}, {self.sql(e.args.get('pattern'))}, {self.sql(e.args.get('replacement'))})",
        exp.Translate: lambda self, e: f"TRANSLATE({self.sql(e.this)}, {self.sql(e.args.get('from'))}, {self.sql(e.args.get('to'))})",

        # Array and Aggregation Functions
        exp.Explode: lambda self, e: f"FLATTEN({self.sql(e.this)})",
        exp.ApproxDistinct: lambda self, e: f"NDV({self.sql(e.this)})",
        exp.PercentileCont: lambda self, e: f"PERCENTILE_CONT({self.sql(e.this)}, {self.sql(e.args.get('quantile'))})",
        exp.PercentileDisc: lambda self, e: f"PERCENTILE_DISC({self.sql(e.this)}, {self.sql(e.args.get('quantile'))})",
        exp.ListAgg: lambda self, e: f"LISTAGG({self.sql(e.this)}, {self.sql(e.args.get('separator'))})",

        # Mathematical Functions
        exp.Greatest: lambda self, e: f"GREATEST({self.expressions(e)})",
        exp.Least: lambda self, e: f"LEAST({self.expressions(e)})",
        exp.Pow: lambda self, e: f"POWER({self.sql(e.this)}, {self.sql(e.expression)})",
        exp.Sqrt: lambda self, e: f"SQRT({self.sql(e.this)})",
        exp.Log: lambda self, e: f"LOG({self.sql(e.args.get('base'))}, {self.sql(e.this)})" if e.args.get("base") else f"LN({self.sql(e.this)})",

        # Regex Functions
        exp.RegexpExtract: lambda self, e: f"REGEXP_EXTRACT({self.sql(e.this)}, {self.sql(e.expression)})",
        exp.RegexpReplace: lambda self, e: f"REGEXP_REPLACE({self.sql(e.this)}, {self.sql(e.args.get('pattern'))}, {self.sql(e.args.get('replacement'))})",

        # Window & Analytical Functions
        exp.Qualify: lambda self, e: f"QUALIFY {self.sql(e.this)}",
        exp.Lag: lambda self, e: f"LAG({self.sql(e.this)})",
        exp.Lead: lambda self, e: f"LEAD({self.sql(e.this)})",
        exp.FirstValue: lambda self, e: f"FIRST_VALUE({self.sql(e.this)})",
        exp.LastValue: lambda self, e: f"LAST_VALUE({self.sql(e.this)})",
        exp.NthValue: lambda self, e: f"NTH_VALUE({self.sql(e.this)}, {self.sql(e.expression)})",

        # Conversion Functions
        exp.Unhex: lambda self, e: f"CONVERT_FROM({self.sql(e.this)}, 'HEX')",
        exp.Hex: lambda self, e: f"CONVERT_TO({self.sql(e.this)}, 'HEX')",

        # Additional Date/Time Functions  
        exp.Now: lambda self, e: "NOW()",  
        exp.CurrentDate: lambda self, e: "CURRENT_DATE",  
        exp.CurrentTime: lambda self, e: "CURRENT_TIME",  
        exp.CurrentTimestamp: lambda self, e: "CURRENT_TIMESTAMP",  
        exp.DayOfWeek: lambda self, e: f"DAYOFWEEK({self.sql(e.this)})",  
        exp.DayOfMonth: lambda self, e: f"DAYOFMONTH({self.sql(e.this)})",  
        exp.DayOfYear: lambda self, e: f"DAYOFYEAR({self.sql(e.this)})",  
        exp.Month: lambda self, e: f"MONTH({self.sql(e.this)})",  
        exp.Year: lambda self, e: f"YEAR({self.sql(e.this)})",  
        exp.Hour: lambda self, e: f"HOUR({self.sql(e.this)})",  
        exp.Minute: lambda self, e: f"MINUTE({self.sql(e.this)})",  
        exp.Second: lambda self, e: f"SECOND({self.sql(e.this)})",  

        # Additional String Functions  
        exp.InitCap: lambda self, e: f"INITCAP({self.sql(e.this)})",  
        exp.Repeat: lambda self, e: f"REPEAT({self.sql(e.this)}, {self.sql(e.expression)})",  
        exp.Reverse: lambda self, e: f"REVERSE({self.sql(e.this)})",  
        exp.Ascii: lambda self, e: f"ASCII({self.sql(e.this)})",  

        # Additional Aggregation Functions  
        exp.Median: lambda self, e: f"MEDIAN({self.sql(e.this)})",  
        exp.StddevPop: lambda self, e: f"STDDEV_POP({self.sql(e.this)})",  
        exp.StddevSamp: lambda self, e: f"STDDEV_SAMP({self.sql(e.this)})",  
        exp.VarPop: lambda self, e: f"VAR_POP({self.sql(e.this)})",  
        exp.VarSamp: lambda self, e: f"VAR_SAMP({self.sql(e.this)})",  

        # Additional Window Functions  
        exp.RowNumber: lambda self, e: "ROW_NUMBER() OVER ()",  
        exp.DenseRank: lambda self, e: "DENSE_RANK() OVER ()",  
        exp.PercentRank: lambda self, e: "PERCENT_RANK() OVER ()",  
        exp.CumeDist: lambda self, e: "CUME_DIST() OVER ()",  

        # Additional Mathematical Functions  
        exp.Corr: lambda self, e: f"CORR({self.sql(e.this)}, {self.sql(e.expression)})",  
        exp.CovarPop: lambda self, e: f"COVAR_POP({self.sql(e.this)}, {self.sql(e.expression)})",  
        exp.CovarSamp: lambda self, e: f"COVAR_SAMP({self.sql(e.this)}, {self.sql(e.expression)})",  

        # PIVOT and UNPIVOT  
        exp.Pivot: lambda self, e: f"PIVOT(({self.sql(e.args.get('pivot_clause'))}) FOR {self.sql(e.args.get('pivot_for_clause'))} IN ({self.expressions(e, key='pivot_in_clause')}))",
        exp.Unpivot: lambda self, e: f"UNPIVOT({self.sql(e.this)} INCLUDE NULLS)",

        # WITH (Common Table Expressions - CTEs)  
        exp.With: lambda self, e: f"WITH {self.sql(e.this)} {self.parenthesized('expressions', e, flat=True)} AS ({self.sql(e.expression)}) {self.sql(e.args.get('recursive'), '')}",

        exp.From: lambda self, e: f"FROM {self.expressions(e)}",
        exp.Filter: lambda self, e: f"{self.sql(e.this)} FILTER (WHERE {self.sql(e.expression)})" if e.this else f"FILTER (WHERE {self.sql(e.expression)})",
        exp.On: lambda self, e: f"ON {self.sql(e.this)}",
        exp.AtTime: lambda self, e: f"{self.sql(e.this)} AT {self.sql(e.expression)}",
        exp.Unnest: lambda self, e: f"UNNEST({self.sql(e.this)}) WITH ORDINALITY" if e.args.get("ordinality") else f"UNNEST({self.sql(e.this)})",
        exp.Hint: lambda self, e: f"/*+ {self.sql(e.this)} */",
        exp.Table: lambda self, e: f"TABLE({self.sql(e.this)}({self.sql(e.expression)}))",

        # REGEXP_EXTRACT
        exp.RegexpExtract: lambda self, e: f"REGEXP_EXTRACT({self.sql(e.this)}, {self.sql(e.expression)}{self.format_arg(e.args.get('group'))})",

        # MAP_KEYS
        exp.MapKeys: lambda self, e: f"MAP_KEYS({self.sql(e.this)})",

        # MAP_VALUES
        exp.MapValues: lambda self, e: f"MAP_VALUES({self.sql(e.this)})",



    }

