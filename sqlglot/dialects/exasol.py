from __future__ import annotations
import typing as t
from sqlglot import exp, generator, parser, tokens, parse_one
from sqlglot.dialects.dialect import (
    Dialect,
    binary_from_function,
    build_date_delta,
    rename_func,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType
from sqlglot.generator import unsupported_args

DATEΤΙΜΕ_DELTA = t.Union[
    exp.DateAdd, exp.DateDiff, exp.DateSub, exp.TimestampSub, exp.TimestampAdd
]




class Tokenizer(tokens.Tokenizer):
    IDENTIFIER_ESCAPES = ['"']
    STRING_ESCAPES = ["'"]

    KEYWORDS = {
        **tokens.Tokenizer.KEYWORDS,
        "YEAR": TokenType.YEAR,
        "WITH LOCAL TIME ZONE": TokenType.TIME,
        "TO": TokenType.COMMAND,
        "HASHTYPE": TokenType.UUID,
    }


def _build_truncate(args: t.List) -> exp.Div:
    """
    exasol TRUNC[ATE] (number): https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/trunc[ate]%20(number).htm#TRUNC[ATE]_(number)
    example:
        exasol query: SELECT TRUNC(123.456,-2) -> 100
        generic query: SELECT FLOOR(123.456 * POWER(10,-2)) / POWER(10,-2) -> 100
    """
    number = seq_get(args, 0)
    truncate = seq_get(args, 1) or 0  # if no truncate arg then integer truncation
    sql = f"FLOOR({number} * POWER(10,{truncate})) / POWER(10,{truncate})"
    return parse_one(sql)


def _string_position_sql(self: Exasol.Generator, expression: exp.StrPosition) -> str:
    """
    Generate Exasol SQL for string position expressions.

    Exasol supports:
    - POSITION(substr IN string)
    - LOCATE(substr, string, position)
    - INSTR(string, substr, position, occurrence)

    sqlglot parses all into `exp.StrPosition`, and this method emits the correct SQL form
    based on which arguments are present.
    """
    this = expression.args.get("this")
    substr = expression.args.get("substr")
    position = expression.args.get("position")
    occurrence = expression.args.get("occurrence")
    # POSITION format
    if position is None and occurrence is None:
        return f"POSITION({self.sql(substr)} IN {self.sql(this)})"

    position_sql = self.sql(position)
    occurrence_sql = self.sql(occurrence)

    # LOCATE with optional position
    if occurrence is None:
        if position_sql == "1":
            return self.func("LOCATE", substr, this)
        return self.func("LOCATE", substr, this, position)

    # INSTR default simplification
    if position_sql == "1" and occurrence_sql == "1":
        return self.func("INSTR", this, substr)

    # Full INSTR
    return self.func("INSTR", this, substr, position, occurrence)



class Exasol(Dialect):
    ANNOTATORS = {
        **Dialect.ANNOTATORS,
    }

    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    TIME_MAPPING = {
        # --- Year ---
        "YYYY": "%Y",  # 4-digit year (e.g., 2025)
        "YYY": "%Y",  # 3-digit year (often same as YYYY in practice for TO_CHAR)
        "YY": "%y",  # Last 2 digits of year (e.g., 25)
        "Y": "%y",  # Last digit of year
        # --- Month ---
        "MM": "%m",  # Month (01-12)
        "MON": "%b",  # Abbreviated month name (e.g., JAN)
        "MONTH": "%B",  # Full month name (e.g., JANUARY)
        "RM": "%m",  # Roman numeral month (no direct strftime, maps to decimal)
        # --- Day ---
        "DD": "%d",  # Day of month (01-31)
        "DDD": "%j",  # Day of year (001-366)
        "DY": "%a",  # Abbreviated weekday name (e.g., MON)
        "DAY": "%A",  # Full weekday name (e.g., MONDAY)
        "D": "%w",  # Day of week (1-7, 1=Sunday, matches %w; for ISO 8601 day, see ID)
        # --- Hour ---
        "HH24": "%H",  # Hour (00-23)
        "HH12": "%I",  # Hour (01-12)
        "HH": "%I",  # (alias for HH12)
        "AM": "%p",  # AM/PM meridian indicator
        "PM": "%p",  # AM/PM meridian indicator
        # --- Minute ---
        "MI": "%M",  # Minute (00-59)
        # --- Second ---
        "SS": "%S",  # Second (00-59)
        # Fractional Seconds: Exasol uses FF[1-9]. strftime uses %f (microseconds)
        "FF": "%f",  # Maps to microseconds. Precision (FF1-FF9) might need truncation/padding.
        "FF1": "%f",
        "FF2": "%f",
        "FF3": "%f",  # Often used for milliseconds
        "FF4": "%f",
        "FF5": "%f",
        "FF6": "%f",  # Microseconds
        "FF7": "%f",
        "FF8": "%f",
        "FF9": "%f",
        # --- Timezone ---
        "TZH": "%z",  # Timezone hour offset (+/-HH)
        "TZM": "%z",  # Timezone minute offset (often combined with TZH)
        "TZHTZM": "%z",  # Combined timezone offset (+/-HHMM). Note: strftime %z is +/-HHMM or +/-HH:MM
        # --- ISO 8601 Specific ---
        "IYYY": "%G",  # ISO Year (4-digit)
        "IW": "%V",  # ISO Week of year (01-53)
        "ID": "%u",  # ISO Day of week (1-7, 1=Monday)
    }

    class Tokenizer(tokens.Tokenizer):
        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
        }
        SINGLE_TOKENS.pop("%") 
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }
        KEYWORDS.pop("DIV")

    class Parser(parser.Parser):
        # ADD_(DAYS | HOURS | MINUTES | MONTHS | SECONDS | WEEKS | YEARS)
        # https://docs.exasol.com/db/latest/sql_references/functions/scalarfunctions.htm
        DATE_ADD_FUNCTIONS = {
            f"ADD_{unit}S": build_date_delta(exp.DateAdd, None, unit)
            for unit in ["DAY", "HOUR", "MINUTE", "MONTH", "SECOND", "WEEK", "YEAR"]
        }

        NUMERIC_FUNCTIONS = {
            "DIV": binary_from_function(exp.IntDiv),
            "RANDOM": lambda args: exp.Rand(
                lower=seq_get(args, 0), upper=seq_get(args, 1)
            ),
            "TRUNCATE": _build_truncate,
            "TRUNC": _build_truncate,
        }

        STRING_FUNCTIONS = {
            "ASCII": exp.Unicode.from_arg_list,
            "REGEXP_SUBSTR": exp.RegexpExtract.from_arg_list,
            "EDIT_DISTANCE": exp.Levenshtein.from_arg_list,
            "INITCAP": lambda args: exp.Initcap(this=seq_get(args, 0)),
        }

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            **DATE_ADD_FUNCTIONS,
            **NUMERIC_FUNCTIONS,
            **STRING_FUNCTIONS,
        
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "CHAR": lambda self: self.expression(
                exp.Chr,
                expressions=self._parse_csv(self._parse_assignment),
                charset=self._match(TokenType.USING) and self._parse_var(),
            )
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            "UTF8": lambda self: self.expression(
                exp.CharacterSetColumnConstraint, this=exp.Var(this="UTF8")
            ),
            "ASCII": lambda self: self.expression(
                exp.CharacterSetColumnConstraint, this=exp.Var(this="ASCII")
            ),
        }
        STRING_ALIASES = True  
        MODIFIERS_ATTACHED_TO_SET_OP = False  # default True
        SET_OP_MODIFIERS = {"order", "limit", "offset"}
        OPTIONAL_ALIAS_TOKEN_CTE = False
        VALUES_FOLLOWED_BY_PAREN = True
        STRICT_CAST = True
        LOG_DEFAULTS_TO_LN = False
        ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = True
        TABLESAMPLE_CSV = False
        DEFAULT_SAMPLING_METHOD: t.Optional[str] = None
        TRIM_PATTERN_FIRST = False
        NO_PAREN_IF_COMMANDS = True
        INTERVAL_SPANS = True
        SUPPORTS_IMPLICIT_UNNEST = False
        SUPPORTS_PARTITION_SELECTION = False
        SET_REQUIRES_ASSIGNMENT_DELIMITER = True
        PARTITION_KEYWORDS = {"PARTITION", "SUBPARTITION"}
        AMBIGUOUS_ALIAS_TOKENS = (TokenType.LIMIT, TokenType.OFFSET)
        OPERATION_MODIFIERS: t.Set[str] = set()
        RECURSIVE_CTE_SEARCH_KIND = {"BREADTH", "DEPTH", "CYCLE"}
        MODIFIABLES = (exp.Query, exp.Table, exp.TableFromRows)
        PREFIXED_PIVOT_COLUMNS = False
        IDENTIFY_PIVOT_STRINGS = False
        JSON_ARROWS_REQUIRE_JSON_TYPE = False
        COLON_IS_VARIANT_EXTRACT = False
        WRAPPED_TRANSFORM_COLUMN_CONSTRAINT = True

    class Generator(generator.Generator):
        QUERY_HINTS = False
        STRUCT_DELIMITER = ("(", ")")
        NVL2_SUPPORTED = False
        TABLESAMPLE_REQUIRES_PARENS = False
        TABLESAMPLE_SIZE_IS_ROWS = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        SUPPORTS_TO_NUMBER = False
        JOIN_HINTS = False
        TABLE_HINTS = False
        GROUPINGS_SEP = ""
        SET_OP_MODIFIERS = False
        VALUES_AS_TABLE = False
        ARRAY_SIZE_NAME = "LENGTH"

        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "VARCHAR",
            exp.DataType.Type.CHAR: "CHAR",
            exp.DataType.Type.LONGBLOB: "VARCHAR",
            exp.DataType.Type.LONGTEXT: "VARCHAR",
            exp.DataType.Type.MEDIUMBLOB: "VARCHAR",
            exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
            exp.DataType.Type.TINYBLOB: "VARCHAR",
            exp.DataType.Type.TINYTEXT: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.VARBINARY: "VARCHAR",
            exp.DataType.Type.VARCHAR: "VARCHAR",
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathRoot,  # $
            exp.JSONPathKey,  # .key or ['key']
            exp.JSONPathSubscript,  # ['key'] or [0]
            exp.JSONPathWildcard,  # [*]
            exp.JSONPathUnion,  # ['key1','key2']
            exp.JSONPathSlice,  # [start:end:step]
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.MEDIUMINT: "INTEGER",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.DATETIME2: "TIMESTAMP",
            exp.DataType.Type.SMALLDATETIME: "TIMESTAMP",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP WITH LOCAL TIME ZONE",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Anonymous: lambda self, e: self.anonymous_func(e),
            exp.Unicode: lambda self, e: self.unicode_sql(e),
            exp.GroupConcat: lambda self, e: self.group_concat_sql(e),
            exp.Levenshtein: unsupported_args(
                "ins_cost", "del_cost", "sub_cost", "max_dist"
            )(rename_func("EDIT_DISTANCE")),
            exp.StrPosition: _string_position_sql,
            exp.RegexpExtract: rename_func("REGEXP_SUBSTR"),
            exp.RegexpReplace: rename_func("REGEXP_REPLACE"),
            exp.Substring: lambda self, e: self.substring_sql(e),
            exp.Stuff: rename_func("INSERT"),
            exp.ToChar: lambda self, e: self.to_char_sql(e),
            exp.ToNumber: lambda self, e: self.tonumber_sql(e),
            exp.Upper: lambda self, e: f"{e.args.get('sql_name', 'UPPER')}({self.sql(e.this)})",

        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.OnCluster: exp.Properties.Location.POST_NAME,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.ToTableProperty: exp.Properties.Location.POST_NAME,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def anonymous_func(self, e):
            func_name = e.this.upper()
            handlers = {
                "COLOGNE_PHONETIC": lambda e: self.render_func_with_args(
                    e, expected_arg_count=1
                ),
                "OCTET_LENGTH": lambda e: self.render_func_with_args(
                    e, expected_arg_count=1
                ),
                "REGEXP_INSTR": lambda e: self.render_func_with_args(
                    e, min_arg_count=2, max_arg_count=5
                ),
                "REPLACE": lambda e: self.render_func_with_args(
                    e, expected_arg_count=3
                ),
                "REVERSE": lambda e: self.render_func_with_args(
                    e, expected_arg_count=3
                ),
                "SOUNDEX": lambda e: self.render_func_with_args(
                    e, expected_arg_count=1
                ),
                "SPACE": lambda e: self.render_func_with_args(e, expected_arg_count=1),
                "TRANSLATE": lambda e: self.render_func_with_args(
                    e, expected_arg_count=3
                ),

               
            }

            if func_name in handlers:
                return handlers[func_name](e)
            return f"{func_name}({', '.join(self.sql(arg) for arg in e.expressions)})"

        def render_func_with_args(
            self, e, expected_arg_count=None, min_arg_count=None, max_arg_count=None
        ):
            arg_count = len(e.expressions)

            if expected_arg_count is not None and arg_count != expected_arg_count:
                raise ValueError(
                    f"{e.this} expects exactly {expected_arg_count} arguments, got {arg_count}"
                )

            if min_arg_count is not None and arg_count < min_arg_count:
                raise ValueError(
                    f"{e.this} expects at least {min_arg_count} arguments, got {arg_count}"
                )

            if max_arg_count is not None and arg_count > max_arg_count:
                raise ValueError(
                    f"{e.this} expects at most {max_arg_count} arguments, got {arg_count}"
                )

            def format_arg(arg):
                if (
                    isinstance(arg, exp.Literal)
                    and isinstance(arg.this, str)
                    and (
                        arg.this.startswith("TIMESTAMP ")
                        or arg.this.startswith("DATE ")
                    )
                ):
                    return arg.this
                return self.sql(arg)

            formatted_args = ", ".join(format_arg(arg) for arg in e.expressions)
            return f"{e.this.upper()}({formatted_args})"

        def dump_sql(self, expression: exp.Func) -> str:
            return self.func("DUMP", *expression.expressions)

        def alias_sql(self, expression):
            return f"{self.sql(expression, 'this')} {self.sql(expression, 'alias')}"

        def session_parameter_sql(self, e: exp.SessionParameter) -> str:
            kind = e.args.get("kind")
            return f"SESSION_PARAMETER({self.sql(e.this)}, {self.sql(kind)})"

        def substring_sql(self, expression: exp.Substring) -> str:
            if expression.args.get("_from_for"):
                return f"SUBSTRING({self.sql(expression.this)} FROM {self.sql(expression.args.get('start'))} FOR {self.sql(expression.args.get('length'))})"

            # Default to SUBSTR format
            return self.func(
                "SUBSTR",
                expression.this,
                expression.args.get("start"),
                expression.args.get("length"),
            )

        def to_char_sql(self, expression):
            def format_arg(arg):
                if (
                    isinstance(arg, exp.Literal)
                    and isinstance(arg.this, str)
                    and (
                        arg.this.startswith("TIMESTAMP ")
                        or arg.this.startswith("DATE ")
                    )
                ):
                    return arg.this
                return self.sql(arg)

            args = list(expression.expressions)
            formatted_args = [format_arg(arg) for arg in args]

            return f"TO_CHAR({', '.join(formatted_args)})"

        def convert_tz(self, e):
            args = [
                self.sql(e, "this"),
                self.sql(e, "timezone"),
                self.sql(e, "to_timezone"),
            ]
            if e.args.get("options"):
                args.append(self.sql(e, "options"))
            return f"CONVERT_TZ({', '.join(args)})"


        def tonumber_sql(self, expression):
            return f"TO_NUMBER({', '.join(self.sql(arg) for arg in expression.expressions)})"

        def unicode_sql(self, expression):
            func_name = (
                expression.args.get("sql_name") or expression.__class__._sql_names[0]
            )
            return f"{func_name}({self.sql(expression, 'this')})"

        def group_concat_sql(self, e):
            order = e.args.get("order")
            order_sql = ""
            if order:
                # order is a list of Ordered expressions
                order_sql = " ORDER BY " + ", ".join(self.sql(o) for o in order)

            return (
                "GROUP_CONCAT("
                + ("DISTINCT " if e.args.get("distinct") else "")
                + self.sql(e, "this")
                + order_sql
                + (
                    f" SEPARATOR {self.sql(e, 'separator')}"
                    if e.args.get("separator")
                    else ""
                )
                + ")"
                + self.sql(e, "over")
            )
