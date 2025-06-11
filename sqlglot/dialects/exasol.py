from __future__ import annotations
import typing as t
from typing import TYPE_CHECKING
from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    binary_from_function,
    build_date_delta,
    build_formatted_time,
    rename_func,
    trim_sql,
    timestrtotime_sql,
)
from sqlglot.helper import is_int, seq_get
from sqlglot.tokens import TokenType
from sqlglot.generator import unsupported_args

if TYPE_CHECKING:
    pass


def _str_to_time_sql(self: Exasol.Generator, expression: exp.StrToTime) -> str:
    this_expr = expression.args.get("this") or (
        expression.expressions[0] if expression.expressions else None
    )
    if not this_expr:
        raise ValueError("StrToTime is missing required 'this' argument")

    this = self.sql(this_expr)

    fmt_expr = expression.args.get("format")
    if fmt_expr and hasattr(fmt_expr, "this"):
        fmt_str = self._normalize_date_format(fmt_expr.this)
        result = f"TO_TIMESTAMP({this}, '{fmt_str}')"
    else:
        result = f"CAST({this} AS TIMESTAMP)"

    # Only print when needed
    if this_expr.name == "date":
        print(f"[DEBUG] Generating StrToTime for {this_expr}: {result}")
        return f"TO_DATE({this}, '{fmt_str}')"

    return result


def _str_to_date_sql(self: Exasol.Generator, expression: exp.StrToDate) -> str:
    """
    Handles TO_DATE(expr) translation for Exasol.
    Exasol does not support format strings in TO_DATE like Oracle or MySQL.
    If format is provided, assume it's already in the correct format and ignore it.
    """
    value_expr = expression.this
    value_sql = self.sql(value_expr)
    return f"TO_DATE({value_sql})"


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


def _date_diff_sql(self: Exasol.Generator, expression: exp.DateDiff) -> str:
    # TODO proper error handling
    # expression.unit can be exp.IntervalSpan
    # but exasol can only work with certain units
    assert isinstance(expression.unit, exp.Var)
    unit = expression.text("unit").upper()
    units = {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"}
    if unit not in units:
        # exasol cannot work with other units
        raise Exception()
    return self.func(f"{unit}S_BETWEEN", expression.this, expression.expression)


class Exasol(Dialect):
    ANNOTATORS = {
        **Dialect.ANNOTATORS,
    }

    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    TIME_MAPPING = {
        "YYYY": "%Y",  # 4-digit year
        "YY": "%y",  # 2-digit year
        "MM": "%m",  # 2-digit month
        "MON": "%b",  # Abbreviated month name (JAN)
        "MONTH": "%B",  # Full month name (JANUARY)
        "DD": "%d",  # 2-digit day of month
        "D": "%-d",  # Day of month (single digit without leading zero)
        "DY": "%a",  # Abbreviated day of week (MON)
        "DAY": "%A",  # Full day of week (MONDAY)
        "HH24": "%H",  # 24-hour clock (00-23)
        "HH": "%I",  # 12-hour clock (01-12)
        "HH12": "%I",  # Alias for HH
        "MI": "%M",  # Minutes (00-59)
        "SS": "%S",  # Seconds (00-59)
        "FF": "%f",  # Fractional seconds (microseconds)
        "AM": "%p",  # Meridian indicator (AM/PM)
        "PM": "%p",  # Meridian indicator (AM/PM)
        # Additional patterns if needed
        "TZH:TZM": "%z",  # Time zone offset (not always supported)
    }

    class Tokenizer(tokens.Tokenizer):
        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
        }
        SINGLE_TOKENS.pop("%")  # "%": TokenType.MOD not supported in exasol

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
            "TRUNC": exp.DateTrunc.from_arg_list,
            "DIV": binary_from_function(exp.IntDiv),
            "RANDOM": lambda args: exp.Rand(lower=seq_get(args, 0), upper=seq_get(args, 1)),
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
            "TO_CHAR": build_formatted_time(exp.TimeToStr, "exasol"),
            "TO_DATE": exp.StrToDate,
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

        MODIFIERS_ATTACHED_TO_SET_OP = False
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

        def parse_function(self):
            token = self._prev
            func = super().parse_function()
            func.set("original_name", token.text.upper())
            return func

        def _match_lfp_or_fsp(self) -> bool:
            if self._match(TokenType.L_PAREN):
                self._match(TokenType.NUMBER)
                self._match(TokenType.R_PAREN)
                return True
            return False

        def _handle_interval_day_or_year(
            self, this: t.Optional[exp.Expression]
        ) -> t.Optional[exp.DataType]:
            """
            INTERVAL DAY(lfp) TO SECOND(fsp)
            lfp = leading field precision (optional) 1 <= lfp <= 9, default = 2
            fsp = fractional second precision (optional) 0 <= fsp <= 9, default = 3

            INTERVAL YEAR(p) TO MONTH
            p = leading field precision (optional) 1 <= p <= 9, default = 2
            """
            if not (
                this and isinstance(this, exp.DataType) and isinstance(this.this, exp.Interval)
            ):
                return None  # or raise error if it should not proceed

            interval = this.this
            if isinstance(interval.unit, exp.Var) and (
                interval.unit.this == "DAY" or interval.unit.this == "YEAR"
            ):
                unit = interval.unit.this
                is_match = self._match_lfp_or_fsp()  # handle DAY(lfp) or YEAR(p)
                if is_match and self._match_text_seq("TO"):
                    interval_unit = exp.IntervalSpan(
                        this=self.expression(exp.Var, this=unit),
                        expression=self._parse_var(upper=True),
                    )
                    this = self.expression(
                        exp.DataType,
                        this=self.expression(exp.Interval, unit=interval_unit),
                    )
                    if unit == "DAY":
                        self._match_lfp_or_fsp()  # handle SECOND(fsp)
                if not is_match:
                    self.raise_error(f"Expected `TO` to follow `{unit}` in `INTERVAL` type")
            elif isinstance(interval.unit, exp.IntervalSpan):
                self._match_lfp_or_fsp()
            else:
                self.raise_error("Expected `DAY` or `YEAR` to follow `INTERVAL` type")

            return this

        def _parse_types(
            self,
            check_func: bool = False,
            schema: bool = False,
            allow_identifiers: bool = True,
        ) -> t.Optional[exp.Expression]:
            """
            https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#Intervaldatatypes
            """
            this = super()._parse_types(
                check_func=check_func,
                schema=schema,
                allow_identifiers=allow_identifiers,
            )
            return self._handle_interval_day_or_year(this)

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
            exp.Unicode: lambda self, e: self.unicode_sql(e),
            exp.Anonymous: lambda self, e: self._anonymous_func(e),
            exp.AtTimeZone: lambda self, e: self._transform_at_time_zone(e),
            exp.Any: lambda self, e: self.windowed_func("ANY", e),
            exp.ApproxDistinct: unsupported_args("accuracy")(
                rename_func("APPROXIMATE_COUNT_DISTINCT")
            ),
            exp.Avg: lambda self, e: self.windowed_func("AVG", e),
            exp.BitwiseAnd: rename_func("BIT_AND"),  # Good
            exp.BitwiseLeftShift: rename_func("BIT_LSHIFT"),
            exp.BitwiseNot: rename_func("BIT_NOT"),
            exp.BitwiseOr: rename_func("BIT_OR"),
            exp.BitwiseRightShift: rename_func("BIT_RSHIFT"),
            exp.BitwiseXor: rename_func("BIT_XOR"),
            exp.CommentColumnConstraint: lambda self, e: f"COMMENT IS {self.sql(e, 'this')}",
            exp.Command: lambda self, e: " ".join(self.sql(x) for x in e.expressions),
            exp.ConvertTimezone: lambda self, e: self.convert_tz_sql(e),
            exp.Count: lambda self, e: self.windowed_func("COUNT", e),
            exp.CovarPop: rename_func("COVAR_POP"),
            exp.CovarSamp: rename_func("COVAR_SAMP"),
            exp.CurrentDate: lambda self, e: "CURRENT_DATE",
            exp.CurrentSchema: lambda self, e: "CURRENT_SCHEMA",
            exp.CurrentUser: lambda self, e: "CURRENT_USER",
            exp.CurrentTimestamp: lambda self, e: (
                f"CURRENT_TIMESTAMP({self.sql(e, 'this')})"
                if e.args.get("this")
                else "CURRENT_TIMESTAMP"
            ),
            exp.DateDiff: _date_diff_sql,
            exp.DateTrunc: lambda self, e: self._date_trunc_sql(e),
            # exp.Explode: lambda self, e: self._explode_split_sql(e),
            # exp.Day
            # exp.DaysBetween: lambda self, e: f"DAYS_BETWEEN({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.Date: lambda self, e: f"DATE {self.sql(e, 'this')}",
            exp.Timestamp: lambda self, e: f"TIMESTAMP {self.sql(e, 'this')}",
            exp.Decode: lambda self,
            e: f"DECODE({', '.join(self.sql(arg) for arg in e.expressions)})",
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("EDIT_DISTANCE")
            ),
            exp.All: rename_func("EVERY"),
            exp.Extract: lambda self,
            e: f"EXTRACT({self.sql(e, 'expression')} FROM {self._timestamp_literal(e, 'this')})",
            exp.FirstValue: lambda self, e: self.windowed_func("FIRST_VALUE", e),
            exp.GroupConcat: lambda self, e: self.group_concat_sql(e),
            exp.MD5: lambda self, e: f"HASH_MD5({', '.join(self.sql(x) for x in e.expressions)})",
            exp.SHA: lambda self, e: f"HASH_SHA1({', '.join(self.sql(x) for x in e.expressions)})",
            exp.If: lambda self, e: (
                f"IF {self.sql(e, 'this')} THEN {self.sql(e, 'true')} ELSE {self.sql(e, 'false')} ENDIF"
            ),
            exp.Stuff: rename_func("INSERT"),
            exp.Is: lambda self, e: self.transform_is(e),
            exp.JSONBExtract: lambda self, e: self.jsonb_extract_sql(e),
            exp.JSONValue: lambda self, e: (
                f"JSON_VALUE({self.sql(e, 'this')}, {self.sql(e, 'path')}"
                + (
                    f" NULL ON EMPTY DEFAULT {self.sql(e, 'default')} ON ERROR"
                    if e.args.get("null_on_empty")
                    and e.args.get("default")
                    and e.args.get("on_error")
                    else ""
                )
                + ")"
            ),
            exp.Lag: lambda self, e: (
                f"LAG({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                if e.args.get("expression") is not None
                else f"LAG({self.sql(e, 'this')})"
            ),
            exp.Lead: lambda self, e: (
                f"LEAD({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                if e.args.get("expression") is not None
                else f"LEAD({self.sql(e, 'this')})"
            ),
            exp.Log: lambda self, e: f"LOG({self.sql(e, 'this')}, {self.sql(e, 'base')})",
            exp.Trim: trim_sql,
            exp.Mod: rename_func("MOD"),
            exp.NthValue: lambda self, e: (
                f"NTH_VALUE({self.sql(e, 'this')}, {self.sql(e, 'expression')})"
                + (" FROM LAST" if e.args.get("from_last") else " FROM FIRST")
                + (" RESPECT NULLS" if e.args.get("respect_nulls") else " IGNORE NULLS")
                + self.sql(e, "over")
            ),
            exp.Coalesce: rename_func("NVL"),  # NVL2 also
            exp.PercentileCont: lambda self, e: (
                f"PERCENTILE_CONT({self.sql(e, 'this')})"
                f" WITHIN GROUP ({self.sql(e, 'order').strip()})"
                f"{self.sql(e, 'over')}"
            ),
            exp.PercentileDisc: lambda self, e: (
                f"PERCENTILE_DISC({self.sql(e, 'this')})"
                f" WITHIN GROUP ({self.sql(e, 'order').strip()})"
                f"{self.sql(e, 'over')}"
            ),
            exp.Pow: rename_func("POWER"),
            exp.Rand: rename_func("RANDOM"),
            exp.RegexpExtract: rename_func("REGEXP_SUBSTR"),
            exp.RegexpReplace: rename_func("REGEXP_REPLACE"),
            exp.Round: lambda self, e: self.round_sql(e),
            exp.StrPosition: _string_position_sql,
            exp.SessionParameter: lambda self, e: self.session_parameter_sql(e),
            exp.Substring: lambda self, e: self.substring_sql(e),
            exp.StrToDate: _str_to_date_sql,
            exp.StrToTime: _str_to_time_sql,
            exp.ToChar: lambda self, e: self.to_char_sql(e),
            exp.TimeStrToDate: _str_to_date_sql,
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeToStr: lambda self, e: self.func(
                "TO_CHAR", e.this, self.format_time_to_string(e)
            ),
            exp.TsOrDsToDate: lambda self, e: self.sql(
                exp.StrToTime(this=e.this, format=e.args.get("format"))
            ),
            exp.ToNumber: lambda self, e: self.tonumber_sql(e),
            exp.TsOrDsToTimestamp: rename_func("TO_TIMESTAMP"),
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Variance: lambda self, e: self.variance_sql(e),
            exp.Upper: lambda self, e: f"{e.args.get('sql_name', 'UPPER')}({self.sql(e.this)})",
        }

        RESERVED_KEYWORDS = {
            "DATE",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.OnCluster: exp.Properties.Location.POST_NAME,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.ToTableProperty: exp.Properties.Location.POST_NAME,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def format_time_to_string(self, expression: exp.Expression) -> str:
            if isinstance(expression, exp.TimeToStr):
                fmt = expression.args.get("format")
                if fmt and hasattr(fmt, "this"):
                    return f"'{self._normalize_date_format(fmt.this)}'"
            return "'YYYY-MM-DD'"

        def wrap_to_date_if_date_col(self, expression):
            if isinstance(expression, exp.Column) and expression.name.lower() == "date":
                return exp.TimeStrToDate(
                    this=expression.copy(),
                    format=exp.Literal.string("YYYYMMDD"),
                )
            return expression

        def ordered_sql(self, expression):
            """
            Custom ORDERED SQL generation that wraps 'date' columns with TO_DATE.
            """
            expression = expression.copy()
            expression.set("this", self.wrap_to_date_if_date_col(expression.this))
            return super().ordered_sql(expression)

        def lt_sql(self, expression):
            expression = expression.copy()
            expression.set("this", self.wrap_to_date_if_date_col(expression.this))
            return super().lt_sql(expression)

        def lte_sql(self, expression):
            expression = expression.copy()
            expression.set("this", self.wrap_to_date_if_date_col(expression.this))
            return super().lte_sql(expression)

        def identifier_sql(self, expression: exp.Identifier) -> str:
            parent = expression.parent

            # Do NOT quote if it's the schema (db) or catalog
            if isinstance(parent, exp.Table) and (
                parent.args.get("db") is expression or parent.args.get("catalog") is expression
            ):
                return expression.this

            # Quote everything else (columns, table names, etc.)
            return f'"{expression.this}"'

        def _normalize_date_format(self, fmt: str) -> str:
            # If fmt contains Python style like '%Y%m%d', remove % and convert:
            replacements = {
                "%Y": "YYYY",
                "%y": "YY",
                "%m": "MM",
                "%d": "DD",
                "%a": "DY",
                "%b": "MON",
                "%B": "MONTH",
                "%H": "HH24",
                "%I": "HH12",
                "%M": "MI",
                "%S": "SS",
                "%p": "AM",
                # Add others if needed
            }
            for py_fmt, exa_fmt in replacements.items():
                fmt = fmt.replace(py_fmt, exa_fmt)
            return fmt

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            # Exasol does not support column-level comments in CREATE VIEW/CREATE TABLE
            expression = expression.copy()
            constraints = expression.args.get("constraints", [])
            filtered_constraints = [
                c for c in constraints if not isinstance(c.kind, exp.CommentColumnConstraint)
            ]
            expression.set("constraints", filtered_constraints)
            return super().columndef_sql(expression, sep)

        def _transform_at_time_zone(self, expression):
            # Strip AT TIME ZONE to just the base timestamp
            return expression.this

        def dateadd_sql(self, expression: exp.DateAdd) -> str:
            """
            Render ADD_DAYS, ADD_HOURS, etc., based on the unit in DateAdd expression.
            """
            unit = expression.args.get("unit")
            if not unit:
                raise ValueError("DateAdd requires a unit for Exasol ADD_* functions.")

            # Use the unit name to construct function name
            unit_name = unit.name.upper()  # e.g., DAY, HOUR
            func_name = f"ADD_{unit_name}s"  # Exasol expects plural (e.g., ADD_DAYS)

            return self.func(func_name, expression.this, expression.expression)

        def transform_is(self, e: exp.Is) -> str:
            this_sql = self.sql(e, "this")
            expression = e.args.get("expression")
            type_expr = e.args.get("type")

            if type_expr:
                # Use raw identifier name to avoid quotes like IS_"BOOLEAN"
                type_str = (
                    type_expr.this.upper()
                    if isinstance(type_expr, exp.Identifier)
                    else self.sql(type_expr).upper()
                )
                expr_sql = self.sql(expression) if expression else ""
                return f"IS_{type_str}({this_sql}{', ' + expr_sql if expr_sql else ''})"

            if isinstance(expression, exp.Null):
                return f"{this_sql} IS NULL"

            if isinstance(expression, exp.Boolean):
                return f"{this_sql} IS {self.sql(expression)}"

            if expression:
                return f"{this_sql} IS {self.sql(expression)}"

            return f"{this_sql} IS UNKNOWN"

        # def _explode_split_sql(self, explode: exp.Explode) -> str:
        #     """
        #     Transforms EXPLODE(SPLIT/REGEXP_SPLIT(col, ',')) into Exasol-compatible SQL using REGEXP_SUBSTR + CONNECT BY.
        #     """
        #     split_expr = explode.this

        #     # Accept both Split and RegexpSplit expressions
        #     if not isinstance(split_expr, (exp.Split, exp.RegexpSplit)):
        #         raise ValueError(
        #             "Only SPLIT or REGEXP_SPLIT supported inside EXPLODE for Exasol transformation."
        #         )

        #     column_sql = self.sql(split_expr.this)
        #     delimiter = self.sql(split_expr.expression) if split_expr.expression else "','"
        #     pattern = "[^" + delimiter.strip("'") + "]+"  # default regex pattern

        #     alias = explode.alias_or_name or "col"

        #     return f"""(
        #         SELECT REGEXP_SUBSTR({column_sql}, '{pattern}', 1, n.n) AS {alias}
        #         FROM (SELECT LEVEL AS n FROM dual CONNECT BY LEVEL <= 100) n
        #         WHERE REGEXP_SUBSTR({column_sql}, '{pattern}', 1, n.n) IS NOT NULL
        #     )"""

        # def lateral_sql(self, expression: exp.Lateral) -> str:
        #     this = expression.this

        #     # Only handle explode(split(...)) pattern
        #     if isinstance(this, exp.Explode):
        #         return self._explode_split_sql(this, expression.alias_or_name)

        #     raise ValueError("Unsupported LATERAL construct for Exasol.")

        # def _explode_split_sql(self, expression: exp.Explode, alias: str) -> str:
        #     alias = alias or "col"
        #     inner = expression.this

        #     # Get the source column/expression SQL from SPLIT or REGEXP_SPLIT
        #     if isinstance(inner, (exp.Split, exp.RegexpSplit)):
        #         source_sql = self.sql(inner.this)
        #     elif isinstance(inner, (exp.Anonymous, exp.Func)) and inner.name.upper() == "REGEXP_SPLIT":
        #         source_sql = self.sql(inner.expressions[0])
        #     else:
        #         raise ValueError("Only SPLIT or REGEXP_SPLIT supported inside EXPLODE for Exasol transformation.")

        #     # Prepare regexp extraction once
        #     regexp_substr_expr = f"REGEXP_SUBSTR({source_sql}, '[^,]+', 1, n.n)"

        #     # Generate the join SQL with sequence generator for exploding the string
        #     return f"""
        #     JOIN (
        #         SELECT {regexp_substr_expr} AS {alias}
        #         FROM (
        #             SELECT LEVEL AS n FROM dual CONNECT BY LEVEL <= 100
        #         ) n
        #         WHERE {regexp_substr_expr} IS NOT NULL
        #     ) {alias}
        #     """

        def _anonymous_func(self, e):
            func_name = e.this.upper()
            handlers = {
                "COLOGNE_PHONETIC": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "CONNECT_BY_ISCYCLE": lambda e: "CONNECT_BY_ISCYCLE",
                "CONNECT_BY_ISLEAF": lambda e: "CONNECT_BY_LEAF",
                "MIN_SCALE": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "MONTH": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "MINUTES_BETWEEN": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "MONTHS_BETWEEN": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "NOW": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "NPROC": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "NTILE": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "NULLIFZERO": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "NUMTODSINTERVAL": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "NUMTOYMINTERVAL": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "OCTET_LENGTH": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "PI": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "POSIX_TIME": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "RADIANS": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "RATIO_TO_REPORT": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "REGEXP_INSTR": lambda e: self._render_func_with_args(
                    e, min_arg_count=2, max_arg_count=5
                ),
                "REPLACE": lambda e: self._render_func_with_args(e, expected_arg_count=3),
                "REVERSE": lambda e: self._render_func_with_args(e, expected_arg_count=3),
                "ROWNUM": lambda e: "ROWNUM",
                "ROW_NUMBER": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "ROWID": lambda e: "ROWID",
                "SCOPE_USER": lambda e: "SCOPE_USER",
                "SECOND": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "SECONDS_BETWEEN": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "SESSIONTIMEZONE": lambda e: "SESSIONTIMEZONE",
                "SOUNDEX": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "SPACE": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "SYS_GUID": lambda e: self._render_func_with_args(e, expected_arg_count=0),
                "SYSDATE": lambda e: "SYSDATE",  # exp.CurrentTimestamp
                "SYSTIMESTAMP": lambda e: (
                    "SYSTIMESTAMP"
                    if not e.expressions
                    else self._render_func_with_args(e, expected_arg_count=1)
                ),
                "TO_DSINTERVAL": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "TO_YMINTERVAL": lambda e: self._render_func_with_args(
                    e,
                    expected_arg_count=1,  # Check out interval
                ),
                "TRANSLATE": lambda e: self._render_func_with_args(e, expected_arg_count=3),
                "TRUNC": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "TYPEOF": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "UNICODECHR": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "USER": lambda e: "USER",
                "VALUE2PROC": lambda e: self._render_func_with_args(e, expected_arg_count=1),
                "WIDTH_BUCKET": lambda e: self._render_func_with_args(e, expected_arg_count=4),
                "YEARS_BETWEEN": lambda e: self._render_func_with_args(e, expected_arg_count=2),
                "ZEROIFNULL": lambda e: self._render_func_with_args(e, expected_arg_count=1),
            }

            regr_funcs = [
                "REGR_AVGX",
                "REGR_AVGY",
                "REGR_COUNT",
                "REGR_INTERCEPT",
                "REGR_R2",
                "REGR_SLOPE",
                "REGR_SXX",
                "REGR_SXY",
                "REGR_SYY",
            ]
            st_funcs = [
                "ST_AREA",
                "ST_ASBINARY",
                "ST_ASTEXT",
                "ST_BOUNDARY",
                "ST_BUFFER",
                "ST_CENTROID",
                "ST_CONTAINS",
                "ST_CONVEXHULL",
                "ST_CROSSES",
                "ST_DIFFERENCE",
                "ST_DIMENSION",
                "ST_DISJOINT",
                "ST_DISTANCE",
                "ST_ENDPOINT",
                "ST_ENVELOPE",
                "ST_EQUALS",
                "ST_EXTERIORRING",
                "ST_GEOMETRYN",
                "ST_GEOMETRYTYPE",
                "ST_INTERIORRINGN",
                "ST_INTERSECTION",
                "ST_INTERSECTS",
                "ST_ISEMPTY",
                "ST_ISSIMPLE",
                "ST_LENGTH",
                "ST_NUMGEOMETRIES",
                "ST_NUMINTERIORRINGS",
                "ST_NUMPOINTS",
                "ST_OVERLAPS",
                "ST_POINTN",
                "ST_SETSRID",
                "ST_STARTPOINT",
                "ST_SYMDIFFERENCE",
                "ST_TOUCHES",
                "ST_TRANSFORM",
                "ST_UNION",
                "ST_WITHIN",
                "ST_X",
                "ST_Y",
                "ST_GEOMFROMTEXT",
                "ST_POINT",
            ]

            trig_funcs = [
                "ACOS",
                "ASIN",
                "ATAN",
                "ATAN2",
                "COS",
                "COSH",
                "COT",
                "SIN",
                "TAN",
                "TANH",
            ]

            analytic_funcs = [
                "RANK",
                "DENSE_RANK",
                "PERCENT_RANK",
                "CUME_DIST",
            ]
            for name in analytic_funcs:
                handlers[name] = lambda e, name=name: self._render_func_with_args(
                    e, expected_arg_count=0, ignore_extra_args_for={name}
                )
            for name in regr_funcs:
                handlers[name] = lambda e: self._render_func_with_args(e, expected_arg_count=2)
            for name in trig_funcs:
                expected_args = 2 if name == "ATAN2" else 1
                handlers[name] = lambda e, n=expected_args: self._render_func_with_args(
                    e, expected_arg_count=n
                )
            for name in st_funcs:
                handlers[name] = lambda e: self._render_func_with_args(e)

            if func_name in handlers:
                return handlers[func_name](e)
            return f"{func_name}({', '.join(self.sql(arg) for arg in e.expressions)})"

        def _render_func_with_args(
            self,
            e,
            expected_arg_count=None,
            min_arg_count=None,
            max_arg_count=None,
            ignore_extra_args_for=None,
        ):
            func_name = e.this.upper()
            args = list(e.expressions)
            ignore_extra_args_for = ignore_extra_args_for or set()

            # Truncate extra args if allowed
            if expected_arg_count is not None:
                if func_name in ignore_extra_args_for and len(args) > expected_arg_count:
                    args = args[:expected_arg_count]
                elif len(args) != expected_arg_count:
                    raise ValueError(
                        f"{func_name} expects exactly {expected_arg_count} arguments, got {len(args)}"
                    )

            if min_arg_count is not None and len(args) < min_arg_count:
                raise ValueError(
                    f"{func_name} expects at least {min_arg_count} arguments, got {len(args)}"
                )

            if max_arg_count is not None and len(args) > max_arg_count:
                raise ValueError(
                    f"{func_name} expects at most {max_arg_count} arguments, got {len(args)}"
                )

            formatted_args = ", ".join(self._format_func_arg(arg) for arg in args)
            return f"{func_name}({formatted_args})"

        def _format_func_arg(self, arg):
            if (
                isinstance(arg, exp.Literal)
                and isinstance(arg.this, str)
                and (arg.this.startswith("TIMESTAMP ") or arg.this.startswith("DATE "))
            ):
                return arg.this
            return self.sql(arg)

        def round_sql(self, expression):
            value = expression.this
            arg = expression.args.get("decimals")

            datetime_units = {
                "CC",
                "SCC",
                "YYYY",
                "SYYY",
                "YEAR",
                "SYEAR",
                "YYY",
                "YY",
                "Y",
                "IYYY",
                "IYY",
                "IY",
                "I",
                "Q",
                "MONTH",
                "MON",
                "MM",
                "RM",
                "WW",
                "IW",
                "W",
                "DDD",
                "DD",
                "J",
                "DAY",
                "DY",
                "D",
                "HH",
                "HH24",
                "HH12",
                "MI",
                "SS",
            }

            def is_datetime_literal(lit):
                return (
                    isinstance(lit, exp.Literal)
                    and lit.is_string
                    and (lit.this.startswith("DATE ") or lit.this.startswith("TIMESTAMP "))
                )

            # Render the first argument properly
            value_sql = value.this if is_datetime_literal(value) else self.sql(value)

            if arg is None:
                return f"ROUND({value_sql})"

            # If rounding a datetime, the second arg should be a valid datetime unit
            if is_datetime_literal(value):
                unit = (
                    arg.this.strip("'").upper() if isinstance(arg, exp.Literal) else self.sql(arg)
                )
                if unit not in datetime_units:
                    raise ValueError(f"Invalid ROUND datetime unit for Exasol: {unit}")
                return f"ROUND({value_sql}, '{unit}')"

            # Else, treat second argument as numeric (e.g., ROUND(42.123, 2))
            return f"ROUND({value_sql}, {self.sql(arg)})"

        def connect_sql(self, expression: exp.Connect) -> str:
            prior = "PRIOR " if expression.args.get("prior") else ""
            nocycle = " NOCYCLE" if expression.args.get("nocycle") else ""
            condition = self.sql(expression, "this")
            return self.seg(f"CONNECT BY{nocycle} {prior}{condition}")

        def dump_sql(self, expression: exp.Func) -> str:
            return self.func("DUMP", *expression.expressions)

        def _date_trunc_sql(self, e: exp.DateTrunc) -> str:
            unit_expr = e.args.get("unit") or e.this  # Fallback to .this if .unit not set
            unit = self.sql(unit_expr).strip("'").lower()
            value = self.sql(e.this)
            return f"DATE_TRUNC('{unit}', {value})"

        def _timestamp_literal(self, e, key):
            arg = e.args.get(key)
            if isinstance(arg, exp.Literal) and arg.args.get("prefix"):
                return f"{arg.args['prefix']} '{arg.this}'"
            return self.sql(e, key)

        def alias_sql(self, expression):
            alias = expression.args.get("alias")
            if alias:
                alias_str = alias.this if isinstance(alias, exp.Identifier) else self.sql(alias)
                return f"{self.sql(expression, 'this')} {alias_str}"
            return self.sql(expression, "this")

        def windowed_func(self, name, e):
            sql = f"{name}({self.sql(e, 'this')})"
            if e.args.get("over"):
                sql += f"{self.sql(e, 'over')}"
            return sql

        def convert_tz_sql(self, expression):
            this = self.sql(expression, "this")
            from_tz = self.sql(expression, "from_tz")
            to_tz = self.sql(expression, "to_tz")
            options = self.sql(expression, "options")
            if options:
                return f"CONVERT_TZ({this}, {from_tz}, {to_tz}, {options})"
            return f"CONVERT_TZ({this}, {from_tz}, {to_tz})"

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
                    and (arg.this.startswith("TIMESTAMP ") or arg.this.startswith("DATE "))
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

        def cos_sql(self, expression):
            return f"COS({self.sql(expression, 'this')})"

        def cosh_sql(self, expression):
            return f"COSH({self.sql(expression, 'this')})"

        def cot_sql(self, expression):
            return f"COT({self.sql(expression, 'this')})"

        def tonumber_sql(self, expression):
            return f"TO_NUMBER({', '.join(self.sql(arg) for arg in expression.expressions)})"

        def variance_sql(self, expression):
            func_name = expression.args.get("sql_name") or expression.__class__._sql_names[0]
            return f"{func_name}({self.sql(expression, 'this')})"

        def unicode_sql(self, expression):
            func_name = expression.args.get("sql_name") or expression.__class__._sql_names[0]
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
                + (f" SEPARATOR {self.sql(e, 'separator')}" if e.args.get("separator") else "")
                + ")"
                + self.sql(e, "over")
            )

        def jsonb_extract_sql(self, e):
            args_sql = ", ".join(self.sql(arg) for arg in e.expressions)
            emits = e.args.get("emits")
            emits_sql = ""

            if emits:
                emits_sql = (
                    " EMITS(" + ", ".join(f"{name} {datatype}" for name, datatype in emits) + ")"
                )

            return f"JSON_EXTRACT({self.sql(e, 'this')}, {args_sql}){emits_sql}"

        def convert_sql(self, expression):
            return f"CONVERT({self.sql(expression, 'this')}, {self.sql(expression, 'expression')})"

        def strtodate_sql(self, expression: exp.StrToDate) -> str:
            strtodate_sql = self.function_fallback_sql(expression)

            if not isinstance(expression.parent, exp.Cast):
                # StrToDate returns DATEs in other dialects (eg. postgres), so
                return self.cast_sql(exp.cast(expression, "DATE"))

            return strtodate_sql

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            this = expression.this
            to_type = expression.to

            # Handle casting STR_TO_DATE to DATETIME → just return the function
            if isinstance(this, exp.StrToDate) and to_type == exp.DataType.build("datetime"):
                return self.sql(this)

            # Handle numeric → VARCHAR using TO_CHAR
            if (
                to_type
                and isinstance(to_type, exp.DataType)
                and to_type.this
                in {
                    exp.DataType.Type.VARCHAR,
                    exp.DataType.Type.TEXT,
                }
                and isinstance(this, exp.Expression)
            ):
                return f"TO_CHAR({self.sql(this)})"

            return super().cast_sql(expression, safe_prefix=safe_prefix)

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

        def cte_sql(self, expression: exp.CTE) -> str:
            if expression.args.get("scalar"):
                this = self.sql(expression, "this")
                alias = self.sql(expression, "alias")
                return f"{this} AS {alias}"

            return super().cte_sql(expression)

        def after_limit_modifiers(self, expression: exp.Expression) -> t.List[str]:
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
            return (
                f"REPLACE {self.sql(expression.expression)} FROM {self.sql(expression, 'source')}"
            )

        def projectiondef_sql(self, expression: exp.ProjectionDef) -> str:
            return f"PROJECTION {self.sql(expression.this)} {self.wrap(expression.expression)}"
