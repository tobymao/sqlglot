from __future__ import annotations
from sqlglot import exp, generator, parser
from sqlglot.dialects.dialect import (
    Dialect,
    rename_func,
    trim_sql,
    strposition_sql,
    build_timetostr_or_tochar,
)

from sqlglot.generator import unsupported_args


class Exasol(Dialect):
    # https://docs.exasol.com/db/latest/sql_references/formatmodels.htm#DateTimeFormat
    # adapted from oracle dialect
    TIME_MAPPING = {
        "AM": "%p",  # Meridian indicator with or without periods
        "A.M.": "%p",  # Meridian indicator with or without periods
        "PM": "%p",  # Meridian indicator with or without periods
        "P.M.": "%p",  # Meridian indicator with or without periods
        "D": "%u",  # Day of week (1-7)
        "DAY": "%A",  # name of day
        "DD": "%d",  # day of month (1-31)
        "DDD": "%j",  # day of year (1-366)
        "DY": "%a",  # abbreviated name of day
        "HH12": "%I",  # Hour of day (1-12)
        "HH24": "%H",  # Hour of day (0-23)
        "HH": "%H",  # alias for HH24
        "IW": "%V",  # Calendar week of year (1-52 or 1-53), as defined by the ISO 8601 standard
        "MI": "%M",  # Minute (0-59)
        "MM": "%m",  # Month (01-12; January = 01)
        "MON": "%b",  # Abbreviated name of month
        "MONTH": "%B",  # Name of month
        "SS": "%S",  # Second (0-59)
        "WW": "%W",  # Week of year (1-53)
        "YY": "%y",  # 15
        "YYYY": "%Y",  # 2015
        "FF6": "%f",  # only 6 digits are supported in python formats
    }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "EDIT_DISTANCE": exp.Levenshtein.from_arg_list,
            "MID": exp.Substring.from_arg_list,
            "TO_CHAR": build_timetostr_or_tochar,  # TODO: handle nls_param (https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_char%20(datetime).htm)
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            # exasol allows e.g.: CREATE TABLE t VALUES (a VARCHAR [CHARACTER SET]? UTF8)
            # https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#StringDataType
            "UTF8": lambda self: self.expression(
                exp.CharacterSetColumnConstraint, this=exp.Var(this="UTF8")
            ),
            "ASCII": lambda self: self.expression(
                exp.CharacterSetColumnConstraint, this=exp.Var(this="ASCII")
            ),
        }

        # Whether string aliases are supported `SELECT COUNT(*) 'count'`
        # Exasol: supports this, tested with exasol db
        STRING_ALIASES = True  # default False

        # Whether query modifiers such as LIMIT are attached to the UNION node (vs its right operand)
        # Exasol: only possible to attach to right subquery (operand)
        MODIFIERS_ATTACHED_TO_SET_OP = False  # default True

        # Whether the 'AS' keyword is optional in the CTE definition syntax
        # https://docs.exasol.com/db/latest/sql/select.htm
        OPTIONAL_ALIAS_TOKEN_CTE = False

    class Generator(generator.Generator):
        QUERY_HINTS = False
        STRUCT_DELIMITER = ("(", ")")
        NVL2_SUPPORTED = False
        TABLESAMPLE_REQUIRES_PARENS = False
        TABLESAMPLE_SIZE_IS_ROWS = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        SUPPORTS_TO_NUMBER = True
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
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("EDIT_DISTANCE")
            ),
            exp.StrPosition: lambda self, e: strposition_sql(
                self,
                e,
                func_name="INSTR",
                supports_position=True if e.args.get("position") else False,
                supports_occurrence=True if e.args.get("position") else False,
            ),
            exp.Stuff: rename_func("INSERT"),
            exp.Substring: rename_func("SUBSTR"),
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.Trim: trim_sql,
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

        def alias_sql(self, expression):
            alias = expression.args.get("alias")
            if alias:
                alias_str = alias.this if isinstance(alias, exp.Identifier) else self.sql(alias)
                return f"{self.sql(expression, 'this')} {alias_str}"
            return self.sql(expression, "this")
