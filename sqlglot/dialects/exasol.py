from __future__ import annotations
from sqlglot import exp, generator, parser
from sqlglot.dialects.dialect import Dialect, rename_func, binary_from_function
from sqlglot.helper import seq_get
from sqlglot.generator import unsupported_args


class Exasol(Dialect):
    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "BIT_AND": binary_from_function(exp.BitwiseAnd),
            "BIT_OR": binary_from_function(exp.BitwiseOr),
            "BIT_XOR": binary_from_function(exp.BitwiseXor),
            "BIT_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BIT_LSHIFT": binary_from_function(exp.BitwiseLeftShift),
            "BIT_RSHIFT": binary_from_function(exp.BitwiseRightShift),
            "EVERY": lambda args: exp.All(this=seq_get(args, 0)),
            "EDIT_DISTANCE": exp.Levenshtein.from_arg_list,
            "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                replacement=seq_get(args, 2),
                position=seq_get(args, 3),
                occurrence=seq_get(args, 4),
            ),
        }

    class Generator(generator.Generator):
        # https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#StringDataType
        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "VARCHAR",
            exp.DataType.Type.LONGBLOB: "VARCHAR",
            exp.DataType.Type.LONGTEXT: "VARCHAR",
            exp.DataType.Type.MEDIUMBLOB: "VARCHAR",
            exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
            exp.DataType.Type.TINYBLOB: "VARCHAR",
            exp.DataType.Type.TINYTEXT: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.VARBINARY: "VARCHAR",
        }

        # https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.MEDIUMINT: "INT",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Exasol supports a fixed default precision of 3 for TIMESTAMP WITH LOCAL TIME ZONE
            # and does not allow specifying a different custom precision
            if expression.is_type(exp.DataType.Type.TIMESTAMPLTZ):
                return "TIMESTAMP WITH LOCAL TIME ZONE"

            return super().datatype_sql(expression)

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/every.htm
            exp.All: rename_func("EVERY"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_and.htm
            exp.BitwiseAnd: rename_func("BIT_AND"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_or.htm
            exp.BitwiseOr: rename_func("BIT_OR"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_not.htm
            exp.BitwiseNot: rename_func("BIT_NOT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_lshift.htm
            exp.BitwiseLeftShift: rename_func("BIT_LSHIFT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_rshift.htm
            exp.BitwiseRightShift: rename_func("BIT_RSHIFT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_xor.htm
            exp.BitwiseXor: rename_func("BIT_XOR"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/every.htm
            exp.All: rename_func("EVERY"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/edit_distance.htm#EDIT_DISTANCE
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("EDIT_DISTANCE")
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/mod.htm
            exp.Mod: rename_func("MOD"),
            exp.RegexpReplace: unsupported_args("modifiers")(rename_func("REGEXP_REPLACE")),
        }
