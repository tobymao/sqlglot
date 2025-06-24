from __future__ import annotations
from sqlglot import exp, generator, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType


class Exasol(Dialect):
    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "DATETIME2": TokenType.DATETIME2,
            "SMALLDATETIME": TokenType.SMALLDATETIME,
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

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            # https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.MEDIUMINT: "INT",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP WITH LOCAL TIME ZONE",
        }
