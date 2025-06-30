from sqlglot import expressions as exp
from sqlglot import parser, generator, tokens
from sqlglot.dialects.dialect import Dialect


class Dremio(Dialect):
    SUPPORTS_USER_DEFINED_TYPES = False
    CONCAT_COALESCE = True
    TYPED_DIVISION = True
    SUPPORTS_SEMI_ANTI_JOIN = False
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_VALUES_DEFAULT = False

    class Parser(parser.Parser):
        LOG_DEFAULTS_TO_LN = True

    class Generator(generator.Generator):
        NVL2_SUPPORTED = False
        SUPPORTS_CONVERT_TIMEZONE = True
        INTERVAL_ALLOWS_PLURAL_FORM = False
        JOIN_HINTS = False
        LIMIT_ONLY_LITERALS = True
        MULTI_ARG_DISTINCT = False

        # https://docs.dremio.com/current/reference/sql/data-types/
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.SMALLINT: "INT",
            exp.DataType.Type.TINYINT: "INT",
            exp.DataType.Type.BINARY: "VARBINARY",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.NCHAR: "VARCHAR",
            exp.DataType.Type.CHAR: "VARCHAR",
            exp.DataType.Type.TIMESTAMPNTZ: "TIMESTAMP",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.ARRAY: "LIST",
            exp.DataType.Type.BIT: "BOOLEAN",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Reject time-zone–aware TIMESTAMPs, which Dremio does not accept
            """
            if expression.is_type(
                exp.DataType.Type.TIMESTAMPTZ,
                exp.DataType.Type.TIMESTAMPLTZ,
            ):
                self.unsupported("Dremio does not support time-zone-aware TIMESTAMP")

            return super().datatype_sql(expression)

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "//", ("/*", "*/")]
