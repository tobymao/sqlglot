from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType

from sqlglot.transforms import (
    remove_unique_constraints,
    ctas_with_tmp_tables_to_create_tmp_view,
    preprocess,
)


class Materialize(Postgres):
    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,
            "MZ_NOW": exp.CurrentTimestamp.from_arg_list,
        }

    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "LIST": TokenType.LIST,
        }

    class Generator(Postgres.Generator):
        SUPPORTS_CREATE_TABLE_LIKE = False

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.Create: preprocess(
                [
                    remove_unique_constraints,
                    ctas_with_tmp_tables_to_create_tmp_view,
                ]
            ),
            exp.GeneratedAsIdentityColumnConstraint: lambda self, e: "",
            exp.PrimaryKeyColumnConstraint: lambda self, e: "",
            exp.AutoIncrementColumnConstraint: lambda self, e: "",
            exp.OnConflict: lambda self, e: "",
            exp.CurrentTimestamp: lambda *_: "MZ_NOW()",
        }

        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,
            exp.DataType.Type.TIMESTAMP: "MZ_TIMESTAMP",
            exp.DataType.Type.VARBINARY: "BYTEA",
            exp.DataType.Type.LIST: "TEXT",
        }
