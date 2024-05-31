from sqlglot import exp
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType

from sqlglot.transforms import (
    remove_unique_constraints,
    ctas_with_tmp_tables_to_create_tmp_view,
    preprocess,
)


class Materialize(Postgres):
    # Define any specific normalization strategies or settings here
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,
            # Materialize-specific functions
            "MZ_NOW": exp.CurrentTimestamp.from_arg_list,
        }

    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            # Materialize-specific keywords
            "MAP": TokenType.MAP,
            "LIST": TokenType.ARRAY,
        }

    class Generator(Postgres.Generator):
        LIKE_PROPERTY_INSIDE_SCHEMA = False

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            # Materialize-specific SQL generation rules
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
            # Materialize-specific type mappings
            exp.DataType.Type.TIMESTAMP: "MZ_TIMESTAMP",
            exp.DataType.Type.MAP: "MAP",
            exp.DataType.Type.ARRAY: "LIST",
            exp.DataType.Type.VARBINARY: "BYTES",
        }
