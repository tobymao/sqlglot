from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType


class Vertica(Dialect):
    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"'] # Strings can be delimited by either single or double quotes
        IDENTIFIERS = ["`"] # Identifiers can be delimited by backticks

        # Include Vertica specific keywords
        KEYWORDS = {
             **Tokenizer.KEYWORDS,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
            "VARCHAR": TokenType.STRING,  # Vertica uses VARCHAR for strings
            "DATE": TokenType.DATE,  # Vertica specific data type keywords
            "TIMESTAMP": TokenType.TIMESTAMP,  # Vertica specific data type keywords

        }

    class Generator(Generator):

        TRANSFORMS = {
            exp.Array: lambda self, e: f"[{self.expressions(e)}]",

     }

 # Mapping SqlGlot data types to Vertica data types
        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.SMALLINT: "INT64",
            exp.DataType.Type.INT: "INT64",
            exp.DataType.Type.BIGINT: "INT64",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.FLOAT: "FLOAT64",
            exp.DataType.Type.DOUBLE: "FLOAT64",
            exp.DataType.Type.BOOLEAN: "BOOL",
            exp.DataType.Type.TEXT: "STRING",  # Vertica uses VARCHAR for strings
            exp.DataType.Type.DATE: "DATE",  # mappings for Vertica specific data types
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",  # mappings for Vertica specific data types

             }




