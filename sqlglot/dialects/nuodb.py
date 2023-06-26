from sqlglot import exp
from sqlglot import generator
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType


class NuoDB(Dialect):

    #* Refer to http://nuocrucible/browse/NuoDB/Omega/Parser/SQL.l?r=5926eff6ff3e077c09c390c7acc4649c81b1d27b&r=daafc63d9399e66689d0990a893fbddd115df89f&r=6ef1d2d9e253f74515bf89625434b605be6486ea
    #? Revise so all tokens are considered
    #? Built-in Function Names excluded
    class Tokenizer(Tokenizer):
        QUOTES = [
            "'", '"',
            "N'",    # unicodequote 
            #?
            ]
        COMMENTS = ["--", "//", ("/*", "*/")]
        IDENTIFIERS = ["`", '"'] #?
        STRING_ESCAPES = ["\\"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
            "BITS": TokenType.BIT,   #? Confirm "BIT" is the same as "BITS"
            "BOTH": TokenType.BOTH,
            "BREAK": TokenType.BREAK,
            "BY": TokenType.BY, #? Is this keyword required? Not already added in conjunction with other keywords?
            "CASCADE": TokenType.CASCADE,
            "CATCH": TokenType.CATCH,
            "CONTAINING": TokenType.CONTAINING,
            "CURRENT": TokenType.CURRENT,
            "END_FOR": TokenType.END_FOR,
            "END_FUNCTION": TokenType.END_FUNCTION,
            "END_IF": TokenType.END_IF,
            "END_PROCEDURE": TokenType.END_PROCEDURE,
            "END_TRIGGER": TokenType.END_TRIGGER,
            "END_TRY": TokenType.END_TRY,
            "END_WHILE": TokenType.END_WHILE,
            "FOREIGN": TokenType.FOREIGN, #? Separate keyword from FOREIGN KEY?
            "GENERATED": TokenType.GENERATED,
            "GROUP": TokenType.GROUP, #? Separate keyword from GROUP BY?
            "IDENTITY": TokenType.IDENTITY,
            "INOUT": TokenType.INOUT,
            "KEY": TokenType.KEY, #? Separate keyword from FOREIGN KEY?
            "LEADING": TokenType.LEADING,
            "NATIONAL": TokenType.NATIONAL,
            "NCLOB": TokenType.TEXT, #? Seems like Clob is set to be as type TEXT, so same for NCLOB?
            # NEXT_VALUE #? NEXT VALUE FOR is already considered
            "OCTETS": TokenType.OCTETS,
            "OFF": TokenType.OFF,
            "ONLY": TokenType.ONLY,
            # ORDER #? ORDER BY is included
            "OUT": TokenType.OUT,
            # PRIMARY #? PRIMARY KEY is included
            "RECORD_BATCHING": TokenType.RECORD_BATCHING,
            "RECORD_NUMBER": TokenType.RECORD_NUMBER,
            "RESTRICT": TokenType.RESTRICT,
            "RETURN": TokenType.RETURNING,  #? Same as RETURNING type?
            "STARTING": TokenType.STARTING,
            "THROW": TokenType.THROW,
            "TO": TokenType.TO,
            "TRAILING": TokenType.TRAILING,
            "UNKNOWN": TokenType.UNKNOWN,
            "VAR": TokenType.VAR, #? Is VAR same as any of NCHAR, VARCHAR, NVARCHAR?
            "VER": TokenType.VER,
            "WHILE": TokenType.WHILE,
            "_RECORD_ID": TokenType._RECORD_ID,
            "_RECORD_PARTITIONID": TokenType._RECORD_PARTITIONID,
            "_RECORD_SEQUENCE": TokenType._RECORD_SEQUENCE,
            "_RECORD_TRANSACTION": TokenType._RECORD_TRANSACTION
        }

        #? COMMANDS?

    class Generator(Generator):
        TRANSFORMS = {exp.Array: lambda self, e: f"[{self.expressions(e)}]"}

        TYPE_MAPPING = generator.Generator.TYPE_MAPPING.copy()

        #? Should all of these datatypes that NuoDB doesn't support be popped?
        #? Seems like updating the TYPE_MAPPING such as the following is a good approach
        '''
        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.SMALLINT: "INT64",
            exp.DataType.Type.INT: "INT64",
            exp.DataType.Type.BIGINT: "INT64",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.FLOAT: "FLOAT64",
            exp.DataType.Type.DOUBLE: "FLOAT64",
            exp.DataType.Type.BOOLEAN: "BOOL",
            exp.DataType.Type.TEXT: "STRING",
        }
        '''
        # TYPE_MAPPING.pop(exp.DataType.Type.BIGDECIMAL)
        # TYPE_MAPPING.pop(exp.DataType.Type.BIGSERIAL)
        #? BIT == BYTE ?
        # TYPE_MAPPING.pop(exp.DataType.Type.DATETIME)
        # TYPE_MAPPING.pop(exp.DataType.Type.DATETIME64)
        # TYPE_MAPPING.pop(exp.DataType.Type.ENUM)
        # TYPE_MAPPING.pop(exp.DataType.Type.INT4RANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.INT4MULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.INT8RANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.INT8MULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.NUMRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.NUMMULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.TSRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.TSMULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.TSTZRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.TSTZMULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.DATERANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.DATEMULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.DECIMAL)
        # TYPE_MAPPING.pop(exp.DataType.Type.GEOGRAPHY)
        # TYPE_MAPPING.pop(exp.DataType.Type.GEOMETRY)
        # TYPE_MAPPING.pop(exp.DataType.Type.HLLSKETCH)
        #? STILL NEED TO ADD OTHERS
