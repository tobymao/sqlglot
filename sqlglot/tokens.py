from __future__ import annotations

import os

import typing as t

from sqlglot.errors import SqlglotError, TokenError
from sqlglot.trie import new_trie

# Import Token and TokenType from tokenizer_core (compiled with mypyc)
from sqlglot.tokenizer_core import Token, TokenType

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


try:
    from sqlglotrs import (  # type: ignore
        TokenizerDialectSettings as RsTokenizerDialectSettings,
    )

    USE_RS_TOKENIZER = os.environ.get("SQLGLOTRS_TOKENIZER", "1") == "1"
except ImportError:
    USE_RS_TOKENIZER = False


_ALL_TOKEN_TYPES = list(TokenType)
_TOKEN_TYPE_TO_INDEX = {token_type: i for i, token_type in enumerate(_ALL_TOKEN_TYPES)}


class _Tokenizer(type):
    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)

        def _convert_quotes(arr: t.List[str | t.Tuple[str, str]]) -> t.Dict[str, str]:
            return dict(
                (item, item) if isinstance(item, str) else (item[0], item[1]) for item in arr
            )

        def _quotes_to_format(
            token_type: TokenType, arr: t.List[str | t.Tuple[str, str]]
        ) -> t.Dict[str, t.Tuple[str, TokenType]]:
            return {k: (v, token_type) for k, v in _convert_quotes(arr).items()}

        klass._QUOTES = _convert_quotes(klass.QUOTES)
        klass._IDENTIFIERS = _convert_quotes(klass.IDENTIFIERS)

        klass._FORMAT_STRINGS = {
            **{
                p + s: (e, TokenType.NATIONAL_STRING)
                for s, e in klass._QUOTES.items()
                for p in ("n", "N")
            },
            **_quotes_to_format(TokenType.BIT_STRING, klass.BIT_STRINGS),
            **_quotes_to_format(TokenType.BYTE_STRING, klass.BYTE_STRINGS),
            **_quotes_to_format(TokenType.HEX_STRING, klass.HEX_STRINGS),
            **_quotes_to_format(TokenType.RAW_STRING, klass.RAW_STRINGS),
            **_quotes_to_format(TokenType.HEREDOC_STRING, klass.HEREDOC_STRINGS),
            **_quotes_to_format(TokenType.UNICODE_STRING, klass.UNICODE_STRINGS),
        }

        if "BYTE_STRING_ESCAPES" not in klass.__dict__:
            klass.BYTE_STRING_ESCAPES = klass.STRING_ESCAPES.copy()

        klass._STRING_ESCAPES = set(klass.STRING_ESCAPES)
        klass._BYTE_STRING_ESCAPES = set(klass.BYTE_STRING_ESCAPES)
        klass._ESCAPE_FOLLOW_CHARS = set(klass.ESCAPE_FOLLOW_CHARS)
        klass._IDENTIFIER_ESCAPES = set(klass.IDENTIFIER_ESCAPES)
        klass._COMMENTS = {
            **dict(
                (comment, None) if isinstance(comment, str) else (comment[0], comment[1])
                for comment in klass.COMMENTS
            ),
            "{#": "#}",  # Ensure Jinja comments are tokenized correctly in all dialects
        }
        if klass.HINT_START in klass.KEYWORDS:
            klass._COMMENTS[klass.HINT_START] = "*/"

        klass._KEYWORD_TRIE = new_trie(
            key.upper()
            for key in (
                *klass.KEYWORDS,
                *klass._COMMENTS,
                *klass._QUOTES,
                *klass._FORMAT_STRINGS,
            )
            if " " in key or any(single in key for single in klass.SINGLE_TOKENS)
        )

        if USE_RS_TOKENIZER:
            from sqlglotrs import (  # type: ignore
                Tokenizer as RsTokenizer,
                TokenizerSettings as RsTokenizerSettings,
                TokenTypeSettings as RsTokenTypeSettings,
            )

            settings = RsTokenizerSettings(
                white_space={k: _TOKEN_TYPE_TO_INDEX[v] for k, v in klass.WHITE_SPACE.items()},
                single_tokens={k: _TOKEN_TYPE_TO_INDEX[v] for k, v in klass.SINGLE_TOKENS.items()},
                keywords={k: _TOKEN_TYPE_TO_INDEX[v] for k, v in klass.KEYWORDS.items()},
                numeric_literals=klass.NUMERIC_LITERALS,
                identifiers=klass._IDENTIFIERS,
                identifier_escapes=klass._IDENTIFIER_ESCAPES,
                string_escapes=klass._STRING_ESCAPES,
                byte_string_escapes=klass._BYTE_STRING_ESCAPES,
                quotes=klass._QUOTES,
                format_strings={
                    k: (v1, _TOKEN_TYPE_TO_INDEX[v2])
                    for k, (v1, v2) in klass._FORMAT_STRINGS.items()
                },
                has_bit_strings=bool(klass.BIT_STRINGS),
                has_hex_strings=bool(klass.HEX_STRINGS),
                comments=klass._COMMENTS,
                var_single_tokens=klass.VAR_SINGLE_TOKENS,
                commands={_TOKEN_TYPE_TO_INDEX[v] for v in klass.COMMANDS},
                command_prefix_tokens={
                    _TOKEN_TYPE_TO_INDEX[v] for v in klass.COMMAND_PREFIX_TOKENS
                },
                heredoc_tag_is_identifier=klass.HEREDOC_TAG_IS_IDENTIFIER,
                string_escapes_allowed_in_raw_strings=klass.STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS,
                nested_comments=klass.NESTED_COMMENTS,
                hint_start=klass.HINT_START,
                tokens_preceding_hint={
                    _TOKEN_TYPE_TO_INDEX[v] for v in klass.TOKENS_PRECEDING_HINT
                },
                escape_follow_chars=klass._ESCAPE_FOLLOW_CHARS,
            )
            token_types = RsTokenTypeSettings(
                bit_string=_TOKEN_TYPE_TO_INDEX[TokenType.BIT_STRING],
                byte_string=_TOKEN_TYPE_TO_INDEX[TokenType.BYTE_STRING],
                break_=_TOKEN_TYPE_TO_INDEX[TokenType.BREAK],
                dcolon=_TOKEN_TYPE_TO_INDEX[TokenType.DCOLON],
                heredoc_string=_TOKEN_TYPE_TO_INDEX[TokenType.HEREDOC_STRING],
                raw_string=_TOKEN_TYPE_TO_INDEX[TokenType.RAW_STRING],
                hex_string=_TOKEN_TYPE_TO_INDEX[TokenType.HEX_STRING],
                identifier=_TOKEN_TYPE_TO_INDEX[TokenType.IDENTIFIER],
                number=_TOKEN_TYPE_TO_INDEX[TokenType.NUMBER],
                parameter=_TOKEN_TYPE_TO_INDEX[TokenType.PARAMETER],
                semicolon=_TOKEN_TYPE_TO_INDEX[TokenType.SEMICOLON],
                string=_TOKEN_TYPE_TO_INDEX[TokenType.STRING],
                var=_TOKEN_TYPE_TO_INDEX[TokenType.VAR],
                heredoc_string_alternative=_TOKEN_TYPE_TO_INDEX[klass.HEREDOC_STRING_ALTERNATIVE],
                hint=_TOKEN_TYPE_TO_INDEX[TokenType.HINT],
            )
            klass._RS_TOKENIZER = RsTokenizer(settings, token_types)
        else:
            klass._RS_TOKENIZER = None

        return klass


class Tokenizer(metaclass=_Tokenizer):
    SINGLE_TOKENS = {
        "(": TokenType.L_PAREN,
        ")": TokenType.R_PAREN,
        "[": TokenType.L_BRACKET,
        "]": TokenType.R_BRACKET,
        "{": TokenType.L_BRACE,
        "}": TokenType.R_BRACE,
        "&": TokenType.AMP,
        "^": TokenType.CARET,
        ":": TokenType.COLON,
        ",": TokenType.COMMA,
        ".": TokenType.DOT,
        "-": TokenType.DASH,
        "=": TokenType.EQ,
        ">": TokenType.GT,
        "<": TokenType.LT,
        "%": TokenType.MOD,
        "!": TokenType.NOT,
        "|": TokenType.PIPE,
        "+": TokenType.PLUS,
        ";": TokenType.SEMICOLON,
        "/": TokenType.SLASH,
        "\\": TokenType.BACKSLASH,
        "*": TokenType.STAR,
        "~": TokenType.TILDE,
        "?": TokenType.PLACEHOLDER,
        "@": TokenType.PARAMETER,
        "#": TokenType.HASH,
        # Used for breaking a var like x'y' but nothing else the token type doesn't matter
        "'": TokenType.UNKNOWN,
        "`": TokenType.UNKNOWN,
        '"': TokenType.UNKNOWN,
    }

    BIT_STRINGS: t.List[str | t.Tuple[str, str]] = []
    BYTE_STRINGS: t.List[str | t.Tuple[str, str]] = []
    HEX_STRINGS: t.List[str | t.Tuple[str, str]] = []
    RAW_STRINGS: t.List[str | t.Tuple[str, str]] = []
    HEREDOC_STRINGS: t.List[str | t.Tuple[str, str]] = []
    UNICODE_STRINGS: t.List[str | t.Tuple[str, str]] = []
    IDENTIFIERS: t.List[str | t.Tuple[str, str]] = ['"']
    QUOTES: t.List[t.Tuple[str, str] | str] = ["'"]
    STRING_ESCAPES = ["'"]
    BYTE_STRING_ESCAPES: t.List[str] = []
    VAR_SINGLE_TOKENS: t.Set[str] = set()
    ESCAPE_FOLLOW_CHARS: t.List[str] = []

    # The strings in this list can always be used as escapes, regardless of the surrounding
    # identifier delimiters. By default, the closing delimiter is assumed to also act as an
    # identifier escape, e.g. if we use double-quotes, then they also act as escapes: "x"""
    IDENTIFIER_ESCAPES: t.List[str] = []

    # Whether the heredoc tags follow the same lexical rules as unquoted identifiers
    HEREDOC_TAG_IS_IDENTIFIER = False

    # Token that we'll generate as a fallback if the heredoc prefix doesn't correspond to a heredoc
    HEREDOC_STRING_ALTERNATIVE = TokenType.VAR

    # Whether string escape characters function as such when placed within raw strings
    STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS = True

    NESTED_COMMENTS = True

    HINT_START = "/*+"

    TOKENS_PRECEDING_HINT = {TokenType.SELECT, TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE}

    # Autofilled
    _COMMENTS: t.Dict[str, str] = {}
    _FORMAT_STRINGS: t.Dict[str, t.Tuple[str, TokenType]] = {}
    _IDENTIFIERS: t.Dict[str, str] = {}
    _IDENTIFIER_ESCAPES: t.Set[str] = set()
    _QUOTES: t.Dict[str, str] = {}
    _STRING_ESCAPES: t.Set[str] = set()
    _BYTE_STRING_ESCAPES: t.Set[str] = set()
    _KEYWORD_TRIE: t.Dict = {}
    _RS_TOKENIZER: t.Optional[t.Any] = None
    _ESCAPE_FOLLOW_CHARS: t.Set[str] = set()

    KEYWORDS: t.Dict[str, TokenType] = {
        **{f"{{%{postfix}": TokenType.BLOCK_START for postfix in ("", "+", "-")},
        **{f"{prefix}%}}": TokenType.BLOCK_END for prefix in ("", "+", "-")},
        **{f"{{{{{postfix}": TokenType.BLOCK_START for postfix in ("+", "-")},
        **{f"{prefix}}}}}": TokenType.BLOCK_END for prefix in ("+", "-")},
        HINT_START: TokenType.HINT,
        "&<": TokenType.AMP_LT,
        "&>": TokenType.AMP_GT,
        "==": TokenType.EQ,
        "::": TokenType.DCOLON,
        "?::": TokenType.QDCOLON,
        "||": TokenType.DPIPE,
        "|>": TokenType.PIPE_GT,
        ">=": TokenType.GTE,
        "<=": TokenType.LTE,
        "<>": TokenType.NEQ,
        "!=": TokenType.NEQ,
        ":=": TokenType.COLON_EQ,
        "<=>": TokenType.NULLSAFE_EQ,
        "->": TokenType.ARROW,
        "->>": TokenType.DARROW,
        "=>": TokenType.FARROW,
        "#>": TokenType.HASH_ARROW,
        "#>>": TokenType.DHASH_ARROW,
        "<->": TokenType.LR_ARROW,
        "&&": TokenType.DAMP,
        "??": TokenType.DQMARK,
        "~~~": TokenType.GLOB,
        "~~": TokenType.LIKE,
        "~~*": TokenType.ILIKE,
        "~*": TokenType.IRLIKE,
        "-|-": TokenType.ADJACENT,
        "ALL": TokenType.ALL,
        "AND": TokenType.AND,
        "ANTI": TokenType.ANTI,
        "ANY": TokenType.ANY,
        "ASC": TokenType.ASC,
        "AS": TokenType.ALIAS,
        "ASOF": TokenType.ASOF,
        "AUTOINCREMENT": TokenType.AUTO_INCREMENT,
        "AUTO_INCREMENT": TokenType.AUTO_INCREMENT,
        "BEGIN": TokenType.BEGIN,
        "BETWEEN": TokenType.BETWEEN,
        "CACHE": TokenType.CACHE,
        "UNCACHE": TokenType.UNCACHE,
        "CASE": TokenType.CASE,
        "CHARACTER SET": TokenType.CHARACTER_SET,
        "CLUSTER BY": TokenType.CLUSTER_BY,
        "COLLATE": TokenType.COLLATE,
        "COLUMN": TokenType.COLUMN,
        "COMMIT": TokenType.COMMIT,
        "CONNECT BY": TokenType.CONNECT_BY,
        "CONSTRAINT": TokenType.CONSTRAINT,
        "COPY": TokenType.COPY,
        "CREATE": TokenType.CREATE,
        "CROSS": TokenType.CROSS,
        "CUBE": TokenType.CUBE,
        "CURRENT_DATE": TokenType.CURRENT_DATE,
        "CURRENT_SCHEMA": TokenType.CURRENT_SCHEMA,
        "CURRENT_TIME": TokenType.CURRENT_TIME,
        "CURRENT_TIMESTAMP": TokenType.CURRENT_TIMESTAMP,
        "CURRENT_USER": TokenType.CURRENT_USER,
        "CURRENT_CATALOG": TokenType.CURRENT_CATALOG,
        "DATABASE": TokenType.DATABASE,
        "DEFAULT": TokenType.DEFAULT,
        "DELETE": TokenType.DELETE,
        "DESC": TokenType.DESC,
        "DESCRIBE": TokenType.DESCRIBE,
        "DISTINCT": TokenType.DISTINCT,
        "DISTRIBUTE BY": TokenType.DISTRIBUTE_BY,
        "DIV": TokenType.DIV,
        "DROP": TokenType.DROP,
        "ELSE": TokenType.ELSE,
        "END": TokenType.END,
        "ENUM": TokenType.ENUM,
        "ESCAPE": TokenType.ESCAPE,
        "EXCEPT": TokenType.EXCEPT,
        "EXECUTE": TokenType.EXECUTE,
        "EXISTS": TokenType.EXISTS,
        "FALSE": TokenType.FALSE,
        "FETCH": TokenType.FETCH,
        "FILTER": TokenType.FILTER,
        "FILE": TokenType.FILE,
        "FIRST": TokenType.FIRST,
        "FULL": TokenType.FULL,
        "FUNCTION": TokenType.FUNCTION,
        "FOR": TokenType.FOR,
        "FOREIGN KEY": TokenType.FOREIGN_KEY,
        "FORMAT": TokenType.FORMAT,
        "FROM": TokenType.FROM,
        "GEOGRAPHY": TokenType.GEOGRAPHY,
        "GEOMETRY": TokenType.GEOMETRY,
        "GLOB": TokenType.GLOB,
        "GROUP BY": TokenType.GROUP_BY,
        "GROUPING SETS": TokenType.GROUPING_SETS,
        "HAVING": TokenType.HAVING,
        "ILIKE": TokenType.ILIKE,
        "IN": TokenType.IN,
        "INDEX": TokenType.INDEX,
        "INET": TokenType.INET,
        "INNER": TokenType.INNER,
        "INSERT": TokenType.INSERT,
        "INTERVAL": TokenType.INTERVAL,
        "INTERSECT": TokenType.INTERSECT,
        "INTO": TokenType.INTO,
        "IS": TokenType.IS,
        "ISNULL": TokenType.ISNULL,
        "JOIN": TokenType.JOIN,
        "KEEP": TokenType.KEEP,
        "KILL": TokenType.KILL,
        "LATERAL": TokenType.LATERAL,
        "LEFT": TokenType.LEFT,
        "LIKE": TokenType.LIKE,
        "LIMIT": TokenType.LIMIT,
        "LOAD": TokenType.LOAD,
        "LOCALTIME": TokenType.LOCALTIME,
        "LOCALTIMESTAMP": TokenType.LOCALTIMESTAMP,
        "LOCK": TokenType.LOCK,
        "MERGE": TokenType.MERGE,
        "NAMESPACE": TokenType.NAMESPACE,
        "NATURAL": TokenType.NATURAL,
        "NEXT": TokenType.NEXT,
        "NOT": TokenType.NOT,
        "NOTNULL": TokenType.NOTNULL,
        "NULL": TokenType.NULL,
        "OBJECT": TokenType.OBJECT,
        "OFFSET": TokenType.OFFSET,
        "ON": TokenType.ON,
        "OR": TokenType.OR,
        "XOR": TokenType.XOR,
        "ORDER BY": TokenType.ORDER_BY,
        "ORDINALITY": TokenType.ORDINALITY,
        "OUT": TokenType.OUT,
        "OUTER": TokenType.OUTER,
        "OVER": TokenType.OVER,
        "OVERLAPS": TokenType.OVERLAPS,
        "OVERWRITE": TokenType.OVERWRITE,
        "PARTITION": TokenType.PARTITION,
        "PARTITION BY": TokenType.PARTITION_BY,
        "PARTITIONED BY": TokenType.PARTITION_BY,
        "PARTITIONED_BY": TokenType.PARTITION_BY,
        "PERCENT": TokenType.PERCENT,
        "PIVOT": TokenType.PIVOT,
        "PRAGMA": TokenType.PRAGMA,
        "PRIMARY KEY": TokenType.PRIMARY_KEY,
        "PROCEDURE": TokenType.PROCEDURE,
        "OPERATOR": TokenType.OPERATOR,
        "QUALIFY": TokenType.QUALIFY,
        "RANGE": TokenType.RANGE,
        "RECURSIVE": TokenType.RECURSIVE,
        "REGEXP": TokenType.RLIKE,
        "RENAME": TokenType.RENAME,
        "REPLACE": TokenType.REPLACE,
        "RETURNING": TokenType.RETURNING,
        "REFERENCES": TokenType.REFERENCES,
        "RIGHT": TokenType.RIGHT,
        "RLIKE": TokenType.RLIKE,
        "ROLLBACK": TokenType.ROLLBACK,
        "ROLLUP": TokenType.ROLLUP,
        "ROW": TokenType.ROW,
        "ROWS": TokenType.ROWS,
        "SCHEMA": TokenType.SCHEMA,
        "SELECT": TokenType.SELECT,
        "SEMI": TokenType.SEMI,
        "SESSION": TokenType.SESSION,
        "SESSION_USER": TokenType.SESSION_USER,
        "SET": TokenType.SET,
        "SETTINGS": TokenType.SETTINGS,
        "SHOW": TokenType.SHOW,
        "SIMILAR TO": TokenType.SIMILAR_TO,
        "SOME": TokenType.SOME,
        "SORT BY": TokenType.SORT_BY,
        "START WITH": TokenType.START_WITH,
        "STRAIGHT_JOIN": TokenType.STRAIGHT_JOIN,
        "TABLE": TokenType.TABLE,
        "TABLESAMPLE": TokenType.TABLE_SAMPLE,
        "TEMP": TokenType.TEMPORARY,
        "TEMPORARY": TokenType.TEMPORARY,
        "THEN": TokenType.THEN,
        "TRUE": TokenType.TRUE,
        "TRUNCATE": TokenType.TRUNCATE,
        "UNION": TokenType.UNION,
        "UNKNOWN": TokenType.UNKNOWN,
        "UNNEST": TokenType.UNNEST,
        "UNPIVOT": TokenType.UNPIVOT,
        "UPDATE": TokenType.UPDATE,
        "USE": TokenType.USE,
        "USING": TokenType.USING,
        "UUID": TokenType.UUID,
        "VALUES": TokenType.VALUES,
        "VIEW": TokenType.VIEW,
        "VOLATILE": TokenType.VOLATILE,
        "WHEN": TokenType.WHEN,
        "WHERE": TokenType.WHERE,
        "WINDOW": TokenType.WINDOW,
        "WITH": TokenType.WITH,
        "APPLY": TokenType.APPLY,
        "ARRAY": TokenType.ARRAY,
        "BIT": TokenType.BIT,
        "BOOL": TokenType.BOOLEAN,
        "BOOLEAN": TokenType.BOOLEAN,
        "BYTE": TokenType.TINYINT,
        "MEDIUMINT": TokenType.MEDIUMINT,
        "INT1": TokenType.TINYINT,
        "TINYINT": TokenType.TINYINT,
        "INT16": TokenType.SMALLINT,
        "SHORT": TokenType.SMALLINT,
        "SMALLINT": TokenType.SMALLINT,
        "HUGEINT": TokenType.INT128,
        "UHUGEINT": TokenType.UINT128,
        "INT2": TokenType.SMALLINT,
        "INTEGER": TokenType.INT,
        "INT": TokenType.INT,
        "INT4": TokenType.INT,
        "INT32": TokenType.INT,
        "INT64": TokenType.BIGINT,
        "INT128": TokenType.INT128,
        "INT256": TokenType.INT256,
        "LONG": TokenType.BIGINT,
        "BIGINT": TokenType.BIGINT,
        "INT8": TokenType.TINYINT,
        "UINT": TokenType.UINT,
        "UINT128": TokenType.UINT128,
        "UINT256": TokenType.UINT256,
        "DEC": TokenType.DECIMAL,
        "DECIMAL": TokenType.DECIMAL,
        "DECIMAL32": TokenType.DECIMAL32,
        "DECIMAL64": TokenType.DECIMAL64,
        "DECIMAL128": TokenType.DECIMAL128,
        "DECIMAL256": TokenType.DECIMAL256,
        "DECFLOAT": TokenType.DECFLOAT,
        "BIGDECIMAL": TokenType.BIGDECIMAL,
        "BIGNUMERIC": TokenType.BIGDECIMAL,
        "BIGNUM": TokenType.BIGNUM,
        "LIST": TokenType.LIST,
        "MAP": TokenType.MAP,
        "NULLABLE": TokenType.NULLABLE,
        "NUMBER": TokenType.DECIMAL,
        "NUMERIC": TokenType.DECIMAL,
        "FIXED": TokenType.DECIMAL,
        "REAL": TokenType.FLOAT,
        "FLOAT": TokenType.FLOAT,
        "FLOAT4": TokenType.FLOAT,
        "FLOAT8": TokenType.DOUBLE,
        "DOUBLE": TokenType.DOUBLE,
        "DOUBLE PRECISION": TokenType.DOUBLE,
        "JSON": TokenType.JSON,
        "JSONB": TokenType.JSONB,
        "CHAR": TokenType.CHAR,
        "CHARACTER": TokenType.CHAR,
        "CHAR VARYING": TokenType.VARCHAR,
        "CHARACTER VARYING": TokenType.VARCHAR,
        "NCHAR": TokenType.NCHAR,
        "VARCHAR": TokenType.VARCHAR,
        "VARCHAR2": TokenType.VARCHAR,
        "NVARCHAR": TokenType.NVARCHAR,
        "NVARCHAR2": TokenType.NVARCHAR,
        "BPCHAR": TokenType.BPCHAR,
        "STR": TokenType.TEXT,
        "STRING": TokenType.TEXT,
        "TEXT": TokenType.TEXT,
        "LONGTEXT": TokenType.LONGTEXT,
        "MEDIUMTEXT": TokenType.MEDIUMTEXT,
        "TINYTEXT": TokenType.TINYTEXT,
        "CLOB": TokenType.TEXT,
        "LONGVARCHAR": TokenType.TEXT,
        "BINARY": TokenType.BINARY,
        "BLOB": TokenType.VARBINARY,
        "LONGBLOB": TokenType.LONGBLOB,
        "MEDIUMBLOB": TokenType.MEDIUMBLOB,
        "TINYBLOB": TokenType.TINYBLOB,
        "BYTEA": TokenType.VARBINARY,
        "VARBINARY": TokenType.VARBINARY,
        "TIME": TokenType.TIME,
        "TIMETZ": TokenType.TIMETZ,
        "TIME_NS": TokenType.TIME_NS,
        "TIMESTAMP": TokenType.TIMESTAMP,
        "TIMESTAMPTZ": TokenType.TIMESTAMPTZ,
        "TIMESTAMPLTZ": TokenType.TIMESTAMPLTZ,
        "TIMESTAMP_LTZ": TokenType.TIMESTAMPLTZ,
        "TIMESTAMPNTZ": TokenType.TIMESTAMPNTZ,
        "TIMESTAMP_NTZ": TokenType.TIMESTAMPNTZ,
        "DATE": TokenType.DATE,
        "DATETIME": TokenType.DATETIME,
        "INT4RANGE": TokenType.INT4RANGE,
        "INT4MULTIRANGE": TokenType.INT4MULTIRANGE,
        "INT8RANGE": TokenType.INT8RANGE,
        "INT8MULTIRANGE": TokenType.INT8MULTIRANGE,
        "NUMRANGE": TokenType.NUMRANGE,
        "NUMMULTIRANGE": TokenType.NUMMULTIRANGE,
        "TSRANGE": TokenType.TSRANGE,
        "TSMULTIRANGE": TokenType.TSMULTIRANGE,
        "TSTZRANGE": TokenType.TSTZRANGE,
        "TSTZMULTIRANGE": TokenType.TSTZMULTIRANGE,
        "DATERANGE": TokenType.DATERANGE,
        "DATEMULTIRANGE": TokenType.DATEMULTIRANGE,
        "UNIQUE": TokenType.UNIQUE,
        "VECTOR": TokenType.VECTOR,
        "STRUCT": TokenType.STRUCT,
        "SEQUENCE": TokenType.SEQUENCE,
        "VARIANT": TokenType.VARIANT,
        "ALTER": TokenType.ALTER,
        "ANALYZE": TokenType.ANALYZE,
        "CALL": TokenType.COMMAND,
        "COMMENT": TokenType.COMMENT,
        "EXPLAIN": TokenType.COMMAND,
        "GRANT": TokenType.GRANT,
        "REVOKE": TokenType.REVOKE,
        "OPTIMIZE": TokenType.COMMAND,
        "PREPARE": TokenType.COMMAND,
        "VACUUM": TokenType.COMMAND,
        "USER-DEFINED": TokenType.USERDEFINED,
        "FOR VERSION": TokenType.VERSION_SNAPSHOT,
        "FOR TIMESTAMP": TokenType.TIMESTAMP_SNAPSHOT,
    }

    WHITE_SPACE: t.Dict[t.Optional[str], TokenType] = {
        " ": TokenType.SPACE,
        "\t": TokenType.SPACE,
        "\n": TokenType.BREAK,
        "\r": TokenType.BREAK,
    }

    COMMANDS = {
        TokenType.COMMAND,
        TokenType.EXECUTE,
        TokenType.FETCH,
        TokenType.SHOW,
        TokenType.RENAME,
    }

    COMMAND_PREFIX_TOKENS = {TokenType.SEMICOLON, TokenType.BEGIN}

    # Handle numeric literals like in hive (3L = BIGINT)
    NUMERIC_LITERALS: t.Dict[str, str] = {}

    COMMENTS = ["--", ("/*", "*/")]

    __slots__ = (
        "dialect",
        "use_rs_tokenizer",
        "_rs_dialect_settings",
        "_tokens",
        "_core",
    )

    def __init__(
        self,
        dialect: DialectType = None,
        use_rs_tokenizer: t.Optional[bool] = None,
        **opts: t.Any,
    ) -> None:
        from sqlglot.dialects import Dialect

        self.dialect = Dialect.get_or_raise(dialect)

        # initialize `use_rs_tokenizer`, and allow it to be overwritten per Tokenizer instance
        self.use_rs_tokenizer = (
            use_rs_tokenizer if use_rs_tokenizer is not None else USE_RS_TOKENIZER
        )

        self._tokens: t.List[Token] = []

        if self.use_rs_tokenizer:
            self._rs_dialect_settings = RsTokenizerDialectSettings(
                unescaped_sequences=self.dialect.UNESCAPED_SEQUENCES,
                identifiers_can_start_with_digit=self.dialect.IDENTIFIERS_CAN_START_WITH_DIGIT,
                numbers_can_be_underscore_separated=self.dialect.NUMBERS_CAN_BE_UNDERSCORE_SEPARATED,
            )
            self._core = None
        else:
            # Import core tokenizer after Token/TokenType are defined to avoid circular imports
            from sqlglot.tokenizer_core import (  # type: ignore
                TokenizerCore as _TokenizerCore,
                TokenizerConfig as _TokenizerConfig,
            )

            # Initialize core tokenizer (compiled with mypyc when available)
            config = _TokenizerConfig(
                single_tokens=self.SINGLE_TOKENS,
                keywords=self.KEYWORDS,
                white_space=self.WHITE_SPACE,
                quotes=self._QUOTES,
                format_strings=self._FORMAT_STRINGS,
                identifiers=self._IDENTIFIERS,
                comments=self._COMMENTS,
                string_escapes=self._STRING_ESCAPES,
                byte_string_escapes=self._BYTE_STRING_ESCAPES,
                identifier_escapes=self._IDENTIFIER_ESCAPES,
                escape_follow_chars=self._ESCAPE_FOLLOW_CHARS,
                commands=self.COMMANDS,
                command_prefix_tokens=self.COMMAND_PREFIX_TOKENS,
                nested_comments=self.NESTED_COMMENTS,
                hint_start=self.HINT_START,
                tokens_preceding_hint=self.TOKENS_PRECEDING_HINT,
                bit_strings=list(self.BIT_STRINGS),
                hex_strings=list(self.HEX_STRINGS),
                numeric_literals=self.NUMERIC_LITERALS,
                var_single_tokens=self.VAR_SINGLE_TOKENS,
                string_escapes_allowed_in_raw_strings=self.STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS,
                heredoc_tag_is_identifier=self.HEREDOC_TAG_IS_IDENTIFIER,
                heredoc_string_alternative=self.HEREDOC_STRING_ALTERNATIVE,
                keyword_trie=self._KEYWORD_TRIE,
                numbers_can_be_underscore_separated=self.dialect.NUMBERS_CAN_BE_UNDERSCORE_SEPARATED,
                identifiers_can_start_with_digit=self.dialect.IDENTIFIERS_CAN_START_WITH_DIGIT,
                unescaped_sequences=self.dialect.UNESCAPED_SEQUENCES,
            )
            self._core = _TokenizerCore(config)

    def tokenize(self, sql: str) -> t.List[Token]:
        """Returns a list of tokens corresponding to the SQL string `sql`."""
        if self.use_rs_tokenizer:
            return self.tokenize_rs(sql)

        return self._core.tokenize(sql)  # type: ignore

    @property
    def sql(self) -> str:
        """The SQL string being tokenized."""
        return self._core.sql if self._core else ""

    @property
    def size(self) -> int:
        """Length of the SQL string."""
        return self._core.size if self._core else 0

    @property
    def tokens(self) -> t.List[Token]:
        """The list of tokens produced by tokenization."""
        if self._core:
            return self._core.tokens
        return self._tokens

    def tokenize_rs(self, sql: str) -> t.List[Token]:
        if not self._RS_TOKENIZER:
            raise SqlglotError("Rust tokenizer is not available")

        tokens, error_msg = self._RS_TOKENIZER.tokenize(sql, self._rs_dialect_settings)
        for token in tokens:
            token.token_type = _ALL_TOKEN_TYPES[token.token_type_index]

        # Store for inspection purposes
        self._tokens = tokens

        if error_msg is not None:
            raise TokenError(error_msg)

        return tokens
