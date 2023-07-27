from __future__ import annotations

import typing as t
from enum import auto

from sqlglot.errors import TokenError
from sqlglot.helper import AutoName
from sqlglot.trie import TrieResult, in_trie, new_trie


class TokenType(AutoName):
    L_PAREN = auto()
    R_PAREN = auto()
    L_BRACKET = auto()
    R_BRACKET = auto()
    L_BRACE = auto()
    R_BRACE = auto()
    COMMA = auto()
    DOT = auto()
    DASH = auto()
    PLUS = auto()
    COLON = auto()
    DCOLON = auto()
    SEMICOLON = auto()
    STAR = auto()
    BACKSLASH = auto()
    SLASH = auto()
    LT = auto()
    LTE = auto()
    GT = auto()
    GTE = auto()
    NOT = auto()
    EQ = auto()
    NEQ = auto()
    NULLSAFE_EQ = auto()
    AND = auto()
    OR = auto()
    AMP = auto()
    DPIPE = auto()
    PIPE = auto()
    CARET = auto()
    TILDA = auto()
    ARROW = auto()
    DARROW = auto()
    FARROW = auto()
    HASH = auto()
    HASH_ARROW = auto()
    DHASH_ARROW = auto()
    LR_ARROW = auto()
    LT_AT = auto()
    AT_GT = auto()
    DOLLAR = auto()
    PARAMETER = auto()
    SESSION_PARAMETER = auto()
    DAMP = auto()
    XOR = auto()

    BLOCK_START = auto()
    BLOCK_END = auto()

    SPACE = auto()
    BREAK = auto()

    STRING = auto()
    NUMBER = auto()
    IDENTIFIER = auto()
    DATABASE = auto()
    COLUMN = auto()
    COLUMN_DEF = auto()
    SCHEMA = auto()
    TABLE = auto()
    VAR = auto()
    BIT_STRING = auto()
    HEX_STRING = auto()
    BYTE_STRING = auto()
    NATIONAL_STRING = auto()
    RAW_STRING = auto()

    # types
    BIT = auto()
    BOOLEAN = auto()
    TINYINT = auto()
    UTINYINT = auto()
    SMALLINT = auto()
    USMALLINT = auto()
    INT = auto()
    UINT = auto()
    BIGINT = auto()
    UBIGINT = auto()
    INT128 = auto()
    UINT128 = auto()
    INT256 = auto()
    UINT256 = auto()
    FLOAT = auto()
    DOUBLE = auto()
    DECIMAL = auto()
    BIGDECIMAL = auto()
    CHAR = auto()
    NCHAR = auto()
    VARCHAR = auto()
    NVARCHAR = auto()
    TEXT = auto()
    MEDIUMTEXT = auto()
    LONGTEXT = auto()
    MEDIUMBLOB = auto()
    LONGBLOB = auto()
    BINARY = auto()
    VARBINARY = auto()
    JSON = auto()
    JSONB = auto()
    TIME = auto()
    TIMESTAMP = auto()
    TIMESTAMPTZ = auto()
    TIMESTAMPLTZ = auto()
    DATETIME = auto()
    DATETIME64 = auto()
    DATE = auto()
    INT4RANGE = auto()
    INT4MULTIRANGE = auto()
    INT8RANGE = auto()
    INT8MULTIRANGE = auto()
    NUMRANGE = auto()
    NUMMULTIRANGE = auto()
    TSRANGE = auto()
    TSMULTIRANGE = auto()
    TSTZRANGE = auto()
    TSTZMULTIRANGE = auto()
    DATERANGE = auto()
    DATEMULTIRANGE = auto()
    UUID = auto()
    GEOGRAPHY = auto()
    NULLABLE = auto()
    GEOMETRY = auto()
    HLLSKETCH = auto()
    HSTORE = auto()
    SUPER = auto()
    SERIAL = auto()
    SMALLSERIAL = auto()
    BIGSERIAL = auto()
    XML = auto()
    UNIQUEIDENTIFIER = auto()
    USERDEFINED = auto()
    MONEY = auto()
    SMALLMONEY = auto()
    ROWVERSION = auto()
    IMAGE = auto()
    VARIANT = auto()
    OBJECT = auto()
    INET = auto()
    IPADDRESS = auto()
    IPPREFIX = auto()
    ENUM = auto()

    # keywords
    ALIAS = auto()
    ALTER = auto()
    ALWAYS = auto()
    ALL = auto()
    ANTI = auto()
    ANY = auto()
    APPLY = auto()
    ARRAY = auto()
    ASC = auto()
    ASOF = auto()
    AUTO_INCREMENT = auto()
    BEGIN = auto()
    BETWEEN = auto()
    CACHE = auto()
    CASE = auto()
    CHARACTER_SET = auto()
    CLUSTER_BY = auto()
    COLLATE = auto()
    COMMAND = auto()
    COMMENT = auto()
    COMMIT = auto()
    CONSTRAINT = auto()
    CREATE = auto()
    CROSS = auto()
    CUBE = auto()
    CURRENT_DATE = auto()
    CURRENT_DATETIME = auto()
    CURRENT_TIME = auto()
    CURRENT_TIMESTAMP = auto()
    CURRENT_USER = auto()
    DEFAULT = auto()
    DELETE = auto()
    DESC = auto()
    DESCRIBE = auto()
    DICTIONARY = auto()
    DISTINCT = auto()
    DISTRIBUTE_BY = auto()
    DIV = auto()
    DROP = auto()
    ELSE = auto()
    END = auto()
    ESCAPE = auto()
    EXCEPT = auto()
    EXECUTE = auto()
    EXISTS = auto()
    FALSE = auto()
    FETCH = auto()
    FILTER = auto()
    FINAL = auto()
    FIRST = auto()
    FOR = auto()
    FORCE = auto()
    FOREIGN_KEY = auto()
    FORMAT = auto()
    FROM = auto()
    FULL = auto()
    FUNCTION = auto()
    GLOB = auto()
    GLOBAL = auto()
    GROUP_BY = auto()
    GROUPING_SETS = auto()
    HAVING = auto()
    HINT = auto()
    IF = auto()
    IGNORE = auto()
    ILIKE = auto()
    ILIKE_ANY = auto()
    IN = auto()
    INDEX = auto()
    INNER = auto()
    INSERT = auto()
    INTERSECT = auto()
    INTERVAL = auto()
    INTO = auto()
    INTRODUCER = auto()
    IRLIKE = auto()
    IS = auto()
    ISNULL = auto()
    JOIN = auto()
    JOIN_MARKER = auto()
    KEEP = auto()
    LANGUAGE = auto()
    LATERAL = auto()
    LEFT = auto()
    LIKE = auto()
    LIKE_ANY = auto()
    LIMIT = auto()
    LOAD = auto()
    LOCK = auto()
    MAP = auto()
    MATCH_RECOGNIZE = auto()
    MEMBER_OF = auto()
    MERGE = auto()
    MOD = auto()
    NATURAL = auto()
    NEXT = auto()
    NEXT_VALUE_FOR = auto()
    NOTNULL = auto()
    NULL = auto()
    OFFSET = auto()
    ON = auto()
    ORDER_BY = auto()
    ORDERED = auto()
    ORDINALITY = auto()
    OUTER = auto()
    OVER = auto()
    OVERLAPS = auto()
    OVERWRITE = auto()
    PARTITION = auto()
    PARTITION_BY = auto()
    PERCENT = auto()
    PIVOT = auto()
    PLACEHOLDER = auto()
    PRAGMA = auto()
    PRIMARY_KEY = auto()
    PROCEDURE = auto()
    PROPERTIES = auto()
    PSEUDO_TYPE = auto()
    QUALIFY = auto()
    QUOTE = auto()
    RANGE = auto()
    RECURSIVE = auto()
    REPLACE = auto()
    RETURNING = auto()
    REFERENCES = auto()
    RIGHT = auto()
    RLIKE = auto()
    ROLLBACK = auto()
    ROLLUP = auto()
    ROW = auto()
    ROWS = auto()
    SELECT = auto()
    SEMI = auto()
    SEPARATOR = auto()
    SERDE_PROPERTIES = auto()
    SET = auto()
    SETTINGS = auto()
    SHOW = auto()
    SIMILAR_TO = auto()
    SOME = auto()
    SORT_BY = auto()
    STRUCT = auto()
    TABLE_SAMPLE = auto()
    TEMPORARY = auto()
    TOP = auto()
    THEN = auto()
    TRUE = auto()
    UNCACHE = auto()
    UNION = auto()
    UNNEST = auto()
    UNPIVOT = auto()
    UPDATE = auto()
    USE = auto()
    USING = auto()
    VALUES = auto()
    VIEW = auto()
    VOLATILE = auto()
    WHEN = auto()
    WHERE = auto()
    WINDOW = auto()
    WITH = auto()
    UNIQUE = auto()


class Token:
    __slots__ = ("token_type", "text", "line", "col", "start", "end", "comments")

    @classmethod
    def number(cls, number: int) -> Token:
        """Returns a NUMBER token with `number` as its text."""
        return cls(TokenType.NUMBER, str(number))

    @classmethod
    def string(cls, string: str) -> Token:
        """Returns a STRING token with `string` as its text."""
        return cls(TokenType.STRING, string)

    @classmethod
    def identifier(cls, identifier: str) -> Token:
        """Returns an IDENTIFIER token with `identifier` as its text."""
        return cls(TokenType.IDENTIFIER, identifier)

    @classmethod
    def var(cls, var: str) -> Token:
        """Returns an VAR token with `var` as its text."""
        return cls(TokenType.VAR, var)

    def __init__(
        self,
        token_type: TokenType,
        text: str,
        line: int = 1,
        col: int = 1,
        start: int = 0,
        end: int = 0,
        comments: t.List[str] = [],
    ) -> None:
        """Token initializer.

        Args:
            token_type: The TokenType Enum.
            text: The text of the token.
            line: The line that the token ends on.
            col: The column that the token ends on.
            start: The start index of the token.
            end: The ending index of the token.
            comments: The comments to attach to the token.
        """
        self.token_type = token_type
        self.text = text
        self.line = line
        self.col = col
        self.start = start
        self.end = end
        self.comments = comments

    def __repr__(self) -> str:
        attributes = ", ".join(f"{k}: {getattr(self, k)}" for k in self.__slots__)
        return f"<Token {attributes}>"


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
        }

        klass._STRING_ESCAPES = set(klass.STRING_ESCAPES)
        klass._IDENTIFIER_ESCAPES = set(klass.IDENTIFIER_ESCAPES)
        klass._COMMENTS = {
            **dict(
                (comment, None) if isinstance(comment, str) else (comment[0], comment[1])
                for comment in klass.COMMENTS
            ),
            "{#": "#}",  # Ensure Jinja comments are tokenized correctly in all dialects
        }

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
        "~": TokenType.TILDA,
        "?": TokenType.PLACEHOLDER,
        "@": TokenType.PARAMETER,
        # used for breaking a var like x'y' but nothing else
        # the token type doesn't matter
        "'": TokenType.QUOTE,
        "`": TokenType.IDENTIFIER,
        '"': TokenType.IDENTIFIER,
        "#": TokenType.HASH,
    }

    BIT_STRINGS: t.List[str | t.Tuple[str, str]] = []
    BYTE_STRINGS: t.List[str | t.Tuple[str, str]] = []
    HEX_STRINGS: t.List[str | t.Tuple[str, str]] = []
    RAW_STRINGS: t.List[str | t.Tuple[str, str]] = []
    IDENTIFIERS: t.List[str | t.Tuple[str, str]] = ['"']
    IDENTIFIER_ESCAPES = ['"']
    QUOTES: t.List[t.Tuple[str, str] | str] = ["'"]
    STRING_ESCAPES = ["'"]
    VAR_SINGLE_TOKENS: t.Set[str] = set()

    # Autofilled
    IDENTIFIERS_CAN_START_WITH_DIGIT: bool = False

    _COMMENTS: t.Dict[str, str] = {}
    _FORMAT_STRINGS: t.Dict[str, t.Tuple[str, TokenType]] = {}
    _IDENTIFIERS: t.Dict[str, str] = {}
    _IDENTIFIER_ESCAPES: t.Set[str] = set()
    _QUOTES: t.Dict[str, str] = {}
    _STRING_ESCAPES: t.Set[str] = set()
    _KEYWORD_TRIE: t.Dict = {}

    KEYWORDS: t.Dict[str, TokenType] = {
        **{f"{{%{postfix}": TokenType.BLOCK_START for postfix in ("", "+", "-")},
        **{f"{prefix}%}}": TokenType.BLOCK_END for prefix in ("", "+", "-")},
        **{f"{{{{{postfix}": TokenType.BLOCK_START for postfix in ("+", "-")},
        **{f"{prefix}}}}}": TokenType.BLOCK_END for prefix in ("+", "-")},
        "/*+": TokenType.HINT,
        "==": TokenType.EQ,
        "::": TokenType.DCOLON,
        "||": TokenType.DPIPE,
        ">=": TokenType.GTE,
        "<=": TokenType.LTE,
        "<>": TokenType.NEQ,
        "!=": TokenType.NEQ,
        "<=>": TokenType.NULLSAFE_EQ,
        "->": TokenType.ARROW,
        "->>": TokenType.DARROW,
        "=>": TokenType.FARROW,
        "#>": TokenType.HASH_ARROW,
        "#>>": TokenType.DHASH_ARROW,
        "<->": TokenType.LR_ARROW,
        "&&": TokenType.DAMP,
        "ALL": TokenType.ALL,
        "ALWAYS": TokenType.ALWAYS,
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
        "CONSTRAINT": TokenType.CONSTRAINT,
        "CREATE": TokenType.CREATE,
        "CROSS": TokenType.CROSS,
        "CUBE": TokenType.CUBE,
        "CURRENT_DATE": TokenType.CURRENT_DATE,
        "CURRENT_TIME": TokenType.CURRENT_TIME,
        "CURRENT_TIMESTAMP": TokenType.CURRENT_TIMESTAMP,
        "CURRENT_USER": TokenType.CURRENT_USER,
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
        "ESCAPE": TokenType.ESCAPE,
        "EXCEPT": TokenType.EXCEPT,
        "EXECUTE": TokenType.EXECUTE,
        "EXISTS": TokenType.EXISTS,
        "FALSE": TokenType.FALSE,
        "FETCH": TokenType.FETCH,
        "FILTER": TokenType.FILTER,
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
        "IF": TokenType.IF,
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
        "LATERAL": TokenType.LATERAL,
        "LEFT": TokenType.LEFT,
        "LIKE": TokenType.LIKE,
        "LIMIT": TokenType.LIMIT,
        "LOAD": TokenType.LOAD,
        "LOCK": TokenType.LOCK,
        "MERGE": TokenType.MERGE,
        "NATURAL": TokenType.NATURAL,
        "NEXT": TokenType.NEXT,
        "NEXT VALUE FOR": TokenType.NEXT_VALUE_FOR,
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
        "QUALIFY": TokenType.QUALIFY,
        "RANGE": TokenType.RANGE,
        "RECURSIVE": TokenType.RECURSIVE,
        "REGEXP": TokenType.RLIKE,
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
        "SET": TokenType.SET,
        "SETTINGS": TokenType.SETTINGS,
        "SHOW": TokenType.SHOW,
        "SIMILAR TO": TokenType.SIMILAR_TO,
        "SOME": TokenType.SOME,
        "SORT BY": TokenType.SORT_BY,
        "TABLE": TokenType.TABLE,
        "TABLESAMPLE": TokenType.TABLE_SAMPLE,
        "TEMP": TokenType.TEMPORARY,
        "TEMPORARY": TokenType.TEMPORARY,
        "THEN": TokenType.THEN,
        "TRUE": TokenType.TRUE,
        "UNION": TokenType.UNION,
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
        "TINYINT": TokenType.TINYINT,
        "SHORT": TokenType.SMALLINT,
        "SMALLINT": TokenType.SMALLINT,
        "INT2": TokenType.SMALLINT,
        "INTEGER": TokenType.INT,
        "INT": TokenType.INT,
        "INT4": TokenType.INT,
        "LONG": TokenType.BIGINT,
        "BIGINT": TokenType.BIGINT,
        "INT8": TokenType.BIGINT,
        "DEC": TokenType.DECIMAL,
        "DECIMAL": TokenType.DECIMAL,
        "BIGDECIMAL": TokenType.BIGDECIMAL,
        "BIGNUMERIC": TokenType.BIGDECIMAL,
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
        "CHAR": TokenType.CHAR,
        "CHARACTER": TokenType.CHAR,
        "NCHAR": TokenType.NCHAR,
        "VARCHAR": TokenType.VARCHAR,
        "VARCHAR2": TokenType.VARCHAR,
        "NVARCHAR": TokenType.NVARCHAR,
        "NVARCHAR2": TokenType.NVARCHAR,
        "STR": TokenType.TEXT,
        "STRING": TokenType.TEXT,
        "TEXT": TokenType.TEXT,
        "CLOB": TokenType.TEXT,
        "LONGVARCHAR": TokenType.TEXT,
        "BINARY": TokenType.BINARY,
        "BLOB": TokenType.VARBINARY,
        "BYTEA": TokenType.VARBINARY,
        "VARBINARY": TokenType.VARBINARY,
        "TIME": TokenType.TIME,
        "TIMESTAMP": TokenType.TIMESTAMP,
        "TIMESTAMPTZ": TokenType.TIMESTAMPTZ,
        "TIMESTAMPLTZ": TokenType.TIMESTAMPLTZ,
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
        "STRUCT": TokenType.STRUCT,
        "VARIANT": TokenType.VARIANT,
        "ALTER": TokenType.ALTER,
        "ANALYZE": TokenType.COMMAND,
        "CALL": TokenType.COMMAND,
        "COMMENT": TokenType.COMMENT,
        "COPY": TokenType.COMMAND,
        "EXPLAIN": TokenType.COMMAND,
        "GRANT": TokenType.COMMAND,
        "OPTIMIZE": TokenType.COMMAND,
        "PREPARE": TokenType.COMMAND,
        "TRUNCATE": TokenType.COMMAND,
        "VACUUM": TokenType.COMMAND,
        "USER-DEFINED": TokenType.USERDEFINED,
    }

    WHITE_SPACE: t.Dict[t.Optional[str], TokenType] = {
        " ": TokenType.SPACE,
        "\t": TokenType.SPACE,
        "\n": TokenType.BREAK,
        "\r": TokenType.BREAK,
        "\r\n": TokenType.BREAK,
    }

    COMMANDS = {
        TokenType.COMMAND,
        TokenType.EXECUTE,
        TokenType.FETCH,
        TokenType.SHOW,
    }

    COMMAND_PREFIX_TOKENS = {TokenType.SEMICOLON, TokenType.BEGIN}

    # handle numeric literals like in hive (3L = BIGINT)
    NUMERIC_LITERALS: t.Dict[str, str] = {}
    ENCODE: t.Optional[str] = None

    COMMENTS = ["--", ("/*", "*/")]

    __slots__ = (
        "sql",
        "size",
        "tokens",
        "_start",
        "_current",
        "_line",
        "_col",
        "_comments",
        "_char",
        "_end",
        "_peek",
        "_prev_token_line",
    )

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.sql = ""
        self.size = 0
        self.tokens: t.List[Token] = []
        self._start = 0
        self._current = 0
        self._line = 1
        self._col = 0
        self._comments: t.List[str] = []

        self._char = ""
        self._end = False
        self._peek = ""
        self._prev_token_line = -1

    def tokenize(self, sql: str) -> t.List[Token]:
        """Returns a list of tokens corresponding to the SQL string `sql`."""
        self.reset()
        self.sql = sql
        self.size = len(sql)

        try:
            self._scan()
        except Exception as e:
            start = max(self._current - 50, 0)
            end = min(self._current + 50, self.size - 1)
            context = self.sql[start:end]
            raise TokenError(f"Error tokenizing '{context}'") from e

        return self.tokens

    def _scan(self, until: t.Optional[t.Callable] = None) -> None:
        while self.size and not self._end:
            self._start = self._current
            self._advance()

            if self._char is None:
                break

            if self._char not in self.WHITE_SPACE:
                if self._char.isdigit():
                    self._scan_number()
                elif self._char in self._IDENTIFIERS:
                    self._scan_identifier(self._IDENTIFIERS[self._char])
                else:
                    self._scan_keywords()

            if until and until():
                break

        if self.tokens and self._comments:
            self.tokens[-1].comments.extend(self._comments)

    def _chars(self, size: int) -> str:
        if size == 1:
            return self._char

        start = self._current - 1
        end = start + size

        return self.sql[start:end] if end <= self.size else ""

    def _advance(self, i: int = 1, alnum: bool = False) -> None:
        if self.WHITE_SPACE.get(self._char) is TokenType.BREAK:
            self._col = 1
            self._line += 1
        else:
            self._col += i

        self._current += i
        self._end = self._current >= self.size
        self._char = self.sql[self._current - 1]
        self._peek = "" if self._end else self.sql[self._current]

        if alnum and self._char.isalnum():
            # Here we use local variables instead of attributes for better performance
            _col = self._col
            _current = self._current
            _end = self._end
            _peek = self._peek

            while _peek.isalnum():
                _col += 1
                _current += 1
                _end = _current >= self.size
                _peek = "" if _end else self.sql[_current]

            self._col = _col
            self._current = _current
            self._end = _end
            self._peek = _peek
            self._char = self.sql[_current - 1]

    @property
    def _text(self) -> str:
        return self.sql[self._start : self._current]

    def peek(self, i: int = 0) -> str:
        i = self._current + i
        if i < self.size:
            return self.sql[i]
        return ""

    def _add(self, token_type: TokenType, text: t.Optional[str] = None) -> None:
        self._prev_token_line = self._line
        self.tokens.append(
            Token(
                token_type,
                text=self._text if text is None else text,
                line=self._line,
                col=self._col,
                start=self._start,
                end=self._current - 1,
                comments=self._comments,
            )
        )
        self._comments = []

        # If we have either a semicolon or a begin token before the command's token, we'll parse
        # whatever follows the command's token as a string
        if (
            token_type in self.COMMANDS
            and self._peek != ";"
            and (len(self.tokens) == 1 or self.tokens[-2].token_type in self.COMMAND_PREFIX_TOKENS)
        ):
            start = self._current
            tokens = len(self.tokens)
            self._scan(lambda: self._peek == ";")
            self.tokens = self.tokens[:tokens]
            text = self.sql[start : self._current].strip()
            if text:
                self._add(TokenType.STRING, text)

    def _scan_keywords(self) -> None:
        size = 0
        word = None
        chars = self._text
        char = chars
        prev_space = False
        skip = False
        trie = self._KEYWORD_TRIE
        single_token = char in self.SINGLE_TOKENS

        while chars:
            if skip:
                result = TrieResult.PREFIX
            else:
                result, trie = in_trie(trie, char.upper())

            if result == TrieResult.FAILED:
                break
            if result == TrieResult.EXISTS:
                word = chars

            size += 1
            end = self._current - 1 + size

            if end < self.size:
                char = self.sql[end]
                single_token = single_token or char in self.SINGLE_TOKENS
                is_space = char in self.WHITE_SPACE

                if not is_space or not prev_space:
                    if is_space:
                        char = " "
                    chars += char
                    prev_space = is_space
                    skip = False
                else:
                    skip = True
            else:
                char = ""
                chars = " "

        if not word:
            if self._char in self.SINGLE_TOKENS:
                self._add(self.SINGLE_TOKENS[self._char], text=self._char)
                return
            self._scan_var()
            return

        if self._scan_string(word):
            return
        if self._scan_comment(word):
            return

        self._advance(size - 1)
        word = word.upper()
        self._add(self.KEYWORDS[word], text=word)

    def _scan_comment(self, comment_start: str) -> bool:
        if comment_start not in self._COMMENTS:
            return False

        comment_start_line = self._line
        comment_start_size = len(comment_start)
        comment_end = self._COMMENTS[comment_start]

        if comment_end:
            # Skip the comment's start delimiter
            self._advance(comment_start_size)

            comment_end_size = len(comment_end)
            while not self._end and self._chars(comment_end_size) != comment_end:
                self._advance(alnum=True)

            self._comments.append(self._text[comment_start_size : -comment_end_size + 1])
            self._advance(comment_end_size - 1)
        else:
            while not self._end and not self.WHITE_SPACE.get(self._peek) is TokenType.BREAK:
                self._advance(alnum=True)
            self._comments.append(self._text[comment_start_size:])

        # Leading comment is attached to the succeeding token, whilst trailing comment to the preceding.
        # Multiple consecutive comments are preserved by appending them to the current comments list.
        if comment_start_line == self._prev_token_line:
            self.tokens[-1].comments.extend(self._comments)
            self._comments = []
            self._prev_token_line = self._line

        return True

    def _scan_number(self) -> None:
        if self._char == "0":
            peek = self._peek.upper()
            if peek == "B":
                return self._scan_bits() if self.BIT_STRINGS else self._add(TokenType.NUMBER)
            elif peek == "X":
                return self._scan_hex() if self.HEX_STRINGS else self._add(TokenType.NUMBER)

        decimal = False
        scientific = 0

        while True:
            if self._peek.isdigit():
                self._advance()
            elif self._peek == "." and not decimal:
                after = self.peek(1)
                if after.isdigit() or not after.isalpha():
                    decimal = True
                    self._advance()
                else:
                    return self._add(TokenType.VAR)
            elif self._peek in ("-", "+") and scientific == 1:
                scientific += 1
                self._advance()
            elif self._peek.upper() == "E" and not scientific:
                scientific += 1
                self._advance()
            elif self._peek.isidentifier():
                number_text = self._text
                literal = ""

                while self._peek.strip() and self._peek not in self.SINGLE_TOKENS:
                    literal += self._peek.upper()
                    self._advance()

                token_type = self.KEYWORDS.get(self.NUMERIC_LITERALS.get(literal, ""))

                if token_type:
                    self._add(TokenType.NUMBER, number_text)
                    self._add(TokenType.DCOLON, "::")
                    return self._add(token_type, literal)
                elif self.IDENTIFIERS_CAN_START_WITH_DIGIT:
                    return self._add(TokenType.VAR)

                self._add(TokenType.NUMBER, number_text)
                return self._advance(-len(literal))
            else:
                return self._add(TokenType.NUMBER)

    def _scan_bits(self) -> None:
        self._advance()
        value = self._extract_value()
        try:
            # If `value` can't be converted to a binary, fallback to tokenizing it as an identifier
            int(value, 2)
            self._add(TokenType.BIT_STRING, value[2:])  # Drop the 0b
        except ValueError:
            self._add(TokenType.IDENTIFIER)

    def _scan_hex(self) -> None:
        self._advance()
        value = self._extract_value()
        try:
            # If `value` can't be converted to a hex, fallback to tokenizing it as an identifier
            int(value, 16)
            self._add(TokenType.HEX_STRING, value[2:])  # Drop the 0x
        except ValueError:
            self._add(TokenType.IDENTIFIER)

    def _extract_value(self) -> str:
        while True:
            char = self._peek.strip()
            if char and char not in self.SINGLE_TOKENS:
                self._advance(alnum=True)
            else:
                break

        return self._text

    def _scan_string(self, start: str) -> bool:
        base = None
        token_type = TokenType.STRING

        if start in self._QUOTES:
            end = self._QUOTES[start]
        elif start in self._FORMAT_STRINGS:
            end, token_type = self._FORMAT_STRINGS[start]

            if token_type == TokenType.HEX_STRING:
                base = 16
            elif token_type == TokenType.BIT_STRING:
                base = 2
        else:
            return False

        self._advance(len(start))
        text = self._extract_string(end)

        if base:
            try:
                int(text, base)
            except:
                raise TokenError(
                    f"Numeric string contains invalid characters from {self._line}:{self._start}"
                )
        else:
            text = text.encode(self.ENCODE).decode(self.ENCODE) if self.ENCODE else text

        self._add(token_type, text)
        return True

    def _scan_identifier(self, identifier_end: str) -> None:
        self._advance()
        text = self._extract_string(identifier_end, self._IDENTIFIER_ESCAPES)
        self._add(TokenType.IDENTIFIER, text)

    def _scan_var(self) -> None:
        while True:
            char = self._peek.strip()
            if char and (char in self.VAR_SINGLE_TOKENS or char not in self.SINGLE_TOKENS):
                self._advance(alnum=True)
            else:
                break

        self._add(
            TokenType.VAR
            if self.tokens and self.tokens[-1].token_type == TokenType.PARAMETER
            else self.KEYWORDS.get(self._text.upper(), TokenType.VAR)
        )

    def _extract_string(self, delimiter: str, escapes=None) -> str:
        text = ""
        delim_size = len(delimiter)
        escapes = self._STRING_ESCAPES if escapes is None else escapes

        while True:
            if self._char in escapes and (self._peek == delimiter or self._peek in escapes):
                if self._peek == delimiter:
                    text += self._peek
                else:
                    text += self._char + self._peek

                if self._current + 1 < self.size:
                    self._advance(2)
                else:
                    raise TokenError(f"Missing {delimiter} from {self._line}:{self._current}")
            else:
                if self._chars(delim_size) == delimiter:
                    if delim_size > 1:
                        self._advance(delim_size - 1)
                    break

                if self._end:
                    raise TokenError(f"Missing {delimiter} from {self._line}:{self._start}")

                current = self._current - 1
                self._advance(alnum=True)
                text += self.sql[current : self._current - 1]

        return text
