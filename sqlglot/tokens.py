from __future__ import annotations

import typing as t
from enum import auto

from sqlglot.helper import AutoName
from sqlglot.trie import in_trie, new_trie


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
    DOLLAR = auto()
    PARAMETER = auto()
    SESSION_PARAMETER = auto()

    SPACE = auto()
    BREAK = auto()

    STRING = auto()
    NUMBER = auto()
    IDENTIFIER = auto()
    COLUMN = auto()
    COLUMN_DEF = auto()
    SCHEMA = auto()
    TABLE = auto()
    VAR = auto()
    BIT_STRING = auto()
    HEX_STRING = auto()
    BYTE_STRING = auto()

    # types
    BOOLEAN = auto()
    TINYINT = auto()
    SMALLINT = auto()
    INT = auto()
    BIGINT = auto()
    FLOAT = auto()
    DOUBLE = auto()
    DECIMAL = auto()
    CHAR = auto()
    NCHAR = auto()
    VARCHAR = auto()
    NVARCHAR = auto()
    TEXT = auto()
    BINARY = auto()
    VARBINARY = auto()
    JSON = auto()
    JSONB = auto()
    TIMESTAMP = auto()
    TIMESTAMPTZ = auto()
    TIMESTAMPLTZ = auto()
    DATETIME = auto()
    DATE = auto()
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
    MONEY = auto()
    SMALLMONEY = auto()
    ROWVERSION = auto()
    IMAGE = auto()
    VARIANT = auto()
    OBJECT = auto()

    # keywords
    ALIAS = auto()
    ALWAYS = auto()
    ALL = auto()
    ANTI = auto()
    ANY = auto()
    APPLY = auto()
    ARRAY = auto()
    ASC = auto()
    ASOF = auto()
    AT_TIME_ZONE = auto()
    AUTO_INCREMENT = auto()
    BEGIN = auto()
    BETWEEN = auto()
    BOTH = auto()
    BUCKET = auto()
    BY_DEFAULT = auto()
    CACHE = auto()
    CASCADE = auto()
    CASE = auto()
    CHARACTER_SET = auto()
    CHECK = auto()
    CLUSTER_BY = auto()
    COLLATE = auto()
    COMMAND = auto()
    COMMENT = auto()
    COMMIT = auto()
    COMPOUND = auto()
    CONSTRAINT = auto()
    CREATE = auto()
    CROSS = auto()
    CUBE = auto()
    CURRENT_DATE = auto()
    CURRENT_DATETIME = auto()
    CURRENT_ROW = auto()
    CURRENT_TIME = auto()
    CURRENT_TIMESTAMP = auto()
    DEFAULT = auto()
    DELETE = auto()
    DESC = auto()
    DESCRIBE = auto()
    DETERMINISTIC = auto()
    DISTINCT = auto()
    DISTINCT_FROM = auto()
    DISTKEY = auto()
    DISTRIBUTE_BY = auto()
    DISTSTYLE = auto()
    DIV = auto()
    DROP = auto()
    ELSE = auto()
    ENCODE = auto()
    END = auto()
    ENGINE = auto()
    ESCAPE = auto()
    EXCEPT = auto()
    EXECUTE = auto()
    EXISTS = auto()
    FALSE = auto()
    FETCH = auto()
    FILTER = auto()
    FINAL = auto()
    FIRST = auto()
    FOLLOWING = auto()
    FOR = auto()
    FOREIGN_KEY = auto()
    FORMAT = auto()
    FULL = auto()
    FUNCTION = auto()
    FROM = auto()
    GENERATED = auto()
    GROUP_BY = auto()
    GROUPING_SETS = auto()
    HAVING = auto()
    HINT = auto()
    IDENTITY = auto()
    IF = auto()
    IGNORE_NULLS = auto()
    ILIKE = auto()
    IMMUTABLE = auto()
    IN = auto()
    INDEX = auto()
    INNER = auto()
    INSERT = auto()
    INTERSECT = auto()
    INTERVAL = auto()
    INTO = auto()
    INTRODUCER = auto()
    IS = auto()
    ISNULL = auto()
    JOIN = auto()
    LANGUAGE = auto()
    LATERAL = auto()
    LAZY = auto()
    LEADING = auto()
    LEFT = auto()
    LIKE = auto()
    LIMIT = auto()
    LOAD_DATA = auto()
    LOCAL = auto()
    LOCATION = auto()
    MAP = auto()
    MATERIALIZED = auto()
    MOD = auto()
    NATURAL = auto()
    NEXT = auto()
    NO_ACTION = auto()
    NOTNULL = auto()
    NULL = auto()
    NULLS_FIRST = auto()
    NULLS_LAST = auto()
    OFFSET = auto()
    ON = auto()
    ONLY = auto()
    OPTIONS = auto()
    ORDER_BY = auto()
    ORDERED = auto()
    ORDINALITY = auto()
    OUTER = auto()
    OUT_OF = auto()
    OVER = auto()
    OVERWRITE = auto()
    PARTITION = auto()
    PARTITION_BY = auto()
    PARTITIONED_BY = auto()
    PERCENT = auto()
    PIVOT = auto()
    PLACEHOLDER = auto()
    PRECEDING = auto()
    PRIMARY_KEY = auto()
    PROCEDURE = auto()
    PROPERTIES = auto()
    QUALIFY = auto()
    QUOTE = auto()
    RANGE = auto()
    RECURSIVE = auto()
    REPLACE = auto()
    RESPECT_NULLS = auto()
    REFERENCES = auto()
    RETURNS = auto()
    RIGHT = auto()
    RLIKE = auto()
    ROLLBACK = auto()
    ROLLUP = auto()
    ROW = auto()
    ROWS = auto()
    SCHEMA_COMMENT = auto()
    SEED = auto()
    SELECT = auto()
    SEMI = auto()
    SEPARATOR = auto()
    SET = auto()
    SHOW = auto()
    SIMILAR_TO = auto()
    SOME = auto()
    SORTKEY = auto()
    SORT_BY = auto()
    STABLE = auto()
    STORED = auto()
    STRUCT = auto()
    TABLE_FORMAT = auto()
    TABLE_SAMPLE = auto()
    TEMPORARY = auto()
    TRANSIENT = auto()
    TOP = auto()
    THEN = auto()
    TRAILING = auto()
    TRUE = auto()
    UNBOUNDED = auto()
    UNCACHE = auto()
    UNION = auto()
    UNLOGGED = auto()
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
    WITH_TIME_ZONE = auto()
    WITH_LOCAL_TIME_ZONE = auto()
    WITHIN_GROUP = auto()
    WITHOUT_TIME_ZONE = auto()
    UNIQUE = auto()


class Token:
    __slots__ = ("token_type", "text", "line", "col", "comments")

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
        comments: t.List[str] = [],
    ) -> None:
        self.token_type = token_type
        self.text = text
        self.line = line
        self.col = max(col - len(text), 1)
        self.comments = comments

    def __repr__(self) -> str:
        attributes = ", ".join(f"{k}: {getattr(self, k)}" for k in self.__slots__)
        return f"<Token {attributes}>"


class _Tokenizer(type):
    def __new__(cls, clsname, bases, attrs):  # type: ignore
        klass = super().__new__(cls, clsname, bases, attrs)

        klass._QUOTES = cls._delimeter_list_to_dict(klass.QUOTES)
        klass._BIT_STRINGS = cls._delimeter_list_to_dict(klass.BIT_STRINGS)
        klass._HEX_STRINGS = cls._delimeter_list_to_dict(klass.HEX_STRINGS)
        klass._BYTE_STRINGS = cls._delimeter_list_to_dict(klass.BYTE_STRINGS)
        klass._IDENTIFIERS = cls._delimeter_list_to_dict(klass.IDENTIFIERS)
        klass._ESCAPES = set(klass.ESCAPES)
        klass._COMMENTS = dict(
            (comment, None) if isinstance(comment, str) else (comment[0], comment[1])
            for comment in klass.COMMENTS
        )

        klass.KEYWORD_TRIE = new_trie(
            key.upper()
            for key in {
                **klass.KEYWORDS,
                **{comment: TokenType.COMMENT for comment in klass._COMMENTS},
                **{quote: TokenType.QUOTE for quote in klass._QUOTES},
                **{bit_string: TokenType.BIT_STRING for bit_string in klass._BIT_STRINGS},
                **{hex_string: TokenType.HEX_STRING for hex_string in klass._HEX_STRINGS},
                **{byte_string: TokenType.BYTE_STRING for byte_string in klass._BYTE_STRINGS},
            }
            if " " in key or any(single in key for single in klass.SINGLE_TOKENS)
        )

        return klass

    @staticmethod
    def _delimeter_list_to_dict(list: t.List[str | t.Tuple[str, str]]) -> t.Dict[str, str]:
        return dict((item, item) if isinstance(item, str) else (item[0], item[1]) for item in list)


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

    QUOTES: t.List[t.Tuple[str, str] | str] = ["'"]

    BIT_STRINGS: t.List[str | t.Tuple[str, str]] = []

    HEX_STRINGS: t.List[str | t.Tuple[str, str]] = []

    BYTE_STRINGS: t.List[str | t.Tuple[str, str]] = []

    IDENTIFIERS: t.List[str | t.Tuple[str, str]] = ['"']

    ESCAPES = ["'"]

    KEYWORDS = {
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
        "ALL": TokenType.ALL,
        "AND": TokenType.AND,
        "ANTI": TokenType.ANTI,
        "ANY": TokenType.ANY,
        "ASC": TokenType.ASC,
        "AS": TokenType.ALIAS,
        "AT TIME ZONE": TokenType.AT_TIME_ZONE,
        "AUTO_INCREMENT": TokenType.AUTO_INCREMENT,
        "BEGIN": TokenType.BEGIN,
        "BETWEEN": TokenType.BETWEEN,
        "BOTH": TokenType.BOTH,
        "BUCKET": TokenType.BUCKET,
        "CACHE": TokenType.CACHE,
        "UNCACHE": TokenType.UNCACHE,
        "CASE": TokenType.CASE,
        "CASCADE": TokenType.CASCADE,
        "CHARACTER SET": TokenType.CHARACTER_SET,
        "CHECK": TokenType.CHECK,
        "CLUSTER BY": TokenType.CLUSTER_BY,
        "COLLATE": TokenType.COLLATE,
        "COMMENT": TokenType.SCHEMA_COMMENT,
        "COMMIT": TokenType.COMMIT,
        "COMPOUND": TokenType.COMPOUND,
        "CONSTRAINT": TokenType.CONSTRAINT,
        "CREATE": TokenType.CREATE,
        "CROSS": TokenType.CROSS,
        "CUBE": TokenType.CUBE,
        "CURRENT_DATE": TokenType.CURRENT_DATE,
        "CURRENT ROW": TokenType.CURRENT_ROW,
        "CURRENT_TIMESTAMP": TokenType.CURRENT_TIMESTAMP,
        "DEFAULT": TokenType.DEFAULT,
        "DELETE": TokenType.DELETE,
        "DESC": TokenType.DESC,
        "DESCRIBE": TokenType.DESCRIBE,
        "DETERMINISTIC": TokenType.DETERMINISTIC,
        "DISTINCT": TokenType.DISTINCT,
        "DISTINCT FROM": TokenType.DISTINCT_FROM,
        "DISTKEY": TokenType.DISTKEY,
        "DISTRIBUTE BY": TokenType.DISTRIBUTE_BY,
        "DISTSTYLE": TokenType.DISTSTYLE,
        "DIV": TokenType.DIV,
        "DROP": TokenType.DROP,
        "ELSE": TokenType.ELSE,
        "END": TokenType.END,
        "ENGINE": TokenType.ENGINE,
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
        "FOLLOWING": TokenType.FOLLOWING,
        "FOR": TokenType.FOR,
        "FOREIGN KEY": TokenType.FOREIGN_KEY,
        "FORMAT": TokenType.FORMAT,
        "FROM": TokenType.FROM,
        "GROUP BY": TokenType.GROUP_BY,
        "GROUPING SETS": TokenType.GROUPING_SETS,
        "HAVING": TokenType.HAVING,
        "IF": TokenType.IF,
        "ILIKE": TokenType.ILIKE,
        "IMMUTABLE": TokenType.IMMUTABLE,
        "IGNORE NULLS": TokenType.IGNORE_NULLS,
        "IN": TokenType.IN,
        "INDEX": TokenType.INDEX,
        "INNER": TokenType.INNER,
        "INSERT": TokenType.INSERT,
        "INTERVAL": TokenType.INTERVAL,
        "INTERSECT": TokenType.INTERSECT,
        "INTO": TokenType.INTO,
        "IS": TokenType.IS,
        "ISNULL": TokenType.ISNULL,
        "JOIN": TokenType.JOIN,
        "LANGUAGE": TokenType.LANGUAGE,
        "LATERAL": TokenType.LATERAL,
        "LAZY": TokenType.LAZY,
        "LEADING": TokenType.LEADING,
        "LEFT": TokenType.LEFT,
        "LIKE": TokenType.LIKE,
        "LIMIT": TokenType.LIMIT,
        "LOAD DATA": TokenType.LOAD_DATA,
        "LOCAL": TokenType.LOCAL,
        "LOCATION": TokenType.LOCATION,
        "MATERIALIZED": TokenType.MATERIALIZED,
        "NATURAL": TokenType.NATURAL,
        "NEXT": TokenType.NEXT,
        "NO ACTION": TokenType.NO_ACTION,
        "NOT": TokenType.NOT,
        "NOTNULL": TokenType.NOTNULL,
        "NULL": TokenType.NULL,
        "NULLS FIRST": TokenType.NULLS_FIRST,
        "NULLS LAST": TokenType.NULLS_LAST,
        "OBJECT": TokenType.OBJECT,
        "OFFSET": TokenType.OFFSET,
        "ON": TokenType.ON,
        "ONLY": TokenType.ONLY,
        "OPTIONS": TokenType.OPTIONS,
        "OR": TokenType.OR,
        "ORDER BY": TokenType.ORDER_BY,
        "ORDINALITY": TokenType.ORDINALITY,
        "OUTER": TokenType.OUTER,
        "OUT OF": TokenType.OUT_OF,
        "OVER": TokenType.OVER,
        "OVERWRITE": TokenType.OVERWRITE,
        "PARTITION": TokenType.PARTITION,
        "PARTITION BY": TokenType.PARTITION_BY,
        "PARTITIONED BY": TokenType.PARTITIONED_BY,
        "PARTITIONED_BY": TokenType.PARTITIONED_BY,
        "PERCENT": TokenType.PERCENT,
        "PIVOT": TokenType.PIVOT,
        "PRECEDING": TokenType.PRECEDING,
        "PRIMARY KEY": TokenType.PRIMARY_KEY,
        "PROCEDURE": TokenType.PROCEDURE,
        "RANGE": TokenType.RANGE,
        "RECURSIVE": TokenType.RECURSIVE,
        "REGEXP": TokenType.RLIKE,
        "REPLACE": TokenType.REPLACE,
        "RESPECT NULLS": TokenType.RESPECT_NULLS,
        "REFERENCES": TokenType.REFERENCES,
        "RETURNS": TokenType.RETURNS,
        "RIGHT": TokenType.RIGHT,
        "RLIKE": TokenType.RLIKE,
        "ROLLBACK": TokenType.ROLLBACK,
        "ROLLUP": TokenType.ROLLUP,
        "ROW": TokenType.ROW,
        "ROWS": TokenType.ROWS,
        "SCHEMA": TokenType.SCHEMA,
        "SEED": TokenType.SEED,
        "SELECT": TokenType.SELECT,
        "SEMI": TokenType.SEMI,
        "SET": TokenType.SET,
        "SHOW": TokenType.SHOW,
        "SOME": TokenType.SOME,
        "SORTKEY": TokenType.SORTKEY,
        "SORT BY": TokenType.SORT_BY,
        "STABLE": TokenType.STABLE,
        "STORED": TokenType.STORED,
        "TABLE": TokenType.TABLE,
        "TABLE_FORMAT": TokenType.TABLE_FORMAT,
        "TBLPROPERTIES": TokenType.PROPERTIES,
        "TABLESAMPLE": TokenType.TABLE_SAMPLE,
        "TEMP": TokenType.TEMPORARY,
        "TEMPORARY": TokenType.TEMPORARY,
        "TRANSIENT": TokenType.TRANSIENT,
        "THEN": TokenType.THEN,
        "TRUE": TokenType.TRUE,
        "TRAILING": TokenType.TRAILING,
        "UNBOUNDED": TokenType.UNBOUNDED,
        "UNION": TokenType.UNION,
        "UNLOGGED": TokenType.UNLOGGED,
        "UNNEST": TokenType.UNNEST,
        "UNPIVOT": TokenType.UNPIVOT,
        "UPDATE": TokenType.UPDATE,
        "USE": TokenType.USE,
        "USING": TokenType.USING,
        "VALUES": TokenType.VALUES,
        "VIEW": TokenType.VIEW,
        "VOLATILE": TokenType.VOLATILE,
        "WHEN": TokenType.WHEN,
        "WHERE": TokenType.WHERE,
        "WITH": TokenType.WITH,
        "WITH TIME ZONE": TokenType.WITH_TIME_ZONE,
        "WITH LOCAL TIME ZONE": TokenType.WITH_LOCAL_TIME_ZONE,
        "WITHIN GROUP": TokenType.WITHIN_GROUP,
        "WITHOUT TIME ZONE": TokenType.WITHOUT_TIME_ZONE,
        "APPLY": TokenType.APPLY,
        "ARRAY": TokenType.ARRAY,
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
        "DECIMAL": TokenType.DECIMAL,
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
        "JSON": TokenType.JSON,
        "CHAR": TokenType.CHAR,
        "NCHAR": TokenType.NCHAR,
        "VARCHAR": TokenType.VARCHAR,
        "VARCHAR2": TokenType.VARCHAR,
        "NVARCHAR": TokenType.NVARCHAR,
        "NVARCHAR2": TokenType.NVARCHAR,
        "STRING": TokenType.TEXT,
        "TEXT": TokenType.TEXT,
        "CLOB": TokenType.TEXT,
        "BINARY": TokenType.BINARY,
        "BLOB": TokenType.VARBINARY,
        "BYTEA": TokenType.VARBINARY,
        "VARBINARY": TokenType.VARBINARY,
        "TIMESTAMP": TokenType.TIMESTAMP,
        "TIMESTAMPTZ": TokenType.TIMESTAMPTZ,
        "TIMESTAMPLTZ": TokenType.TIMESTAMPLTZ,
        "DATE": TokenType.DATE,
        "DATETIME": TokenType.DATETIME,
        "UNIQUE": TokenType.UNIQUE,
        "STRUCT": TokenType.STRUCT,
        "VARIANT": TokenType.VARIANT,
        "ALTER": TokenType.COMMAND,
        "ANALYZE": TokenType.COMMAND,
        "CALL": TokenType.COMMAND,
        "EXPLAIN": TokenType.COMMAND,
        "OPTIMIZE": TokenType.COMMAND,
        "PREPARE": TokenType.COMMAND,
        "TRUNCATE": TokenType.COMMAND,
        "VACUUM": TokenType.COMMAND,
    }

    WHITE_SPACE = {
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
        TokenType.SET,
        TokenType.SHOW,
    }

    # handle numeric literals like in hive (3L = BIGINT)
    NUMERIC_LITERALS: t.Dict[str, str] = {}
    ENCODE: t.Optional[str] = None

    COMMENTS = ["--", ("/*", "*/")]
    KEYWORD_TRIE = None  # autofilled

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
        "_prev_token_comments",
        "_prev_token_type",
        "_replace_backslash",
    )

    def __init__(self) -> None:
        self._replace_backslash = "\\" in self._ESCAPES  # type: ignore
        self.reset()

    def reset(self) -> None:
        self.sql = ""
        self.size = 0
        self.tokens: t.List[Token] = []
        self._start = 0
        self._current = 0
        self._line = 1
        self._col = 1
        self._comments: t.List[str] = []

        self._char = None
        self._end = None
        self._peek = None
        self._prev_token_line = -1
        self._prev_token_comments: t.List[str] = []
        self._prev_token_type = None

    def tokenize(self, sql: str) -> t.List[Token]:
        """Returns a list of tokens corresponding to the SQL string `sql`."""
        self.reset()
        self.sql = sql
        self.size = len(sql)

        while self.size and not self._end:
            self._start = self._current
            self._advance()

            if not self._char:
                break

            white_space = self.WHITE_SPACE.get(self._char)  # type: ignore
            identifier_end = self._IDENTIFIERS.get(self._char)  # type: ignore

            if white_space:
                if white_space == TokenType.BREAK:
                    self._col = 1
                    self._line += 1
            elif self._char.isdigit():  # type:ignore
                self._scan_number()
            elif identifier_end:
                self._scan_identifier(identifier_end)
            else:
                self._scan_keywords()
        return self.tokens

    def _chars(self, size: int) -> str:
        if size == 1:
            return self._char  # type: ignore
        start = self._current - 1
        end = start + size
        if end <= self.size:
            return self.sql[start:end]
        return ""

    def _advance(self, i: int = 1) -> None:
        self._col += i
        self._current += i
        self._end = self._current >= self.size  # type: ignore
        self._char = self.sql[self._current - 1]  # type: ignore
        self._peek = self.sql[self._current] if self._current < self.size else ""  # type: ignore

    @property
    def _text(self) -> str:
        return self.sql[self._start : self._current]

    def _add(self, token_type: TokenType, text: t.Optional[str] = None) -> None:
        self._prev_token_line = self._line
        self._prev_token_comments = self._comments
        self._prev_token_type = token_type  # type: ignore
        self.tokens.append(
            Token(
                token_type,
                self._text if text is None else text,
                self._line,
                self._col,
                self._comments,
            )
        )
        self._comments = []

        if token_type in self.COMMANDS and (
            len(self.tokens) == 1 or self.tokens[-2].token_type == TokenType.SEMICOLON
        ):
            self._start = self._current
            while not self._end and self._peek != ";":
                self._advance()
            if self._start < self._current:
                self._add(TokenType.STRING)

    def _scan_keywords(self) -> None:
        size = 0
        word = None
        chars = self._text
        char = chars
        prev_space = False
        skip = False
        trie = self.KEYWORD_TRIE

        while chars:
            if skip:
                result = 1
            else:
                result, trie = in_trie(trie, char.upper())  # type: ignore

            if result == 0:
                break
            if result == 2:
                word = chars
            size += 1
            end = self._current - 1 + size

            if end < self.size:
                char = self.sql[end]
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
                chars = None  # type: ignore

        if not word:
            if self._char in self.SINGLE_TOKENS:
                self._add(self.SINGLE_TOKENS[self._char])  # type: ignore
                return
            self._scan_var()
            return

        if self._scan_string(word):
            return
        if self._scan_formatted_string(word):
            return
        if self._scan_comment(word):
            return

        self._advance(size - 1)
        self._add(self.KEYWORDS[word.upper()])

    def _scan_comment(self, comment_start: str) -> bool:
        if comment_start not in self._COMMENTS:  # type: ignore
            return False

        comment_start_line = self._line
        comment_start_size = len(comment_start)
        comment_end = self._COMMENTS[comment_start]  # type: ignore

        if comment_end:
            comment_end_size = len(comment_end)

            while not self._end and self._chars(comment_end_size) != comment_end:
                self._advance()

            self._comments.append(self._text[comment_start_size : -comment_end_size + 1])  # type: ignore
            self._advance(comment_end_size - 1)
        else:
            while not self._end and self.WHITE_SPACE.get(self._peek) != TokenType.BREAK:  # type: ignore
                self._advance()
            self._comments.append(self._text[comment_start_size:])  # type: ignore

        # Leading comment is attached to the succeeding token, whilst trailing comment to the preceding.
        # Multiple consecutive comments are preserved by appending them to the current comments list.
        if comment_start_line == self._prev_token_line:
            self.tokens[-1].comments.extend(self._comments)
            self._comments = []

        return True

    def _scan_number(self) -> None:
        if self._char == "0":
            peek = self._peek.upper()  # type: ignore
            if peek == "B":
                return self._scan_bits()
            elif peek == "X":
                return self._scan_hex()

        decimal = False
        scientific = 0

        while True:
            if self._peek.isdigit():  # type: ignore
                self._advance()
            elif self._peek == "." and not decimal:
                decimal = True
                self._advance()
            elif self._peek in ("-", "+") and scientific == 1:
                scientific += 1
                self._advance()
            elif self._peek.upper() == "E" and not scientific:  # type: ignore
                scientific += 1
                self._advance()
            elif self._peek.isalpha():  # type: ignore
                self._add(TokenType.NUMBER)
                literal = []
                while self._peek.isalpha():  # type: ignore
                    literal.append(self._peek.upper())  # type: ignore
                    self._advance()
                literal = "".join(literal)  # type: ignore
                token_type = self.KEYWORDS.get(self.NUMERIC_LITERALS.get(literal))  # type: ignore
                if token_type:
                    self._add(TokenType.DCOLON, "::")
                    return self._add(token_type, literal)  # type: ignore
                return self._advance(-len(literal))
            else:
                return self._add(TokenType.NUMBER)

    def _scan_bits(self) -> None:
        self._advance()
        value = self._extract_value()
        try:
            self._add(TokenType.BIT_STRING, f"{int(value, 2)}")
        except ValueError:
            self._add(TokenType.IDENTIFIER)

    def _scan_hex(self) -> None:
        self._advance()
        value = self._extract_value()
        try:
            self._add(TokenType.HEX_STRING, f"{int(value, 16)}")
        except ValueError:
            self._add(TokenType.IDENTIFIER)

    def _extract_value(self) -> str:
        while True:
            char = self._peek.strip()  # type: ignore
            if char and char not in self.SINGLE_TOKENS:
                self._advance()
            else:
                break

        return self._text

    def _scan_string(self, quote: str) -> bool:
        quote_end = self._QUOTES.get(quote)  # type: ignore
        if quote_end is None:
            return False

        self._advance(len(quote))
        text = self._extract_string(quote_end)
        text = text.encode(self.ENCODE).decode(self.ENCODE) if self.ENCODE else text  # type: ignore
        text = text.replace("\\\\", "\\") if self._replace_backslash else text
        self._add(TokenType.STRING, text)
        return True

    # X'1234, b'0110', E'\\\\\' etc.
    def _scan_formatted_string(self, string_start: str) -> bool:
        if string_start in self._HEX_STRINGS:  # type: ignore
            delimiters = self._HEX_STRINGS  # type: ignore
            token_type = TokenType.HEX_STRING
            base = 16
        elif string_start in self._BIT_STRINGS:  # type: ignore
            delimiters = self._BIT_STRINGS  # type: ignore
            token_type = TokenType.BIT_STRING
            base = 2
        elif string_start in self._BYTE_STRINGS:  # type: ignore
            delimiters = self._BYTE_STRINGS  # type: ignore
            token_type = TokenType.BYTE_STRING
            base = None
        else:
            return False

        self._advance(len(string_start))
        string_end = delimiters.get(string_start)
        text = self._extract_string(string_end)

        if base is None:
            self._add(token_type, text)
        else:
            try:
                self._add(token_type, f"{int(text, base)}")
            except:
                raise RuntimeError(
                    f"Numeric string contains invalid characters from {self._line}:{self._start}"
                )

        return True

    def _scan_identifier(self, identifier_end: str) -> None:
        while self._peek != identifier_end:
            if self._end:
                raise RuntimeError(f"Missing {identifier_end} from {self._line}:{self._start}")
            self._advance()
        self._advance()
        self._add(TokenType.IDENTIFIER, self._text[1:-1])

    def _scan_var(self) -> None:
        while True:
            char = self._peek.strip()  # type: ignore
            if char and char not in self.SINGLE_TOKENS:
                self._advance()
            else:
                break
        self._add(
            TokenType.VAR
            if self._prev_token_type == TokenType.PARAMETER
            else self.KEYWORDS.get(self._text.upper(), TokenType.VAR)
        )

    def _extract_string(self, delimiter: str) -> str:
        text = ""
        delim_size = len(delimiter)

        while True:
            if self._char in self._ESCAPES and self._peek == delimiter:  # type: ignore
                text += delimiter
                self._advance(2)
            else:
                if self._chars(delim_size) == delimiter:
                    if delim_size > 1:
                        self._advance(delim_size - 1)
                    break

                if self._end:
                    raise RuntimeError(f"Missing {delimiter} from {self._line}:{self._start}")
                text += self._char  # type: ignore
                self._advance()

        return text
