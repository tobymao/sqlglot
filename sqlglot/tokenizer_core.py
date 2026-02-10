"""
Core tokenizer implementation - compiled with mypyc for performance.
This module contains Token, TokenType, and the hot-path scanning functions.
"""

from __future__ import annotations

import typing as t
from dataclasses import dataclass
from enum import auto

from sqlglot.helper import AutoName
from sqlglot.trie import TrieResult, in_trie
from sqlglot.errors import TokenError


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
    DOTCOLON = auto()
    DCOLON = auto()
    DCOLONDOLLAR = auto()
    DCOLONPERCENT = auto()
    DCOLONQMARK = auto()
    DQMARK = auto()
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
    COLON_EQ = auto()
    COLON_GT = auto()
    NCOLON_GT = auto()
    AND = auto()
    OR = auto()
    AMP = auto()
    DPIPE = auto()
    PIPE_GT = auto()
    PIPE = auto()
    PIPE_SLASH = auto()
    DPIPE_SLASH = auto()
    CARET = auto()
    CARET_AT = auto()
    TILDE = auto()
    ARROW = auto()
    DARROW = auto()
    FARROW = auto()
    HASH = auto()
    HASH_ARROW = auto()
    DHASH_ARROW = auto()
    LR_ARROW = auto()
    DAT = auto()
    LT_AT = auto()
    AT_GT = auto()
    DOLLAR = auto()
    PARAMETER = auto()
    SESSION = auto()
    SESSION_PARAMETER = auto()
    SESSION_USER = auto()
    DAMP = auto()
    AMP_LT = auto()
    AMP_GT = auto()
    ADJACENT = auto()
    XOR = auto()
    DSTAR = auto()
    QMARK_AMP = auto()
    QMARK_PIPE = auto()
    HASH_DASH = auto()
    EXCLAMATION = auto()
    URI_START = auto()
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
    WAREHOUSE = auto()
    STAGE = auto()
    STREAMLIT = auto()
    VAR = auto()
    BIT_STRING = auto()
    HEX_STRING = auto()
    BYTE_STRING = auto()
    NATIONAL_STRING = auto()
    RAW_STRING = auto()
    HEREDOC_STRING = auto()
    UNICODE_STRING = auto()
    # types
    BIT = auto()
    BOOLEAN = auto()
    TINYINT = auto()
    UTINYINT = auto()
    SMALLINT = auto()
    USMALLINT = auto()
    MEDIUMINT = auto()
    UMEDIUMINT = auto()
    INT = auto()
    UINT = auto()
    BIGINT = auto()
    UBIGINT = auto()
    BIGNUM = auto()
    INT128 = auto()
    UINT128 = auto()
    INT256 = auto()
    UINT256 = auto()
    FLOAT = auto()
    DOUBLE = auto()
    UDOUBLE = auto()
    DECIMAL = auto()
    DECIMAL32 = auto()
    DECIMAL64 = auto()
    DECIMAL128 = auto()
    DECIMAL256 = auto()
    DECFLOAT = auto()
    UDECIMAL = auto()
    BIGDECIMAL = auto()
    CHAR = auto()
    NCHAR = auto()
    VARCHAR = auto()
    NVARCHAR = auto()
    BPCHAR = auto()
    TEXT = auto()
    MEDIUMTEXT = auto()
    LONGTEXT = auto()
    BLOB = auto()
    MEDIUMBLOB = auto()
    LONGBLOB = auto()
    TINYBLOB = auto()
    TINYTEXT = auto()
    NAME = auto()
    BINARY = auto()
    VARBINARY = auto()
    JSON = auto()
    JSONB = auto()
    TIME = auto()
    TIMETZ = auto()
    TIME_NS = auto()
    TIMESTAMP = auto()
    TIMESTAMPTZ = auto()
    TIMESTAMPLTZ = auto()
    TIMESTAMPNTZ = auto()
    TIMESTAMP_S = auto()
    TIMESTAMP_MS = auto()
    TIMESTAMP_NS = auto()
    DATETIME = auto()
    DATETIME2 = auto()
    DATETIME64 = auto()
    SMALLDATETIME = auto()
    DATE = auto()
    DATE32 = auto()
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
    GEOGRAPHYPOINT = auto()
    NULLABLE = auto()
    GEOMETRY = auto()
    POINT = auto()
    RING = auto()
    LINESTRING = auto()
    LOCALTIME = auto()
    LOCALTIMESTAMP = auto()
    SYSTIMESTAMP = auto()
    MULTILINESTRING = auto()
    POLYGON = auto()
    MULTIPOLYGON = auto()
    HLLSKETCH = auto()
    HSTORE = auto()
    SUPER = auto()
    SERIAL = auto()
    SMALLSERIAL = auto()
    BIGSERIAL = auto()
    XML = auto()
    YEAR = auto()
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
    IPV4 = auto()
    IPV6 = auto()
    ENUM = auto()
    ENUM8 = auto()
    ENUM16 = auto()
    FIXEDSTRING = auto()
    LOWCARDINALITY = auto()
    NESTED = auto()
    AGGREGATEFUNCTION = auto()
    SIMPLEAGGREGATEFUNCTION = auto()
    TDIGEST = auto()
    UNKNOWN = auto()
    VECTOR = auto()
    DYNAMIC = auto()
    VOID = auto()
    # keywords
    ALIAS = auto()
    ALTER = auto()
    ALL = auto()
    ANTI = auto()
    ANY = auto()
    APPLY = auto()
    ARRAY = auto()
    ASC = auto()
    ASOF = auto()
    ATTACH = auto()
    AUTO_INCREMENT = auto()
    BEGIN = auto()
    BETWEEN = auto()
    BULK_COLLECT_INTO = auto()
    CACHE = auto()
    CASE = auto()
    CHARACTER_SET = auto()
    CLUSTER_BY = auto()
    COLLATE = auto()
    COMMAND = auto()
    COMMENT = auto()
    COMMIT = auto()
    CONNECT_BY = auto()
    CONSTRAINT = auto()
    COPY = auto()
    CREATE = auto()
    CROSS = auto()
    CUBE = auto()
    CURRENT_DATE = auto()
    CURRENT_DATETIME = auto()
    CURRENT_SCHEMA = auto()
    CURRENT_TIME = auto()
    CURRENT_TIMESTAMP = auto()
    CURRENT_USER = auto()
    CURRENT_ROLE = auto()
    CURRENT_CATALOG = auto()
    DECLARE = auto()
    DEFAULT = auto()
    DELETE = auto()
    DESC = auto()
    DESCRIBE = auto()
    DETACH = auto()
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
    FILE = auto()
    FILE_FORMAT = auto()
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
    GET = auto()
    GLOB = auto()
    GLOBAL = auto()
    GRANT = auto()
    GROUP_BY = auto()
    GROUPING_SETS = auto()
    HAVING = auto()
    HINT = auto()
    IGNORE = auto()
    ILIKE = auto()
    IN = auto()
    INDEX = auto()
    INDEXED_BY = auto()
    INNER = auto()
    INSERT = auto()
    INSTALL = auto()
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
    KEY = auto()
    KILL = auto()
    LANGUAGE = auto()
    LATERAL = auto()
    LEFT = auto()
    LIKE = auto()
    LIMIT = auto()
    LIST = auto()
    LOAD = auto()
    LOCK = auto()
    MAP = auto()
    MATCH = auto()
    MATCH_CONDITION = auto()
    MATCH_RECOGNIZE = auto()
    MEMBER_OF = auto()
    MERGE = auto()
    MOD = auto()
    MODEL = auto()
    NATURAL = auto()
    NEXT = auto()
    NOTHING = auto()
    NOTNULL = auto()
    NULL = auto()
    OBJECT_IDENTIFIER = auto()
    OFFSET = auto()
    ON = auto()
    ONLY = auto()
    OPERATOR = auto()
    ORDER_BY = auto()
    ORDER_SIBLINGS_BY = auto()
    ORDERED = auto()
    ORDINALITY = auto()
    OUT = auto()
    INOUT = auto()
    OUTER = auto()
    OVER = auto()
    OVERLAPS = auto()
    OVERWRITE = auto()
    PARTITION = auto()
    PARTITION_BY = auto()
    PERCENT = auto()
    PIVOT = auto()
    PLACEHOLDER = auto()
    POSITIONAL = auto()
    PRAGMA = auto()
    PREWHERE = auto()
    PRIMARY_KEY = auto()
    PROCEDURE = auto()
    PROPERTIES = auto()
    PSEUDO_TYPE = auto()
    PUT = auto()
    QUALIFY = auto()
    QUOTE = auto()
    QDCOLON = auto()
    RANGE = auto()
    RECURSIVE = auto()
    REFRESH = auto()
    RENAME = auto()
    REPLACE = auto()
    RETURNING = auto()
    REVOKE = auto()
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
    SEQUENCE = auto()
    SERDE_PROPERTIES = auto()
    SET = auto()
    SETTINGS = auto()
    SHOW = auto()
    SIMILAR_TO = auto()
    SOME = auto()
    SORT_BY = auto()
    SOUNDS_LIKE = auto()
    START_WITH = auto()
    STORAGE_INTEGRATION = auto()
    STRAIGHT_JOIN = auto()
    STRUCT = auto()
    SUMMARIZE = auto()
    TABLE_SAMPLE = auto()
    TAG = auto()
    TEMPORARY = auto()
    TOP = auto()
    THEN = auto()
    TRUE = auto()
    TRUNCATE = auto()
    UNCACHE = auto()
    UNION = auto()
    UNNEST = auto()
    UNPIVOT = auto()
    UPDATE = auto()
    USE = auto()
    USING = auto()
    VALUES = auto()
    VARIADIC = auto()
    VIEW = auto()
    SEMANTIC_VIEW = auto()
    VOLATILE = auto()
    WHEN = auto()
    WHERE = auto()
    WINDOW = auto()
    WITH = auto()
    UNIQUE = auto()
    UTC_DATE = auto()
    UTC_TIME = auto()
    UTC_TIMESTAMP = auto()
    VERSION_SNAPSHOT = auto()
    TIMESTAMP_SNAPSHOT = auto()
    OPTION = auto()
    SINK = auto()
    SOURCE = auto()
    ANALYZE = auto()
    NAMESPACE = auto()
    EXPORT = auto()
    # sentinel
    HIVE_TOKEN_STREAM = auto()


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
        comments: t.Optional[t.List[str]] = None,
    ) -> None:
        self.token_type = token_type
        self.text = text
        self.line = line
        self.col = col
        self.start = start
        self.end = end
        self.comments = [] if comments is None else comments

    def __repr__(self) -> str:
        return (
            f"<Token token_type: {self.token_type}, text: {self.text}, "
            f"line: {self.line}, col: {self.col}, start: {self.start}, "
            f"end: {self.end}, comments: {self.comments}>"
        )


@dataclass
class TokenizerConfig:
    """Configuration passed from Tokenizer to TokenizerCore."""

    # Class-level config from Tokenizer
    single_tokens: t.Dict[str, TokenType]
    keywords: t.Dict[str, TokenType]
    white_space: t.Dict[t.Optional[str], TokenType]
    quotes: t.Dict[str, str]
    format_strings: t.Dict[str, t.Tuple[str, TokenType]]
    identifiers: t.Dict[str, str]
    comments: t.Dict[str, t.Any]  # values can be str or None
    string_escapes: t.Set[str]
    byte_string_escapes: t.Set[str]
    identifier_escapes: t.Set[str]
    escape_follow_chars: t.Set[str]
    commands: t.Set[TokenType]
    command_prefix_tokens: t.Set[TokenType]
    nested_comments: bool
    hint_start: str
    tokens_preceding_hint: t.Set[TokenType]
    bit_strings: t.List[t.Any]
    hex_strings: t.List[t.Any]
    numeric_literals: t.Dict[str, str]
    var_single_tokens: t.Set[str]
    string_escapes_allowed_in_raw_strings: bool
    heredoc_tag_is_identifier: bool
    heredoc_string_alternative: TokenType
    keyword_trie: t.Dict[t.Any, t.Any]
    # Dialect-specific config
    numbers_can_be_underscore_separated: bool
    identifiers_can_start_with_digit: bool
    unescaped_sequences: t.Dict[str, str]


class TokenizerCore:
    """
    Core tokenizer with hot-path scanning methods.
    This class is designed to be compiled with mypyc for performance.
    """

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
        "_config",
    )

    def __init__(self, config: TokenizerConfig) -> None:
        self._config = config
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

    def _scan(self, until: t.Optional[t.Callable[[], bool]] = None) -> None:
        cfg = self._config

        while self.size and not self._end:
            current = self._current

            # Skip spaces here rather than iteratively calling advance() for performance reasons
            while current < self.size:
                char = self.sql[current]

                if char.isspace() and (char == " " or char == "\t"):
                    current += 1
                else:
                    break

            offset = current - self._current if current > self._current else 1

            self._start = current
            self._advance(offset)

            if not self._char.isspace():
                if self._char.isdigit():
                    self._scan_number()
                elif self._char in cfg.identifiers:
                    self._scan_identifier(cfg.identifiers[self._char])
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
        # Check for line break directly instead of dict lookup
        # Common line break chars: \n, \r (BREAK type in WHITE_SPACE)
        char = self._char
        if char == "\n" or char == "\r":
            # Ensures we don't count an extra line if we get a \r\n line break sequence
            if not (char == "\r" and self._peek == "\n"):
                self._col = i
                self._line += 1
        else:
            self._col += i

        self._current += i
        self._end = self._current >= self.size
        self._char = self.sql[self._current - 1]
        self._peek = "" if self._end else self.sql[self._current]

        if alnum and self._char.isalnum():
            # Here we use local variables instead of attributes for better performance
            sql = self.sql
            size = self.size
            _col = self._col
            _current = self._current
            _end = self._end
            _peek = self._peek

            while _peek.isalnum():
                _col += 1
                _current += 1
                _end = _current >= size
                _peek = "" if _end else sql[_current]

            self._col = _col
            self._current = _current
            self._end = _end
            self._peek = _peek
            self._char = sql[_current - 1]

    @property
    def _text(self) -> str:
        return self.sql[self._start : self._current]

    def _add(self, token_type: TokenType, text: t.Optional[str] = None) -> None:
        cfg = self._config
        self._prev_token_line = self._line

        if self._comments and token_type == TokenType.SEMICOLON and self.tokens:
            self.tokens[-1].comments.extend(self._comments)
            self._comments = []

        # Inline _text to avoid property overhead
        if text is None:
            text = self.sql[self._start : self._current]

        self.tokens.append(
            Token(
                token_type,
                text=text,
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
            token_type in cfg.commands
            and self._peek != ";"
            and (len(self.tokens) == 1 or self.tokens[-2].token_type in cfg.command_prefix_tokens)
        ):
            start = self._current
            tokens = len(self.tokens)
            self._scan(self._until_semicolon)
            self.tokens = self.tokens[:tokens]
            text = self.sql[start : self._current].strip()
            if text:
                self._add(TokenType.STRING, text)

    def _until_semicolon(self) -> bool:
        return self._peek == ";"

    def _scan_keywords(self) -> None:
        cfg = self._config
        sql = self.sql
        sql_size = self.size
        single_tokens = cfg.single_tokens
        size = 0
        word = None
        chars = self._char
        char = chars
        prev_space = False
        skip = False
        trie = cfg.keyword_trie
        single_token = char in single_tokens

        while chars:
            if skip:
                result = TrieResult.PREFIX
            else:
                result, trie = in_trie(trie, char.upper())

            if result == TrieResult.FAILED:
                break
            if result == TrieResult.EXISTS:
                word = chars

            end = self._current + size
            size += 1

            if end < sql_size:
                char = sql[end]
                single_token = single_token or char in single_tokens
                is_space = char.isspace()

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
                break

        if word:
            if self._scan_string(word):
                return
            if self._scan_comment(word):
                return
            if prev_space or single_token or not char:
                self._advance(size - 1)
                word = word.upper()
                self._add(cfg.keywords[word], text=word)
                return

        if self._char in single_tokens:
            self._add(single_tokens[self._char], text=self._char)
            return

        self._scan_var()

    def _scan_comment(self, comment_start: str) -> bool:
        cfg = self._config
        if comment_start not in cfg.comments:
            return False

        comment_start_line = self._line
        comment_start_size = len(comment_start)
        comment_end = cfg.comments[comment_start]

        if comment_end:
            # Skip the comment's start delimiter
            self._advance(comment_start_size)

            comment_count = 1
            comment_end_size = len(comment_end)

            while not self._end:
                if self._chars(comment_end_size) == comment_end:
                    comment_count -= 1
                    if not comment_count:
                        break

                self._advance(alnum=True)

                # Nested comments are allowed by some dialects, e.g. databricks, duckdb, postgres
                if (
                    cfg.nested_comments
                    and not self._end
                    and self._chars(comment_end_size) == comment_start
                ):
                    self._advance(comment_start_size)
                    comment_count += 1

            self._comments.append(self._text[comment_start_size : -comment_end_size + 1])
            self._advance(comment_end_size - 1)
        else:
            while not self._end and cfg.white_space.get(self._peek) is not TokenType.BREAK:
                self._advance(alnum=True)
            self._comments.append(self._text[comment_start_size:])

        if (
            comment_start == cfg.hint_start
            and self.tokens
            and self.tokens[-1].token_type in cfg.tokens_preceding_hint
        ):
            self._add(TokenType.HINT)

        # Leading comment is attached to the succeeding token, whilst trailing comment to the preceding.
        # Multiple consecutive comments are preserved by appending them to the current comments list.
        if comment_start_line == self._prev_token_line:
            self.tokens[-1].comments.extend(self._comments)
            self._comments = []
            self._prev_token_line = self._line

        return True

    def _scan_number(self) -> None:
        cfg = self._config

        if self._char == "0":
            peek = self._peek.upper()
            if peek == "B":
                return self._scan_bits() if cfg.bit_strings else self._add(TokenType.NUMBER)
            elif peek == "X":
                return self._scan_hex() if cfg.hex_strings else self._add(TokenType.NUMBER)

        decimal = False
        scientific = 0

        while True:
            if self._peek.isdigit():
                self._advance()
            elif self._peek == "." and not decimal:
                if self.tokens and self.tokens[-1].token_type == TokenType.PARAMETER:
                    return self._add(TokenType.NUMBER)
                decimal = True
                self._advance()
            elif self._peek in ("-", "+") and scientific == 1:
                # Only consume +/- if followed by a digit
                if self._current + 1 < self.size and self.sql[self._current + 1].isdigit():
                    scientific += 1
                    self._advance()
                else:
                    return self._add(TokenType.NUMBER)
            elif self._peek.upper() == "E" and not scientific:
                scientific += 1
                self._advance()
            elif self._peek == "_" and cfg.numbers_can_be_underscore_separated:
                self._advance()
            elif self._peek.isidentifier():
                number_text = self._text
                literal = ""

                while self._peek.strip() and self._peek not in cfg.single_tokens:
                    literal += self._peek
                    self._advance()

                token_type = cfg.keywords.get(cfg.numeric_literals.get(literal.upper(), ""))

                if token_type:
                    self._add(TokenType.NUMBER, number_text)
                    self._add(TokenType.DCOLON, "::")
                    return self._add(token_type, literal)
                elif cfg.identifiers_can_start_with_digit:
                    return self._add(TokenType.VAR)

                self._advance(-len(literal))
                return self._add(TokenType.NUMBER, number_text)
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
        cfg = self._config
        while True:
            char = self._peek.strip()
            if char and char not in cfg.single_tokens:
                self._advance(alnum=True)
            else:
                break

        return self._text

    def _scan_string(self, start: str) -> bool:
        cfg = self._config
        base = None
        token_type = TokenType.STRING

        if start in cfg.quotes:
            end = cfg.quotes[start]
        elif start in cfg.format_strings:
            end, token_type = cfg.format_strings[start]

            if token_type == TokenType.HEX_STRING:
                base = 16
            elif token_type == TokenType.BIT_STRING:
                base = 2
            elif token_type == TokenType.HEREDOC_STRING:
                self._advance()

                if self._char == end:
                    tag = ""
                else:
                    tag = self._extract_string(
                        end,
                        raw_string=True,
                        raise_unmatched=not cfg.heredoc_tag_is_identifier,
                    )

                if (
                    tag
                    and cfg.heredoc_tag_is_identifier
                    and (self._end or tag.isdigit() or any(c.isspace() for c in tag))
                ):
                    if not self._end:
                        self._advance(-1)

                    self._advance(-len(tag))
                    self._add(cfg.heredoc_string_alternative)
                    return True

                end = f"{start}{tag}{end}"
        else:
            return False

        self._advance(len(start))
        text = self._extract_string(
            end,
            escapes=(
                cfg.byte_string_escapes
                if token_type == TokenType.BYTE_STRING
                else cfg.string_escapes
            ),
            raw_string=token_type == TokenType.RAW_STRING,
        )

        if base and text:
            try:
                int(text, base)
            except Exception:
                raise TokenError(
                    f"Numeric string contains invalid characters from {self._line}:{self._start}"
                )

        self._add(token_type, text)
        return True

    def _scan_identifier(self, identifier_end: str) -> None:
        cfg = self._config
        self._advance()
        text = self._extract_string(
            identifier_end, escapes=cfg.identifier_escapes | {identifier_end}
        )
        self._add(TokenType.IDENTIFIER, text)

    def _scan_var(self) -> None:
        cfg = self._config
        var_single_tokens = cfg.var_single_tokens
        single_tokens = cfg.single_tokens

        while True:
            peek = self._peek
            # Avoid .strip() - just check if it's whitespace or empty
            if not peek or peek.isspace():
                break
            if peek not in var_single_tokens and peek in single_tokens:
                break
            self._advance(alnum=True)

        self._add(
            TokenType.VAR
            if self.tokens and self.tokens[-1].token_type == TokenType.PARAMETER
            else cfg.keywords.get(self.sql[self._start : self._current].upper(), TokenType.VAR)
        )

    def _extract_string(
        self,
        delimiter: str,
        escapes: t.Optional[t.Set[str]] = None,
        raw_string: bool = False,
        raise_unmatched: bool = True,
    ) -> str:
        cfg = self._config
        text = ""
        delim_size = len(delimiter)
        escapes = cfg.string_escapes if escapes is None else escapes

        while True:
            if not raw_string and cfg.unescaped_sequences and self._peek and self._char in escapes:
                unescaped_sequence = cfg.unescaped_sequences.get(self._char + self._peek)
                if unescaped_sequence:
                    self._advance(2)
                    text += unescaped_sequence
                    continue

            is_valid_custom_escape = (
                cfg.escape_follow_chars
                and self._char == "\\"
                and self._peek not in cfg.escape_follow_chars
            )

            if (
                (cfg.string_escapes_allowed_in_raw_strings or not raw_string)
                and self._char in escapes
                and (self._peek == delimiter or self._peek in escapes or is_valid_custom_escape)
                and (self._char not in cfg.quotes or self._char == self._peek)
            ):
                if self._peek == delimiter:
                    text += self._peek
                elif is_valid_custom_escape and self._char != self._peek:
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
                    if not raise_unmatched:
                        return text + self._char

                    raise TokenError(f"Missing {delimiter} from {self._line}:{self._start}")

                current = self._current - 1
                self._advance(alnum=True)
                text += self.sql[current : self._current - 1]

        return text
