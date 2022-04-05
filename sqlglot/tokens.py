from enum import auto

from sqlglot.helper import AutoName, ensure_list, list_get
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
    AND = auto()
    OR = auto()
    AMP = auto()
    DPIPE = auto()
    PIPE = auto()
    CARET = auto()
    TILDA = auto()
    LSHIFT = auto()
    RSHIFT = auto()
    LAMBDA = auto()
    ANNOTATION = auto()

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
    VARCHAR = auto()
    TEXT = auto()
    BINARY = auto()
    BYTEA = auto()
    JSON = auto()
    TIMESTAMP = auto()
    TIMESTAMPTZ = auto()
    DATE = auto()
    UUID = auto()

    # keywords
    ADD_FILE = auto()
    ALIAS = auto()
    ALL = auto()
    ALTER = auto()
    ARRAY = auto()
    ASC = auto()
    AUTO_INCREMENT = auto()
    BETWEEN = auto()
    BUCKET = auto()
    BY = auto()
    CACHE = auto()
    UNCACHE = auto()
    CASE = auto()
    CAST = auto()
    CHARACTER_SET = auto()
    COUNT = auto()
    COLLATE = auto()
    COMMENT = auto()
    COMMENT_END = auto()
    COMMENT_START = auto()
    CREATE = auto()
    CROSS = auto()
    CURRENT_ROW = auto()
    DIV = auto()
    DEFAULT = auto()
    DELETE = auto()
    DESC = auto()
    DISTINCT = auto()
    DROP = auto()
    ELSE = auto()
    END = auto()
    ENGINE = auto()
    EXCEPT = auto()
    EXISTS = auto()
    EXPLAIN = auto()
    EXTRACT = auto()
    FALSE = auto()
    FOLLOWING = auto()
    FULL = auto()
    FROM = auto()
    GROUP = auto()
    HAVING = auto()
    HINT = auto()
    IF = auto()
    ILIKE = auto()
    IN = auto()
    INNER = auto()
    INSERT = auto()
    INTERSECT = auto()
    INTERVAL = auto()
    INTO = auto()
    IS = auto()
    JOIN = auto()
    LATERAL = auto()
    LAZY = auto()
    LEFT = auto()
    LIKE = auto()
    LIMIT = auto()
    MAP = auto()
    MOD = auto()
    NULL = auto()
    OFFSET = auto()
    ON = auto()
    OPTIMIZE = auto()
    OPTIONS = auto()
    ORDER = auto()
    ORDERED = auto()
    ORDINALITY = auto()
    OUTER = auto()
    OUT_OF = auto()
    OVER = auto()
    OVERWRITE = auto()
    PARTITION = auto()
    PERCENT = auto()
    PRECEDING = auto()
    PRIMARY_KEY = auto()
    PROPERTIES = auto()
    RANGE = auto()
    RECURSIVE = auto()
    REPLACE = auto()
    RIGHT = auto()
    RLIKE = auto()
    ROWS = auto()
    SCHEMA_COMMENT = auto()
    SELECT = auto()
    SET = auto()
    SHOW = auto()
    STORED = auto()
    TABLE_SAMPLE = auto()
    TEMPORARY = auto()
    TIME = auto()
    THEN = auto()
    TRUE = auto()
    TRUNCATE = auto()
    TRY_CAST = auto()
    UNBOUNDED = auto()
    UNION = auto()
    UNNEST = auto()
    UPDATE = auto()
    USE = auto()
    VALUES = auto()
    VIEW = auto()
    WHEN = auto()
    WHERE = auto()
    WITH = auto()
    WITHOUT = auto()
    ZONE = auto()


class Token:
    __slots__ = ("token_type", "text", "line", "col")

    @classmethod
    def number(cls, number):
        return cls(TokenType.NUMBER, str(number))

    @classmethod
    def string(cls, string):
        return cls(TokenType.STRING, string)

    @classmethod
    def identifier(cls, identifier):
        return cls(TokenType.IDENTIFIER, identifier)

    @classmethod
    def var(cls, var):
        return cls(TokenType.VAR, var)

    def __init__(self, token_type, text, line=1, col=1):
        self.token_type = token_type
        self.text = text
        self.line = line
        self.col = max(col - len(text), 1)

    def __repr__(self):
        attributes = ", ".join(f"{k}: {getattr(self, k)}" for k in self.__slots__)
        return f"<Token {attributes}>"


def new_ambiguous(keywords, single_tokens):
    return new_trie(
        key
        for key, value in keywords.items()
        if value not in (TokenType.COMMENT, TokenType.COMMENT_START)
        and (" " in key or any(single in key for single in single_tokens))
    )


class Tokenizer:
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
    }

    KEYWORDS = {
        "/*+": TokenType.HINT,
        "--": TokenType.COMMENT,
        "/*": TokenType.COMMENT_START,
        "*/": TokenType.COMMENT_END,
        "==": TokenType.EQ,
        "::": TokenType.DCOLON,
        "||": TokenType.DPIPE,
        ">=": TokenType.GTE,
        "<=": TokenType.LTE,
        "<>": TokenType.NEQ,
        "!=": TokenType.NEQ,
        "<<": TokenType.LSHIFT,
        ">>": TokenType.RSHIFT,
        "->": TokenType.LAMBDA,
        "ADD ARCHIVE": TokenType.ADD_FILE,
        "ADD ARCHIVES": TokenType.ADD_FILE,
        "ADD FILE": TokenType.ADD_FILE,
        "ADD FILES": TokenType.ADD_FILE,
        "ADD JAR": TokenType.ADD_FILE,
        "ADD JARS": TokenType.ADD_FILE,
        "ALL": TokenType.ALL,
        "ALTER": TokenType.ALTER,
        "AND": TokenType.AND,
        "ASC": TokenType.ASC,
        "AS": TokenType.ALIAS,
        "AUTO_INCREMENT": TokenType.AUTO_INCREMENT,
        "BETWEEN": TokenType.BETWEEN,
        "BUCKET": TokenType.BUCKET,
        "BY": TokenType.BY,
        "CACHE": TokenType.CACHE,
        "UNCACHE": TokenType.UNCACHE,
        "CASE": TokenType.CASE,
        "CAST": TokenType.CAST,
        "CHARACTER SET": TokenType.CHARACTER_SET,
        "COLLATE": TokenType.COLLATE,
        "COMMENT": TokenType.SCHEMA_COMMENT,
        "COUNT": TokenType.COUNT,
        "CREATE": TokenType.CREATE,
        "CROSS": TokenType.CROSS,
        "CURRENT ROW": TokenType.CURRENT_ROW,
        "DIV": TokenType.DIV,
        "DEFAULT": TokenType.DEFAULT,
        "DELETE": TokenType.DELETE,
        "DESC": TokenType.DESC,
        "DISTINCT": TokenType.DISTINCT,
        "DROP": TokenType.DROP,
        "ELSE": TokenType.ELSE,
        "END": TokenType.END,
        "ENGINE": TokenType.ENGINE,
        "EXCEPT": TokenType.EXCEPT,
        "EXISTS": TokenType.EXISTS,
        "EXPLAIN": TokenType.EXPLAIN,
        "EXTRACT": TokenType.EXTRACT,
        "FALSE": TokenType.FALSE,
        "FULL": TokenType.FULL,
        "FOLLOWING": TokenType.FOLLOWING,
        "FROM": TokenType.FROM,
        "GROUP": TokenType.GROUP,
        "HAVING": TokenType.HAVING,
        "IF": TokenType.IF,
        "ILIKE": TokenType.ILIKE,
        "IN": TokenType.IN,
        "INNER": TokenType.INNER,
        "INSERT": TokenType.INSERT,
        "INTERVAL": TokenType.INTERVAL,
        "INTERSECT": TokenType.INTERSECT,
        "INTO": TokenType.INTO,
        "IS": TokenType.IS,
        "JOIN": TokenType.JOIN,
        "LATERAL": TokenType.LATERAL,
        "LAZY": TokenType.LAZY,
        "LEFT": TokenType.LEFT,
        "LIKE": TokenType.LIKE,
        "LIMIT": TokenType.LIMIT,
        "NOT": TokenType.NOT,
        "NULL": TokenType.NULL,
        "OFFSET": TokenType.OFFSET,
        "ON": TokenType.ON,
        "OPTIMIZE": TokenType.OPTIMIZE,
        "OPTIONS": TokenType.OPTIONS,
        "OR": TokenType.OR,
        "ORDER": TokenType.ORDER,
        "ORDINALITY": TokenType.ORDINALITY,
        "OUTER": TokenType.OUTER,
        "OUT OF": TokenType.OUT_OF,
        "OVER": TokenType.OVER,
        "OVERWRITE": TokenType.OVERWRITE,
        "PARTITION": TokenType.PARTITION,
        "PARTITIONED": TokenType.PARTITION,
        "PERCENT": TokenType.PERCENT,
        "PRECEDING": TokenType.PRECEDING,
        "PRIMARY KEY": TokenType.PRIMARY_KEY,
        "RANGE": TokenType.RANGE,
        "RECURSIVE": TokenType.RECURSIVE,
        "REGEXP": TokenType.RLIKE,
        "REPLACE": TokenType.REPLACE,
        "RIGHT": TokenType.RIGHT,
        "RLIKE": TokenType.RLIKE,
        "ROWS": TokenType.ROWS,
        "SELECT": TokenType.SELECT,
        "SET": TokenType.SET,
        "SHOW": TokenType.SHOW,
        "STORED": TokenType.STORED,
        "TABLE": TokenType.TABLE,
        "TBLPROPERTIES": TokenType.PROPERTIES,
        "TABLESAMPLE": TokenType.TABLE_SAMPLE,
        "TEMP": TokenType.TEMPORARY,
        "TEMPORARY": TokenType.TEMPORARY,
        "THEN": TokenType.THEN,
        "TIME": TokenType.TIME,
        "TRUE": TokenType.TRUE,
        "TRUNCATE": TokenType.TRUNCATE,
        "TRY_CAST": TokenType.TRY_CAST,
        "UNBOUNDED": TokenType.UNBOUNDED,
        "UNION": TokenType.UNION,
        "UNNEST": TokenType.UNNEST,
        "UPDATE": TokenType.UPDATE,
        "USE": TokenType.USE,
        "VALUES": TokenType.VALUES,
        "VIEW": TokenType.VIEW,
        "WHEN": TokenType.WHEN,
        "WHERE": TokenType.WHERE,
        "WITH": TokenType.WITH,
        "WITHOUT": TokenType.WITHOUT,
        "ZONE": TokenType.ZONE,
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
        "VARCHAR": TokenType.VARCHAR,
        "STRING": TokenType.TEXT,
        "TEXT": TokenType.TEXT,
        "BINARY": TokenType.BINARY,
        "BYTEA": TokenType.BINARY,
        "TIMESTAMP": TokenType.TIMESTAMP,
        "TIMESTAMPTZ": TokenType.TIMESTAMPTZ,
        "DATE": TokenType.DATE,
        "UUID": TokenType.UUID,
    }

    WHITE_SPACE = {
        " ": TokenType.SPACE,
        "\t": TokenType.SPACE,
        "\n": TokenType.BREAK,
        "\r": TokenType.BREAK,
        "\rn": TokenType.BREAK,
    }

    COMMANDS = {
        TokenType.ALTER,
        TokenType.ADD_FILE,
        TokenType.EXPLAIN,
        TokenType.OPTIMIZE,
        TokenType.SET,
        TokenType.SHOW,
        TokenType.TRUNCATE,
        TokenType.USE,
    }

    ESCAPE_CODE = "__sqlglot_escape__"

    AMBIGUOUS = new_ambiguous(KEYWORDS, SINGLE_TOKENS)
    COMMENTS = ["--"]
    COMMENT_START = "/*"
    COMMENT_END = "*/"

    __slots__ = (
        "quotes",
        "identifier",
        "escape",
        "encode",
        "numeric_literals",
        "code",
        "size",
        "tokens",
        "_start",
        "_current",
        "_line",
        "_col",
        "_char",
        "_end",
        "_peek",
        "__text",
    )

    def __init__(
        self,
        quotes=None,
        identifier=None,
        escape=None,
        encode=None,
        numeric_literals=None,
    ):
        """
        Tokenizer consumes a sql string and produces an array of :class:`~sqlglot.tokens.Token`

        Args
            quotes (str | list): character to identify string literals
            identifier (str): the identifier character
            escape (str): the escape code character
            encode (str): if passed in, encode string literals and then decode
            numeric_literals (dict): if passed in, handle numeric literals like in hive (3L = BIGINT)
        """
        self.quotes = set(ensure_list(quotes) or ["'"])
        self.identifier = identifier or '"'
        self.escape = escape or "'"
        self.encode = encode
        self.numeric_literals = numeric_literals or {}
        self.reset()

    def reset(self):
        self.code = ""
        self.size = 0
        self.tokens = []
        self._start = 0
        self._current = 0
        self._line = 1
        self._col = 1

        self._char = None
        self._end = None
        self._peek = None
        self.__text = None

    def tokenize(self, code):  # pylint: disable=too-many-branches
        self.reset()
        self.code = code
        self.size = len(code)

        while not self._end:
            self._start = self._current
            self._advance()

            if not self._char:
                break
            if self._scan_ambiguous():
                pass
            elif self._scan_comments():
                pass
            elif self._char in self.SINGLE_TOKENS:
                self._add(self.SINGLE_TOKENS[self._char])
            elif self._char in self.WHITE_SPACE:
                white_space = self.WHITE_SPACE[self._char]
                if white_space == TokenType.BREAK:
                    self._col = 1
                    self._line += 1
            elif self._char.isdigit():
                self._scan_number()
            elif self._char in self.quotes:
                self._scan_string()
            elif self._char == self.identifier:
                self._scan_identifier()
            elif self._char == "#":
                self._scan_annotation()
            else:
                self._scan_var()
        return self.tokens

    def _chars(self, size):
        start = self._current - 1
        end = start + size
        if end <= self.size:
            return self.code[start:end].upper()
        return ""

    def _advance(self, i=1):
        self._col += i
        self._current += i
        self._char = list_get(self.code, self._current - 1)
        self._peek = list_get(self.code, self._current) or ""
        self._end = self._current >= self.size
        self.__text = None

    @property
    def _text(self):
        if self.__text is None:
            self.__text = self.code[self._start : self._current]
        return self.__text

    def _add(self, token_type, text=None):
        text = self._text if text is None else text
        self.tokens.append(Token(token_type, text, self._line, self._col))

        if token_type in self.COMMANDS and (
            len(self.tokens) == 1 or self.tokens[-2].token_type == TokenType.SEMICOLON
        ):
            self._start = self._current
            while not self._end and self._peek != ";":
                self._advance()
            if self._start < self._current:
                self._add(TokenType.STRING)

    def _scan_ambiguous(self):
        size = 1
        word = None
        chars = self._chars(size)

        while chars:
            result = in_trie(self.AMBIGUOUS, chars)

            if result == 0:
                break
            if result == 2:
                word = chars
            size += 1
            chars = self._chars(size)

        if word:
            self._advance(len(word) - 1)
            self._add(self.KEYWORDS[word])
            return True
        return False

    def _scan_comments(self):
        for comment in self.COMMENTS:
            if self._chars(len(comment)) == comment:
                while (
                    not self._end
                    and self.WHITE_SPACE.get(self._char) != TokenType.BREAK
                ):
                    self._advance()
                return True

        if self._chars(len(self.COMMENT_START)) == self.COMMENT_START:
            comment_end_size = len(self.COMMENT_END)
            while not self._end and self._chars(comment_end_size) != self.COMMENT_END:
                self._advance()
            self._advance(comment_end_size - 1)
            return True
        return False

    def _scan_annotation(self):
        while (
            not self._end and self._peek not in self.WHITE_SPACE and self._peek != ","
        ):
            self._advance()
        self._add(TokenType.ANNOTATION, self._text[1:])

    def _scan_number(self):
        decimal = False
        scientific = 0

        while True:
            if self._peek.isdigit():
                self._advance()
            elif self._peek == "." and not decimal:
                decimal = True
                self._advance()
            elif self._peek == "-" and scientific == 1:
                scientific += 1
                self._advance()
            elif self._peek.upper() == "E" and not scientific:
                scientific += 1
                self._advance()
            elif self._peek.isalpha():
                self._add(TokenType.NUMBER)
                literal = []
                while self._peek.isalpha():
                    literal.append(self._peek.upper())
                    self._advance()
                literal = "".join(literal)
                token_type = self.KEYWORDS.get(self.numeric_literals.get(literal))
                if token_type:
                    self._add(TokenType.DCOLON, "::")
                    return self._add(token_type, literal)
                return self._advance(-len(literal))
            else:
                return self._add(TokenType.NUMBER)

    def _scan_string(self):
        text = []
        quote = self._char

        while True:
            if self._end:
                raise RuntimeError(f"Missing {quote} from {self._line}:{self._start}")
            text.append(self._char)
            self._advance()

            if self._char == self.escape and self._peek == quote:
                text.append(self.ESCAPE_CODE)
                self._advance()
            elif self._char == quote:
                break
            elif self._char == "'":
                text.append(self.ESCAPE_CODE)

        text.append(self._char)
        text = "".join(text[1:-1])
        text = text.encode(self.encode).decode(self.encode) if self.encode else text
        text = text.replace("\\\\", "\\") if self.escape == "\\" else text
        self._add(TokenType.STRING, text)

    def _scan_identifier(self):
        while self._peek != self.identifier:
            if self._end:
                raise RuntimeError(
                    f"Missing {self.identifier} from {self._line}:{self._start}"
                )
            self._advance()
        self._advance()
        self._add(TokenType.IDENTIFIER, self._text[1:-1])

    def _scan_var(self):
        while True:
            char = self._peek.strip()
            if char and char not in self.SINGLE_TOKENS:
                self._advance()
            else:
                break
        self._add(self.KEYWORDS.get(self._text.upper(), TokenType.VAR))
