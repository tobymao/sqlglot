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
    AND = auto()
    OR = auto()
    AMP = auto()
    DPIPE = auto()
    PIPE = auto()
    CARET = auto()
    TILDA = auto()
    ARROW = auto()
    DARROW = auto()
    HASH_ARROW = auto()
    DHASH_ARROW = auto()
    ANNOTATION = auto()
    DOLLAR = auto()

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
    NCHAR = auto()
    VARCHAR = auto()
    NVARCHAR = auto()
    TEXT = auto()
    BINARY = auto()
    BYTEA = auto()
    JSON = auto()
    TIMESTAMP = auto()
    TIMESTAMPTZ = auto()
    DATETIME = auto()
    DATE = auto()
    UUID = auto()
    GEOGRAPHY = auto()
    NULLABLE = auto()

    # keywords
    ADD_FILE = auto()
    ALIAS = auto()
    ALL = auto()
    ALTER = auto()
    ANALYZE = auto()
    ANY = auto()
    ARRAY = auto()
    ASC = auto()
    AT_TIME_ZONE = auto()
    AUTO_INCREMENT = auto()
    BEGIN = auto()
    BETWEEN = auto()
    BUCKET = auto()
    CACHE = auto()
    CALL = auto()
    CASE = auto()
    CAST = auto()
    CHARACTER_SET = auto()
    CLUSTER_BY = auto()
    COLLATE = auto()
    COMMENT = auto()
    COMMENT_END = auto()
    COMMENT_START = auto()
    COMMIT = auto()
    CONSTRAINT = auto()
    CONVERT = auto()
    CREATE = auto()
    CROSS = auto()
    CUBE = auto()
    CURRENT_DATE = auto()
    CURRENT_DATETIME = auto()
    CURRENT_ROW = auto()
    CURRENT_TIME = auto()
    CURRENT_TIMESTAMP = auto()
    DIV = auto()
    DEFAULT = auto()
    DELETE = auto()
    DESC = auto()
    DISTINCT = auto()
    DISTRIBUTE_BY = auto()
    DROP = auto()
    ELSE = auto()
    END = auto()
    ENGINE = auto()
    ESCAPE = auto()
    EXCEPT = auto()
    EXISTS = auto()
    EXPLAIN = auto()
    EXTRACT = auto()
    FALSE = auto()
    FETCH = auto()
    FILTER = auto()
    FINAL = auto()
    FIRST = auto()
    FOLLOWING = auto()
    FOREIGN_KEY = auto()
    FORMAT = auto()
    FULL = auto()
    FUNCTION = auto()
    FROM = auto()
    GROUP_BY = auto()
    GROUPING_SETS = auto()
    HAVING = auto()
    HINT = auto()
    IF = auto()
    IGNORE_NULLS = auto()
    ILIKE = auto()
    IN = auto()
    INNER = auto()
    INSERT = auto()
    INTERSECT = auto()
    INTERVAL = auto()
    INTO = auto()
    IS = auto()
    ISNULL = auto()
    JOIN = auto()
    LATERAL = auto()
    LAZY = auto()
    LEFT = auto()
    LIKE = auto()
    LIMIT = auto()
    LOCATION = auto()
    MAP = auto()
    MOD = auto()
    NEXT = auto()
    NO_ACTION = auto()
    NULL = auto()
    NULLS_FIRST = auto()
    NULLS_LAST = auto()
    OFFSET = auto()
    ON = auto()
    ONLY = auto()
    OPTIMIZE = auto()
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
    PLACEHOLDER = auto()
    PRECEDING = auto()
    PRIMARY_KEY = auto()
    PROPERTIES = auto()
    QUALIFY = auto()
    QUOTE = auto()
    RANGE = auto()
    RECURSIVE = auto()
    REPLACE = auto()
    RESPECT_NULLS = auto()
    REFERENCES = auto()
    RIGHT = auto()
    RLIKE = auto()
    ROLLUP = auto()
    ROW = auto()
    ROWS = auto()
    SCHEMA_COMMENT = auto()
    SELECT = auto()
    SET = auto()
    SHOW = auto()
    SOME = auto()
    SORT_BY = auto()
    STORED = auto()
    STRUCT = auto()
    TABLE_FORMAT = auto()
    TABLE_SAMPLE = auto()
    TEMPORARY = auto()
    TIME = auto()
    TOP = auto()
    THEN = auto()
    TRUE = auto()
    TRUNCATE = auto()
    TRY_CAST = auto()
    UNBOUNDED = auto()
    UNCACHE = auto()
    UNION = auto()
    UNNEST = auto()
    UPDATE = auto()
    USE = auto()
    USING = auto()
    VALUES = auto()
    VIEW = auto()
    WHEN = auto()
    WHERE = auto()
    WINDOW = auto()
    WITH = auto()
    WITH_TIME_ZONE = auto()
    WITHIN_GROUP = auto()
    WITHOUT_TIME_ZONE = auto()
    UNIQUE = auto()


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


class _Tokenizer(type):
    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)

        klass.QUOTES = [
            prefix + quote for prefix in ["", "r"] for quote in klass.QUOTES
        ]

        klass.KEYWORD_TRIE = new_trie(
            key.upper()
            for key, value in {
                **klass.KEYWORDS,
                **{quote: TokenType.QUOTE for quote in klass.QUOTES},
            }.items()
            if value in (TokenType.COMMENT, TokenType.COMMENT_START, TokenType.QUOTE)
            or " " in key
            or any(single in key for single in klass.SINGLE_TOKENS)
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
        "*": TokenType.STAR,
        "~": TokenType.TILDA,
        "?": TokenType.PLACEHOLDER,
        "#": TokenType.ANNOTATION,
        "$": TokenType.DOLLAR,
    }

    QUOTES = ["'"]

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
        "->": TokenType.ARROW,
        "->>": TokenType.DARROW,
        "#>": TokenType.HASH_ARROW,
        "#>>": TokenType.DHASH_ARROW,
        "ADD ARCHIVE": TokenType.ADD_FILE,
        "ADD ARCHIVES": TokenType.ADD_FILE,
        "ADD FILE": TokenType.ADD_FILE,
        "ADD FILES": TokenType.ADD_FILE,
        "ADD JAR": TokenType.ADD_FILE,
        "ADD JARS": TokenType.ADD_FILE,
        "ALL": TokenType.ALL,
        "ALTER": TokenType.ALTER,
        "ANALYZE": TokenType.ANALYZE,
        "AND": TokenType.AND,
        "ANY": TokenType.ANY,
        "ASC": TokenType.ASC,
        "AS": TokenType.ALIAS,
        "AT TIME ZONE": TokenType.AT_TIME_ZONE,
        "AUTO_INCREMENT": TokenType.AUTO_INCREMENT,
        "BEGIN": TokenType.BEGIN,
        "BETWEEN": TokenType.BETWEEN,
        "BUCKET": TokenType.BUCKET,
        "CALL": TokenType.CALL,
        "CACHE": TokenType.CACHE,
        "UNCACHE": TokenType.UNCACHE,
        "CASE": TokenType.CASE,
        "CAST": TokenType.CAST,
        "CHARACTER SET": TokenType.CHARACTER_SET,
        "CLUSTER BY": TokenType.CLUSTER_BY,
        "COLLATE": TokenType.COLLATE,
        "COMMENT": TokenType.SCHEMA_COMMENT,
        "COMMIT": TokenType.COMMIT,
        "CONSTRAINT": TokenType.CONSTRAINT,
        "CONVERT": TokenType.CONVERT,
        "CREATE": TokenType.CREATE,
        "CROSS": TokenType.CROSS,
        "CUBE": TokenType.CUBE,
        "CURRENT_DATE": TokenType.CURRENT_DATE,
        "CURRENT ROW": TokenType.CURRENT_ROW,
        "CURRENT_TIMESTAMP": TokenType.CURRENT_TIMESTAMP,
        "DIV": TokenType.DIV,
        "DEFAULT": TokenType.DEFAULT,
        "DELETE": TokenType.DELETE,
        "DESC": TokenType.DESC,
        "DISTINCT": TokenType.DISTINCT,
        "DISTRIBUTE BY": TokenType.DISTRIBUTE_BY,
        "DROP": TokenType.DROP,
        "ELSE": TokenType.ELSE,
        "END": TokenType.END,
        "ENGINE": TokenType.ENGINE,
        "ESCAPE": TokenType.ESCAPE,
        "EXCEPT": TokenType.EXCEPT,
        "EXISTS": TokenType.EXISTS,
        "EXPLAIN": TokenType.EXPLAIN,
        "EXTRACT": TokenType.EXTRACT,
        "FALSE": TokenType.FALSE,
        "FETCH": TokenType.FETCH,
        "FILTER": TokenType.FILTER,
        "FIRST": TokenType.FIRST,
        "FULL": TokenType.FULL,
        "FUNCTION": TokenType.FUNCTION,
        "FOLLOWING": TokenType.FOLLOWING,
        "FOREIGN KEY": TokenType.FOREIGN_KEY,
        "FORMAT": TokenType.FORMAT,
        "FROM": TokenType.FROM,
        "GROUP BY": TokenType.GROUP_BY,
        "GROUPING SETS": TokenType.GROUPING_SETS,
        "HAVING": TokenType.HAVING,
        "IF": TokenType.IF,
        "ILIKE": TokenType.ILIKE,
        "IGNORE NULLS": TokenType.IGNORE_NULLS,
        "IN": TokenType.IN,
        "INNER": TokenType.INNER,
        "INSERT": TokenType.INSERT,
        "INTERVAL": TokenType.INTERVAL,
        "INTERSECT": TokenType.INTERSECT,
        "INTO": TokenType.INTO,
        "IS": TokenType.IS,
        "ISNULL": TokenType.ISNULL,
        "JOIN": TokenType.JOIN,
        "LATERAL": TokenType.LATERAL,
        "LAZY": TokenType.LAZY,
        "LEFT": TokenType.LEFT,
        "LIKE": TokenType.LIKE,
        "LIMIT": TokenType.LIMIT,
        "LOCATION": TokenType.LOCATION,
        "NEXT": TokenType.NEXT,
        "NO ACTION": TokenType.NO_ACTION,
        "NOT": TokenType.NOT,
        "NULL": TokenType.NULL,
        "NULLS FIRST": TokenType.NULLS_FIRST,
        "NULLS LAST": TokenType.NULLS_LAST,
        "OFFSET": TokenType.OFFSET,
        "ON": TokenType.ON,
        "ONLY": TokenType.ONLY,
        "OPTIMIZE": TokenType.OPTIMIZE,
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
        "PERCENT": TokenType.PERCENT,
        "PRECEDING": TokenType.PRECEDING,
        "PRIMARY KEY": TokenType.PRIMARY_KEY,
        "RANGE": TokenType.RANGE,
        "RECURSIVE": TokenType.RECURSIVE,
        "REGEXP": TokenType.RLIKE,
        "REPLACE": TokenType.REPLACE,
        "RESPECT NULLS": TokenType.RESPECT_NULLS,
        "REFERENCES": TokenType.REFERENCES,
        "RIGHT": TokenType.RIGHT,
        "RLIKE": TokenType.RLIKE,
        "ROLLUP": TokenType.ROLLUP,
        "ROW": TokenType.ROW,
        "ROWS": TokenType.ROWS,
        "SELECT": TokenType.SELECT,
        "SET": TokenType.SET,
        "SHOW": TokenType.SHOW,
        "SOME": TokenType.SOME,
        "SORT BY": TokenType.SORT_BY,
        "STORED": TokenType.STORED,
        "TABLE": TokenType.TABLE,
        "TABLE_FORMAT": TokenType.TABLE_FORMAT,
        "TBLPROPERTIES": TokenType.PROPERTIES,
        "TABLESAMPLE": TokenType.TABLE_SAMPLE,
        "TEMP": TokenType.TEMPORARY,
        "TEMPORARY": TokenType.TEMPORARY,
        "THEN": TokenType.THEN,
        "TRUE": TokenType.TRUE,
        "TRUNCATE": TokenType.TRUNCATE,
        "TRY_CAST": TokenType.TRY_CAST,
        "UNBOUNDED": TokenType.UNBOUNDED,
        "UNION": TokenType.UNION,
        "UNNEST": TokenType.UNNEST,
        "UPDATE": TokenType.UPDATE,
        "USE": TokenType.USE,
        "USING": TokenType.USING,
        "VALUES": TokenType.VALUES,
        "VIEW": TokenType.VIEW,
        "WHEN": TokenType.WHEN,
        "WHERE": TokenType.WHERE,
        "WITH": TokenType.WITH,
        "WITH TIME ZONE": TokenType.WITH_TIME_ZONE,
        "WITHIN GROUP": TokenType.WITHIN_GROUP,
        "WITHOUT TIME ZONE": TokenType.WITHOUT_TIME_ZONE,
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
        "NCHAR": TokenType.NCHAR,
        "VARCHAR": TokenType.VARCHAR,
        "VARCHAR2": TokenType.VARCHAR,
        "NVARCHAR": TokenType.NVARCHAR,
        "NVARCHAR2": TokenType.NVARCHAR,
        "STRING": TokenType.TEXT,
        "TEXT": TokenType.TEXT,
        "BINARY": TokenType.BINARY,
        "BYTEA": TokenType.BINARY,
        "TIMESTAMP": TokenType.TIMESTAMP,
        "TIMESTAMPTZ": TokenType.TIMESTAMPTZ,
        "DATE": TokenType.DATE,
        "DATETIME": TokenType.DATETIME,
        "UNIQUE": TokenType.UNIQUE,
        "STRUCT": TokenType.STRUCT,
    }

    WHITE_SPACE = {
        " ": TokenType.SPACE,
        "\t": TokenType.SPACE,
        "\n": TokenType.BREAK,
        "\r": TokenType.BREAK,
        "\r\n": TokenType.BREAK,
    }

    COMMANDS = {
        TokenType.ALTER,
        TokenType.ADD_FILE,
        TokenType.ANALYZE,
        TokenType.BEGIN,
        TokenType.CALL,
        TokenType.COMMIT,
        TokenType.EXPLAIN,
        TokenType.OPTIMIZE,
        TokenType.SET,
        TokenType.SHOW,
        TokenType.TRUNCATE,
        TokenType.USE,
    }

    # handle numeric literals like in hive (3L = BIGINT)
    NUMERIC_LITERALS = {}
    ENCODE = None

    COMMENTS = {"--"}
    COMMENT_START = "/*"
    COMMENT_END = "*/"
    KEYWORD_TRIE = None  # autofilled

    __slots__ = (
        "identifiers",
        "escape",
        "sql",
        "size",
        "tokens",
        "_start",
        "_current",
        "_line",
        "_col",
        "_char",
        "_end",
        "_peek",
    )

    def __init__(
        self,
        identifiers=None,
        escape=None,
    ):
        """
        Tokenizer consumes a sql string and produces an array of :class:`~sqlglot.tokens.Token`

        Args
            identifiers (dict): keys are the identifer start characters and values are the identifier end characters
            escape (str): the escape code character
        """
        self.identifiers = identifiers or {'"': '"'}
        self.escape = escape or "'"
        self.reset()

    def reset(self):
        self.sql = ""
        self.size = 0
        self.tokens = []
        self._start = 0
        self._current = 0
        self._line = 1
        self._col = 1

        self._char = None
        self._end = None
        self._peek = None

    def tokenize(self, sql):
        self.reset()
        self.sql = sql
        self.size = len(sql)

        while self.size and not self._end:
            self._start = self._current
            self._advance()

            if not self._char:
                break

            white_space = self.WHITE_SPACE.get(self._char)
            identifier_end = self.identifiers.get(self._char)

            if white_space:
                if white_space == TokenType.BREAK:
                    self._col = 1
                    self._line += 1
            elif self._char.isdigit():
                self._scan_number()
            elif identifier_end:
                self._scan_identifier(identifier_end)
            else:
                self._scan_keywords()
        return self.tokens

    def _chars(self, size):
        if size == 1:
            return self._char
        start = self._current - 1
        end = start + size
        if end <= self.size:
            return self.sql[start:end]
        return ""

    def _advance(self, i=1):
        self._col += i
        self._current += i
        self._end = self._current >= self.size
        self._char = self.sql[self._current - 1]
        self._peek = self.sql[self._current] if self._current < self.size else ""

    @property
    def _text(self):
        return self.sql[self._start : self._current]

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

    def _scan_keywords(self):
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
                result, trie = in_trie(trie, char.upper())

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
                chars = None

        if not word:
            if self._char in self.SINGLE_TOKENS:
                token = self.SINGLE_TOKENS[self._char]
                if token == TokenType.ANNOTATION:
                    self._scan_annotation()
                    return
                self._add(token)
                return
            self._scan_var()
            return

        if self._scan_comment(word):
            return
        if self._scan_string(word):
            return

        self._advance(size - 1)
        self._add(self.KEYWORDS[word.upper()])

    def _scan_comment(self, comment):
        if comment in self.COMMENTS:
            while not self._end and self.WHITE_SPACE.get(self._peek) != TokenType.BREAK:
                self._advance()
            return True

        if comment == self.COMMENT_START:
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
            elif self._peek in ("-", "+") and scientific == 1:
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
                token_type = self.KEYWORDS.get(self.NUMERIC_LITERALS.get(literal))
                if token_type:
                    self._add(TokenType.DCOLON, "::")
                    return self._add(token_type, literal)
                return self._advance(-len(literal))
            else:
                return self._add(TokenType.NUMBER)

    def _scan_string(self, quote):
        if quote not in self.QUOTES:
            return False

        size = len(quote)
        text = ""
        self._advance(size)

        if quote.startswith("r"):
            quote = quote[1:]
            size = len(quote)

        while True:
            if self._char == self.escape and self._peek == quote:
                text += quote
                self._advance(2)
            else:
                if self._chars(size) == quote:
                    if size > 1:
                        self._advance(size - 1)
                    break

                if self._end:
                    raise RuntimeError(
                        f"Missing {quote} from {self._line}:{self._start}"
                    )
                text += self._char
                self._advance()

        text = text.encode(self.ENCODE).decode(self.ENCODE) if self.ENCODE else text
        text = text.replace("\\\\", "\\") if self.escape == "\\" else text
        self._add(TokenType.STRING, text)
        return True

    def _scan_identifier(self, identifier_end):
        while self._peek != identifier_end:
            if self._end:
                raise RuntimeError(
                    f"Missing {identifier_end} from {self._line}:{self._start}"
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
