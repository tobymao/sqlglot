from enum import auto

from sqlglot.helper import AutoName, list_get
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
    JSON = auto()
    TIMESTAMP = auto()
    TIMESTAMPTZ = auto()
    DATE = auto()

    # keywords
    ALIAS = auto()
    ALL = auto()
    ARRAY = auto()
    ASC = auto()
    AUTO_INCREMENT = auto()
    BETWEEN = auto()
    BY = auto()
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
    DESC = auto()
    DISTINCT = auto()
    DROP = auto()
    ELSE = auto()
    END = auto()
    ENGINE = auto()
    EXISTS = auto()
    EXTRACT = auto()
    FOLLOWING = auto()
    FORMAT = auto()
    FULL = auto()
    FROM = auto()
    GROUP = auto()
    HAVING = auto()
    HINT = auto()
    IF = auto()
    IN = auto()
    INNER = auto()
    INSERT = auto()
    INTERVAL = auto()
    INTO = auto()
    IS = auto()
    JOIN = auto()
    LATERAL = auto()
    LEFT = auto()
    LIKE = auto()
    LIMIT = auto()
    MAP = auto()
    MOD = auto()
    NULL = auto()
    ON = auto()
    ORDER = auto()
    ORDERED = auto()
    ORDINALITY = auto()
    OUTER = auto()
    OVER = auto()
    OVERWRITE = auto()
    PARTITION = auto()
    PRECEDING = auto()
    PRIMARY_KEY = auto()
    RANGE = auto()
    RECURSIVE = auto()
    REPLACE = auto()
    RIGHT = auto()
    RLIKE = auto()
    ROWS = auto()
    SCHEMA_COMMENT = auto()
    SELECT = auto()
    SET = auto()
    STORED = auto()
    TEMPORARY = auto()
    TIME = auto()
    THEN = auto()
    UNBOUNDED = auto()
    UNION = auto()
    UNNEST = auto()
    UPDATE = auto()
    VALUES = auto()
    VIEW = auto()
    WHEN = auto()
    WHERE = auto()
    WITH = auto()
    WITHOUT = auto()
    ZONE = auto()


class Token:
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
        attributes = ", ".join(f"{k}: {v}" for k, v in self.__dict__.items())
        return f"<Token {attributes}>"


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
        "ALL": TokenType.ALL,
        "AND": TokenType.AND,
        "ASC": TokenType.ASC,
        "AS": TokenType.ALIAS,
        "AUTO_INCREMENT": TokenType.AUTO_INCREMENT,
        "BETWEEN": TokenType.BETWEEN,
        "BY": TokenType.BY,
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
        "DESC": TokenType.DESC,
        "DISTINCT": TokenType.DISTINCT,
        "DROP": TokenType.DROP,
        "ELSE": TokenType.ELSE,
        "END": TokenType.END,
        "ENGINE": TokenType.ENGINE,
        "EXISTS": TokenType.EXISTS,
        "EXTRACT": TokenType.EXTRACT,
        "FORMAT": TokenType.FORMAT,
        "FULL": TokenType.FULL,
        "FOLLOWING": TokenType.FOLLOWING,
        "FROM": TokenType.FROM,
        "GROUP BY": TokenType.GROUP,
        "HAVING": TokenType.HAVING,
        "IF": TokenType.IF,
        "IN": TokenType.IN,
        "INNER": TokenType.INNER,
        "INSERT": TokenType.INSERT,
        "INTERVAL": TokenType.INTERVAL,
        "INTO": TokenType.INTO,
        "IS": TokenType.IS,
        "JOIN": TokenType.JOIN,
        "LATERAL": TokenType.LATERAL,
        "LEFT": TokenType.LEFT,
        "LIKE": TokenType.LIKE,
        "LIMIT": TokenType.LIMIT,
        "NOT": TokenType.NOT,
        "NULL": TokenType.NULL,
        "ON": TokenType.ON,
        "OR": TokenType.OR,
        "ORDER BY": TokenType.ORDER,
        "ORDINALITY": TokenType.ORDINALITY,
        "OUTER": TokenType.OUTER,
        "OVER": TokenType.OVER,
        "OVERWRITE": TokenType.OVERWRITE,
        "PARTITION BY": TokenType.PARTITION,
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
        "STORED": TokenType.STORED,
        "TABLE": TokenType.TABLE,
        "TEMPORARY": TokenType.TEMPORARY,
        "THEN": TokenType.THEN,
        "TIME": TokenType.TIME,
        "UNBOUNDED": TokenType.UNBOUNDED,
        "UNION": TokenType.UNION,
        "UNNEST": TokenType.UNNEST,
        "UPDATE": TokenType.UPDATE,
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
        "TIMESTAMP": TokenType.TIMESTAMP,
        "TIMESTAMPTZ": TokenType.TIMESTAMPTZ,
        "DATE": TokenType.DATE,
    }

    WHITE_SPACE = {
        " ": TokenType.SPACE,
        "\t": TokenType.SPACE,
        "\n": TokenType.BREAK,
        "\r": TokenType.BREAK,
        "\rn": TokenType.BREAK,
    }

    ESCAPE_CODE = "__sqlglot_escape__"

    def __init__(self, **opts):
        self.quotes = set(opts.get("quotes") or "'")
        self.identifier = opts.get("identifier") or '"'
        self.escape = opts.get("escape") or "'"
        self.single_tokens = {**self.SINGLE_TOKENS, **opts.get("single_tokens", {})}
        self.keywords = {**self.KEYWORDS, **opts.get("keywords", {})}
        self.white_space = {**self.WHITE_SPACE, **opts.get("white_space", {})}
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
        self._text = None

    def tokenize(self, code):  # pylint: disable=too-many-branches
        self.reset()
        self.code = code
        self.size = len(code)

        ambiguous_trie = new_trie(
            *{
                key
                for key in self.keywords
                if self.keywords[key]
                not in (TokenType.COMMENT, TokenType.COMMENT_START)
                and (" " in key or any(single in key for single in self.single_tokens))
            }
        )

        comments = []
        comment_start = None
        comment_end = None

        for key, token in self.keywords.items():
            if token == TokenType.COMMENT:
                comments.append(key)
            elif token == TokenType.COMMENT_START:
                comment_start = key
            elif token == TokenType.COMMENT_END:
                comment_end = key

        while not self._end:
            self._start = self._current
            self._advance()

            if not self._char:
                break
            if self._scan_ambiguous(ambiguous_trie):
                pass
            elif self._scan_comments(comments, comment_start, comment_end):
                pass
            elif self._char in self.single_tokens:
                self._add(self.single_tokens[self._char])
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
        self._text = self.code[self._start : self._current]
        self._end = self._current >= self.size

    def _add(self, token_type, text=None):
        text = self._text if text is None else text
        self.tokens.append(Token(token_type, text, self._line, self._col))

    def _scan_ambiguous(self, ambiguous_trie):
        size = 1
        word = None
        chars = self._chars(size)

        while chars:
            result = in_trie(ambiguous_trie, chars)

            if result == 0:
                break
            if result == 2:
                word = chars
            size += 1
            chars = self._chars(size)

        if word:
            self._advance(len(word) - 1)
            self._add(self.keywords[word])
            return True
        return False

    def _scan_comments(self, comments, comment_start, comment_end):
        for comment in comments:
            if self._chars(len(comment)) == comment:
                while (
                    not self._end
                    and self.WHITE_SPACE.get(self._char) != TokenType.BREAK
                ):
                    self._advance()
                return True

        if self._chars(len(comment_start)) == comment_start:
            comment_end_size = len(comment_end)
            while not self._end and self._chars(comment_end_size) != comment_end:
                self._advance()
            self._advance(comment_end_size - 1)
            return True
        return False

    def _scan_number(self):
        decimal = False
        scientific = 0

        while True:
            if self._peek.isdigit():
                self._advance()
            elif self._peek == "." and not decimal:
                decimal = True
                self._advance()
            elif self._peek.upper() == "E" and not scientific:
                scientific += 1
                self._advance()
            elif self._peek == "-" and scientific == 1:
                scientific += 1
                self._advance()
            else:
                return self._add(TokenType.NUMBER)

    def _scan_string(self):
        text = []
        quote = self._char
        others = self.quotes.difference(quote)

        while True:
            if self._end:
                raise RuntimeError(f"Missing {quote} from {self._line}:{self._start}")
            text.append(self._char)
            self._advance()

            if self._char == self.escape and self._peek == quote:
                text.append(self.ESCAPE_CODE)
                self._advance()
            elif self._char in others:
                text.append(self.ESCAPE_CODE)
            elif self._char == quote:
                break

        text.append(self._char)
        self._add(TokenType.STRING, "".join(text[1:-1]))

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
            if char and char not in self.single_tokens:
                self._advance()
            else:
                break
        self._add(self.keywords.get(self._text.upper(), TokenType.VAR))
