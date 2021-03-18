from enum import auto

from sqlglot.helper import AutoName


class TokenType(AutoName):
    PAREN = auto()
    L_PAREN = auto()
    R_PAREN = auto()
    BRACKET = auto()
    L_BRACKET = auto()
    R_BRACKET = auto()
    L_BRACE = auto()
    R_BRACE = auto()
    COMMA = auto()
    DOT = auto()
    DASH = auto()
    PLUS = auto()
    COLON = auto()
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

    SPACE = auto()
    BREAK = auto()

    STRING = auto()
    NUMBER = auto()
    IDENTIFIER = auto()
    COLUMN = auto()
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

    # keywords
    ALIAS = auto()
    ALL = auto()
    ARRAY = auto()
    ASC = auto()
    BETWEEN = auto()
    BY = auto()
    CASE = auto()
    CAST = auto()
    COUNT = auto()
    CROSS = auto()
    DESC = auto()
    DISTINCT = auto()
    ELSE = auto()
    END = auto()
    FULL = auto()
    FUNC = auto()
    FROM = auto()
    GROUP = auto()
    HAVING = auto()
    IN = auto()
    INNER = auto()
    IS = auto()
    JOIN = auto()
    LEFT = auto()
    MAP = auto()
    NULL = auto()
    ON = auto()
    ORDER = auto()
    OUTER = auto()
    OVER = auto()
    PARTITION = auto()
    SELECT = auto()
    RIGHT = auto()
    THEN = auto()
    UNION = auto()
    WHEN = auto()
    WHERE = auto()
    WITH = auto()


class Token:
    def __init__(self, token_type, text, line, col):
        self.token_type = token_type
        self.text = text
        self.line = line
        self.col = col - len(text)

    def __repr__(self):
        attributes = ", ".join([f"{k}: {v}" for k, v in self.__dict__.items()])
        return f"<Token {attributes}>"

    def to_s(self, _level=0):
        return self.text

class Tokenizer:
    SINGLE_TOKENS = {
        '(': TokenType.L_PAREN,
        ')': TokenType.R_PAREN,
        '[': TokenType.L_BRACKET,
        ']': TokenType.R_BRACKET,
        '{': TokenType.L_BRACE,
        '}': TokenType.R_BRACE,
        ',': TokenType.COMMA,
        '.': TokenType.DOT,
        '-': TokenType.DASH,
        '+': TokenType.PLUS,
        ':': TokenType.COLON,
        ';': TokenType.SEMICOLON,
        '*': TokenType.STAR,
        '/': TokenType.SLASH,
        '=': TokenType.EQ,
        '!': TokenType.NOT,
    }

    KEYWORDS = {
        'ALL': TokenType.ALL,
        'AND': TokenType.AND,
        'ASC': TokenType.ASC,
        'AS': TokenType.ALIAS,
        'BETWEEN': TokenType.BETWEEN,
        'BY': TokenType.BY,
        'CASE': TokenType.CASE,
        'CAST': TokenType.CAST,
        'COUNT': TokenType.COUNT,
        'CROSS': TokenType.CROSS,
        'DESC': TokenType.DESC,
        'DISTINCT': TokenType.DISTINCT,
        'ELSE': TokenType.ELSE,
        'END': TokenType.END,
        'FULL': TokenType.FULL,
        'FROM': TokenType.FROM,
        'GROUP': TokenType.GROUP,
        'HAVING': TokenType.HAVING,
        'IN': TokenType.IN,
        'INNER': TokenType.INNER,
        'IS': TokenType.IS,
        'JOIN': TokenType.JOIN,
        'LEFT': TokenType.LEFT,
        'NOT': TokenType.NOT,
        'NULL': TokenType.NULL,
        'ON': TokenType.ON,
        'OR': TokenType.OR,
        'ORDER': TokenType.ORDER,
        'OUTER': TokenType.OUTER,
        'OVER': TokenType.OVER,
        'PARTITION': TokenType.PARTITION,
        'RIGHT': TokenType.RIGHT,
        'SELECT': TokenType.SELECT,
        'THEN': TokenType.THEN,
        'UNION': TokenType.UNION,
        'WHEN': TokenType.WHEN,
        'WHERE': TokenType.WHERE,
        'WITH': TokenType.WITH,

        'ARRAY': TokenType.ARRAY,
        'BOOL': TokenType.BOOLEAN,
        'BOOLEAN': TokenType.BOOLEAN,
        'TINYINT': TokenType.TINYINT,
        'SMALLINT': TokenType.SMALLINT,
        'INT2': TokenType.SMALLINT,
        'INTEGER': TokenType.INT,
        'INT': TokenType.INT,
        'INT4': TokenType.INT,
        'BIGINT': TokenType.BIGINT,
        'INT8': TokenType.BIGINT,
        'DECIMAL': TokenType.DECIMAL,
        'MAP': TokenType.MAP,
        'NUMERIC': TokenType.DECIMAL,
        'DEC': TokenType.DECIMAL,
        'FIXED': TokenType.DECIMAL,
        'REAL': TokenType.FLOAT,
        'FLOAT': TokenType.FLOAT,
        'FLOAT4': TokenType.FLOAT,
        'FLOAT8': TokenType.DOUBLE,
        'DOUBLE': TokenType.DOUBLE,
        'JSON': TokenType.JSON,
        'CHAR': TokenType.CHAR,
        'VARCHAR': TokenType.VARCHAR,
        'TEXT': TokenType.TEXT,
        'BINARY': TokenType.BINARY,
    }

    WHITE_SPACE = {
        ' ': TokenType.SPACE,
        "\t": TokenType.SPACE,
        "\n": TokenType.BREAK,
        "\r": TokenType.BREAK,
        "\rn": TokenType.BREAK,
    }

    def __init__(self, **opts):
        self.quote = opts.get('quote') or "'"
        self.identifier = opts.get('identifier') or '"'
        self.single_tokens = {**self.SINGLE_TOKENS, **opts.get('single_tokens', {})}
        self.keywords = {**self.KEYWORDS, **opts.get('keywords', {})}
        self.white_space = {**self.WHITE_SPACE, **opts.get('white_space', {})}
        self.reset()

    def reset(self):
        self.code = ''
        self.tokens = []
        self._start = 0
        self._current = 0
        self._line = 0
        self._col = 0

    def tokenize(self, code): # pylint: disable=too-many-branches
        self.reset()
        self.code = code

        while self._current < len(self.code):
            self._start = self._current
            self._advance()

            if self._char == '<':
                if self._peek == '=':
                    self._advance()
                    self._add(TokenType.LTE)
                elif self._peek == '>':
                    self._advance()
                    self._add(TokenType.NEQ)
                else:
                    self._add(TokenType.LT)
            elif self._char == '>':
                if self._peek == '=':
                    self._advance()
                    self._add(TokenType.GTE)
                else:
                    self._add(TokenType.GT)
            elif self._char == '!':
                if self._peek == '=':
                    self._advance()
                    self._add(TokenType.NEQ)
                else:
                    self._add(TokenType.NOT)
            elif self._char in self.single_tokens:
                self._add(self.single_tokens[self._char])
            elif self._char in self.WHITE_SPACE:
                white_space = self.WHITE_SPACE[self._char]
                if white_space == TokenType.BREAK:
                    self._col = 0
                    self._line += 1
            elif self._char.isdigit():
                self._scan_number()
            elif self._char == self.quote:
                self._scan_string()
            elif self._char == self.identifier:
                self._scan_identifier()
            else:
                self._scan_var()

        return self.tokens

    @property
    def _char(self):
        return self.code[self._current - 1]

    @property
    def _text(self):
        return self.code[self._start:self._current]

    @property
    def _peek(self):
        if not self._end:
            return self.code[self._current]
        return ''

    @property
    def _end(self):
        return self._current >= len(self.code)

    def _advance(self):
        self._col += 1
        self._current += 1

    def _add(self, token_type, text=None):
        self.tokens.append(Token(token_type, text or self._text, self._line, self._col))

    def _scan_number(self):
        while self._peek.isdigit() or self._peek == '.':
            self._advance()
        self._add(TokenType.NUMBER)

    def _scan_string(self):
        while self._peek != self.quote:
            if self._end:
                raise RuntimeError(f"Missing {self.quote} from {self._line}:{self._start}")
            self._advance()
        self._advance()
        self._add(TokenType.STRING)

    def _scan_identifier(self):
        while self._peek != self.identifier:
            if self._end:
                raise RuntimeError(f"Missing {self.identifier} from {self._line}:{self._start}")
            self._advance()
        self._advance()
        self._add(TokenType.IDENTIFIER)

    def _scan_var(self):
        while True:
            char = self._peek.strip()
            if char and char not in self.single_tokens:
                self._advance()
            else:
                break
        self._add(self.keywords.get(self._text.upper(), TokenType.VAR))
