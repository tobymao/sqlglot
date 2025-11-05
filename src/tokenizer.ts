import { Token, TokenType } from './tokens';
import { TokenError } from './errors';

export class Tokenizer {
  private sql: string = '';
  private size: number = 0;
  private tokens: Token[] = [];
  private start: number = 0;
  private current: number = 0;
  private line: number = 1;
  private col: number = 1;
  private comments: string[] = [];

  private readonly keywords: Map<string, TokenType> = new Map([
    ['SELECT', TokenType.SELECT],
    ['FROM', TokenType.FROM],
    ['WHERE', TokenType.WHERE],
    ['AS', TokenType.AS],
    ['JOIN', TokenType.JOIN],
    ['LEFT', TokenType.LEFT],
    ['RIGHT', TokenType.RIGHT],
    ['INNER', TokenType.INNER],
    ['OUTER', TokenType.OUTER],
    ['ON', TokenType.ON],
    ['USING', TokenType.USING],
    ['GROUP', TokenType.GROUP],
    ['BY', TokenType.BY],
    ['HAVING', TokenType.HAVING],
    ['ORDER', TokenType.ORDER],
    ['ASC', TokenType.ASC],
    ['DESC', TokenType.DESC],
    ['LIMIT', TokenType.LIMIT],
    ['OFFSET', TokenType.OFFSET],
    ['UNION', TokenType.UNION],
    ['INTERSECT', TokenType.INTERSECT],
    ['EXCEPT', TokenType.EXCEPT],
    ['ALL', TokenType.ALL],
    ['DISTINCT', TokenType.DISTINCT],
    ['CASE', TokenType.CASE],
    ['WHEN', TokenType.WHEN],
    ['THEN', TokenType.THEN],
    ['ELSE', TokenType.ELSE],
    ['END', TokenType.END],
    ['CAST', TokenType.CAST],
    ['NULL', TokenType.NULL],
    ['IS', TokenType.IS],
    ['IN', TokenType.IN],
    ['BETWEEN', TokenType.BETWEEN],
    ['LIKE', TokenType.LIKE],
    ['EXISTS', TokenType.EXISTS],
    ['AND', TokenType.AND],
    ['OR', TokenType.OR],
    ['NOT', TokenType.NOT],
    ['XOR', TokenType.XOR],
    ['CREATE', TokenType.CREATE],
    ['TABLE', TokenType.TABLE],
    ['VIEW', TokenType.VIEW],
    ['INSERT', TokenType.INSERT],
    ['INTO', TokenType.INTO],
    ['VALUES', TokenType.VALUES],
    ['UPDATE', TokenType.UPDATE],
    ['SET', TokenType.SET],
    ['DELETE', TokenType.DELETE],
    ['DROP', TokenType.DROP],
    ['ALTER', TokenType.ALTER],
    ['ADD', TokenType.ADD],
    ['COLUMN', TokenType.COLUMN_KW],
    ['PRIMARY', TokenType.PRIMARY],
    ['KEY', TokenType.KEY],
    ['FOREIGN', TokenType.FOREIGN],
    ['REFERENCES', TokenType.REFERENCES],
    ['INDEX', TokenType.INDEX],
    ['UNIQUE', TokenType.UNIQUE],
    ['DEFAULT', TokenType.DEFAULT],
    ['CHECK', TokenType.CHECK],
    ['CONSTRAINT', TokenType.CONSTRAINT],
    ['INT', TokenType.INT],
    ['INTEGER', TokenType.INTEGER],
    ['BIGINT', TokenType.BIGINT],
    ['SMALLINT', TokenType.SMALLINT],
    ['TINYINT', TokenType.TINYINT],
    ['FLOAT', TokenType.FLOAT],
    ['DOUBLE', TokenType.DOUBLE],
    ['DECIMAL', TokenType.DECIMAL],
    ['NUMERIC', TokenType.NUMERIC],
    ['BOOLEAN', TokenType.BOOLEAN],
    ['BOOL', TokenType.BOOL],
    ['DATE', TokenType.DATE],
    ['DATETIME', TokenType.DATETIME],
    ['TIMESTAMP', TokenType.TIMESTAMP],
    ['TIME', TokenType.TIME],
    ['VARCHAR', TokenType.VARCHAR],
    ['CHAR', TokenType.CHAR],
    ['TEXT', TokenType.TEXT],
  ]);

  tokenize(sql: string): Token[] {
    this.sql = sql;
    this.size = sql.length;
    this.tokens = [];
    this.start = 0;
    this.current = 0;
    this.line = 1;
    this.col = 1;
    this.comments = [];

    while (!this.isAtEnd()) {
      this.start = this.current;
      this.scanToken();
    }

    this.addToken(TokenType.EOF, '');
    return this.tokens;
  }

  private isAtEnd(): boolean {
    return this.current >= this.size;
  }

  private scanToken(): void {
    const c = this.advance();

    switch (c) {
      case ' ':
      case '\r':
      case '\t':
        // Skip whitespace
        break;
      case '\n':
        this.line++;
        this.col = 1;
        break;
      case '(':
        this.addToken(TokenType.L_PAREN, c);
        break;
      case ')':
        this.addToken(TokenType.R_PAREN, c);
        break;
      case '[':
        this.addToken(TokenType.L_BRACKET, c);
        break;
      case ']':
        this.addToken(TokenType.R_BRACKET, c);
        break;
      case '{':
        this.addToken(TokenType.L_BRACE, c);
        break;
      case '}':
        this.addToken(TokenType.R_BRACE, c);
        break;
      case ',':
        this.addToken(TokenType.COMMA, c);
        break;
      case '.':
        this.addToken(TokenType.DOT, c);
        break;
      case ';':
        this.addToken(TokenType.SEMICOLON, c);
        break;
      case ':':
        if (this.match(':')) {
          this.addToken(TokenType.DCOLON, '::');
        } else {
          this.addToken(TokenType.COLON, c);
        }
        break;
      case '+':
        this.addToken(TokenType.PLUS, c);
        break;
      case '-':
        if (this.match('-')) {
          // Line comment
          this.lineComment();
        } else {
          this.addToken(TokenType.DASH, c);
        }
        break;
      case '*':
        this.addToken(TokenType.STAR, c);
        break;
      case '/':
        if (this.match('*')) {
          // Block comment
          this.blockComment();
        } else {
          this.addToken(TokenType.SLASH, c);
        }
        break;
      case '\\':
        this.addToken(TokenType.BACKSLASH, c);
        break;
      case '%':
        this.addToken(TokenType.PERCENT, c);
        break;
      case '&':
        if (this.match('&')) {
          this.addToken(TokenType.DAMP, '&&');
        } else {
          this.addToken(TokenType.AMP, c);
        }
        break;
      case '|':
        if (this.match('|')) {
          this.addToken(TokenType.DPIPE, '||');
        } else {
          this.addToken(TokenType.PIPE, c);
        }
        break;
      case '^':
        this.addToken(TokenType.CARET, c);
        break;
      case '~':
        this.addToken(TokenType.TILDA, c);
        break;
      case '#':
        this.lineComment();
        break;
      case '<':
        if (this.match('=')) {
          this.addToken(TokenType.LTE, '<=');
        } else if (this.match('>')) {
          this.addToken(TokenType.NEQ, '<>');
        } else if (this.match('-')) {
          this.addToken(TokenType.ARROW, '<-');
        } else {
          this.addToken(TokenType.LT, c);
        }
        break;
      case '>':
        if (this.match('=')) {
          this.addToken(TokenType.GTE, '>=');
        } else {
          this.addToken(TokenType.GT, c);
        }
        break;
      case '=':
        if (this.match('=')) {
          this.addToken(TokenType.EQ, '==');
        } else if (this.match('>')) {
          this.addToken(TokenType.DARROW, '=>');
        } else {
          this.addToken(TokenType.EQ, c);
        }
        break;
      case '!':
        if (this.match('=')) {
          this.addToken(TokenType.NEQ, '!=');
        } else {
          this.addToken(TokenType.NOT, c);
        }
        break;
      case "'":
      case '"':
        this.string(c);
        break;
      case '`':
        this.quotedIdentifier(c);
        break;
      default:
        if (this.isDigit(c)) {
          this.number();
        } else if (this.isAlpha(c)) {
          this.identifier();
        } else {
          throw new TokenError(
            `Unexpected character '${c}' at line ${this.line}, col ${this.col}`
          );
        }
        break;
    }
  }

  private advance(): string {
    this.col++;
    return this.sql.charAt(this.current++);
  }

  private match(expected: string): boolean {
    if (this.isAtEnd()) return false;
    if (this.sql.charAt(this.current) !== expected) return false;
    this.current++;
    this.col++;
    return true;
  }

  private peek(): string {
    if (this.isAtEnd()) return '\0';
    return this.sql.charAt(this.current);
  }

  private peekNext(): string {
    if (this.current + 1 >= this.size) return '\0';
    return this.sql.charAt(this.current + 1);
  }

  private isDigit(c: string): boolean {
    return c >= '0' && c <= '9';
  }

  private isAlpha(c: string): boolean {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c === '_';
  }

  private isAlphaNumeric(c: string): boolean {
    return this.isAlpha(c) || this.isDigit(c);
  }

  private string(quote: string): void {
    const startLine = this.line;
    const startCol = this.col;

    while (this.peek() !== quote && !this.isAtEnd()) {
      if (this.peek() === '\n') {
        this.line++;
        this.col = 0;
      }
      if (this.peek() === '\\' && this.peekNext() === quote) {
        // Backslash-escaped quote
        this.advance();
        this.advance();
      } else {
        this.advance();
      }
    }

    if (this.isAtEnd()) {
      throw new TokenError(
        `Unterminated string starting at line ${startLine}, col ${startCol}`
      );
    }

    // Closing quote
    this.advance();

    // Check for doubled quote (SQL standard escape)
    if (this.peek() === quote) {
      // This is an escaped quote, continue parsing the string
      this.advance();
      this.string(quote);
      return;
    }

    const value = this.sql.substring(this.start + 1, this.current - 1);
    this.addToken(TokenType.STRING, value);
  }

  private quotedIdentifier(quote: string): void {
    while (this.peek() !== quote && !this.isAtEnd()) {
      if (this.peek() === '\n') {
        this.line++;
        this.col = 0;
      }
      this.advance();
    }

    if (this.isAtEnd()) {
      throw new TokenError(`Unterminated identifier at line ${this.line}`);
    }

    // Closing quote
    this.advance();

    const value = this.sql.substring(this.start + 1, this.current - 1);
    this.addToken(TokenType.IDENTIFIER, value);
  }

  private number(): void {
    while (this.isDigit(this.peek())) {
      this.advance();
    }

    // Look for decimal part
    if (this.peek() === '.' && this.isDigit(this.peekNext())) {
      // Consume the "."
      this.advance();

      while (this.isDigit(this.peek())) {
        this.advance();
      }
    }

    // Look for scientific notation
    if (this.peek() === 'e' || this.peek() === 'E') {
      this.advance();
      if (this.peek() === '+' || this.peek() === '-') {
        this.advance();
      }
      while (this.isDigit(this.peek())) {
        this.advance();
      }
    }

    const value = this.sql.substring(this.start, this.current);
    this.addToken(TokenType.NUMBER, value);
  }

  private identifier(): void {
    while (this.isAlphaNumeric(this.peek())) {
      this.advance();
    }

    const text = this.sql.substring(this.start, this.current);
    const upperText = text.toUpperCase();
    const type = this.keywords.get(upperText) || TokenType.IDENTIFIER;
    this.addToken(type, text);
  }

  private lineComment(): void {
    const start = this.current - 1;
    while (this.peek() !== '\n' && !this.isAtEnd()) {
      this.advance();
    }
    const comment = this.sql.substring(start, this.current);
    this.comments.push(comment);
  }

  private blockComment(): void {
    const start = this.current - 1;
    while (!this.isAtEnd()) {
      if (this.peek() === '*' && this.peekNext() === '/') {
        this.advance();
        this.advance();
        break;
      }
      if (this.peek() === '\n') {
        this.line++;
        this.col = 0;
      }
      this.advance();
    }
    const comment = this.sql.substring(start, this.current);
    this.comments.push(comment);
  }

  private addToken(type: TokenType, text: string): void {
    const token = new Token(
      type,
      text,
      this.line,
      this.col - text.length,
      this.start,
      this.current,
      [...this.comments]
    );
    this.tokens.push(token);
    this.comments = [];
  }
}
