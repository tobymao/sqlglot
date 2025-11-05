import { describe, it, expect } from 'vitest';
import { Tokenizer } from './tokenizer';
import { TokenType } from './tokens';
import { TokenError } from './errors';

describe('Tokenizer', () => {
  const tokenizer = new Tokenizer();

  describe('Basic Tokens', () => {
    it('should tokenize simple SELECT statement', () => {
      const tokens = tokenizer.tokenize('SELECT * FROM users');
      expect(tokens).toHaveLength(5); // SELECT, *, FROM, users, EOF
      expect(tokens[0].type).toBe(TokenType.SELECT);
      expect(tokens[1].type).toBe(TokenType.STAR);
      expect(tokens[2].type).toBe(TokenType.FROM);
      expect(tokens[3].type).toBe(TokenType.IDENTIFIER);
      expect(tokens[3].text).toBe('users');
      expect(tokens[4].type).toBe(TokenType.EOF);
    });

    it('should tokenize keywords case-insensitively', () => {
      const tokens = tokenizer.tokenize('select FROM where');
      expect(tokens[0].type).toBe(TokenType.SELECT);
      expect(tokens[1].type).toBe(TokenType.FROM);
      expect(tokens[2].type).toBe(TokenType.WHERE);
    });

    it('should tokenize identifiers', () => {
      const tokens = tokenizer.tokenize('table_name column1 _underscore');
      expect(tokens[0].type).toBe(TokenType.IDENTIFIER);
      expect(tokens[0].text).toBe('table_name');
      expect(tokens[1].type).toBe(TokenType.IDENTIFIER);
      expect(tokens[1].text).toBe('column1');
      expect(tokens[2].type).toBe(TokenType.IDENTIFIER);
      expect(tokens[2].text).toBe('_underscore');
    });
  });

  describe('Operators', () => {
    it('should tokenize comparison operators', () => {
      const tokens = tokenizer.tokenize('= <> < > <= >=');
      expect(tokens[0].type).toBe(TokenType.EQ);
      expect(tokens[1].type).toBe(TokenType.NEQ);
      expect(tokens[2].type).toBe(TokenType.LT);
      expect(tokens[3].type).toBe(TokenType.GT);
      expect(tokens[4].type).toBe(TokenType.LTE);
      expect(tokens[5].type).toBe(TokenType.GTE);
    });

    it('should tokenize arithmetic operators', () => {
      const tokens = tokenizer.tokenize('+ - * / %');
      expect(tokens[0].type).toBe(TokenType.PLUS);
      expect(tokens[1].type).toBe(TokenType.DASH);
      expect(tokens[2].type).toBe(TokenType.STAR);
      expect(tokens[3].type).toBe(TokenType.SLASH);
      expect(tokens[4].type).toBe(TokenType.PERCENT);
    });

    it('should tokenize logical operators', () => {
      const tokens = tokenizer.tokenize('AND OR NOT');
      expect(tokens[0].type).toBe(TokenType.AND);
      expect(tokens[1].type).toBe(TokenType.OR);
      expect(tokens[2].type).toBe(TokenType.NOT);
    });

    it('should tokenize compound operators', () => {
      const tokens = tokenizer.tokenize('|| && ::');
      expect(tokens[0].type).toBe(TokenType.DPIPE);
      expect(tokens[1].type).toBe(TokenType.DAMP);
      expect(tokens[2].type).toBe(TokenType.DCOLON);
    });
  });

  describe('Literals', () => {
    it('should tokenize integer numbers', () => {
      const tokens = tokenizer.tokenize('123 456 0');
      expect(tokens[0].type).toBe(TokenType.NUMBER);
      expect(tokens[0].text).toBe('123');
      expect(tokens[1].type).toBe(TokenType.NUMBER);
      expect(tokens[1].text).toBe('456');
      expect(tokens[2].type).toBe(TokenType.NUMBER);
      expect(tokens[2].text).toBe('0');
    });

    it('should tokenize decimal numbers', () => {
      const tokens = tokenizer.tokenize('3.14 0.5 100.0');
      expect(tokens[0].type).toBe(TokenType.NUMBER);
      expect(tokens[0].text).toBe('3.14');
      expect(tokens[1].type).toBe(TokenType.NUMBER);
      expect(tokens[1].text).toBe('0.5');
      expect(tokens[2].type).toBe(TokenType.NUMBER);
      expect(tokens[2].text).toBe('100.0');
    });

    it('should tokenize scientific notation', () => {
      const tokens = tokenizer.tokenize('1e10 2.5e-3 3E+5');
      expect(tokens[0].type).toBe(TokenType.NUMBER);
      expect(tokens[0].text).toBe('1e10');
      expect(tokens[1].type).toBe(TokenType.NUMBER);
      expect(tokens[1].text).toBe('2.5e-3');
      expect(tokens[2].type).toBe(TokenType.NUMBER);
      expect(tokens[2].text).toBe('3E+5');
    });

    it('should tokenize single-quoted strings', () => {
      const tokens = tokenizer.tokenize("'hello' 'world'");
      expect(tokens[0].type).toBe(TokenType.STRING);
      expect(tokens[0].text).toBe('hello');
      expect(tokens[1].type).toBe(TokenType.STRING);
      expect(tokens[1].text).toBe('world');
    });

    it('should tokenize double-quoted strings', () => {
      const tokens = tokenizer.tokenize('"hello" "world"');
      expect(tokens[0].type).toBe(TokenType.STRING);
      expect(tokens[0].text).toBe('hello');
      expect(tokens[1].type).toBe(TokenType.STRING);
      expect(tokens[1].text).toBe('world');
    });

    it('should handle escaped quotes in strings', () => {
      const tokens = tokenizer.tokenize("'it\\'s' \"quote\\\"\"");
      expect(tokens[0].type).toBe(TokenType.STRING);
      expect(tokens[0].text).toBe("it\\'s");
      expect(tokens[1].type).toBe(TokenType.STRING);
      expect(tokens[1].text).toBe('quote\\"');
    });

    it('should tokenize backtick-quoted identifiers', () => {
      const tokens = tokenizer.tokenize('`table name` `column-1`');
      expect(tokens[0].type).toBe(TokenType.IDENTIFIER);
      expect(tokens[0].text).toBe('table name');
      expect(tokens[1].type).toBe(TokenType.IDENTIFIER);
      expect(tokens[1].text).toBe('column-1');
    });
  });

  describe('Punctuation', () => {
    it('should tokenize parentheses', () => {
      const tokens = tokenizer.tokenize('( )');
      expect(tokens[0].type).toBe(TokenType.L_PAREN);
      expect(tokens[1].type).toBe(TokenType.R_PAREN);
    });

    it('should tokenize brackets', () => {
      const tokens = tokenizer.tokenize('[ ]');
      expect(tokens[0].type).toBe(TokenType.L_BRACKET);
      expect(tokens[1].type).toBe(TokenType.R_BRACKET);
    });

    it('should tokenize braces', () => {
      const tokens = tokenizer.tokenize('{ }');
      expect(tokens[0].type).toBe(TokenType.L_BRACE);
      expect(tokens[1].type).toBe(TokenType.R_BRACE);
    });

    it('should tokenize comma, dot, semicolon', () => {
      const tokens = tokenizer.tokenize(', . ;');
      expect(tokens[0].type).toBe(TokenType.COMMA);
      expect(tokens[1].type).toBe(TokenType.DOT);
      expect(tokens[2].type).toBe(TokenType.SEMICOLON);
    });
  });

  describe('Comments', () => {
    it('should handle line comments with --', () => {
      const tokens = tokenizer.tokenize('SELECT * -- this is a comment\nFROM users');
      expect(tokens[0].type).toBe(TokenType.SELECT);
      expect(tokens[1].type).toBe(TokenType.STAR);
      expect(tokens[2].type).toBe(TokenType.FROM);
      expect(tokens[3].type).toBe(TokenType.IDENTIFIER);
    });

    it('should handle line comments with #', () => {
      const tokens = tokenizer.tokenize('SELECT * # comment\nFROM users');
      expect(tokens[0].type).toBe(TokenType.SELECT);
      expect(tokens[1].type).toBe(TokenType.STAR);
      expect(tokens[2].type).toBe(TokenType.FROM);
    });

    it('should handle block comments', () => {
      const tokens = tokenizer.tokenize('SELECT /* comment */ * FROM users');
      expect(tokens[0].type).toBe(TokenType.SELECT);
      expect(tokens[1].type).toBe(TokenType.STAR);
      expect(tokens[2].type).toBe(TokenType.FROM);
    });

    it('should handle multi-line block comments', () => {
      const tokens = tokenizer.tokenize(`SELECT /* multi
        line
        comment */ * FROM users`);
      expect(tokens[0].type).toBe(TokenType.SELECT);
      expect(tokens[1].type).toBe(TokenType.STAR);
      expect(tokens[2].type).toBe(TokenType.FROM);
    });
  });

  describe('Line and Column Tracking', () => {
    it('should track line numbers', () => {
      const tokens = tokenizer.tokenize('SELECT\n*\nFROM\nusers');
      expect(tokens[0].line).toBe(1);
      expect(tokens[1].line).toBe(2);
      expect(tokens[2].line).toBe(3);
      expect(tokens[3].line).toBe(4);
    });

    it('should track column numbers', () => {
      const tokens = tokenizer.tokenize('SELECT * FROM');
      expect(tokens[0].col).toBeGreaterThan(0);
      expect(tokens[1].col).toBeGreaterThan(tokens[0].col);
      expect(tokens[2].col).toBeGreaterThan(tokens[1].col);
    });
  });

  describe('Error Handling', () => {
    it('should throw error for unterminated string', () => {
      expect(() => tokenizer.tokenize("'unterminated")).toThrow(TokenError);
    });

    it('should throw error for unterminated identifier', () => {
      expect(() => tokenizer.tokenize('`unterminated')).toThrow(TokenError);
    });

    it('should throw error for unexpected character', () => {
      expect(() => tokenizer.tokenize('SELECT @ FROM users')).toThrow(TokenError);
    });
  });

  describe('Complex Queries', () => {
    it('should tokenize complex SELECT statement', () => {
      const sql = 'SELECT u.name, COUNT(*) as count FROM users u WHERE u.age > 18';
      const tokens = tokenizer.tokenize(sql);

      expect(tokens[0].type).toBe(TokenType.SELECT);
      expect(tokens[1].type).toBe(TokenType.IDENTIFIER); // u
      expect(tokens[2].type).toBe(TokenType.DOT);
      expect(tokens[3].type).toBe(TokenType.IDENTIFIER); // name
      expect(tokens[4].type).toBe(TokenType.COMMA);
      expect(tokens[5].type).toBe(TokenType.IDENTIFIER); // COUNT
    });

    it('should tokenize JOIN statement', () => {
      const sql = 'SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id';
      const tokens = tokenizer.tokenize(sql);

      const types = tokens.map(t => t.type);
      expect(types).toContain(TokenType.LEFT);
      expect(types).toContain(TokenType.JOIN);
      expect(types).toContain(TokenType.ON);
    });
  });

  describe('Whitespace Handling', () => {
    it('should handle multiple spaces', () => {
      const tokens = tokenizer.tokenize('SELECT    *    FROM    users');
      expect(tokens).toHaveLength(5);
      expect(tokens[0].type).toBe(TokenType.SELECT);
      expect(tokens[4].type).toBe(TokenType.EOF);
    });

    it('should handle tabs and newlines', () => {
      const tokens = tokenizer.tokenize('SELECT\t*\nFROM\r\nusers');
      expect(tokens).toHaveLength(5);
      expect(tokens[0].type).toBe(TokenType.SELECT);
    });

    it('should handle mixed whitespace', () => {
      const tokens = tokenizer.tokenize('  \t\n  SELECT  \t  *  \n\n  FROM  \t\n  users  ');
      expect(tokens).toHaveLength(5);
    });
  });
});
