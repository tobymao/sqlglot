import { describe, it, expect } from 'vitest';
import { Parser } from './parser';
import { Tokenizer } from './tokenizer';
import * as exp from './expressions';
import { ParseError } from './errors';

describe('Parser', () => {
  const tokenizer = new Tokenizer();
  const parser = new Parser();

  function parseSQL(sql: string): exp.Expression {
    const tokens = tokenizer.tokenize(sql);
    const result = parser.parse(tokens);
    return result[0];
  }

  describe('Basic SELECT Statements', () => {
    it('should parse simple SELECT *', () => {
      const ast = parseSQL('SELECT * FROM users');
      expect(ast).toBeInstanceOf(exp.Select);
      const select = ast as exp.Select;
      expect(select.expressions).toHaveLength(1);
      expect(select.expressions[0]).toBeInstanceOf(exp.Star);
      expect(select.from).toBeInstanceOf(exp.From);
    });

    it('should parse SELECT with column names', () => {
      const ast = parseSQL('SELECT name, age FROM users');
      expect(ast).toBeInstanceOf(exp.Select);
      const select = ast as exp.Select;
      expect(select.expressions).toHaveLength(2);
      expect(select.expressions[0]).toBeInstanceOf(exp.Column);
      expect(select.expressions[1]).toBeInstanceOf(exp.Column);
    });

    it('should parse SELECT with qualified columns', () => {
      const ast = parseSQL('SELECT users.name, users.age FROM users');
      const select = ast as exp.Select;
      expect(select.expressions).toHaveLength(2);
      const col = select.expressions[0] as exp.Column;
      expect(col.table).toBeDefined();
      expect(col.table?.name).toBe('users');
    });

    it('should parse SELECT DISTINCT', () => {
      const ast = parseSQL('SELECT DISTINCT name FROM users');
      const select = ast as exp.Select;
      expect(select.distinct).toBe(true);
    });
  });

  describe('WHERE Clause', () => {
    it('should parse WHERE with simple condition', () => {
      const ast = parseSQL('SELECT * FROM users WHERE id = 1');
      const select = ast as exp.Select;
      expect(select.where).toBeInstanceOf(exp.Where);
      expect(select.where?.expression).toBeInstanceOf(exp.EQ);
    });

    it('should parse WHERE with AND condition', () => {
      const ast = parseSQL('SELECT * FROM users WHERE age > 18 AND status = 1');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.And);
    });

    it('should parse WHERE with OR condition', () => {
      const ast = parseSQL('SELECT * FROM users WHERE age < 18 OR age > 65');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.Or);
    });

    it('should parse WHERE with NOT condition', () => {
      const ast = parseSQL('SELECT * FROM users WHERE NOT deleted');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.Not);
    });

    it('should parse WHERE with IS NULL', () => {
      const ast = parseSQL('SELECT * FROM users WHERE email IS NULL');
      const select = ast as exp.Select;
      expect(select.where).toBeDefined();
    });

    it('should parse WHERE with IS NOT NULL', () => {
      const ast = parseSQL('SELECT * FROM users WHERE email IS NOT NULL');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.Not);
    });
  });

  describe('Arithmetic Expressions', () => {
    it('should parse addition', () => {
      const ast = parseSQL('SELECT a + b FROM t');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Add);
    });

    it('should parse subtraction', () => {
      const ast = parseSQL('SELECT a - b FROM t');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Sub);
    });

    it('should parse multiplication', () => {
      const ast = parseSQL('SELECT a * b FROM t');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Mul);
    });

    it('should parse division', () => {
      const ast = parseSQL('SELECT a / b FROM t');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Div);
    });

    it('should parse modulo', () => {
      const ast = parseSQL('SELECT a % b FROM t');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Mod);
    });

    it('should parse complex arithmetic', () => {
      const ast = parseSQL('SELECT (a + b) * c / d FROM t');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Div);
    });

    it('should respect operator precedence', () => {
      const ast = parseSQL('SELECT a + b * c FROM t');
      const select = ast as exp.Select;
      const add = select.expressions[0] as exp.Add;
      expect(add).toBeInstanceOf(exp.Add);
      expect(add.right).toBeInstanceOf(exp.Mul);
    });
  });

  describe('Comparison Operators', () => {
    it('should parse equality', () => {
      const ast = parseSQL('SELECT * FROM t WHERE a = b');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.EQ);
    });

    it('should parse inequality', () => {
      const ast = parseSQL('SELECT * FROM t WHERE a <> b');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.NEQ);
    });

    it('should parse less than', () => {
      const ast = parseSQL('SELECT * FROM t WHERE a < b');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.LT);
    });

    it('should parse greater than', () => {
      const ast = parseSQL('SELECT * FROM t WHERE a > b');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.GT);
    });

    it('should parse less than or equal', () => {
      const ast = parseSQL('SELECT * FROM t WHERE a <= b');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.LTE);
    });

    it('should parse greater than or equal', () => {
      const ast = parseSQL('SELECT * FROM t WHERE a >= b');
      const select = ast as exp.Select;
      expect(select.where?.expression).toBeInstanceOf(exp.GTE);
    });
  });

  describe('Functions', () => {
    it('should parse function calls', () => {
      const ast = parseSQL('SELECT COUNT(*) FROM users');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.FunctionCall);
      const fn = select.expressions[0] as exp.FunctionCall;
      expect(fn.name.name).toBe('COUNT');
    });

    it('should parse function with arguments', () => {
      const ast = parseSQL('SELECT MAX(age) FROM users');
      const select = ast as exp.Select;
      const fn = select.expressions[0] as exp.FunctionCall;
      expect(fn.args).toHaveLength(1);
    });

    it('should parse function with multiple arguments', () => {
      const ast = parseSQL('SELECT CONCAT(first_name, last_name) FROM users');
      const select = ast as exp.Select;
      const fn = select.expressions[0] as exp.FunctionCall;
      expect(fn.args).toHaveLength(2);
    });

    it('should parse COUNT DISTINCT', () => {
      const ast = parseSQL('SELECT COUNT(DISTINCT user_id) FROM orders');
      const select = ast as exp.Select;
      const fn = select.expressions[0] as exp.FunctionCall;
      expect(fn.distinct).toBe(true);
    });
  });

  describe('Aliases', () => {
    it('should parse column alias with AS', () => {
      const ast = parseSQL('SELECT name AS user_name FROM users');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Alias);
      const alias = select.expressions[0] as exp.Alias;
      expect(alias.alias.name).toBe('user_name');
    });

    it('should parse column alias without AS', () => {
      const ast = parseSQL('SELECT COUNT(*) as count FROM users');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Alias);
    });

    it('should parse table alias', () => {
      const ast = parseSQL('SELECT u.name FROM users u');
      const select = ast as exp.Select;
      const from = select.from as exp.From;
      const table = from.expressions[0] as exp.Table;
      expect(table.alias?.name).toBe('u');
    });

    it('should parse table alias with AS', () => {
      const ast = parseSQL('SELECT u.name FROM users AS u');
      const select = ast as exp.Select;
      const from = select.from as exp.From;
      const table = from.expressions[0] as exp.Table;
      expect(table.alias?.name).toBe('u');
    });
  });

  describe('GROUP BY', () => {
    it('should parse GROUP BY with single column', () => {
      const ast = parseSQL('SELECT name, COUNT(*) FROM users GROUP BY name');
      const select = ast as exp.Select;
      expect(select.groupBy).toBeInstanceOf(exp.GroupBy);
      expect(select.groupBy?.expressions).toHaveLength(1);
    });

    it('should parse GROUP BY with multiple columns', () => {
      const ast = parseSQL('SELECT city, state, COUNT(*) FROM users GROUP BY city, state');
      const select = ast as exp.Select;
      expect(select.groupBy?.expressions).toHaveLength(2);
    });
  });

  describe('HAVING', () => {
    it('should parse HAVING clause', () => {
      const ast = parseSQL('SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 5');
      const select = ast as exp.Select;
      expect(select.having).toBeInstanceOf(exp.Having);
      expect(select.having?.expression).toBeInstanceOf(exp.GT);
    });
  });

  describe('ORDER BY', () => {
    it('should parse ORDER BY with single column', () => {
      const ast = parseSQL('SELECT * FROM users ORDER BY name');
      const select = ast as exp.Select;
      expect(select.orderBy).toBeInstanceOf(exp.OrderBy);
      expect(select.orderBy?.expressions).toHaveLength(1);
    });

    it('should parse ORDER BY with ASC', () => {
      const ast = parseSQL('SELECT * FROM users ORDER BY name ASC');
      const select = ast as exp.Select;
      const ordered = select.orderBy?.expressions[0] as exp.Ordered;
      expect(ordered.desc).toBe(false);
    });

    it('should parse ORDER BY with DESC', () => {
      const ast = parseSQL('SELECT * FROM users ORDER BY name DESC');
      const select = ast as exp.Select;
      const ordered = select.orderBy?.expressions[0] as exp.Ordered;
      expect(ordered.desc).toBe(true);
    });

    it('should parse ORDER BY with multiple columns', () => {
      const ast = parseSQL('SELECT * FROM users ORDER BY name ASC, age DESC');
      const select = ast as exp.Select;
      expect(select.orderBy?.expressions).toHaveLength(2);
    });
  });

  describe('LIMIT and OFFSET', () => {
    it('should parse LIMIT', () => {
      const ast = parseSQL('SELECT * FROM users LIMIT 10');
      const select = ast as exp.Select;
      expect(select.limit).toBeInstanceOf(exp.Limit);
    });

    it('should parse OFFSET', () => {
      const ast = parseSQL('SELECT * FROM users OFFSET 20');
      const select = ast as exp.Select;
      expect(select.offset).toBeInstanceOf(exp.Offset);
    });

    it('should parse LIMIT and OFFSET', () => {
      const ast = parseSQL('SELECT * FROM users LIMIT 10 OFFSET 20');
      const select = ast as exp.Select;
      expect(select.limit).toBeInstanceOf(exp.Limit);
      expect(select.offset).toBeInstanceOf(exp.Offset);
    });
  });

  describe('JOINs', () => {
    it('should parse INNER JOIN', () => {
      const ast = parseSQL('SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id');
      const select = ast as exp.Select;
      expect(select.from).toBeInstanceOf(exp.From);
    });

    it('should parse LEFT JOIN', () => {
      const ast = parseSQL('SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id');
      const select = ast as exp.Select;
      expect(select.from).toBeInstanceOf(exp.From);
    });

    it('should parse RIGHT JOIN', () => {
      const ast = parseSQL('SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id');
      const select = ast as exp.Select;
      expect(select.from).toBeInstanceOf(exp.From);
    });

    it('should parse JOIN with USING', () => {
      const ast = parseSQL('SELECT * FROM users JOIN orders USING (user_id)');
      const select = ast as exp.Select;
      expect(select.from).toBeInstanceOf(exp.From);
    });
  });

  describe('Subqueries', () => {
    it('should parse subquery in FROM', () => {
      const ast = parseSQL('SELECT * FROM (SELECT id FROM users) AS u');
      const select = ast as exp.Select;
      const from = select.from as exp.From;
      expect(from.expressions[0]).toBeInstanceOf(exp.Subquery);
    });

    it('should parse subquery with alias', () => {
      const ast = parseSQL('SELECT * FROM (SELECT id FROM users) u');
      const select = ast as exp.Select;
      const from = select.from as exp.From;
      const subquery = from.expressions[0] as exp.Subquery;
      expect(subquery.alias?.name).toBe('u');
    });

    it('should parse nested subqueries', () => {
      const ast = parseSQL('SELECT * FROM (SELECT * FROM (SELECT id FROM users) AS u1) AS u2');
      const select = ast as exp.Select;
      expect(select.from).toBeInstanceOf(exp.From);
    });
  });

  describe('CAST', () => {
    it('should parse CAST expression', () => {
      const ast = parseSQL('SELECT CAST(age AS VARCHAR) FROM users');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Cast);
    });

    it('should parse CAST with size', () => {
      const ast = parseSQL('SELECT CAST(name AS VARCHAR(100)) FROM users');
      const select = ast as exp.Select;
      const cast = select.expressions[0] as exp.Cast;
      expect(cast.dataType.size).toBe(100);
    });

    it('should parse CAST with decimal precision', () => {
      const ast = parseSQL('SELECT CAST(price AS DECIMAL(10, 2)) FROM products');
      const select = ast as exp.Select;
      const cast = select.expressions[0] as exp.Cast;
      expect(cast.dataType.size).toBe(10);
      expect(cast.dataType.scale).toBe(2);
    });
  });

  describe('Literals', () => {
    it('should parse string literals', () => {
      const ast = parseSQL("SELECT 'hello' FROM users");
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Literal);
      const literal = select.expressions[0] as exp.Literal;
      expect(literal.value).toBe('hello');
    });

    it('should parse numeric literals', () => {
      const ast = parseSQL('SELECT 123 FROM users');
      const select = ast as exp.Select;
      const literal = select.expressions[0] as exp.Literal;
      expect(literal.value).toBe(123);
    });

    it('should parse NULL', () => {
      const ast = parseSQL('SELECT NULL FROM users');
      const select = ast as exp.Select;
      const literal = select.expressions[0] as exp.Literal;
      expect(literal.value).toBe(null);
    });
  });

  describe('Parentheses', () => {
    it('should parse parenthesized expressions', () => {
      const ast = parseSQL('SELECT (a + b) FROM t');
      const select = ast as exp.Select;
      expect(select.expressions[0]).toBeInstanceOf(exp.Paren);
    });

    it('should respect parentheses for precedence', () => {
      const ast = parseSQL('SELECT (a + b) * c FROM t');
      const select = ast as exp.Select;
      const mul = select.expressions[0] as exp.Mul;
      expect(mul.left).toBeInstanceOf(exp.Paren);
    });
  });

  describe('Complex Queries', () => {
    it('should parse complex query with all clauses', () => {
      const sql = `
        SELECT u.name, COUNT(*) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.age > 18
        GROUP BY u.name
        HAVING COUNT(*) > 5
        ORDER BY order_count DESC
        LIMIT 10
      `;
      const ast = parseSQL(sql);
      const select = ast as exp.Select;
      expect(select.from).toBeDefined();
      expect(select.where).toBeDefined();
      expect(select.groupBy).toBeDefined();
      expect(select.having).toBeDefined();
      expect(select.orderBy).toBeDefined();
      expect(select.limit).toBeDefined();
    });
  });

  describe('Error Handling', () => {
    it('should throw error for missing FROM in SELECT', () => {
      expect(() => parseSQL('SELECT')).toThrow();
    });

    it('should throw error for incomplete WHERE', () => {
      expect(() => parseSQL('SELECT * FROM users WHERE')).toThrow();
    });

    it('should throw error for incomplete expression', () => {
      expect(() => parseSQL('SELECT a + FROM users')).toThrow();
    });
  });
});
