import { describe, it, expect } from 'vitest';
import * as exp from './expressions';
import { parseOne } from './index';

describe('Expressions', () => {
  describe('SQL Generation', () => {
    it('should generate SQL for Identifier', () => {
      const id = new exp.Identifier('user_name', false);
      expect(id.sql()).toBe('user_name');
    });

    it('should generate SQL for quoted Identifier', () => {
      const id = new exp.Identifier('user name', true);
      expect(id.sql()).toBe('"user name"');
    });

    it('should generate SQL for Literal string', () => {
      const lit = new exp.Literal('hello', true);
      expect(lit.sql()).toBe("'hello'");
    });

    it('should generate SQL for Literal number', () => {
      const lit = new exp.Literal(42, false);
      expect(lit.sql()).toBe('42');
    });

    it('should generate SQL for Literal NULL', () => {
      const lit = new exp.Literal(null, false);
      expect(lit.sql()).toBe('NULL');
    });

    it('should generate SQL for Column', () => {
      const col = new exp.Column(new exp.Identifier('name', false));
      expect(col.sql()).toBe('name');
    });

    it('should generate SQL for qualified Column', () => {
      const col = new exp.Column(
        new exp.Identifier('name', false),
        new exp.Identifier('users', false)
      );
      expect(col.sql()).toBe('users.name');
    });

    it('should generate SQL for Star', () => {
      const star = new exp.Star();
      expect(star.sql()).toBe('*');
    });

    it('should generate SQL for Add', () => {
      const add = new exp.Add(
        new exp.Literal(1, false),
        new exp.Literal(2, false)
      );
      expect(add.sql()).toBe('1 + 2');
    });

    it('should generate SQL for complex arithmetic', () => {
      const expr = new exp.Mul(
        new exp.Add(
          new exp.Literal(1, false),
          new exp.Literal(2, false)
        ),
        new exp.Literal(3, false)
      );
      expect(expr.sql()).toBe('1 + 2 * 3');
    });

    it('should generate SQL for Alias', () => {
      const alias = new exp.Alias(
        new exp.Column(new exp.Identifier('name', false)),
        new exp.Identifier('user_name', false)
      );
      expect(alias.sql()).toBe('name AS user_name');
    });

    it('should generate SQL for FunctionCall', () => {
      const fn = new exp.FunctionCall(
        new exp.Identifier('COUNT', false),
        [new exp.Star()]
      );
      expect(fn.sql()).toBe('COUNT(*)');
    });

    it('should generate SQL for FunctionCall with arguments', () => {
      const fn = new exp.FunctionCall(
        new exp.Identifier('MAX', false),
        [new exp.Column(new exp.Identifier('age', false))]
      );
      expect(fn.sql()).toBe('MAX(age)');
    });

    it('should generate SQL for Cast', () => {
      const cast = new exp.Cast(
        new exp.Column(new exp.Identifier('age', false)),
        new exp.DataType('VARCHAR', 10)
      );
      expect(cast.sql()).toBe('CAST(age AS VARCHAR(10))');
    });
  });

  describe('Expression Finding', () => {
    it('should find single expression', () => {
      const ast = parseOne('SELECT name FROM users');
      const column = ast.find(exp.Column);
      expect(column).toBeInstanceOf(exp.Column);
    });

    it('should find all expressions of type', () => {
      const ast = parseOne('SELECT name, age, salary FROM users');
      const columns = ast.findAll(exp.Column);
      expect(columns).toHaveLength(3);
      expect(columns.every(c => c instanceof exp.Column)).toBe(true);
    });

    it('should find nested expressions', () => {
      const ast = parseOne('SELECT COUNT(*) FROM users WHERE age > 18');
      const literals = ast.findAll(exp.Literal);
      expect(literals).toHaveLength(1);
      expect(literals[0].value).toBe(18);
    });

    it('should find expressions in subqueries', () => {
      const ast = parseOne('SELECT * FROM (SELECT id FROM users)');
      const columns = ast.findAll(exp.Column);
      expect(columns).toHaveLength(1);
    });

    it('should return empty array when no matches', () => {
      const ast = parseOne('SELECT * FROM users');
      const functions = ast.findAll(exp.FunctionCall);
      expect(functions).toHaveLength(0);
    });

    it('should find first match with find()', () => {
      const ast = parseOne('SELECT name, age FROM users');
      const column = ast.find(exp.Column);
      expect(column).toBeInstanceOf(exp.Column);
      expect((column as exp.Column).aliasOrName).toBe('name');
    });
  });

  describe('Expression Transformation', () => {
    it('should transform single expression', () => {
      const ast = parseOne('SELECT a FROM x');
      const transformed = ast.transform((node) => {
        if (node instanceof exp.Column && node.aliasOrName === 'a') {
          return new exp.Column(new exp.Identifier('b', false));
        }
        return node;
      });
      expect(transformed.sql()).toContain('b');
    });

    it('should transform multiple expressions', () => {
      const ast = parseOne('SELECT a, a FROM x');
      const transformed = ast.transform((node) => {
        if (node instanceof exp.Column && node.aliasOrName === 'a') {
          return new exp.Column(new exp.Identifier('b', false));
        }
        return node;
      });
      const sql = transformed.sql();
      expect(sql).toContain('b');
      expect(sql).not.toContain(' a');
    });

    it('should transform nested expressions', () => {
      const ast = parseOne('SELECT a + b FROM x');
      const transformed = ast.transform((node) => {
        if (node instanceof exp.Literal) {
          return new exp.Literal((node.value as number) * 2, false);
        }
        if (node instanceof exp.Column && node.aliasOrName === 'a') {
          return new exp.Literal(10, false);
        }
        return node;
      });
      expect(transformed.sql()).toContain('10');
    });

    it('should wrap columns in function', () => {
      const ast = parseOne('SELECT a FROM x');
      const transformed = ast.transform((node) => {
        if (node instanceof exp.Column && node.aliasOrName === 'a') {
          return new exp.FunctionCall(
            new exp.Identifier('UPPER', false),
            [node]
          );
        }
        return node;
      });
      expect(transformed.sql()).toContain('UPPER(a)');
    });

    it('should not transform unmatched expressions', () => {
      const ast = parseOne('SELECT a FROM x');
      const transformed = ast.transform((node) => {
        if (node instanceof exp.Column && node.aliasOrName === 'b') {
          return new exp.Column(new exp.Identifier('c', false));
        }
        return node;
      });
      expect(transformed.sql()).toContain('a');
    });

    it('should handle identity transformation', () => {
      const ast = parseOne('SELECT a FROM x');
      const transformed = ast.transform((node) => node);
      expect(transformed.sql()).toBe(ast.sql());
    });
  });

  describe('aliasOrName', () => {
    it('should return column name', () => {
      const col = new exp.Column(new exp.Identifier('username', false));
      expect(col.aliasOrName).toBe('username');
    });

    it('should return qualified column name', () => {
      const col = new exp.Column(
        new exp.Identifier('name', false),
        new exp.Identifier('users', false)
      );
      expect(col.aliasOrName).toBe('name');
    });
  });

  describe('Complex Expression Trees', () => {
    it('should build Select expression manually', () => {
      const select = new exp.Select(
        [new exp.Star()],
        new exp.From([
          new exp.Table(new exp.Identifier('users', false))
        ])
      );
      expect(select.sql()).toContain('SELECT *');
      expect(select.sql()).toContain('FROM users');
    });

    it('should build WHERE clause manually', () => {
      const where = new exp.Where(
        new exp.GT(
          new exp.Column(new exp.Identifier('age', false)),
          new exp.Literal(18, false)
        )
      );
      expect(where.sql()).toBe('WHERE age > 18');
    });

    it('should build complex expression tree', () => {
      const select = new exp.Select(
        [
          new exp.Alias(
            new exp.FunctionCall(
              new exp.Identifier('COUNT', false),
              [new exp.Star()]
            ),
            new exp.Identifier('total', false)
          )
        ],
        new exp.From([
          new exp.Table(new exp.Identifier('orders', false))
        ]),
        new exp.Where(
          new exp.GT(
            new exp.Column(new exp.Identifier('amount', false)),
            new exp.Literal(100, false)
          )
        )
      );

      const sql = select.sql();
      expect(sql).toContain('COUNT(*)');
      expect(sql).toContain('total');
      expect(sql).toContain('orders');
      expect(sql).toContain('amount > 100');
    });
  });

  describe('Table Expressions', () => {
    it('should generate SQL for simple table', () => {
      const table = new exp.Table(new exp.Identifier('users', false));
      expect(table.sql()).toBe('users');
    });

    it('should generate SQL for table with schema', () => {
      const table = new exp.Table(
        new exp.Identifier('users', false),
        new exp.Identifier('public', false)
      );
      expect(table.sql()).toBe('public.users');
    });

    it('should generate SQL for table with catalog', () => {
      const table = new exp.Table(
        new exp.Identifier('users', false),
        new exp.Identifier('mydb', false),
        new exp.Identifier('prod', false)
      );
      expect(table.sql()).toBe('prod.mydb.users');
    });

    it('should generate SQL for table with alias', () => {
      const table = new exp.Table(
        new exp.Identifier('users', false),
        undefined,
        undefined,
        new exp.Identifier('u', false)
      );
      expect(table.sql()).toBe('users AS u');
    });
  });

  describe('Logical Operators', () => {
    it('should generate SQL for AND', () => {
      const and = new exp.And(
        new exp.EQ(
          new exp.Column(new exp.Identifier('a', false)),
          new exp.Literal(1, false)
        ),
        new exp.EQ(
          new exp.Column(new exp.Identifier('b', false)),
          new exp.Literal(2, false)
        )
      );
      expect(and.sql()).toBe('a = 1 AND b = 2');
    });

    it('should generate SQL for OR', () => {
      const or = new exp.Or(
        new exp.EQ(
          new exp.Column(new exp.Identifier('a', false)),
          new exp.Literal(1, false)
        ),
        new exp.EQ(
          new exp.Column(new exp.Identifier('b', false)),
          new exp.Literal(2, false)
        )
      );
      expect(or.sql()).toBe('a = 1 OR b = 2');
    });

    it('should generate SQL for NOT', () => {
      const not = new exp.Not(
        new exp.EQ(
          new exp.Column(new exp.Identifier('a', false)),
          new exp.Literal(1, false)
        )
      );
      expect(not.sql()).toBe('NOT a = 1');
    });
  });

  describe('Subquery', () => {
    it('should generate SQL for subquery', () => {
      const subquery = new exp.Subquery(
        new exp.Select(
          [new exp.Column(new exp.Identifier('id', false))],
          new exp.From([new exp.Table(new exp.Identifier('users', false))])
        )
      );
      expect(subquery.sql()).toContain('(SELECT id FROM users)');
    });

    it('should generate SQL for subquery with alias', () => {
      const subquery = new exp.Subquery(
        new exp.Select(
          [new exp.Star()],
          new exp.From([new exp.Table(new exp.Identifier('users', false))])
        ),
        new exp.Identifier('u', false)
      );
      const sql = subquery.sql();
      expect(sql).toContain('(SELECT * FROM users)');
      expect(sql).toContain('AS u');
    });
  });

  describe('Paren', () => {
    it('should generate SQL for parenthesized expression', () => {
      const paren = new exp.Paren(
        new exp.Add(
          new exp.Literal(1, false),
          new exp.Literal(2, false)
        )
      );
      expect(paren.sql()).toBe('(1 + 2)');
    });
  });

  describe('String Escaping', () => {
    it('should escape single quotes in strings', () => {
      const lit = new exp.Literal("it's", true);
      expect(lit.sql()).toBe("'it''s'");
    });

    it('should handle strings with multiple quotes', () => {
      const lit = new exp.Literal("'hello' 'world'", true);
      expect(lit.sql()).toBe("'''hello'' ''world'''");
    });
  });
});
