import { describe, it, expect } from 'vitest';
import { parseOne, transpile } from './index';
import * as exp from './expressions';

describe('Integration Tests', () => {
  describe('End-to-End SQL Processing', () => {
    it('should parse and regenerate simple query', () => {
      const sql = 'SELECT * FROM users';
      const ast = parseOne(sql);
      const result = ast.sql();
      expect(result).toBe(sql);
    });

    it('should parse and regenerate complex query', () => {
      const sql = 'SELECT u.name, COUNT(*) AS count FROM users AS u WHERE u.age > 18 GROUP BY u.name ORDER BY count DESC LIMIT 10';
      const ast = parseOne(sql);
      const result = ast.sql();
      // Check that all major parts are present
      expect(result).toContain('SELECT');
      expect(result).toContain('COUNT(*)');
      expect(result).toContain('FROM');
      expect(result).toContain('WHERE');
      expect(result).toContain('GROUP BY');
      expect(result).toContain('ORDER BY');
      expect(result).toContain('LIMIT');
    });

    it('should handle round-trip parsing', () => {
      const sql = 'SELECT name, age FROM users WHERE age > 18';
      const ast1 = parseOne(sql);
      const sql1 = ast1.sql();
      const ast2 = parseOne(sql1);
      const sql2 = ast2.sql();
      expect(sql1).toBe(sql2);
    });
  });

  describe('Real-World Query Examples', () => {
    it('should handle user analytics query', () => {
      const sql = `
        SELECT
          u.country,
          COUNT(DISTINCT u.id) AS user_count,
          COUNT(o.id) AS order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at > 1234567890
        GROUP BY u.country
        HAVING COUNT(o.id) > 10
        ORDER BY user_count DESC
        LIMIT 100
      `;
      const ast = parseOne(sql);
      const result = ast.sql();
      expect(result).toContain('COUNT(DISTINCT');
      expect(result).toContain('LEFT JOIN');
      expect(result).toContain('GROUP BY');
      expect(result).toContain('HAVING');
    });

    it('should handle sales report query', () => {
      const sql = `
        SELECT
          p.category,
          SUM(o.quantity * o.price) AS total_revenue,
          AVG(o.price) AS avg_price
        FROM products p
        INNER JOIN orders o ON p.id = o.product_id
        WHERE o.status = 1
        GROUP BY p.category
        ORDER BY total_revenue DESC
      `;
      const ast = parseOne(sql);
      expect(ast).toBeInstanceOf(exp.Select);
      const result = ast.sql();
      expect(result).toContain('INNER JOIN');
      expect(result).toContain('SUM');
    });

    it('should handle subquery for top customers', () => {
      const sql = `
        SELECT name, email
        FROM users
        WHERE id IN (
          SELECT user_id
          FROM orders
          GROUP BY user_id
          HAVING COUNT(*) > 5
        )
      `;
      const ast = parseOne(sql);
      const result = ast.sql();
      expect(result).toContain('SELECT');
      expect(result).toContain('WHERE');
    });
  });

  describe('Expression Manipulation Workflows', () => {
    it('should find and count all columns', () => {
      const sql = 'SELECT a, b, c FROM t WHERE d = 1 AND e > 2';
      const ast = parseOne(sql);
      const columns = ast.findAll(exp.Column);
      expect(columns.length).toBeGreaterThanOrEqual(5);
    });

    it('should find all function calls', () => {
      const sql = 'SELECT COUNT(*), MAX(age), MIN(salary) FROM users';
      const ast = parseOne(sql);
      const functions = ast.findAll(exp.FunctionCall);
      expect(functions).toHaveLength(3);
    });

    it('should find all literals', () => {
      const sql = "SELECT * FROM users WHERE age > 18 AND name = 'John' AND status = 1";
      const ast = parseOne(sql);
      const literals = ast.findAll(exp.Literal);
      expect(literals).toHaveLength(3);
    });

    it('should transform column names to uppercase', () => {
      const sql = 'SELECT a, b FROM t WHERE c = 1';
      const ast = parseOne(sql);
      const transformed = ast.transform((node) => {
        if (node instanceof exp.Column) {
          return new exp.Column(
            new exp.Identifier(node.name.name.toUpperCase(), false),
            node.table
          );
        }
        return node;
      });
      const result = transformed.sql();
      expect(result).toContain('A');
      expect(result).toContain('B');
      expect(result).toContain('C');
    });

    it('should wrap all columns in LOWER function', () => {
      const sql = 'SELECT name, email FROM users';
      const ast = parseOne(sql);
      const transformed = ast.transform((node) => {
        if (node instanceof exp.Column && !(node.name.name === '*')) {
          return new exp.FunctionCall(
            new exp.Identifier('LOWER', false),
            [node]
          );
        }
        return node;
      });
      const result = transformed.sql();
      expect(result).toContain('LOWER(name)');
      expect(result).toContain('LOWER(email)');
    });

    it('should add WHERE clause to query without one', () => {
      const sql = 'SELECT * FROM users';
      const ast = parseOne(sql) as exp.Select;

      // Create new Select with WHERE clause
      const newAst = new exp.Select(
        ast.expressions,
        ast.from,
        new exp.Where(
          new exp.GT(
            new exp.Column(new exp.Identifier('age', false)),
            new exp.Literal(18, false)
          )
        ),
        ast.groupBy,
        ast.having,
        ast.orderBy,
        ast.limit,
        ast.offset,
        ast.distinct
      );

      const result = newAst.sql();
      expect(result).toContain('WHERE age > 18');
    });
  });

  describe('Cross-Dialect Compatibility', () => {
    it('should transpile between dialects', () => {
      const sql = 'SELECT * FROM users WHERE id = 1';
      const result = transpile(sql, 'postgres', 'mysql');
      expect(result[0]).toContain('SELECT');
      expect(result[0]).toContain('FROM');
      expect(result[0]).toContain('WHERE');
    });

    it('should maintain query semantics across dialects', () => {
      const sql = 'SELECT COUNT(*) FROM users GROUP BY country';
      const dialects = ['postgres', 'mysql', 'sqlite', 'bigquery'];

      const results = dialects.map(dialect =>
        transpile(sql, dialect, dialect)[0]
      );

      // All results should contain the same basic structure
      results.forEach(result => {
        expect(result).toContain('COUNT(*)');
        expect(result).toContain('GROUP BY');
      });
    });
  });

  describe('Error Recovery and Edge Cases', () => {
    it('should handle empty SELECT list gracefully', () => {
      // This is invalid SQL but parser should handle gracefully
      expect(() => parseOne('SELECT FROM users')).toThrow();
    });

    it('should handle multiple spaces and newlines', () => {
      const sql = `
        SELECT
          *
        FROM
          users
        WHERE
          age > 18
      `;
      const ast = parseOne(sql);
      expect(ast).toBeInstanceOf(exp.Select);
    });

    it('should preserve query semantics with different formatting', () => {
      const sql1 = 'SELECT * FROM users WHERE age>18 AND status=1';
      const sql2 = 'SELECT * FROM users WHERE age > 18 AND status = 1';

      const ast1 = parseOne(sql1);
      const ast2 = parseOne(sql2);

      // Both should have the same structure
      expect(ast1.findAll(exp.And)).toHaveLength(1);
      expect(ast2.findAll(exp.And)).toHaveLength(1);
    });
  });

  describe('Performance and Scalability', () => {
    it('should handle query with many columns', () => {
      const columns = Array.from({ length: 50 }, (_, i) => `col${i}`).join(', ');
      const sql = `SELECT ${columns} FROM users`;
      const ast = parseOne(sql);
      const columnNodes = ast.findAll(exp.Column);
      expect(columnNodes).toHaveLength(50);
    });

    it('should handle deeply nested expressions', () => {
      const sql = 'SELECT a + b + c + d + e + f + g + h FROM t';
      const ast = parseOne(sql);
      expect(ast).toBeInstanceOf(exp.Select);
      const result = ast.sql();
      expect(result).toContain('+');
    });

    it('should handle query with many JOINs', () => {
      const sql = `
        SELECT *
        FROM t1
        JOIN t2 ON t1.id = t2.t1_id
        JOIN t3 ON t2.id = t3.t2_id
        JOIN t4 ON t3.id = t4.t3_id
      `;
      const ast = parseOne(sql);
      expect(ast).toBeInstanceOf(exp.Select);
    });
  });

  describe('SQL Generation Consistency', () => {
    it('should generate consistent output', () => {
      const sql = 'SELECT name, age FROM users WHERE age > 18';
      const ast = parseOne(sql);

      const result1 = ast.sql();
      const result2 = ast.sql();

      expect(result1).toBe(result2);
    });

    it('should handle operator precedence correctly', () => {
      const sql = 'SELECT a + b * c FROM t';
      const ast = parseOne(sql);
      const result = ast.sql();

      // Multiplication should bind tighter than addition
      expect(result).toContain('a + b * c');
    });

    it('should preserve parentheses', () => {
      const sql = 'SELECT (a + b) * c FROM t';
      const ast = parseOne(sql);
      const result = ast.sql();
      expect(result).toContain('(a + b)');
    });
  });

  describe('Practical Use Cases', () => {
    it('should validate SQL syntax by parsing', () => {
      const validSQL = 'SELECT * FROM users';
      expect(() => parseOne(validSQL)).not.toThrow();

      const invalidSQL = 'SELECT * FROM';
      expect(() => parseOne(invalidSQL)).toThrow();
    });

    it('should extract table names from query', () => {
      const sql = 'SELECT * FROM users JOIN orders ON users.id = orders.user_id';
      const ast = parseOne(sql);
      const tables = ast.findAll(exp.Table);
      expect(tables.length).toBeGreaterThanOrEqual(2);
    });

    it('should identify aggregation queries', () => {
      const sql = 'SELECT COUNT(*), AVG(price) FROM orders';
      const ast = parseOne(sql);
      const functions = ast.findAll(exp.FunctionCall);
      const hasAggregation = functions.length > 0;
      expect(hasAggregation).toBe(true);
    });

    it('should detect queries with WHERE clause', () => {
      const sql = 'SELECT * FROM users WHERE age > 18';
      const ast = parseOne(sql) as exp.Select;
      expect(ast.where).toBeDefined();
    });

    it('should detect queries with GROUP BY', () => {
      const sql = 'SELECT country, COUNT(*) FROM users GROUP BY country';
      const ast = parseOne(sql) as exp.Select;
      expect(ast.groupBy).toBeDefined();
    });
  });

  describe('String Handling', () => {
    it('should handle single-quoted strings', () => {
      const sql = "SELECT * FROM users WHERE name = 'John'";
      const ast = parseOne(sql);
      const literals = ast.findAll(exp.Literal);
      expect(literals.some(l => l.value === 'John')).toBe(true);
    });

    it('should handle strings with special characters', () => {
      const sql = "SELECT * FROM users WHERE name = 'O''Brien'";
      const ast = parseOne(sql);
      expect(ast).toBeInstanceOf(exp.Select);
    });

    it('should handle empty strings', () => {
      const sql = "SELECT * FROM users WHERE name = ''";
      const ast = parseOne(sql);
      expect(ast).toBeInstanceOf(exp.Select);
    });
  });

  describe('NULL Handling', () => {
    it('should parse NULL literal', () => {
      const sql = 'SELECT NULL FROM users';
      const ast = parseOne(sql);
      const literals = ast.findAll(exp.Literal);
      expect(literals.some(l => l.value === null)).toBe(true);
    });

    it('should handle IS NULL', () => {
      const sql = 'SELECT * FROM users WHERE email IS NULL';
      const ast = parseOne(sql);
      expect(ast).toBeInstanceOf(exp.Select);
    });

    it('should handle IS NOT NULL', () => {
      const sql = 'SELECT * FROM users WHERE email IS NOT NULL';
      const ast = parseOne(sql);
      const select = ast as exp.Select;
      expect(select.where).toBeDefined();
    });
  });
});
