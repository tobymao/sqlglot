import { describe, it, expect } from 'vitest';
import { transpile, parseOne, tokenize, format } from './index';

describe('Transpile', () => {
  describe('Basic Transpilation', () => {
    it('should transpile simple SELECT', () => {
      const result = transpile('SELECT * FROM users', 'postgres', 'mysql');
      expect(result).toHaveLength(1);
      expect(result[0]).toBe('SELECT * FROM users');
    });

    it('should transpile with same dialect', () => {
      const sql = 'SELECT name, age FROM users WHERE age > 18';
      const result = transpile(sql, 'postgres', 'postgres');
      expect(result[0]).toBe(sql);
    });

    it('should transpile multiple statements', () => {
      const sql = 'SELECT * FROM users; SELECT * FROM orders';
      const result = transpile(sql);
      expect(result).toHaveLength(2);
      expect(result[0]).toContain('users');
      expect(result[1]).toContain('orders');
    });

    it('should handle identity flag', () => {
      const sql = 'SELECT * FROM users';
      const result = transpile(sql, 'postgres', undefined, true);
      expect(result[0]).toBe(sql);
    });
  });

  describe('Dialect Variations', () => {
    it('should support postgres dialect', () => {
      const result = transpile('SELECT * FROM users', 'postgres', 'postgres');
      expect(result[0]).toContain('SELECT');
    });

    it('should support mysql dialect', () => {
      const result = transpile('SELECT * FROM users', 'mysql', 'mysql');
      expect(result[0]).toContain('SELECT');
    });

    it('should support sqlite dialect', () => {
      const result = transpile('SELECT * FROM users', 'sqlite', 'sqlite');
      expect(result[0]).toContain('SELECT');
    });

    it('should support bigquery dialect', () => {
      const result = transpile('SELECT * FROM users', 'bigquery', 'bigquery');
      expect(result[0]).toContain('SELECT');
    });

    it('should support snowflake dialect', () => {
      const result = transpile('SELECT * FROM users', 'snowflake', 'snowflake');
      expect(result[0]).toContain('SELECT');
    });
  });

  describe('Complex Query Transpilation', () => {
    it('should transpile query with JOINs', () => {
      const sql = 'SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id';
      const result = transpile(sql, 'postgres', 'mysql');
      expect(result[0]).toContain('JOIN');
      expect(result[0]).toContain('ON');
    });

    it('should transpile query with WHERE clause', () => {
      const sql = 'SELECT * FROM users WHERE age > 18 AND status = 1';
      const result = transpile(sql);
      expect(result[0]).toContain('WHERE');
      expect(result[0]).toContain('AND');
    });

    it('should transpile query with GROUP BY', () => {
      const sql = 'SELECT name, COUNT(*) FROM users GROUP BY name';
      const result = transpile(sql);
      expect(result[0]).toContain('GROUP BY');
    });

    it('should transpile query with ORDER BY', () => {
      const sql = 'SELECT * FROM users ORDER BY name DESC';
      const result = transpile(sql);
      expect(result[0]).toContain('ORDER BY');
      expect(result[0]).toContain('DESC');
    });

    it('should transpile query with LIMIT', () => {
      const sql = 'SELECT * FROM users LIMIT 10';
      const result = transpile(sql);
      expect(result[0]).toContain('LIMIT');
    });
  });

  describe('Subquery Transpilation', () => {
    it('should transpile subquery in FROM', () => {
      const sql = 'SELECT * FROM (SELECT id FROM users) AS u';
      const result = transpile(sql);
      expect(result[0]).toContain('(SELECT id FROM users)');
    });

    it('should transpile nested subqueries', () => {
      const sql = 'SELECT * FROM (SELECT * FROM (SELECT id FROM users) t1) t2';
      const result = transpile(sql);
      expect(result[0]).toContain('SELECT');
    });
  });

  describe('Function Transpilation', () => {
    it('should transpile COUNT function', () => {
      const sql = 'SELECT COUNT(*) FROM users';
      const result = transpile(sql);
      expect(result[0]).toContain('COUNT(*)');
    });

    it('should transpile with function arguments', () => {
      const sql = 'SELECT MAX(age), MIN(age) FROM users';
      const result = transpile(sql);
      expect(result[0]).toContain('MAX(age)');
      expect(result[0]).toContain('MIN(age)');
    });

    it('should transpile CAST', () => {
      const sql = 'SELECT CAST(age AS VARCHAR) FROM users';
      const result = transpile(sql);
      expect(result[0]).toContain('CAST');
      expect(result[0]).toContain('VARCHAR');
    });
  });

  describe('Expression Transpilation', () => {
    it('should transpile arithmetic expressions', () => {
      const sql = 'SELECT a + b * c FROM t';
      const result = transpile(sql);
      expect(result[0]).toContain('+');
      expect(result[0]).toContain('*');
    });

    it('should transpile comparison expressions', () => {
      const sql = 'SELECT * FROM t WHERE a = b AND c > d';
      const result = transpile(sql);
      expect(result[0]).toContain('=');
      expect(result[0]).toContain('>');
    });

    it('should transpile logical expressions', () => {
      const sql = 'SELECT * FROM t WHERE a AND b OR c';
      const result = transpile(sql);
      expect(result[0]).toContain('AND');
      expect(result[0]).toContain('OR');
    });
  });

  describe('Options', () => {
    it('should support pretty printing', () => {
      const sql = 'SELECT name, age FROM users WHERE age > 18';
      const result = transpile(sql, undefined, undefined, true, { pretty: true });
      // Pretty printing adds formatting
      expect(result[0]).toContain('SELECT');
    });

    it('should handle empty SQL', () => {
      const result = transpile('');
      expect(result).toHaveLength(0);
    });
  });
});

describe('Format', () => {
  it('should format simple SQL', () => {
    const sql = 'SELECT * FROM users';
    const formatted = format(sql);
    expect(formatted).toContain('SELECT');
    expect(formatted).toContain('FROM');
  });

  it('should format with default pretty option', () => {
    const sql = 'SELECT name,age FROM users WHERE age>18';
    const formatted = format(sql);
    expect(formatted).toBeTruthy();
  });

  it('should join multiple statements with semicolons', () => {
    const sql = 'SELECT * FROM users; SELECT * FROM orders';
    const formatted = format(sql);
    expect(formatted).toContain(';');
  });
});

describe('ParseOne', () => {
  it('should parse single statement', () => {
    const ast = parseOne('SELECT * FROM users');
    expect(ast).toBeDefined();
    expect(ast.sql()).toContain('SELECT');
  });

  it('should return first statement when multiple exist', () => {
    const ast = parseOne('SELECT * FROM users; SELECT * FROM orders');
    expect(ast.sql()).toContain('users');
  });

  it('should throw error for empty SQL', () => {
    expect(() => parseOne('')).toThrow();
  });

  it('should parse with dialect', () => {
    const ast = parseOne('SELECT * FROM users', 'postgres');
    expect(ast).toBeDefined();
  });
});

describe('Tokenize', () => {
  it('should tokenize SQL string', () => {
    const tokens = tokenize('SELECT * FROM users');
    expect(tokens.length).toBeGreaterThan(0);
  });

  it('should tokenize with dialect', () => {
    const tokens = tokenize('SELECT * FROM users', 'postgres');
    expect(tokens.length).toBeGreaterThan(0);
  });

  it('should handle empty string', () => {
    const tokens = tokenize('');
    expect(tokens).toHaveLength(1); // Just EOF token
  });
});

describe('Dialect Support', () => {
  const sql = 'SELECT * FROM users WHERE id = 1';

  it('should support default dialect', () => {
    const result = transpile(sql);
    expect(result[0]).toBeTruthy();
  });

  it('should support postgres', () => {
    const result = transpile(sql, 'postgres', 'postgres');
    expect(result[0]).toBeTruthy();
  });

  it('should support mysql', () => {
    const result = transpile(sql, 'mysql', 'mysql');
    expect(result[0]).toBeTruthy();
  });

  it('should support sqlite', () => {
    const result = transpile(sql, 'sqlite', 'sqlite');
    expect(result[0]).toBeTruthy();
  });

  it('should support bigquery', () => {
    const result = transpile(sql, 'bigquery', 'bigquery');
    expect(result[0]).toBeTruthy();
  });

  it('should support snowflake', () => {
    const result = transpile(sql, 'snowflake', 'snowflake');
    expect(result[0]).toBeTruthy();
  });

  it('should support redshift', () => {
    const result = transpile(sql, 'redshift', 'redshift');
    expect(result[0]).toBeTruthy();
  });

  it('should support mssql', () => {
    const result = transpile(sql, 'mssql', 'mssql');
    expect(result[0]).toBeTruthy();
  });

  it('should support oracle', () => {
    const result = transpile(sql, 'oracle', 'oracle');
    expect(result[0]).toBeTruthy();
  });

  it('should support hive', () => {
    const result = transpile(sql, 'hive', 'hive');
    expect(result[0]).toBeTruthy();
  });

  it('should support spark', () => {
    const result = transpile(sql, 'spark', 'spark');
    expect(result[0]).toBeTruthy();
  });

  it('should support presto', () => {
    const result = transpile(sql, 'presto', 'presto');
    expect(result[0]).toBeTruthy();
  });

  it('should support trino', () => {
    const result = transpile(sql, 'trino', 'trino');
    expect(result[0]).toBeTruthy();
  });

  it('should support clickhouse', () => {
    const result = transpile(sql, 'clickhouse', 'clickhouse');
    expect(result[0]).toBeTruthy();
  });

  it('should support duckdb', () => {
    const result = transpile(sql, 'duckdb', 'duckdb');
    expect(result[0]).toBeTruthy();
  });
});
