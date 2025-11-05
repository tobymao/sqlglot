# SQLGlot-JS

A TypeScript reimplementation of SQLGlot - a no-dependency SQL parser, transpiler, and formatter.

## Features

- **Zero Dependencies**: Pure TypeScript implementation with no external libraries
- **SQL Parsing**: Parse SQL into an Abstract Syntax Tree (AST)
- **SQL Generation**: Generate SQL from AST
- **SQL Transpilation**: Convert between different SQL dialects (basic support)
- **SQL Formatting**: Format and prettify SQL queries
- **Expression Traversal**: Find and transform AST nodes
- **Multiple Dialects**: Basic support for PostgreSQL, MySQL, SQLite, BigQuery, Snowflake, and more

## Installation

```bash
npm install
npm run build
```

## Usage

### Parsing SQL

```typescript
import { parse, parseOne } from './src/index';

// Parse a single statement
const ast = parseOne('SELECT * FROM users WHERE id = 1');

// Parse multiple statements
const statements = parse('SELECT * FROM users; SELECT * FROM orders;');
```

### Generating SQL

```typescript
import { parseOne } from './src/index';

const ast = parseOne('SELECT name, age FROM users');
const sql = ast.sql(); // Regenerate SQL from AST
```

### Transpiling SQL

```typescript
import { transpile } from './src/index';

const result = transpile(
  'SELECT name FROM users',
  'postgres',  // source dialect
  'mysql'      // target dialect
);
console.log(result[0]);
```

### Finding Expressions

```typescript
import { parseOne } from './src/index';
import { Column } from './src/expressions';

const ast = parseOne('SELECT name, age FROM users');

// Find all columns
const columns = ast.findAll(Column);
console.log(columns.map(c => c.aliasOrName));
```

### Transforming Expressions

```typescript
import { parseOne } from './src/index';
import { Column, FunctionCall, Identifier } from './src/expressions';

const ast = parseOne('SELECT a FROM x');

// Transform column 'a' to UPPER(a)
const transformed = ast.transform((node) => {
  if (node instanceof Column && node.aliasOrName === 'a') {
    return new FunctionCall(
      new Identifier('UPPER', false),
      [node]
    );
  }
  return node;
});

console.log(transformed.sql()); // SELECT UPPER(a) FROM x
```

### Tokenization

```typescript
import { tokenize } from './src/index';

const tokens = tokenize('SELECT * FROM users');
tokens.forEach(token => {
  console.log(`${token.type}: ${token.text}`);
});
```

## Architecture

The library consists of several core components:

### 1. Tokenizer (`tokenizer.ts`)
Breaks SQL strings into tokens (keywords, identifiers, operators, etc.)

### 2. Parser (`parser.ts`)
Parses tokens into an Abstract Syntax Tree (AST)

### 3. Expressions (`expressions.ts`)
Defines all AST node types (Select, Column, Table, BinaryExpression, etc.)

### 4. Generator (`generator.ts`)
Converts AST back into SQL strings

### 5. Dialect (`dialect.ts`)
Manages different SQL dialects and their specific parsing/generation rules

### 6. Errors (`errors.ts`)
Custom error types for parsing and tokenization errors

## Supported SQL Features

- SELECT statements with:
  - Column selection (including `*`)
  - FROM clause with tables and subqueries
  - WHERE conditions
  - JOINs (INNER, LEFT, RIGHT, OUTER)
  - GROUP BY and HAVING
  - ORDER BY with ASC/DESC
  - LIMIT and OFFSET
- Binary operators: `+`, `-`, `*`, `/`, `%`, `=`, `<>`, `<`, `>`, `<=`, `>=`
- Logical operators: `AND`, `OR`, `NOT`
- Function calls
- CAST expressions
- String and numeric literals
- NULL values
- Subqueries
- Aliases (AS)
- Comments (line and block)

## Running Tests

```bash
npm run build
npm test
```

## Example Output

```
SQLGlot-JS Test Suite

==================================================

1. Tokenization Test
--------------------------------------------------
SQL: SELECT * FROM users WHERE id = 1
Tokens: 10 tokens
First 5 tokens: SELECT(SELECT), STAR(*), FROM(FROM), IDENTIFIER(users), WHERE(WHERE)

2. Basic Parsing Test
--------------------------------------------------
SQL: SELECT name, age FROM users
AST Type: Select
Regenerated: SELECT name, age FROM users

3. Complex Query Test
--------------------------------------------------
SQL: SELECT u.name, COUNT(*) as count FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 18 GROUP BY u.name ORDER BY count DESC LIMIT 10
Regenerated: SELECT u.name, COUNT(*) AS count FROM users AS u LEFT JOIN orders AS o ON u.id = o.user_id WHERE u.age > 18 GROUP BY u.name ORDER BY count DESC LIMIT 10
```

## Differences from Python SQLGlot

This TypeScript implementation is a simplified version focusing on core functionality:

- Fewer SQL dialects fully implemented (structure exists for 14 dialects)
- Simplified optimizer (not yet implemented)
- No SQL execution engine
- Subset of SQL features compared to full Python version
- No external dependencies (Python version can use dateutil and Rust tokenizer)

## Future Enhancements

- [ ] Full dialect-specific transformations
- [ ] SQL optimizer
- [ ] More SQL statement types (INSERT, UPDATE, DELETE, CREATE, etc.)
- [ ] Window functions
- [ ] CTEs (Common Table Expressions)
- [ ] More comprehensive error reporting
- [ ] Performance optimizations

## License

MIT

## Credits

This is a TypeScript reimplementation inspired by the original [SQLGlot](https://github.com/tobymao/sqlglot) Python library by Toby Mao.
