import { parse, parseOne, transpile, tokenize, format } from './index';
import * as exp from './expressions';

console.log('SQLGlot-JS Test Suite\n');
console.log('='.repeat(50));

// Test 1: Tokenization
console.log('\n1. Tokenization Test');
console.log('-'.repeat(50));
const sql1 = 'SELECT * FROM users WHERE id = 1';
const tokens = tokenize(sql1);
console.log(`SQL: ${sql1}`);
console.log(`Tokens: ${tokens.length} tokens`);
console.log(`First 5 tokens: ${tokens.slice(0, 5).map(t => `${t.type}(${t.text})`).join(', ')}`);

// Test 2: Basic Parsing
console.log('\n2. Basic Parsing Test');
console.log('-'.repeat(50));
const sql2 = 'SELECT name, age FROM users';
const ast = parseOne(sql2);
console.log(`SQL: ${sql2}`);
console.log(`AST Type: ${ast.constructor.name}`);
console.log(`Regenerated: ${ast.sql()}`);

// Test 3: Complex Query
console.log('\n3. Complex Query Test');
console.log('-'.repeat(50));
const sql3 = 'SELECT u.name, COUNT(*) as count FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 18 GROUP BY u.name ORDER BY count DESC LIMIT 10';
const ast3 = parseOne(sql3);
console.log(`SQL: ${sql3}`);
console.log(`Regenerated: ${ast3.sql()}`);

// Test 4: Transpilation (currently no real dialect differences implemented, but structure is there)
console.log('\n4. Transpilation Test');
console.log('-'.repeat(50));
const sql4 = 'SELECT a + b AS total FROM table1';
const transpiled = transpile(sql4, 'postgres', 'mysql');
console.log(`Original SQL: ${sql4}`);
console.log(`Transpiled: ${transpiled[0]}`);

// Test 5: Expression Finding
console.log('\n5. Expression Finding Test');
console.log('-'.repeat(50));
const sql5 = 'SELECT name, age, salary FROM employees WHERE age > 30';
const ast5 = parseOne(sql5);
const columns = ast5.findAll(exp.Column);
console.log(`SQL: ${sql5}`);
console.log(`Found ${columns.length} columns: ${columns.map(c => c.aliasOrName).join(', ')}`);

// Test 6: Expression Transformation
console.log('\n6. Expression Transformation Test');
console.log('-'.repeat(50));
const sql6 = 'SELECT a FROM x';
const ast6 = parseOne(sql6);
const transformed = ast6.transform((node) => {
  if (node instanceof exp.Column && node.aliasOrName === 'a') {
    return new exp.FunctionCall(
      new exp.Identifier('UPPER', false),
      [node]
    );
  }
  return node;
});
console.log(`Original SQL: ${sql6}`);
console.log(`Transformed: ${transformed.sql()}`);

// Test 7: Arithmetic Operations
console.log('\n7. Arithmetic Operations Test');
console.log('-'.repeat(50));
const sql7 = 'SELECT price * quantity + tax AS total FROM orders';
const ast7 = parseOne(sql7);
console.log(`SQL: ${sql7}`);
console.log(`Regenerated: ${ast7.sql()}`);

// Test 8: Subqueries
console.log('\n8. Subquery Test');
console.log('-'.repeat(50));
const sql8 = 'SELECT * FROM (SELECT id, name FROM users) AS u WHERE u.id = 1';
const ast8 = parseOne(sql8);
console.log(`SQL: ${sql8}`);
console.log(`Regenerated: ${ast8.sql()}`);

// Test 9: String Literals
console.log('\n9. String Literal Test');
console.log('-'.repeat(50));
const sql9 = "SELECT * FROM users WHERE name = 'John Doe' AND status = 'active'";
const ast9 = parseOne(sql9);
console.log(`SQL: ${sql9}`);
console.log(`Regenerated: ${ast9.sql()}`);

// Test 10: NULL values
console.log('\n10. NULL Value Test');
console.log('-'.repeat(50));
const sql10 = 'SELECT * FROM users WHERE deleted_at IS NULL';
const ast10 = parseOne(sql10);
console.log(`SQL: ${sql10}`);
console.log(`Regenerated: ${ast10.sql()}`);

console.log('\n' + '='.repeat(50));
console.log('All tests completed successfully!');
