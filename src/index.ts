import { Dialect, DialectType } from './dialect';
import { Expression } from './expressions';
import { Token } from './tokens';
import { ParseError } from './errors';

export * from './tokens';
export * from './errors';
export * from './expressions';
export * from './tokenizer';
export * from './parser';
export * from './generator';
export * from './dialect';

export interface ParseOptions {
  errorLevel?: string;
}

export interface TranspileOptions extends ParseOptions {
  identity?: boolean;
  pretty?: boolean;
  identify?: boolean;
}

/**
 * Tokenizes the given SQL string.
 *
 * @param sql - The SQL code string to tokenize.
 * @param read - The SQL dialect to apply during tokenizing (e.g., "spark", "postgres", "mysql").
 * @param dialect - The SQL dialect (alias for read).
 * @returns The resulting list of tokens.
 */
export function tokenize(
  sql: string,
  read?: DialectType,
  dialect?: DialectType
): Token[] {
  return Dialect.getOrRaise(read || dialect).tokenize(sql);
}

/**
 * Parses the given SQL string into a collection of syntax trees.
 *
 * @param sql - The SQL code string to parse.
 * @param read - The SQL dialect to apply during parsing (e.g., "spark", "postgres", "mysql").
 * @param dialect - The SQL dialect (alias for read).
 * @param options - Other parser options.
 * @returns The resulting syntax tree collection.
 */
export function parse(
  sql: string,
  read?: DialectType,
  dialect?: DialectType,
  options: ParseOptions = {}
): Expression[] {
  return Dialect.getOrRaise(read || dialect).parse(sql, options);
}

/**
 * Parses the given SQL string and returns a syntax tree for the first parsed SQL statement.
 *
 * @param sql - The SQL code string to parse.
 * @param read - The SQL dialect to apply during parsing (e.g., "spark", "postgres", "mysql").
 * @param dialect - The SQL dialect (alias for read).
 * @param options - Other parser options.
 * @returns The syntax tree for the first parsed statement.
 */
export function parseOne(
  sql: string,
  read?: DialectType,
  dialect?: DialectType,
  options: ParseOptions = {}
): Expression {
  const readDialect = Dialect.getOrRaise(read || dialect);
  const result = readDialect.parse(sql, options);

  for (const expression of result) {
    if (!expression) {
      throw new ParseError(`No expression was parsed from '${sql}'`);
    }
    return expression;
  }

  throw new ParseError(`No expression was parsed from '${sql}'`);
}

/**
 * Parses the given SQL string in accordance with the source dialect and returns a list of SQL strings
 * transformed to conform to the target dialect.
 *
 * @param sql - The SQL code string to transpile.
 * @param read - The source dialect used to parse the input (e.g., "spark", "postgres", "mysql").
 * @param write - The target dialect into which the input should be transformed.
 * @param identity - If true and target dialect is not specified, the source dialect will be used as both.
 * @param options - Other generator options (pretty, identify, etc.).
 * @returns The list of transpiled SQL statements.
 */
export function transpile(
  sql: string,
  read?: DialectType,
  write?: DialectType,
  identity: boolean = true,
  options: TranspileOptions = {}
): string[] {
  const writeDialect = (write ?? (identity ? read : null)) ?? null;
  const targetDialect = Dialect.getOrRaise(writeDialect);
  const sourceDialect = Dialect.getOrRaise(read);

  const expressions = sourceDialect.parse(sql, options);

  return expressions.map((expression) =>
    expression ? targetDialect.generate(expression, options) : ''
  );
}

/**
 * Formats SQL by parsing and regenerating it.
 *
 * @param sql - The SQL code string to format.
 * @param read - The SQL dialect to use.
 * @param pretty - Whether to pretty-print the output.
 * @returns The formatted SQL string.
 */
export function format(
  sql: string,
  read?: DialectType,
  pretty: boolean = true
): string {
  const results = transpile(sql, read, read, true, { pretty });
  return results.join(';\n');
}
