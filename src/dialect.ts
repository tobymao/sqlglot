import { Tokenizer } from './tokenizer';
import { Parser } from './parser';
import { Generator, GeneratorOptions } from './generator';
import { Token } from './tokens';
import { Expression } from './expressions';
import { ParseError } from './errors';

export type DialectType = string | Dialect | null | undefined;

export class Dialect {
  private static dialects: Map<string, Dialect> = new Map();

  constructor(
    public readonly name: string,
    private tokenizerClass: typeof Tokenizer = Tokenizer,
    private parserClass: typeof Parser = Parser,
    private generatorClass: typeof Generator = Generator
  ) {
    Dialect.dialects.set(name.toLowerCase(), this);
  }

  static get(name: DialectType): Dialect | null {
    if (!name) {
      return Dialect.getOrRaise(null);
    }
    if (name instanceof Dialect) {
      return name;
    }
    return Dialect.dialects.get(name.toLowerCase()) || null;
  }

  static getOrRaise(name: DialectType): Dialect {
    if (!name) {
      return defaultDialect;
    }
    if (name instanceof Dialect) {
      return name;
    }
    const dialect = Dialect.get(name);
    if (!dialect) {
      throw new Error(`Unknown dialect: ${name}`);
    }
    return dialect;
  }

  tokenize(sql: string): Token[] {
    const tokenizer = new this.tokenizerClass();
    return tokenizer.tokenize(sql);
  }

  parse(sql: string, options: any = {}): Expression[] {
    const tokens = this.tokenize(sql);
    const parser = new this.parserClass();
    return parser.parse(tokens);
  }

  parseOne(sql: string, options: any = {}): Expression {
    const expressions = this.parse(sql, options);
    if (expressions.length === 0) {
      throw new ParseError(`No expression was parsed from '${sql}'`);
    }
    return expressions[0];
  }

  generate(expression: Expression, options: GeneratorOptions = {}): string {
    const generator = new this.generatorClass({
      ...options,
      dialect: this.name,
    });
    return generator.generate(expression);
  }
}

// Default dialect (generic SQL)
export const defaultDialect = new Dialect('default');

// Register common dialects
new Dialect('postgres');
new Dialect('mysql');
new Dialect('sqlite');
new Dialect('bigquery');
new Dialect('snowflake');
new Dialect('redshift');
new Dialect('mssql');
new Dialect('oracle');
new Dialect('hive');
new Dialect('spark');
new Dialect('presto');
new Dialect('trino');
new Dialect('clickhouse');
new Dialect('duckdb');
