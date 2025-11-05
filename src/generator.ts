import { Expression } from './expressions';

export interface GeneratorOptions {
  pretty?: boolean;
  identify?: boolean;
  dialect?: string;
}

export class Generator {
  private options: GeneratorOptions;
  private indent: number = 0;
  private indentStr: string = '  ';

  constructor(options: GeneratorOptions = {}) {
    this.options = {
      pretty: false,
      identify: false,
      ...options,
    };
  }

  generate(expression: Expression | null): string {
    if (!expression) {
      return '';
    }

    return expression.sql(this.options.dialect);
  }

  sql(expression: Expression | null): string {
    return this.generate(expression);
  }

  private increaseIndent(): void {
    this.indent++;
  }

  private decreaseIndent(): void {
    this.indent--;
  }

  private getIndent(): string {
    return this.options.pretty ? this.indentStr.repeat(this.indent) : '';
  }

  private newline(): string {
    return this.options.pretty ? '\n' : ' ';
  }

  private space(): string {
    return ' ';
  }

  format(sql: string): string {
    if (!this.options.pretty) {
      return sql;
    }

    const lines: string[] = [];
    let currentIndent = 0;
    const tokens = sql.split(/\s+/);

    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i];
      const upperToken = token.toUpperCase();

      if (upperToken === 'SELECT' || upperToken === 'FROM' || upperToken === 'WHERE') {
        if (lines.length > 0) {
          lines.push('\n');
        }
        lines.push(this.indentStr.repeat(currentIndent) + token);
      } else if (upperToken === 'JOIN' || upperToken.includes('JOIN')) {
        lines.push('\n' + this.indentStr.repeat(currentIndent) + token);
      } else {
        if (lines.length > 0 && !lines[lines.length - 1].endsWith('\n')) {
          lines.push(' ');
        }
        lines.push(token);
      }
    }

    return lines.join('');
  }
}
