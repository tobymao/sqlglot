import { Token, TokenType } from './tokens';
import { ParseError } from './errors';
import * as exp from './expressions';

export class Parser {
  private tokens: Token[] = [];
  private current: number = 0;

  parse(tokens: Token[]): exp.Expression[] {
    this.tokens = tokens;
    this.current = 0;
    const statements: exp.Expression[] = [];

    while (!this.isAtEnd()) {
      if (this.check(TokenType.SEMICOLON)) {
        this.advance();
        continue;
      }
      statements.push(this.statement());
    }

    return statements;
  }

  private statement(): exp.Expression {
    if (this.check(TokenType.SELECT)) {
      return this.selectStatement();
    }

    throw this.error(this.peek(), 'Expected statement');
  }

  private selectStatement(): exp.Select {
    this.consume(TokenType.SELECT, "Expected 'SELECT'");

    const distinct = this.match(TokenType.DISTINCT);
    const expressions = this.expressionList();

    let from: exp.From | undefined;
    let where: exp.Where | undefined;
    let groupBy: exp.GroupBy | undefined;
    let having: exp.Having | undefined;
    let orderBy: exp.OrderBy | undefined;
    let limit: exp.Limit | undefined;
    let offset: exp.Offset | undefined;

    if (this.match(TokenType.FROM)) {
      from = this.fromClause();
    }

    if (this.match(TokenType.WHERE)) {
      where = this.whereClause();
    }

    if (this.match(TokenType.GROUP)) {
      this.consume(TokenType.BY, "Expected 'BY' after 'GROUP'");
      groupBy = this.groupByClause();
    }

    if (this.match(TokenType.HAVING)) {
      having = this.havingClause();
    }

    if (this.match(TokenType.ORDER)) {
      this.consume(TokenType.BY, "Expected 'BY' after 'ORDER'");
      orderBy = this.orderByClause();
    }

    if (this.match(TokenType.LIMIT)) {
      limit = this.limitClause();
    }

    if (this.match(TokenType.OFFSET)) {
      offset = this.offsetClause();
    }

    return new exp.Select(
      expressions,
      from,
      where,
      groupBy,
      having,
      orderBy,
      limit,
      offset,
      distinct
    );
  }

  private expressionList(): exp.Expression[] {
    const expressions: exp.Expression[] = [];
    do {
      let expr = this.expression();
      // Handle AS alias in SELECT list
      if (this.match(TokenType.AS)) {
        const alias = this.identifier();
        expr = new exp.Alias(expr, alias);
      }
      expressions.push(expr);
    } while (this.match(TokenType.COMMA));
    return expressions;
  }

  private fromClause(): exp.From {
    const tables: exp.Expression[] = [];
    do {
      tables.push(this.tableExpression());
    } while (this.match(TokenType.COMMA));
    return new exp.From(tables);
  }

  private tableExpression(): exp.Expression {
    let table: exp.Expression;

    if (this.check(TokenType.L_PAREN)) {
      this.advance();
      const query = this.selectStatement();
      this.consume(TokenType.R_PAREN, "Expected ')'");
      let alias: exp.Identifier | undefined;
      if (this.match(TokenType.AS)) {
        alias = this.identifier();
      } else if (this.check(TokenType.IDENTIFIER)) {
        alias = this.identifier();
      }
      table = new exp.Subquery(query, alias);
    } else {
      table = this.tableReference();
    }

    // Handle joins
    while (this.checkJoin()) {
      table = this.join(table);
    }

    return table;
  }

  private tableReference(): exp.Table {
    const parts: exp.Identifier[] = [];
    parts.push(this.identifier());

    while (this.match(TokenType.DOT)) {
      parts.push(this.identifier());
    }

    let alias: exp.Identifier | undefined;
    if (this.match(TokenType.AS)) {
      alias = this.identifier();
    } else if (this.check(TokenType.IDENTIFIER) && !this.checkKeyword()) {
      alias = this.identifier();
    }

    if (parts.length === 1) {
      return new exp.Table(parts[0], undefined, undefined, alias);
    } else if (parts.length === 2) {
      return new exp.Table(parts[1], parts[0], undefined, alias);
    } else {
      return new exp.Table(parts[2], parts[1], parts[0], alias);
    }
  }

  private checkJoin(): boolean {
    return (
      this.check(TokenType.JOIN) ||
      this.check(TokenType.LEFT) ||
      this.check(TokenType.RIGHT) ||
      this.check(TokenType.INNER) ||
      this.check(TokenType.OUTER)
    );
  }

  private join(left: exp.Expression): exp.Expression {
    let side: 'LEFT' | 'RIGHT' | undefined;
    let kind = 'JOIN';

    if (this.match(TokenType.LEFT)) {
      side = 'LEFT';
      if (this.match(TokenType.OUTER)) {
        kind = 'OUTER JOIN';
      }
      this.consume(TokenType.JOIN, "Expected 'JOIN'");
    } else if (this.match(TokenType.RIGHT)) {
      side = 'RIGHT';
      if (this.match(TokenType.OUTER)) {
        kind = 'OUTER JOIN';
      }
      this.consume(TokenType.JOIN, "Expected 'JOIN'");
    } else if (this.match(TokenType.INNER)) {
      this.consume(TokenType.JOIN, "Expected 'JOIN'");
      kind = 'INNER JOIN';
    } else {
      this.consume(TokenType.JOIN, "Expected 'JOIN'");
    }

    const right = this.tableReference();

    let on: exp.Expression | undefined;
    let using: exp.Expression[] | undefined;

    if (this.match(TokenType.ON)) {
      on = this.expression();
    } else if (this.match(TokenType.USING)) {
      this.consume(TokenType.L_PAREN, "Expected '('");
      using = [];
      do {
        using.push(this.identifier());
      } while (this.match(TokenType.COMMA));
      this.consume(TokenType.R_PAREN, "Expected ')'");
    }

    const joinExpr = new exp.Join(right, on, using, kind, side);

    // Create a From clause with both tables
    return new exp.From([left, joinExpr]);
  }

  private whereClause(): exp.Where {
    const condition = this.expression();
    return new exp.Where(condition);
  }

  private groupByClause(): exp.GroupBy {
    const expressions: exp.Expression[] = [];
    do {
      expressions.push(this.expression());
    } while (this.match(TokenType.COMMA));
    return new exp.GroupBy(expressions);
  }

  private havingClause(): exp.Having {
    const condition = this.expression();
    return new exp.Having(condition);
  }

  private orderByClause(): exp.OrderBy {
    const expressions: exp.Expression[] = [];
    do {
      const expr = this.expression();
      const desc = this.match(TokenType.DESC);
      if (!desc) {
        this.match(TokenType.ASC); // Optional ASC
      }
      expressions.push(new exp.Ordered(expr, desc));
    } while (this.match(TokenType.COMMA));
    return new exp.OrderBy(expressions);
  }

  private limitClause(): exp.Limit {
    const expr = this.primary();
    return new exp.Limit(expr);
  }

  private offsetClause(): exp.Offset {
    const expr = this.primary();
    return new exp.Offset(expr);
  }

  private expression(): exp.Expression {
    return this.or();
  }

  private or(): exp.Expression {
    let expr = this.and();

    while (this.match(TokenType.OR)) {
      const right = this.and();
      expr = new exp.Or(expr, right);
    }

    return expr;
  }

  private and(): exp.Expression {
    let expr = this.not();

    while (this.match(TokenType.AND)) {
      const right = this.not();
      expr = new exp.And(expr, right);
    }

    return expr;
  }

  private not(): exp.Expression {
    if (this.match(TokenType.NOT)) {
      const expr = this.not();
      return new exp.Not(expr);
    }

    return this.comparison();
  }

  private comparison(): exp.Expression {
    let expr = this.addition();

    while (true) {
      if (this.match(TokenType.GT)) {
        const right = this.addition();
        expr = new exp.GT(expr, right);
      } else if (this.match(TokenType.GTE)) {
        const right = this.addition();
        expr = new exp.GTE(expr, right);
      } else if (this.match(TokenType.LT)) {
        const right = this.addition();
        expr = new exp.LT(expr, right);
      } else if (this.match(TokenType.LTE)) {
        const right = this.addition();
        expr = new exp.LTE(expr, right);
      } else if (this.match(TokenType.EQ)) {
        const right = this.addition();
        expr = new exp.EQ(expr, right);
      } else if (this.match(TokenType.NEQ)) {
        const right = this.addition();
        expr = new exp.NEQ(expr, right);
      } else if (this.match(TokenType.IS)) {
        if (this.match(TokenType.NOT)) {
          const right = this.addition();
          expr = new exp.Not(new exp.EQ(expr, right));
        } else {
          const right = this.addition();
          expr = new exp.EQ(expr, right);
        }
      } else if (this.match(TokenType.IN)) {
        // Parse IN (subquery or list)
        this.consume(TokenType.L_PAREN, "Expected '(' after IN");
        if (this.check(TokenType.SELECT)) {
          const subquery = this.selectStatement();
          this.consume(TokenType.R_PAREN, "Expected ')'");
          // For now, just represent IN as a function call
          expr = new exp.FunctionCall(
            new exp.Identifier('IN', false),
            [expr, new exp.Paren(subquery)]
          );
        } else {
          // Parse list of values
          const values: exp.Expression[] = [];
          do {
            values.push(this.addition());
          } while (this.match(TokenType.COMMA));
          this.consume(TokenType.R_PAREN, "Expected ')'");
          expr = new exp.FunctionCall(
            new exp.Identifier('IN', false),
            [expr, ...values]
          );
        }
      } else {
        break;
      }
    }

    return expr;
  }

  private addition(): exp.Expression {
    let expr = this.multiplication();

    while (true) {
      if (this.match(TokenType.PLUS)) {
        const right = this.multiplication();
        expr = new exp.Add(expr, right);
      } else if (this.match(TokenType.DASH)) {
        const right = this.multiplication();
        expr = new exp.Sub(expr, right);
      } else {
        break;
      }
    }

    return expr;
  }

  private multiplication(): exp.Expression {
    let expr = this.unary();

    while (true) {
      if (this.match(TokenType.STAR)) {
        const right = this.unary();
        expr = new exp.Mul(expr, right);
      } else if (this.match(TokenType.SLASH)) {
        const right = this.unary();
        expr = new exp.Div(expr, right);
      } else if (this.match(TokenType.PERCENT)) {
        const right = this.unary();
        expr = new exp.Mod(expr, right);
      } else {
        break;
      }
    }

    return expr;
  }

  private unary(): exp.Expression {
    if (this.match(TokenType.DASH)) {
      const expr = this.unary();
      return new exp.Mul(new exp.Literal(-1, false), expr);
    }

    if (this.match(TokenType.PLUS)) {
      return this.unary();
    }

    return this.postfix();
  }

  private postfix(): exp.Expression {
    let expr = this.primary();

    while (true) {
      if (this.match(TokenType.DOT)) {
        const property = this.identifier();
        expr = new exp.Column(property, expr as exp.Identifier);
      } else {
        break;
      }
    }

    return expr;
  }

  private primary(): exp.Expression {
    if (this.match(TokenType.NULL)) {
      return new exp.Literal(null, false);
    }

    if (this.check(TokenType.NUMBER)) {
      const token = this.advance();
      const value = token.text.includes('.') ? parseFloat(token.text) : parseInt(token.text, 10);
      return new exp.Literal(value, false);
    }

    if (this.check(TokenType.STRING)) {
      const token = this.advance();
      return new exp.Literal(token.text, true);
    }

    if (this.match(TokenType.STAR)) {
      return new exp.Star();
    }

    if (this.match(TokenType.L_PAREN)) {
      if (this.check(TokenType.SELECT)) {
        const query = this.selectStatement();
        this.consume(TokenType.R_PAREN, "Expected ')'");
        return new exp.Subquery(query);
      }
      const expr = this.expression();
      this.consume(TokenType.R_PAREN, "Expected ')'");
      return new exp.Paren(expr);
    }

    if (this.match(TokenType.CAST)) {
      this.consume(TokenType.L_PAREN, "Expected '(' after CAST");
      const expr = this.expression();
      this.consume(TokenType.AS, "Expected AS in CAST");
      const dataType = this.dataType();
      this.consume(TokenType.R_PAREN, "Expected ')' after CAST");
      return new exp.Cast(expr, dataType);
    }

    if (this.check(TokenType.IDENTIFIER)) {
      const name = this.identifier();

      if (this.match(TokenType.L_PAREN)) {
        // Function call
        const args: exp.Expression[] = [];
        let distinct = false;

        if (this.match(TokenType.DISTINCT)) {
          distinct = true;
        }

        if (!this.check(TokenType.R_PAREN)) {
          do {
            args.push(this.expression());
          } while (this.match(TokenType.COMMA));
        }

        this.consume(TokenType.R_PAREN, "Expected ')'");
        return new exp.FunctionCall(name, args, distinct);
      }

      if (this.match(TokenType.DOT)) {
        const column = this.identifier();
        return new exp.Column(column, name);
      }

      return new exp.Column(name);
    }

    throw this.error(this.peek(), 'Expected expression');
  }

  private dataType(): exp.DataType {
    const token = this.advance();
    let type = token.text.toUpperCase();

    // Map token types to type names
    if (token.type === TokenType.INT || token.type === TokenType.INTEGER) {
      type = 'INT';
    } else if (token.type === TokenType.VARCHAR) {
      type = 'VARCHAR';
    } else if (token.type === TokenType.CHAR) {
      type = 'CHAR';
    } else if (token.type === TokenType.TEXT) {
      type = 'TEXT';
    } else if (token.type === TokenType.FLOAT) {
      type = 'FLOAT';
    } else if (token.type === TokenType.DOUBLE) {
      type = 'DOUBLE';
    } else if (token.type === TokenType.DECIMAL || token.type === TokenType.NUMERIC) {
      type = 'DECIMAL';
    } else if (token.type === TokenType.BOOLEAN || token.type === TokenType.BOOL) {
      type = 'BOOLEAN';
    } else if (token.type === TokenType.DATE) {
      type = 'DATE';
    } else if (token.type === TokenType.DATETIME) {
      type = 'DATETIME';
    } else if (token.type === TokenType.TIMESTAMP) {
      type = 'TIMESTAMP';
    } else if (token.type === TokenType.TIME) {
      type = 'TIME';
    } else if (token.type === TokenType.IDENTIFIER) {
      type = token.text.toUpperCase();
    }

    let size: number | undefined;
    let scale: number | undefined;

    if (this.match(TokenType.L_PAREN)) {
      const sizeToken = this.consume(TokenType.NUMBER, 'Expected number for type size');
      size = parseInt(sizeToken.text, 10);

      if (this.match(TokenType.COMMA)) {
        const scaleToken = this.consume(TokenType.NUMBER, 'Expected number for type scale');
        scale = parseInt(scaleToken.text, 10);
      }

      this.consume(TokenType.R_PAREN, "Expected ')'");
    }

    return new exp.DataType(type, size, scale);
  }

  private identifier(): exp.Identifier {
    const token = this.consume(TokenType.IDENTIFIER, 'Expected identifier');
    return new exp.Identifier(token.text, false);
  }

  private match(...types: TokenType[]): boolean {
    for (const type of types) {
      if (this.check(type)) {
        this.advance();
        return true;
      }
    }
    return false;
  }

  private check(type: TokenType): boolean {
    if (this.isAtEnd()) return false;
    return this.peek().type === type;
  }

  private checkKeyword(): boolean {
    const type = this.peek().type;
    return (
      type === TokenType.SELECT ||
      type === TokenType.FROM ||
      type === TokenType.WHERE ||
      type === TokenType.JOIN ||
      type === TokenType.LEFT ||
      type === TokenType.RIGHT ||
      type === TokenType.INNER ||
      type === TokenType.OUTER ||
      type === TokenType.ON ||
      type === TokenType.GROUP ||
      type === TokenType.ORDER ||
      type === TokenType.LIMIT ||
      type === TokenType.OFFSET
    );
  }

  private advance(): Token {
    if (!this.isAtEnd()) this.current++;
    return this.previous();
  }

  private isAtEnd(): boolean {
    return this.peek().type === TokenType.EOF;
  }

  private peek(): Token {
    return this.tokens[this.current];
  }

  private previous(): Token {
    return this.tokens[this.current - 1];
  }

  private consume(type: TokenType, message: string): Token {
    if (this.check(type)) return this.advance();
    throw this.error(this.peek(), message);
  }

  private error(token: Token, message: string): ParseError {
    const errorInfo = {
      description: message,
      line: token.line,
      col: token.col,
      startContext: '',
      highlight: token.text,
      endContext: '',
    };
    return new ParseError(`${message}. Line ${token.line}, Col: ${token.col}`, [errorInfo]);
  }
}
