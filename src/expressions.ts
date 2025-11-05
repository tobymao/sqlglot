export abstract class Expression {
  public comments: string[] = [];

  abstract sql(dialect?: string): string;

  find<T extends Expression>(type: new (...args: any[]) => T): T | null {
    if (this instanceof type) {
      return this as T;
    }
    for (const child of this.children()) {
      const found = child.find(type);
      if (found) return found;
    }
    return null;
  }

  findAll<T extends Expression>(type: new (...args: any[]) => T): T[] {
    const results: T[] = [];
    if (this instanceof type) {
      results.push(this as T);
    }
    for (const child of this.children()) {
      results.push(...child.findAll(type));
    }
    return results;
  }

  transform(fn: (node: Expression) => Expression): Expression {
    const transformed = fn(this);
    if (transformed !== this) {
      return transformed;
    }
    return this.transformChildren(fn);
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    return this;
  }

  protected children(): Expression[] {
    return [];
  }
}

export class Identifier extends Expression {
  constructor(public readonly name: string, public readonly quoted: boolean = false) {
    super();
  }

  sql(dialect?: string): string {
    if (this.quoted) {
      return `"${this.name}"`;
    }
    return this.name;
  }

  protected children(): Expression[] {
    return [];
  }
}

export class Literal extends Expression {
  constructor(public readonly value: string | number | boolean | null, public readonly isString: boolean = false) {
    super();
  }

  sql(dialect?: string): string {
    if (this.value === null) {
      return 'NULL';
    }
    if (this.isString || typeof this.value === 'string') {
      return `'${String(this.value).replace(/'/g, "''")}'`;
    }
    return String(this.value);
  }

  protected children(): Expression[] {
    return [];
  }
}

export class Column extends Expression {
  constructor(
    public readonly name: Identifier,
    public readonly table?: Identifier
  ) {
    super();
  }

  sql(dialect?: string): string {
    if (this.table) {
      return `${this.table.sql(dialect)}.${this.name.sql(dialect)}`;
    }
    return this.name.sql(dialect);
  }

  get aliasOrName(): string {
    return this.name.name;
  }

  protected children(): Expression[] {
    return this.table ? [this.name, this.table] : [this.name];
  }
}

export class Table extends Expression {
  constructor(
    public readonly name: Identifier,
    public readonly db?: Identifier,
    public readonly catalog?: Identifier,
    public readonly alias?: Identifier
  ) {
    super();
  }

  sql(dialect?: string): string {
    let result = '';
    if (this.catalog) {
      result += `${this.catalog.sql(dialect)}.`;
    }
    if (this.db) {
      result += `${this.db.sql(dialect)}.`;
    }
    result += this.name.sql(dialect);
    if (this.alias) {
      result += ` AS ${this.alias.sql(dialect)}`;
    }
    return result;
  }

  protected children(): Expression[] {
    const children: Expression[] = [this.name];
    if (this.db) children.push(this.db);
    if (this.catalog) children.push(this.catalog);
    if (this.alias) children.push(this.alias);
    return children;
  }
}

export class Alias extends Expression {
  constructor(
    public readonly expression: Expression,
    public readonly alias: Identifier
  ) {
    super();
  }

  sql(dialect?: string): string {
    return `${this.expression.sql(dialect)} AS ${this.alias.sql(dialect)}`;
  }

  protected children(): Expression[] {
    return [this.expression, this.alias];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Alias(newExpr, this.alias);
    }
    return this;
  }
}

export class Star extends Expression {
  sql(dialect?: string): string {
    return '*';
  }

  protected children(): Expression[] {
    return [];
  }
}

export abstract class BinaryExpression extends Expression {
  constructor(
    public readonly left: Expression,
    public readonly right: Expression,
    public readonly operator: string
  ) {
    super();
  }

  sql(dialect?: string): string {
    return `${this.left.sql(dialect)} ${this.operator} ${this.right.sql(dialect)}`;
  }

  protected children(): Expression[] {
    return [this.left, this.right];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newLeft = this.left.transform(fn);
    const newRight = this.right.transform(fn);
    if (newLeft !== this.left || newRight !== this.right) {
      return new (this.constructor as any)(newLeft, newRight, this.operator);
    }
    return this;
  }
}

export class Add extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '+');
  }
}

export class Sub extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '-');
  }
}

export class Mul extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '*');
  }
}

export class Div extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '/');
  }
}

export class Mod extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '%');
  }
}

export class EQ extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '=');
  }
}

export class NEQ extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '<>');
  }
}

export class LT extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '<');
  }
}

export class LTE extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '<=');
  }
}

export class GT extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '>');
  }
}

export class GTE extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, '>=');
  }
}

export class And extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, 'AND');
  }
}

export class Or extends BinaryExpression {
  constructor(left: Expression, right: Expression) {
    super(left, right, 'OR');
  }
}

export class Not extends Expression {
  constructor(public readonly expression: Expression) {
    super();
  }

  sql(dialect?: string): string {
    return `NOT ${this.expression.sql(dialect)}`;
  }

  protected children(): Expression[] {
    return [this.expression];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Not(newExpr);
    }
    return this;
  }
}

export class Paren extends Expression {
  constructor(public readonly expression: Expression) {
    super();
  }

  sql(dialect?: string): string {
    return `(${this.expression.sql(dialect)})`;
  }

  protected children(): Expression[] {
    return [this.expression];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Paren(newExpr);
    }
    return this;
  }
}

export class FunctionCall extends Expression {
  constructor(
    public readonly name: Identifier,
    public readonly args: Expression[],
    public readonly distinct: boolean = false
  ) {
    super();
  }

  sql(dialect?: string): string {
    const distinctStr = this.distinct ? 'DISTINCT ' : '';
    const argsStr = this.args.map(arg => arg.sql(dialect)).join(', ');
    return `${this.name.sql(dialect)}(${distinctStr}${argsStr})`;
  }

  protected children(): Expression[] {
    return [this.name, ...this.args];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newArgs = this.args.map(arg => arg.transform(fn));
    if (newArgs.some((arg, i) => arg !== this.args[i])) {
      return new FunctionCall(this.name, newArgs, this.distinct);
    }
    return this;
  }
}

export class Cast extends Expression {
  constructor(
    public readonly expression: Expression,
    public readonly dataType: DataType
  ) {
    super();
  }

  sql(dialect?: string): string {
    return `CAST(${this.expression.sql(dialect)} AS ${this.dataType.sql(dialect)})`;
  }

  protected children(): Expression[] {
    return [this.expression, this.dataType];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Cast(newExpr, this.dataType);
    }
    return this;
  }
}

export class DataType extends Expression {
  constructor(
    public readonly type: string,
    public readonly size?: number,
    public readonly scale?: number
  ) {
    super();
  }

  sql(dialect?: string): string {
    let result = this.type;
    if (this.size !== undefined) {
      result += `(${this.size}`;
      if (this.scale !== undefined) {
        result += `, ${this.scale}`;
      }
      result += ')';
    }
    return result;
  }

  protected children(): Expression[] {
    return [];
  }
}

export class Select extends Expression {
  constructor(
    public readonly expressions: Expression[],
    public readonly from?: From,
    public readonly where?: Where,
    public readonly groupBy?: GroupBy,
    public readonly having?: Having,
    public readonly orderBy?: OrderBy,
    public readonly limit?: Limit,
    public readonly offset?: Offset,
    public readonly distinct: boolean = false
  ) {
    super();
  }

  sql(dialect?: string): string {
    let parts: string[] = [];

    const distinctStr = this.distinct ? 'DISTINCT ' : '';
    const exprs = this.expressions.map(e => e.sql(dialect)).join(', ');
    parts.push(`SELECT ${distinctStr}${exprs}`);

    if (this.from) parts.push(this.from.sql(dialect));
    if (this.where) parts.push(this.where.sql(dialect));
    if (this.groupBy) parts.push(this.groupBy.sql(dialect));
    if (this.having) parts.push(this.having.sql(dialect));
    if (this.orderBy) parts.push(this.orderBy.sql(dialect));
    if (this.limit) parts.push(this.limit.sql(dialect));
    if (this.offset) parts.push(this.offset.sql(dialect));

    return parts.join(' ');
  }

  protected children(): Expression[] {
    const children: Expression[] = [...this.expressions];
    if (this.from) children.push(this.from);
    if (this.where) children.push(this.where);
    if (this.groupBy) children.push(this.groupBy);
    if (this.having) children.push(this.having);
    if (this.orderBy) children.push(this.orderBy);
    if (this.limit) children.push(this.limit);
    if (this.offset) children.push(this.offset);
    return children;
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpressions = this.expressions.map(e => e.transform(fn));
    const newFrom = this.from ? this.from.transform(fn) as From : undefined;
    const newWhere = this.where ? this.where.transform(fn) as Where : undefined;
    const newGroupBy = this.groupBy ? this.groupBy.transform(fn) as GroupBy : undefined;
    const newHaving = this.having ? this.having.transform(fn) as Having : undefined;
    const newOrderBy = this.orderBy ? this.orderBy.transform(fn) as OrderBy : undefined;
    const newLimit = this.limit ? this.limit.transform(fn) as Limit : undefined;
    const newOffset = this.offset ? this.offset.transform(fn) as Offset : undefined;

    if (
      newExpressions.some((e, i) => e !== this.expressions[i]) ||
      newFrom !== this.from ||
      newWhere !== this.where ||
      newGroupBy !== this.groupBy ||
      newHaving !== this.having ||
      newOrderBy !== this.orderBy ||
      newLimit !== this.limit ||
      newOffset !== this.offset
    ) {
      return new Select(
        newExpressions,
        newFrom,
        newWhere,
        newGroupBy,
        newHaving,
        newOrderBy,
        newLimit,
        newOffset,
        this.distinct
      );
    }
    return this;
  }
}

export class From extends Expression {
  constructor(public readonly expressions: Expression[]) {
    super();
  }

  sql(dialect?: string): string {
    return `FROM ${this.expressions.map(e => e.sql(dialect)).join(', ')}`;
  }

  protected children(): Expression[] {
    return this.expressions;
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpressions = this.expressions.map(e => e.transform(fn));
    if (newExpressions.some((e, i) => e !== this.expressions[i])) {
      return new From(newExpressions);
    }
    return this;
  }
}

export class Where extends Expression {
  constructor(public readonly expression: Expression) {
    super();
  }

  sql(dialect?: string): string {
    return `WHERE ${this.expression.sql(dialect)}`;
  }

  protected children(): Expression[] {
    return [this.expression];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Where(newExpr);
    }
    return this;
  }
}

export class GroupBy extends Expression {
  constructor(public readonly expressions: Expression[]) {
    super();
  }

  sql(dialect?: string): string {
    return `GROUP BY ${this.expressions.map(e => e.sql(dialect)).join(', ')}`;
  }

  protected children(): Expression[] {
    return this.expressions;
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpressions = this.expressions.map(e => e.transform(fn));
    if (newExpressions.some((e, i) => e !== this.expressions[i])) {
      return new GroupBy(newExpressions);
    }
    return this;
  }
}

export class Having extends Expression {
  constructor(public readonly expression: Expression) {
    super();
  }

  sql(dialect?: string): string {
    return `HAVING ${this.expression.sql(dialect)}`;
  }

  protected children(): Expression[] {
    return [this.expression];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Having(newExpr);
    }
    return this;
  }
}

export class OrderBy extends Expression {
  constructor(public readonly expressions: Expression[]) {
    super();
  }

  sql(dialect?: string): string {
    return `ORDER BY ${this.expressions.map(e => e.sql(dialect)).join(', ')}`;
  }

  protected children(): Expression[] {
    return this.expressions;
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpressions = this.expressions.map(e => e.transform(fn));
    if (newExpressions.some((e, i) => e !== this.expressions[i])) {
      return new OrderBy(newExpressions);
    }
    return this;
  }
}

export class Ordered extends Expression {
  constructor(
    public readonly expression: Expression,
    public readonly desc: boolean = false
  ) {
    super();
  }

  sql(dialect?: string): string {
    return `${this.expression.sql(dialect)}${this.desc ? ' DESC' : ' ASC'}`;
  }

  protected children(): Expression[] {
    return [this.expression];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Ordered(newExpr, this.desc);
    }
    return this;
  }
}

export class Limit extends Expression {
  constructor(public readonly expression: Expression) {
    super();
  }

  sql(dialect?: string): string {
    return `LIMIT ${this.expression.sql(dialect)}`;
  }

  protected children(): Expression[] {
    return [this.expression];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Limit(newExpr);
    }
    return this;
  }
}

export class Offset extends Expression {
  constructor(public readonly expression: Expression) {
    super();
  }

  sql(dialect?: string): string {
    return `OFFSET ${this.expression.sql(dialect)}`;
  }

  protected children(): Expression[] {
    return [this.expression];
  }

  protected transformChildren(fn: (node: Expression) => Expression): Expression {
    const newExpr = this.expression.transform(fn);
    if (newExpr !== this.expression) {
      return new Offset(newExpr);
    }
    return this;
  }
}

export class Join extends Expression {
  constructor(
    public readonly expression: Expression,
    public readonly on?: Expression,
    public readonly using?: Expression[],
    public readonly kind: string = 'JOIN',
    public readonly side?: 'LEFT' | 'RIGHT'
  ) {
    super();
  }

  sql(dialect?: string): string {
    let result = '';
    if (this.side) {
      result += `${this.side} `;
    }
    result += `${this.kind} ${this.expression.sql(dialect)}`;
    if (this.on) {
      result += ` ON ${this.on.sql(dialect)}`;
    }
    if (this.using) {
      result += ` USING (${this.using.map(e => e.sql(dialect)).join(', ')})`;
    }
    return result;
  }

  protected children(): Expression[] {
    const children: Expression[] = [this.expression];
    if (this.on) children.push(this.on);
    if (this.using) children.push(...this.using);
    return children;
  }
}

export class Union extends Expression {
  constructor(
    public readonly left: Expression,
    public readonly right: Expression,
    public readonly all: boolean = false
  ) {
    super();
  }

  sql(dialect?: string): string {
    const allStr = this.all ? ' ALL' : '';
    return `${this.left.sql(dialect)} UNION${allStr} ${this.right.sql(dialect)}`;
  }

  protected children(): Expression[] {
    return [this.left, this.right];
  }
}

export class Subquery extends Expression {
  constructor(
    public readonly query: Expression,
    public readonly alias?: Identifier
  ) {
    super();
  }

  sql(dialect?: string): string {
    let result = `(${this.query.sql(dialect)})`;
    if (this.alias) {
      result += ` AS ${this.alias.sql(dialect)}`;
    }
    return result;
  }

  protected children(): Expression[] {
    const children: Expression[] = [this.query];
    if (this.alias) children.push(this.alias);
    return children;
  }
}
