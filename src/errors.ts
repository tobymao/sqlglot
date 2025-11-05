export class SqlglotError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SqlglotError';
  }
}

export class TokenError extends SqlglotError {
  constructor(message: string) {
    super(message);
    this.name = 'TokenError';
  }
}

export class ParseError extends SqlglotError {
  public readonly errors: Array<{
    description: string;
    line: number;
    col: number;
    startContext: string;
    highlight: string;
    endContext: string;
  }>;

  constructor(message: string, errors: ParseError['errors'] = []) {
    super(message);
    this.name = 'ParseError';
    this.errors = errors;
  }
}

export class UnsupportedError extends SqlglotError {
  constructor(message: string) {
    super(message);
    this.name = 'UnsupportedError';
  }
}

export enum ErrorLevel {
  IGNORE = 'IGNORE',
  WARN = 'WARN',
  RAISE = 'RAISE',
  IMMEDIATE = 'IMMEDIATE',
}
