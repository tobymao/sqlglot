from __future__ import annotations

import typing as t
from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType

## CUSTOM FUNCTIONS
def diagnostics(val):
    """
    Generate a diagnostic message for a given expression.
    """
    print("diagnostic")
    print(val)
    return val
    

## TOKENIZER
class DremioTokenizer(tokens.Tokenizer):
    print("Dremio tokenizer initialized")
    """Tokenizer for Dremio SQL."""
    KEYWORDS = {
        **tokens.Tokenizer.KEYWORDS,
        "WITH": TokenType.WITH,
        "SELECT": TokenType.SELECT,
        "ALL": TokenType.ALL,
        "DISTINCT": TokenType.DISTINCT,
        "FROM": TokenType.FROM,
        "WHERE": TokenType.WHERE,
        "JOIN": TokenType.JOIN,
        "ON": TokenType.ON,
        "GROUP BY": TokenType.GROUP_BY,
        "ORDER BY": TokenType.ORDER_BY,
        "HAVING": TokenType.HAVING,
        "LIMIT": TokenType.LIMIT,
        "OFFSET": TokenType.OFFSET,
        "QUALIFY": TokenType.QUALIFY,
        "INSERT INTO": TokenType.INSERT,
        "VALUES": TokenType.VALUES,
        "UPDATE": TokenType.UPDATE,
        "SET": TokenType.SET,
        "DELETE FROM": TokenType.DELETE,
        "CREATE TABLE": TokenType.CREATE,
        "ALTER TABLE": TokenType.ALTER,
        "DROP TABLE": TokenType.DROP,
        "PIVOT": TokenType.PIVOT,
        "UNPIVOT": TokenType.UNPIVOT,
        "UNNEST": TokenType.UNNEST,
    }

## PARSER
class DremioParser(parser.Parser):
    print("Dremio parser initialized")
    """Parser for Dremio SQL."""
    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "UNNEST": exp.Unnest.from_arg_list,
        "LENGTH": exp.Length.from_arg_list,
        "SUBSTRING": exp.Substring.from_arg_list,
        "UPPER": exp.Upper.from_arg_list,
        "LOWER": exp.Lower.from_arg_list,
        "TRIM": exp.Trim.from_arg_list,
        "ROUND": exp.Round.from_arg_list,
        "CEIL": exp.Ceil.from_arg_list,
        "FLOOR": exp.Floor.from_arg_list,
        "COALESCE": exp.Coalesce.from_arg_list,
        "ABS": exp.Abs.from_arg_list,
        "POWER": exp.Pow.from_arg_list,
        "SQRT": exp.Sqrt.from_arg_list,
        "LOG": exp.Ln.from_arg_list,  # LOG(x) -> Natural log
        "EXP": exp.Exp.from_arg_list,
        "SIGN": exp.Sign.from_arg_list,
        "EXTRACT": exp.Extract.from_arg_list,
        "DATE_ADD": lambda args: exp.DateAdd(
            this=args[0], 
            expression=exp.Interval(
                this=exp.Literal.number(int(args[1].this)),  
                unit=exp.Var(this="DAY")
            )
        ),
        "DATE_SUB": lambda args: exp.DateSub(
            this=args[0], 
            expression=exp.Interval(
                this=exp.Literal.number(int(args[1].this)), 
                unit=exp.Var(this="DAY")
            )
        ),
        "COUNT": exp.Count.from_arg_list,
        "SUM": exp.Sum.from_arg_list,
        "AVG": exp.Avg.from_arg_list,
        "MIN": exp.Min.from_arg_list,
        "MAX": exp.Max.from_arg_list,
        "VARIANCE": exp.Variance.from_arg_list,
        "STDDEV": exp.Stddev.from_arg_list,
        "NULLIF": exp.Nullif.from_arg_list,
    }
    

class DremioGenerator(generator.Generator):
    print("Dremio generator initialized")
    """SQL generator for Dremio."""
    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.DataType: lambda self, e: e.this.value if isinstance(e.this, exp.DataType.Type) else e.this,
        exp.Unnest: lambda self, e: f"UNNEST({self.sql(e, 'this')})",
        exp.Pow: lambda self, e: f"POWER({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.Ln: lambda self, e: f"LOG({self.sql(e, 'this')})",  # LOG(x) -> Natural log
        exp.Exp: lambda self, e: f"EXP({self.sql(e, 'this')})",
        exp.DateAdd: lambda self, e: diagnostics(f"DATE_ADD({self.sql(e, 'this')}, INTERVAL {self.sql(e, 'expression.this')} DAY)"),
        exp.DateSub: lambda self, e: diagnostics(f"DATE_SUB({self.sql(e, 'this')}, INTERVAL {self.sql(e, 'expression.this')} DAY)"),
    }
    def sql(self, expression, *args, **kwargs):
        return super().sql(expression, *args, **kwargs)
    
    def generate(self, expression, copy=True, **opts):
        print("🚨 DremioGenerator is generating!")
        return super().generate(expression, copy=copy, **opts)



class Dremio(Dialect):
    print("Dremio dialect initialized")
    """Dremio dialect for SQLGlot."""
    TOKENIZER = DremioTokenizer
    PARSER = DremioParser
    GENERATOR = DremioGenerator
    
    @classmethod
    def generator(cls, **kwargs):
        return DremioGenerator(**kwargs)


