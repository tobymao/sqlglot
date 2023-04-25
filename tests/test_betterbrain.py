import unittest

from sqlglot import Parser, exp, parse, parse_one, sql_suggest
from sqlglot.betterbrain import SQLSuggestion, TableIdentifier
from sqlglot.dialects import Postgres
from sqlglot.errors import ErrorLevel, ParseError
from tests.helpers import assert_logger_contains
from sqlglot.tokens import TokenType

EXPECTED_TOKEN_SUGGESTIONS_FOR_WHERE_COL_CURSOR = [Postgres.Tokenizer.TOKEN_TO_KEYWORD[token] for token in [TokenType.INTERVAL,
             TokenType.NOTNULL, 
             TokenType.COLUMN, 
             TokenType.AT_TIME_ZONE, 
             TokenType.ISNULL, 
             TokenType.NOT, 
             TokenType.IS]]

EXPECTED_TOKEN_SUGGESTIONS_FOR_COL_CURSOR = EXPECTED_TOKEN_SUGGESTIONS_FOR_WHERE_COL_CURSOR

EXPECTED_TOKEN_SUGGESTIONS_FOR_FIRST_COL_CURSOR = [Postgres.Tokenizer.TOKEN_TO_KEYWORD[token] for token in [TokenType.ALL, TokenType.FETCH, TokenType.DISTINCT, TokenType.HINT]] + EXPECTED_TOKEN_SUGGESTIONS_FOR_COL_CURSOR

EXPECTED_TOKEN_SUGGESTIONS_FOR_TABLE_CURSOR = [Postgres.Tokenizer.TOKEN_TO_KEYWORD[token] for token in [
            TokenType.LATERAL,
            TokenType.UNNEST,
            TokenType.OUTER,
            TokenType.TABLE,
            TokenType.WITH,
            TokenType.CROSS,
            TokenType.VALUES,
]]

EXPECTED_TOKEN_SUGGESTIONS_FOR_POST_FROM_CURSOR = [Postgres.Tokenizer.TOKEN_TO_KEYWORD[token] for token in [TokenType.DISTRIBUTE_BY,
             TokenType.CLUSTER_BY, 
             TokenType.FETCH,
             TokenType.NATURAL,
             TokenType.QUALIFY,
             TokenType.TABLE_SAMPLE,
             TokenType.JOIN,
             TokenType.LIMIT,
             TokenType.LATERAL,
             TokenType.WHERE,
             TokenType.CROSS,
             TokenType.WINDOW,
             TokenType.ORDER_BY,
             TokenType.OUTER,
             TokenType.HAVING,
             TokenType.SORT_BY,
             TokenType.GROUP_BY]]


EXPECTED_TOKEN_SUGGESTIONS_FOR_POST_SELECT_CURSOR = [Postgres.Tokenizer.TOKEN_TO_KEYWORD[token] for token in [
            TokenType.INTO,
            TokenType.FROM,
]]

class TestBetterBrain(unittest.TestCase):
    def test_not_working(self):
        with self.assertRaises(ParseError) as ctx:
            sql_suggest("select * from <>")
        with self.assertRaises(ParseError) as ctx:
            sql_suggest("select")
        with self.assertRaises(ParseError) as ctx:
            sql_suggest("select col_1, from table_1", 'postgres')
        with self.assertRaises(ParseError) as ctx:
            sql_suggest("select col_1 where << from table_1", 'postgres')

    def test_individual(self):
        suggestion = sql_suggest("select asian, abbb from table_1 as tototot join table_2 as tatata join table_3 as momomo where <<", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_WHERE_COL_CURSOR, 
             table_ids=[TableIdentifier(name="table_1", alias="tototot"), 
                        TableIdentifier(name="table_2", alias="tatata"), 
                        TableIdentifier(name="table_3", alias="momomo")]))

        suggestion = sql_suggest("select asian, << from table_1 as tototot join table_2 as tatata join table_3 as momomo", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_COL_CURSOR, 
             table_ids=[TableIdentifier(name="table_1", alias="tototot"), 
                        TableIdentifier(name="table_2", alias="tatata"), 
                        TableIdentifier(name="table_3", alias="momomo")]))
        
        suggestion = sql_suggest("select col_1, sfd<< from table_1", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_COL_CURSOR, 
             table_ids=[TableIdentifier(name="table_1")]))
        
        suggestion = sql_suggest("select << from table_1", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_FIRST_COL_CURSOR, 
             table_ids=[TableIdentifier(name="table_1")]))

        suggestion = sql_suggest("select asian, a<< from table_1", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_COL_CURSOR, 
             table_ids=[TableIdentifier(name="table_1")]))
        
        suggestion = sql_suggest("select asian from table_2222 <<", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=EXPECTED_TOKEN_SUGGESTIONS_FOR_POST_FROM_CURSOR))

        suggestion = sql_suggest("select asian from <<", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_TABLE_CURSOR))
        

        suggestion = sql_suggest("select asian <<", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_POST_SELECT_CURSOR))
        
        suggestion = sql_suggest("select col_1 sfd<< a a a a a a a a a a", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_POST_SELECT_CURSOR))
        

        suggestion = sql_suggest("select col_1 sfd<< limit where from table_1", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_POST_SELECT_CURSOR))
        

        suggestion = sql_suggest("select * from <<", 'postgres')
        self.assertEqual(suggestion, SQLSuggestion(suggestions=                                    
            EXPECTED_TOKEN_SUGGESTIONS_FOR_TABLE_CURSOR))