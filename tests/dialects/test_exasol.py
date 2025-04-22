from datetime import date, datetime, timezone
from sqlglot import exp, parse_one
from sqlglot.dialects.exasol import Tokenizer, ExasolTokenType
from sqlglot.expressions import convert
from sqlglot.optimizer import traverse_scope
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.tokens import TokenType
from tests.dialects.test_dialect import Validator
from sqlglot.errors import ErrorLevel



class TestExasol(Validator):
    dialect = "exasol"
    
    def test_tokenizer_hashtype_variants(self):
        sql = "HASHTYPE(256 BIT), HASHTYPE(64 BYTE)"
        tokenizer = Tokenizer()
        tokens = tokenizer.tokenize(sql)
        token_data = [(token.token_type, token.text) for token in tokens]
        expected = [
            (ExasolTokenType.HASHTYPE, "HASHTYPE"),
            (TokenType.L_PAREN, "("),
            (TokenType.NUMBER, "256"),
            (TokenType.BIT, "BIT"),
            (TokenType.R_PAREN, ")"),
            (TokenType.COMMA, ","),
            (ExasolTokenType.HASHTYPE, "HASHTYPE"),
            (TokenType.L_PAREN, "("),
            (TokenType.NUMBER, "64"),
            (ExasolTokenType.BYTE, "BYTE"),
            (TokenType.R_PAREN, ")"),
        ]
        self.assertEqual(token_data, expected)

    def test_tokenizer_timestamp_with_local_time_zone(self):
        sql = "TIMESTAMP(3) WITH LOCAL TIME ZONE"
        tokenizer = Tokenizer()
        tokens = tokenizer.tokenize(sql)
        token_data = [(token.token_type, token.text) for token in tokens]
        expected = [
            (TokenType.TIMESTAMP, "TIMESTAMP"),
            (TokenType.L_PAREN, "("),
            (TokenType.NUMBER, "3"),
            (TokenType.R_PAREN, ")"),
            (ExasolTokenType.WITH_LOCAL_TIME_ZONE, "WITH LOCAL TIME ZONE"),
        ]
        self.assertEqual(token_data, expected)

    def test_tokenizer_interval_year_to_month(self):
        sql = "INTERVAL YEAR(2) TO MONTH"
        tokenizer = Tokenizer()
        tokens = tokenizer.tokenize(sql)
        token_data = [(token.token_type, token.text) for token in tokens]

        expected = [
            (TokenType.INTERVAL, "INTERVAL"),
            (TokenType.YEAR, "YEAR"),
            (TokenType.L_PAREN, "("),
            (TokenType.NUMBER, "2"),
            (TokenType.R_PAREN, ")"),
            (ExasolTokenType.TO, "TO"),
            (ExasolTokenType.MONTH, "MONTH"),
        ]

        self.assertEqual(token_data, expected)
    
    def test_tokenizer_interval_day_to_second(self):
        sql = "INTERVAL DAY(3) TO SECOND(2)"
        tokenizer = Tokenizer()
        tokens = tokenizer.tokenize(sql)
        token_data = [(token.token_type, token.text) for token in tokens]

        expected = [
            (TokenType.INTERVAL, "INTERVAL"),
            (ExasolTokenType.DAY, "DAY"),
            (TokenType.L_PAREN, "("),
            (TokenType.NUMBER, "3"),
            (TokenType.R_PAREN, ")"),
            (ExasolTokenType.TO, "TO"),
            (ExasolTokenType.SECOND, "SECOND"),
            (TokenType.L_PAREN, "("),
            (TokenType.NUMBER, "2"),
            (TokenType.R_PAREN, ")"),
        ]

        self.assertEqual(token_data, expected)

   