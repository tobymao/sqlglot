from sqlglot import parse_one, TokenType
from sqlglot.dialects.singlestore import SingleStore
from sqlglot.optimizer.qualify import qualify
from tests.dialects.test_dialect import Validator


class TestSingleStore(Validator):
    dialect = "singlestore"

    def test_basic(self):
        self.validate_identity("SELECT 1")
        self.validate_identity("SELECT * FROM users ORDER BY ALL")
        ast = parse_one(
            "SELECT id AS my_id FROM data WHERE my_id = 1 GROUP BY my_id HAVING my_id = 1",
            dialect=self.dialect,
        )
        ast = qualify(ast, dialect=self.dialect, schema={"data": {"id": "INT", "my_id": "INT"}})
        self.assertEqual(
            "SELECT `data`.`id` AS `my_id` FROM `data` AS `data` WHERE `data`.`my_id` = 1 GROUP BY `data`.`my_id` HAVING `data`.`id` = 1",
            ast.sql(dialect=self.dialect),
        )

    def test_tokenizer(self):
        self.validate_identity("SELECT e'text'")
        self.validate_identity("SELECT E'text'", "SELECT e'text'")

        assert any(
            token.token_type == TokenType.JSONB for token in SingleStore().tokenize(sql="BSON")
        )
        assert any(
            token.token_type == TokenType.GEOGRAPHY
            for token in SingleStore().tokenize(sql="GEOGRAPHYPOINT")
        )
        assert any(
            token.token_type == TokenType.COLON_GT for token in SingleStore().tokenize(sql=":>")
        )
        assert any(
            token.token_type == TokenType.NCOLON_GT for token in SingleStore().tokenize(sql="!:>")
        )
        assert any(
            token.token_type == TokenType.DCOLONDOLLAR
            for token in SingleStore().tokenize(sql="::$")
        )
        assert any(
            token.token_type == TokenType.DCOLONPERCENT
            for token in SingleStore().tokenize(sql="::%")
        )
