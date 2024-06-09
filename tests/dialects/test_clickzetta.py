from sqlglot import UnsupportedError, exp, parse_one
from sqlglot.helper import logger as helper_logger
from tests.dialects.test_dialect import Validator


class TestClickzetta(Validator):
    dialect = "clickzetta"

    def test_reserved_keyword(self):
        self.validate_all(
            "SELECT user.id from t as user",
            write={
                "clickzetta": "SELECT `user`.id FROM t AS `user`",
            },
        )

    def test_ddl_types(self):
        self.validate_all(
            'CREATE TABLE foo (a INT)',
            read={'postgres': 'create table foo (a serial)'}
        )
        self.validate_all(
            'CREATE TABLE foo (a BIGINT)',
            read={'postgres': 'create table foo (a bigserial)'}
        )
        self.validate_all(
            'CREATE TABLE foo (a DECIMAL)',
            read={'mysql': 'create table foo (a decimal unsigned)'}
        )
        self.validate_all(
            'CREATE TABLE foo (a STRING)',
            read={'mysql': 'create table foo (a longtext)'}
        )
        self.validate_all(
            'CREATE TABLE foo (a STRING)',
            read={'postgres': 'create table foo (a enum)'}
        )

    def functions(self):
        self.validate_all(
            "select approx_percentile(a, 0.9)",
            write={
                "clickzetta": "SELECT APPROX_PERCENTILE(a, 0.9)",
            }
        )