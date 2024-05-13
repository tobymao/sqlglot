from sqlglot import UnsupportedError, exp, parse_one
from sqlglot.helper import logger as helper_logger
from tests.dialects.test_dialect import Validator


class TestClickzetta(Validator):
    dialect = "clickzetta"

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
