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
        self.validate_all(
            "with all as (select 1) select * from all",
            write={
                "clickzetta": "WITH `all` AS (SELECT 1) SELECT * FROM `all`",
            },
        )
        self.validate_all(
            "with check as (select 1) select * from check",
            write={
                "clickzetta": "WITH `check` AS (SELECT 1) SELECT * FROM `check`",
            },
        )
        self.validate_all(
            "select json_extract(to, '$.billing_zone') as to_billing_zone",
            write={
                "clickzetta": "SELECT GET_JSON_OBJECT(`to`, '$.billing_zone') AS to_billing_zone",
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

    def test_functions(self):
        self.validate_all(
            "select approx_percentile(a, 0.9)",
            write={
                "clickzetta": "SELECT APPROX_PERCENTILE(a, 0.9)",
            }
        )
        self.validate_all(
            "select to_iso8601(current_timestamp)",
            write={
                "clickzetta": r"SELECT DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy-MM-dd\'T\'hh:mm:ss.SSSxxx')",
            }
        )
        self.validate_all(
            "select nullif('a',0)",
            write={
                "clickzetta": "SELECT IF('a' = 0, NULL, 'a')",
            }
        )
        self.validate_all(
            "select try(1/0), try_cast(a as bigint)",
            write={
                "clickzetta": "SELECT 1 / 0, TRY_CAST(a AS BIGINT)",
            }
        )
        self.validate_all(
            "select if(true, 'a')",
            write={
                "clickzetta": "SELECT IF(TRUE, 'a', NULL)",
            }
        )
        self.validate_all(
            "select power(10, 2)",
            write={
                "clickzetta": "SELECT POW(10, 2)",
            }
        )
        self.validate_all(
            "select last_day_of_month(a)",
            write={
                "clickzetta": "SELECT LAST_DAY(a)",
            }
        )
        self.validate_all(
            "select * from unnest(array[('a',1),('b',2),('c',3)]) as t(s,i)",
            write={
                "clickzetta": "SELECT * FROM VALUES ('a', 1), ('b', 2), ('c', 3) AS t(s, i)",
            }
        )
        self.validate_all(
            "select json_format(json '{\"a\":1,\"b\":2}')",
            write={
                "clickzetta": "SELECT TO_JSON(JSON '{\"a\":1,\"b\":2}')",
            }
        )
        self.validate_all(
            """with a as (
  with a as (
    select 1 as i
  )
  select i as j from a
)
select j from a""",
            write={
                "clickzetta": "WITH a AS (WITH a AS (SELECT 1 AS i) SELECT i AS j FROM a) SELECT j FROM a",
            }
        )
        self.validate_all(
            "select map_agg('a', date_trunc('day',now()))",
            write={
                "clickzetta": "SELECT MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT('a', DATE_TRUNC('DAY', now()))))",
            }
        )
        self.validate_all(
            "SELECT SEQUENCE(min_date, max_date, INTERVAL '1' DAY)",
            read={'presto': "select sequence(min_date,max_date,interval '1' day)"}
        )
        self.validate_all(
            "SELECT s.n FROM tmp LATERAL VIEW EXPLODE(SEQUENCE(min_date, max_date, INTERVAL '1' DAY)) s AS n",
            read={'presto': "select s.n from tmp cross join unnest(sequence(min_date,max_date, INTERVAL '1' DAY)) s (n)"},
        )
        self.validate_all(
            "SELECT j[2]",
            read={'presto': "select json_array_get(j,2)"}
        )
        self.validate_all(
            "SELECT DATEADD(HOUR, 1, CURRENT_TIMESTAMP())",
            read={'presto': "select date_add('hour', 1, now())"}
        )

    def test_read_dialect_related_function(self):
        import os

        # aes_decrypt
        os.environ['READ_DIALECT'] = 'mysql'
        self.validate_all(
            'SELECT AES_DECRYPT_MYSQL(encrypted_string, key_string)',
            read={'mysql': 'select AES_DECRYPT(encrypted_string, key_string)'}
        )
        os.environ.pop('READ_DIALECT')
        self.validate_all(
            "SELECT AES_DECRYPT(encrypted_string, key_string)",
            read={'spark': "select AES_DECRYPT(encrypted_string, key_string)"}
        )

        # date_format
        os.environ['READ_DIALECT'] = 'mysql'
        self.validate_all(
            r"SELECT DATE_FORMAT_MYSQL(CURRENT_DATE, '%x-%v')",
            read={'presto': r"select DATE_FORMAT(CURRENT_DATE, '%x-%v')"}
        )
        os.environ['READ_DIALECT'] = 'postgres'
        self.validate_all(
            r"SELECT DATE_FORMAT_PG(CURRENT_TIMESTAMP(), 'Mon-dd-%Y,%H:mi:ss')",
            read={'postgres': r"select to_char(now(), 'Mon-dd-YYYY,HH24:mi:ss')"}
        )

        # struct/tuple
        os.environ['READ_DIALECT'] = 'presto'
        self.validate_all(
            "SELECT (1 AS __c1, 'hello' AS __c2) = (2 AS __c1, 'world' AS __c2)",
            read={'presto': "select (1,'hello') = (2,'world')"}
        )
        self.validate_all(
            "SELECT * FROM VALUES (1, 'hello'), (2, 'world')",
            read={'presto': "select * from values (1,'hello'),(2,'world')"}
        )
        os.environ['READ_DIALECT'] = 'spark'
        self.validate_all(
            "SELECT (1, 'hello') = (2, 'world')",
            read={'spark': "select (1,'hello') = (2,'world')"}
        )

        # date_add
        os.environ['READ_DIALECT'] = 'presto'
        self.validate_all(
            "SELECT CURRENT_TIMESTAMP() + INTERVAL 1 HOUR * (1 + 2)",
            read={'presto': "select date_add('hour', 1+2, now())"}
        )

        os.environ.pop('READ_DIALECT')
