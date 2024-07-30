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

    def test_dml(self):
        self.validate_all(
            "INSERT INTO a.b.c (`x`, `y`, `z`) VALUES (1, 'hello', CAST('2024-07-23 15:17:12' AS TIMESTAMP))",
            read={'presto': "insert into a.b.c (\"x\", \"y\", \"z\") values (1, 'hello', timestamp '2024-07-23 15:17:12')"}
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
        self.validate_all(
            "SELECT i FROM EXPLODE(SEQUENCE(-3, 0)) AS t(i)",
            read={'presto': "select i from unnest(sequence(-3,0)) as t(i)"}
        )
        self.validate_all(
            "SELECT * FROM EXPLODE(SEQUENCE(-3, 0))",
            read={'presto': "select * from unnest(sequence(-3,0))"}
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP(a, 'yyyy-MM-dd\\\'T\\\'HH:mm:ss\\\'Z\\\'')",
            read={'presto': "select parse_datetime(a, 'yyyy-MM-dd''T''HH:mm:ss''Z''')"}
        )
        self.validate_all(
            "SELECT CAST('2020-01-01T00:00:00.000Z' AS TIMESTAMP)",
            read={'presto': "select from_iso8601_timestamp('2020-01-01T00:00:00.000Z')"}
        )
        self.validate_all(
            "SELECT * FROM t GROUP BY GROUPING SETS ((a), (b, c))",
            read={'presto': "select * from t group by grouping sets ((a), (b,c))"}
        )
        self.validate_all(
            "SELECT DAYOFWEEK(d), DAYOFWEEK(d), DAYOFYEAR(d), DAYOFYEAR(d), YEAROFWEEK(d), YEAROFWEEK(d)",
            read={'presto': "select dow(d), day_of_week(d), doy(d), day_of_year(d), yow(d), year_of_week(d)"}
        )
        self.validate_all(
            "SELECT REGEXP_EXTRACT('aaaa', 'a|b|c')",
            read={'spark': "select regexp_extract('aaaa', 'a|b|c')"}
        )
        self.validate_all(
            "SELECT LOG10(10)",
            read={'presto': "select log10(10)"}
        )
        self.validate_all(
            "SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())",
            read={'presto': "select now() at time zone 'UTC'"}
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
            "SELECT TIMESTAMP_OR_DATE_ADD(LOWER('hour'), 1 + 2, CURRENT_TIMESTAMP())",
            read={'presto': "select date_add(lower('hour'), 1+2, now())"}
        )
        self.validate_all(
            "SELECT TIMESTAMP_OR_DATE_ADD('HOUR', 1 + 2, CURRENT_TIMESTAMP())",
            read={'presto': "select date_add('hour', 1+2, now())"}
        )

        # regexp_extract
        os.environ['READ_DIALECT'] = 'presto'
        self.validate_all(
            "SELECT REGEXP_EXTRACT('aaaa', 'a|b|c', 0)",
            read={'presto': "select regexp_extract('aaaa', 'a|b|c')"}
        )
        self.validate_all(
            "SELECT REGEXP_EXTRACT('aaaa', 'a|b|c', 1)",
            read={'presto': "select regexp_extract('aaaa', 'a|b|c', 1)"}
        )

        os.environ.pop('READ_DIALECT')
