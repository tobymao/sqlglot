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
        # self.validate_all(
        #     "SELECT j[2]",
        #     read={'presto': "select json_array_get(j,2)"}
        # )
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
        self.validate_all(
            "SELECT DAYOFWEEK(TO_DATE(d))",
            read={'spark': "select dayofweek(d)"}
        )
        self.validate_all(
            "SELECT GROUPING_ID(a, b), GROUPING_ID(a, c) FROM foo GROUP BY GROUPING SETS ((a, b), (a, c))",
            read={'presto': "select grouping(a,b), grouping(a,c) from foo group by grouping sets ((a,b),(a,c))"}
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
        self.validate_all(
            "SELECT CAST(DATE_FORMAT_MYSQL(TIMESTAMP('2024-08-22 14:53:12'), '%Y-%m-%d') AS DATE)",
            read={'presto': r"""SELECT CAST(date_format(timestamp('2024-08-22 14:53:12'), '%Y-%m-%d') AS DATE);"""}
        )
        self.validate_all(
            "SELECT TIMESTAMP('2024-08-22 14:53:12'), DATE_FORMAT_MYSQL(TIMESTAMP('2024-08-22 "
            "14:53:12'), '%Y %M') /* expected: 2024 August */, DATE_FORMAT_MYSQL(TIMESTAMP('2024-08"
            "-22 14:53:12'), '%e') /* expected: 22 */, DATE_FORMAT_MYSQL(TIMESTAMP('2024-08-22 14:5"
            "3:12'), '%H %i %s') /* expected: 14 53 12 */",
            read={'presto': r"""select timestamp('2024-08-22 14:53:12')
                    , date_format(timestamp('2024-08-22 14:53:12'), '%Y %M') -- expected: 2024 August
                    , date_format(timestamp('2024-08-22 14:53:12'), '%e') -- expected: 22
                    , date_format(timestamp('2024-08-22 14:53:12'), '%H %i %s') -- expected: 14 53 12"""}
        )
        os.environ['READ_DIALECT'] = 'postgres'
        self.validate_all(
            r"SELECT DATE_FORMAT_PG(CURRENT_TIMESTAMP(), 'Mon-dd-YYYY,HH24:mi:ss')",
            read={'postgres': r"select to_char(now(), 'Mon-dd-YYYY,HH24:mi:ss')"}
        )
        self.validate_all(
            r"SELECT DATE_FORMAT_PG(CURRENT_TIMESTAMP(), 'YYYY-MM-DD')",
            read={'postgres': r"SELECT to_char(now(), 'YYYY-MM-DD');"}
        )
        self.validate_all(
            r"""SELECT DATE_FORMAT_PG(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')""",
            read={'postgres': r"""SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS');"""}
        )
        self.validate_all(
            r"""SELECT DATE_FORMAT_PG(CURRENT_TIMESTAMP(), 'YYYY-MM-DD"T"HH24:MI:SS.MS')""",
            read={
                'postgres': r"""SELECT to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS');"""}
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
            "SELECT REGEXP_EXTRACT(`a`, 'a|b|c', 1)",
            read={'presto': "select regexp_extract(\"a\", 'a|b|c', 1)"}
        )

        # day_of_week
        os.environ['READ_DIALECT'] = 'presto'
        self.validate_all(
            "SELECT DAYOFWEEK_ISO(d), DAYOFWEEK_ISO(d), DAYOFYEAR(d), DAYOFYEAR(d), YEAROFWEEK(d), YEAROFWEEK(d)",
            read={'presto': "select dow(d), day_of_week(d), doy(d), day_of_year(d), yow(d), year_of_week(d)"}
        )

        os.environ.pop('READ_DIALECT')

    def test_group_sql_expression(self):
        self.validate_all(
            "SELECT a, AVG(b), AVG(c), COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1) "
            "tab(a, b, c) GROUP BY a",
            write={
                "clickzetta": "SELECT a, AVG(b), AVG(c), COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3),"
                              " ('A1', 1, 1) AS tab(a, b, c) GROUP BY a",
            },
        )
        self.validate_all(
            "SELECT a, AVG(b), AVG(c), COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1) "
            "tab(a, b, c) GROUP BY a, b WITH CUBE",
            write={
                "clickzetta": "SELECT a, AVG(b), AVG(c), COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3),"
                              " ('A1', 1, 1) AS tab(a, b, c) GROUP BY CUBE (a, b)",
            },
        )
        self.validate_all(
            "SELECT a, AVG(b), AVG(c), COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1) "
            "tab(a, b, c) GROUP BY a, b WITH rollup",
            write={
                "clickzetta": "SELECT a, AVG(b), AVG(c), COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3),"
                              " ('A1', 1, 1) AS tab(a, b, c) GROUP BY ROLLUP (a, b)",
            },
        )
        self.validate_all(
            """SELECT a, b, AVG(c), COUNT(*)
                        FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1) tab(a, b, c)
                    GROUP BY CUBE (a, b)""",
            write={
                "clickzetta": "SELECT a, b, AVG(c), COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), "
                              "('A1', 1, 1) AS tab(a, b, c) GROUP BY CUBE (a, b)",
            },
        )
        self.validate_all(
            """SELECT a, b, count(*)
                    FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1) tab(a, b, c)
                    group by a, b with cube order by a, b LIMIT 10""",
            write={
                "clickzetta": "SELECT a, b, COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1)"
                              " AS tab(a, b, c) GROUP BY CUBE (a, b) ORDER BY a, b LIMIT 10",
            },
        )
        self.validate_all(
            """select category, max(live) live, max(comments) comments, rank() OVER 
                    (PARTITION BY category ORDER BY comments) rank1
                    FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1) tab(a, b, c)
                    GROUP BY category
                    GROUPING SETS ((), (category))
                    HAVING max(comments) > 0""",
            write={
                "clickzetta": "SELECT category, MAX(live) AS live, MAX(comments) AS comments, rank() OVER "
                              "(PARTITION BY category ORDER BY comments) AS rank1 FROM VALUES "
                              "('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1) AS tab(a, b, c) "
                              "GROUP BY GROUPING SETS ((), (category)) HAVING MAX(comments) > 0",
            },
        )
        self.validate_all(
            """SELECT a, b, count(*)
                    FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1) tab(a, b, c)
                    group by rollup(a,b), cube(c) order by a, b LIMIT 10""",
            write={
                "clickzetta": "SELECT a, b, COUNT(*) FROM VALUES ('A1', 2, 2), ('A1', 1, 1), ('A2', 3, 3), ('A1', 1, 1)"
                              " AS tab(a, b, c) GROUP BY CUBE (c), ROLLUP (a, b) ORDER BY a, b LIMIT 10",
            },
        )

    def test_hash_func(self):
        self.validate_all("select sum(murmur_hash3_32('test'))",
                          write={"clickzetta": "SELECT SUM(MURMURHASH3_32('test'))"})
