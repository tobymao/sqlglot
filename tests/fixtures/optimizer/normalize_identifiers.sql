foo;
foo;

# dialect: snowflake
foo + "bar".baz;
FOO + "bar".BAZ;

SELECT a FROM x;
SELECT a FROM x;

# dialect: snowflake
SELECT A FROM X;
SELECT A FROM X;

SELECT "A" FROM "X";
SELECT "A" FROM "X";

SELECT a AS A FROM x;
SELECT a AS a FROM x;

# dialect: snowflake
SELECT A AS a FROM X;
SELECT A AS A FROM X;

SELECT * FROM x;
SELECT * FROM x;

SELECT A FROM x;
SELECT a FROM x;

# dialect: snowflake
SELECT a FROM X;
SELECT A FROM X;

SELECT a FROM X;
SELECT a FROM x;

# dialect: snowflake
SELECT A FROM x;
SELECT A FROM X;

SELECT A AS A FROM (SELECT a AS A FROM x);
SELECT a AS a FROM (SELECT a AS a FROM x);

SELECT a AS B FROM x ORDER BY B;
SELECT a AS b FROM x ORDER BY b;

SELECT A FROM x ORDER BY A;
SELECT a FROM x ORDER BY a;

SELECT A AS B FROM X GROUP BY A HAVING SUM(B) > 0;
SELECT a AS b FROM x GROUP BY a HAVING SUM(b) > 0;

SELECT A AS B, SUM(B) AS C FROM X GROUP BY A HAVING C > 0;
SELECT a AS b, SUM(b) AS c FROM x GROUP BY a HAVING c > 0;

SELECT A FROM X UNION SELECT A FROM X;
SELECT a FROM x UNION SELECT a FROM x;

SELECT A AS A FROM X UNION SELECT A AS A FROM X;
SELECT a AS a FROM x UNION SELECT a AS a FROM x;

(SELECT A AS A FROM X);
(SELECT a AS a FROM x);

# dialect: snowflake
SELECT a /* sqlglot.meta case_sensitive */, b FROM table /* sqlglot.meta case_sensitive */;
SELECT a /* sqlglot.meta case_sensitive */, B FROM table /* sqlglot.meta case_sensitive */;

# dialect: redshift
SELECT COALESCE(json_val.a /* sqlglot.meta case_sensitive */, json_val.A /* sqlglot.meta case_sensitive */) FROM tbl;
SELECT COALESCE(json_val.a /* sqlglot.meta case_sensitive */, json_val.A /* sqlglot.meta case_sensitive */) FROM tbl;

SELECT @X;
SELECT @X;

# dialect: bigquery,normalization_strategy=case_insensitive_uppercase
SELECT `foo`, `BaR` FROM baz CROSS JOIN `bla` CROSS JOIN bloo;
SELECT `FOO`, `BAR` FROM BAZ CROSS JOIN `BLA` CROSS JOIN BLOO;

# title: case_insensitive_except_tables keeps qualified table names' case in any dialect
# dialect: trino,normalization_strategy=case_insensitive_except_tables
WITH Cte AS (SELECT 1) SELECT o.Amount, "Foo" FROM "my-proj".Sales.Orders AS O CROSS JOIN Cte;
WITH cte AS (SELECT 1) SELECT o.amount, "foo" FROM "my-proj".Sales.Orders AS o CROSS JOIN cte;

# title: duckdb only normalizes ascii characters
# dialect: duckdb
SELECT "FoÄ" FROM "BaÜ";
SELECT "foÄ" FROM "baÜ";

# title: postgres only normalizes ascii characters
# dialect: postgres
SELECT FoÄ FROM BaÜ;
SELECT foÄ FROM baÜ;

# title: bigquery only normalizes ascii characters
# dialect: bigquery
SELECT `FoÄ` FROM `BaÜ`;
SELECT `foÄ` FROM `baÜ`;

# title: redshift only normalizes ascii characters
# dialect: redshift
SELECT "FoÄ" FROM "BaÜ";
SELECT "foÄ" FROM "baÜ";

# title: spark normalizes unicode characters
# dialect: spark
SELECT `FoÄ` FROM `BaÜ`;
SELECT `foä` FROM `baü`;

# title: sqlite only normalizes ascii characters
# dialect: sqlite
SELECT "FoÄ" FROM "BaÜ";
SELECT "foÄ" FROM "baÜ";

# dialect: teradata
SELECT "FOO" FROM BAR;
SELECT "foo" FROM bar;

# title: pattern variables are uppercased unless quoted, whatever the dialect's identifiers
# dialect: trino
SELECT * FROM T MATCH_RECOGNIZE (MEASURES a.X AS Y, "b".X AS z PATTERN (a "b") DEFINE A AS a.X > 0, "b" AS TRUE);
SELECT * FROM t MATCH_RECOGNIZE (MEASURES A.x AS y, "b".x AS z PATTERN (a "b") DEFINE A AS A.x > 0, "b" AS TRUE);

# dialect: snowflake
SELECT * FROM t MATCH_RECOGNIZE (MEASURES a.x AS y PATTERN (a) DEFINE a AS x > 0);
SELECT * FROM T MATCH_RECOGNIZE (MEASURES A.X AS Y PATTERN (a) DEFINE A AS X > 0);

# dialect: trino
SELECT * FROM t MATCH_RECOGNIZE (MEASURES b.s.f AS y, CLASSIFIER(b) AS c PATTERN (b) DEFINE b AS b.s.f > (SELECT MAX(b.x) FROM u AS b));
SELECT * FROM t MATCH_RECOGNIZE (MEASURES B.s.f AS y, CLASSIFIER(B) AS c PATTERN (b) DEFINE B AS B.s.f > (SELECT MAX(b.x) FROM u AS b));

# dialect: trino
SELECT * FROM t MATCH_RECOGNIZE (MEASURES CLASSIFIER(ab) AS c PATTERN (a b) SUBSET ab = (a, b) DEFINE a AS ab.x > 0);
SELECT * FROM t MATCH_RECOGNIZE (MEASURES CLASSIFIER(AB) AS c PATTERN (a b) SUBSET AB = (A, B) DEFINE A AS AB.x > 0);

# dialect: trino, normalization_strategy=case_sensitive
SELECT * FROM t MATCH_RECOGNIZE (MEASURES a.x AS y PATTERN (a) DEFINE a AS TRUE);
SELECT * FROM t MATCH_RECOGNIZE (MEASURES a.x AS y PATTERN (a) DEFINE a AS TRUE);
