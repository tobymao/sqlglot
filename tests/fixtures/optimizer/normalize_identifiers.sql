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
SELECT COALESCE(json_val.a /* sqlglot.meta case_sensitive */, json_val.A /* sqlglot.meta case_sensitive */) FROM table;
SELECT COALESCE(json_val.a /* sqlglot.meta case_sensitive */, json_val.A /* sqlglot.meta case_sensitive */) FROM table;

SELECT @X;
SELECT @X;
