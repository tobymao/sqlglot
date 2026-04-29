# title: simple select
SELECT a FROM x;
SELECT "_t0"."a" AS "a" FROM "c"."db"."x" AS "_t0";

# title: select with where
SELECT a FROM x WHERE b > 5;
SELECT "_t0"."a" AS "a" FROM "c"."db"."x" AS "_t0" WHERE "_t0"."b" > 5;

# title: select star expanded
SELECT * FROM x;
SELECT "_t0"."a" AS "a", "_t0"."b" AS "b" FROM "c"."db"."x" AS "_t0";

# title: two columns
SELECT a, b FROM x;
SELECT "_t0"."a" AS "a", "_t0"."b" AS "b" FROM "c"."db"."x" AS "_t0";

# title: single cte
WITH t AS (SELECT a, b FROM x) SELECT * FROM t;
WITH "_t1" AS (SELECT "_t0"."a" AS "_c0", "_t0"."b" AS "_c1" FROM "c"."db"."x" AS "_t0") SELECT "_t1"."_c0" AS "a", "_t1"."_c1" AS "b" FROM "_t1" AS "_t1";

# title: multi cte
WITH t1 AS (SELECT a FROM x), t2 AS (SELECT b FROM y) SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.a = t2.b;
WITH "_t2" AS (SELECT "_t0"."a" AS "_c0" FROM "c"."db"."x" AS "_t0"), "_t3" AS (SELECT "_t1"."b" AS "_c1" FROM "c"."db"."y" AS "_t1") SELECT "_t2"."_c0" AS "a", "_t3"."_c1" AS "b" FROM "_t2" AS "_t2" JOIN "_t3" AS "_t3" ON "_t2"."_c0" = "_t3"."_c1";

# title: cross join
SELECT x.a, y.c FROM x CROSS JOIN y;
SELECT "_t0"."a" AS "a", "_t1"."c" AS "c" FROM "c"."db"."x" AS "_t0" CROSS JOIN "c"."db"."y" AS "_t1";

# title: inner join
SELECT x.a, y.c FROM x JOIN y ON x.b = y.b;
SELECT "_t0"."a" AS "a", "_t1"."c" AS "c" FROM "c"."db"."x" AS "_t0" JOIN "c"."db"."y" AS "_t1" ON "_t0"."b" = "_t1"."b";

# title: self join
SELECT a.a, b.b FROM x AS a JOIN x AS b ON a.a = b.b;
SELECT "_t0"."a" AS "a", "_t1"."b" AS "b" FROM "c"."db"."x" AS "_t0" JOIN "c"."db"."x" AS "_t1" ON "_t0"."a" = "_t1"."b";

# title: subquery in from
SELECT t.a FROM (SELECT a FROM x) AS t;
SELECT "_t1"."_c0" AS "a" FROM (SELECT "_t0"."a" AS "_c0" FROM "c"."db"."x" AS "_t0") AS "_t1";

# title: subquery with column aliases, user-declared aliases on the subquery are internal handles
SELECT t.p, t.q FROM (SELECT a, b FROM x) AS t(p, q);
SELECT "_t1"."_c0" AS "p", "_t1"."_c1" AS "q" FROM (SELECT "_t0"."a" AS "_c0", "_t0"."b" AS "_c1" FROM "c"."db"."x" AS "_t0") AS "_t1";

# title: nested subqueries
SELECT t.a FROM (SELECT t.a FROM (SELECT a FROM x) AS t) AS t;
SELECT "_t2"."_c1" AS "a" FROM (SELECT "_t1"."_c0" AS "_c1" FROM (SELECT "_t0"."a" AS "_c0" FROM "c"."db"."x" AS "_t0") AS "_t1") AS "_t2";

# title: uncorrelated subquery
SELECT a FROM x WHERE b IN (SELECT b FROM y);
SELECT "_t1"."a" AS "a" FROM "c"."db"."x" AS "_t1" WHERE "_t1"."b" IN (SELECT "_t0"."b" AS "_c0" FROM "c"."db"."y" AS "_t0");

# title: correlated subquery
SELECT a FROM x WHERE EXISTS (SELECT 1 FROM y WHERE y.b = x.b);
SELECT "_t1"."a" AS "a" FROM "c"."db"."x" AS "_t1" WHERE EXISTS(SELECT 1 AS "_c0" FROM "c"."db"."y" AS "_t0" WHERE "_t0"."b" = "_t1"."b");

# title: aggregation, auto-generated _col_N alias from qualify is left untouched at top level
SELECT a, COUNT(b) FROM x GROUP BY a HAVING COUNT(b) > 1;
SELECT "_t0"."a" AS "a", COUNT("_t0"."b") AS "_col_1" FROM "c"."db"."x" AS "_t0" GROUP BY "_t0"."a" HAVING COUNT("_t0"."b") > 1;

# title: expression in select
SELECT a + 1 FROM x;
SELECT "_t0"."a" + 1 AS "_col_0" FROM "c"."db"."x" AS "_t0";

# title: order by alias reference
SELECT a, b FROM x ORDER BY a LIMIT 10;
SELECT "_t0"."a" AS "a", "_t0"."b" AS "b" FROM "c"."db"."x" AS "_t0" ORDER BY "a" LIMIT 10;

# title: order by positional reference
SELECT a, b FROM x ORDER BY 1 DESC;
SELECT "_t0"."a" AS "a", "_t0"."b" AS "b" FROM "c"."db"."x" AS "_t0" ORDER BY "a" DESC;

# title: group by positional reference
SELECT a, COUNT(b) FROM x GROUP BY 1;
SELECT "_t0"."a" AS "a", COUNT("_t0"."b") AS "_col_1" FROM "c"."db"."x" AS "_t0" GROUP BY "_t0"."a";

# title: group by multiple positional references
SELECT a, b, COUNT(*) FROM x GROUP BY 1, 2;
SELECT "_t0"."a" AS "a", "_t0"."b" AS "b", COUNT(*) AS "_col_2" FROM "c"."db"."x" AS "_t0" GROUP BY "_t0"."a", "_t0"."b";

# title: union
SELECT a FROM x UNION SELECT b FROM y;
SELECT "_t0"."a" AS "a" FROM "c"."db"."x" AS "_t0" UNION SELECT "_t1"."b" AS "_c0" FROM "c"."db"."y" AS "_t1";

# title: union by name, matching column names unify to the same canonical name
# dialect: duckdb
SELECT a, b FROM x UNION BY NAME SELECT b, c FROM z;
SELECT "_t0"."a" AS "a", "_t0"."b" AS "b" FROM "c"."db"."x" AS "_t0" UNION BY NAME SELECT "_t1"."b" AS "b", "_t1"."c" AS "c" FROM "c"."db"."z" AS "_t1";

# title: union by name, disjoint column names stay distinct
# dialect: duckdb
SELECT a FROM x UNION BY NAME SELECT c FROM y;
SELECT "_t0"."a" AS "a" FROM "c"."db"."x" AS "_t0" UNION BY NAME SELECT "_t1"."c" AS "c" FROM "c"."db"."y" AS "_t1";

# title: union by name, nested rhs — inner branch aliases align with the outer left's canonical name
# dialect: duckdb
SELECT a + 1 AS shared FROM x UNION BY NAME (SELECT b AS shared FROM y UNION BY NAME SELECT c AS shared FROM z);
SELECT "_t0"."a" + 1 AS "shared" FROM "c"."db"."x" AS "_t0" UNION BY NAME (SELECT "_t1"."b" AS "shared" FROM "c"."db"."y" AS "_t1" UNION BY NAME SELECT "_t2"."c" AS "shared" FROM "c"."db"."z" AS "_t2");

# title: union by name inside a CTE, right branch's internal _cN is aligned with the left's so UBN merges correctly
# dialect: duckdb
WITH t AS (SELECT a AS k FROM x UNION BY NAME SELECT b AS k FROM y) SELECT k FROM t;
WITH "_t2" AS (SELECT "_t0"."a" AS "_c0" FROM "c"."db"."x" AS "_t0" UNION BY NAME SELECT "_t1"."b" AS "_c0" FROM "c"."db"."y" AS "_t1") SELECT "_t2"."_c0" AS "k" FROM "_t2" AS "_t2";

# title: case when
SELECT CASE WHEN a > 0 THEN b ELSE a END FROM x;
SELECT CASE WHEN "_t0"."a" > 0 THEN "_t0"."b" ELSE "_t0"."a" END AS "_col_0" FROM "c"."db"."x" AS "_t0";

# title: three way join
SELECT x.a, y.b, z.c FROM x JOIN y ON x.b = y.b JOIN z ON y.c = z.c;
SELECT "_t0"."a" AS "a", "_t1"."b" AS "b", "_t2"."c" AS "c" FROM "c"."db"."x" AS "_t0" JOIN "c"."db"."y" AS "_t1" ON "_t0"."b" = "_t1"."b" JOIN "c"."db"."z" AS "_t2" ON "_t1"."c" = "_t2"."c";

# title: struct field access, field names are preserved
SELECT structs.one.a_1 FROM structs;
SELECT "_t0"."one"."a_1" AS "a_1" FROM "c"."db"."structs" AS "_t0";

# title: nested struct field access, field names are preserved
SELECT structs.nested_0.nested_1.a_2 FROM structs;
SELECT "_t0"."nested_0"."nested_1"."a_2" AS "a_2" FROM "c"."db"."structs" AS "_t0";

# title: struct field access in where, field names are preserved
SELECT structs.one.b_1 FROM structs WHERE structs.one.a_1 > 0;
SELECT "_t0"."one"."b_1" AS "b_1" FROM "c"."db"."structs" AS "_t0" WHERE "_t0"."one"."a_1" > 0;

# title: json bracket access, field names are preserved
SELECT j['name'] FROM jtbl WHERE j['age'] > 18;
SELECT "_t0"."j"['name'] AS "name" FROM "c"."db"."jtbl" AS "_t0" WHERE "_t0"."j"['age'] > 18;

# title: json nested bracket access, field names are preserved
SELECT j['a']['b'] FROM jtbl;
SELECT "_t0"."j"['a']['b'] AS "b" FROM "c"."db"."jtbl" AS "_t0";

# title: json extract function, path string is preserved
SELECT JSON_EXTRACT(j, '$.field') FROM jtbl;
SELECT JSON_EXTRACT("_t0"."j", '$.field') AS "field" FROM "c"."db"."jtbl" AS "_t0";

# title: postgres json arrow operator, field name is preserved
# dialect: postgres
SELECT j -> 'field' FROM jtbl;
SELECT "_t0"."j" -> 'field' AS "field" FROM "c"."db"."jtbl" AS "_t0";

# title: select star except, excluded columns are dropped before canonicalization
# dialect: duckdb
SELECT * EXCEPT (a) FROM x;
SELECT "_t0"."b" AS "b" FROM "c"."db"."x" AS "_t0";

# title: select star replace, replacement expression is canonicalized (top-level alias 'a' is preserved)
# dialect: bigquery
SELECT * REPLACE (a + 1 AS a) FROM x;
SELECT `_t0`.`a` + 1 AS `a`, `_t0`.`b` AS `b` FROM `c`.`db`.`x` AS `_t0`;

# title: select star rename, the renamed top-level alias is preserved
# dialect: snowflake
SELECT * RENAME (a AS new_a) FROM x;
SELECT "_t0"."A" AS "NEW_A", "_t0"."B" AS "B" FROM "C"."DB"."X" AS "_t0";

# title: values clause with column aliases, alias and column names are internal handles
SELECT t.i, t.s FROM (VALUES (1, 'a'), (2, 'b')) AS t(i, s);
SELECT "_t0"."_c0" AS "i", "_t0"."_c1" AS "s" FROM (VALUES (1, 'a'), (2, 'b')) AS "_t0"("_c0", "_c1");

# title: lateral subquery alias is canonicalized, outer table shared with lateral body
# dialect: postgres
SELECT x.a, t.b FROM x, LATERAL (SELECT x.a + 1 AS b) AS t;
SELECT "_t0"."a" AS "a", "_t1"."_c0" AS "b" FROM "c"."db"."x" AS "_t0", LATERAL (SELECT "_t0"."a" + 1 AS "_c0") AS "_t1";

# title: window function with partition and order
SELECT a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn FROM x;
SELECT "_t0"."a" AS "a", ROW_NUMBER() OVER (PARTITION BY "_t0"."a" ORDER BY "_t0"."b") AS "rn" FROM "c"."db"."x" AS "_t0";

# title: qualify clause with window function
# dialect: bigquery
SELECT a, b FROM x QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1;
SELECT `_t0`.`a` AS `a`, `_t0`.`b` AS `b` FROM `c`.`db`.`x` AS `_t0` QUALIFY ROW_NUMBER() OVER (PARTITION BY `_t0`.`a` ORDER BY `_t0`.`b`) = 1;

# title: scalar subquery in select, the subquery's internal alias is canonicalized, the outer one is preserved
SELECT x.a, (SELECT MAX(y.c) FROM y) AS m FROM x;
SELECT "_t1"."a" AS "a", (SELECT MAX("_t0"."c") AS "_c0" FROM "c"."db"."y" AS "_t0") AS "m" FROM "c"."db"."x" AS "_t1";

# title: distinct on
# dialect: postgres
SELECT DISTINCT ON (a) a, b FROM x;
SELECT DISTINCT ON ("a") "_t0"."a" AS "a", "_t0"."b" AS "b" FROM "c"."db"."x" AS "_t0";

# title: join using is expanded to on with coalesce
SELECT * FROM x JOIN y USING (b);
SELECT "_t0"."a" AS "a", COALESCE("_t0"."b", "_t1"."b") AS "b", "_t1"."c" AS "c" FROM "c"."db"."x" AS "_t0" JOIN "c"."db"."y" AS "_t1" ON "_t0"."b" = "_t1"."b";

# title: chained union
SELECT a FROM x UNION SELECT b FROM y UNION SELECT c FROM z;
SELECT "_t0"."a" AS "a" FROM "c"."db"."x" AS "_t0" UNION SELECT "_t1"."b" AS "_c0" FROM "c"."db"."y" AS "_t1" UNION SELECT "_t2"."c" AS "_c1" FROM "c"."db"."z" AS "_t2";

# title: filter clause on aggregate
SELECT SUM(a) FILTER (WHERE b > 0) FROM x;
SELECT SUM("_t0"."a") FILTER(WHERE "_t0"."b" > 0) AS "_col_0" FROM "c"."db"."x" AS "_t0";

# title: left semi join, right side columns are canonicalized even when not selected
# dialect: spark
SELECT a FROM x LEFT SEMI JOIN y ON x.b = y.b;
SELECT `_t0`.`a` AS `a` FROM `c`.`db`.`x` AS `_t0` LEFT SEMI JOIN `c`.`db`.`y` AS `_t1` ON `_t0`.`b` = `_t1`.`b`;

# title: array constructor in select
# dialect: duckdb
SELECT [a, b] FROM x;
SELECT ["_t0"."a", "_t0"."b"] AS "_col_0" FROM "c"."db"."x" AS "_t0";

# title: recursive cte, self-reference and definition share the same canonical name
# validate_qualify_columns: false
WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte;
WITH RECURSIVE "_t0" AS (SELECT 1 AS "_c0" UNION ALL SELECT "_t0"."_c0" + 1 AS "_c1" FROM "_t0" AS "_t0" WHERE "_t0"."_c0" < 5) SELECT "_t0"."_c0" AS "n" FROM "_t0" AS "_t0";

# title: bigquery unnest with offset, both primary and offset pseudo-columns are canonicalized
# dialect: bigquery
SELECT n, off FROM UNNEST([10, 20, 30]) AS n WITH OFFSET AS off;
SELECT `_c0` AS `n`, `_c1` AS `off` FROM UNNEST([10, 20, 30]) AS `_c0` WITH OFFSET AS `_c1`;


# title: bigquery correlated unnest, outer table shared with unnest expression
# dialect: bigquery
SELECT t.id, u FROM t CROSS JOIN UNNEST(t.arr) AS u;
SELECT `_t0`.`id` AS `id`, `_c0` AS `u` FROM `c`.`db`.`t` AS `_t0` CROSS JOIN UNNEST(`_t0`.`arr`) AS `_c0`;

# title: bigquery whole-row struct selection — TableColumn follows the table's canonical name, output alias preserves the row-struct's contract name
# dialect: bigquery
SELECT t FROM t;
SELECT `_t0` AS `t` FROM `c`.`db`.`t` AS `_t0`;

# title: table valued function with column alias, base-table-style column name is preserved
# dialect: postgres
SELECT * FROM generate_series(1, 10) AS g(n);
SELECT "_t0"."n" AS "n" FROM GENERATE_SERIES(1, 10) AS "_t0"("n");

# title: pivot with user alias, alias is canonicalized but output column names (from IN literals) are preserved
# dialect: bigquery
SELECT my_pivot.x FROM pvt PIVOT(SUM(v) FOR c IN ('x')) AS my_pivot;
SELECT `_t1`.`x` AS `x` FROM `c`.`db`.`pvt` AS `_t0` PIVOT(SUM(`_t0`.`v`) FOR `_t0`.`c` IN ('x')) AS `_t1`;

# title: CTE-level user alias on a pass-through column is replaced with _cN, outer preserves the user-facing alias
WITH t AS (SELECT a AS foo FROM x) SELECT foo FROM t;
WITH "_t1" AS (SELECT "_t0"."a" AS "_c0" FROM "c"."db"."x" AS "_t0") SELECT "_t1"."_c0" AS "foo" FROM "_t1" AS "_t1";

# title: top-level user-provided alias on an expression stays as-is
SELECT a + 1 AS total FROM x;
SELECT "_t0"."a" + 1 AS "total" FROM "c"."db"."x" AS "_t0";

# title: CTE-level user alias on an expression is replaced with _cN, outer preserves the user-facing alias
WITH t AS (SELECT a + 1 AS total FROM x) SELECT total FROM t;
WITH "_t1" AS (SELECT "_t0"."a" + 1 AS "_c0" FROM "c"."db"."x" AS "_t0") SELECT "_t1"."_c0" AS "total" FROM "_t1" AS "_t1";

# title: ORDER BY reference to a CTE-level alias follows the alias's _cN canonicalization
WITH t AS (SELECT a + 1 AS total FROM x ORDER BY total) SELECT total FROM t;
WITH "_t1" AS (SELECT "_t0"."a" + 1 AS "_c0" FROM "c"."db"."x" AS "_t0" ORDER BY "_c0") SELECT "_t1"."_c0" AS "total" FROM "_t1" AS "_t1";
