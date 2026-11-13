SELECT * FROM x JOIN y ON y.a = 1 JOIN z ON x.a = z.a AND y.a = z.a;
SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON y.a = 1 AND y.a = z.a;

SELECT * FROM x JOIN y ON y.a = 1 JOIN z ON x.a = z.a;
SELECT * FROM x JOIN y ON y.a = 1 JOIN z ON x.a = z.a;

SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a AND y.a = z.a;
SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON y.a = z.a;

SELECT * FROM x LEFT JOIN y ON y.a = 1 JOIN z ON x.a = z.a AND y.a = z.a;
SELECT * FROM x LEFT JOIN y ON y.a = 1 JOIN z ON x.a = z.a AND y.a = z.a;

SELECT * FROM x INNER JOIN z ON x.id = z.id;
SELECT * FROM x JOIN z ON x.id = z.id;

SELECT * FROM x LEFT OUTER JOIN z;
SELECT * FROM x LEFT JOIN z ON TRUE;

SELECT * FROM x CROSS JOIN z;
SELECT * FROM x CROSS JOIN z;

SELECT * FROM x JOIN z;
SELECT * FROM x CROSS JOIN z;

SELECT * FROM x FULL JOIN z;
SELECT * FROM x FULL JOIN z ON TRUE;

SELECT * FROM x NATURAL JOIN z;
SELECT * FROM x NATURAL JOIN z ON TRUE;

SELECT * FROM x RIGHT JOIN z;
SELECT * FROM x RIGHT JOIN z ON TRUE;

SELECT * FROM x JOIN z USING (id);
SELECT * FROM x JOIN z USING (id);

SELECT * FROM x CROSS JOIN z ON TRUE;
SELECT * FROM x CROSS JOIN z;

SELECT * FROM x LEFT ANTI JOIN y ON x.a = y.a; 
SELECT * FROM x LEFT ANTI JOIN y ON x.a = y.a;

SELECT * FROM x CROSS JOIN y ANTI JOIN z ON x.a = z.a AND y.a = x.a;
SELECT * FROM x CROSS JOIN y ANTI JOIN z ON x.a = z.a AND y.a = x.a;

SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a OR y.a = x.a;
SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a OR y.a = x.a;

SELECT * FROM x CROSS JOIN y SEMI JOIN z ON x.a = z.a OR y.a = x.a;
SELECT * FROM x CROSS JOIN y SEMI JOIN z ON x.a = z.a OR y.a = x.a;

SELECT * FROM x LEFT SEMI JOIN y ON x.a = y.a; 
SELECT * FROM x LEFT SEMI JOIN y ON x.a = y.a;

# title: Preserve parenthesized join operands
# execute: true
SELECT x.a, y.b, z.b, w.c FROM (x LEFT JOIN y ON x.b = y.b) INNER JOIN (y AS z LEFT JOIN y AS w ON z.c = w.c) ON y.c = z.b ORDER BY x.a, y.b, z.b, w.c;
SELECT x.a, y.b, z.b, w.c FROM (x LEFT JOIN y ON x.b = y.b) JOIN (y AS z LEFT JOIN y AS w ON z.c = w.c) ON y.c = z.b ORDER BY x.a, y.b, z.b, w.c;

# title: Preserve multiple unnamed join operands
# execute: true
SELECT x.a, y.b, z.b, v.a, w.c FROM x JOIN (y LEFT JOIN y AS z ON y.c = z.c) ON x.b = y.b JOIN (x AS v LEFT JOIN y AS w ON v.b = w.b) ON z.c = v.a ORDER BY x.a, y.b, z.b, v.a, w.c;
SELECT x.a, y.b, z.b, v.a, w.c FROM x JOIN (y LEFT JOIN y AS z ON y.c = z.c) ON x.b = y.b JOIN (x AS v LEFT JOIN y AS w ON v.b = w.b) ON z.c = v.a ORDER BY x.a, y.b, z.b, v.a, w.c;

# title: Preserve predicates referencing unnamed join operands
# execute: true
SELECT x.a, y.b, z.b FROM x CROSS JOIN y JOIN (y AS z JOIN y AS w ON z.c = w.c) ON x.b = z.b AND y.c = z.c ORDER BY x.a, y.b, z.b;
SELECT x.a, y.b, z.b FROM x CROSS JOIN y JOIN (y AS z JOIN y AS w ON z.c = w.c) ON x.b = z.b AND y.c = z.c ORDER BY x.a, y.b, z.b;

# title: Preserve comma join precedence before RIGHT JOIN
# execute: true
SELECT x.a, y.b, z.c FROM x, y RIGHT JOIN y AS z ON y.b = z.b ORDER BY x.a, y.b, z.c;
SELECT x.a, y.b, z.c FROM x CROSS JOIN (y RIGHT JOIN y AS z ON y.b = z.b) ORDER BY x.a, y.b, z.c;

# title: Preserve comma join precedence before FULL JOIN
# execute: true
SELECT x.a, y.b, z.c FROM x, y FULL OUTER JOIN y AS z ON y.b = z.b ORDER BY x.a, y.b, z.c;
SELECT x.a, y.b, z.c FROM x CROSS JOIN (y FULL JOIN y AS z ON y.b = z.b) ORDER BY x.a, y.b, z.c;

# title: Preserve comma join precedence across an intervening inner join
# execute: true
SELECT x.a, y.b, z.b, v.c FROM x, y JOIN y AS z ON y.b = z.b RIGHT JOIN y AS v ON z.c = v.c ORDER BY x.a, y.b, z.b, v.c;
SELECT x.a, y.b, z.b, v.c FROM x CROSS JOIN (y JOIN y AS z ON y.b = z.b RIGHT JOIN y AS v ON z.c = v.c) ORDER BY x.a, y.b, z.b, v.c;

# title: Normalize independent comma groups
# execute: true
SELECT x.a, y.b, z.c, v.a, w.b FROM x, y RIGHT JOIN y AS z ON y.b = z.b, x AS v, y AS w ORDER BY x.a, y.b, z.c, v.a, w.b;
SELECT x.a, y.b, z.c, v.a, w.b FROM x CROSS JOIN (y RIGHT JOIN y AS z ON y.b = z.b) CROSS JOIN x AS v CROSS JOIN y AS w ORDER BY x.a, y.b, z.c, v.a, w.b;

# title: Stop at the next comma group
# execute: true
SELECT x.a, y.b, z.b, v.c FROM x, y, y AS z FULL JOIN y AS v ON z.c = v.c ORDER BY x.a, y.b, z.b, v.c;
SELECT x.a, y.b, z.b, v.c FROM x CROSS JOIN y CROSS JOIN (y AS z FULL JOIN y AS v ON z.c = v.c) ORDER BY x.a, y.b, z.b, v.c;

# title: Preserve comma join precedence with USING
# execute: true
SELECT x.a, y.b, z.c FROM x, y RIGHT JOIN y AS z USING (b) ORDER BY x.a, y.b, z.c;
SELECT x.a, y.b, z.c FROM x CROSS JOIN (y RIGHT JOIN y AS z USING (b)) ORDER BY x.a, y.b, z.c;

# title: Preserve comma join precedence inside subqueries
# execute: true
SELECT t.a, t.b, t.c, w.d FROM (SELECT x.a, y.b, z.c FROM x, y RIGHT JOIN y AS z ON y.b = z.b) AS t CROSS JOIN w ORDER BY t.a, t.b, t.c, w.d;
SELECT t.a, t.b, t.c, w.d FROM (SELECT x.a, y.b, z.c FROM x CROSS JOIN (y RIGHT JOIN y AS z ON y.b = z.b)) AS t CROSS JOIN w ORDER BY t.a, t.b, t.c, w.d;

# title: Normalize comma joins before parenthesized operands
# execute: true
SELECT x.a, y.b, z.c FROM x, (y RIGHT JOIN y AS z ON y.b = z.b) ORDER BY x.a, y.b, z.c;
SELECT x.a, y.b, z.c FROM x CROSS JOIN (y RIGHT JOIN y AS z ON y.b = z.b) ORDER BY x.a, y.b, z.c;

# title: Normalize comma joins before LEFT JOIN
# execute: true
SELECT x.a, y.b, z.c FROM x, y LEFT JOIN y AS z ON y.b = z.b ORDER BY x.a, y.b, z.c;
SELECT x.a, y.b, z.c FROM x CROSS JOIN y LEFT JOIN y AS z ON y.b = z.b ORDER BY x.a, y.b, z.c;
