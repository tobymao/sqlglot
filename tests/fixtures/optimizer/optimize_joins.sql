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
