SELECT * FROM x JOIN y ON y.a = 1 JOIN z ON x.a = z.a AND y.a = z.a;
SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON y.a = 1 AND y.a = z.a;

SELECT * FROM x JOIN y ON y.a = 1 JOIN z ON x.a = z.a;
SELECT * FROM x JOIN y ON y.a = 1 JOIN z ON x.a = z.a;

SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a AND y.a = z.a;
SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON TRUE AND y.a = z.a;

SELECT * FROM x LEFT JOIN y ON y.a = 1 JOIN z ON x.a = z.a AND y.a = z.a;
SELECT * FROM x JOIN z ON x.a = z.a AND TRUE LEFT JOIN y ON y.a = 1 AND y.a = z.a;
