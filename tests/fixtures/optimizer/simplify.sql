--------------------------------------
-- Conditions
--------------------------------------
x AND x;
x;

y OR y;
y;

x AND NOT x;
FALSE;

x OR NOT x;
TRUE;

1 AND TRUE;
TRUE;

TRUE AND TRUE;
TRUE;

1 AND TRUE AND 1 AND 1;
TRUE;

TRUE AND FALSE;
FALSE;

FALSE AND FALSE;
FALSE;

FALSE AND TRUE AND TRUE;
FALSE;

x > y OR FALSE;
x > y;

FALSE OR x = y;
x = y;

1 = 1;
TRUE;

1.0 = 1;
TRUE;

CAST('2023-01-01' AS DATE) = CAST('2023-01-01' AS DATE);
TRUE;

'x' = 'y';
FALSE;

'x' = 'x';
TRUE;

NULL AND TRUE;
NULL;

NULL AND FALSE;
FALSE;

NULL AND NULL;
NULL;

NULL OR TRUE;
TRUE;

NULL OR NULL;
NULL;

FALSE OR NULL;
NULL;

NOT TRUE;
FALSE;

NOT FALSE;
TRUE;

NOT NULL;
NULL;

NULL = NULL;
NULL;

-- Can't optimize this because different engines do different things
-- mysql converts to 0 and 1 but tsql does true and false
NULL <=> NULL;
NULL IS NOT DISTINCT FROM NULL;

a IS NOT DISTINCT FROM a;
a IS NOT DISTINCT FROM a;

NULL IS DISTINCT FROM NULL;
NULL IS DISTINCT FROM NULL;

NOT (NOT TRUE);
TRUE;

a AND (b OR b);
a AND b;

a AND (b AND b);
a AND b;

--------------------------------------
-- Absorption
--------------------------------------
(A OR B) AND (C OR NOT A);
(A OR B) AND (C OR NOT A);

A AND (A OR B);
A;

A AND D AND E AND (B OR A);
A AND D AND E;

D AND A AND E AND (B OR A);
A AND D AND E;

(A OR B) AND A;
A;

C AND D AND (A OR B) AND E AND F AND A;
A AND C AND D AND E AND F;

A OR (A AND B);
A;

(A AND B) OR A;
A;

A AND (NOT A OR B);
A AND B;

(NOT A OR B) AND A;
A AND B;

A OR (NOT A AND B);
A OR B;

(A OR C) AND ((A OR C) OR B);
A OR C;

(A OR C) AND (A OR B OR C);
A OR C;

A AND (B AND C) AND (D AND E);
A AND B AND C AND D AND E;

--------------------------------------
-- Elimination
--------------------------------------
(A AND B) OR (A AND NOT B);
A;

(A AND B) OR (NOT A AND B);
B;

(A AND NOT B) OR (A AND B);
A;

(NOT A AND B) OR (A AND B);
B;

(A OR B) AND (A OR NOT B);
A;

(A OR B) AND (NOT A OR B);
B;

(A OR NOT B) AND (A OR B);
A;

(NOT A OR B) AND (A OR B);
B;

(NOT A OR NOT B) AND (NOT A OR B);
NOT A;

(NOT A OR NOT B) AND (NOT A OR NOT NOT B);
NOT A;

E OR (A AND B) OR C OR D OR (A AND NOT B);
A OR C OR D OR E;

--------------------------------------
-- Associativity
--------------------------------------
(A AND B) AND C;
A AND B AND C;

A AND (B AND C);
A AND B AND C;

(A OR B) OR C;
A OR B OR C;

A OR (B OR C);
A OR B OR C;

((A AND B) AND C) AND D;
A AND B AND C AND D;

(((((A) AND B)) AND C)) AND D;
A AND B AND C AND D;

(x + 1) + 2;
x + 3;

x + (1 + 2);
x + 3;

(x * 2) * 4 + (1 + 3) + 5;
x * 8 + 9;

(x - 1) - 2;
(x - 1) - 2;

x - (3 - 2);
x - 1;

--------------------------------------
-- Comparison and Pruning
--------------------------------------
A AND D AND B AND E AND F AND G AND E AND A;
A AND B AND D AND E AND F AND G;

A AND NOT B AND C AND B;
FALSE;

(a AND b AND c AND d) AND (d AND c AND b AND a);
a AND b AND c AND d;

(c AND (a AND b)) AND ((b AND a) AND c);
a AND b AND c;

(A AND B AND C) OR (C AND B AND A);
A AND B AND C;

--------------------------------------
-- Where removal
--------------------------------------
SELECT x WHERE TRUE;
SELECT x;

SELECT x FROM y JOIN z ON TRUE;
SELECT x FROM y CROSS JOIN z;

SELECT x FROM y RIGHT JOIN z ON TRUE;
SELECT x FROM y CROSS JOIN z;

SELECT x FROM y LEFT JOIN z ON TRUE;
SELECT x FROM y LEFT JOIN z ON TRUE;

SELECT x FROM y FULL OUTER JOIN z ON TRUE;
SELECT x FROM y FULL OUTER JOIN z ON TRUE;

SELECT x FROM y JOIN z USING (x);
SELECT x FROM y JOIN z USING (x);

--------------------------------------
-- Parenthesis removal
--------------------------------------
(TRUE);
TRUE;

(FALSE);
FALSE;

((TRUE));
TRUE;

(FALSE OR TRUE);
TRUE;

TRUE OR (((FALSE) OR (TRUE)) OR FALSE);
TRUE;

(NOT FALSE) AND (NOT TRUE);
FALSE;

((NOT FALSE) AND (x = x)) AND (TRUE OR 1 <> 3);
x = x;

((NOT FALSE) AND (x = x)) AND (FALSE OR 1 <> 2);
x = x;

(('a' = 'a') AND TRUE and NOT FALSE);
TRUE;

(x = y) and z;
x = y AND z;

x * (1 - y);
x * (1 - y);

(((x % 20) = 0) = TRUE);
((x % 20) = 0) = TRUE;

--------------------------------------
-- Literals
--------------------------------------
1 + 1;
2;

0.06 + 0.01;
0.07;

0.06 + 1;
1.06;

1.2E+1 + 15E-3;
12.015;

1.2E1 + 15E-3;
12.015;

1 - 2;
-1;

-1 + 3;
2;

1 - 2 - 4;
-5;

-(-1);
1;

- -+1;
1;

+-1;
-1;

++1;
1;

0.06 - 0.01;
0.05;

3 * 4;
12;

3.0 * 9;
27.0;

0.03 * 0.73;
0.0219;

1 / 3;
1 / 3;

1 / 3.0;
0.3333333333333333333333333333;

20.0 / 6;
3.333333333333333333333333333;

10 / 5;
10 / 5;

(1.0 * 3) * 4 - 2 * (5 / 2);
12.0 - 2 * (5 / 2);

a * 0.5 / 10 / (2.0 + 3);
a * 0.5 / 10 / 5.0;

a * 0.5 - 10 - (2.0 + 3);
a * 0.5 - 10 - 5.0;

x * (10 - 5);
x * 5;

6 - 2 + 4 * 2 + a;
12 + a;

a + 1 + 1 + 2;
a + 4;

a + (1 + 1) + (10);
a + 12;

a + (1 * 1) + (1 - (1 * 1));
a + 1;

a + (b * c) + (d - (e * f));
a + b * c + (d - e * f);

5 + 4 * 3;
17;

1 < 2;
TRUE;

2 <= 2;
TRUE;

2 >= 2;
TRUE;

2 > 1;
TRUE;

2 > 2.5;
FALSE;

3 > 2.5;
TRUE;

1 > NULL;
NULL;

1 <= NULL;
NULL;

1 IS NULL;
FALSE;

NULL IS NULL;
TRUE;

NULL IS NOT NULL;
FALSE;

1 IS NOT NULL;
TRUE;

date '1998-12-01' - interval x day;
CAST('1998-12-01' AS DATE) - INTERVAL x DAY;

date '1998-12-01' - interval '90' day;
CAST('1998-09-02' AS DATE);

date '1998-12-01' + interval '1' week;
CAST('1998-12-08' AS DATE);

interval '1' year + date '1998-01-01';
CAST('1999-01-01' AS DATE);

interval '1' year + date '1998-01-01' + 3 * 7 * 4;
CAST('1999-01-01' AS DATE) + 84;

date '1998-12-01' - interval '90' foo;
CAST('1998-12-01' AS DATE) - INTERVAL '90' FOO;

date '1998-12-01' + interval '90' foo;
CAST('1998-12-01' AS DATE) + INTERVAL '90' FOO;

CAST(x AS DATE) + interval '1' week;
CAST(x AS DATE) + INTERVAL '1' WEEK;

CAST('2008-11-11' AS DATETIME) + INTERVAL '5' MONTH;
CAST('2009-04-11 00:00:00' AS DATETIME);

datetime '1998-12-01' - interval '90' day;
CAST('1998-09-02 00:00:00' AS DATETIME);

CAST(x AS DATETIME) + interval '1' WEEK;
CAST(x AS DATETIME) + INTERVAL '1' WEEK;

TS_OR_DS_TO_DATE('1998-12-01 00:00:01') - interval '90' day;
CAST('1998-09-02' AS DATE);

DATE_ADD(CAST('2023-01-02' AS DATE), -2, 'MONTH');
CAST('2022-11-02' AS DATE);

DATE_SUB(CAST('2023-01-02' AS DATE), 1 + 1, 'DAY');
CAST('2022-12-31' AS DATE);

DATE_ADD(CAST('2023-01-02' AS DATETIME), -2, 'HOUR');
CAST('2023-01-01 22:00:00' AS DATETIME);

DATETIME_ADD(CAST('2023-01-02' AS DATETIME), -2, 'HOUR');
CAST('2023-01-01 22:00:00' AS DATETIME);

DATETIME_SUB(CAST('2023-01-02' AS DATETIME), 1 + 1, 'HOUR');
CAST('2023-01-01 22:00:00' AS DATETIME);

DATE_ADD(x, 1, 'MONTH');
DATE_ADD(x, 1, 'MONTH');

--------------------------------------
-- Comparisons
--------------------------------------
x < 0 OR x > 1;
x < 0 OR x > 1;

x < 0 OR x > 0;
x < 0 OR x > 0;

x < 1 OR x > 0;
x < 1 OR x > 0;

x < 1 OR x >= 0;
x < 1 OR x >= 0;

x <= 1 OR x > 0;
x <= 1 OR x > 0;

x <= 1 OR x >= 0;
x <= 1 OR x >= 0;

x <= 1 AND x <= 0;
x <= 0;

x <= 1 AND x > 0;
x <= 1 AND x > 0;

x <= 1 OR x > 0;
x <= 1 OR x > 0;

x <= 0 OR x < 0;
x <= 0;

x >= 0 OR x > 0;
x >= 0;

x >= 0 OR x > 1;
x >= 0;

x <= 0 OR x >= 0;
x <= 0 OR x >= 0;

x <= 0 AND x >= 0;
x <= 0 AND x >= 0;

x < 1 AND x < 2;
x < 1;

x < 1 OR x < 2;
x < 2;

x < 2 AND x < 1;
x < 1;

x < 2 OR x < 1;
x < 2;

x < 1 AND x < 1;
x < 1;

x < 1 OR x < 1;
x < 1;

x <= 1 AND x < 1;
x < 1;

x <= 1 OR x < 1;
x <= 1;

x < 1 AND x <= 1;
x < 1;

x < 1 OR x <= 1;
x <= 1;

x > 1 AND x > 2;
x > 2;

x > 1 OR x > 2;
x > 1;

x > 2 AND x > 1;
x > 2;

x > 2 OR x > 1;
x > 1;

x > 1 AND x > 1;
x > 1;

x > 1 OR x > 1;
x > 1;

x >= 1 AND x > 1;
x > 1;

x >= 1 OR x > 1;
x >= 1;

x > 1 AND x >= 1;
x > 1;

x > 1 OR x >= 1;
x >= 1;

x > 1 AND x >= 2;
x >= 2;

x > 1 OR x >= 2;
x > 1;

x > 1 AND x >= 2 AND x > 3 AND x > 0;
x > 3;

(x > 1 AND x >= 2 AND x > 3 AND x > 0) OR x > 0;
x > 0;

x > 1 AND x < 2 AND x > 3;
FALSE;

x > 1 AND x < 1;
FALSE;

x < 2 AND x > 1;
x < 2 AND x > 1;

x = 1 AND x < 1;
FALSE;

x = 1 AND x < 1.1;
x = 1;

x = 1 AND x <= 1;
x = 1;

x = 1 AND x <= 0.9;
FALSE;

x = 1 AND x > 0.9;
x = 1;

x = 1 AND x > 1;
FALSE;

x = 1 AND x >= 1;
x = 1;

x = 1 AND x >= 2;
FALSE;

x = 1 AND x <> 2;
x = 1;

x <> 1 AND x = 1;
FALSE;

x BETWEEN 0 AND 5 AND x > 3;
x <= 5 AND x > 3;

x > 3 AND 5 > x AND x BETWEEN 0 AND 10;
x < 5 AND x > 3;

x > 3 AND 5 < x AND x BETWEEN 9 AND 10;
x <= 10 AND x >= 9;

NOT x BETWEEN 0 AND 1;
x < 0 OR x > 1;

1 < x AND 3 < x;
x > 3;

'a' < 'b';
TRUE;

x = 2018 OR x <> 2018;
x <> 2018 OR x = 2018;

t0.x = t1.x AND t0.y < t1.y AND t0.y <= t1.y;
t0.x = t1.x AND t0.y < t1.y AND t0.y <= t1.y;

1 < x;
x > 1;

1 <= x;
x >= 1;

1 > x;
x < 1;

1 >= x;
x <= 1;

1 = x;
x = 1;

1 <> x;
x <> 1;

NOT 1 < x;
x <= 1;

NOT 1 <= x;
x < 1;

NOT 1 > x;
x >= 1;

NOT 1 >= x;
x > 1;

NOT 1 = x;
x <> 1;

NOT 1 <> x;
x = 1;

x > CAST('2024-01-01' AS DATE) OR x > CAST('2023-12-31' AS DATE);
x > CAST('2023-12-31' AS DATE);

CAST(x AS DATE) > CAST('2024-01-01' AS DATE) OR CAST(x AS DATE) > CAST('2023-12-31' AS DATE);
CAST(x AS DATE) > CAST('2023-12-31' AS DATE);

FUN() > 0 OR FUN() > 1;
FUN() > 0;

RAND() > 0 OR RAND() > 1;
RAND() > 0 OR RAND() > 1;

--------------------------------------
-- COALESCE
--------------------------------------
COALESCE(x);
x;

COALESCE(x, 1) = 2;
NOT x IS NULL AND x = 2;

2 = COALESCE(x, 1);
NOT x IS NULL AND x = 2;

COALESCE(x, 1, 1) = 1 + 1;
NOT x IS NULL AND x = 2;

COALESCE(x, 1, 2) = 2;
NOT x IS NULL AND x = 2;

COALESCE(x, 3) <= 2;
NOT x IS NULL AND x <= 2;

COALESCE(x, 1) <> 2;
x <> 2 OR x IS NULL;

COALESCE(x, 1) <= 2;
x <= 2 OR x IS NULL;

COALESCE(x, 1) = 1;
x = 1 OR x IS NULL;

COALESCE(x, 1) IS NULL;
FALSE;

COALESCE(ROW() OVER (), 1) = 1;
ROW() OVER () = 1 OR ROW() OVER () IS NULL;

a AND b AND COALESCE(ROW() OVER (), 1) = 1;
a AND b AND (ROW() OVER () = 1 OR ROW() OVER () IS NULL);

COALESCE(1, 2);
1;

COALESCE(CAST(CAST('2023-01-01' AS TIMESTAMP) AS DATE), x);
CAST(CAST('2023-01-01' AS TIMESTAMP) AS DATE);

COALESCE(CAST(NULL AS DATE), x);
COALESCE(CAST(NULL AS DATE), x);

NOT COALESCE(x, 1) = 2 AND y = 3;
(x <> 2 OR x IS NULL) AND y = 3;

--------------------------------------
-- CONCAT
--------------------------------------
CONCAT(x, y);
CONCAT(x, y);

CONCAT_WS(sep, x, y);
CONCAT_WS(sep, x, y);

CONCAT(x);
CONCAT(x);

CONCAT('a', 'b', 'c');
'abc';

CONCAT('a', NULL);
CONCAT('a', NULL);

CONCAT_WS('-', 'a', 'b', 'c');
'a-b-c';

CONCAT('a', x, y, 'b', 'c');
CONCAT('a', x, y, 'bc');

CONCAT_WS('-', 'a', x, y, 'b', 'c');
CONCAT_WS('-', 'a', x, y, 'b-c');

'a' || 'b';
'ab';

CONCAT_WS('-', 'a');
'a';

CONCAT_WS('-', x, y);
CONCAT_WS('-', x, y);

CONCAT_WS('', x, y);
CONCAT_WS('', x, y);

CONCAT_WS('-', x);
CONCAT_WS('-', x);

CONCAT_WS(sep, 'a', 'b');
CONCAT_WS(sep, 'a', 'b');

'a' || 'b' || x;
CONCAT('ab', x);

CONCAT(a, b) IN (SELECT * FROM foo WHERE cond);
CONCAT(a, b) IN (SELECT * FROM foo WHERE cond);

--------------------------------------
-- DATE_TRUNC
--------------------------------------
DATE_TRUNC('week', CAST('2023-12-15' AS DATE));
CAST('2023-12-11' AS DATE);

DATE_TRUNC('week', CAST('2023-12-16' AS DATE));
CAST('2023-12-11' AS DATE);

# dialect: bigquery
DATE_TRUNC(CAST('2023-12-15' AS DATE), WEEK);
CAST('2023-12-10' AS DATE);

# dialect: bigquery
DATE_TRUNC(CAST('2023-12-16' AS DATE), WEEK);
CAST('2023-12-10' AS DATE);

DATE_TRUNC('year', x) = CAST('2021-01-01' AS DATE);
x < CAST('2022-01-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE);

DATE_TRUNC('quarter', x) = CAST('2021-01-01' AS DATE);
x < CAST('2021-04-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE);

DATE_TRUNC('month', x) = CAST('2021-01-01' AS DATE);
x < CAST('2021-02-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE);

DATE_TRUNC('week', x) = CAST('2021-01-04' AS DATE);
x < CAST('2021-01-11' AS DATE) AND x >= CAST('2021-01-04' AS DATE);

DATE_TRUNC('day', x) = CAST('2021-01-01' AS DATE);
x < CAST('2021-01-02' AS DATE) AND x >= CAST('2021-01-01' AS DATE);

CAST('2021-01-01' AS DATE) = DATE_TRUNC('year', x);
x < CAST('2022-01-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE);

-- Always false, except for nulls
DATE_TRUNC('quarter', x) = CAST('2021-01-02' AS DATE);
DATE_TRUNC('QUARTER', x) = CAST('2021-01-02' AS DATE);

DATE_TRUNC('year', x) <> CAST('2021-01-01' AS DATE);
FALSE;

-- Always true, except for nulls
DATE_TRUNC('year', x) <> CAST('2021-01-02' AS DATE);
DATE_TRUNC('YEAR', x) <> CAST('2021-01-02' AS DATE);

DATE_TRUNC('year', x) <= CAST('2021-01-01' AS DATE);
x < CAST('2022-01-01' AS DATE);

DATE_TRUNC('year', x) <= CAST('2021-01-02' AS DATE);
x < CAST('2022-01-01' AS DATE);

CAST('2021-01-01' AS DATE) >= DATE_TRUNC('year', x);
x < CAST('2022-01-01' AS DATE);

DATE_TRUNC('year', x) < CAST('2021-01-01' AS DATE);
x < CAST('2021-01-01' AS DATE);

DATE_TRUNC('year', x) < CAST('2021-01-02' AS DATE);
x < CAST('2022-01-01' AS DATE);

DATE_TRUNC('year', x) >= CAST('2021-01-01' AS DATE);
x >= CAST('2021-01-01' AS DATE);

DATE_TRUNC('year', x) >= CAST('2021-01-02' AS DATE);
x >= CAST('2022-01-01' AS DATE);

DATE_TRUNC('year', x) > CAST('2021-01-01' AS DATE);
x >= CAST('2022-01-01' AS DATE);

DATE_TRUNC('year', x) > CAST('2021-01-02' AS DATE);
x >= CAST('2022-01-01' AS DATE);

DATE_TRUNC('year', x) > TS_OR_DS_TO_DATE(TS_OR_DS_TO_DATE('2021-01-02'));
x >= CAST('2022-01-01' AS DATE);

DATE_TRUNC('year', x) > TS_OR_DS_TO_DATE(TS_OR_DS_TO_DATE('2021-01-02', '%Y'));
DATE_TRUNC('YEAR', x) > CAST(STR_TO_TIME('2021-01-02', '%Y') AS DATE);

-- right is not a date
DATE_TRUNC('year', x) <> '2021-01-02';
DATE_TRUNC('YEAR', x) <> '2021-01-02';

DATE_TRUNC('year', x) IN (CAST('2021-01-01' AS DATE), CAST('2023-01-01' AS DATE));
(x < CAST('2022-01-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE)) OR (x < CAST('2024-01-01' AS DATE) AND x >= CAST('2023-01-01' AS DATE));

-- merge ranges
DATE_TRUNC('year', x) IN (CAST('2021-01-01' AS DATE), CAST('2022-01-01' AS DATE));
x < CAST('2023-01-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE);

-- one of the values will always be false
DATE_TRUNC('year', x) IN (CAST('2021-01-01' AS DATE), CAST('2022-01-02' AS DATE));
x < CAST('2022-01-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE);

TIMESTAMP_TRUNC(x, YEAR) = CAST('2021-01-01' AS DATETIME);
x < CAST('2022-01-01 00:00:00' AS DATETIME) AND x >= CAST('2021-01-01 00:00:00' AS DATETIME);

-- right side is not a date literal
DATE_TRUNC('day', x) = CAST(y AS DATE);
CAST(y AS DATE) = DATE_TRUNC('DAY', x);

-- nested cast
DATE_TRUNC('day', x) = CAST(CAST('2021-01-01 01:02:03' AS DATETIME) AS DATE);
x < CAST('2021-01-02' AS DATE) AND x >= CAST('2021-01-01' AS DATE);

TIMESTAMP_TRUNC(x, YEAR) = CAST(CAST('2021-01-01 01:02:03' AS DATE) AS DATETIME);
x < CAST('2022-01-01 00:00:00' AS DATETIME) AND x >= CAST('2021-01-01 00:00:00' AS DATETIME);

--------------------------------------
-- EQUALITY
--------------------------------------
x + 1 = 3;
x = 2;

1 + x = 3;
x = 2;

3 = x + 1;
x = 2;

x - 1 = 3;
x = 4;

x + 1 > 3;
x > 2;

x + 1 >= 3;
x >= 2;

x + 1 <= 3;
x <= 2;

x + 1 <= 3;
x <= 2;

x + 1 <> 3;
x <> 2;

1 + x + 1 = 3 + 1;
x = 2;

x - INTERVAL 1 DAY = CAST('2021-01-01' AS DATE);
x = CAST('2021-01-02' AS DATE);

x - INTERVAL 1 DAY = TS_OR_DS_TO_DATE('2021-01-01 00:00:01');
x = CAST('2021-01-02' AS DATE);

x - INTERVAL 1 HOUR > CAST('2021-01-01' AS DATETIME);
x > CAST('2021-01-01 01:00:00' AS DATETIME);

DATETIME_ADD(x, 1, HOUR) < CAST('2021-01-01' AS DATETIME);
x < CAST('2020-12-31 23:00:00' AS DATETIME);

DATETIME_SUB(x, 1, DAY) >= CAST('2021-01-01' AS DATETIME);
x >= CAST('2021-01-02 00:00:00' AS DATETIME);

DATE_ADD(x, 1, DAY) <= CAST('2021-01-01' AS DATE);
x <= CAST('2020-12-31' AS DATE);

DATE_SUB(x, 1, DAY) <> CAST('2021-01-01' AS DATE);
x <> CAST('2021-01-02' AS DATE);

DATE_ADD(DATE_ADD(DATE_TRUNC('week', DATE_SUB(x, 1, DAY)), 1, DAY), 1, YEAR) < CAST('2021-01-08' AS DATE);
x < CAST('2020-01-14' AS DATE);

x - INTERVAL '1' day = CAST(y AS DATE);
CAST(y AS DATE) = x - INTERVAL '1' DAY;

--------------------------------------
-- Constant Propagation
--------------------------------------
x = 5 AND y = x;
x = 5 AND y = 5;

5 = x AND y = x;
x = 5 AND y = 5;

x = 5 OR y = x;
x = 5 OR x = y;

(x = 5 AND y = x) OR y = 1;
(x = 5 AND y = 5) OR y = 1;

t.x = 5 AND y = x;
t.x = 5 AND x = y;

t.x = 'a' AND y = CONCAT_WS('-', t.x, 'b');
t.x = 'a' AND y = 'a-b';

x = 5 AND y = x AND y + 1 < 5;
FALSE;

x = 5 AND x = 6;
FALSE;

x = 5 AND (y = x OR z = 1);
x = 5 AND (x = y OR z = 1);

x = 5 AND x + 3 = 8;
x = 5;

x = 5 AND (SELECT x FROM t WHERE y = 1);
(SELECT x FROM t WHERE y = 1) AND x = 5;

x = 1 AND y > 0 AND (SELECT z = 5 FROM t WHERE y = 1);
(SELECT z = 5 FROM t WHERE y = 1) AND x = 1 AND y > 0;

x = 1 AND x = y AND (SELECT z FROM t WHERE a AND (b OR c));
(SELECT z FROM t WHERE a AND (b OR c)) AND x = 1 AND y = 1;

t1.a = 39 AND t2.b = t1.a AND t3.c = t2.b;
t1.a = 39 AND t2.b = 39 AND t3.c = 39;

x = 1 AND CASE WHEN x = 5 THEN FALSE ELSE TRUE END;
x = 1;

x = 1 AND IF(x = 5, FALSE, TRUE);
x = 1;

x = 1 AND CASE x WHEN 5 THEN FALSE ELSE TRUE END;
x = 1;

x = y AND CASE WHEN x = 5 THEN FALSE ELSE TRUE END;
CASE WHEN x = 5 THEN FALSE ELSE TRUE END AND x = y;

x = 1 AND CASE WHEN y = 5 THEN x = z END;
CASE WHEN y = 5 THEN z = 1 END AND x = 1;

--------------------------------------
-- Simplify Conditionals
--------------------------------------
IF(TRUE, x, y);
x;

IF(FALSE, x, y);
y;

IF(FALSE, x);
NULL;

IF(NULL, x, y);
y;

IF(cond, x, y);
CASE WHEN cond THEN x ELSE y END;

CASE WHEN TRUE THEN x ELSE y END;
x;

CASE WHEN FALSE THEN x ELSE y END;
y;

CASE WHEN FALSE THEN x WHEN FALSE THEN y WHEN TRUE THEN z END;
z;

CASE NULL WHEN NULL THEN x ELSE y END;
y;

CASE 4 WHEN 1 THEN x WHEN 2 THEN y WHEN 3 THEN z ELSE w END;
w;

CASE 4 WHEN 1 THEN x WHEN 2 THEN y WHEN 3 THEN z WHEN 4 THEN w END;
w;

CASE WHEN value = 1 THEN x ELSE y END;
CASE WHEN value = 1 THEN x ELSE y END;

CASE WHEN FALSE THEN x END;
NULL;

CASE 1 WHEN 1 + 1 THEN x END;
NULL;

CASE WHEN cond THEN x ELSE y END;
CASE WHEN cond THEN x ELSE y END;

CASE WHEN cond THEN x END;
CASE WHEN cond THEN x END;

CASE x WHEN y THEN z ELSE w END;
CASE WHEN x = y THEN z ELSE w END;

CASE x WHEN y THEN z END;
CASE WHEN x = y THEN z END;

CASE x1 + x2 WHEN x3 THEN x4 WHEN x5 + x6 THEN x7 ELSE x8 END;
CASE WHEN x3 = (x1 + x2) THEN x4 WHEN (x1 + x2) = (x5 + x6) THEN x7 ELSE x8 END;

--------------------------------------
-- Simplify STARTSWITH
--------------------------------------
STARTS_WITH('foo', 'f');
TRUE;

STARTS_WITH('foo', 'g');
FALSE;

STARTS_WITH('', 'f');
FALSE;

STARTS_WITH('', '');
TRUE;

STARTS_WITH('foo', '');
TRUE;

STARTS_WITH(NULL, y);
STARTS_WITH(NULL, y);

STARTS_WITH(x, y);
STARTS_WITH(x, y);

STARTS_WITH('x', y);
STARTS_WITH('x', y);

STARTS_WITH(x, 'y');
STARTS_WITH(x, 'y');
