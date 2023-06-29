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

SELECT x FROM y LEFT JOIN z ON TRUE;
SELECT x FROM y CROSS JOIN z;

SELECT x FROM y JOIN z USING (x);
SELECT x FROM y JOIN z USING (x);

--------------------------------------
-- Parenthesis removal
--------------------------------------
(TRUE);
TRUE;

(FALSE);
FALSE;

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

date '1998-12-01' - interval '90' day;
CAST('1998-09-02' AS DATE);

date '1998-12-01' + interval '1' week;
CAST('1998-12-08' AS DATE);

interval '1' year + date '1998-01-01';
CAST('1999-01-01' AS DATE);

interval '1' year + date '1998-01-01' + 3 * 7 * 4;
CAST('1999-01-01' AS DATE) + 84;

date '1998-12-01' - interval '90' foo;
CAST('1998-12-01' AS DATE) - INTERVAL '90' foo;

date '1998-12-01' + interval '90' foo;
CAST('1998-12-01' AS DATE) + INTERVAL '90' foo;

CAST(x AS DATE) + interval '1' week;
CAST(x AS DATE) + INTERVAL '1' week;

CAST('2008-11-11' AS DATETIME) + INTERVAL '5' MONTH;
CAST('2009-04-11 00:00:00' AS DATETIME);

datetime '1998-12-01' - interval '90' day;
CAST('1998-09-02 00:00:00' AS DATETIME);

CAST(x AS DATETIME) + interval '1' week;
CAST(x AS DATETIME) + INTERVAL '1' week;

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

1 < x AND 3 < x;
x > 3;

'a' < 'b';
TRUE;

x = 2018 OR x <> 2018;
x <> 2018 OR x = 2018;
