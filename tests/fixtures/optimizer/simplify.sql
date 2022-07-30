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

NULL = NULL;
NULL;

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
TRUE;

((NOT FALSE) AND (x = x)) AND (FALSE OR 1 <> 2);
TRUE;

(('a' = 'a') AND TRUE and NOT FALSE);
TRUE;

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

0.06 - 0.01;
0.05;

3 * 4;
12;

3.0 * 9;
27.0;

0.03 * 0.73;
0.0219;

1 / 3;
0;

20.0 / 6;
3.333333333333333333333333333;

10 / 5;
2;

(1.0 * 3) * 4 - 2 * (5 / 2);
8.0;

6 - 2 + 4 * 2 + a;
12 + a;

a + 1 + 1 + 2;
a + 4;

a + (1 + 1) + (10);
a + 12;

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
