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
1.0 = 1;

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

--------------------------------------
-- Absorption
--------------------------------------
A AND (A OR B);
A;

A AND (B OR A);
A;

(A OR B) AND A;
A;

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
(1 <> 2);

(('a' = 'a') AND TRUE and NOT FALSE);
TRUE;
