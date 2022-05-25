--------------------------------------
-- Conditions
--------------------------------------
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

'x' = 'y';
'x' = 'y';

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

((NOT FALSE) AND (x = x)) AND (TRUE OR 1 != 2);
TRUE;

((NOT FALSE) AND (x = x)) AND (FALSE OR 1 <> 2);
TRUE AND (1 <> 2)
