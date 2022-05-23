SELECT 1 FROM z;
SELECT 1 FROM "c"."db"."z" AS "z";

SELECT 1 FROM y.z;
SELECT 1 FROM "c"."y"."z" AS "z";

SELECT 1 FROM x.y.z;
SELECT 1 FROM "x"."y"."z" AS "z";

SELECT 1 FROM "x"."y"."z" AS "z";
SELECT 1 FROM "x"."y"."z" AS "z";
