# name: integer_division
SELECT 5 / 2, -5 / 2, 5 % 3, -5 % 3, 5.0 / 2;

# name: column_division
SELECT id, v_int4 / 7, v_int4 % 7, v_int8 / 1000 FROM scalars WHERE v_int4 IS NOT NULL ORDER BY id;

# name: decimal_division
SELECT 1.0 / 3, 10 / 4.0, v_dec / 3, v_dec38 / 7 FROM scalars WHERE id = 3;

# name: rounding
SELECT ROUND(2.5), ROUND(-2.5), ROUND(3.14159, 2), ROUND(1234.5, -2), TRUNC(3.789, 1), TRUNC(-3.789), CEIL(1.2), CEIL(-1.2), FLOOR(1.8), FLOOR(-1.8);

# name: round_double
SELECT ROUND(CAST(2.5 AS DOUBLE PRECISION)), ROUND(CAST(3.5 AS DOUBLE PRECISION)), ROUND(CAST(-0.5 AS DOUBLE PRECISION));

# name: math_functions
SELECT ABS(-3), SIGN(-3.5), POWER(2, 10), SQRT(16.0), EXP(0), LN(1), LOG(100), LOG(2, 8), MOD(10, 3), CBRT(27.0);

# name: trigonometry
SELECT PI(), DEGREES(PI()), RADIANS(180.0), SIN(0), COS(0), ATAN2(1, 1);

# name: power_operator
SELECT 2 ^ 10, 2.0 ^ 0.5;

# name: bitwise
SELECT 12 & 10, 12 | 10, 12 # 10, ~12, 1 << 4, 256 >> 2;

# name: greatest_least
SELECT GREATEST(1, 5, 3), LEAST(4, 2, 8), GREATEST(1, NULL, 3), LEAST(NULL, 2);

# name: float_arithmetic
SELECT id, v_float8 + 1, v_float8 * 2, -v_float8, v_float4 * 2 FROM scalars WHERE id IN (3, 5, 8, 9, 10, 11, 12) ORDER BY id;

# name: decimal_arithmetic
SELECT id, v_dec + 1, v_dec * 2, v_dec38 - 1, v_dec * v_dec FROM scalars WHERE v_dec IS NOT NULL ORDER BY id;

# name: nan_comparisons
SELECT id, v_float8 > 1, v_float8 = CAST('NaN' AS DOUBLE PRECISION) FROM scalars WHERE v_float8 IS NOT NULL ORDER BY id;

# name: mixed_type_arithmetic
SELECT 1 + 1.5, 2 * 2.5, 10 - 0.5, v_int4 + v_dec FROM scalars WHERE id = 3;
