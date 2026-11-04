# name: division
SELECT 5 / 2, -5 / 2, 5 DIV 2, -5 DIV 2, 5 % 3, -5 % 3, PMOD(-5, 3);

# name: column_division
SELECT id, v_int4 / 7, v_int4 DIV 7, v_int8 / 1000 FROM scalars WHERE v_int4 IS NOT NULL ORDER BY id;

# name: decimal_division
SELECT 1.0 / 3, v_dec / 3 FROM scalars WHERE id = 3;

# name: rounding
SELECT ROUND(2.5), ROUND(-2.5), ROUND(3.14159, 2), ROUND(1234.5, -2), BROUND(2.5), BROUND(3.5), CEIL(1.2), FLOOR(-1.2);

# name: round_double
SELECT ROUND(CAST(2.5 AS DOUBLE)), ROUND(CAST(3.5 AS DOUBLE)), ROUND(CAST(-0.5 AS DOUBLE));

# name: math_functions
SELECT ABS(-3), SIGN(-3.5), POWER(2, 10), SQRT(16.0), EXP(0), LN(1), LOG(100), LOG(2, 8), LOG10(1000), LOG2(8), CBRT(27.0), FACTORIAL(5);

# name: bitwise
SELECT 12 & 10, 12 | 10, 12 ^ 10, ~12, SHIFTLEFT(1, 4), SHIFTRIGHT(256, 2), BIT_COUNT(7);

# name: greatest_least
SELECT GREATEST(1, 5, 3), LEAST(4, 2, 8), GREATEST(1, NULL, 3), LEAST(NULL, 2);

# name: float_arithmetic
SELECT id, v_float8 + 1, v_float8 * 2, -v_float8 FROM scalars WHERE id IN (3, 5, 8, 9, 10, 11, 12) ORDER BY id;

# name: nan_handling
SELECT id, ISNAN(v_float8), NANVL(v_float8, 0.0), v_float8 = DOUBLE('NaN') FROM scalars WHERE v_float8 IS NOT NULL ORDER BY id;

# name: try_arithmetic
SELECT TRY_DIVIDE(1, 0), TRY_ADD(2147483647, 1), TRY_MULTIPLY(2, 3);

# name: base_conversions
SELECT CONV('ff', 16, 10), BIN(5), HEX(255), UNHEX('41');

# name: mod_and_sign
SELECT MOD(10, 3), MOD(-10, 3), POSITIVE(-1), NEGATIVE(1), SIGNUM(-2);
