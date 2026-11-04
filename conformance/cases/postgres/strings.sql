# name: length_and_case
SELECT id, LENGTH(v_text), UPPER(v_text), LOWER(v_varchar) FROM scalars ORDER BY id;

# name: initcap
SELECT id, INITCAP(v_text), INITCAP(v_varchar) FROM scalars ORDER BY id;

# name: substring
SELECT id, SUBSTRING(v_text, 2, 3), SUBSTRING(v_text FROM 2), SUBSTR(v_text, 3) FROM scalars ORDER BY id;

# name: left_right
SELECT id, LEFT(v_text, 2), RIGHT(v_text, 2), LEFT(v_text, -2), RIGHT(v_text, -2) FROM scalars ORDER BY id;

# name: position
SELECT id, POSITION('l' IN v_text), STRPOS(v_text, 'o') FROM scalars ORDER BY id;

# name: trim
SELECT id, TRIM(v_text), LTRIM(v_text), RTRIM(v_text), TRIM(BOTH 'x' FROM v_varchar), BTRIM(v_text, 'a') FROM scalars ORDER BY id;

# name: concatenation
SELECT id, v_text || '-' || v_varchar, CONCAT(v_text, v_varchar), CONCAT_WS(',', v_text, NULL, v_varchar) FROM scalars ORDER BY id;

# name: replace_reverse_repeat
SELECT id, REPLACE(v_text, 'l', 'L'), REVERSE(v_text), REPEAT(v_varchar, 2) FROM scalars ORDER BY id;

# name: padding
SELECT id, LPAD(v_varchar, 8, '*'), RPAD(v_varchar, 8, '*'), LPAD(v_varchar, 3) FROM scalars ORDER BY id;

# name: like
SELECT id, v_text LIKE '%o%', v_text ILIKE 'H%', v_text NOT LIKE '_e%', v_text LIKE '50\%%' FROM scalars ORDER BY id;

# name: regex
SELECT id, v_text ~ '^[a-z]+', v_text ~* '^h', REGEXP_REPLACE(v_text, '[aeiou]', '#', 'g'), REGEXP_REPLACE(v_text, '[aeiou]', '#') FROM scalars ORDER BY id;

# name: split_part
SELECT id, SPLIT_PART(v_text, ' ', 2), SPLIT_PART(v_text, ' ', 1) FROM scalars ORDER BY id;

# name: character_codes
SELECT id, ASCII(v_text), CHR(65), MD5(v_text) FROM scalars ORDER BY id;

# name: byte_lengths
SELECT id, OCTET_LENGTH(v_text), BIT_LENGTH(v_text), CHAR_LENGTH(v_text) FROM scalars ORDER BY id;

# name: translate_starts_with
SELECT id, TRANSLATE(v_text, 'lo', '01'), STARTS_WITH(v_text, 'he') FROM scalars ORDER BY id;

# name: char_semantics
SELECT id, LENGTH(v_char), v_char = 'ab', CAST(v_char AS TEXT), v_char || '|' FROM scalars ORDER BY id;

# name: collation_order
SELECT id, v_varchar FROM scalars WHERE id BETWEEN 21 AND 26 ORDER BY v_varchar;

# name: string_comparison
SELECT id, v_text < v_varchar, v_text = LOWER(v_varchar) FROM scalars ORDER BY id;
