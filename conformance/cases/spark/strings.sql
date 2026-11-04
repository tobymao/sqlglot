# name: length_and_case
SELECT id, LENGTH(v_text), CHAR_LENGTH(v_text), UPPER(v_text), LOWER(v_varchar), UCASE(v_text), LCASE(v_text) FROM scalars ORDER BY id;

# name: substring
SELECT id, SUBSTRING(v_text, 2, 3), SUBSTR(v_text, 3), SUBSTRING(v_text, -3), SUBSTR(v_text, 0, 2) FROM scalars ORDER BY id;

# name: left_right
SELECT id, LEFT(v_text, 2), RIGHT(v_text, 2) FROM scalars ORDER BY id;

# name: instr_locate
SELECT id, INSTR(v_text, 'o'), LOCATE('l', v_text), LOCATE('l', v_text, 4) FROM scalars ORDER BY id;

# name: trim
SELECT id, TRIM(v_text), LTRIM(v_text), RTRIM(v_text), TRIM(BOTH 'x' FROM v_varchar), TRIM(LEADING ' ' FROM v_text) FROM scalars ORDER BY id;

# name: concatenation
SELECT id, CONCAT(v_text, v_varchar), CONCAT_WS(',', v_text, NULL, v_varchar), v_text || v_varchar FROM scalars ORDER BY id;

# name: replace_reverse_repeat
SELECT id, REPLACE(v_text, 'l', 'L'), REVERSE(v_text), REPEAT(v_varchar, 2) FROM scalars ORDER BY id;

# name: padding
SELECT id, LPAD(v_varchar, 8, '*'), RPAD(v_varchar, 8, '*'), LPAD(v_varchar, 3) FROM scalars ORDER BY id;

# name: like_rlike
SELECT id, v_text LIKE '%o%', v_text ILIKE 'H%', v_text RLIKE '^[a-z]+', v_text REGEXP 'o$' FROM scalars ORDER BY id;

# name: regexp_functions
SELECT id, REGEXP_REPLACE(v_text, '[aeiou]', '#'), REGEXP_EXTRACT(v_text, '([a-z]+) ([a-z]+)', 2), REGEXP_LIKE(v_text, 'l+') FROM scalars ORDER BY id;

# name: split
SELECT id, SPLIT(v_text, ' '), SPLIT_PART(v_text, ' ', 2), SPLIT(v_text, ' ')[0] FROM scalars ORDER BY id;

# name: hashes
SELECT id, ASCII(v_text), CHR(65), MD5(v_text), SHA1(v_text), SHA2(v_text, 256) FROM scalars ORDER BY id;

# name: misc_functions
SELECT id, SOUNDEX(v_text), LEVENSHTEIN(v_text, 'hello'), TRANSLATE(v_text, 'lo', '01') FROM scalars ORDER BY id;

# name: initcap
SELECT id, INITCAP(v_text), INITCAP(v_varchar) FROM scalars ORDER BY id;

# name: string_predicates
SELECT id, STARTSWITH(v_text, 'he'), ENDSWITH(v_text, 'ld'), CONTAINS(v_text, 'o') FROM scalars ORDER BY id;

# name: char_semantics
SELECT id, LENGTH(v_char), v_char = 'ab', CONCAT(v_char, '|') FROM scalars ORDER BY id;

# name: collation_order
SELECT id, v_varchar FROM scalars WHERE id BETWEEN 21 AND 26 ORDER BY v_varchar;

# name: format_string
SELECT FORMAT_STRING('%d-%s', 5, 'x'), PRINTF('%05.2f', CAST(3.14159 AS DOUBLE));

# name: substring_index
SELECT id, SUBSTRING_INDEX(v_text, ' ', 1), SUBSTRING_INDEX(v_text, ' ', -1) FROM scalars ORDER BY id;
