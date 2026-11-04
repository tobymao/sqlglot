# name: basic
SELECT COUNT(*), COUNT(qty), COUNT(DISTINCT grp), SUM(qty), AVG(qty), MIN(qty), MAX(qty) FROM facts;

# name: decimal_and_float
SELECT SUM(price), AVG(price), MIN(price), MAX(price), SUM(score), AVG(score) FROM facts;

# name: group_by
SELECT grp, COUNT(*), SUM(price), AVG(qty), MIN(d), MAX(ts) FROM facts GROUP BY grp ORDER BY grp;

# name: group_by_text
SELECT cat, COUNT(*), COUNT(DISTINCT grp) FROM facts GROUP BY cat ORDER BY cat;

# name: statistics
SELECT STDDEV(score), VARIANCE(score), STDDEV_POP(score), VAR_POP(score), STDDEV_SAMP(qty) FROM facts;

# name: correlation
SELECT CORR(qty, score), COVAR_POP(qty, score), COVAR_SAMP(qty, score), REGR_SLOPE(score, qty) FROM facts;

# name: boolean_aggregates
SELECT grp, BOOL_AND(flag), BOOL_OR(flag), EVERY(flag) FROM facts GROUP BY grp ORDER BY grp;

# name: string_agg
SELECT grp, STRING_AGG(cat, ',' ORDER BY id) FROM facts WHERE id <= 40 GROUP BY grp ORDER BY grp;

# name: array_agg
SELECT grp, ARRAY_AGG(qty ORDER BY id) FROM facts WHERE id <= 40 GROUP BY grp ORDER BY grp;

# name: filter_clause
SELECT grp, COUNT(*) FILTER (WHERE flag), SUM(qty) FILTER (WHERE cat = 'alpha') FROM facts GROUP BY grp ORDER BY grp;

# name: percentiles
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY qty), PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY price) FROM facts;

# name: rollup
SELECT grp, cat, COUNT(*) FROM facts WHERE grp < 3 AND cat IS NOT NULL GROUP BY ROLLUP (grp, cat) ORDER BY grp, cat;

# name: cube
SELECT flag, cat, COUNT(*) FROM facts WHERE flag IS NOT NULL AND cat IS NOT NULL GROUP BY CUBE (flag, cat) ORDER BY flag, cat;

# name: grouping_sets
SELECT grp, flag, COUNT(*), GROUPING(grp, flag) FROM facts WHERE grp < 3 AND flag IS NOT NULL GROUP BY GROUPING SETS ((grp), (flag), ()) ORDER BY grp, flag;

# name: having
SELECT grp, COUNT(*) FROM facts GROUP BY grp HAVING COUNT(*) > 100 ORDER BY grp;

# name: distinct_aggregates
SELECT SUM(DISTINCT grp), AVG(DISTINCT qty), COUNT(DISTINCT cat) FROM facts;

# name: aggregate_expressions
SELECT grp, SUM(qty * price), MAX(qty) - MIN(qty), COUNT(*) * 2 FROM facts GROUP BY grp ORDER BY grp;

# name: empty_input
SELECT COUNT(*), SUM(qty), AVG(qty), MAX(cat) FROM facts WHERE id < 0;

# name: count_if_style
SELECT SUM(CASE WHEN flag THEN 1 ELSE 0 END), COUNT(CASE WHEN qty > 500 THEN 1 END) FROM facts;
