# name: basic
SELECT COUNT(*), COUNT(qty), COUNT(DISTINCT grp), SUM(qty), AVG(qty), MIN(qty), MAX(qty) FROM facts;

# name: decimal_and_float
SELECT SUM(price), AVG(price), SUM(score), AVG(score) FROM facts;

# name: group_by
SELECT grp, COUNT(*), SUM(price), AVG(qty), MIN(d), MAX(ts) FROM facts GROUP BY grp ORDER BY grp;

# name: statistics
SELECT STDDEV(score), VARIANCE(score), STDDEV_POP(score), VAR_POP(score), SKEWNESS(score), KURTOSIS(score) FROM facts;

# name: conditional_aggregates
SELECT grp, COUNT_IF(flag), SUM(IF(cat = 'alpha', qty, 0)), BOOL_AND(flag), BOOL_OR(flag), EVERY(flag), SOME(flag) FROM facts GROUP BY grp ORDER BY grp;

# name: collect
SELECT grp, SORT_ARRAY(COLLECT_LIST(qty)), SIZE(COLLECT_SET(cat)) FROM facts WHERE id <= 40 GROUP BY grp ORDER BY grp;

# name: percentiles
SELECT PERCENTILE(qty, 0.5), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score), MEDIAN(qty) FROM facts;

# name: max_by_min_by
SELECT grp, MAX_BY(qty, id), MIN_BY(qty, id) FROM facts GROUP BY grp ORDER BY grp;

# name: rollup
SELECT grp, cat, COUNT(*) FROM facts WHERE grp < 3 AND cat IS NOT NULL GROUP BY ROLLUP (grp, cat) ORDER BY grp, cat;

# name: grouping_id
SELECT grp, flag, COUNT(*), GROUPING_ID(grp, flag), GROUPING(grp) FROM facts WHERE grp < 3 AND flag IS NOT NULL GROUP BY grp, flag WITH ROLLUP ORDER BY grp, flag;

# name: having_alias
SELECT grp, COUNT(*) AS c FROM facts GROUP BY grp HAVING c > 100 ORDER BY grp;

# name: filter_clause
SELECT grp, COUNT(*) FILTER (WHERE flag), SUM(qty) FILTER (WHERE cat = 'alpha') FROM facts GROUP BY grp ORDER BY grp;

# name: group_by_all
SELECT grp, flag, COUNT(*) FROM facts GROUP BY ALL ORDER BY grp, flag;

# name: empty_input
SELECT COUNT(*), SUM(qty), AVG(qty), MAX(cat) FROM facts WHERE id < 0;

# name: string_agg
SELECT grp, STRING_AGG(cat, ',') WITHIN GROUP (ORDER BY id) FROM facts WHERE id <= 40 GROUP BY grp ORDER BY grp;
