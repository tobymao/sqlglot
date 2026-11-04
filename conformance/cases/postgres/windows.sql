# name: row_number
SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) FROM facts WHERE id <= 200 ORDER BY id;

# name: rank
SELECT id, RANK() OVER (ORDER BY grp), DENSE_RANK() OVER (ORDER BY grp) FROM facts WHERE id <= 200 ORDER BY id;

# name: rank_desc
SELECT id, RANK() OVER (ORDER BY qty DESC), DENSE_RANK() OVER (PARTITION BY cat ORDER BY qty DESC) FROM facts WHERE id <= 200 ORDER BY id;

# name: running_sum
SELECT id, SUM(qty) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM facts WHERE id <= 200 ORDER BY id;

# name: running_total_default_frame
SELECT id, SUM(qty) OVER (ORDER BY grp), COUNT(*) OVER (ORDER BY grp) FROM facts WHERE id <= 200 ORDER BY id;

# name: lag_lead
SELECT id, LAG(qty) OVER (ORDER BY id), LEAD(qty, 2, 0) OVER (ORDER BY id), LAG(cat, 1, 'none') OVER (PARTITION BY grp ORDER BY id) FROM facts WHERE id <= 200 ORDER BY id;

# name: first_last_value
SELECT id, FIRST_VALUE(qty) OVER (PARTITION BY grp ORDER BY id), LAST_VALUE(qty) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), NTH_VALUE(qty, 2) OVER (PARTITION BY grp ORDER BY id) FROM facts WHERE id <= 200 ORDER BY id;

# name: distribution
SELECT id, NTILE(4) OVER (ORDER BY id), PERCENT_RANK() OVER (ORDER BY qty), CUME_DIST() OVER (ORDER BY qty) FROM facts WHERE id <= 200 ORDER BY id;

# name: partition_aggregate
SELECT id, AVG(price) OVER (PARTITION BY cat), MAX(ts) OVER (PARTITION BY grp) FROM facts WHERE id <= 200 ORDER BY id;

# name: range_frame
SELECT id, SUM(qty) OVER (ORDER BY d RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW) FROM facts WHERE id <= 200 ORDER BY id;

# name: named_window
SELECT id, SUM(qty) OVER w, ROW_NUMBER() OVER w FROM facts WHERE id <= 100 WINDOW w AS (PARTITION BY grp ORDER BY id) ORDER BY id;

# name: qualify_via_subquery
SELECT id, grp FROM (SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY qty DESC, id) AS rn FROM facts) AS t WHERE rn = 1 ORDER BY grp;
