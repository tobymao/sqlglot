# name: row_number
SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) FROM facts WHERE id <= 200 ORDER BY id;

# name: rank
SELECT id, RANK() OVER (ORDER BY grp), DENSE_RANK() OVER (ORDER BY grp) FROM facts WHERE id <= 200 ORDER BY id;

# name: running_sum
SELECT id, SUM(qty) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM facts WHERE id <= 200 ORDER BY id;

# name: lag_lead
SELECT id, LAG(qty) OVER (ORDER BY id), LEAD(qty, 2, 0) OVER (ORDER BY id) FROM facts WHERE id <= 200 ORDER BY id;

# name: ignore_nulls
SELECT id, FIRST_VALUE(qty, TRUE) OVER (PARTITION BY grp ORDER BY id), LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id) FROM facts WHERE id <= 200 ORDER BY id;

# name: distribution
SELECT id, NTILE(4) OVER (ORDER BY id), PERCENT_RANK() OVER (ORDER BY qty), CUME_DIST() OVER (ORDER BY qty) FROM facts WHERE id <= 200 ORDER BY id;

# name: range_frame
SELECT id, SUM(qty) OVER (ORDER BY id RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) FROM facts WHERE id <= 200 ORDER BY id;

# name: partition_aggregate
SELECT id, AVG(price) OVER (PARTITION BY cat), COUNT(*) OVER () FROM facts WHERE id <= 200 ORDER BY id;
