# name: extract_date_parts
SELECT id, EXTRACT(YEAR FROM v_date), EXTRACT(MONTH FROM v_date), EXTRACT(DAY FROM v_date), EXTRACT(QUARTER FROM v_date) FROM scalars ORDER BY id;

# name: extract_weekday
SELECT id, EXTRACT(DOW FROM v_date), EXTRACT(ISODOW FROM v_date), EXTRACT(DOY FROM v_date), EXTRACT(WEEK FROM v_date) FROM scalars ORDER BY id;

# name: extract_time_parts
SELECT id, EXTRACT(HOUR FROM v_ts), EXTRACT(MINUTE FROM v_ts), EXTRACT(SECOND FROM v_ts) FROM scalars ORDER BY id;

# name: date_part
SELECT id, DATE_PART('year', v_ts), DATE_PART('month', v_tstz) FROM scalars ORDER BY id;

# name: epoch
SELECT id, EXTRACT(EPOCH FROM v_tstz) FROM scalars WHERE v_tstz > TIMESTAMPTZ '1900-01-01 00:00:00+00' ORDER BY id;

# name: date_trunc
SELECT id, DATE_TRUNC('year', v_ts), DATE_TRUNC('month', v_ts), DATE_TRUNC('week', v_ts), DATE_TRUNC('hour', v_ts) FROM scalars ORDER BY id;

# name: date_trunc_tstz
SELECT id, DATE_TRUNC('day', v_tstz), DATE_TRUNC('month', v_tstz) FROM scalars ORDER BY id;

# name: date_integer_arithmetic
SELECT id, v_date + 1, v_date - 30 FROM scalars WHERE v_date BETWEEN DATE '1900-01-01' AND DATE '2100-12-31' ORDER BY id;

# name: date_difference
SELECT id, v_date - DATE '2020-01-01' FROM scalars WHERE v_date IS NOT NULL ORDER BY id;

# name: interval_arithmetic
SELECT id, v_ts + INTERVAL '1 day 2 hours', v_ts - INTERVAL '30 minutes', v_date + INTERVAL '1 month' FROM scalars WHERE v_date BETWEEN DATE '1900-01-01' AND DATE '2100-12-31' ORDER BY id;

# name: interval_units
SELECT id, v_ts + INTERVAL '1 year', v_ts + INTERVAL '3 weeks', v_ts - INTERVAL '90 seconds' FROM scalars WHERE v_ts BETWEEN TIMESTAMP '1900-01-01' AND TIMESTAMP '2100-12-31' ORDER BY id;

# name: to_char
SELECT id, TO_CHAR(v_ts, 'YYYY-MM-DD HH24:MI:SS'), TO_CHAR(v_date, 'DD/MM/YYYY'), TO_CHAR(v_ts, 'Mon DD, YYYY HH12:MI AM') FROM scalars WHERE v_ts > TIMESTAMP '1900-01-01' ORDER BY id;

# name: to_date_timestamp
SELECT TO_DATE('2024-03-15', 'YYYY-MM-DD'), TO_TIMESTAMP('2024-03-15 10:20:30', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('15/03/2024', 'DD/MM/YYYY');

# name: make_date
SELECT MAKE_DATE(2024, 2, 29), MAKE_TIMESTAMP(2024, 2, 29, 12, 30, 45.5);

# name: at_time_zone
SELECT id, v_tstz AT TIME ZONE 'America/New_York', v_ts AT TIME ZONE 'Asia/Kolkata' FROM scalars WHERE v_ts BETWEEN TIMESTAMP '1900-01-01' AND TIMESTAMP '2100-01-01' ORDER BY id;

# name: temporal_comparisons
SELECT id, v_date < DATE '2000-01-01', v_ts >= TIMESTAMP '2024-01-01', v_tstz BETWEEN TIMESTAMPTZ '2020-01-01 00:00:00+00' AND TIMESTAMPTZ '2025-01-01 00:00:00+00' FROM scalars ORDER BY id;

# name: last_day_of_month
SELECT id, CAST(DATE_TRUNC('month', v_date) + INTERVAL '1 month' - INTERVAL '1 day' AS DATE) FROM scalars WHERE v_date BETWEEN DATE '1900-01-01' AND DATE '2100-12-31' ORDER BY id;
