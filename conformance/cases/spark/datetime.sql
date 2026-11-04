# name: date_parts
SELECT id, YEAR(v_date), MONTH(v_date), DAY(v_date), QUARTER(v_date), DAYOFWEEK(v_date), DAYOFYEAR(v_date), WEEKOFYEAR(v_date) FROM scalars ORDER BY id;

# name: time_parts
SELECT id, HOUR(v_ts), MINUTE(v_ts), SECOND(v_ts) FROM scalars ORDER BY id;

# name: extract
SELECT id, EXTRACT(YEAR FROM v_date), EXTRACT(DOW FROM v_date), EXTRACT(SECOND FROM v_ts) FROM scalars ORDER BY id;

# name: date_add_sub
SELECT id, DATE_ADD(v_date, 1), DATE_SUB(v_date, 30), ADD_MONTHS(v_date, 1) FROM scalars WHERE v_date BETWEEN DATE'1900-01-01' AND DATE'2100-12-31' ORDER BY id;

# name: datediff
SELECT id, DATEDIFF(v_date, DATE'2020-01-01'), DATEDIFF(DATE'2024-03-01', v_date) FROM scalars WHERE v_date IS NOT NULL ORDER BY id;

# name: months_between
SELECT id, MONTHS_BETWEEN(v_date, DATE'2020-01-15') FROM scalars WHERE v_date BETWEEN DATE'1900-01-01' AND DATE'2100-12-31' ORDER BY id;

# name: trunc
SELECT id, TRUNC(v_date, 'MM'), TRUNC(v_date, 'YEAR'), DATE_TRUNC('HOUR', v_ts), DATE_TRUNC('WEEK', v_ts) FROM scalars ORDER BY id;

# name: last_day_next_day
SELECT id, LAST_DAY(v_date), NEXT_DAY(v_date, 'MON') FROM scalars WHERE v_date BETWEEN DATE'1900-01-01' AND DATE'2100-12-31' ORDER BY id;

# name: date_format
SELECT id, DATE_FORMAT(v_ts, 'yyyy-MM-dd HH:mm:ss'), DATE_FORMAT(v_date, 'dd/MM/yyyy'), DATE_FORMAT(v_ts, 'MMM d, yyyy h:mm a') FROM scalars WHERE v_ts > TIMESTAMP_NTZ'1900-01-01 00:00:00' ORDER BY id;

# name: to_date_timestamp
SELECT TO_DATE('2024-03-15'), TO_DATE('15/03/2024', 'dd/MM/yyyy'), TO_TIMESTAMP('2024-03-15 10:20:30'), TO_TIMESTAMP('2024-03-15 10:20', 'yyyy-MM-dd HH:mm');

# name: unix_time
SELECT id, UNIX_TIMESTAMP(v_tstz), UNIX_SECONDS(v_tstz), UNIX_MICROS(v_tstz) FROM scalars WHERE v_tstz > TIMESTAMP'1900-01-01 00:00:00' ORDER BY id;

# name: from_unixtime
SELECT FROM_UNIXTIME(0), FROM_UNIXTIME(1700000000, 'yyyy-MM-dd'), TIMESTAMP_SECONDS(1700000000), TIMESTAMP_MILLIS(1700000000123);

# name: make_date
SELECT MAKE_DATE(2024, 2, 29), MAKE_TIMESTAMP(2024, 2, 29, 12, 30, 45.5);

# name: timezone_conversion
SELECT id, FROM_UTC_TIMESTAMP(v_tstz, 'America/New_York'), TO_UTC_TIMESTAMP(v_tstz, 'Asia/Kolkata') FROM scalars WHERE v_tstz BETWEEN TIMESTAMP'1900-01-01 00:00:00' AND TIMESTAMP'2100-01-01 00:00:00' ORDER BY id;

# name: interval_arithmetic
SELECT id, v_ts + INTERVAL 1 DAY, v_ts - INTERVAL '30' MINUTE, v_date + INTERVAL 1 MONTH FROM scalars WHERE v_date BETWEEN DATE'1900-01-01' AND DATE'2100-12-31' ORDER BY id;

# name: date_integer_arithmetic
SELECT id, v_date + 1, v_date - 1 FROM scalars WHERE v_date BETWEEN DATE'1900-01-01' AND DATE'2100-12-31' ORDER BY id;

# name: timestampadd_diff
SELECT TIMESTAMPADD(HOUR, 3, TIMESTAMP'2024-01-01 00:00:00'), TIMESTAMPDIFF(DAY, DATE'2024-01-01', DATE'2024-03-01'), DATEADD(MONTH, 1, DATE'2024-01-31');
