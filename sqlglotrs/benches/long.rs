use std::path::Path;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sqlglotrs::settings::{TokenTypeSettings, TokenizerDialectSettings, TokenizerSettings};
use sqlglotrs::tokenizer::Tokenizer;

pub const LONG: &str = r#"
SELECT
  "e"."employee_id" AS "Employee #",
  "e"."first_name" || ' ' || "e"."last_name" AS "Name",
  "e"."email" AS "Email",
  "e"."phone_number" AS "Phone",
  TO_CHAR("e"."hire_date", 'MM/DD/YYYY') AS "Hire Date",
  TO_CHAR("e"."salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS "Salary",
  "e"."commission_pct" AS "Commission %",
  'works as ' || "j"."job_title" || ' in ' || "d"."department_name" || ' department (manager: ' || "dm"."first_name" || ' ' || "dm"."last_name" || ') and immediate supervisor: ' || "m"."first_name" || ' ' || "m"."last_name" AS "Current Job",
  TO_CHAR("j"."min_salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') || ' - ' || TO_CHAR("j"."max_salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS "Current Salary",
  "l"."street_address" || ', ' || "l"."postal_code" || ', ' || "l"."city" || ', ' || "l"."state_province" || ', ' || "c"."country_name" || ' (' || "r"."region_name" || ')' AS "Location",
  "jh"."job_id" AS "History Job ID",
  'worked from ' || TO_CHAR("jh"."start_date", 'MM/DD/YYYY') || ' to ' || TO_CHAR("jh"."end_date", 'MM/DD/YYYY') || ' as ' || "jj"."job_title" || ' in ' || "dd"."department_name" || ' department' AS "History Job Title",
  case when 1 then 1 when 2 then 2 when 3 then 3 when 4 then 4 when 5 then 5 else a(b(c + 1 * 3 % 4)) end
FROM "employees" AS e
JOIN "jobs" AS j
  ON "e"."job_id" = "j"."job_id"
LEFT JOIN "employees" AS m
  ON "e"."manager_id" = "m"."employee_id"
LEFT JOIN "departments" AS d
  ON "d"."department_id" = "e"."department_id"
LEFT JOIN "employees" AS dm
  ON "d"."manager_id" = "dm"."employee_id"
LEFT JOIN "locations" AS l
  ON "d"."location_id" = "l"."location_id"
LEFT JOIN "countries" AS c
  ON "l"."country_id" = "c"."country_id"
LEFT JOIN "regions" AS r
  ON "c"."region_id" = "r"."region_id"
LEFT JOIN "job_history" AS jh
  ON "e"."employee_id" = "jh"."employee_id"
LEFT JOIN "jobs" AS jj
  ON "jj"."job_id" = "jh"."job_id"
LEFT JOIN "departments" AS dd
  ON "dd"."department_id" = "jh"."department_id"
ORDER BY
  "e"."employee_id"
"#;

fn long(c: &mut Criterion) {
    // Read tokenizer settings
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("benches");
    let settings_file = std::fs::read_to_string(path.join("tokenizer_settings.json")).unwrap();
    let tokenizer_settings = serde_json::from_str::<TokenizerSettings>(&settings_file).unwrap();

    let settings_type_file =
        std::fs::read_to_string(path.join("token_type_settings.json")).unwrap();
    let settings_type_file =
        serde_json::from_str::<TokenTypeSettings>(&settings_type_file).unwrap();

    let dialect_settings = std::fs::read_to_string(path.join("dialect_settings.json")).unwrap();
    let dialect_settings =
        serde_json::from_str::<TokenizerDialectSettings>(&dialect_settings).unwrap();
    let tokenizer = Tokenizer::new(tokenizer_settings, settings_type_file);

    c.bench_function("long", |b| {
        b.iter(|| black_box(tokenizer.tokenize(LONG, &dialect_settings)));
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = long
}

criterion_main!(benches);
