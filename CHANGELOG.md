Changelog
=========

## [v27.22.1] - 2025-10-08
### :boom: BREAKING CHANGES
- due to [`7ac01c2`](https://github.com/tobymao/sqlglot/commit/7ac01c2ae9bc4375efb63c60e3221e85088fdd1f) - bump sqlglotrs to 0.7.1 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.7.1


### :wrench: Chores
- [`7ac01c2`](https://github.com/tobymao/sqlglot/commit/7ac01c2ae9bc4375efb63c60e3221e85088fdd1f) - bump sqlglotrs to 0.7.1 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.22.0] - 2025-10-08
### :boom: BREAKING CHANGES
- due to [`6beb917`](https://github.com/tobymao/sqlglot/commit/6beb9172dffd0aaea46b75477485060737e774b9) - Annotate type for snowflake ROUND function *(PR [#6032](https://github.com/tobymao/sqlglot/pull/6032) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake ROUND function (#6032)

- due to [`0939d69`](https://github.com/tobymao/sqlglot/commit/0939d69223a860581b1c30cc2f762294946b93f3) - move odbc date literal handling in t-sql closes [#6037](https://github.com/tobymao/sqlglot/pull/6037) *(PR [#6044](https://github.com/tobymao/sqlglot/pull/6044) by [@georgesittas](https://github.com/georgesittas))*:

  move odbc date literal handling in t-sql closes #6037 (#6044)

- due to [`56c8b3b`](https://github.com/tobymao/sqlglot/commit/56c8b3bbff7451b9049e1a168716bb41222a86ed) - Support CHANGE COLUMN statements in Hive and CHANGE/ALTER COLUMN statements in Spark *(PR [#6004](https://github.com/tobymao/sqlglot/pull/6004) by [@tsamaras](https://github.com/tsamaras))*:

  Support CHANGE COLUMN statements in Hive and CHANGE/ALTER COLUMN statements in Spark (#6004)


### :sparkles: New Features
- [`6beb917`](https://github.com/tobymao/sqlglot/commit/6beb9172dffd0aaea46b75477485060737e774b9) - **optimizer**: Annotate type for snowflake ROUND function *(PR [#6032](https://github.com/tobymao/sqlglot/pull/6032) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`8e03ad9`](https://github.com/tobymao/sqlglot/commit/8e03ad9dd087ebc72bf58cb6383607c0ce2e8f8f) - **optimizer**: Annotate type for snowflake MOD function *(PR [#6031](https://github.com/tobymao/sqlglot/pull/6031) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`15b3fac`](https://github.com/tobymao/sqlglot/commit/15b3fac3dd5efd4c347ac40055f07a9be5906802) - **mysql**: support `FOR ORDINALITY` clause in `COLUMN` expression *(PR [#6046](https://github.com/tobymao/sqlglot/pull/6046) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#6039](https://github.com/tobymao/sqlglot/issues/6039) opened by [@jdddog](https://github.com/jdddog)*
- [`56c8b3b`](https://github.com/tobymao/sqlglot/commit/56c8b3bbff7451b9049e1a168716bb41222a86ed) - **hive,spark**: Support CHANGE COLUMN statements in Hive and CHANGE/ALTER COLUMN statements in Spark *(PR [#6004](https://github.com/tobymao/sqlglot/pull/6004) by [@tsamaras](https://github.com/tsamaras))*

### :bug: Bug Fixes
- [`6a6ca92`](https://github.com/tobymao/sqlglot/commit/6a6ca927c4e6e06f5cb38ad1153a8b556999ef90) - **parser**: Allow nested GROUPING SETS *(PR [#6041](https://github.com/tobymao/sqlglot/pull/6041) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6038](https://github.com/tobymao/sqlglot/issues/6038) opened by [@denis-komarov](https://github.com/denis-komarov)*
- [`41baeaa`](https://github.com/tobymao/sqlglot/commit/41baeaa1530c5419c945409133e3b7caa5250ec7) - **optimizer**: more robust CROSS JOIN substitution and JOIN reordering *(PR [#6021](https://github.com/tobymao/sqlglot/pull/6021) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#6009](https://github.com/tobymao/sqlglot/issues/6009) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`e2f299f`](https://github.com/tobymao/sqlglot/commit/e2f299f5ad18d75a394e55bd1ee59ed243d77e54) - allow subqueries to have modifiers closes [#6014](https://github.com/tobymao/sqlglot/pull/6014) *(PR [#6034](https://github.com/tobymao/sqlglot/pull/6034) by [@tobymao](https://github.com/tobymao))*
- [`0d65266`](https://github.com/tobymao/sqlglot/commit/0d6526693f8e7dda9b7c180d31c364bde91afc72) - parse lambda for arg_min/max arguments closes [#6036](https://github.com/tobymao/sqlglot/pull/6036) *(PR [#6042](https://github.com/tobymao/sqlglot/pull/6042) by [@georgesittas](https://github.com/georgesittas))*
- [`0939d69`](https://github.com/tobymao/sqlglot/commit/0939d69223a860581b1c30cc2f762294946b93f3) - move odbc date literal handling in t-sql closes [#6037](https://github.com/tobymao/sqlglot/pull/6037) *(PR [#6044](https://github.com/tobymao/sqlglot/pull/6044) by [@georgesittas](https://github.com/georgesittas))*
- [`65848e5`](https://github.com/tobymao/sqlglot/commit/65848e5a3e4c1cb26e6ca4deb7819a282838c3c2) - **tsql**: UPDATE with OPTIONS *(PR [#6043](https://github.com/tobymao/sqlglot/pull/6043) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#6033](https://github.com/tobymao/sqlglot/issues/6033) opened by [@ligfx](https://github.com/ligfx)*

### :recycle: Refactors
- [`8f00c80`](https://github.com/tobymao/sqlglot/commit/8f00c804a67209a5eca1fcb28aeb95941c58e583) - _parse_in expr len check *(commit by [@geooo109](https://github.com/geooo109))*


## [v27.21.0] - 2025-10-07
### :boom: BREAKING CHANGES
- due to [`3c7b5c0`](https://github.com/tobymao/sqlglot/commit/3c7b5c0e2dc071b7b9f6da308ba58a3a43da93dc) - Annotate type for snowflake SOUNDEX_P123 function *(PR [#5987](https://github.com/tobymao/sqlglot/pull/5987) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake SOUNDEX_P123 function (#5987)

- due to [`f25e42e`](https://github.com/tobymao/sqlglot/commit/f25e42e3f5b3b7b671bd724ba7b09a9b07d13995) - annotate type for Snowflake REGEXP_INSTR function *(PR [#5978](https://github.com/tobymao/sqlglot/pull/5978) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake REGEXP_INSTR function (#5978)

- due to [`13cb26e`](https://github.com/tobymao/sqlglot/commit/13cb26e2f29373538d60a8124ddebf95fd22a8d8) - annotate type for Snowflake REGEXP_SUBSTR_ALL function *(PR [#5979](https://github.com/tobymao/sqlglot/pull/5979) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake REGEXP_SUBSTR_ALL function (#5979)

- due to [`4ce683e`](https://github.com/tobymao/sqlglot/commit/4ce683eb8ac5716a334cbd7625438b9f89623c7a) - Annotate type for snowflake UNICODE function *(PR [#5993](https://github.com/tobymao/sqlglot/pull/5993) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake UNICODE function (#5993)

- due to [`c7657fb`](https://github.com/tobymao/sqlglot/commit/c7657fbd27a4350c424ef65947471ab9ec086831) - remove `unalias_group_by` transformation since it is unsafe *(PR [#5997](https://github.com/tobymao/sqlglot/pull/5997) by [@georgesittas](https://github.com/georgesittas))*:

  remove `unalias_group_by` transformation since it is unsafe (#5997)

- due to [`587196c`](https://github.com/tobymao/sqlglot/commit/587196c9c2d122f73f9deb7e87c2831f27f6ed02) - Annotate type for snowflake STRTOK_TO_ARRAY function *(PR [#5994](https://github.com/tobymao/sqlglot/pull/5994) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake STRTOK_TO_ARRAY function (#5994)

- due to [`bced710`](https://github.com/tobymao/sqlglot/commit/bced71084ffb3a8f7a11db843777f05b68f367da) - Annotate type for snowflake STRTOK function. *(PR [#5991](https://github.com/tobymao/sqlglot/pull/5991) by [@georgesittas](https://github.com/georgesittas))*:

  Annotate type for snowflake STRTOK function. (#5991)

- due to [`be1cdc8`](https://github.com/tobymao/sqlglot/commit/be1cdc81b511d462b710b50941d5c2770d901e91) - Fix roundtrip of ~ operator *(PR [#6017](https://github.com/tobymao/sqlglot/pull/6017) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix roundtrip of ~ operator (#6017)

- due to [`74a13f2`](https://github.com/tobymao/sqlglot/commit/74a13f2a548b9cd41061e835cb3cd9dd2a5a9fb3) - Annotate type for snowflake DIV0 and DIVNULL functions *(PR [#6008](https://github.com/tobymao/sqlglot/pull/6008) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake DIV0 and DIVNULL functions (#6008)

- due to [`fec2b31`](https://github.com/tobymao/sqlglot/commit/fec2b31956f2debdad7c53744a577894cd8d747c) - Annotate type for snowflake SEARCH function *(PR [#5985](https://github.com/tobymao/sqlglot/pull/5985) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake SEARCH function (#5985)

- due to [`27a76cd`](https://github.com/tobymao/sqlglot/commit/27a76cdfe4212f16f945521eb3997580eacf1d61) - Annotate type for snowflake COT, SIN and TAN functions *(PR [#6022](https://github.com/tobymao/sqlglot/pull/6022) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake COT, SIN and TAN functions (#6022)

- due to [`0911276`](https://github.com/tobymao/sqlglot/commit/091127663ab4cb94b02be5aa40c6a46dd7f89243) - annotate type for Snowflake EXP function *(PR [#6007](https://github.com/tobymao/sqlglot/pull/6007) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake EXP function (#6007)

- due to [`a96d50e`](https://github.com/tobymao/sqlglot/commit/a96d50e14bed5e87ff2dce9c545e0c48897b64d6) - annotate type for Snowflake COSH function *(PR [#6006](https://github.com/tobymao/sqlglot/pull/6006) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake COSH function (#6006)

- due to [`4df58e0`](https://github.com/tobymao/sqlglot/commit/4df58e0f0b8985590fb29a8ab6ba0ced987ac5b9) - annotate type for Snowflake DEGREES function *(PR [#6027](https://github.com/tobymao/sqlglot/pull/6027) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake DEGREES function (#6027)

- due to [`db71a20`](https://github.com/tobymao/sqlglot/commit/db71a2023aaeca2ffda782ae7b91fdee356c402e) - annotate type for Snowflake COS function *(PR [#6028](https://github.com/tobymao/sqlglot/pull/6028) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake COS function (#6028)

- due to [`5dd2ed3`](https://github.com/tobymao/sqlglot/commit/5dd2ed3c69cf9e8c3e327297e0cc932f0954e108) - bump sqlglotrs to 0.7.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.7.0


### :sparkles: New Features
- [`3c7b5c0`](https://github.com/tobymao/sqlglot/commit/3c7b5c0e2dc071b7b9f6da308ba58a3a43da93dc) - **optimizer**: Annotate type for snowflake SOUNDEX_P123 function *(PR [#5987](https://github.com/tobymao/sqlglot/pull/5987) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`475c09b`](https://github.com/tobymao/sqlglot/commit/475c09bd27179db4d186638645698dd4ad6553cd) - **optimizer**: Annotate type for snowflake TRANSLATE function *(PR [#5992](https://github.com/tobymao/sqlglot/pull/5992) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`f25e42e`](https://github.com/tobymao/sqlglot/commit/f25e42e3f5b3b7b671bd724ba7b09a9b07d13995) - **optimizer**: annotate type for Snowflake REGEXP_INSTR function *(PR [#5978](https://github.com/tobymao/sqlglot/pull/5978) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`13cb26e`](https://github.com/tobymao/sqlglot/commit/13cb26e2f29373538d60a8124ddebf95fd22a8d8) - **optimizer**: annotate type for Snowflake REGEXP_SUBSTR_ALL function *(PR [#5979](https://github.com/tobymao/sqlglot/pull/5979) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`4ce683e`](https://github.com/tobymao/sqlglot/commit/4ce683eb8ac5716a334cbd7625438b9f89623c7a) - **optimizer**: Annotate type for snowflake UNICODE function *(PR [#5993](https://github.com/tobymao/sqlglot/pull/5993) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`587196c`](https://github.com/tobymao/sqlglot/commit/587196c9c2d122f73f9deb7e87c2831f27f6ed02) - **optimizer**: Annotate type for snowflake STRTOK_TO_ARRAY function *(PR [#5994](https://github.com/tobymao/sqlglot/pull/5994) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`bced710`](https://github.com/tobymao/sqlglot/commit/bced71084ffb3a8f7a11db843777f05b68f367da) - **optimizer**: Annotate type for snowflake STRTOK function. *(PR [#5991](https://github.com/tobymao/sqlglot/pull/5991) by [@georgesittas](https://github.com/georgesittas))*
- [`74a13f2`](https://github.com/tobymao/sqlglot/commit/74a13f2a548b9cd41061e835cb3cd9dd2a5a9fb3) - **optimizer**: Annotate type for snowflake DIV0 and DIVNULL functions *(PR [#6008](https://github.com/tobymao/sqlglot/pull/6008) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`fec2b31`](https://github.com/tobymao/sqlglot/commit/fec2b31956f2debdad7c53744a577894cd8d747c) - **optimizer**: Annotate type for snowflake SEARCH function *(PR [#5985](https://github.com/tobymao/sqlglot/pull/5985) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`27a76cd`](https://github.com/tobymao/sqlglot/commit/27a76cdfe4212f16f945521eb3997580eacf1d61) - **optimizer**: Annotate type for snowflake COT, SIN and TAN functions *(PR [#6022](https://github.com/tobymao/sqlglot/pull/6022) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`8b48f7b`](https://github.com/tobymao/sqlglot/commit/8b48f7b985342cfcc45bc2b94540a1a2bf5995c4) - **optimizer**: Annotate type for snowflake SIGN and ABS functions *(PR [#6025](https://github.com/tobymao/sqlglot/pull/6025) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`0911276`](https://github.com/tobymao/sqlglot/commit/091127663ab4cb94b02be5aa40c6a46dd7f89243) - **optimizer**: annotate type for Snowflake EXP function *(PR [#6007](https://github.com/tobymao/sqlglot/pull/6007) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`a96d50e`](https://github.com/tobymao/sqlglot/commit/a96d50e14bed5e87ff2dce9c545e0c48897b64d6) - **optimizer**: annotate type for Snowflake COSH function *(PR [#6006](https://github.com/tobymao/sqlglot/pull/6006) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`4df58e0`](https://github.com/tobymao/sqlglot/commit/4df58e0f0b8985590fb29a8ab6ba0ced987ac5b9) - **optimizer**: annotate type for Snowflake DEGREES function *(PR [#6027](https://github.com/tobymao/sqlglot/pull/6027) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`db71a20`](https://github.com/tobymao/sqlglot/commit/db71a2023aaeca2ffda782ae7b91fdee356c402e) - **optimizer**: annotate type for Snowflake COS function *(PR [#6028](https://github.com/tobymao/sqlglot/pull/6028) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*

### :bug: Bug Fixes
- [`51b1bb1`](https://github.com/tobymao/sqlglot/commit/51b1bb178fa952edc13b2cbc6f624d30b0bde798) - move `WATERMARK` logic to risingwave fixes [#5989](https://github.com/tobymao/sqlglot/pull/5989) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`033ddf0`](https://github.com/tobymao/sqlglot/commit/033ddf04da895f1f5d38aff5361b2ae0793fefea) - **optimizer**: convert INNER JOINs to LEFT JOINs when merging LEFT JOIN subqueries *(PR [#5980](https://github.com/tobymao/sqlglot/pull/5980) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5969](https://github.com/tobymao/sqlglot/issues/5969) opened by [@karta0807913](https://github.com/karta0807913)*
- [`c7657fb`](https://github.com/tobymao/sqlglot/commit/c7657fbd27a4350c424ef65947471ab9ec086831) - remove `unalias_group_by` transformation since it is unsafe *(PR [#5997](https://github.com/tobymao/sqlglot/pull/5997) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5995](https://github.com/tobymao/sqlglot/issues/5995) opened by [@capricornsky0119](https://github.com/capricornsky0119)*
- [`b6f9694`](https://github.com/tobymao/sqlglot/commit/b6f9694c535cdd1403a63036cc246fda4e6d4d22) - **optimizer**: avoid merging subquery with JOIN when outer query uses JOIN *(PR [#5999](https://github.com/tobymao/sqlglot/pull/5999) by [@geooo109](https://github.com/geooo109))*
- [`23fd7b9`](https://github.com/tobymao/sqlglot/commit/23fd7b9116541b96e5d89389e862c6004e92d109) - respect multi-part Column units instead of converting to Var *(PR [#6005](https://github.com/tobymao/sqlglot/pull/6005) by [@georgesittas](https://github.com/georgesittas))*
- [`be1cdc8`](https://github.com/tobymao/sqlglot/commit/be1cdc81b511d462b710b50941d5c2770d901e91) - **duckdb**: Fix roundtrip of ~ operator *(PR [#6017](https://github.com/tobymao/sqlglot/pull/6017) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6016](https://github.com/tobymao/sqlglot/issues/6016) opened by [@denis-komarov](https://github.com/denis-komarov)*
- [`27c278f`](https://github.com/tobymao/sqlglot/commit/27c278f562f5ce98a1a4d31f8e66f148a1f42236) - **parser**: Allow LIMIT with % percentage *(PR [#6019](https://github.com/tobymao/sqlglot/pull/6019) by [@VaggelisD](https://github.com/VaggelisD))*
- [`39bf3f8`](https://github.com/tobymao/sqlglot/commit/39bf3f893389663796cdd799ef0f1e684f315a01) - **parser**: Allow CUBE & ROLLUP inside GROUPING SETS *(PR [#6018](https://github.com/tobymao/sqlglot/pull/6018) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6015](https://github.com/tobymao/sqlglot/issues/6015) opened by [@denis-komarov](https://github.com/denis-komarov)*
- [`ba7ad34`](https://github.com/tobymao/sqlglot/commit/ba7ad341d5ee1298b8fe54be11ca6252c1a44c99) - **duckdb**: Parse ROW type as STRUCT *(PR [#6020](https://github.com/tobymao/sqlglot/pull/6020) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6012](https://github.com/tobymao/sqlglot/issues/6012) opened by [@denis-komarov](https://github.com/denis-komarov)*
- [`718d6bb`](https://github.com/tobymao/sqlglot/commit/718d6bbf7f40e5b3e99563e2f1ac9eadeff57c3d) - handle unicode heredoc tags & Rust grapheme clusters properly *(PR [#6024](https://github.com/tobymao/sqlglot/pull/6024) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#6010](https://github.com/tobymao/sqlglot/issues/6010) opened by [@denis-komarov](https://github.com/denis-komarov)*
- [`c8cfb9d`](https://github.com/tobymao/sqlglot/commit/c8cfb9db2e789be2dc7f8a154082a9210b736502) - **snowflake**: transpile ARRAY_CONTAINS with VARIANT CAST *(PR [#6029](https://github.com/tobymao/sqlglot/pull/6029) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#6026](https://github.com/tobymao/sqlglot/issues/6026) opened by [@Birkman](https://github.com/Birkman)*

### :wrench: Chores
- [`1b1c6f8`](https://github.com/tobymao/sqlglot/commit/1b1c6f8d418371d49f0d3511baf3c5e35dd3ef42) - coerce type for EXTRACT canonicalization *(PR [#5998](https://github.com/tobymao/sqlglot/pull/5998) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5996](https://github.com/tobymao/sqlglot/issues/5996) opened by [@snovik75](https://github.com/snovik75)*
- [`f00ae73`](https://github.com/tobymao/sqlglot/commit/f00ae735c8f185b4c6c132373c9fa9bbe58e37b7) - **optimizer**: Annotate type for sqrt function *(PR [#6003](https://github.com/tobymao/sqlglot/pull/6003) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`ac97f14`](https://github.com/tobymao/sqlglot/commit/ac97f14ee1a576a276018f6c9ae1237ecf9ceda7) - simplify `SEARCH` Snowflake instantiation *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`5dd2ed3`](https://github.com/tobymao/sqlglot/commit/5dd2ed3c69cf9e8c3e327297e0cc932f0954e108) - bump sqlglotrs to 0.7.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.20.0] - 2025-09-30
### :boom: BREAKING CHANGES
- due to [`13a30df`](https://github.com/tobymao/sqlglot/commit/13a30dfa37096df5bfc2c31538325c40a49f7917) - Annotate type for snowflake TRY_BASE64_DECODE_BINARY function *(PR [#5972](https://github.com/tobymao/sqlglot/pull/5972) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake TRY_BASE64_DECODE_BINARY function (#5972)

- due to [`1f5fdd7`](https://github.com/tobymao/sqlglot/commit/1f5fdd799c047de167a4572f7ac26b7ad92167f2) - Annotate type for snowflake TRY_BASE64_DECODE_STRING function *(PR [#5974](https://github.com/tobymao/sqlglot/pull/5974) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake TRY_BASE64_DECODE_STRING function (#5974)

- due to [`324e82f`](https://github.com/tobymao/sqlglot/commit/324e82fe1fb11722f91341010602a743b151e055) - Annotate type for snowflake TRY_HEX_DECODE_BINARY function *(PR [#5975](https://github.com/tobymao/sqlglot/pull/5975) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake TRY_HEX_DECODE_BINARY function (#5975)

- due to [`6caf99d`](https://github.com/tobymao/sqlglot/commit/6caf99d556a3357ffaa6c294a9babcd30dd5fac5) - Annotate type for snowflake TRY_HEX_DECODE_STRING function *(PR [#5976](https://github.com/tobymao/sqlglot/pull/5976) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake TRY_HEX_DECODE_STRING function (#5976)

- due to [`73186a8`](https://github.com/tobymao/sqlglot/commit/73186a812ce422c108ee81b3de11da6ee9a9e902) - annotate type for Snowflake REGEXP_COUNT function *(PR [#5963](https://github.com/tobymao/sqlglot/pull/5963) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake REGEXP_COUNT function (#5963)

- due to [`c3bdb3c`](https://github.com/tobymao/sqlglot/commit/c3bdb3cd1af1809ed82be0ae40744d9fffc8ce18) - array start index is 1, support array_flatten, fixes [#5983](https://github.com/tobymao/sqlglot/pull/5983) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  array start index is 1, support array_flatten, fixes #5983

- due to [`244fb48`](https://github.com/tobymao/sqlglot/commit/244fb48fc9c4776f427c08b825d139b1c172fd26) - annotate type for Snowflake SPLIT_PART function *(PR [#5988](https://github.com/tobymao/sqlglot/pull/5988) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake SPLIT_PART function (#5988)

- due to [`0d772e0`](https://github.com/tobymao/sqlglot/commit/0d772e0b9d687b24d49203c05d7a90cc1dce02d5) - add ast node for `DIRECTORY` source *(PR [#5990](https://github.com/tobymao/sqlglot/pull/5990) by [@georgesittas](https://github.com/georgesittas))*:

  add ast node for `DIRECTORY` source (#5990)


### :sparkles: New Features
- [`13a30df`](https://github.com/tobymao/sqlglot/commit/13a30dfa37096df5bfc2c31538325c40a49f7917) - **optimizer**: Annotate type for snowflake TRY_BASE64_DECODE_BINARY function *(PR [#5972](https://github.com/tobymao/sqlglot/pull/5972) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`1f5fdd7`](https://github.com/tobymao/sqlglot/commit/1f5fdd799c047de167a4572f7ac26b7ad92167f2) - **optimizer**: Annotate type for snowflake TRY_BASE64_DECODE_STRING function *(PR [#5974](https://github.com/tobymao/sqlglot/pull/5974) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`324e82f`](https://github.com/tobymao/sqlglot/commit/324e82fe1fb11722f91341010602a743b151e055) - **optimizer**: Annotate type for snowflake TRY_HEX_DECODE_BINARY function *(PR [#5975](https://github.com/tobymao/sqlglot/pull/5975) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`6caf99d`](https://github.com/tobymao/sqlglot/commit/6caf99d556a3357ffaa6c294a9babcd30dd5fac5) - **optimizer**: Annotate type for snowflake TRY_HEX_DECODE_STRING function *(PR [#5976](https://github.com/tobymao/sqlglot/pull/5976) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`73186a8`](https://github.com/tobymao/sqlglot/commit/73186a812ce422c108ee81b3de11da6ee9a9e902) - **optimizer**: annotate type for Snowflake REGEXP_COUNT function *(PR [#5963](https://github.com/tobymao/sqlglot/pull/5963) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`6124de7`](https://github.com/tobymao/sqlglot/commit/6124de76fa6d6725e844cd37e09ebfe99469b0ec) - **optimizer**: Annotate type for snowflake SOUNDEX function *(PR [#5986](https://github.com/tobymao/sqlglot/pull/5986) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`244fb48`](https://github.com/tobymao/sqlglot/commit/244fb48fc9c4776f427c08b825d139b1c172fd26) - **optimizer**: annotate type for Snowflake SPLIT_PART function *(PR [#5988](https://github.com/tobymao/sqlglot/pull/5988) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`0d772e0`](https://github.com/tobymao/sqlglot/commit/0d772e0b9d687b24d49203c05d7a90cc1dce02d5) - **snowflake**: add ast node for `DIRECTORY` source *(PR [#5990](https://github.com/tobymao/sqlglot/pull/5990) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`7a3744f`](https://github.com/tobymao/sqlglot/commit/7a3744f203b93211e5dd97e6730b6bf59d6d96e0) - **sqlite**: support `RANGE CURRENT ROW` in window spec *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`c3bdb3c`](https://github.com/tobymao/sqlglot/commit/c3bdb3cd1af1809ed82be0ae40744d9fffc8ce18) - **starrocks**: array start index is 1, support array_flatten, fixes [#5983](https://github.com/tobymao/sqlglot/pull/5983) *(commit by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`d425ba2`](https://github.com/tobymao/sqlglot/commit/d425ba26b96b368801f8f486fa375cd75105993d) - make hash and eq non recursive *(PR [#5966](https://github.com/tobymao/sqlglot/pull/5966) by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`345c6a1`](https://github.com/tobymao/sqlglot/commit/345c6a153481a22d6df1b12ef1863e2133688fdf) - add uv support to Makefile *(PR [#5973](https://github.com/tobymao/sqlglot/pull/5973) by [@eakmanrq](https://github.com/eakmanrq))*


## [v27.19.0] - 2025-09-26
### :boom: BREAKING CHANGES
- due to [`68473ac`](https://github.com/tobymao/sqlglot/commit/68473ac3ec8dc76512dc76819892a1b0324c7ddc) - Annotate type for snowflake PARSE_URL function *(PR [#5962](https://github.com/tobymao/sqlglot/pull/5962) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake PARSE_URL function (#5962)

- due to [`b015a9d`](https://github.com/tobymao/sqlglot/commit/b015a9d944d0a87069a7750ad74953c399d7da34) - annotate type for Snowflake REGEXP_INSTR function *(commit by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake REGEXP_INSTR function

- due to [`1f29ba7`](https://github.com/tobymao/sqlglot/commit/1f29ba710f4213beb1a2f993244d7d824f3536ce) - annotate type for Snowflake PARSE_IP function *(PR [#5961](https://github.com/tobymao/sqlglot/pull/5961) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake PARSE_IP function (#5961)

- due to [`bf45d5d`](https://github.com/tobymao/sqlglot/commit/bf45d5d3cb0c0f380824019eb32ec29049268a61) - annotate types for Snowflake RTRIMMED_LENGTH function *(PR [#5968](https://github.com/tobymao/sqlglot/pull/5968) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake RTRIMMED_LENGTH function (#5968)

- due to [`13caa69`](https://github.com/tobymao/sqlglot/commit/13caa6991f003ad7abb590073451e591b6fd888c) - Annotate type for snowflake POSITION function *(PR [#5964](https://github.com/tobymao/sqlglot/pull/5964) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake POSITION function (#5964)


### :sparkles: New Features
- [`88e4e4c`](https://github.com/tobymao/sqlglot/commit/88e4e4c55f3a113127eb3c82c0be46c29bcf15ab) - **optimizer**: Annotate type for OCTET_LENGTH function *(PR [#5960](https://github.com/tobymao/sqlglot/pull/5960) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`68473ac`](https://github.com/tobymao/sqlglot/commit/68473ac3ec8dc76512dc76819892a1b0324c7ddc) - **optimizer**: Annotate type for snowflake PARSE_URL function *(PR [#5962](https://github.com/tobymao/sqlglot/pull/5962) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`b015a9d`](https://github.com/tobymao/sqlglot/commit/b015a9d944d0a87069a7750ad74953c399d7da34) - **optimizer**: annotate type for Snowflake REGEXP_INSTR function *(commit by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`1f29ba7`](https://github.com/tobymao/sqlglot/commit/1f29ba710f4213beb1a2f993244d7d824f3536ce) - **optimizer**: annotate type for Snowflake PARSE_IP function *(PR [#5961](https://github.com/tobymao/sqlglot/pull/5961) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`bf45d5d`](https://github.com/tobymao/sqlglot/commit/bf45d5d3cb0c0f380824019eb32ec29049268a61) - **optimizer**: annotate types for Snowflake RTRIMMED_LENGTH function *(PR [#5968](https://github.com/tobymao/sqlglot/pull/5968) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`13caa69`](https://github.com/tobymao/sqlglot/commit/13caa6991f003ad7abb590073451e591b6fd888c) - **optimizer**: Annotate type for snowflake POSITION function *(PR [#5964](https://github.com/tobymao/sqlglot/pull/5964) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`1471306`](https://github.com/tobymao/sqlglot/commit/1471306ed317830c294e3654075f55424d14bf5a) - support parse into grant principal and privilege *(PR [#5971](https://github.com/tobymao/sqlglot/pull/5971) by [@eakmanrq](https://github.com/eakmanrq))*

### :bug: Bug Fixes
- [`5432976`](https://github.com/tobymao/sqlglot/commit/543297680755344185e0f306843bc4909f4f75ed) - **bigquery**: allow GRANT as an id var *(PR [#5965](https://github.com/tobymao/sqlglot/pull/5965) by [@treysp](https://github.com/treysp))*

### :wrench: Chores
- [`1514bc6`](https://github.com/tobymao/sqlglot/commit/1514bc640ec129a96aedd9e89bfd5d61e832d6b1) - **optimizer**: add type inference tests for Snowflake RPAD function *(PR [#5967](https://github.com/tobymao/sqlglot/pull/5967) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`050b89d`](https://github.com/tobymao/sqlglot/commit/050b89deb9be842f2ddd07c78ea201ec4eae4779) - **optimizer**: Annotate type for snowflake regexp function *(PR [#5970](https://github.com/tobymao/sqlglot/pull/5970) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*


## [v27.18.0] - 2025-09-25
### :boom: BREAKING CHANGES
- due to [`7f13eaf`](https://github.com/tobymao/sqlglot/commit/7f13eaf7769a3381a56c9209af590835be2f95cd) - Annotate type for snowflake DECOMPRESS_BINARY function *(PR [#5945](https://github.com/tobymao/sqlglot/pull/5945) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake DECOMPRESS_BINARY function (#5945)

- due to [`be12b29`](https://github.com/tobymao/sqlglot/commit/be12b29b5a7bd6d6e09dbd8c17086bd77c19abc0) - Annotate type for snowflake DECOMPRESS_STRING function *(PR [#5947](https://github.com/tobymao/sqlglot/pull/5947) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake DECOMPRESS_STRING function (#5947)

- due to [`1573fef`](https://github.com/tobymao/sqlglot/commit/1573fefac27b5b1215e3d458f8ccf1b9dadbb772) - annotate types for Snowflake JAROWINKLER_SIMILARITY function *(PR [#5950](https://github.com/tobymao/sqlglot/pull/5950) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake JAROWINKLER_SIMILARITY function (#5950)

- due to [`883c6ab`](https://github.com/tobymao/sqlglot/commit/883c6abe589865f478d95604e8d670e57afd04af) - annotate type for Snowflake COLLATION function *(PR [#5939](https://github.com/tobymao/sqlglot/pull/5939) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake COLLATION function (#5939)


### :sparkles: New Features
- [`7f13eaf`](https://github.com/tobymao/sqlglot/commit/7f13eaf7769a3381a56c9209af590835be2f95cd) - **optimizer**: Annotate type for snowflake DECOMPRESS_BINARY function *(PR [#5945](https://github.com/tobymao/sqlglot/pull/5945) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`be12b29`](https://github.com/tobymao/sqlglot/commit/be12b29b5a7bd6d6e09dbd8c17086bd77c19abc0) - **optimizer**: Annotate type for snowflake DECOMPRESS_STRING function *(PR [#5947](https://github.com/tobymao/sqlglot/pull/5947) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`a55fce5`](https://github.com/tobymao/sqlglot/commit/a55fce5310a50af132c5d06bb299fe3f025442c4) - **optimizer**: Annotate type for snowflake LPAD function *(PR [#5948](https://github.com/tobymao/sqlglot/pull/5948) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`05e07aa`](https://github.com/tobymao/sqlglot/commit/05e07aa740d7977a6b42ec15ae4fa9c2168a15f5) - **optimizer**: annotate type for Snowflake INSERT function *(PR [#5942](https://github.com/tobymao/sqlglot/pull/5942) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`6268e10`](https://github.com/tobymao/sqlglot/commit/6268e107a947badaa00508544f5389412806ecd0) - **solr**: initial dialect implementation *(PR [#5946](https://github.com/tobymao/sqlglot/pull/5946) by [@aadel](https://github.com/aadel))*
- [`1573fef`](https://github.com/tobymao/sqlglot/commit/1573fefac27b5b1215e3d458f8ccf1b9dadbb772) - **optimizer**: annotate types for Snowflake JAROWINKLER_SIMILARITY function *(PR [#5950](https://github.com/tobymao/sqlglot/pull/5950) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`883c6ab`](https://github.com/tobymao/sqlglot/commit/883c6abe589865f478d95604e8d670e57afd04af) - **optimizer**: annotate type for Snowflake COLLATION function *(PR [#5939](https://github.com/tobymao/sqlglot/pull/5939) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`627c18d`](https://github.com/tobymao/sqlglot/commit/627c18d7da6bf644bc14c0f17963dea0be20604a) - **mysql**: add valid INTERVAL units *(PR [#5951](https://github.com/tobymao/sqlglot/pull/5951) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`3846d4d`](https://github.com/tobymao/sqlglot/commit/3846d4dcdf8cbf8e90b2661083a567ab0547ad3c) - **solr**: properly support OR alternative operator *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`df428d5`](https://github.com/tobymao/sqlglot/commit/df428d516113a47ae50d04cd50a250830589c072) - **parser**: interval identifier followed by END *(PR [#5944](https://github.com/tobymao/sqlglot/pull/5944) by [@geooo109](https://github.com/geooo109))*
- [`e178d16`](https://github.com/tobymao/sqlglot/commit/e178d1674a71e6f35a6acfa8f4a317f0fe2e4516) - **duckdb**: UNNEST as table *(PR [#5953](https://github.com/tobymao/sqlglot/pull/5953) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5952](https://github.com/tobymao/sqlglot/issues/5952) opened by [@denis-komarov](https://github.com/denis-komarov)*
- [`24feb8e`](https://github.com/tobymao/sqlglot/commit/24feb8ee0bc43f3f14fd768c9a0d986355becea2) - **parser**: parse `UPDATE` clauses in any order *(PR [#5958](https://github.com/tobymao/sqlglot/pull/5958) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5956](https://github.com/tobymao/sqlglot/issues/5956) opened by [@sfc-gh-clathrope](https://github.com/sfc-gh-clathrope)*
- [`980f99a`](https://github.com/tobymao/sqlglot/commit/980f99a4cc0613012a189ee5636af37ec736040c) - **snowflake**: properly generate inferred `STRUCT` data types *(PR [#5954](https://github.com/tobymao/sqlglot/pull/5954) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c18aaf8`](https://github.com/tobymao/sqlglot/commit/c18aaf80fd7375e89dfc8863da619d84f3257353) - cleanup *(commit by [@tobymao](https://github.com/tobymao))*


## [v27.17.0] - 2025-09-23
### :boom: BREAKING CHANGES
- due to [`f4ad258`](https://github.com/tobymao/sqlglot/commit/f4ad25882951de4e4442dfd5189a56d5a1c5e630) - Annotate types for Snowflake BASE64_DECODE_BINARY function *(PR [#5917](https://github.com/tobymao/sqlglot/pull/5917) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate types for Snowflake BASE64_DECODE_BINARY function (#5917)

- due to [`6d0e3f8`](https://github.com/tobymao/sqlglot/commit/6d0e3f8dcae7ed1a7659ece69b1f94cec5e7300e) - Add parser support to ilike like function versions. *(PR [#5915](https://github.com/tobymao/sqlglot/pull/5915) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Add parser support to ilike like function versions. (#5915)

- due to [`22c7ed7`](https://github.com/tobymao/sqlglot/commit/22c7ed7734b41ca544bb67bcc1ca4151f6d5f05f) - parse tuple *(PR [#5920](https://github.com/tobymao/sqlglot/pull/5920) by [@geooo109](https://github.com/geooo109))*:

  parse tuple (#5920)

- due to [`fc5624e`](https://github.com/tobymao/sqlglot/commit/fc5624eca43d2855ac350c92d85b184a6893d5ca) - annotate types for Snowflake ASCII function *(PR [#5926](https://github.com/tobymao/sqlglot/pull/5926) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake ASCII function (#5926)

- due to [`4e81690`](https://github.com/tobymao/sqlglot/commit/4e8169045edcaa28ae43abeb07370df63846fbfd) - annotate type for Snowflake COLLATE function *(PR [#5931](https://github.com/tobymao/sqlglot/pull/5931) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake COLLATE function (#5931)

- due to [`f07d35d`](https://github.com/tobymao/sqlglot/commit/f07d35d29104c6203efaab738118d1903614b83c) - annotate type for Snowflake CHR function *(PR [#5929](https://github.com/tobymao/sqlglot/pull/5929) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake CHR function (#5929)

- due to [`f8c0ee4`](https://github.com/tobymao/sqlglot/commit/f8c0ee4d3c1a4d4a92b897d1cc85f9904c8e566b) - Add function and annotate snowflake hex decode string and binary functions *(PR [#5928](https://github.com/tobymao/sqlglot/pull/5928) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Add function and annotate snowflake hex decode string and binary functions (#5928)

- due to [`66f9501`](https://github.com/tobymao/sqlglot/commit/66f9501d76d087798bad93e578273ab2a45e2575) - annotate types for Snowflake BIT_LENGTH function *(PR [#5927](https://github.com/tobymao/sqlglot/pull/5927) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake BIT_LENGTH function (#5927)

- due to [`7878437`](https://github.com/tobymao/sqlglot/commit/78784370712df65a2e1e79a1c2b441131ed7222a) - annotate snowflake's `BASE64_DECODE_STRING`, `BASE64_ENCODE` *(PR [#5922](https://github.com/tobymao/sqlglot/pull/5922) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  annotate snowflake's `BASE64_DECODE_STRING`, `BASE64_ENCODE` (#5922)

- due to [`9bcad04`](https://github.com/tobymao/sqlglot/commit/9bcad040bd51dd03821c68eea1a73534fc7a81b7) - Annotate type for HEX ENCODE function. *(PR [#5936](https://github.com/tobymao/sqlglot/pull/5936) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for HEX ENCODE function. (#5936)

- due to [`590928f`](https://github.com/tobymao/sqlglot/commit/590928f4637306e8cf3f1302d5dd5d5dbc76e7e0) - annotate type for Snowflake INITCAP function *(PR [#5941](https://github.com/tobymao/sqlglot/pull/5941) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake INITCAP function (#5941)

- due to [`ac04de1`](https://github.com/tobymao/sqlglot/commit/ac04de1944c7a976406581b489b3cf9b11dafb77) - annotate type for Snowflake EDITDISTANCE function *(PR [#5940](https://github.com/tobymao/sqlglot/pull/5940) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for Snowflake EDITDISTANCE function (#5940)

- due to [`9e28af8`](https://github.com/tobymao/sqlglot/commit/9e28af8a52ced951ecf7f4e85a6305e20a13de1f) - Annotate type for snowflake COMPRESS function *(PR [#5938](https://github.com/tobymao/sqlglot/pull/5938) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate type for snowflake COMPRESS function (#5938)


### :sparkles: New Features
- [`f4ad258`](https://github.com/tobymao/sqlglot/commit/f4ad25882951de4e4442dfd5189a56d5a1c5e630) - **optimizer**: Annotate types for Snowflake BASE64_DECODE_BINARY function *(PR [#5917](https://github.com/tobymao/sqlglot/pull/5917) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`6d0e3f8`](https://github.com/tobymao/sqlglot/commit/6d0e3f8dcae7ed1a7659ece69b1f94cec5e7300e) - **optimizer**: Add parser support to ilike like function versions. *(PR [#5915](https://github.com/tobymao/sqlglot/pull/5915) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`fc5624e`](https://github.com/tobymao/sqlglot/commit/fc5624eca43d2855ac350c92d85b184a6893d5ca) - **optimizer**: annotate types for Snowflake ASCII function *(PR [#5926](https://github.com/tobymao/sqlglot/pull/5926) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`4e81690`](https://github.com/tobymao/sqlglot/commit/4e8169045edcaa28ae43abeb07370df63846fbfd) - **optimizer**: annotate type for Snowflake COLLATE function *(PR [#5931](https://github.com/tobymao/sqlglot/pull/5931) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`f07d35d`](https://github.com/tobymao/sqlglot/commit/f07d35d29104c6203efaab738118d1903614b83c) - **optimizer**: annotate type for Snowflake CHR function *(PR [#5929](https://github.com/tobymao/sqlglot/pull/5929) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`f8c0ee4`](https://github.com/tobymao/sqlglot/commit/f8c0ee4d3c1a4d4a92b897d1cc85f9904c8e566b) - **optimizer**: Add function and annotate snowflake hex decode string and binary functions *(PR [#5928](https://github.com/tobymao/sqlglot/pull/5928) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`66f9501`](https://github.com/tobymao/sqlglot/commit/66f9501d76d087798bad93e578273ab2a45e2575) - **optimizer**: annotate types for Snowflake BIT_LENGTH function *(PR [#5927](https://github.com/tobymao/sqlglot/pull/5927) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`f4c810e`](https://github.com/tobymao/sqlglot/commit/f4c810e043d9379e94efb185e368e27ad9c15715) - transpile Trino `FORMAT` to DuckDB and Snowflake, closes [#5933](https://github.com/tobymao/sqlglot/pull/5933) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`7878437`](https://github.com/tobymao/sqlglot/commit/78784370712df65a2e1e79a1c2b441131ed7222a) - **optimizer**: annotate snowflake's `BASE64_DECODE_STRING`, `BASE64_ENCODE` *(PR [#5922](https://github.com/tobymao/sqlglot/pull/5922) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`9bcad04`](https://github.com/tobymao/sqlglot/commit/9bcad040bd51dd03821c68eea1a73534fc7a81b7) - **optimizer**: Annotate type for HEX ENCODE function. *(PR [#5936](https://github.com/tobymao/sqlglot/pull/5936) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`590928f`](https://github.com/tobymao/sqlglot/commit/590928f4637306e8cf3f1302d5dd5d5dbc76e7e0) - **optimizer**: annotate type for Snowflake INITCAP function *(PR [#5941](https://github.com/tobymao/sqlglot/pull/5941) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`ac04de1`](https://github.com/tobymao/sqlglot/commit/ac04de1944c7a976406581b489b3cf9b11dafb77) - **optimizer**: annotate type for Snowflake EDITDISTANCE function *(PR [#5940](https://github.com/tobymao/sqlglot/pull/5940) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`a385990`](https://github.com/tobymao/sqlglot/commit/a38599080932a8b54a169df8b7a69650cb47b6bc) - **parser**: support wrapped aggregate functions *(PR [#5943](https://github.com/tobymao/sqlglot/pull/5943) by [@geooo109](https://github.com/geooo109))*
- [`9e28af8`](https://github.com/tobymao/sqlglot/commit/9e28af8a52ced951ecf7f4e85a6305e20a13de1f) - **optimizer**: Annotate type for snowflake COMPRESS function *(PR [#5938](https://github.com/tobymao/sqlglot/pull/5938) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*

### :bug: Bug Fixes
- [`6807a32`](https://github.com/tobymao/sqlglot/commit/6807a32cccf984dc13a30b815750b2c41374b845) - escape byte string delimiters *(PR [#5916](https://github.com/tobymao/sqlglot/pull/5916) by [@georgesittas](https://github.com/georgesittas))*
- [`22c7ed7`](https://github.com/tobymao/sqlglot/commit/22c7ed7734b41ca544bb67bcc1ca4151f6d5f05f) - **clickhouse**: parse tuple *(PR [#5920](https://github.com/tobymao/sqlglot/pull/5920) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5913](https://github.com/tobymao/sqlglot/issues/5913) opened by [@tiagoskaneta](https://github.com/tiagoskaneta)*
- [`223160b`](https://github.com/tobymao/sqlglot/commit/223160bd7914d51e9ec1abb8d0f1053e13a65c98) - **parser**: NULLABLE as an identifier *(PR [#5921](https://github.com/tobymao/sqlglot/pull/5921) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5919](https://github.com/tobymao/sqlglot/issues/5919) opened by [@baruchoxman](https://github.com/baruchoxman)*
- [`42cfc79`](https://github.com/tobymao/sqlglot/commit/42cfc79ce120dee83084e2bb6b8bbd19f45bf06f) - **snowflake**: parse DAYOFWEEKISO *(PR [#5925](https://github.com/tobymao/sqlglot/pull/5925) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5924](https://github.com/tobymao/sqlglot/issues/5924) opened by [@baruchoxman](https://github.com/baruchoxman)*
- [`0be2cb4`](https://github.com/tobymao/sqlglot/commit/0be2cb448ee1a5ac020ac47e9944875c30e42632) - **postgres**: support `DISTINCT` qualifier in `JSON_AGG` fixes [#5935](https://github.com/tobymao/sqlglot/pull/5935) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`e34b2e1`](https://github.com/tobymao/sqlglot/commit/e34b2e14d1f87d095955765173a5e17fc9985220) - allow grouping set parser to consume more syntax fixes [#5937](https://github.com/tobymao/sqlglot/pull/5937) *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.16.3] - 2025-09-18
### :bug: Bug Fixes
- [`d127051`](https://github.com/tobymao/sqlglot/commit/d1270517c3e124ca59caf29e4506eb3848f7452e) - precedence issue with column operator parsing *(PR [#5914](https://github.com/tobymao/sqlglot/pull/5914) by [@georgesittas](https://github.com/georgesittas))*


## [v27.16.2] - 2025-09-18
### :wrench: Chores
- [`837890c`](https://github.com/tobymao/sqlglot/commit/837890c7e8bcc3695541bbe32fd8088eee70fea3) - handle badly formed binary expressions gracefully in type inference *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.16.1] - 2025-09-18
### :bug: Bug Fixes
- [`0e256b3`](https://github.com/tobymao/sqlglot/commit/0e256b3f864bc2d026817bd08e89ee89f44ad256) - edge case with parsing `interval` as identifier *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.16.0] - 2025-09-18
### :boom: BREAKING CHANGES
- due to [`5a973e9`](https://github.com/tobymao/sqlglot/commit/5a973e9a88fa7f522a9bf91dc60fb0f6effef53d) - annotate types for Snowflake AI_CLASSIFY function *(PR [#5909](https://github.com/tobymao/sqlglot/pull/5909) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake AI_CLASSIFY function (#5909)

- due to [`2d0d908`](https://github.com/tobymao/sqlglot/commit/2d0d908b5bbc32ff3bc92eb1ae9fc6e5ac3409bc) - produce TableAlias instead of Alias for USING in merge builder *(PR [#5911](https://github.com/tobymao/sqlglot/pull/5911) by [@georgesittas](https://github.com/georgesittas))*:

  produce TableAlias instead of Alias for USING in merge builder (#5911)


### :sparkles: New Features
- [`5a973e9`](https://github.com/tobymao/sqlglot/commit/5a973e9a88fa7f522a9bf91dc60fb0f6effef53d) - **optimizer**: annotate types for Snowflake AI_CLASSIFY function *(PR [#5909](https://github.com/tobymao/sqlglot/pull/5909) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*

### :bug: Bug Fixes
- [`2d0d908`](https://github.com/tobymao/sqlglot/commit/2d0d908b5bbc32ff3bc92eb1ae9fc6e5ac3409bc) - produce TableAlias instead of Alias for USING in merge builder *(PR [#5911](https://github.com/tobymao/sqlglot/pull/5911) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5910](https://github.com/tobymao/sqlglot/issues/5910) opened by [@deepyaman](https://github.com/deepyaman)*

### :wrench: Chores
- [`e8974e7`](https://github.com/tobymao/sqlglot/commit/e8974e70d9956ce7a5cb119ba465660f5f172a17) - **optimizer**: Add tests for snowflake likeall, likeany and ilikeany functions *(PR [#5908](https://github.com/tobymao/sqlglot/pull/5908) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*


## [v27.15.3] - 2025-09-17
### :bug: Bug Fixes
- [`bd3e965`](https://github.com/tobymao/sqlglot/commit/bd3e9655aa72ffef8a9e0221205fa2c3915ef58b) - allow `lock` to be used as an identifier *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.15.2] - 2025-09-17
### :sparkles: New Features
- [`d5cf114`](https://github.com/tobymao/sqlglot/commit/d5cf1149932850a91cb5f1ebecda2652616729ef) - **duckdb**: support INSTALL *(PR [#5904](https://github.com/tobymao/sqlglot/pull/5904) by [@geooo109](https://github.com/geooo109))*
- [`73e05bb`](https://github.com/tobymao/sqlglot/commit/73e05bb15bb86e4a07cc09bf02028a6cf7fa1e6f) - **snowflake**: properly generate `BITNOT` *(PR [#5906](https://github.com/tobymao/sqlglot/pull/5906) by [@YuvalOmerRep](https://github.com/YuvalOmerRep))*
- [`16f317c`](https://github.com/tobymao/sqlglot/commit/16f317c04f7c0a398c38b461e05f4d4c30baf98b) - **snowflake**: add support for `<model>!<attribute>` syntax *(PR [#5907](https://github.com/tobymao/sqlglot/pull/5907) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`095b2ac`](https://github.com/tobymao/sqlglot/commit/095b2ac3af230eff86d9bc1b0fd3a0a2095f151c) - clean up duckdb INSTALL tests *(commit by [@geooo109](https://github.com/geooo109))*


## [v27.15.1] - 2025-09-17
### :sparkles: New Features
- [`1ee026d`](https://github.com/tobymao/sqlglot/commit/1ee026d22d4f6c3613c1809a6738cdea846c48a9) - **postgres**: support `SUBSTRING(value FOR length FROM start)` variant *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.15.0] - 2025-09-17
### :boom: BREAKING CHANGES
- due to [`96ae7a3`](https://github.com/tobymao/sqlglot/commit/96ae7a3bcbf9de1932150baa0bd704d4ce05c9f7) - Annotate and add tests for snowflake REPEAT and SPLIT functions *(PR [#5875](https://github.com/tobymao/sqlglot/pull/5875) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate and add tests for snowflake REPEAT and SPLIT functions (#5875)

- due to [`f2d3bf7`](https://github.com/tobymao/sqlglot/commit/f2d3bf74e804e5a5e2ac6ca94210ba04df07e7f3) - annotate types for Snowflake UUID_STRING function *(PR [#5881](https://github.com/tobymao/sqlglot/pull/5881) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake UUID_STRING function (#5881)

- due to [`ec80ff3`](https://github.com/tobymao/sqlglot/commit/ec80ff34957c3e3f80c44175383b06cf72988a68) - make dump a list instead of a nested dict to avoid all recursion errors *(PR [#5885](https://github.com/tobymao/sqlglot/pull/5885) by [@tobymao](https://github.com/tobymao))*:

  make dump a list instead of a nested dict to avoid all recursion errors (#5885)

- due to [`2fdaccd`](https://github.com/tobymao/sqlglot/commit/2fdaccd1a9045bda3d529025a4706c397b8a836f) - annotate types for Snowflake SHA1, SHA2 functions *(PR [#5884](https://github.com/tobymao/sqlglot/pull/5884) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake SHA1, SHA2 functions (#5884)

- due to [`faba309`](https://github.com/tobymao/sqlglot/commit/faba30905390e5efaf0ba9a05aab9ac2724b1b85) - annotate types for Snowflake AI_AGG function *(PR [#5894](https://github.com/tobymao/sqlglot/pull/5894) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake AI_AGG function (#5894)

- due to [`304bec5`](https://github.com/tobymao/sqlglot/commit/304bec5f7342501ad28ea4cd0a4b9aa092f2192f) - Annotate snowflake MD5 functions *(PR [#5883](https://github.com/tobymao/sqlglot/pull/5883) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate snowflake MD5 functions (#5883)

- due to [`c0180ec`](https://github.com/tobymao/sqlglot/commit/c0180ec163a43836fed754efcb6f26ad37cdae50) - annotate types for Snowflake AI_SUMMARIZE_AGG function *(PR [#5902](https://github.com/tobymao/sqlglot/pull/5902) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake AI_SUMMARIZE_AGG function (#5902)

- due to [`f5409df`](https://github.com/tobymao/sqlglot/commit/f5409df64ed6069880669878db687e4b98c3e280) - use column name in struct type annotation *(PR [#5903](https://github.com/tobymao/sqlglot/pull/5903) by [@georgesittas](https://github.com/georgesittas))*:

  use column name in struct type annotation (#5903)


### :sparkles: New Features
- [`cd818ba`](https://github.com/tobymao/sqlglot/commit/cd818bad51e93ec349b97675e4c1f5bd7c4c1522) - **singlestore**: Fixed generation/parsing of computed collumns *(PR [#5878](https://github.com/tobymao/sqlglot/pull/5878) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`5d1f241`](https://github.com/tobymao/sqlglot/commit/5d1f241209197419111e9eda37fb6f2a5ec2bc4b) - **tsql**: support JSON_ARRAYAGG *(PR [#5879](https://github.com/tobymao/sqlglot/pull/5879) by [@geooo109](https://github.com/geooo109))*
- [`96ae7a3`](https://github.com/tobymao/sqlglot/commit/96ae7a3bcbf9de1932150baa0bd704d4ce05c9f7) - **optimizer**: Annotate and add tests for snowflake REPEAT and SPLIT functions *(PR [#5875](https://github.com/tobymao/sqlglot/pull/5875) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`0fe6a25`](https://github.com/tobymao/sqlglot/commit/0fe6a25e366dcbc5a4a0878b285d147a6aa00412) - **postgres**: support JSON_AGG *(PR [#5880](https://github.com/tobymao/sqlglot/pull/5880) by [@geooo109](https://github.com/geooo109))*
- [`854eeeb`](https://github.com/tobymao/sqlglot/commit/854eeeb5b25954cc26b91135d58eb8370271f1de) - **optimizer**: annotate types for Snowflake REGEXP_LIKE, REGEXP_REPLACE, REGEXP_SUBSTR functions *(PR [#5876](https://github.com/tobymao/sqlglot/pull/5876) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`f2d3bf7`](https://github.com/tobymao/sqlglot/commit/f2d3bf74e804e5a5e2ac6ca94210ba04df07e7f3) - **optimizer**: annotate types for Snowflake UUID_STRING function *(PR [#5881](https://github.com/tobymao/sqlglot/pull/5881) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`5b9463a`](https://github.com/tobymao/sqlglot/commit/5b9463ad11a49c821585985c35394ebb30e827dd) - **mysql**: add support for binary `MOD` operator fixes [#5887](https://github.com/tobymao/sqlglot/pull/5887) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`d24eabc`](https://github.com/tobymao/sqlglot/commit/d24eabcbe30dc0f7c2dbae346e429efef58b5680) - **bigquery**: Add support for ML.GENERATE_TEXT_EMBEDDING(...) *(PR [#5891](https://github.com/tobymao/sqlglot/pull/5891) by [@VaggelisD](https://github.com/VaggelisD))*
- [`950a3fa`](https://github.com/tobymao/sqlglot/commit/950a3fa6d6307f7713f40117655da2f9710ebfa9) - **mysql**: SOUNDS LIKE, SUBSTR *(PR [#5886](https://github.com/tobymao/sqlglot/pull/5886) by [@vuvova](https://github.com/vuvova))*
- [`688afc5`](https://github.com/tobymao/sqlglot/commit/688afc55ab08588636eba92893c603ca68e43e6e) - **singlestore**: Fixed generation of exp.National *(PR [#5890](https://github.com/tobymao/sqlglot/pull/5890) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`c77147e`](https://github.com/tobymao/sqlglot/commit/c77147ebaafa6942f80af75dd6c2d7a62a7e6fe2) - **parser**: Extend support for `IS UNKOWN` across all dialects *(PR [#5888](https://github.com/tobymao/sqlglot/pull/5888) by [@VaggelisD](https://github.com/VaggelisD))*
- [`ec80ff3`](https://github.com/tobymao/sqlglot/commit/ec80ff34957c3e3f80c44175383b06cf72988a68) - make dump a list instead of a nested dict to avoid all recursion errors *(PR [#5885](https://github.com/tobymao/sqlglot/pull/5885) by [@tobymao](https://github.com/tobymao))*
- [`2fdaccd`](https://github.com/tobymao/sqlglot/commit/2fdaccd1a9045bda3d529025a4706c397b8a836f) - **optimizer**: annotate types for Snowflake SHA1, SHA2 functions *(PR [#5884](https://github.com/tobymao/sqlglot/pull/5884) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`faba309`](https://github.com/tobymao/sqlglot/commit/faba30905390e5efaf0ba9a05aab9ac2724b1b85) - **optimizer**: annotate types for Snowflake AI_AGG function *(PR [#5894](https://github.com/tobymao/sqlglot/pull/5894) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`dd27844`](https://github.com/tobymao/sqlglot/commit/dd2784435c7bdd2ceaaaaa359fcd112ad1f8190c) - **snowflake**: transpile `BYTE_LENGTH` *(PR [#5899](https://github.com/tobymao/sqlglot/pull/5899) by [@ozadari](https://github.com/ozadari))*
- [`304bec5`](https://github.com/tobymao/sqlglot/commit/304bec5f7342501ad28ea4cd0a4b9aa092f2192f) - **optimizer**: Annotate snowflake MD5 functions *(PR [#5883](https://github.com/tobymao/sqlglot/pull/5883) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`ec3006d`](https://github.com/tobymao/sqlglot/commit/ec3006d815951fdc1a80d6722ce6f1176417d595) - **optimizer**: Add tests for snowflake NOT ILIKE and NOT LIKE *(PR [#5901](https://github.com/tobymao/sqlglot/pull/5901) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`c0180ec`](https://github.com/tobymao/sqlglot/commit/c0180ec163a43836fed754efcb6f26ad37cdae50) - **optimizer**: annotate types for Snowflake AI_SUMMARIZE_AGG function *(PR [#5902](https://github.com/tobymao/sqlglot/pull/5902) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*

### :bug: Bug Fixes
- [`1d9e357`](https://github.com/tobymao/sqlglot/commit/1d9e357fb7549635ca25c6c42299880d7864e074) - **optimizer**: expand columns on the LHS of recursive CTEs *(PR [#5872](https://github.com/tobymao/sqlglot/pull/5872) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5814](https://github.com/tobymao/sqlglot/issues/5814) opened by [@suresh-summation](https://github.com/suresh-summation)*
- [`7fcc52a`](https://github.com/tobymao/sqlglot/commit/7fcc52a22241c480c22b3e6f843e7a210c75a0ec) - **parser**: Require an explicit alias in EXCLUDE/RENAME/REPLACE star ops *(PR [#5892](https://github.com/tobymao/sqlglot/pull/5892) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5fdcc65`](https://github.com/tobymao/sqlglot/commit/5fdcc651277ba4e86e11d0c5952a56e40299a998) - **snowflake**: parse OCTET_LENGTH *(PR [#5900](https://github.com/tobymao/sqlglot/pull/5900) by [@geooo109](https://github.com/geooo109))*
- [`f5409df`](https://github.com/tobymao/sqlglot/commit/f5409df64ed6069880669878db687e4b98c3e280) - **optimizer**: use column name in struct type annotation *(PR [#5903](https://github.com/tobymao/sqlglot/pull/5903) by [@georgesittas](https://github.com/georgesittas))*
- [`74886d8`](https://github.com/tobymao/sqlglot/commit/74886d82f70c9317af51c77b322e67a6aa260a5e) - **snowflake**: transpile BQ UNNEST with alias *(PR [#5897](https://github.com/tobymao/sqlglot/pull/5897) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5895](https://github.com/tobymao/sqlglot/issues/5895) opened by [@YuvalOmerRep](https://github.com/YuvalOmerRep)*


## [v27.14.0] - 2025-09-11
### :boom: BREAKING CHANGES
- due to [`9c8a600`](https://github.com/tobymao/sqlglot/commit/9c8a6001f41816035f391d046eb9692d6f13cefc) - correct parsing of TO_VARCHAR *(PR [#5840](https://github.com/tobymao/sqlglot/pull/5840) by [@geooo109](https://github.com/geooo109))*:

  correct parsing of TO_VARCHAR (#5840)

- due to [`1e9aef1`](https://github.com/tobymao/sqlglot/commit/1e9aef1bb20f4dc5e9c03d59cb3165c235c11ce1) - convert NULL annotations to UNKNOWN *(PR [#5842](https://github.com/tobymao/sqlglot/pull/5842) by [@georgesittas](https://github.com/georgesittas))*:

  convert NULL annotations to UNKNOWN (#5842)

- due to [`44c9e70`](https://github.com/tobymao/sqlglot/commit/44c9e70bd8c9421035eb0e87e4286061ec5d2fa8) - add tests for snowflake STARTSWITH function *(PR [#5847](https://github.com/tobymao/sqlglot/pull/5847) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  add tests for snowflake STARTSWITH function (#5847)

- due to [`0779c2d`](https://github.com/tobymao/sqlglot/commit/0779c2d4e8ce0228592de6882763940783fa5e87) - support BIT_X aggregates again for duckdb, postgres *(PR [#5851](https://github.com/tobymao/sqlglot/pull/5851) by [@georgesittas](https://github.com/georgesittas))*:

  support BIT_X aggregates again for duckdb, postgres (#5851)

- due to [`c50d6e3`](https://github.com/tobymao/sqlglot/commit/c50d6e3c7b96f00d27c34a02c8e0dced21e6c373) - annotate type for snowflake LEFT, RIGHT and SUBSTRING functions *(PR [#5849](https://github.com/tobymao/sqlglot/pull/5849) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  annotate type for snowflake LEFT, RIGHT and SUBSTRING functions (#5849)

- due to [`e441e16`](https://github.com/tobymao/sqlglot/commit/e441e16991626c2da2d38bc9c3a2b408e3f773bd) - make dump/pickling non-recursive to avoid hitting stack limits *(PR [#5850](https://github.com/tobymao/sqlglot/pull/5850) by [@tobymao](https://github.com/tobymao))*:

  make dump/pickling non-recursive to avoid hitting stack limits (#5850)

- due to [`b128339`](https://github.com/tobymao/sqlglot/commit/b12833977e2a395712481cf11e293fdbd70fd4ce) - annotate and add tests for snowflake LENGTH and LOWER functions *(PR [#5856](https://github.com/tobymao/sqlglot/pull/5856) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  annotate and add tests for snowflake LENGTH and LOWER functions (#5856)

- due to [`134957a`](https://github.com/tobymao/sqlglot/commit/134957af11c55a4ab16f58d0725d6bb8ab23eb28) - annotate types for Snowflake TRIM function *(PR [#5811](https://github.com/tobymao/sqlglot/pull/5811) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate types for Snowflake TRIM function (#5811)

- due to [`d3cd6bf`](https://github.com/tobymao/sqlglot/commit/d3cd6bf6e5fbaa490868ee3cd2cc99dd5e40a396) - Annotate and add tests for snowflake REPLACE and SPACE functions *(PR [#5871](https://github.com/tobymao/sqlglot/pull/5871) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Annotate and add tests for snowflake REPLACE and SPACE functions (#5871)


### :sparkles: New Features
- [`a398fb4`](https://github.com/tobymao/sqlglot/commit/a398fb4df28c868f4cfc34530044b9d7b78e2e90) - **singlestore**: Splitted truncation of multiple tables into several queries *(PR [#5839](https://github.com/tobymao/sqlglot/pull/5839) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`cd27c96`](https://github.com/tobymao/sqlglot/commit/cd27c96fe85aba5f54116f38649edd8db064a5e6) - **snowflake**: transpile `TO_HEX` from bigquery *(PR [#5838](https://github.com/tobymao/sqlglot/pull/5838) by [@YuvalOmerRep](https://github.com/YuvalOmerRep))*
- [`d2e4ab7`](https://github.com/tobymao/sqlglot/commit/d2e4ab7df41ae3601e9b66e1338db3d851729339) - **snowflake**: add tests for endswith function *(PR [#5846](https://github.com/tobymao/sqlglot/pull/5846) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`c50d6e3`](https://github.com/tobymao/sqlglot/commit/c50d6e3c7b96f00d27c34a02c8e0dced21e6c373) - **optimizer**: annotate type for snowflake LEFT, RIGHT and SUBSTRING functions *(PR [#5849](https://github.com/tobymao/sqlglot/pull/5849) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`ca6c8f7`](https://github.com/tobymao/sqlglot/commit/ca6c8f753ba8458544439e20671f0981c98d168d) - **singlestore**: Improved parsting/generation of exp.Show *(PR [#5853](https://github.com/tobymao/sqlglot/pull/5853) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`722eceb`](https://github.com/tobymao/sqlglot/commit/722ecebfa43aa5948031edd1828b6482a241d9ef) - **snowflake**: MD5Digest transpiling to MD5_BINARY *(PR [#5855](https://github.com/tobymao/sqlglot/pull/5855) by [@YuvalOmerRep](https://github.com/YuvalOmerRep))*
- [`b128339`](https://github.com/tobymao/sqlglot/commit/b12833977e2a395712481cf11e293fdbd70fd4ce) - **optimizer**: annotate and add tests for snowflake LENGTH and LOWER functions *(PR [#5856](https://github.com/tobymao/sqlglot/pull/5856) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`134957a`](https://github.com/tobymao/sqlglot/commit/134957af11c55a4ab16f58d0725d6bb8ab23eb28) - **optimizer**: annotate types for Snowflake TRIM function *(PR [#5811](https://github.com/tobymao/sqlglot/pull/5811) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`0475dae`](https://github.com/tobymao/sqlglot/commit/0475dae21231b85407bf778fd9f1abaecdeb68de) - **singlestore**: Marked several exp.Describe args as unsupported *(PR [#5861](https://github.com/tobymao/sqlglot/pull/5861) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`7a07b41`](https://github.com/tobymao/sqlglot/commit/7a07b41b2357149adc6afb50bb98e37e6a3175f1) - **optimizer**: Add tests for snowflake LTRIM and RTRIM functions *(PR [#5857](https://github.com/tobymao/sqlglot/pull/5857) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`fb90666`](https://github.com/tobymao/sqlglot/commit/fb90666ff3e710d70815a68defde3dc85aeef7b3) - **singlestore**: Added collate handling to exp.AlterColumn *(PR [#5864](https://github.com/tobymao/sqlglot/pull/5864) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`2f27692`](https://github.com/tobymao/sqlglot/commit/2f276929d6b6f788eb5b3ee0b1a8a8c108833474) - **snowflake**: JSONFormat transpiling to TO_JSON  *(PR [#5860](https://github.com/tobymao/sqlglot/pull/5860) by [@YuvalOmerRep](https://github.com/YuvalOmerRep))*
- [`487c811`](https://github.com/tobymao/sqlglot/commit/487c8119cbfaf2783f5f17ec90c8e69e4432a4fa) - **singlestore**: Fixed parsing/generation of exp.RenameColumn *(PR [#5865](https://github.com/tobymao/sqlglot/pull/5865) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`76cf4d8`](https://github.com/tobymao/sqlglot/commit/76cf4d892a6d011a2e0020fb1ea82518d4f49e71) - **bigquery**: add support for ML.TRANSLATE func *(PR [#5859](https://github.com/tobymao/sqlglot/pull/5859) by [@geooo109](https://github.com/geooo109))*
- [`a899eb1`](https://github.com/tobymao/sqlglot/commit/a899eb188d5e354d3ed56d1e7c32861eecf3e906) - **singlestore**: Fixed parsing and generation of VECTOR type *(PR [#5854](https://github.com/tobymao/sqlglot/pull/5854) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`0acf076`](https://github.com/tobymao/sqlglot/commit/0acf0769773061fca3ec03125a5d43a4aa9c8e4b) - **postgres**: Support `?|` JSONB operator *(PR [#5866](https://github.com/tobymao/sqlglot/pull/5866) by [@VaggelisD](https://github.com/VaggelisD))*
- [`bd4b278`](https://github.com/tobymao/sqlglot/commit/bd4b2780c32ee52d25b6539d7b4479b6a7f80d18) - **optimizer**: annotate types for Snowflake UPPER function *(PR [#5812](https://github.com/tobymao/sqlglot/pull/5812) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`edab189`](https://github.com/tobymao/sqlglot/commit/edab1890e2c790b737be4995a31667448eff148e) - **postgres**: Support ?& JSONB operator *(PR [#5867](https://github.com/tobymao/sqlglot/pull/5867) by [@VaggelisD](https://github.com/VaggelisD))*
- [`960ec06`](https://github.com/tobymao/sqlglot/commit/960ec069eb275b7b8cc6705dbbb1143159f06237) - **postgres**: Support #- JSONB operator *(PR [#5868](https://github.com/tobymao/sqlglot/pull/5868) by [@VaggelisD](https://github.com/VaggelisD))*
- [`d3cd6bf`](https://github.com/tobymao/sqlglot/commit/d3cd6bf6e5fbaa490868ee3cd2cc99dd5e40a396) - **optimizer**: Annotate and add tests for snowflake REPLACE and SPACE functions *(PR [#5871](https://github.com/tobymao/sqlglot/pull/5871) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`ba22531`](https://github.com/tobymao/sqlglot/commit/ba2253113ea5a7c76c8df7ec9b6faf37da698fa4) - **bigquery**: Add support for ML.FORECAST(...) *(PR [#5873](https://github.com/tobymao/sqlglot/pull/5873) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`9c8a600`](https://github.com/tobymao/sqlglot/commit/9c8a6001f41816035f391d046eb9692d6f13cefc) - **snowflake**: correct parsing of TO_VARCHAR *(PR [#5840](https://github.com/tobymao/sqlglot/pull/5840) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5837](https://github.com/tobymao/sqlglot/issues/5837) opened by [@ultrabear](https://github.com/ultrabear)*
- [`f3d07fd`](https://github.com/tobymao/sqlglot/commit/f3d07fd8a106b034f64bb100291671c0fe39a106) - **snowflake**: Enable parsing of COPY INTO without files list *(PR [#5841](https://github.com/tobymao/sqlglot/pull/5841) by [@whummer](https://github.com/whummer))*
- [`0ffb1fa`](https://github.com/tobymao/sqlglot/commit/0ffb1faac3b32aad845306eed0e000ff0d055554) - **duckdb**: transpile joins without ON/USING to CROSS JOIN *(PR [#5804](https://github.com/tobymao/sqlglot/pull/5804) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5795](https://github.com/tobymao/sqlglot/issues/5795) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`1e9aef1`](https://github.com/tobymao/sqlglot/commit/1e9aef1bb20f4dc5e9c03d59cb3165c235c11ce1) - **optimizer**: convert NULL annotations to UNKNOWN *(PR [#5842](https://github.com/tobymao/sqlglot/pull/5842) by [@georgesittas](https://github.com/georgesittas))*
- [`bbcf0d4`](https://github.com/tobymao/sqlglot/commit/bbcf0d4404ea014f08319c44313719b4377adcdb) - **duckdb**: support trailing commas before `FOR` in pivot, fixes [#5843](https://github.com/tobymao/sqlglot/pull/5843) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`ad8a408`](https://github.com/tobymao/sqlglot/commit/ad8a408a4e3e26e32472fc55c67b44687992ae47) - **parser**: more robust nested pipe syntax *(PR [#5845](https://github.com/tobymao/sqlglot/pull/5845) by [@geooo109](https://github.com/geooo109))*
- [`44c9e70`](https://github.com/tobymao/sqlglot/commit/44c9e70bd8c9421035eb0e87e4286061ec5d2fa8) - **optimizer**: add tests for snowflake STARTSWITH function *(PR [#5847](https://github.com/tobymao/sqlglot/pull/5847) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`0779c2d`](https://github.com/tobymao/sqlglot/commit/0779c2d4e8ce0228592de6882763940783fa5e87) - support BIT_X aggregates again for duckdb, postgres *(PR [#5851](https://github.com/tobymao/sqlglot/pull/5851) by [@georgesittas](https://github.com/georgesittas))*
- [`d131aab`](https://github.com/tobymao/sqlglot/commit/d131aab6815bf77d444a763d9bb4028d8f0e742d) - **redshift**: convert FETCH clauses to LIMIT for Redshift dialect *(PR [#5848](https://github.com/tobymao/sqlglot/pull/5848) by [@tomasmontielp](https://github.com/tomasmontielp))*
- [`b22c4ec`](https://github.com/tobymao/sqlglot/commit/b22c4ecf4c032d89ca737f01d614102aa9c2b1ed) - **fabric**: UUID to UNIQUEIDENTIFIER *(PR [#5863](https://github.com/tobymao/sqlglot/pull/5863) by [@fresioAS](https://github.com/fresioAS))*
- [`03d4f49`](https://github.com/tobymao/sqlglot/commit/03d4f49d92cd034d37074359b8c2cf96c5c3f5cf) - **clickhouse**: arrays are 1-indexed *(PR [#5862](https://github.com/tobymao/sqlglot/pull/5862) by [@joeyutong](https://github.com/joeyutong))*

### :recycle: Refactors
- [`e441e16`](https://github.com/tobymao/sqlglot/commit/e441e16991626c2da2d38bc9c3a2b408e3f773bd) - make dump/pickling non-recursive to avoid hitting stack limits *(PR [#5850](https://github.com/tobymao/sqlglot/pull/5850) by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`b244f30`](https://github.com/tobymao/sqlglot/commit/b244f30524846bd08d03a73410ae9b4674254ecd) - move `exp.Contains` to `BOOLEAN` entry in `TYPE_TO_EXPRESSIONS` *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.13.2] - 2025-09-08
### :bug: Bug Fixes
- [`5e7979f`](https://github.com/tobymao/sqlglot/commit/5e7979f3cf5f7996e198ddd81069d49a4a3b9391) - select session *(PR [#5836](https://github.com/tobymao/sqlglot/pull/5836) by [@tobymao](https://github.com/tobymao))*


## [v27.13.1] - 2025-09-08
### :bug: Bug Fixes
- [`f3d55c0`](https://github.com/tobymao/sqlglot/commit/f3d55c05c8411c9871f8ca4d23f726f976c9236b) - remove always token *(PR [#5832](https://github.com/tobymao/sqlglot/pull/5832) by [@tobymao](https://github.com/tobymao))*
- [`1724775`](https://github.com/tobymao/sqlglot/commit/1724775429f66c2768864c8f96ace861eaa435fd) - suppert types() with no args *(PR [#5833](https://github.com/tobymao/sqlglot/pull/5833) by [@tobymao](https://github.com/tobymao))*
- [`31c82c6`](https://github.com/tobymao/sqlglot/commit/31c82c6d6cd402e59cb59a94daafd22410eae0f6) - support `case.*` *(PR [#5835](https://github.com/tobymao/sqlglot/pull/5835) by [@georgesittas](https://github.com/georgesittas))*
- [`c00f73b`](https://github.com/tobymao/sqlglot/commit/c00f73bac2530a62c25093c60bf02d0a4231bb0b) - window spec no and only exclude *(PR [#5834](https://github.com/tobymao/sqlglot/pull/5834) by [@tobymao](https://github.com/tobymao))*


## [v27.13.0] - 2025-09-08
### :boom: BREAKING CHANGES
- due to [`3726b33`](https://github.com/tobymao/sqlglot/commit/3726b33bb6b4ab286617f510e96e1fbd27c429f3) - support nulls_first arg for array_sort *(PR [#5802](https://github.com/tobymao/sqlglot/pull/5802) by [@treysp](https://github.com/treysp))*:

  support nulls_first arg for array_sort (#5802)

- due to [`cf1d1e3`](https://github.com/tobymao/sqlglot/commit/cf1d1e3e0ef9e6cd1b1c6128c63ddf06c30f1339) - annotate type for snowflake's REVERSE function *(PR [#5803](https://github.com/tobymao/sqlglot/pull/5803) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*:

  annotate type for snowflake's REVERSE function (#5803)

- due to [`ad0b407`](https://github.com/tobymao/sqlglot/commit/ad0b407098e1611d4fc0e1f0916511337b9aefdb) - Mark 'BEGIN' as TokenType.BEGIN for transactions *(PR [#5826](https://github.com/tobymao/sqlglot/pull/5826) by [@VaggelisD](https://github.com/VaggelisD))*:

  Mark 'BEGIN' as TokenType.BEGIN for transactions (#5826)

- due to [`0198282`](https://github.com/tobymao/sqlglot/commit/0198282a82bbf3e81476e164718d63fd1210acdc) - : Update tests for concat string function *(PR [#5809](https://github.com/tobymao/sqlglot/pull/5809) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  : Update tests for concat string function (#5809)

- due to [`db2c430`](https://github.com/tobymao/sqlglot/commit/db2c4303237a1244070c359245c398a724df6de2) - annoate the "contains" function *(PR [#5829](https://github.com/tobymao/sqlglot/pull/5829) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  annoate the "contains" function (#5829)


### :sparkles: New Features
- [`cf1d1e3`](https://github.com/tobymao/sqlglot/commit/cf1d1e3e0ef9e6cd1b1c6128c63ddf06c30f1339) - **optimizer**: annotate type for snowflake's REVERSE function *(PR [#5803](https://github.com/tobymao/sqlglot/pull/5803) by [@fivetran-BradfordPaskewitz](https://github.com/fivetran-BradfordPaskewitz))*
- [`1d07c52`](https://github.com/tobymao/sqlglot/commit/1d07c52badb2e392e6895cbb275d2224789366c9) - **SingleStore**: Implemented generation of CURRENT_DATETIME *(PR [#5816](https://github.com/tobymao/sqlglot/pull/5816) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`cad4fd0`](https://github.com/tobymao/sqlglot/commit/cad4fd0c5b0ec90e693fa6883af0ab287b921019) - **singlestore**: Added handling of exp.JSONObject *(PR [#5817](https://github.com/tobymao/sqlglot/pull/5817) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`e3cb076`](https://github.com/tobymao/sqlglot/commit/e3cb0766bd5c3ccb31ea52cfc76201f548798dc1) - **singlestore**: Implemented generation of exp.StandardHash *(PR [#5823](https://github.com/tobymao/sqlglot/pull/5823) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`0198282`](https://github.com/tobymao/sqlglot/commit/0198282a82bbf3e81476e164718d63fd1210acdc) - **optimizer**: : Update tests for concat string function *(PR [#5809](https://github.com/tobymao/sqlglot/pull/5809) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`4e8a436`](https://github.com/tobymao/sqlglot/commit/4e8a436c16f487a72bd1ac2432bcb1c46599d901) - **singlestore**: Added generation of exp.JSONExists *(PR [#5820](https://github.com/tobymao/sqlglot/pull/5820) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`82bea49`](https://github.com/tobymao/sqlglot/commit/82bea49978ae459492b5127a2a52049826e2fd06) - **singlestore**: Refactored parsing of JSON_BUILD_OBJECT *(PR [#5828](https://github.com/tobymao/sqlglot/pull/5828) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`f7d38c3`](https://github.com/tobymao/sqlglot/commit/f7d38c3a10c505346f04e39a2712d60b4c96370f) - **singlestore**: Implemented generation of exp.Stuff *(PR [#5825](https://github.com/tobymao/sqlglot/pull/5825) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`030a5b5`](https://github.com/tobymao/sqlglot/commit/030a5b5ea03ecee869b07cfd27f4ea044732822e) - **singlestore**: Added generation of exp.JSONBExists *(PR [#5821](https://github.com/tobymao/sqlglot/pull/5821) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`e58fef1`](https://github.com/tobymao/sqlglot/commit/e58fef1d6dc654a3b36461bcbea21c99cdc96477) - **singlestore**: Implemented parsing and generation of exp.MatchAgainst *(PR [#5822](https://github.com/tobymao/sqlglot/pull/5822) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`e94f530`](https://github.com/tobymao/sqlglot/commit/e94f530af0e0cdad995b4c8dc5ed86953490d37f) - **singlestore**: Added handling of exp.JSONArray *(PR [#5818](https://github.com/tobymao/sqlglot/pull/5818) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`1c42ef4`](https://github.com/tobymao/sqlglot/commit/1c42ef4374aeab8a1ee9848892d7f8c4511c7f04) - **singlestore**: Fixed parsing/generation of exp.JSONArrayAgg *(PR [#5819](https://github.com/tobymao/sqlglot/pull/5819) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`67219f0`](https://github.com/tobymao/sqlglot/commit/67219f0606231514f430e146e2fdb99e796f718b) - **singlestore**: Added support of UTC_TIMESTAMP and CURRENT_TIMESTAMP *(PR [#5808](https://github.com/tobymao/sqlglot/pull/5808) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`db2c430`](https://github.com/tobymao/sqlglot/commit/db2c4303237a1244070c359245c398a724df6de2) - **optimizer**: annoate the "contains" function *(PR [#5829](https://github.com/tobymao/sqlglot/pull/5829) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*

### :bug: Bug Fixes
- [`3726b33`](https://github.com/tobymao/sqlglot/commit/3726b33bb6b4ab286617f510e96e1fbd27c429f3) - **snowflake**: support nulls_first arg for array_sort *(PR [#5802](https://github.com/tobymao/sqlglot/pull/5802) by [@treysp](https://github.com/treysp))*
- [`3408de0`](https://github.com/tobymao/sqlglot/commit/3408de09e50d2510c1a6f511dc2dec357059044f) - parsing quoted built-in data types *(PR [#5810](https://github.com/tobymao/sqlglot/pull/5810) by [@treysp](https://github.com/treysp))*
- [`ad0b407`](https://github.com/tobymao/sqlglot/commit/ad0b407098e1611d4fc0e1f0916511337b9aefdb) - **postgres**: Mark 'BEGIN' as TokenType.BEGIN for transactions *(PR [#5826](https://github.com/tobymao/sqlglot/pull/5826) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5815](https://github.com/tobymao/sqlglot/issues/5815) opened by [@karakanb](https://github.com/karakanb)*
- [`e1a1b5b`](https://github.com/tobymao/sqlglot/commit/e1a1b5befefb0ca30ac1310cecb82a44f6089034) - **snowflake**: transpile BigQuery's `&` to `BITAND` *(PR [#5827](https://github.com/tobymao/sqlglot/pull/5827) by [@YuvalOmerRep](https://github.com/YuvalOmerRep))*
- [`32d0278`](https://github.com/tobymao/sqlglot/commit/32d027827eaa7aa0cd9faf2ac1f84739f914050f) - parse and generation of BITWISE AGG funcs across dialects *(PR [#5831](https://github.com/tobymao/sqlglot/pull/5831) by [@geooo109](https://github.com/geooo109))*
- [`5f39a83`](https://github.com/tobymao/sqlglot/commit/5f39a83f1ff957aca57eb4745f83c296436acaac) - **bigquery**: properly generate `LIMIT` for `STRING_AGG` *(PR [#5830](https://github.com/tobymao/sqlglot/pull/5830) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`164fec1`](https://github.com/tobymao/sqlglot/commit/164fec1b36e3c7df41e2e5a5ad6b226fc5f76305) - **optimizer**: test type annotation for snowflake CHARINDEX function *(PR [#5805](https://github.com/tobymao/sqlglot/pull/5805) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*


## [v27.12.0] - 2025-09-04
### :boom: BREAKING CHANGES
- due to [`1c551d5`](https://github.com/tobymao/sqlglot/commit/1c551d5ed3315e314013c1f063deabd9d8613e5d) - parse and annotate type for bq TO_JSON *(PR [#5768](https://github.com/tobymao/sqlglot/pull/5768) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq TO_JSON (#5768)

- due to [`1707f2d`](https://github.com/tobymao/sqlglot/commit/1707f2d7f9d3b58e8c216db638f8e572f9fe6f13) - annotate type for ABS *(PR [#5770](https://github.com/tobymao/sqlglot/pull/5770) by [@geooo109](https://github.com/geooo109))*:

  annotate type for ABS (#5770)

- due to [`69acc51`](https://github.com/tobymao/sqlglot/commit/69acc5142b2d4f0b30832c350aa49f16d1adabef) - annotate type for bq IS_INF, IS_NAN *(PR [#5771](https://github.com/tobymao/sqlglot/pull/5771) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq IS_INF, IS_NAN (#5771)

- due to [`0da2076`](https://github.com/tobymao/sqlglot/commit/0da207652331920416b29e2cc67bdc3c3f964466) - annotate type for bq CBRT *(PR [#5772](https://github.com/tobymao/sqlglot/pull/5772) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq CBRT (#5772)

- due to [`a4968cb`](https://github.com/tobymao/sqlglot/commit/a4968cb5693670c1a2e9cd2c86404dd90fd76160) - annotate type for bq RAND *(PR [#5774](https://github.com/tobymao/sqlglot/pull/5774) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq RAND (#5774)

- due to [`3e63350`](https://github.com/tobymao/sqlglot/commit/3e63350bd1d58b510cecd1a573d27be3fd2565ce) - parse and annotate type for bq ACOS *(PR [#5776](https://github.com/tobymao/sqlglot/pull/5776) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq ACOS (#5776)

- due to [`2be9d01`](https://github.com/tobymao/sqlglot/commit/2be9d01830c778186dc274c94c6db0dd6c4116d1) - parse and annotate type for bq ACOSH *(PR [#5779](https://github.com/tobymao/sqlglot/pull/5779) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq ACOSH (#5779)

- due to [`b77d3da`](https://github.com/tobymao/sqlglot/commit/b77d3da8f2548858d2b9d8590fcde83e1ec62b8a) - remove `"EXCLUDE" -> TokenType.EXCEPT` in DuckDB, Snowflake *(PR [#5766](https://github.com/tobymao/sqlglot/pull/5766) by [@treysp](https://github.com/treysp))*:

  remove `"EXCLUDE" -> TokenType.EXCEPT` in DuckDB, Snowflake (#5766)

- due to [`7da2f31`](https://github.com/tobymao/sqlglot/commit/7da2f31d6613f16585e98c3fa1f592c617ae40c9) - parse and annotate type for bq ASIN/H *(PR [#5783](https://github.com/tobymao/sqlglot/pull/5783) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq ASIN/H (#5783)

- due to [`341ea83`](https://github.com/tobymao/sqlglot/commit/341ea83a07c707fdbf565b8d9ef4b9b6341ed1d5) - parse and annotate type for bq ATAN/H/2 *(PR [#5784](https://github.com/tobymao/sqlglot/pull/5784) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq ATAN/H/2 (#5784)

- due to [`aa360cb`](https://github.com/tobymao/sqlglot/commit/aa360cb0e204aa056557ff8b15aa2d4f678430e6) - use regexp_like as it exists *(PR [#5781](https://github.com/tobymao/sqlglot/pull/5781) by [@jasonthomassql](https://github.com/jasonthomassql))*:

  use regexp_like as it exists (#5781)

- due to [`c2a1ad4`](https://github.com/tobymao/sqlglot/commit/c2a1ad4050771401a5b26bcadd90060e4527fbff) - parse and annotate type for bq COT/H *(PR [#5786](https://github.com/tobymao/sqlglot/pull/5786) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq COT/H (#5786)

- due to [`316ae91`](https://github.com/tobymao/sqlglot/commit/316ae913d8b1a63f3071ebb1b826328108d74cef) - Added handling of UTC_DATE and exp.CurrentDate *(PR [#5785](https://github.com/tobymao/sqlglot/pull/5785) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*:

  Added handling of UTC_DATE and exp.CurrentDate (#5785)

- due to [`2c6d237`](https://github.com/tobymao/sqlglot/commit/2c6d23742ea9fcc2b9c784315d3d5364e360fea5) - parse and annotate type for bq CSC/H *(PR [#5787](https://github.com/tobymao/sqlglot/pull/5787) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq CSC/H (#5787)

- due to [`8a35076`](https://github.com/tobymao/sqlglot/commit/8a350763c2337f6910a5f0e19af387ba488fcb70) - parse and annotate type for bq SEC/H *(PR [#5788](https://github.com/tobymao/sqlglot/pull/5788) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq SEC/H (#5788)

- due to [`79901cb`](https://github.com/tobymao/sqlglot/commit/79901cb506737ae1932fa44a705858d2597ee587) - parse and annotate type for bq SIN\H *(PR [#5790](https://github.com/tobymao/sqlglot/pull/5790) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq SIN\H (#5790)

- due to [`74fb547`](https://github.com/tobymao/sqlglot/commit/74fb5476def1b389da425885db56bd6592fd7f78) - parse and annotate type for bq RANGE_BUCKET *(PR [#5793](https://github.com/tobymao/sqlglot/pull/5793) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq RANGE_BUCKET (#5793)

- due to [`eca65e8`](https://github.com/tobymao/sqlglot/commit/eca65e8b79f65850b014a4cb7913ba4a5861dbe9) - parse and annotate type for bq COSINE/EUCLIDEAN_DISTANCE *(PR [#5792](https://github.com/tobymao/sqlglot/pull/5792) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq COSINE/EUCLIDEAN_DISTANCE (#5792)

- due to [`a180d3f`](https://github.com/tobymao/sqlglot/commit/a180d3f2f9f3938611027269028c03274aa1889c) - parse and annotate type for bq SAFE math funcs *(PR [#5797](https://github.com/tobymao/sqlglot/pull/5797) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq SAFE math funcs (#5797)

- due to [`fc7ad7a`](https://github.com/tobymao/sqlglot/commit/fc7ad7a4d953424b56542eacfe1835f5789921c7) - parse ALTER SESSION  *(PR [#5734](https://github.com/tobymao/sqlglot/pull/5734) by [@tekumara](https://github.com/tekumara))*:

  parse ALTER SESSION  (#5734)

- due to [`8ec1a6c`](https://github.com/tobymao/sqlglot/commit/8ec1a6cf5a8edc2d834c713ce0fd8d87237f11ed) - annotate type for bq STRING_AGG *(PR [#5798](https://github.com/tobymao/sqlglot/pull/5798) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq STRING_AGG (#5798)

- due to [`dd97bfa`](https://github.com/tobymao/sqlglot/commit/dd97bfa1dc2f86b727c55b06b3c54b18c02e360d) - annotate type for bq DATETIME_TRUNC *(PR [#5799](https://github.com/tobymao/sqlglot/pull/5799) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq DATETIME_TRUNC (#5799)

- due to [`d3e9dda`](https://github.com/tobymao/sqlglot/commit/d3e9dda183695dd1e4a9832a6671bccc6db561a0) - annotate type for bq GENERATE_UUID *(commit by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq GENERATE_UUID


### :sparkles: New Features
- [`1c551d5`](https://github.com/tobymao/sqlglot/commit/1c551d5ed3315e314013c1f063deabd9d8613e5d) - **optimizer**: parse and annotate type for bq TO_JSON *(PR [#5768](https://github.com/tobymao/sqlglot/pull/5768) by [@geooo109](https://github.com/geooo109))*
- [`a024d48`](https://github.com/tobymao/sqlglot/commit/a024d48fedd049796329050a1f51822dd1388695) - **singlestore**: Added generation of exp.TsOrDsDiff *(PR [#5769](https://github.com/tobymao/sqlglot/pull/5769) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`1707f2d`](https://github.com/tobymao/sqlglot/commit/1707f2d7f9d3b58e8c216db638f8e572f9fe6f13) - **optimizer**: annotate type for ABS *(PR [#5770](https://github.com/tobymao/sqlglot/pull/5770) by [@geooo109](https://github.com/geooo109))*
- [`69acc51`](https://github.com/tobymao/sqlglot/commit/69acc5142b2d4f0b30832c350aa49f16d1adabef) - **optimizer**: annotate type for bq IS_INF, IS_NAN *(PR [#5771](https://github.com/tobymao/sqlglot/pull/5771) by [@geooo109](https://github.com/geooo109))*
- [`0da2076`](https://github.com/tobymao/sqlglot/commit/0da207652331920416b29e2cc67bdc3c3f964466) - **optimizer**: annotate type for bq CBRT *(PR [#5772](https://github.com/tobymao/sqlglot/pull/5772) by [@geooo109](https://github.com/geooo109))*
- [`a4968cb`](https://github.com/tobymao/sqlglot/commit/a4968cb5693670c1a2e9cd2c86404dd90fd76160) - **optimizer**: annotate type for bq RAND *(PR [#5774](https://github.com/tobymao/sqlglot/pull/5774) by [@geooo109](https://github.com/geooo109))*
- [`dd7781a`](https://github.com/tobymao/sqlglot/commit/dd7781a15b842a5826714958ed7af9024903cd1e) - **singlestore**: Fixed generation of exp.Collate *(PR [#5775](https://github.com/tobymao/sqlglot/pull/5775) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`fb684cb`](https://github.com/tobymao/sqlglot/commit/fb684cbdb6178ddc441f598cc1a6e914291cd00e) - **singelstore**: Fixed generation of exp.RegexpILike *(PR [#5777](https://github.com/tobymao/sqlglot/pull/5777) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`3e63350`](https://github.com/tobymao/sqlglot/commit/3e63350bd1d58b510cecd1a573d27be3fd2565ce) - **optimizer**: parse and annotate type for bq ACOS *(PR [#5776](https://github.com/tobymao/sqlglot/pull/5776) by [@geooo109](https://github.com/geooo109))*
- [`8705a78`](https://github.com/tobymao/sqlglot/commit/8705a787df034b4cecb4ba95e9599772c5561ba9) - **singlestore**: Fixed generation of exp.CastToStrType *(PR [#5778](https://github.com/tobymao/sqlglot/pull/5778) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`e3c35ad`](https://github.com/tobymao/sqlglot/commit/e3c35ade797f46549cc803e1acd8816041713a10) - **singlestore**: Fixed generation of exp.UnicodeString *(PR [#5773](https://github.com/tobymao/sqlglot/pull/5773) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`2be9d01`](https://github.com/tobymao/sqlglot/commit/2be9d01830c778186dc274c94c6db0dd6c4116d1) - **optimizer**: parse and annotate type for bq ACOSH *(PR [#5779](https://github.com/tobymao/sqlglot/pull/5779) by [@geooo109](https://github.com/geooo109))*
- [`7da2f31`](https://github.com/tobymao/sqlglot/commit/7da2f31d6613f16585e98c3fa1f592c617ae40c9) - **optimizer**: parse and annotate type for bq ASIN/H *(PR [#5783](https://github.com/tobymao/sqlglot/pull/5783) by [@geooo109](https://github.com/geooo109))*
- [`341ea83`](https://github.com/tobymao/sqlglot/commit/341ea83a07c707fdbf565b8d9ef4b9b6341ed1d5) - **optimizer**: parse and annotate type for bq ATAN/H/2 *(PR [#5784](https://github.com/tobymao/sqlglot/pull/5784) by [@geooo109](https://github.com/geooo109))*
- [`be54a45`](https://github.com/tobymao/sqlglot/commit/be54a458413ce3be6c321e5f4feb3e5df5ee6d08) - **singlestore**: Implemented generation of exp.Cbrt *(PR [#5782](https://github.com/tobymao/sqlglot/pull/5782) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`aa360cb`](https://github.com/tobymao/sqlglot/commit/aa360cb0e204aa056557ff8b15aa2d4f678430e6) - **databricks**: use regexp_like as it exists *(PR [#5781](https://github.com/tobymao/sqlglot/pull/5781) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`c2a1ad4`](https://github.com/tobymao/sqlglot/commit/c2a1ad4050771401a5b26bcadd90060e4527fbff) - **optimizer**: parse and annotate type for bq COT/H *(PR [#5786](https://github.com/tobymao/sqlglot/pull/5786) by [@geooo109](https://github.com/geooo109))*
- [`316ae91`](https://github.com/tobymao/sqlglot/commit/316ae913d8b1a63f3071ebb1b826328108d74cef) - **singlestore**: Added handling of UTC_DATE and exp.CurrentDate *(PR [#5785](https://github.com/tobymao/sqlglot/pull/5785) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`2c6d237`](https://github.com/tobymao/sqlglot/commit/2c6d23742ea9fcc2b9c784315d3d5364e360fea5) - **optimizer**: parse and annotate type for bq CSC/H *(PR [#5787](https://github.com/tobymao/sqlglot/pull/5787) by [@geooo109](https://github.com/geooo109))*
- [`8a35076`](https://github.com/tobymao/sqlglot/commit/8a350763c2337f6910a5f0e19af387ba488fcb70) - **optimizer**: parse and annotate type for bq SEC/H *(PR [#5788](https://github.com/tobymao/sqlglot/pull/5788) by [@geooo109](https://github.com/geooo109))*
- [`566bfb2`](https://github.com/tobymao/sqlglot/commit/566bfb2a64a64b74da63b3a89d68caf702ab6522) - **singlestore**: Added support of UTC_TIME and CURRENT_TIME *(PR [#5789](https://github.com/tobymao/sqlglot/pull/5789) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`79901cb`](https://github.com/tobymao/sqlglot/commit/79901cb506737ae1932fa44a705858d2597ee587) - **optimizer**: parse and annotate type for bq SIN\H *(PR [#5790](https://github.com/tobymao/sqlglot/pull/5790) by [@geooo109](https://github.com/geooo109))*
- [`74fb547`](https://github.com/tobymao/sqlglot/commit/74fb5476def1b389da425885db56bd6592fd7f78) - **optimizer**: parse and annotate type for bq RANGE_BUCKET *(PR [#5793](https://github.com/tobymao/sqlglot/pull/5793) by [@geooo109](https://github.com/geooo109))*
- [`eca65e8`](https://github.com/tobymao/sqlglot/commit/eca65e8b79f65850b014a4cb7913ba4a5861dbe9) - **optimizer**: parse and annotate type for bq COSINE/EUCLIDEAN_DISTANCE *(PR [#5792](https://github.com/tobymao/sqlglot/pull/5792) by [@geooo109](https://github.com/geooo109))*
- [`a180d3f`](https://github.com/tobymao/sqlglot/commit/a180d3f2f9f3938611027269028c03274aa1889c) - **optimizer**: parse and annotate type for bq SAFE math funcs *(PR [#5797](https://github.com/tobymao/sqlglot/pull/5797) by [@geooo109](https://github.com/geooo109))*
- [`fc7ad7a`](https://github.com/tobymao/sqlglot/commit/fc7ad7a4d953424b56542eacfe1835f5789921c7) - **snowflake**: parse ALTER SESSION  *(PR [#5734](https://github.com/tobymao/sqlglot/pull/5734) by [@tekumara](https://github.com/tekumara))*
- [`8ec1a6c`](https://github.com/tobymao/sqlglot/commit/8ec1a6cf5a8edc2d834c713ce0fd8d87237f11ed) - **optimizer**: annotate type for bq STRING_AGG *(PR [#5798](https://github.com/tobymao/sqlglot/pull/5798) by [@geooo109](https://github.com/geooo109))*
- [`dd97bfa`](https://github.com/tobymao/sqlglot/commit/dd97bfa1dc2f86b727c55b06b3c54b18c02e360d) - **optimizer**: annotate type for bq DATETIME_TRUNC *(PR [#5799](https://github.com/tobymao/sqlglot/pull/5799) by [@geooo109](https://github.com/geooo109))*
- [`d3e9dda`](https://github.com/tobymao/sqlglot/commit/d3e9dda183695dd1e4a9832a6671bccc6db561a0) - **optimizer**: annotate type for bq GENERATE_UUID *(commit by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`d8f6a37`](https://github.com/tobymao/sqlglot/commit/d8f6a376ba1fcca48e4a65923dd7a319ce6cfb91) - **optimizer**: allow aliased negative integer literal as group by column *(PR [#5791](https://github.com/tobymao/sqlglot/pull/5791) by [@treysp](https://github.com/treysp))*
- [`1259576`](https://github.com/tobymao/sqlglot/commit/1259576283f1d45abb70ec40c60e500214a27b6f) - **hive**: DATE_SUB to DATE_ADD use parens if needed *(PR [#5796](https://github.com/tobymao/sqlglot/pull/5796) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5794](https://github.com/tobymao/sqlglot/issues/5794) opened by [@mingelchan](https://github.com/mingelchan)*
- [`b0516b4`](https://github.com/tobymao/sqlglot/commit/b0516b4bc9cf2bba2cb57e6bb79ff09b5e2244e3) - **optimizer**: Do not qualify columns if a projection coflicts with a source *(PR [#5780](https://github.com/tobymao/sqlglot/pull/5780) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5262](https://github.com/TobikoData/sqlmesh/issues/5262) opened by [@mChlopek](https://github.com/mChlopek)*
- [`8af0d40`](https://github.com/tobymao/sqlglot/commit/8af0d40055450f71b7e36e576f4a9a1104bc02b2) - **parser**: address edge case where `values` is used as an identifier *(PR [#5801](https://github.com/tobymao/sqlglot/pull/5801) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`b77d3da`](https://github.com/tobymao/sqlglot/commit/b77d3da8f2548858d2b9d8590fcde83e1ec62b8a) - remove `"EXCLUDE" -> TokenType.EXCEPT` in DuckDB, Snowflake *(PR [#5766](https://github.com/tobymao/sqlglot/pull/5766) by [@treysp](https://github.com/treysp))*
- [`005564a`](https://github.com/tobymao/sqlglot/commit/005564ab28cb14be469f09e89b01275d6e25874e) - **snowflake**: refactor logic related to ALTER SESSION *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.11.0] - 2025-09-03
### :boom: BREAKING CHANGES
- due to [`baffd2c`](https://github.com/tobymao/sqlglot/commit/baffd2c0be9657683781f3f8831c47e32dbf68bb) - parse and annotate type for bq REGEXP_INSTR *(PR [#5710](https://github.com/tobymao/sqlglot/pull/5710) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq REGEXP_INSTR (#5710)

- due to [`b79eb19`](https://github.com/tobymao/sqlglot/commit/b79eb198cc21203efa82128b357d435338e9133d) - annotate type for bq ROW_NUMBER *(PR [#5716](https://github.com/tobymao/sqlglot/pull/5716) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq ROW_NUMBER (#5716)

- due to [`f709bef`](https://github.com/tobymao/sqlglot/commit/f709bef3af7cd0daa25fe3d58b1753c3e65720ef) - annotate type for bq FIRST_VALUE *(PR [#5718](https://github.com/tobymao/sqlglot/pull/5718) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq FIRST_VALUE (#5718)

- due to [`15a9061`](https://github.com/tobymao/sqlglot/commit/15a906170e5d5cdaa207ec7607edfdd7d4a8b774) - annotate type for bq PERCENTILE_DISC *(PR [#5722](https://github.com/tobymao/sqlglot/pull/5722) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq PERCENTILE_DISC (#5722)

- due to [`7d49609`](https://github.com/tobymao/sqlglot/commit/7d4960963f0ef70b96f5b969bb008d2742e833ea) - annotate type for bq NTH_VALUE *(PR [#5720](https://github.com/tobymao/sqlglot/pull/5720) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq NTH_VALUE (#5720)

- due to [`d41acf1`](https://github.com/tobymao/sqlglot/commit/d41acf11221bee30a5ae089cbac9b158ed3dd515) - annotate type for bq LEAD *(PR [#5719](https://github.com/tobymao/sqlglot/pull/5719) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq LEAD (#5719)

- due to [`ff12130`](https://github.com/tobymao/sqlglot/commit/ff12130c23a215917f20fda7d50322f1cb7de599) - annotate type for bq PERNCENTILE_CONT *(PR [#5729](https://github.com/tobymao/sqlglot/pull/5729) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq PERNCENTILE_CONT (#5729)

- due to [`fdb8a0a`](https://github.com/tobymao/sqlglot/commit/fdb8a0a6d0d74194255f313bd934db7fc1ce0d3f) - parse and annotate type for bq FORMAT *(PR [#5715](https://github.com/tobymao/sqlglot/pull/5715) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq FORMAT (#5715)

- due to [`012bdd3`](https://github.com/tobymao/sqlglot/commit/012bdd3c8aeff180f85354ffd403fc1aa5815dcf) - parse and annotate type for bq CUME_DIST *(PR [#5735](https://github.com/tobymao/sqlglot/pull/5735) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq CUME_DIST (#5735)

- due to [`b99eaeb`](https://github.com/tobymao/sqlglot/commit/b99eaeb0c6eb3dc613e76d205e02632bd6af353b) - parse and annotate type for bq DENSE_RANK *(PR [#5736](https://github.com/tobymao/sqlglot/pull/5736) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq DENSE_RANK (#5736)

- due to [`bb95c73`](https://github.com/tobymao/sqlglot/commit/bb95c7312c942ef987955f01e060604d60e32e83) - parse and annotate type for bq RANK *(PR [#5738](https://github.com/tobymao/sqlglot/pull/5738) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq RANK (#5738)

- due to [`8713c08`](https://github.com/tobymao/sqlglot/commit/8713c082b0aa8454a5773fc2a85e08a132dc6ce3) - parse and annotate type for bq PERCENT_RANK *(PR [#5739](https://github.com/tobymao/sqlglot/pull/5739) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq PERCENT_RANK (#5739)

- due to [`9ce4e31`](https://github.com/tobymao/sqlglot/commit/9ce4e31aecbde6ea1f227a7166c0f3dc9e302a66) - annotate type for bq JSON_OBJECT *(PR [#5740](https://github.com/tobymao/sqlglot/pull/5740) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq JSON_OBJECT (#5740)

- due to [`d35ec6e`](https://github.com/tobymao/sqlglot/commit/d35ec6e37e21cf3cec848ed55bd73128c4633cd2) - annotate type for bq JSON_QUERY/JSON_QUERY_ARRAY *(PR [#5741](https://github.com/tobymao/sqlglot/pull/5741) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq JSON_QUERY/JSON_QUERY_ARRAY (#5741)

- due to [`4753642`](https://github.com/tobymao/sqlglot/commit/4753642cfcfb1f192ec4d21a492737b27affef09) - annotate type for bq JSON_EXTRACT_SCALAR *(commit by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq JSON_EXTRACT_SCALAR

- due to [`113a530`](https://github.com/tobymao/sqlglot/commit/113a5308d050fd5ceacab4c6188e5eea5dd740b1) - parse and annotate type for bq JSON_ARRAY_APPEND *(PR [#5747](https://github.com/tobymao/sqlglot/pull/5747) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq JSON_ARRAY_APPEND (#5747)

- due to [`268e2c6`](https://github.com/tobymao/sqlglot/commit/268e2c694d1eb99f1fe64477bc38ed4946bf1c32) - parse and annotate type for bq JSON_ARRAY_INSERT *(PR [#5748](https://github.com/tobymao/sqlglot/pull/5748) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq JSON_ARRAY_INSERT (#5748)

- due to [`455ec1f`](https://github.com/tobymao/sqlglot/commit/455ec1f4f8aecb5435fa4cb2912bfc21db8dd44d) - parse and annotate type for bq JSON_KEYS *(PR [#5749](https://github.com/tobymao/sqlglot/pull/5749) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq JSON_KEYS (#5749)

- due to [`59895fa`](https://github.com/tobymao/sqlglot/commit/59895faa23ebe1b27938c37a7b39df87de609844) - parse and annotate type for bq JSON_REMOVE *(PR [#5750](https://github.com/tobymao/sqlglot/pull/5750) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq JSON_REMOVE (#5750)

- due to [`06d7df7`](https://github.com/tobymao/sqlglot/commit/06d7df7a05f2824cabf48e8d1e8a4ebca8fda496) - parse and annotate type for bq JSON_SET *(PR [#5751](https://github.com/tobymao/sqlglot/pull/5751) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq JSON_SET (#5751)

- due to [`e72b341`](https://github.com/tobymao/sqlglot/commit/e72b3419c8a367caa0e5e80030979cd94e87a40d) - parse and annotate type for bq JSON_STRIP_NULLS *(PR [#5753](https://github.com/tobymao/sqlglot/pull/5753) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq JSON_STRIP_NULLS (#5753)

- due to [`5de61a7`](https://github.com/tobymao/sqlglot/commit/5de61a7ab850d4e68fde4d76ee396d30d7bdef33) - parse and annotate type for bq JSON_EXTRACT_STRING_ARRAY *(PR [#5758](https://github.com/tobymao/sqlglot/pull/5758) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq JSON_EXTRACT_STRING_ARRAY (#5758)

- due to [`36c9393`](https://github.com/tobymao/sqlglot/commit/36c93939575a19bd611269719c39d3d216be8cde) - parse and annotate type for bq JSON LAX funcs *(PR [#5760](https://github.com/tobymao/sqlglot/pull/5760) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq JSON LAX funcs (#5760)

- due to [`88862b5`](https://github.com/tobymao/sqlglot/commit/88862b56bc29c8a600b4d0e4693d5846d3a577ff) - annotate type for bq TO_JSON_STRING *(PR [#5762](https://github.com/tobymao/sqlglot/pull/5762) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq TO_JSON_STRING (#5762)


### :sparkles: New Features
- [`baffd2c`](https://github.com/tobymao/sqlglot/commit/baffd2c0be9657683781f3f8831c47e32dbf68bb) - **optimizer**: parse and annotate type for bq REGEXP_INSTR *(PR [#5710](https://github.com/tobymao/sqlglot/pull/5710) by [@geooo109](https://github.com/geooo109))*
- [`b79eb19`](https://github.com/tobymao/sqlglot/commit/b79eb198cc21203efa82128b357d435338e9133d) - **optimizer**: annotate type for bq ROW_NUMBER *(PR [#5716](https://github.com/tobymao/sqlglot/pull/5716) by [@geooo109](https://github.com/geooo109))*
- [`f709bef`](https://github.com/tobymao/sqlglot/commit/f709bef3af7cd0daa25fe3d58b1753c3e65720ef) - **optimizer**: annotate type for bq FIRST_VALUE *(PR [#5718](https://github.com/tobymao/sqlglot/pull/5718) by [@geooo109](https://github.com/geooo109))*
- [`b9ae9e5`](https://github.com/tobymao/sqlglot/commit/b9ae9e534dee1e32fccbf22cab9bc17fbd920629) - **singlestore**: Implemeted generation of exp.TsOrDiToDi *(PR [#5724](https://github.com/tobymao/sqlglot/pull/5724) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`9b14fff`](https://github.com/tobymao/sqlglot/commit/9b14fffd2c9404f76a3faced2ec9d6eaac8feb01) - **singlestore**: Implemented generation of exp.DateToDi *(PR [#5717](https://github.com/tobymao/sqlglot/pull/5717) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`07d8c23`](https://github.com/tobymao/sqlglot/commit/07d8c2347baba6523310c4d31cddfb0e5c0eddc1) - **singlestore**: Implemented generation of exp.DiToDate *(PR [#5721](https://github.com/tobymao/sqlglot/pull/5721) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`ad34a85`](https://github.com/tobymao/sqlglot/commit/ad34a855a433bc0f51a707cbcb66f8dce667a562) - **singlestore**: Implemented generation of exp.FromTimeZone *(PR [#5723](https://github.com/tobymao/sqlglot/pull/5723) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`29d5e4f`](https://github.com/tobymao/sqlglot/commit/29d5e4f62a799f35c0904a23cedacc6efa95a63b) - **singlestore**: Implemented generation of exp.DatetimeAdd *(PR [#5728](https://github.com/tobymao/sqlglot/pull/5728) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`15a9061`](https://github.com/tobymao/sqlglot/commit/15a906170e5d5cdaa207ec7607edfdd7d4a8b774) - **optimizer**: annotate type for bq PERCENTILE_DISC *(PR [#5722](https://github.com/tobymao/sqlglot/pull/5722) by [@geooo109](https://github.com/geooo109))*
- [`7d49609`](https://github.com/tobymao/sqlglot/commit/7d4960963f0ef70b96f5b969bb008d2742e833ea) - **optimizer**: annotate type for bq NTH_VALUE *(PR [#5720](https://github.com/tobymao/sqlglot/pull/5720) by [@geooo109](https://github.com/geooo109))*
- [`d41acf1`](https://github.com/tobymao/sqlglot/commit/d41acf11221bee30a5ae089cbac9b158ed3dd515) - **optimizer**: annotate type for bq LEAD *(PR [#5719](https://github.com/tobymao/sqlglot/pull/5719) by [@geooo109](https://github.com/geooo109))*
- [`113809a`](https://github.com/tobymao/sqlglot/commit/113809a07efee0f12758bd2571c8515885568466) - **singlestore**: Implemented exp.TimeStrToDate generation *(PR [#5725](https://github.com/tobymao/sqlglot/pull/5725) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`cf63d0d`](https://github.com/tobymao/sqlglot/commit/cf63d0df4c2f58b2cf0c87e2a3a6f63f836a50a1) - **dremio**: add regexp_like and alias regexp_matches *(PR [#5731](https://github.com/tobymao/sqlglot/pull/5731) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`e42160f`](https://github.com/tobymao/sqlglot/commit/e42160f27fa68828898969073f2f4a0014f5e3e9) - **dremio**: support alias repeatstr *(PR [#5730](https://github.com/tobymao/sqlglot/pull/5730) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`ff12130`](https://github.com/tobymao/sqlglot/commit/ff12130c23a215917f20fda7d50322f1cb7de599) - **optimizer**: annotate type for bq PERNCENTILE_CONT *(PR [#5729](https://github.com/tobymao/sqlglot/pull/5729) by [@geooo109](https://github.com/geooo109))*
- [`fdb8a0a`](https://github.com/tobymao/sqlglot/commit/fdb8a0a6d0d74194255f313bd934db7fc1ce0d3f) - **optimizer**: parse and annotate type for bq FORMAT *(PR [#5715](https://github.com/tobymao/sqlglot/pull/5715) by [@geooo109](https://github.com/geooo109))*
- [`e272292`](https://github.com/tobymao/sqlglot/commit/e272292197f2bb81ccfad1de06a95f321f0b565f) - **singlestore**: Implemented generation of exp.Time *(PR [#5727](https://github.com/tobymao/sqlglot/pull/5727) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`012bdd3`](https://github.com/tobymao/sqlglot/commit/012bdd3c8aeff180f85354ffd403fc1aa5815dcf) - **optimizer**: parse and annotate type for bq CUME_DIST *(PR [#5735](https://github.com/tobymao/sqlglot/pull/5735) by [@geooo109](https://github.com/geooo109))*
- [`b99eaeb`](https://github.com/tobymao/sqlglot/commit/b99eaeb0c6eb3dc613e76d205e02632bd6af353b) - **optimizer**: parse and annotate type for bq DENSE_RANK *(PR [#5736](https://github.com/tobymao/sqlglot/pull/5736) by [@geooo109](https://github.com/geooo109))*
- [`8cf6ef9`](https://github.com/tobymao/sqlglot/commit/8cf6ef92a0f43943efb0fe380f41dc09f43aca85) - **optimizer**: parse and annotate_type for bq NTILE *(PR [#5737](https://github.com/tobymao/sqlglot/pull/5737) by [@geooo109](https://github.com/geooo109))*
- [`bb95c73`](https://github.com/tobymao/sqlglot/commit/bb95c7312c942ef987955f01e060604d60e32e83) - **optimizer**: parse and annotate type for bq RANK *(PR [#5738](https://github.com/tobymao/sqlglot/pull/5738) by [@geooo109](https://github.com/geooo109))*
- [`8713c08`](https://github.com/tobymao/sqlglot/commit/8713c082b0aa8454a5773fc2a85e08a132dc6ce3) - **optimizer**: parse and annotate type for bq PERCENT_RANK *(PR [#5739](https://github.com/tobymao/sqlglot/pull/5739) by [@geooo109](https://github.com/geooo109))*
- [`9ce4e31`](https://github.com/tobymao/sqlglot/commit/9ce4e31aecbde6ea1f227a7166c0f3dc9e302a66) - **optimizer**: annotate type for bq JSON_OBJECT *(PR [#5740](https://github.com/tobymao/sqlglot/pull/5740) by [@geooo109](https://github.com/geooo109))*
- [`d35ec6e`](https://github.com/tobymao/sqlglot/commit/d35ec6e37e21cf3cec848ed55bd73128c4633cd2) - **optimizer**: annotate type for bq JSON_QUERY/JSON_QUERY_ARRAY *(PR [#5741](https://github.com/tobymao/sqlglot/pull/5741) by [@geooo109](https://github.com/geooo109))*
- [`4753642`](https://github.com/tobymao/sqlglot/commit/4753642cfcfb1f192ec4d21a492737b27affef09) - **optimizer**: annotate type for bq JSON_EXTRACT_SCALAR *(commit by [@geooo109](https://github.com/geooo109))*
- [`6249dbe`](https://github.com/tobymao/sqlglot/commit/6249dbe4173ad5278adf84452dcf7253a2395b91) - **singlestore**: Added generation of exp.DatetimeDiff *(PR [#5743](https://github.com/tobymao/sqlglot/pull/5743) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`113a530`](https://github.com/tobymao/sqlglot/commit/113a5308d050fd5ceacab4c6188e5eea5dd740b1) - **optimizer**: parse and annotate type for bq JSON_ARRAY_APPEND *(PR [#5747](https://github.com/tobymao/sqlglot/pull/5747) by [@geooo109](https://github.com/geooo109))*
- [`8603705`](https://github.com/tobymao/sqlglot/commit/8603705a8e5513699adc2499389c67412eee70cb) - **singlestore**: feat(singlestore): Implemented generation of exp.DatetimeSub *(PR [#5744](https://github.com/tobymao/sqlglot/pull/5744) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`7d71c0b`](https://github.com/tobymao/sqlglot/commit/7d71c0bb576f9de3447b4780ab64a3f4d92c6432) - **singlestore**: Fixed generation of exp.DatetimeTrunc and exp.DateTrunc *(PR [#5745](https://github.com/tobymao/sqlglot/pull/5745) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`268e2c6`](https://github.com/tobymao/sqlglot/commit/268e2c694d1eb99f1fe64477bc38ed4946bf1c32) - **optimizer**: parse and annotate type for bq JSON_ARRAY_INSERT *(PR [#5748](https://github.com/tobymao/sqlglot/pull/5748) by [@geooo109](https://github.com/geooo109))*
- [`455ec1f`](https://github.com/tobymao/sqlglot/commit/455ec1f4f8aecb5435fa4cb2912bfc21db8dd44d) - **optimizer**: parse and annotate type for bq JSON_KEYS *(PR [#5749](https://github.com/tobymao/sqlglot/pull/5749) by [@geooo109](https://github.com/geooo109))*
- [`59895fa`](https://github.com/tobymao/sqlglot/commit/59895faa23ebe1b27938c37a7b39df87de609844) - **optimizer**: parse and annotate type for bq JSON_REMOVE *(PR [#5750](https://github.com/tobymao/sqlglot/pull/5750) by [@geooo109](https://github.com/geooo109))*
- [`06d7df7`](https://github.com/tobymao/sqlglot/commit/06d7df7a05f2824cabf48e8d1e8a4ebca8fda496) - **optimizer**: parse and annotate type for bq JSON_SET *(PR [#5751](https://github.com/tobymao/sqlglot/pull/5751) by [@geooo109](https://github.com/geooo109))*
- [`7f5079a`](https://github.com/tobymao/sqlglot/commit/7f5079a1b71c4dd28e98b77b5b749e074fce862c) - **singlestore**: Improved geneation of exp.DataType *(PR [#5746](https://github.com/tobymao/sqlglot/pull/5746) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`ad9405c`](https://github.com/tobymao/sqlglot/commit/ad9405cd43108ff80d16711f8b33ff57430ed686) - **singlestore**: fixed generation of exp.TimestampTrunc *(PR [#5754](https://github.com/tobymao/sqlglot/pull/5754) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`a1852f9`](https://github.com/tobymao/sqlglot/commit/a1852f93fdfe926072c12954c95796d038e15140) - **dremio**: parse date_part *(PR [#5756](https://github.com/tobymao/sqlglot/pull/5756) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`0db1df6`](https://github.com/tobymao/sqlglot/commit/0db1df617ec4f05b1ee6cf1d606272f6e799a9b9) - **singlestore**: Fixed generation of exp.DateDiff *(PR [#5752](https://github.com/tobymao/sqlglot/pull/5752) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`e72b341`](https://github.com/tobymao/sqlglot/commit/e72b3419c8a367caa0e5e80030979cd94e87a40d) - **optimizer**: parse and annotate type for bq JSON_STRIP_NULLS *(PR [#5753](https://github.com/tobymao/sqlglot/pull/5753) by [@geooo109](https://github.com/geooo109))*
- [`5de61a7`](https://github.com/tobymao/sqlglot/commit/5de61a7ab850d4e68fde4d76ee396d30d7bdef33) - **optimizer**: parse and annotate type for bq JSON_EXTRACT_STRING_ARRAY *(PR [#5758](https://github.com/tobymao/sqlglot/pull/5758) by [@geooo109](https://github.com/geooo109))*
- [`36c9393`](https://github.com/tobymao/sqlglot/commit/36c93939575a19bd611269719c39d3d216be8cde) - **optimizer**: parse and annotate type for bq JSON LAX funcs *(PR [#5760](https://github.com/tobymao/sqlglot/pull/5760) by [@geooo109](https://github.com/geooo109))*
- [`c443d5c`](https://github.com/tobymao/sqlglot/commit/c443d5caf2d9695856103eebfff21cb215777112) - **dremio**: parse datetype *(PR [#5759](https://github.com/tobymao/sqlglot/pull/5759) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`5172a99`](https://github.com/tobymao/sqlglot/commit/5172a99fc4d5e21a1dbe4509d6d7ab1ccfe8bff7) - **singlestore**: Fixed parsing of columns with table name *(PR [#5767](https://github.com/tobymao/sqlglot/pull/5767) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`88862b5`](https://github.com/tobymao/sqlglot/commit/88862b56bc29c8a600b4d0e4693d5846d3a577ff) - **optimizer**: annotate type for bq TO_JSON_STRING *(PR [#5762](https://github.com/tobymao/sqlglot/pull/5762) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`ec93497`](https://github.com/tobymao/sqlglot/commit/ec93497bac82090b88c6e749ec2adc99bbc23a61) - **bigquery**: support commands inside for loops *(PR [#5732](https://github.com/tobymao/sqlglot/pull/5732) by [@treysp](https://github.com/treysp))*
- [`85845bb`](https://github.com/tobymao/sqlglot/commit/85845bb941ac9a4ee090a89cd3d3dab4ab5835a7) - **snowflake**: allow exclude as id var *(PR [#5764](https://github.com/tobymao/sqlglot/pull/5764) by [@treysp](https://github.com/treysp))*
- [`db2d9cc`](https://github.com/tobymao/sqlglot/commit/db2d9cca9718fb196066dbf60840124917d1f8ac) - **tokenizer**: handle empty hex strings *(PR [#5763](https://github.com/tobymao/sqlglot/pull/5763) by [@paulolieuthier](https://github.com/paulolieuthier))*
  - :arrow_lower_right: *fixes issue [#5761](https://github.com/tobymao/sqlglot/issues/5761) opened by [@paulolieuthier](https://github.com/paulolieuthier)*
- [`982257b`](https://github.com/tobymao/sqlglot/commit/982257b40973cdfc20a8d6dd9a1674cda7eb75c4) - **bigquery**: Crash when ARRAY_CONCAT is called with no expressions *(PR [#5755](https://github.com/tobymao/sqlglot/pull/5755) by [@ozadari](https://github.com/ozadari))*
- [`24ca504`](https://github.com/tobymao/sqlglot/commit/24ca504360779c8a20a58accf506eb9600ac9bf8) - **bigquery**: Crash when ARRAY_CONCAT is called with no expressions *(PR [#5755](https://github.com/tobymao/sqlglot/pull/5755) by [@ozadari](https://github.com/ozadari))*

### :wrench: Chores
- [`41521e3`](https://github.com/tobymao/sqlglot/commit/41521e31b465acd51ab02b1ac4e5512b98175b7e) - bump sqlglotrs to 0.6.2 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.10.0] - 2025-08-28
### :boom: BREAKING CHANGES
- due to [`de2fe15`](https://github.com/tobymao/sqlglot/commit/de2fe1503b5bb003431d1f0c7b9ae87932a6cc1c) - annotate type for bq CONTAINS_SUBSTR *(PR [#5705](https://github.com/tobymao/sqlglot/pull/5705) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq CONTAINS_SUBSTR (#5705)

- due to [`770888f`](https://github.com/tobymao/sqlglot/commit/770888f4e9a9061329e3c416f968f7dd9639fb81) - annotate type for bq NORMALIZE *(PR [#5711](https://github.com/tobymao/sqlglot/pull/5711) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bq NORMALIZE (#5711)

- due to [`506033f`](https://github.com/tobymao/sqlglot/commit/506033f299f7a4c28f6efd8bf715be5dcf73e929) - parse and annotate type for bq NORMALIZE_AND_CASEFOLD *(PR [#5712](https://github.com/tobymao/sqlglot/pull/5712) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq NORMALIZE_AND_CASEFOLD (#5712)

- due to [`848aea1`](https://github.com/tobymao/sqlglot/commit/848aea1dbaaeb580b633796dcca06c28314b9c3e) - parse and annotate type for bq OCTET_LENGTH *(PR [#5713](https://github.com/tobymao/sqlglot/pull/5713) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq OCTET_LENGTH (#5713)

- due to [`727bf83`](https://github.com/tobymao/sqlglot/commit/727bf8378f232188d35834d980b035552999ea3b) - add support for REVOKE DDL *(PR [#5703](https://github.com/tobymao/sqlglot/pull/5703) by [@newtonapple](https://github.com/newtonapple))*:

  add support for REVOKE DDL (#5703)


### :sparkles: New Features
- [`f6f8f56`](https://github.com/tobymao/sqlglot/commit/f6f8f56a59d550dfc7dfcab0c3b9a6885c7e758a) - **singlestore**: Fixed parsing/generation of exp.JSONFormat *(PR [#5706](https://github.com/tobymao/sqlglot/pull/5706) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`de2fe15`](https://github.com/tobymao/sqlglot/commit/de2fe1503b5bb003431d1f0c7b9ae87932a6cc1c) - **optimizer**: annotate type for bq CONTAINS_SUBSTR *(PR [#5705](https://github.com/tobymao/sqlglot/pull/5705) by [@geooo109](https://github.com/geooo109))*
- [`a78146e`](https://github.com/tobymao/sqlglot/commit/a78146e37bfc972050b4467c39769407061e9bc3) - **singlestore**: Fixed parsing/generation of exp.DateBin *(PR [#5709](https://github.com/tobymao/sqlglot/pull/5709) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`ab0c985`](https://github.com/tobymao/sqlglot/commit/ab0c985424ae9d9340eafd15ecdc9b31bdd8837c) - **singlestore**: Marked exp.Reduce finish argument as unsupported *(PR [#5707](https://github.com/tobymao/sqlglot/pull/5707) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`770888f`](https://github.com/tobymao/sqlglot/commit/770888f4e9a9061329e3c416f968f7dd9639fb81) - **optimizer**: annotate type for bq NORMALIZE *(PR [#5711](https://github.com/tobymao/sqlglot/pull/5711) by [@geooo109](https://github.com/geooo109))*
- [`506033f`](https://github.com/tobymao/sqlglot/commit/506033f299f7a4c28f6efd8bf715be5dcf73e929) - **optimizer**: parse and annotate type for bq NORMALIZE_AND_CASEFOLD *(PR [#5712](https://github.com/tobymao/sqlglot/pull/5712) by [@geooo109](https://github.com/geooo109))*
- [`848aea1`](https://github.com/tobymao/sqlglot/commit/848aea1dbaaeb580b633796dcca06c28314b9c3e) - **optimizer**: parse and annotate type for bq OCTET_LENGTH *(PR [#5713](https://github.com/tobymao/sqlglot/pull/5713) by [@geooo109](https://github.com/geooo109))*
- [`727bf83`](https://github.com/tobymao/sqlglot/commit/727bf8378f232188d35834d980b035552999ea3b) - add support for REVOKE DDL *(PR [#5703](https://github.com/tobymao/sqlglot/pull/5703) by [@newtonapple](https://github.com/newtonapple))*

### :bug: Bug Fixes
- [`0427c7b`](https://github.com/tobymao/sqlglot/commit/0427c7b7aa9f8161324085a98c5f531fa35c8b0c) - **optimizer**: qualify columns for AggFunc with DISTINCT *(PR [#5708](https://github.com/tobymao/sqlglot/pull/5708) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5698](https://github.com/tobymao/sqlglot/issues/5698) opened by [@georgesittas](https://github.com/georgesittas)*


## [v27.9.0] - 2025-08-27
### :boom: BREAKING CHANGES
- due to [`7b180bd`](https://github.com/tobymao/sqlglot/commit/7b180bdc3da9e39946c22970bd2523f7d8beaf29) - raise if query modifier is specified multiple times *(PR [#5608](https://github.com/tobymao/sqlglot/pull/5608) by [@georgesittas](https://github.com/georgesittas))*:

  raise if query modifier is specified multiple times (#5608)

- due to [`36602a2`](https://github.com/tobymao/sqlglot/commit/36602a2ecc9ffca98e89044d23e40f33c6ed71e4) - parse LIST_FILTER into ArrayFilter closes [#5633](https://github.com/tobymao/sqlglot/pull/5633) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  parse LIST_FILTER into ArrayFilter closes #5633

- due to [`0188d21`](https://github.com/tobymao/sqlglot/commit/0188d21d443c991a528eb9d220459890b7dca477) - parse LIST_TRANSFORM into Transform closes [#5634](https://github.com/tobymao/sqlglot/pull/5634) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  parse LIST_TRANSFORM into Transform closes #5634

- due to [`3ab1d44`](https://github.com/tobymao/sqlglot/commit/3ab1d4487279cab3be2d3764e51516c6db21629d) - Wrap CONCAT items with COALESCE less aggressively *(PR [#5641](https://github.com/tobymao/sqlglot/pull/5641) by [@VaggelisD](https://github.com/VaggelisD))*:

  Wrap CONCAT items with COALESCE less aggressively (#5641)

- due to [`af0b299`](https://github.com/tobymao/sqlglot/commit/af0b299561914953b30ab36004e53dcb92d39e1c) - Qualify columns generated by exp.Aliases *(PR [#5647](https://github.com/tobymao/sqlglot/pull/5647) by [@VaggelisD](https://github.com/VaggelisD))*:

  Qualify columns generated by exp.Aliases (#5647)

- due to [`53aa8fe`](https://github.com/tobymao/sqlglot/commit/53aa8fe7f188012f765066f32c4179035fff036d) - support alter table with check closes [#5649](https://github.com/tobymao/sqlglot/pull/5649) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  support alter table with check closes #5649

- due to [`1a60a5a`](https://github.com/tobymao/sqlglot/commit/1a60a5a845c7431d7d3d7ccb71119699316f4b41) - Added parsing/generation of JSON_ARRAY_CONTAINS function *(PR [#5661](https://github.com/tobymao/sqlglot/pull/5661) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*:

  Added parsing/generation of JSON_ARRAY_CONTAINS function (#5661)

- due to [`e0db0a9`](https://github.com/tobymao/sqlglot/commit/e0db0a95d3cb7614242dbd1b439d408e7e7bd475) - add parse and annotate type for bigquery FARM_FINGERPRINT *(PR [#5667](https://github.com/tobymao/sqlglot/pull/5667) by [@geooo109](https://github.com/geooo109))*:

  add parse and annotate type for bigquery FARM_FINGERPRINT (#5667)

- due to [`56588c7`](https://github.com/tobymao/sqlglot/commit/56588c7e22b4db4f0e44696a460483ca1e549163) - Add support for vector_search function. Move predict to BigQuery dialect. *(PR [#5660](https://github.com/tobymao/sqlglot/pull/5660) by [@rloredo](https://github.com/rloredo))*:

  Add support for vector_search function. Move predict to BigQuery dialect. (#5660)

- due to [`a688a0f`](https://github.com/tobymao/sqlglot/commit/a688a0f0d70f87139e531d1419b338b695bec384) - parse and annotate type for bigquery APPROX_TOP_COUNT *(PR [#5670](https://github.com/tobymao/sqlglot/pull/5670) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery APPROX_TOP_COUNT (#5670)

- due to [`3c93fcc`](https://github.com/tobymao/sqlglot/commit/3c93fcce96ec82e78753f6c9dd5fb0e730a82058) - parse and annotate type for bigquery APPROX_TOP_SUM *(PR [#5675](https://github.com/tobymao/sqlglot/pull/5675) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery APPROX_TOP_SUM (#5675)

- due to [`741d45a`](https://github.com/tobymao/sqlglot/commit/741d45a0ca7c1bad67da4393cd10cc9cfa49ea68) - parse and annotate type for bigquery FROM/TO_BASE32 *(PR [#5676](https://github.com/tobymao/sqlglot/pull/5676) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery FROM/TO_BASE32 (#5676)

- due to [`9ae045c`](https://github.com/tobymao/sqlglot/commit/9ae045c0405e43b148e3b9261825288ebf09100c) - parse and annotate type for bigquery FROM_HEX *(PR [#5679](https://github.com/tobymao/sqlglot/pull/5679) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery FROM_HEX (#5679)

- due to [`5a22a25`](https://github.com/tobymao/sqlglot/commit/5a22a254143978989027f6e7f6163019a34f112a) - annotate type for bigquery TO_HEX *(PR [#5680](https://github.com/tobymao/sqlglot/pull/5680) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery TO_HEX (#5680)

- due to [`5c1eb2d`](https://github.com/tobymao/sqlglot/commit/5c1eb2df5dd3dcc6ed2c8204cec56b5c3d276f87) - parse and annotate type for bq PARSE_BIG/NUMERIC *(PR [#5690](https://github.com/tobymao/sqlglot/pull/5690) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq PARSE_BIG/NUMERIC (#5690)

- due to [`311373d`](https://github.com/tobymao/sqlglot/commit/311373d22134de906d1c1cef019541e85e2f7c9f) - parse and annotate type for bq CODE_POINTS_TO_BYTES *(PR [#5686](https://github.com/tobymao/sqlglot/pull/5686) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq CODE_POINTS_TO_BYTES (#5686)

- due to [`79d9de1`](https://github.com/tobymao/sqlglot/commit/79d9de1745598f8f3ae2c82c1389dd455c946a09) - parse and annotate type for bq TO_CODE_POINTS *(PR [#5685](https://github.com/tobymao/sqlglot/pull/5685) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq TO_CODE_POINTS (#5685)

- due to [`5df3ea9`](https://github.com/tobymao/sqlglot/commit/5df3ea92f59125955124ea1883b777b489db3042) - parse and annotate type for bq SAFE_CONVERT_BYTES_TO_STRING *(PR [#5681](https://github.com/tobymao/sqlglot/pull/5681) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq SAFE_CONVERT_BYTES_TO_STRING (#5681)

- due to [`c832746`](https://github.com/tobymao/sqlglot/commit/c832746018fbc2c531d5b2a7c7f8cd5d78e511ff) - parse and annotate type for bigquery APPROX_QUANTILES *(PR [#5678](https://github.com/tobymao/sqlglot/pull/5678) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery APPROX_QUANTILES (#5678)

- due to [`99e169e`](https://github.com/tobymao/sqlglot/commit/99e169ea13d5be3712a47f6b55b98a4764a3c24d) - parse and annotate type for bq BOOL *(PR [#5697](https://github.com/tobymao/sqlglot/pull/5697) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq BOOL (#5697)

- due to [`3f31770`](https://github.com/tobymao/sqlglot/commit/3f31770c793f464fcac1ce2b8dfa03d4b7f0231c) - parse and annotate type for bq FLOAT64 *(PR [#5700](https://github.com/tobymao/sqlglot/pull/5700) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bq FLOAT64 (#5700)


### :sparkles: New Features
- [`02e60e7`](https://github.com/tobymao/sqlglot/commit/02e60e73fc0c2dae815aa225be247a17ccdf4b82) - **singlestore**: desugarize DAYNAME into DATE_FORMAT *(PR [#5610](https://github.com/tobymao/sqlglot/pull/5610) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`7b180bd`](https://github.com/tobymao/sqlglot/commit/7b180bdc3da9e39946c22970bd2523f7d8beaf29) - **parser**: raise if query modifier is specified multiple times *(PR [#5608](https://github.com/tobymao/sqlglot/pull/5608) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5604](https://github.com/tobymao/sqlglot/issues/5604) opened by [@bricct](https://github.com/bricct)*
- [`442eafc`](https://github.com/tobymao/sqlglot/commit/442eafcb00a2650930bd6023aa9a5febfebbe796) - **singlestore**: Added parsing of HOUR function *(PR [#5612](https://github.com/tobymao/sqlglot/pull/5612) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`5320359`](https://github.com/tobymao/sqlglot/commit/532035978605efd1d43de75aafca750e2894c0b9) - **singlestore**: Added parsing of MICROSECOND function *(PR [#5619](https://github.com/tobymao/sqlglot/pull/5619) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`db1db97`](https://github.com/tobymao/sqlglot/commit/db1db9732352187629df853ad937ebaf4abfe487) - **doris**: update exp.UniqueKeyProperty SQL generation logic *(PR [#5613](https://github.com/tobymao/sqlglot/pull/5613) by [@xinge-ji](https://github.com/xinge-ji))*
- [`54623a6`](https://github.com/tobymao/sqlglot/commit/54623a6b85432272703f12a197b05ced78529f90) - **singlestore**: Added parsing of MINUTE function *(PR [#5620](https://github.com/tobymao/sqlglot/pull/5620) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`565c9f8`](https://github.com/tobymao/sqlglot/commit/565c9f8c55cfbef5d3a9e1470551f1dc4416825e) - **singlestore**: Added generation of DAYOFWEEK_ISO function *(PR [#5627](https://github.com/tobymao/sqlglot/pull/5627) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`8db916e`](https://github.com/tobymao/sqlglot/commit/8db916e2f2ce241bdff130d626f98df182b48f3e) - **singlestore**: Added parsing of WEEKDAY function *(PR [#5624](https://github.com/tobymao/sqlglot/pull/5624) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`aa6274a`](https://github.com/tobymao/sqlglot/commit/aa6274a0ea647df1251563945635260a6ddd4972) - **singlestore**: Fixed generation of DAY_OF_MONTH function *(PR [#5629](https://github.com/tobymao/sqlglot/pull/5629) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`dee44b8`](https://github.com/tobymao/sqlglot/commit/dee44b8c1d70ca6079867896fb68cad256909dad) - **singlestore**: Added parsing of MONTHNAME function *(PR [#5623](https://github.com/tobymao/sqlglot/pull/5623) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`deebf0c`](https://github.com/tobymao/sqlglot/commit/deebf0c3cc379e28c4ab66b6bb7a9c84c14e88c6) - **singlestore**: Added parsing of SECOND function *(PR [#5621](https://github.com/tobymao/sqlglot/pull/5621) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`12a60b9`](https://github.com/tobymao/sqlglot/commit/12a60b99b6b2b0673b57218c691794deb67aa3a5) - **singlestore**: Removed redundant deletions from TRANSFORMS *(PR [#5632](https://github.com/tobymao/sqlglot/pull/5632) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`36602a2`](https://github.com/tobymao/sqlglot/commit/36602a2ecc9ffca98e89044d23e40f33c6ed71e4) - **duckdb**: parse LIST_FILTER into ArrayFilter closes [#5633](https://github.com/tobymao/sqlglot/pull/5633) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`0188d21`](https://github.com/tobymao/sqlglot/commit/0188d21d443c991a528eb9d220459890b7dca477) - **duckdb**: parse LIST_TRANSFORM into Transform closes [#5634](https://github.com/tobymao/sqlglot/pull/5634) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b117d59`](https://github.com/tobymao/sqlglot/commit/b117d59f3c43f6f44cd0ccdf22717f7bcd990889) - **dremio**: add dremio date_add and date_sub parsing *(PR [#5617](https://github.com/tobymao/sqlglot/pull/5617) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`999b9e7`](https://github.com/tobymao/sqlglot/commit/999b9e793c0819a4d2af6400fc924946d26b3e6f) - **singlestore**: Changed generation of exp.TsOrDsToDate to handle case when format is not provided *(PR [#5639](https://github.com/tobymao/sqlglot/pull/5639) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`b556e97`](https://github.com/tobymao/sqlglot/commit/b556e97f8cfbde21c0a921ac1c01c9e4f2ec2535) - **singlestore**: Marked exp.All as unsupported *(PR [#5640](https://github.com/tobymao/sqlglot/pull/5640) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`c076694`](https://github.com/tobymao/sqlglot/commit/c0766946e6799fb61c38e855fd18812d08a5c251) - **clickhouse**: support custom partition key expressions *(PR [#5645](https://github.com/tobymao/sqlglot/pull/5645) by [@GaliFFun](https://github.com/GaliFFun))*
- [`cab62b0`](https://github.com/tobymao/sqlglot/commit/cab62b06ce926e3116a6a45a9c57e4901cd8a281) - **doris**: add support for BUILD and REFRESH properties in materialized view *(PR [#5614](https://github.com/tobymao/sqlglot/pull/5614) by [@xinge-ji](https://github.com/xinge-ji))*
- [`af0b299`](https://github.com/tobymao/sqlglot/commit/af0b299561914953b30ab36004e53dcb92d39e1c) - **optimizer**: Qualify columns generated by exp.Aliases *(PR [#5647](https://github.com/tobymao/sqlglot/pull/5647) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5638](https://github.com/tobymao/sqlglot/issues/5638) opened by [@catlynkong](https://github.com/catlynkong)*
- [`981e0e7`](https://github.com/tobymao/sqlglot/commit/981e0e70a304665e746158c859bcc81f99384685) - **doris**: add support for PARTITION BY LIST *(PR [#5615](https://github.com/tobymao/sqlglot/pull/5615) by [@xinge-ji](https://github.com/xinge-ji))*
- [`53aa8fe`](https://github.com/tobymao/sqlglot/commit/53aa8fe7f188012f765066f32c4179035fff036d) - **tsql**: support alter table with check closes [#5649](https://github.com/tobymao/sqlglot/pull/5649) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`23cac6c`](https://github.com/tobymao/sqlglot/commit/23cac6c58099a9ac818ac5d3970a427ca3579cca) - **exasol**: Add support for GROUP_CONCAT and LISTAGG functions *(PR [#5646](https://github.com/tobymao/sqlglot/pull/5646) by [@nnamdi16](https://github.com/nnamdi16))*
- [`d087ac8`](https://github.com/tobymao/sqlglot/commit/d087ac89376df5ab16de99c8b67f99060f0a6170) - **bigquery**: Add support for ml.generate_embedding function *(PR [#5652](https://github.com/tobymao/sqlglot/pull/5652) by [@rloredo](https://github.com/rloredo))*
- [`e71bcb5`](https://github.com/tobymao/sqlglot/commit/e71bcb51181de63c8ad13004216506529fcf9644) - **dremio**: support array_generate_range *(PR [#5653](https://github.com/tobymao/sqlglot/pull/5653) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`edbd04b`](https://github.com/tobymao/sqlglot/commit/edbd04b6a91b1a6f76e4fa938098ba5ed581ba72) - **singlestore**: Fixed generation of exp.RegexpLike *(PR [#5663](https://github.com/tobymao/sqlglot/pull/5663) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`4992edb`](https://github.com/tobymao/sqlglot/commit/4992edbb79f4922917cc5ce5aa687e6f7da7798c) - **singlestore**: Fixed exp.Xor generation *(PR [#5662](https://github.com/tobymao/sqlglot/pull/5662) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`20de3d3`](https://github.com/tobymao/sqlglot/commit/20de3d37cdae0705c67f80fbacbe024a62f34657) - **singlestore**: Fixed parsing/generation of exp.Hll *(PR [#5664](https://github.com/tobymao/sqlglot/pull/5664) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`1a60a5a`](https://github.com/tobymao/sqlglot/commit/1a60a5a845c7431d7d3d7ccb71119699316f4b41) - **singlestore**: Added parsing/generation of JSON_ARRAY_CONTAINS function *(PR [#5661](https://github.com/tobymao/sqlglot/pull/5661) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`f662dc0`](https://github.com/tobymao/sqlglot/commit/f662dc0b47fd14d00899c14a899756a5ba1fe9da) - **singlestore**: Fixed generation of exp.ApproxDistinct *(PR [#5666](https://github.com/tobymao/sqlglot/pull/5666) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`e0db0a9`](https://github.com/tobymao/sqlglot/commit/e0db0a95d3cb7614242dbd1b439d408e7e7bd475) - **optimizer**: add parse and annotate type for bigquery FARM_FINGERPRINT *(PR [#5667](https://github.com/tobymao/sqlglot/pull/5667) by [@geooo109](https://github.com/geooo109))*
- [`dcd4ef7`](https://github.com/tobymao/sqlglot/commit/dcd4ef769727ed1227911f2d9a85244d61173003) - **singlestore**: Fixed exp.CountIf generation *(PR [#5668](https://github.com/tobymao/sqlglot/pull/5668) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`e431e85`](https://github.com/tobymao/sqlglot/commit/e431e851c2c5d20f049adbc38e370a64d39c346f) - **singlestore**: Fixed generation of exp.LogicalOr *(PR [#5669](https://github.com/tobymao/sqlglot/pull/5669) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`56588c7`](https://github.com/tobymao/sqlglot/commit/56588c7e22b4db4f0e44696a460483ca1e549163) - **bigquery**: Add support for vector_search function. Move predict to BigQuery dialect. *(PR [#5660](https://github.com/tobymao/sqlglot/pull/5660) by [@rloredo](https://github.com/rloredo))*
- [`f0d2cc2`](https://github.com/tobymao/sqlglot/commit/f0d2cc2b0f72340172ecd154f632aa6a24c15512) - **singlestore**: Fixed generation of exp.LogicalAnd *(PR [#5671](https://github.com/tobymao/sqlglot/pull/5671) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`a688a0f`](https://github.com/tobymao/sqlglot/commit/a688a0f0d70f87139e531d1419b338b695bec384) - **optimizer**: parse and annotate type for bigquery APPROX_TOP_COUNT *(PR [#5670](https://github.com/tobymao/sqlglot/pull/5670) by [@geooo109](https://github.com/geooo109))*
- [`fa8d571`](https://github.com/tobymao/sqlglot/commit/fa8d57132b1d21d92eb5de3ba88b41f880e14889) - **singlestore**: Fixed generation/parsing of exp.ApproxQuantile *(PR [#5672](https://github.com/tobymao/sqlglot/pull/5672) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`9955ebe`](https://github.com/tobymao/sqlglot/commit/9955ebe90d3421815738ecb643806add755c5df3) - **singlestore**: Fixed parsing/generation of exp.Variance *(PR [#5673](https://github.com/tobymao/sqlglot/pull/5673) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`3c93fcc`](https://github.com/tobymao/sqlglot/commit/3c93fcce96ec82e78753f6c9dd5fb0e730a82058) - **optimizer**: parse and annotate type for bigquery APPROX_TOP_SUM *(PR [#5675](https://github.com/tobymao/sqlglot/pull/5675) by [@geooo109](https://github.com/geooo109))*
- [`60cbb9d`](https://github.com/tobymao/sqlglot/commit/60cbb9d0e3c9b5a36c1368c9b5bb05def8ce8658) - **dremio**: add CURRENT_DATE_UTC *(PR [#5674](https://github.com/tobymao/sqlglot/pull/5674) by [@jasonthomassql](https://github.com/jasonthomassql))*
  - :arrow_lower_right: *addresses issue [#5655](https://github.com/tobymao/sqlglot/issues/5655) opened by [@jasonthomassql](https://github.com/jasonthomassql)*
- [`741d45a`](https://github.com/tobymao/sqlglot/commit/741d45a0ca7c1bad67da4393cd10cc9cfa49ea68) - **optimizer**: parse and annotate type for bigquery FROM/TO_BASE32 *(PR [#5676](https://github.com/tobymao/sqlglot/pull/5676) by [@geooo109](https://github.com/geooo109))*
- [`9ae045c`](https://github.com/tobymao/sqlglot/commit/9ae045c0405e43b148e3b9261825288ebf09100c) - **optimizer**: parse and annotate type for bigquery FROM_HEX *(PR [#5679](https://github.com/tobymao/sqlglot/pull/5679) by [@geooo109](https://github.com/geooo109))*
- [`5a22a25`](https://github.com/tobymao/sqlglot/commit/5a22a254143978989027f6e7f6163019a34f112a) - **optimizer**: annotate type for bigquery TO_HEX *(PR [#5680](https://github.com/tobymao/sqlglot/pull/5680) by [@geooo109](https://github.com/geooo109))*
- [`d920ac3`](https://github.com/tobymao/sqlglot/commit/d920ac3886ce006d76616bc31884ee2f5c4162bc) - **singlestore**: Fixed parsing/generation of exp.RegexpExtractAll *(PR [#5692](https://github.com/tobymao/sqlglot/pull/5692) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`260c72b`](https://github.com/tobymao/sqlglot/commit/260c72befc0510ebe1d007284c0eef9343de20d7) - **singlestore**: Fixed parsing/generation of exp.Contains *(PR [#5684](https://github.com/tobymao/sqlglot/pull/5684) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`081dc67`](https://github.com/tobymao/sqlglot/commit/081dc673b89d3d8d0709b29e359142297ff64536) - **singlestore**: Fixed generaion/parsing of exp.VariancePop *(PR [#5682](https://github.com/tobymao/sqlglot/pull/5682) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`eb538bf`](https://github.com/tobymao/sqlglot/commit/eb538bf225645d0a54d614733e447c13cf91a37a) - **singlestore**: Fixed generation of exp.Chr *(PR [#5683](https://github.com/tobymao/sqlglot/pull/5683) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`32d9dd1`](https://github.com/tobymao/sqlglot/commit/32d9dd1309ce0876114f57993596c4456aa1d50f) - **singlestore**: Fixed exp.MD5Digest generation *(PR [#5688](https://github.com/tobymao/sqlglot/pull/5688) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`5c1eb2d`](https://github.com/tobymao/sqlglot/commit/5c1eb2df5dd3dcc6ed2c8204cec56b5c3d276f87) - **optimizer**: parse and annotate type for bq PARSE_BIG/NUMERIC *(PR [#5690](https://github.com/tobymao/sqlglot/pull/5690) by [@geooo109](https://github.com/geooo109))*
- [`6f88500`](https://github.com/tobymao/sqlglot/commit/6f885007a075339cf20034459571a6ae821c61c0) - **singlestore**: Fixed exp.IsAscii generation *(PR [#5687](https://github.com/tobymao/sqlglot/pull/5687) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`311373d`](https://github.com/tobymao/sqlglot/commit/311373d22134de906d1c1cef019541e85e2f7c9f) - **optimizer**: parse and annotate type for bq CODE_POINTS_TO_BYTES *(PR [#5686](https://github.com/tobymao/sqlglot/pull/5686) by [@geooo109](https://github.com/geooo109))*
- [`79d9de1`](https://github.com/tobymao/sqlglot/commit/79d9de1745598f8f3ae2c82c1389dd455c946a09) - **optimizer**: parse and annotate type for bq TO_CODE_POINTS *(PR [#5685](https://github.com/tobymao/sqlglot/pull/5685) by [@geooo109](https://github.com/geooo109))*
- [`5df3ea9`](https://github.com/tobymao/sqlglot/commit/5df3ea92f59125955124ea1883b777b489db3042) - **optimizer**: parse and annotate type for bq SAFE_CONVERT_BYTES_TO_STRING *(PR [#5681](https://github.com/tobymao/sqlglot/pull/5681) by [@geooo109](https://github.com/geooo109))*
- [`c832746`](https://github.com/tobymao/sqlglot/commit/c832746018fbc2c531d5b2a7c7f8cd5d78e511ff) - **optimizer**: parse and annotate type for bigquery APPROX_QUANTILES *(PR [#5678](https://github.com/tobymao/sqlglot/pull/5678) by [@geooo109](https://github.com/geooo109))*
- [`8fa5ae8`](https://github.com/tobymao/sqlglot/commit/8fa5ae8a61c698abaea265b4950390ea3ddfa7e9) - **singlestore**: Fixed generation/parsing of exp.RegexpExtract *(PR [#5691](https://github.com/tobymao/sqlglot/pull/5691) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`d6d409a`](https://github.com/tobymao/sqlglot/commit/d6d409a548042063f80d02dfaf5b61a0096d1d50) - **singlestore**: Fixed generaion of exp.Repeat *(PR [#5693](https://github.com/tobymao/sqlglot/pull/5693) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`b7db08b`](https://github.com/tobymao/sqlglot/commit/b7db08b96c7d7d02ec54f26b8749b3d57f021d8b) - **singlestore**: Fixed generation of exp.StartsWith *(PR [#5694](https://github.com/tobymao/sqlglot/pull/5694) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`87b04ef`](https://github.com/tobymao/sqlglot/commit/87b04ef0fc2df5064be9e6b75b264cff0639face) - **singlestore**: Fixed generation of exp.FromBase *(PR [#5695](https://github.com/tobymao/sqlglot/pull/5695) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`9c1d0fd`](https://github.com/tobymao/sqlglot/commit/9c1d0fdac9acd3fb3109ca3d3cae9c9ffaed1a7d) - **duckdb**: transpile array unique aggregation closes [#5689](https://github.com/tobymao/sqlglot/pull/5689) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`99e169e`](https://github.com/tobymao/sqlglot/commit/99e169ea13d5be3712a47f6b55b98a4764a3c24d) - **optimizer**: parse and annotate type for bq BOOL *(PR [#5697](https://github.com/tobymao/sqlglot/pull/5697) by [@geooo109](https://github.com/geooo109))*
- [`3f31770`](https://github.com/tobymao/sqlglot/commit/3f31770c793f464fcac1ce2b8dfa03d4b7f0231c) - **optimizer**: parse and annotate type for bq FLOAT64 *(PR [#5700](https://github.com/tobymao/sqlglot/pull/5700) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`f1269f5`](https://github.com/tobymao/sqlglot/commit/f1269f5ecfccfee4cdeeda5bfd10eb1c47994fad) - **tsql**: do not attach limit modifier to set operation *(PR [#5609](https://github.com/tobymao/sqlglot/pull/5609) by [@georgesittas](https://github.com/georgesittas))*
- [`a6edf8e`](https://github.com/tobymao/sqlglot/commit/a6edf8ee3273a7736ed801ef8dea302613b119da) - **tsql**: Remove ORDER from set op modifiers too *(PR [#5626](https://github.com/tobymao/sqlglot/pull/5626) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5618](https://github.com/tobymao/sqlglot/issues/5618) opened by [@MQMMMQM](https://github.com/MQMMMQM)*
- [`ce5840e`](https://github.com/tobymao/sqlglot/commit/ce5840ed615e162a93cd911ab6207160878fcc64) - **exasol**: update several dialect properties to correctly reflect semantics *(PR [#5642](https://github.com/tobymao/sqlglot/pull/5642) by [@nnamdi16](https://github.com/nnamdi16))*
- [`3ab1d44`](https://github.com/tobymao/sqlglot/commit/3ab1d4487279cab3be2d3764e51516c6db21629d) - **generator**: Wrap CONCAT items with COALESCE less aggressively *(PR [#5641](https://github.com/tobymao/sqlglot/pull/5641) by [@VaggelisD](https://github.com/VaggelisD))*
- [`045d2f0`](https://github.com/tobymao/sqlglot/commit/045d2f02649b0e6dc178c079e4e0db201ed9bf08) - **duckdb**: Transpile Spark's FIRST(col, TRUE) *(PR [#5644](https://github.com/tobymao/sqlglot/pull/5644) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5643](https://github.com/tobymao/sqlglot/issues/5643) opened by [@michal-clutch](https://github.com/michal-clutch)*

### :wrench: Chores
- [`4c04c0c`](https://github.com/tobymao/sqlglot/commit/4c04c0ce859ab8314ed36fb8779f14c0fc2f1094) - use a valid SPDX identifier as license classifier *(PR [#5606](https://github.com/tobymao/sqlglot/pull/5606) by [@ecederstrand](https://github.com/ecederstrand))*
- [`249f638`](https://github.com/tobymao/sqlglot/commit/249f638877ddd2a1732d1e6bc859793f3bc0622d) - add table to document dialect support level *(PR [#5628](https://github.com/tobymao/sqlglot/pull/5628) by [@georgesittas](https://github.com/georgesittas))*
- [`3357125`](https://github.com/tobymao/sqlglot/commit/33571250d172d64a3e0450738b3ad330e5c0a795) - **doris**: refactor unique key prop generation *(PR [#5625](https://github.com/tobymao/sqlglot/pull/5625) by [@georgesittas](https://github.com/georgesittas))*
- [`545f1ac`](https://github.com/tobymao/sqlglot/commit/545f1acd76bdc4e537209266984137f6c69ce622) - Clean up of PR5614 *(PR [#5648](https://github.com/tobymao/sqlglot/pull/5648) by [@VaggelisD](https://github.com/VaggelisD))*


## [v27.8.0] - 2025-08-19
### :boom: BREAKING CHANGES
- due to [`2a33339`](https://github.com/tobymao/sqlglot/commit/2a333395cde71936df911488afcff92cae735e11) - annotate type for bigquery REPLACE *(PR [#5572](https://github.com/tobymao/sqlglot/pull/5572) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery REPLACE (#5572)

- due to [`1e6f813`](https://github.com/tobymao/sqlglot/commit/1e6f81343de641e588f1a05ce7dc01bed72bd849) - annotate type for bigquery REGEXP_EXTRACT_ALL *(PR [#5573](https://github.com/tobymao/sqlglot/pull/5573) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery REGEXP_EXTRACT_ALL (#5573)

- due to [`d0d62ed`](https://github.com/tobymao/sqlglot/commit/d0d62ede6320b3fd0eee04b7073f5708676dc58c) - support `TO_CHAR` with numeric inputs *(PR [#5570](https://github.com/tobymao/sqlglot/pull/5570) by [@jasonthomassql](https://github.com/jasonthomassql))*:

  support `TO_CHAR` with numeric inputs (#5570)

- due to [`7928985`](https://github.com/tobymao/sqlglot/commit/7928985a655c3d0244bc9175a37f502b19a5c5f0) - allow dashes in JSONPath keys *(PR [#5574](https://github.com/tobymao/sqlglot/pull/5574) by [@georgesittas](https://github.com/georgesittas))*:

  allow dashes in JSONPath keys (#5574)

- due to [`eb09e6e`](https://github.com/tobymao/sqlglot/commit/eb09e6e32491a05846488de7b72b1dca0e0a2669) - parse and annotate type for bigquery TRANSLATE *(PR [#5575](https://github.com/tobymao/sqlglot/pull/5575) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery TRANSLATE (#5575)

- due to [`f9a522b`](https://github.com/tobymao/sqlglot/commit/f9a522b26cd5d643b8b18fa64d70f2a3f0ff2d2c) - parse and annotate type for bigquery SOUNDEX *(PR [#5576](https://github.com/tobymao/sqlglot/pull/5576) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery SOUNDEX (#5576)

- due to [`51da41b`](https://github.com/tobymao/sqlglot/commit/51da41b90ce421b154e45add28353ac044640a1c) - annotate type for bigquery MD5 *(PR [#5577](https://github.com/tobymao/sqlglot/pull/5577) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery MD5 (#5577)

- due to [`bcf302f`](https://github.com/tobymao/sqlglot/commit/bcf302ff6ad2d0adfc29f708a8b53b5c0e547619) - annotate type for bigquery MIN/MAX BY *(PR [#5579](https://github.com/tobymao/sqlglot/pull/5579) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery MIN/MAX BY (#5579)

- due to [`c501d9e`](https://github.com/tobymao/sqlglot/commit/c501d9e6f58e4880e4d23f21f53f72dcb5fdaa8c) - parse and annotate type for bigquery GROUPING *(PR [#5581](https://github.com/tobymao/sqlglot/pull/5581) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery GROUPING (#5581)


### :sparkles: New Features
- [`2a33339`](https://github.com/tobymao/sqlglot/commit/2a333395cde71936df911488afcff92cae735e11) - **optimizer**: annotate type for bigquery REPLACE *(PR [#5572](https://github.com/tobymao/sqlglot/pull/5572) by [@geooo109](https://github.com/geooo109))*
- [`1e6f813`](https://github.com/tobymao/sqlglot/commit/1e6f81343de641e588f1a05ce7dc01bed72bd849) - **optimizer**: annotate type for bigquery REGEXP_EXTRACT_ALL *(PR [#5573](https://github.com/tobymao/sqlglot/pull/5573) by [@geooo109](https://github.com/geooo109))*
- [`eb09e6e`](https://github.com/tobymao/sqlglot/commit/eb09e6e32491a05846488de7b72b1dca0e0a2669) - **optimizer**: parse and annotate type for bigquery TRANSLATE *(PR [#5575](https://github.com/tobymao/sqlglot/pull/5575) by [@geooo109](https://github.com/geooo109))*
- [`f9a522b`](https://github.com/tobymao/sqlglot/commit/f9a522b26cd5d643b8b18fa64d70f2a3f0ff2d2c) - **optimizer**: parse and annotate type for bigquery SOUNDEX *(PR [#5576](https://github.com/tobymao/sqlglot/pull/5576) by [@geooo109](https://github.com/geooo109))*
- [`51da41b`](https://github.com/tobymao/sqlglot/commit/51da41b90ce421b154e45add28353ac044640a1c) - **optimizer**: annotate type for bigquery MD5 *(PR [#5577](https://github.com/tobymao/sqlglot/pull/5577) by [@geooo109](https://github.com/geooo109))*
- [`bcf302f`](https://github.com/tobymao/sqlglot/commit/bcf302ff6ad2d0adfc29f708a8b53b5c0e547619) - **optimizer**: annotate type for bigquery MIN/MAX BY *(PR [#5579](https://github.com/tobymao/sqlglot/pull/5579) by [@geooo109](https://github.com/geooo109))*
- [`c501d9e`](https://github.com/tobymao/sqlglot/commit/c501d9e6f58e4880e4d23f21f53f72dcb5fdaa8c) - **optimizer**: parse and annotate type for bigquery GROUPING *(PR [#5581](https://github.com/tobymao/sqlglot/pull/5581) by [@geooo109](https://github.com/geooo109))*
- [`8612825`](https://github.com/tobymao/sqlglot/commit/86128253f911b733d45b073356e3b8ddf261c22b) - **spark**: generate date/time ops as interval binary ops *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`8fda774`](https://github.com/tobymao/sqlglot/commit/8fda774b7a9b0c66948349dfe030d3c122ff6eee) - **singlestore**: Added parsing and generation of JSON_EXTRACT *(PR [#5555](https://github.com/tobymao/sqlglot/pull/5555) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`82cc954`](https://github.com/tobymao/sqlglot/commit/82cc9549a875211a400e5c4e818b05ca48a0a9f4) - **exasol**: map div function to IntDiv in exasol dialect *(PR [#5593](https://github.com/tobymao/sqlglot/pull/5593) by [@nnamdi16](https://github.com/nnamdi16))*
- [`eb0fe68`](https://github.com/tobymao/sqlglot/commit/eb0fe68d6b5977053c871badf2f5c1895b3e1c66) - **trino**: add JSON_VALUE function support with RETURNING clause *(PR [#5590](https://github.com/tobymao/sqlglot/pull/5590) by [@rev-rwasilewski](https://github.com/rev-rwasilewski))*
- [`9e95c11`](https://github.com/tobymao/sqlglot/commit/9e95c115ea0304d9ccb4cb0be8389f5ff5f2a952) - **exasol**: mapped weekofyear to week in Exasol dialect *(PR [#5594](https://github.com/tobymao/sqlglot/pull/5594) by [@nnamdi16](https://github.com/nnamdi16))*
- [`8f013c3`](https://github.com/tobymao/sqlglot/commit/8f013c37a412ca5978889c1e47b0c6f7add0715d) - **singlestore**: Fixed parsing of DATE function *(PR [#5601](https://github.com/tobymao/sqlglot/pull/5601) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`a4a299a`](https://github.com/tobymao/sqlglot/commit/a4a299acbaf4461f0c2b470bc4e9e9590515eda7) - transpile `TO_CHAR` from Dremio to Databricks *(PR [#5598](https://github.com/tobymao/sqlglot/pull/5598) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`093f35c`](https://github.com/tobymao/sqlglot/commit/093f35c201c3c22c3a14c6f8de26c06246bdf19c) - **dremio**: handle `DATE_FORMAT`, `TO_DATE`, and `TO_TIMESTAMP` *(PR [#5597](https://github.com/tobymao/sqlglot/pull/5597) by [@jasonthomassql](https://github.com/jasonthomassql))*

### :bug: Bug Fixes
- [`d0d62ed`](https://github.com/tobymao/sqlglot/commit/d0d62ede6320b3fd0eee04b7073f5708676dc58c) - **dremio**: support `TO_CHAR` with numeric inputs *(PR [#5570](https://github.com/tobymao/sqlglot/pull/5570) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`7928985`](https://github.com/tobymao/sqlglot/commit/7928985a655c3d0244bc9175a37f502b19a5c5f0) - **bigquery**: allow dashes in JSONPath keys *(PR [#5574](https://github.com/tobymao/sqlglot/pull/5574) by [@georgesittas](https://github.com/georgesittas))*
- [`866042d`](https://github.com/tobymao/sqlglot/commit/866042d0268da0cebce042c0868878c0fb39c3d1) - Remove TokenType.APPLY from table alias tokens *(PR [#5592](https://github.com/tobymao/sqlglot/pull/5592) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5591](https://github.com/tobymao/sqlglot/issues/5591) opened by [@saadbelgi](https://github.com/saadbelgi)*
- [`b485f66`](https://github.com/tobymao/sqlglot/commit/b485f6666fa8625b7da45ef832b5d666fbb707ea) - **dremio**: improve `TO_CHAR` transpilability *(PR [#5580](https://github.com/tobymao/sqlglot/pull/5580) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`81874e9`](https://github.com/tobymao/sqlglot/commit/81874e9c3aafcc2cf8fb443f65146c5b3598b9b3) - handle unknown types in `unit_to_str` *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`173e442`](https://github.com/tobymao/sqlglot/commit/173e4425b692728abffa8542324690823f984303) - refactor JSON_VALUE handling for MySQL and Trino *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.7.0] - 2025-08-13
### :boom: BREAKING CHANGES
- due to [`938f4b6`](https://github.com/tobymao/sqlglot/commit/938f4b6ebc1c0d26bd3c1400883978c79a435189) - annotate type for LAST_DAY *(PR [#5528](https://github.com/tobymao/sqlglot/pull/5528) by [@geooo109](https://github.com/geooo109))*:

  annotate type for LAST_DAY (#5528)

- due to [`7d12dac`](https://github.com/tobymao/sqlglot/commit/7d12dac613ba5119334408f2c52cb270067156d9) - annotate type for bigquery GENERATE_TIMESTAMP_ARRAY *(PR [#5529](https://github.com/tobymao/sqlglot/pull/5529) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery GENERATE_TIMESTAMP_ARRAY (#5529)

- due to [`d50ebe2`](https://github.com/tobymao/sqlglot/commit/d50ebe286dd8e2836b9eb2a3406f15976db3aa05) - annotate type for bigquery TIME_TRUNC *(PR [#5530](https://github.com/tobymao/sqlglot/pull/5530) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery TIME_TRUNC (#5530)

- due to [`29748be`](https://github.com/tobymao/sqlglot/commit/29748be7dfc10edc9f29665c98327883dd25c13d) - annotate type for bigquery TIME *(PR [#5531](https://github.com/tobymao/sqlglot/pull/5531) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery TIME (#5531)

- due to [`7003b3f`](https://github.com/tobymao/sqlglot/commit/7003b3fa39cd455e3643066364696708d1ac4f38) - parse and annotate type for bigquery DATE_FROM_UNIX_DATE *(PR [#5532](https://github.com/tobymao/sqlglot/pull/5532) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery DATE_FROM_UNIX_DATE (#5532)

- due to [`a276ca6`](https://github.com/tobymao/sqlglot/commit/a276ca6fd5f9d47fa8c90fcfa19f9864e7a28f8f) - parse and annotate type for bigquery JUSTIFY funcs *(PR [#5534](https://github.com/tobymao/sqlglot/pull/5534) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery JUSTIFY funcs (#5534)

- due to [`374178e`](https://github.com/tobymao/sqlglot/commit/374178e22fe8d2d2275b65fe08e27ef66c611220) - parse and annotate type for bigquery UNIX_MICROS and UNIX_MILLIS *(PR [#5535](https://github.com/tobymao/sqlglot/pull/5535) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery UNIX_MICROS and UNIX_MILLIS (#5535)

- due to [`1d8d1ab`](https://github.com/tobymao/sqlglot/commit/1d8d1abe459053a135a46525d0a13bb861220927) - annotate type for bigquery DATE_TRUNC *(PR [#5540](https://github.com/tobymao/sqlglot/pull/5540) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery DATE_TRUNC (#5540)

- due to [`306ba65`](https://github.com/tobymao/sqlglot/commit/306ba6531839ea2823f5165de7bde01d17560845) - annotate type for bigquery TIMESTAMP_TRUNC *(PR [#5541](https://github.com/tobymao/sqlglot/pull/5541) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery TIMESTAMP_TRUNC (#5541)

- due to [`d799c5a`](https://github.com/tobymao/sqlglot/commit/d799c5af23010a67c29edb6d45a40fb24903e1a3) - preserve projection names when merging subqueries *(commit by [@snovik75](https://github.com/snovik75))*:

  preserve projection names when merging subqueries

- due to [`8130bd4`](https://github.com/tobymao/sqlglot/commit/8130bd40815803a6781ee8f20fccd30987516192) - WEEKDAY of WEEK as VAR *(PR [#5552](https://github.com/tobymao/sqlglot/pull/5552) by [@geooo109](https://github.com/geooo109))*:

  WEEKDAY of WEEK as VAR (#5552)

- due to [`f3ffe19`](https://github.com/tobymao/sqlglot/commit/f3ffe19ec01533c5f27b9d3a7b6704b83c005118) - annotate type for bigquery format_time *(PR [#5559](https://github.com/tobymao/sqlglot/pull/5559) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery format_time (#5559)

- due to [`6872b43`](https://github.com/tobymao/sqlglot/commit/6872b43ba17a39137172fd2fa9f0d059ce595ef9) - use dialect in DataType.build fixes [#5560](https://github.com/tobymao/sqlglot/pull/5560) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  use dialect in DataType.build fixes #5560

- due to [`3ab3690`](https://github.com/tobymao/sqlglot/commit/3ab369096313b418699b7942b1c513c0c66a5331) - parse and annotate type for bigquery PARSE_DATETIME *(PR [#5558](https://github.com/tobymao/sqlglot/pull/5558) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery PARSE_DATETIME (#5558)

- due to [`e5da951`](https://github.com/tobymao/sqlglot/commit/e5da951542eb55691bc43fbbfbec4a30100de038) - parse and annotate type for bigquery PARSE_TIME *(PR [#5561](https://github.com/tobymao/sqlglot/pull/5561) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery PARSE_TIME (#5561)

- due to [`798e213`](https://github.com/tobymao/sqlglot/commit/798e213fd10c3b61afbd8cef621546de65fa6f26) - improve transpilability of ANY_VALUE closes [#5563](https://github.com/tobymao/sqlglot/pull/5563) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilability of ANY_VALUE closes #5563

- due to [`8c0cb76`](https://github.com/tobymao/sqlglot/commit/8c0cb764fd825062fb7334032b8eeffbc39627d5) - more robust CREATE SEQUENCE *(PR [#5566](https://github.com/tobymao/sqlglot/pull/5566) by [@geooo109](https://github.com/geooo109))*:

  more robust CREATE SEQUENCE (#5566)

- due to [`c7041c7`](https://github.com/tobymao/sqlglot/commit/c7041c71250b17192c2f25fb8f33407324d332c2) - parse and annotate type for bigquery BYTE_LENGHT *(PR [#5568](https://github.com/tobymao/sqlglot/pull/5568) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery BYTE_LENGHT (#5568)

- due to [`a6c61c3`](https://github.com/tobymao/sqlglot/commit/a6c61c34f1e168c97dd5c2b8ec071372ba593992) - parse and annotate type for bigquery CODE_POINTS_TO_STRING *(PR [#5569](https://github.com/tobymao/sqlglot/pull/5569) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery CODE_POINTS_TO_STRING (#5569)

- due to [`51e0335`](https://github.com/tobymao/sqlglot/commit/51e0335377fe2bc2e2a94a623475791e9dd19fb9) - parse and annotate type for bigquery REVERSE *(PR [#5571](https://github.com/tobymao/sqlglot/pull/5571) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery REVERSE (#5571)


### :sparkles: New Features
- [`1fb90db`](https://github.com/tobymao/sqlglot/commit/1fb90db52b59e6e3a40597c6f611d0476b72025b) - **teradata**: Add support for Teradata set query band expression *(PR [#5519](https://github.com/tobymao/sqlglot/pull/5519) by [@treff7es](https://github.com/treff7es))*
- [`a49baaf`](https://github.com/tobymao/sqlglot/commit/a49baaf717cb41abb25ca51ae5adddc8473baa8b) - **doris**: Override table_sql to avoid AS keyword in UPDATE and DELETE statements *(PR [#5517](https://github.com/tobymao/sqlglot/pull/5517) by [@peterylh](https://github.com/peterylh))*
- [`75fd6d2`](https://github.com/tobymao/sqlglot/commit/75fd6d21fb7bc8399432e73d10b4837ae62d2ab5) - **exasol**: Add support for date difference functions in Exasol dialect *(PR [#5510](https://github.com/tobymao/sqlglot/pull/5510) by [@nnamdi16](https://github.com/nnamdi16))*
- [`2a91bb4`](https://github.com/tobymao/sqlglot/commit/2a91bb4f17c7569a5b409cc07e970e5d68235149) - **teradata**: Add support for Teradata locking select *(PR [#5524](https://github.com/tobymao/sqlglot/pull/5524) by [@treff7es](https://github.com/treff7es))*
- [`938f4b6`](https://github.com/tobymao/sqlglot/commit/938f4b6ebc1c0d26bd3c1400883978c79a435189) - **optimizer**: annotate type for LAST_DAY *(PR [#5528](https://github.com/tobymao/sqlglot/pull/5528) by [@geooo109](https://github.com/geooo109))*
- [`7d12dac`](https://github.com/tobymao/sqlglot/commit/7d12dac613ba5119334408f2c52cb270067156d9) - **optimizer**: annotate type for bigquery GENERATE_TIMESTAMP_ARRAY *(PR [#5529](https://github.com/tobymao/sqlglot/pull/5529) by [@geooo109](https://github.com/geooo109))*
- [`d50ebe2`](https://github.com/tobymao/sqlglot/commit/d50ebe286dd8e2836b9eb2a3406f15976db3aa05) - **optimizer**: annotate type for bigquery TIME_TRUNC *(PR [#5530](https://github.com/tobymao/sqlglot/pull/5530) by [@geooo109](https://github.com/geooo109))*
- [`29748be`](https://github.com/tobymao/sqlglot/commit/29748be7dfc10edc9f29665c98327883dd25c13d) - **optimizer**: annotate type for bigquery TIME *(PR [#5531](https://github.com/tobymao/sqlglot/pull/5531) by [@geooo109](https://github.com/geooo109))*
- [`7003b3f`](https://github.com/tobymao/sqlglot/commit/7003b3fa39cd455e3643066364696708d1ac4f38) - **optimizer**: parse and annotate type for bigquery DATE_FROM_UNIX_DATE *(PR [#5532](https://github.com/tobymao/sqlglot/pull/5532) by [@geooo109](https://github.com/geooo109))*
- [`a276ca6`](https://github.com/tobymao/sqlglot/commit/a276ca6fd5f9d47fa8c90fcfa19f9864e7a28f8f) - **optimizer**: parse and annotate type for bigquery JUSTIFY funcs *(PR [#5534](https://github.com/tobymao/sqlglot/pull/5534) by [@geooo109](https://github.com/geooo109))*
- [`374178e`](https://github.com/tobymao/sqlglot/commit/374178e22fe8d2d2275b65fe08e27ef66c611220) - **optimizer**: parse and annotate type for bigquery UNIX_MICROS and UNIX_MILLIS *(PR [#5535](https://github.com/tobymao/sqlglot/pull/5535) by [@geooo109](https://github.com/geooo109))*
- [`1d8d1ab`](https://github.com/tobymao/sqlglot/commit/1d8d1abe459053a135a46525d0a13bb861220927) - **optimizer**: annotate type for bigquery DATE_TRUNC *(PR [#5540](https://github.com/tobymao/sqlglot/pull/5540) by [@geooo109](https://github.com/geooo109))*
- [`306ba65`](https://github.com/tobymao/sqlglot/commit/306ba6531839ea2823f5165de7bde01d17560845) - **optimizer**: annotate type for bigquery TIMESTAMP_TRUNC *(PR [#5541](https://github.com/tobymao/sqlglot/pull/5541) by [@geooo109](https://github.com/geooo109))*
- [`6a68cca`](https://github.com/tobymao/sqlglot/commit/6a68cca97ad4bdd75c544ada0a5af0fa92ec4664) - **dremio**: support lowercase `TIME_MAPPING` formats *(PR [#5556](https://github.com/tobymao/sqlglot/pull/5556) by [@jasonthomassql](https://github.com/jasonthomassql))*
- [`f3ffe19`](https://github.com/tobymao/sqlglot/commit/f3ffe19ec01533c5f27b9d3a7b6704b83c005118) - **optimizer**: annotate type for bigquery format_time *(PR [#5559](https://github.com/tobymao/sqlglot/pull/5559) by [@geooo109](https://github.com/geooo109))*
- [`3ab3690`](https://github.com/tobymao/sqlglot/commit/3ab369096313b418699b7942b1c513c0c66a5331) - **optimizer**: parse and annotate type for bigquery PARSE_DATETIME *(PR [#5558](https://github.com/tobymao/sqlglot/pull/5558) by [@geooo109](https://github.com/geooo109))*
- [`e5da951`](https://github.com/tobymao/sqlglot/commit/e5da951542eb55691bc43fbbfbec4a30100de038) - **optimizer**: parse and annotate type for bigquery PARSE_TIME *(PR [#5561](https://github.com/tobymao/sqlglot/pull/5561) by [@geooo109](https://github.com/geooo109))*
- [`902a0cd`](https://github.com/tobymao/sqlglot/commit/902a0cdfe46f693aa55612d45a2de2def21f0b8c) - **singlestore**: Added parsing/generation of UNIXTIME functions *(PR [#5562](https://github.com/tobymao/sqlglot/pull/5562) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`798e213`](https://github.com/tobymao/sqlglot/commit/798e213fd10c3b61afbd8cef621546de65fa6f26) - **duckdb**: improve transpilability of ANY_VALUE closes [#5563](https://github.com/tobymao/sqlglot/pull/5563) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`c7041c7`](https://github.com/tobymao/sqlglot/commit/c7041c71250b17192c2f25fb8f33407324d332c2) - **optimizer**: parse and annotate type for bigquery BYTE_LENGHT *(PR [#5568](https://github.com/tobymao/sqlglot/pull/5568) by [@geooo109](https://github.com/geooo109))*
- [`a6c61c3`](https://github.com/tobymao/sqlglot/commit/a6c61c34f1e168c97dd5c2b8ec071372ba593992) - **optimizer**: parse and annotate type for bigquery CODE_POINTS_TO_STRING *(PR [#5569](https://github.com/tobymao/sqlglot/pull/5569) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`9020684`](https://github.com/tobymao/sqlglot/commit/9020684a7e984a10fa4775339596ac5a0d6a6d93) - nested natural join performance closes [#5514](https://github.com/tobymao/sqlglot/pull/5514) *(PR [#5515](https://github.com/tobymao/sqlglot/pull/5515) by [@tobymao](https://github.com/tobymao))*
- [`394870a`](https://github.com/tobymao/sqlglot/commit/394870a7ee9bb3bc814b7c3847193687f06b432b) - **duckdb**: transpile ADD_MONTHS *(PR [#5523](https://github.com/tobymao/sqlglot/pull/5523) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5505](https://github.com/tobymao/sqlglot/issues/5505) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`249692c`](https://github.com/tobymao/sqlglot/commit/249692c67450a1fe3775e1f35b6f62fdb0a62e1a) - **duckdb**: put guard in AddMonths generator before annotating it *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`d799c5a`](https://github.com/tobymao/sqlglot/commit/d799c5af23010a67c29edb6d45a40fb24903e1a3) - **optimizer**: preserve projection names when merging subqueries *(commit by [@snovik75](https://github.com/snovik75))*
- [`8130bd4`](https://github.com/tobymao/sqlglot/commit/8130bd40815803a6781ee8f20fccd30987516192) - **parser**: WEEKDAY of WEEK as VAR *(PR [#5552](https://github.com/tobymao/sqlglot/pull/5552) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5547](https://github.com/tobymao/sqlglot/issues/5547) opened by [@rloredo](https://github.com/rloredo)*
- [`4e1373f`](https://github.com/tobymao/sqlglot/commit/4e1373f301cbea3cb5762fc1430b65deae3f9d04) - **doris**: Rename Table *(PR [#5549](https://github.com/tobymao/sqlglot/pull/5549) by [@xinge-ji](https://github.com/xinge-ji))*
- [`16f544d`](https://github.com/tobymao/sqlglot/commit/16f544dc25d5d61277d32f02e4be18c10d16cf9f) - **doris**: fix DATE_TRUNC and partition by *(PR [#5553](https://github.com/tobymao/sqlglot/pull/5553) by [@xinge-ji](https://github.com/xinge-ji))*
- [`6295414`](https://github.com/tobymao/sqlglot/commit/6295414fb41401f92993e661b880a0727e74c087) - convert unit to Var instead of choosing default in `unit_to_var` *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6872b43`](https://github.com/tobymao/sqlglot/commit/6872b43ba17a39137172fd2fa9f0d059ce595ef9) - **parser**: use dialect in DataType.build fixes [#5560](https://github.com/tobymao/sqlglot/pull/5560) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6f354d9`](https://github.com/tobymao/sqlglot/commit/6f354d958fb9ca9242b7fc1d2da86af74d57fedc) - **clickhouse**: add ROWS keyword in OFFSET followed by FETCH fixes [#5564](https://github.com/tobymao/sqlglot/pull/5564) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`8c0cb76`](https://github.com/tobymao/sqlglot/commit/8c0cb764fd825062fb7334032b8eeffbc39627d5) - **parser**: more robust CREATE SEQUENCE *(PR [#5566](https://github.com/tobymao/sqlglot/pull/5566) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5537](https://github.com/tobymao/sqlglot/issues/5537) opened by [@tekumara](https://github.com/tekumara)*
- [`7e9df88`](https://github.com/tobymao/sqlglot/commit/7e9df880bc118d0dbb2dbd6344f805f79af2fe5e) - **doris**: CURRENT_DATE *(PR [#5567](https://github.com/tobymao/sqlglot/pull/5567) by [@xinge-ji](https://github.com/xinge-ji))*
- [`51e0335`](https://github.com/tobymao/sqlglot/commit/51e0335377fe2bc2e2a94a623475791e9dd19fb9) - **optimizer**: parse and annotate type for bigquery REVERSE *(PR [#5571](https://github.com/tobymao/sqlglot/pull/5571) by [@geooo109](https://github.com/geooo109))*

### :wrench: Chores
- [`720f634`](https://github.com/tobymao/sqlglot/commit/720f6343f6144e8986ec6b7e50419c3d7a331f0a) - Fix style on main, refactor exasol tests *(PR [#5527](https://github.com/tobymao/sqlglot/pull/5527) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5653501`](https://github.com/tobymao/sqlglot/commit/5653501606f041282b6315c3efa33b9a3baf8d98) - Refactor PR 5517 *(PR [#5526](https://github.com/tobymao/sqlglot/pull/5526) by [@VaggelisD](https://github.com/VaggelisD))*
- [`d15dfe3`](https://github.com/tobymao/sqlglot/commit/d15dfe3f0f4444e4999ad65051b2474e62f422b3) - build type using dialect for bigquery *(PR [#5539](https://github.com/tobymao/sqlglot/pull/5539) by [@geooo109](https://github.com/geooo109))*


## [v27.6.0] - 2025-08-01
### :boom: BREAKING CHANGES
- due to [`6b691b3`](https://github.com/tobymao/sqlglot/commit/6b691b33c3528c0377bd8822a3df90de869c6cb1) - Parse and transpile GET(...) extract function *(PR [#5500](https://github.com/tobymao/sqlglot/pull/5500) by [@VaggelisD](https://github.com/VaggelisD))*:

  Parse and transpile GET(...) extract function (#5500)

- due to [`964a275`](https://github.com/tobymao/sqlglot/commit/964a275b42314380de3b301ada9f9756602729f7) - Make `UNION` column qualification recursive *(PR [#5508](https://github.com/tobymao/sqlglot/pull/5508) by [@VaggelisD](https://github.com/VaggelisD))*:

  Make `UNION` column qualification recursive (#5508)


### :sparkles: New Features
- [`6b691b3`](https://github.com/tobymao/sqlglot/commit/6b691b33c3528c0377bd8822a3df90de869c6cb1) - **snowflake**: Parse and transpile GET(...) extract function *(PR [#5500](https://github.com/tobymao/sqlglot/pull/5500) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5495](https://github.com/tobymao/sqlglot/issues/5495) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`a2a2f0f`](https://github.com/tobymao/sqlglot/commit/a2a2f0fe910228651c5c39beebcc02172a0b7e94) - **exasol**: Add support for IF, NULLIFZERO, and ZEROIFNULL functions *(PR [#5502](https://github.com/tobymao/sqlglot/pull/5502) by [@nnamdi16](https://github.com/nnamdi16))*
- [`2d8ce58`](https://github.com/tobymao/sqlglot/commit/2d8ce587c75f21b188ec4c201936eedac3b051e8) - **singlestore**: Added cast operator *(PR [#5504](https://github.com/tobymao/sqlglot/pull/5504) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`6256348`](https://github.com/tobymao/sqlglot/commit/6256348a28b72ae9052d4244736846af209410b0) - **exasol**: add support for ADD_DAYS function in exasol dialect *(PR [#5507](https://github.com/tobymao/sqlglot/pull/5507) by [@nnamdi16](https://github.com/nnamdi16))*
- [`2f40fc5`](https://github.com/tobymao/sqlglot/commit/2f40fc578a840c9276a4c3b91351fb8d95c837fc) - add more pseudocols to bq which are not expanded by star *(PR [#5509](https://github.com/tobymao/sqlglot/pull/5509) by [@z3z1ma](https://github.com/z3z1ma))*

### :bug: Bug Fixes
- [`3b52061`](https://github.com/tobymao/sqlglot/commit/3b520611c5a894ddea935d13aadd27c791a8a755) - **exasol**: fix TokenType.TEXT mapping in exasol dialect *(PR [#5506](https://github.com/tobymao/sqlglot/pull/5506) by [@nnamdi16](https://github.com/nnamdi16))*
- [`964a275`](https://github.com/tobymao/sqlglot/commit/964a275b42314380de3b301ada9f9756602729f7) - Make `UNION` column qualification recursive *(PR [#5508](https://github.com/tobymao/sqlglot/pull/5508) by [@VaggelisD](https://github.com/VaggelisD))*


## [v27.5.1] - 2025-07-30
### :bug: Bug Fixes
- [`caf71d6`](https://github.com/tobymao/sqlglot/commit/caf71d687c0048d2346fddaee58b519e4f2e7945) - `between` builder should not set `symmetric` by default *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.5.0] - 2025-07-30
### :boom: BREAKING CHANGES
- due to [`002286e`](https://github.com/tobymao/sqlglot/commit/002286ee05a608e303a2238a9a74ab963709b5da) - remove AM/PM entries from postgres, oracle `TIME_MAPPING` *(PR [#5491](https://github.com/tobymao/sqlglot/pull/5491) by [@georgesittas](https://github.com/georgesittas))*:

  remove AM/PM entries from postgres, oracle `TIME_MAPPING` (#5491)

- due to [`ad78db6`](https://github.com/tobymao/sqlglot/commit/ad78db6c9002a5bf9188d66f0080dfefd070f77b) - Refactor `LIKE ANY` and support  `ALL | SOME` quantifiers *(PR [#5493](https://github.com/tobymao/sqlglot/pull/5493) by [@VaggelisD](https://github.com/VaggelisD))*:

  Refactor `LIKE ANY` and support  `ALL | SOME` quantifiers (#5493)


### :sparkles: New Features
- [`8cdd9e8`](https://github.com/tobymao/sqlglot/commit/8cdd9e8715b4cf67c200c723940743ed69bbfd80) - **mysql**: Parse UNIQUE INDEX constraint similar to UNIQUE KEY *(PR [#5489](https://github.com/tobymao/sqlglot/pull/5489) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5479](https://github.com/tobymao/sqlglot/issues/5479) opened by [@nathanchapman](https://github.com/nathanchapman)*
- [`787d167`](https://github.com/tobymao/sqlglot/commit/787d167d694b557d6e43ed391f59847a888fa572) - **exasol**: add support for REGEXP_SUBSTR  in exasol dialect *(PR [#5487](https://github.com/tobymao/sqlglot/pull/5487) by [@nnamdi16](https://github.com/nnamdi16))*
- [`0963f60`](https://github.com/tobymao/sqlglot/commit/0963f60987c267c64f2fcfbde469b8b28911a14b) - **singlestore**: Fixed time formatting *(PR [#5476](https://github.com/tobymao/sqlglot/pull/5476) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`488d2e4`](https://github.com/tobymao/sqlglot/commit/488d2e4bf9d4eb148356d1fd6c2360bbf77f283c) - **singlestore**: Added RESERVED_KEYWORDS *(PR [#5497](https://github.com/tobymao/sqlglot/pull/5497) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*
- [`fad9992`](https://github.com/tobymao/sqlglot/commit/fad9992a00478a964552f72802b95ca3918c4377) - **exasol**: Add support for TRUNC, TRUNCATE and DATE_TRUNC function… *(PR [#5490](https://github.com/tobymao/sqlglot/pull/5490) by [@nnamdi16](https://github.com/nnamdi16))*
- [`ad78db6`](https://github.com/tobymao/sqlglot/commit/ad78db6c9002a5bf9188d66f0080dfefd070f77b) - Refactor `LIKE ANY` and support  `ALL | SOME` quantifiers *(PR [#5493](https://github.com/tobymao/sqlglot/pull/5493) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5484](https://github.com/tobymao/sqlglot/issues/5484) opened by [@mazum21](https://github.com/mazum21)*
- [`a7a6f16`](https://github.com/tobymao/sqlglot/commit/a7a6f167d30ac19383ad15931c26751c66a61976) - **singlestore**: Added Tokenizer *(PR [#5492](https://github.com/tobymao/sqlglot/pull/5492) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*

### :bug: Bug Fixes
- [`3982653`](https://github.com/tobymao/sqlglot/commit/3982653e62a42ca1be2bdd8722119e27bd1ba680) - Do not consume BUCKET/TRUNCATE as partitioning keywords *(PR [#5488](https://github.com/tobymao/sqlglot/pull/5488) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5485](https://github.com/tobymao/sqlglot/issues/5485) opened by [@chenkovsky](https://github.com/chenkovsky)*
- [`002286e`](https://github.com/tobymao/sqlglot/commit/002286ee05a608e303a2238a9a74ab963709b5da) - remove AM/PM entries from postgres, oracle `TIME_MAPPING` *(PR [#5491](https://github.com/tobymao/sqlglot/pull/5491) by [@georgesittas](https://github.com/georgesittas))*
- [`74f278a`](https://github.com/tobymao/sqlglot/commit/74f278a226058e196270042e2a9664b9acded28a) - **optimizer**: Fix SEMI/ANTI join handling in optimizer rules *(PR [#5498](https://github.com/tobymao/sqlglot/pull/5498) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5481](https://github.com/tobymao/sqlglot/issues/5481) opened by [@themattmorris](https://github.com/themattmorris)*
- [`42633fb`](https://github.com/tobymao/sqlglot/commit/42633fb49b3c04eeea42e061e33ee08e61960cb4) - dont print (A)SYMMETRIC keyword in BETWEEN for postgres subclasses *(PR [#5503](https://github.com/tobymao/sqlglot/pull/5503) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`18b7d0f`](https://github.com/tobymao/sqlglot/commit/18b7d0fe19708d88b224770d844a8f6a74fe2aa7) - fix deprecated 'license' specification format *(PR [#5494](https://github.com/tobymao/sqlglot/pull/5494) by [@loonies](https://github.com/loonies))*


## [v27.4.1] - 2025-07-27
### :bug: Bug Fixes
- [`ba2b3e2`](https://github.com/tobymao/sqlglot/commit/ba2b3e21ca5454402808b68697ea4eb62963d341) - **bigquery**: make exp.Array type inference more robust *(PR [#5483](https://github.com/tobymao/sqlglot/pull/5483) by [@georgesittas](https://github.com/georgesittas))*


## [v27.4.0] - 2025-07-25
### :boom: BREAKING CHANGES
- due to [`4f348bd`](https://github.com/tobymao/sqlglot/commit/4f348bddda21b18841fd2d728fe486e95cdaa549) - store Query schemas in meta dict instead of type attr *(PR [#5480](https://github.com/tobymao/sqlglot/pull/5480) by [@georgesittas](https://github.com/georgesittas))*:

  store Query schemas in meta dict instead of type attr (#5480)


### :sparkles: New Features
- [`7961ece`](https://github.com/tobymao/sqlglot/commit/7961ece058f3771364aad5beedba9484e3a2e27c) - **exasol**: Add support for HASH_SHA1 function *(PR [#5468](https://github.com/tobymao/sqlglot/pull/5468) by [@nnamdi16](https://github.com/nnamdi16))*
- [`406815d`](https://github.com/tobymao/sqlglot/commit/406815de21f0fdc9874ff46155d4ee0274aa6337) - **exasol**: support HASH_SHA1 *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`e6f4fc9`](https://github.com/tobymao/sqlglot/commit/e6f4fc9c6d59d96777b2a2ec5dcc360e53639f8d) - **sqlite**: support ATTACH/DETACH DATABASE *(PR [#5469](https://github.com/tobymao/sqlglot/pull/5469) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5459](https://github.com/tobymao/sqlglot/issues/5459) opened by [@mariofox](https://github.com/mariofox)*
- [`8aa3498`](https://github.com/tobymao/sqlglot/commit/8aa349890673dccdd4daa0aea6ca5fcb9fdaf46f) - **hive, spark**: Add support for LOCATION in ADD PARTITION *(PR [#5472](https://github.com/tobymao/sqlglot/pull/5472) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5457](https://github.com/tobymao/sqlglot/issues/5457) opened by [@tsamaras](https://github.com/tsamaras)*
- [`44adfc0`](https://github.com/tobymao/sqlglot/commit/44adfc0e74da9d1b05a5a8a67b81fb7c67634c70) - **exasol**: add HASH_MD5 functionality to exasol dialect *(PR [#5473](https://github.com/tobymao/sqlglot/pull/5473) by [@nnamdi16](https://github.com/nnamdi16))*
- [`05e1c4d`](https://github.com/tobymao/sqlglot/commit/05e1c4dbf795915448173a894a89a33b289a3b5b) - **snowflake**: Transpile BQ's `STRUCT` dot access *(PR [#5471](https://github.com/tobymao/sqlglot/pull/5471) by [@VaggelisD](https://github.com/VaggelisD))*
- [`3c5ecdf`](https://github.com/tobymao/sqlglot/commit/3c5ecdf7f27629c01f0f3402e64a9dedf0583851) - **exasol**: Add HASHTYPE_MD5 functions to Exasol dialect *(PR [#5474](https://github.com/tobymao/sqlglot/pull/5474) by [@nnamdi16](https://github.com/nnamdi16))*
- [`1d640d2`](https://github.com/tobymao/sqlglot/commit/1d640d2278288b9a39a65b2532a13bc17e06c4e8) - **exasol**: add support for HASH_SHA256 and HASH_SHA512 hashing *(PR [#5475](https://github.com/tobymao/sqlglot/pull/5475) by [@nnamdi16](https://github.com/nnamdi16))*

### :bug: Bug Fixes
- [`e1819d6`](https://github.com/tobymao/sqlglot/commit/e1819d6451fec0eb3a1f77c90fd8d5c5b0d89889) - only strip kind from joins when it is inner|outer *(PR [#5477](https://github.com/tobymao/sqlglot/pull/5477) by [@themattmorris](https://github.com/themattmorris))*
  - :arrow_lower_right: *fixes issue [#5470](https://github.com/tobymao/sqlglot/issues/5470) opened by [@themattmorris](https://github.com/themattmorris)*
- [`4f348bd`](https://github.com/tobymao/sqlglot/commit/4f348bddda21b18841fd2d728fe486e95cdaa549) - **bigquery**: store Query schemas in meta dict instead of type attr *(PR [#5480](https://github.com/tobymao/sqlglot/pull/5480) by [@georgesittas](https://github.com/georgesittas))*


## [v27.3.1] - 2025-07-24
### :boom: BREAKING CHANGES
- due to [`48703c4`](https://github.com/tobymao/sqlglot/commit/48703c4fadd9f24de151a63d1bfa74f4b8e71133) - temporarily move VARCHAR length inference logic to Fabric *(commit by [@georgesittas](https://github.com/georgesittas))*:

  temporarily move VARCHAR length inference logic to Fabric


### :sparkles: New Features
- [`4cc321c`](https://github.com/tobymao/sqlglot/commit/4cc321cc1995d538ab0c48a7a0a473c31e76ddff) - **singlestore**: Added initial implementation of SingleStore dialect *(PR [#5447](https://github.com/tobymao/sqlglot/pull/5447) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*

### :wrench: Chores
- [`48703c4`](https://github.com/tobymao/sqlglot/commit/48703c4fadd9f24de151a63d1bfa74f4b8e71133) - **tsql**: temporarily move VARCHAR length inference logic to Fabric *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.3.0] - 2025-07-24
### :boom: BREAKING CHANGES
- due to [`d7ccb48`](https://github.com/tobymao/sqlglot/commit/d7ccb48e542c49258e31cc4df45f49beebc2e238) - week/quarter support *(PR [#5374](https://github.com/tobymao/sqlglot/pull/5374) by [@eakmanrq](https://github.com/eakmanrq))*:

  week/quarter support (#5374)

- due to [`b368fba`](https://github.com/tobymao/sqlglot/commit/b368fba59b606e038d445b2ca2d8436e115af3d6) - parse and annotate type for ASCII *(PR [#5377](https://github.com/tobymao/sqlglot/pull/5377) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for ASCII (#5377)

- due to [`7f19b31`](https://github.com/tobymao/sqlglot/commit/7f19b31ebd7981e53a8f8ba343b4f3222fe160c7) - annotate type for UNICODE *(PR [#5381](https://github.com/tobymao/sqlglot/pull/5381) by [@geooo109](https://github.com/geooo109))*:

  annotate type for UNICODE (#5381)

- due to [`9e8d3ab`](https://github.com/tobymao/sqlglot/commit/9e8d3abedcffb1c267ed0e6a8332af3b52105d41) - Preserve struct-column parentheses for RisingWave dialect *(PR [#5376](https://github.com/tobymao/sqlglot/pull/5376) by [@MisterWheatley](https://github.com/MisterWheatley))*:

  Added dialect as argument to `simplify_parens` function  
  * style: Ran formatter and tests. Fixed type annotation for simplify_parens  
  * Fix: Make dialect in `simplify_parens` optional.  
  Co-authored-by: Jo <46752250+georgesittas@users.noreply.github.com>  
  * Fix(optimizer): Tweaks to make simple non-nested star expand pass unit test for RW  
  * Fix(optimizer): Added test for deep nested unpacking for BigQuery and RisingWave  
  * style: Ran formatting check  
  * fix: Remove unuses function from RisingWave dialect test  
  * docs: updated docstring of new _expand_struct_stars_risingwave internal function  
  * fix: apply suggestions from code review 2  
  Co-authored-by: Jo <46752250+georgesittas@users.noreply.github.com>  
  * fix(optimizer,risingwave): Ensure that struct star-expansion to the correct level for RisingWave  
  Updated logic for expanding (struct_col).* expressions in RisingWave to correctly handle the level of nesting.  
  Moved struct expansion tests to tests/fixtures/qualify_columns.sql on behest of maintainers.  
  ---------

- due to [`3223e63`](https://github.com/tobymao/sqlglot/commit/3223e6394fdd3f8e48c68bbb940b661ff8e76fd8) - cast datetimeoffset to datetime2 *(PR [#5385](https://github.com/tobymao/sqlglot/pull/5385) by [@mattiasthalen](https://github.com/mattiasthalen))*:

  cast datetimeoffset to datetime2 (#5385)

- due to [`06cea31`](https://github.com/tobymao/sqlglot/commit/06cea310bd9fd3a9a9fa0ba008596e878a430df8) - support KEY related locks *(PR [#5397](https://github.com/tobymao/sqlglot/pull/5397) by [@geooo109](https://github.com/geooo109))*:

  support KEY related locks (#5397)

- due to [`1014a67`](https://github.com/tobymao/sqlglot/commit/1014a6759b0917ef1bf5af0dbbdcca72214a8dea) - remove redundant todate in dayofweek closes [#5398](https://github.com/tobymao/sqlglot/pull/5398) *(PR [#5399](https://github.com/tobymao/sqlglot/pull/5399) by [@tobymao](https://github.com/tobymao))*:

  remove redundant todate in dayofweek closes #5398 (#5399)

- due to [`b2631ae`](https://github.com/tobymao/sqlglot/commit/b2631aec8d1bdb08decb201b6bd2ba5d927bb121) - annotate type for bigquery BIT_AND, BIT_OR, BIT_XOR, BIT_COUNT *(PR [#5405](https://github.com/tobymao/sqlglot/pull/5405) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery BIT_AND, BIT_OR, BIT_XOR, BIT_COUNT (#5405)

- due to [`5835b8d`](https://github.com/tobymao/sqlglot/commit/5835b8d6c7fe77d9645691bb88021af137ed0bac) - make bracket parsing aware of duckdb MAP func *(PR [#5423](https://github.com/tobymao/sqlglot/pull/5423) by [@geooo109](https://github.com/geooo109))*:

  make bracket parsing aware of duckdb MAP func (#5423)

- due to [`489dc5c`](https://github.com/tobymao/sqlglot/commit/489dc5c2f7506e0fe4de549384dd0f816e9fd12f) - parse and annotate type support for JSON_ARRAY *(PR [#5424](https://github.com/tobymao/sqlglot/pull/5424) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type support for JSON_ARRAY (#5424)

- due to [`0ed518c`](https://github.com/tobymao/sqlglot/commit/0ed518c67042002ee0af91bee0b9e7093c85f926) - annotate type for bigquery JSON_VALUE *(PR [#5427](https://github.com/tobymao/sqlglot/pull/5427) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery JSON_VALUE (#5427)

- due to [`6091617`](https://github.com/tobymao/sqlglot/commit/6091617067c263e3e834e579b37aa1c601b1ddc7) - annotate type for bigquery JSON_VALUE_ARRAY *(PR [#5428](https://github.com/tobymao/sqlglot/pull/5428) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery JSON_VALUE_ARRAY (#5428)

- due to [`631c851`](https://github.com/tobymao/sqlglot/commit/631c851cbbfbf55cb66a79c2549aeeb443fcab83) - parse and annotate type support for bigquery JSON_TYPE *(PR [#5430](https://github.com/tobymao/sqlglot/pull/5430) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type support for bigquery JSON_TYPE (#5430)

- due to [`6268f6f`](https://github.com/tobymao/sqlglot/commit/6268f6f39dda1ca5cf2ad0403e219b49c5c9593a) - add default precision to CHAR/VARCHAR create expressions *(PR [#5434](https://github.com/tobymao/sqlglot/pull/5434) by [@mattiasthalen](https://github.com/mattiasthalen))*:

  add default precision to CHAR/VARCHAR create expressions (#5434)

- due to [`8467bad`](https://github.com/tobymao/sqlglot/commit/8467bad405e27c842c989e71588adc39cf2383fc) - add parsing/generating for BigQuery `DECLARE` *(PR [#5442](https://github.com/tobymao/sqlglot/pull/5442) by [@plaflamme](https://github.com/plaflamme))*:

  add parsing/generating for BigQuery `DECLARE` (#5442)

- due to [`79c5c30`](https://github.com/tobymao/sqlglot/commit/79c5c30f3802c6959376b3b0f3c4d055a30b6b43) - transpile STRING_AGG *(PR [#5449](https://github.com/tobymao/sqlglot/pull/5449) by [@geooo109](https://github.com/geooo109))*:

  transpile STRING_AGG (#5449)

- due to [`190f8ab`](https://github.com/tobymao/sqlglot/commit/190f8abe3d3bbda09e2f945287398d2aa9d6a863) - improve BigQuery `UNNEST` transpilation *(PR [#5451](https://github.com/tobymao/sqlglot/pull/5451) by [@georgesittas](https://github.com/georgesittas))*:

  improve BigQuery `UNNEST` transpilation (#5451)

- due to [`3590e75`](https://github.com/tobymao/sqlglot/commit/3590e75c1df2d572e2fea664893dba5565a17e05) - support ? placeholder *(PR [#5455](https://github.com/tobymao/sqlglot/pull/5455) by [@geooo109](https://github.com/geooo109))*:

  support ? placeholder (#5455)

- due to [`cdbf595`](https://github.com/tobymao/sqlglot/commit/cdbf5953171c8d4c8e4a24262f278c6f7d74e057) - Wrap GET_PATH value with PARSE_JSON preemptively *(PR [#5458](https://github.com/tobymao/sqlglot/pull/5458) by [@VaggelisD](https://github.com/VaggelisD))*:

  Wrap GET_PATH value with PARSE_JSON preemptively (#5458)

- due to [`bee82f3`](https://github.com/tobymao/sqlglot/commit/bee82f37ac537780495ff408738d88871208517a) - Remove `UNKNOWN` type from `TRY_CAST` *(PR [#5466](https://github.com/tobymao/sqlglot/pull/5466) by [@VaggelisD](https://github.com/VaggelisD))*:

  Remove `UNKNOWN` type from `TRY_CAST` (#5466)


### :sparkles: New Features
- [`b368fba`](https://github.com/tobymao/sqlglot/commit/b368fba59b606e038d445b2ca2d8436e115af3d6) - **optimizer**: parse and annotate type for ASCII *(PR [#5377](https://github.com/tobymao/sqlglot/pull/5377) by [@geooo109](https://github.com/geooo109))*
- [`7f19b31`](https://github.com/tobymao/sqlglot/commit/7f19b31ebd7981e53a8f8ba343b4f3222fe160c7) - **optimizer**: annotate type for UNICODE *(PR [#5381](https://github.com/tobymao/sqlglot/pull/5381) by [@geooo109](https://github.com/geooo109))*
- [`f035bf0`](https://github.com/tobymao/sqlglot/commit/f035bf0eb582aa07d4ad79e0ed1958ce0d091ad9) - **dremio**: Add TIME_MAPPING for Dremio dialect *(PR [#5378](https://github.com/tobymao/sqlglot/pull/5378) by [@mateuszpoleski](https://github.com/mateuszpoleski))*
- [`31cfd0f`](https://github.com/tobymao/sqlglot/commit/31cfd0fc3309bc1080b7a2ba8d40b2aba5c098a3) - **exasol**: add to_date and refactored to_char functions with respect to time mapping *(PR [#5379](https://github.com/tobymao/sqlglot/pull/5379) by [@nnamdi16](https://github.com/nnamdi16))*
- [`bd3776e`](https://github.com/tobymao/sqlglot/commit/bd3776eaa26d40b44c4cebc2f3838b4055653548) - **doris**: add PROPERTIES_LOCATION mapping for Doris dialect *(PR [#5391](https://github.com/tobymao/sqlglot/pull/5391) by [@xinge-ji](https://github.com/xinge-ji))*
- [`7eaa67a`](https://github.com/tobymao/sqlglot/commit/7eaa67acb216501046c739f56839418b84f244c0) - **doris**: properly supported PROPERTIES and UNIQUE KEY table prop *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1e78163`](https://github.com/tobymao/sqlglot/commit/1e78163b829e910e7960c79e7ab118c07d1ecdc3) - **duckdb**: support column access via index *(PR [#5395](https://github.com/tobymao/sqlglot/pull/5395) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5392](https://github.com/tobymao/sqlglot/issues/5392) opened by [@tekumara](https://github.com/tekumara)*
- [`1014a67`](https://github.com/tobymao/sqlglot/commit/1014a6759b0917ef1bf5af0dbbdcca72214a8dea) - remove redundant todate in dayofweek closes [#5398](https://github.com/tobymao/sqlglot/pull/5398) *(PR [#5399](https://github.com/tobymao/sqlglot/pull/5399) by [@tobymao](https://github.com/tobymao))*
- [`be52f78`](https://github.com/tobymao/sqlglot/commit/be52f7866b03e436d103d9201d1a44c6632c643a) - **exasol**: add support for CONVERT_TZ function *(PR [#5401](https://github.com/tobymao/sqlglot/pull/5401) by [@nnamdi16](https://github.com/nnamdi16))*
- [`d637161`](https://github.com/tobymao/sqlglot/commit/d637161406faf623418f112162268bedb422213b) - **exasol**: add mapping to TIME_TO_STR in exasol dialect *(PR [#5403](https://github.com/tobymao/sqlglot/pull/5403) by [@nnamdi16](https://github.com/nnamdi16))*
- [`b2631ae`](https://github.com/tobymao/sqlglot/commit/b2631aec8d1bdb08decb201b6bd2ba5d927bb121) - **optimizer**: annotate type for bigquery BIT_AND, BIT_OR, BIT_XOR, BIT_COUNT *(PR [#5405](https://github.com/tobymao/sqlglot/pull/5405) by [@geooo109](https://github.com/geooo109))*
- [`b81ae62`](https://github.com/tobymao/sqlglot/commit/b81ae629bfb27760ddd832402a86dabe4e65072f) - **exasol**: map STR_TO_TIME to TO_DATE and *(PR [#5407](https://github.com/tobymao/sqlglot/pull/5407) by [@nnamdi16](https://github.com/nnamdi16))*
- [`c2fb9ab`](https://github.com/tobymao/sqlglot/commit/c2fb9abeb2f077f00278e46efd9573a3806cd218) - add `DateStrToTime` *(PR [#5409](https://github.com/tobymao/sqlglot/pull/5409) by [@betodealmeida](https://github.com/betodealmeida))*
- [`a95993a`](https://github.com/tobymao/sqlglot/commit/a95993ae4e8aa99969db059a534819a4f0b62b96) - **snowflake**: improve transpilation of queries with UNNEST sources *(PR [#5408](https://github.com/tobymao/sqlglot/pull/5408) by [@georgesittas](https://github.com/georgesittas))*
- [`7b69f54`](https://github.com/tobymao/sqlglot/commit/7b69f545bbcfeb1e1f2f3b7e0b9757cfd675e4a5) - **snowflake**: Support SEMANTIC_VIEW *(PR [#5414](https://github.com/tobymao/sqlglot/pull/5414) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5406](https://github.com/tobymao/sqlglot/issues/5406) opened by [@jkillian](https://github.com/jkillian)*
- [`7dba6f6`](https://github.com/tobymao/sqlglot/commit/7dba6f64d9a7945bbdef1b6e014d802014567a1e) - **exasol**: map AT TIME ZONE to CONVERT_TZ *(PR [#5416](https://github.com/tobymao/sqlglot/pull/5416) by [@nnamdi16](https://github.com/nnamdi16))*
- [`25f2c1b`](https://github.com/tobymao/sqlglot/commit/25f2c1bb18f9d073b128150566cb27c0c2da0865) - **postgres**: query placeholders *(PR [#5415](https://github.com/tobymao/sqlglot/pull/5415) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5412](https://github.com/tobymao/sqlglot/issues/5412) opened by [@aersam](https://github.com/aersam)*
- [`c309c87`](https://github.com/tobymao/sqlglot/commit/c309c8763a90bf0bce02e21f4088b38d85556cce) - **doris**: support range partitioning *(PR [#5402](https://github.com/tobymao/sqlglot/pull/5402) by [@xinge-ji](https://github.com/xinge-ji))*
- [`394d3a8`](https://github.com/tobymao/sqlglot/commit/394d3a81ef41d3052c0b0d6e48180c344b7db143) - **dremio**: Add support for DATE_ADD and DATE_SUB *(PR [#5411](https://github.com/tobymao/sqlglot/pull/5411) by [@mateuszpoleski](https://github.com/mateuszpoleski))*
- [`9cfac4f`](https://github.com/tobymao/sqlglot/commit/9cfac4fb04ce1fd038c3e8cbdb755cc24c052497) - **doris**: enhance partitioning support *(PR [#5421](https://github.com/tobymao/sqlglot/pull/5421) by [@xinge-ji](https://github.com/xinge-ji))*
- [`a018bea`](https://github.com/tobymao/sqlglot/commit/a018bea159261a3ad4ac082f29e30fe1153995b3) - **exasol**: mapped exp.CurrentUser to exasol CURRENT_USER *(PR [#5422](https://github.com/tobymao/sqlglot/pull/5422) by [@nnamdi16](https://github.com/nnamdi16))*
- [`489dc5c`](https://github.com/tobymao/sqlglot/commit/489dc5c2f7506e0fe4de549384dd0f816e9fd12f) - **optimizer**: parse and annotate type support for JSON_ARRAY *(PR [#5424](https://github.com/tobymao/sqlglot/pull/5424) by [@geooo109](https://github.com/geooo109))*
- [`0ed518c`](https://github.com/tobymao/sqlglot/commit/0ed518c67042002ee0af91bee0b9e7093c85f926) - **optimizer**: annotate type for bigquery JSON_VALUE *(PR [#5427](https://github.com/tobymao/sqlglot/pull/5427) by [@geooo109](https://github.com/geooo109))*
- [`6091617`](https://github.com/tobymao/sqlglot/commit/6091617067c263e3e834e579b37aa1c601b1ddc7) - **optimizer**: annotate type for bigquery JSON_VALUE_ARRAY *(PR [#5428](https://github.com/tobymao/sqlglot/pull/5428) by [@geooo109](https://github.com/geooo109))*
- [`631c851`](https://github.com/tobymao/sqlglot/commit/631c851cbbfbf55cb66a79c2549aeeb443fcab83) - **optimizer**: parse and annotate type support for bigquery JSON_TYPE *(PR [#5430](https://github.com/tobymao/sqlglot/pull/5430) by [@geooo109](https://github.com/geooo109))*
- [`732548f`](https://github.com/tobymao/sqlglot/commit/732548ff7a6792cfa38dba8b3b8a73a302532ae7) - **postgresql**: add support for table creation DDL that contains a primary key alongside the INCLUDE keyword *(PR [#5425](https://github.com/tobymao/sqlglot/pull/5425) by [@amosbiras](https://github.com/amosbiras))*
- [`9f887f1`](https://github.com/tobymao/sqlglot/commit/9f887f14d20cd493b4a0a4489649fc5b9f2ae7fd) - Add support for BETWEEN flags *(PR [#5435](https://github.com/tobymao/sqlglot/pull/5435) by [@mateuszpoleski](https://github.com/mateuszpoleski))*
- [`edef00a`](https://github.com/tobymao/sqlglot/commit/edef00af9b703ace76871b989d9b94d9c30dcafd) - **duckdb**: Add reset command for duckdb *(PR [#5448](https://github.com/tobymao/sqlglot/pull/5448) by [@themisvaltinos](https://github.com/themisvaltinos))*
- [`6268f6f`](https://github.com/tobymao/sqlglot/commit/6268f6f39dda1ca5cf2ad0403e219b49c5c9593a) - **tsql**: add default precision to CHAR/VARCHAR create expressions *(PR [#5434](https://github.com/tobymao/sqlglot/pull/5434) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`bbf26e9`](https://github.com/tobymao/sqlglot/commit/bbf26e9610bee341d4e6df12a031b05ff6b57861) - **mysql**: Add support for SELECT DISTINCTROW *(PR [#5446](https://github.com/tobymao/sqlglot/pull/5446) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5445](https://github.com/tobymao/sqlglot/issues/5445) opened by [@chenweida123](https://github.com/chenweida123)*
- [`8467bad`](https://github.com/tobymao/sqlglot/commit/8467bad405e27c842c989e71588adc39cf2383fc) - add parsing/generating for BigQuery `DECLARE` *(PR [#5442](https://github.com/tobymao/sqlglot/pull/5442) by [@plaflamme](https://github.com/plaflamme))*
- [`190f8ab`](https://github.com/tobymao/sqlglot/commit/190f8abe3d3bbda09e2f945287398d2aa9d6a863) - improve BigQuery `UNNEST` transpilation *(PR [#5451](https://github.com/tobymao/sqlglot/pull/5451) by [@georgesittas](https://github.com/georgesittas))*
- [`dbef44d`](https://github.com/tobymao/sqlglot/commit/dbef44db64d8c80e5000c55c981e0de89054e6eb) - **exasol**: mapped STRPOS to INSTR in exasol dialect *(PR [#5454](https://github.com/tobymao/sqlglot/pull/5454) by [@nnamdi16](https://github.com/nnamdi16))*
- [`010c34c`](https://github.com/tobymao/sqlglot/commit/010c34c1803df0223cf65263f2fb03b404e5141c) - support `DESC SEMANTIC VIEW` *(PR [#5452](https://github.com/tobymao/sqlglot/pull/5452) by [@betodealmeida](https://github.com/betodealmeida))*
- [`9795021`](https://github.com/tobymao/sqlglot/commit/9795021ff35bae17ff5a9ba7c5cdb46a75aab63b) - **exasol**: transformed column comments *(PR [#5464](https://github.com/tobymao/sqlglot/pull/5464) by [@nnamdi16](https://github.com/nnamdi16))*
- [`4c5b687`](https://github.com/tobymao/sqlglot/commit/4c5b68746dcede62ca9d1217bd428f50a1731e2c) - **snowflake**: transpile IS <boolean> (IS can only be used with NULL) *(PR [#5467](https://github.com/tobymao/sqlglot/pull/5467) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`d7ccb48`](https://github.com/tobymao/sqlglot/commit/d7ccb48e542c49258e31cc4df45f49beebc2e238) - **duckdb**: week/quarter support *(PR [#5374](https://github.com/tobymao/sqlglot/pull/5374) by [@eakmanrq](https://github.com/eakmanrq))*
- [`252469d`](https://github.com/tobymao/sqlglot/commit/252469d2d0ed221dbb2fde86043506ad15dbe7e5) - **snowflake**: transpile bigquery CURRENT_DATE with timezone *(PR [#5387](https://github.com/tobymao/sqlglot/pull/5387) by [@geooo109](https://github.com/geooo109))*
- [`7511853`](https://github.com/tobymao/sqlglot/commit/751185325caf838107ecb4e8f35ad77bf3cc9bf2) - **postgres**: add XML type *(PR [#5396](https://github.com/tobymao/sqlglot/pull/5396) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5393](https://github.com/tobymao/sqlglot/issues/5393) opened by [@aersam](https://github.com/aersam)*
- [`9e8d3ab`](https://github.com/tobymao/sqlglot/commit/9e8d3abedcffb1c267ed0e6a8332af3b52105d41) - **optimizer**: Preserve struct-column parentheses for RisingWave dialect *(PR [#5376](https://github.com/tobymao/sqlglot/pull/5376) by [@MisterWheatley](https://github.com/MisterWheatley))*
- [`3223e63`](https://github.com/tobymao/sqlglot/commit/3223e6394fdd3f8e48c68bbb940b661ff8e76fd8) - **fabric**: cast datetimeoffset to datetime2 *(PR [#5385](https://github.com/tobymao/sqlglot/pull/5385) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`12b49dd`](https://github.com/tobymao/sqlglot/commit/12b49dd800951a48ea8bc0f01d7c35340236f559) - remove equal sign from CREATE TABLE comment (doris, starrocks) *(PR [#5390](https://github.com/tobymao/sqlglot/pull/5390) by [@xinge-ji](https://github.com/xinge-ji))*
- [`06cea31`](https://github.com/tobymao/sqlglot/commit/06cea310bd9fd3a9a9fa0ba008596e878a430df8) - **postgres**: support KEY related locks *(PR [#5397](https://github.com/tobymao/sqlglot/pull/5397) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5394](https://github.com/tobymao/sqlglot/issues/5394) opened by [@aurimasandriusaitis](https://github.com/aurimasandriusaitis)*
- [`92d93a6`](https://github.com/tobymao/sqlglot/commit/92d93a624b41df8bb4628c1f2d0cbb8c7844c927) - **parser**: do not consume modifier prefixes in group parser, fixes [#5400](https://github.com/tobymao/sqlglot/pull/5400) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`ba0c801`](https://github.com/tobymao/sqlglot/commit/ba0c801e3dab8e08d4b5f7f73247ec6cfdc667e5) - **tsql**: change READ_ONLY to READONLY *(PR [#5410](https://github.com/tobymao/sqlglot/pull/5410) by [@CrispinStichartFNSB](https://github.com/CrispinStichartFNSB))*
- [`63da895`](https://github.com/tobymao/sqlglot/commit/63da89563fddc13ee7aec06ee36d8a0f74227ee1) - **risingwave**: Fix RisingWave dialect SQL for MAP datatype declaration *(PR [#5418](https://github.com/tobymao/sqlglot/pull/5418) by [@MisterWheatley](https://github.com/MisterWheatley))*
- [`edacae1`](https://github.com/tobymao/sqlglot/commit/edacae183fe26ea25bffe1bccd335bf57ed34ecb) - **snowflake**: transpile bigquery GENERATE_DATE_ARRAY with column access *(PR [#5388](https://github.com/tobymao/sqlglot/pull/5388) by [@geooo109](https://github.com/geooo109))*
- [`5835b8d`](https://github.com/tobymao/sqlglot/commit/5835b8d6c7fe77d9645691bb88021af137ed0bac) - **duckdb**: make bracket parsing aware of duckdb MAP func *(PR [#5423](https://github.com/tobymao/sqlglot/pull/5423) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5417](https://github.com/tobymao/sqlglot/issues/5417) opened by [@MisterWheatley](https://github.com/MisterWheatley)*
- [`5c59816`](https://github.com/tobymao/sqlglot/commit/5c59816f5572f8adb1de9c97f0007d19091910ec) - **snowflake**: ALTER TABLE ADD with multiple columns *(PR [#5431](https://github.com/tobymao/sqlglot/pull/5431) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5426](https://github.com/tobymao/sqlglot/issues/5426) opened by [@ca0904](https://github.com/ca0904)*
- [`9f860a0`](https://github.com/tobymao/sqlglot/commit/9f860a0ce47f74930efa1afcd86fe7668a40c239) - **snowflake**: ALTER TABLE ADD with IF NOT EXISTS *(PR [#5438](https://github.com/tobymao/sqlglot/pull/5438) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5432](https://github.com/tobymao/sqlglot/issues/5432) opened by [@ca0904](https://github.com/ca0904)*
- [`d7b3a26`](https://github.com/tobymao/sqlglot/commit/d7b3a261647e4ce675c84bbf72a33d320099fc01) - **postgres**: transpile duckdb LIST_HAS_ANY and LIST_CONTAINS *(PR [#5440](https://github.com/tobymao/sqlglot/pull/5440) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5437](https://github.com/tobymao/sqlglot/issues/5437) opened by [@aersam](https://github.com/aersam)*
- [`79c5c30`](https://github.com/tobymao/sqlglot/commit/79c5c30f3802c6959376b3b0f3c4d055a30b6b43) - **spark**: transpile STRING_AGG *(PR [#5449](https://github.com/tobymao/sqlglot/pull/5449) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5441](https://github.com/tobymao/sqlglot/issues/5441) opened by [@dxaen](https://github.com/dxaen)*
- [`3590e75`](https://github.com/tobymao/sqlglot/commit/3590e75c1df2d572e2fea664893dba5565a17e05) - **postgres**: support ? placeholder *(PR [#5455](https://github.com/tobymao/sqlglot/pull/5455) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5453](https://github.com/tobymao/sqlglot/issues/5453) opened by [@jkillian](https://github.com/jkillian)*
- [`cdbf595`](https://github.com/tobymao/sqlglot/commit/cdbf5953171c8d4c8e4a24262f278c6f7d74e057) - **snowflake**: Wrap GET_PATH value with PARSE_JSON preemptively *(PR [#5458](https://github.com/tobymao/sqlglot/pull/5458) by [@VaggelisD](https://github.com/VaggelisD))*
- [`8f16f52`](https://github.com/tobymao/sqlglot/commit/8f16f52859b66e3f8b30fff82f0c1679c7e37a25) - restore default `sql_names` for `DecodeCase` *(PR [#5465](https://github.com/tobymao/sqlglot/pull/5465) by [@georgesittas](https://github.com/georgesittas))*
- [`bee82f3`](https://github.com/tobymao/sqlglot/commit/bee82f37ac537780495ff408738d88871208517a) - **snowflake**: Remove `UNKNOWN` type from `TRY_CAST` *(PR [#5466](https://github.com/tobymao/sqlglot/pull/5466) by [@VaggelisD](https://github.com/VaggelisD))*

### :wrench: Chores
- [`71b1349`](https://github.com/tobymao/sqlglot/commit/71b1349a26d2b9839899900ef8fdfb1ebc3d68fd) - **postgres, hive**: use ASCII node instead of UNICODE node *(PR [#5380](https://github.com/tobymao/sqlglot/pull/5380) by [@geooo109](https://github.com/geooo109))*
- [`a5c2245`](https://github.com/tobymao/sqlglot/commit/a5c2245c3e30f5bc3f410edacf3a077ce99f4a80) - improve error msg for PIVOT with missing aggregation *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`59fd875`](https://github.com/tobymao/sqlglot/commit/59fd875cd4ee1c44f9ca20f701215ae64d669d60) - Refactor PRIMARY KEY ... INCLUDE handling *(PR [#5433](https://github.com/tobymao/sqlglot/pull/5433) by [@VaggelisD](https://github.com/VaggelisD))*
- [`e9bb3e8`](https://github.com/tobymao/sqlglot/commit/e9bb3e8ccb52c76ed77fc5e7d04cf75230b737fa) - Refactor DECLARE statement *(PR [#5450](https://github.com/tobymao/sqlglot/pull/5450) by [@VaggelisD](https://github.com/VaggelisD))*


## [v27.2.0] - 2025-07-22
### :boom: BREAKING CHANGES
- due to [`6268f6f`](https://github.com/tobymao/sqlglot/commit/6268f6f39dda1ca5cf2ad0403e219b49c5c9593a) - add default precision to CHAR/VARCHAR create expressions *(PR [#5434](https://github.com/tobymao/sqlglot/pull/5434) by [@mattiasthalen](https://github.com/mattiasthalen))*:

  add default precision to CHAR/VARCHAR create expressions (#5434)

- due to [`8467bad`](https://github.com/tobymao/sqlglot/commit/8467bad405e27c842c989e71588adc39cf2383fc) - add parsing/generating for BigQuery `DECLARE` *(PR [#5442](https://github.com/tobymao/sqlglot/pull/5442) by [@plaflamme](https://github.com/plaflamme))*:

  add parsing/generating for BigQuery `DECLARE` (#5442)

- due to [`79c5c30`](https://github.com/tobymao/sqlglot/commit/79c5c30f3802c6959376b3b0f3c4d055a30b6b43) - transpile STRING_AGG *(PR [#5449](https://github.com/tobymao/sqlglot/pull/5449) by [@geooo109](https://github.com/geooo109))*:

  transpile STRING_AGG (#5449)

- due to [`190f8ab`](https://github.com/tobymao/sqlglot/commit/190f8abe3d3bbda09e2f945287398d2aa9d6a863) - improve BigQuery `UNNEST` transpilation *(PR [#5451](https://github.com/tobymao/sqlglot/pull/5451) by [@georgesittas](https://github.com/georgesittas))*:

  improve BigQuery `UNNEST` transpilation (#5451)


### :sparkles: New Features
- [`732548f`](https://github.com/tobymao/sqlglot/commit/732548ff7a6792cfa38dba8b3b8a73a302532ae7) - **postgresql**: add support for table creation DDL that contains a primary key alongside the INCLUDE keyword *(PR [#5425](https://github.com/tobymao/sqlglot/pull/5425) by [@amosbiras](https://github.com/amosbiras))*
- [`9f887f1`](https://github.com/tobymao/sqlglot/commit/9f887f14d20cd493b4a0a4489649fc5b9f2ae7fd) - Add support for BETWEEN flags *(PR [#5435](https://github.com/tobymao/sqlglot/pull/5435) by [@mateuszpoleski](https://github.com/mateuszpoleski))*
- [`edef00a`](https://github.com/tobymao/sqlglot/commit/edef00af9b703ace76871b989d9b94d9c30dcafd) - **duckdb**: Add reset command for duckdb *(PR [#5448](https://github.com/tobymao/sqlglot/pull/5448) by [@themisvaltinos](https://github.com/themisvaltinos))*
- [`6268f6f`](https://github.com/tobymao/sqlglot/commit/6268f6f39dda1ca5cf2ad0403e219b49c5c9593a) - **tsql**: add default precision to CHAR/VARCHAR create expressions *(PR [#5434](https://github.com/tobymao/sqlglot/pull/5434) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`bbf26e9`](https://github.com/tobymao/sqlglot/commit/bbf26e9610bee341d4e6df12a031b05ff6b57861) - **mysql**: Add support for SELECT DISTINCTROW *(PR [#5446](https://github.com/tobymao/sqlglot/pull/5446) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5445](https://github.com/tobymao/sqlglot/issues/5445) opened by [@chenweida123](https://github.com/chenweida123)*
- [`8467bad`](https://github.com/tobymao/sqlglot/commit/8467bad405e27c842c989e71588adc39cf2383fc) - add parsing/generating for BigQuery `DECLARE` *(PR [#5442](https://github.com/tobymao/sqlglot/pull/5442) by [@plaflamme](https://github.com/plaflamme))*
- [`190f8ab`](https://github.com/tobymao/sqlglot/commit/190f8abe3d3bbda09e2f945287398d2aa9d6a863) - improve BigQuery `UNNEST` transpilation *(PR [#5451](https://github.com/tobymao/sqlglot/pull/5451) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`9f860a0`](https://github.com/tobymao/sqlglot/commit/9f860a0ce47f74930efa1afcd86fe7668a40c239) - **snowflake**: ALTER TABLE ADD with IF NOT EXISTS *(PR [#5438](https://github.com/tobymao/sqlglot/pull/5438) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5432](https://github.com/tobymao/sqlglot/issues/5432) opened by [@ca0904](https://github.com/ca0904)*
- [`d7b3a26`](https://github.com/tobymao/sqlglot/commit/d7b3a261647e4ce675c84bbf72a33d320099fc01) - **postgres**: transpile duckdb LIST_HAS_ANY and LIST_CONTAINS *(PR [#5440](https://github.com/tobymao/sqlglot/pull/5440) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5437](https://github.com/tobymao/sqlglot/issues/5437) opened by [@aersam](https://github.com/aersam)*
- [`79c5c30`](https://github.com/tobymao/sqlglot/commit/79c5c30f3802c6959376b3b0f3c4d055a30b6b43) - **spark**: transpile STRING_AGG *(PR [#5449](https://github.com/tobymao/sqlglot/pull/5449) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5441](https://github.com/tobymao/sqlglot/issues/5441) opened by [@dxaen](https://github.com/dxaen)*

### :wrench: Chores
- [`59fd875`](https://github.com/tobymao/sqlglot/commit/59fd875cd4ee1c44f9ca20f701215ae64d669d60) - Refactor PRIMARY KEY ... INCLUDE handling *(PR [#5433](https://github.com/tobymao/sqlglot/pull/5433) by [@VaggelisD](https://github.com/VaggelisD))*
- [`e9bb3e8`](https://github.com/tobymao/sqlglot/commit/e9bb3e8ccb52c76ed77fc5e7d04cf75230b737fa) - Refactor DECLARE statement *(PR [#5450](https://github.com/tobymao/sqlglot/pull/5450) by [@VaggelisD](https://github.com/VaggelisD))*


## [v27.1.0] - 2025-07-18
### :boom: BREAKING CHANGES
- due to [`5724538`](https://github.com/tobymao/sqlglot/commit/5724538f278b2178114b88850251afd7c3db0dda) - ARRAY_CONCAT type annotation *(PR [#5293](https://github.com/tobymao/sqlglot/pull/5293) by [@geooo109](https://github.com/geooo109))*:

  ARRAY_CONCAT type annotation (#5293)

- due to [`c103b23`](https://github.com/tobymao/sqlglot/commit/c103b2304dca552ac8cf6733156db8b59d3614f3) - add support for `SUBSTRING_INDEX` *(PR [#5296](https://github.com/tobymao/sqlglot/pull/5296) by [@ankur334](https://github.com/ankur334))*:

  add support for `SUBSTRING_INDEX` (#5296)

- due to [`a7bd823`](https://github.com/tobymao/sqlglot/commit/a7bd8234e0dd02abfe6fa56287e7bda14a549e5a) - annotate type of ARRAY_TO_STRING *(PR [#5301](https://github.com/tobymao/sqlglot/pull/5301) by [@geooo109](https://github.com/geooo109))*:

  annotate type of ARRAY_TO_STRING (#5301)

- due to [`6b42353`](https://github.com/tobymao/sqlglot/commit/6b4235340a2e432015c27b2aeadbdcb930bfa6b0) - annotate type of ARRAY_FIRST, ARRAY_LAST *(PR [#5303](https://github.com/tobymao/sqlglot/pull/5303) by [@geooo109](https://github.com/geooo109))*:

  annotate type of ARRAY_FIRST, ARRAY_LAST (#5303)

- due to [`db9b61e`](https://github.com/tobymao/sqlglot/commit/db9b61e4ecaa0600418eb90f637fb8b06b08c399) - parse, annotate type for ARRAY_REVERSE *(PR [#5306](https://github.com/tobymao/sqlglot/pull/5306) by [@geooo109](https://github.com/geooo109))*:

  parse, annotate type for ARRAY_REVERSE (#5306)

- due to [`5612a6d`](https://github.com/tobymao/sqlglot/commit/5612a6da6dee3545f3600db1e5b87c9450952eba) - add support for SPACE *(PR [#5308](https://github.com/tobymao/sqlglot/pull/5308) by [@ankur334](https://github.com/ankur334))*:

  add support for SPACE (#5308)

- due to [`8a2f65d`](https://github.com/tobymao/sqlglot/commit/8a2f65d6b2b68ad5ba45a5aed5e56c4dc0fea6fc) - parse and annotate type for ARRAY_SLICE *(PR [#5312](https://github.com/tobymao/sqlglot/pull/5312) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for ARRAY_SLICE (#5312)

- due to [`8d118ea`](https://github.com/tobymao/sqlglot/commit/8d118ead9c15e7b2b4b51b7cf93cab94e61c2625) - route statements to hive/trino depending on their type *(PR [#5314](https://github.com/tobymao/sqlglot/pull/5314) by [@georgesittas](https://github.com/georgesittas))*:

  route statements to hive/trino depending on their type (#5314)

- due to [`d2f7c41`](https://github.com/tobymao/sqlglot/commit/d2f7c41f9f30f4cf0c74782be9be0cc6e75565f3) - add TypeOf / toTypeName support *(PR [#5315](https://github.com/tobymao/sqlglot/pull/5315) by [@ankur334](https://github.com/ankur334))*:

  add TypeOf / toTypeName support (#5315)

- due to [`5a0f589`](https://github.com/tobymao/sqlglot/commit/5a0f589a0fdb6743c3be2f98b74a34780f51332b) - distinguish STORED AS from USING *(PR [#5320](https://github.com/tobymao/sqlglot/pull/5320) by [@geooo109](https://github.com/geooo109))*:

  distinguish STORED AS from USING (#5320)

- due to [`c4ca182`](https://github.com/tobymao/sqlglot/commit/c4ca182ad637b7a22b55d0ecf320c5a09ec5d56c) - annotate type for FROM_BASE64 *(PR [#5329](https://github.com/tobymao/sqlglot/pull/5329) by [@geooo109](https://github.com/geooo109))*:

  annotate type for FROM_BASE64 (#5329)

- due to [`7b72bbe`](https://github.com/tobymao/sqlglot/commit/7b72bbed3a0930e11ce4a0fdd9082de715326ac9) - annotate type for ANY_VALUE *(PR [#5331](https://github.com/tobymao/sqlglot/pull/5331) by [@geooo109](https://github.com/geooo109))*:

  annotate type for ANY_VALUE (#5331)

- due to [`c0d57e7`](https://github.com/tobymao/sqlglot/commit/c0d57e747bf5d2bed7ba2007ac2092d5797ee038) - annotate type for CHR *(PR [#5332](https://github.com/tobymao/sqlglot/pull/5332) by [@geooo109](https://github.com/geooo109))*:

  annotate type for CHR (#5332)

- due to [`d65b5c2`](https://github.com/tobymao/sqlglot/commit/d65b5c22c29416007cca0154fd35f1d4b5efc929) - annotate type for COUNTIF *(PR [#5334](https://github.com/tobymao/sqlglot/pull/5334) by [@geooo109](https://github.com/geooo109))*:

  annotate type for COUNTIF (#5334)

- due to [`521b705`](https://github.com/tobymao/sqlglot/commit/521b7053213df8577f609409af2552c2ff4fd8c9) - annotate type for GENERATE_ARRAY *(PR [#5335](https://github.com/tobymao/sqlglot/pull/5335) by [@geooo109](https://github.com/geooo109))*:

  annotate type for GENERATE_ARRAY (#5335)

- due to [`5fb26c5`](https://github.com/tobymao/sqlglot/commit/5fb26c58026018360f36a732394b612a3baac38b) - annotate type for INT64 *(PR [#5339](https://github.com/tobymao/sqlglot/pull/5339) by [@geooo109](https://github.com/geooo109))*:

  annotate type for INT64 (#5339)

- due to [`cff9b55`](https://github.com/tobymao/sqlglot/commit/cff9b55d70a3b85057e6385c93c0814eaa50f40b) - annotate type for LOGICAL_AND and LOGICAL_OR *(PR [#5340](https://github.com/tobymao/sqlglot/pull/5340) by [@geooo109](https://github.com/geooo109))*:

  annotate type for LOGICAL_AND and LOGICAL_OR (#5340)

- due to [`b94a6f9`](https://github.com/tobymao/sqlglot/commit/b94a6f9228aa730296c3152179bfbf3503521063) - annotate type for MAKE_INTERVAL *(PR [#5341](https://github.com/tobymao/sqlglot/pull/5341) by [@geooo109](https://github.com/geooo109))*:

  annotate type for MAKE_INTERVAL (#5341)

- due to [`2c9a7c6`](https://github.com/tobymao/sqlglot/commit/2c9a7c6f0b097a9e8514fc5e2af21c52f145920c) - annotate type for LAST_VALUE *(PR [#5336](https://github.com/tobymao/sqlglot/pull/5336) by [@geooo109](https://github.com/geooo109))*:

  annotate type for LAST_VALUE (#5336)

- due to [`d862a28`](https://github.com/tobymao/sqlglot/commit/d862a28b0a30f0c5774351f38a61f195120ad904) - annoate type for TO_BASE64 *(PR [#5342](https://github.com/tobymao/sqlglot/pull/5342) by [@geooo109](https://github.com/geooo109))*:

  annoate type for TO_BASE64 (#5342)

- due to [`85888c1`](https://github.com/tobymao/sqlglot/commit/85888c1b7cbbd0eee179d902a54fbd2a899cc16b) - annotate type for UNIX_DATE *(PR [#5343](https://github.com/tobymao/sqlglot/pull/5343) by [@geooo109](https://github.com/geooo109))*:

  annotate type for UNIX_DATE (#5343)

- due to [`8a214e0`](https://github.com/tobymao/sqlglot/commit/8a214e0859dfb715fcef0dd6b2d6392012b1f3fb) - annotate type for UNIX_SECONDS *(PR [#5344](https://github.com/tobymao/sqlglot/pull/5344) by [@geooo109](https://github.com/geooo109))*:

  annotate type for UNIX_SECONDS (#5344)

- due to [`625cb74`](https://github.com/tobymao/sqlglot/commit/625cb74b69e99ea1a707549366ea960d759848c9) - annotate type for STARTS_WITH *(PR [#5345](https://github.com/tobymao/sqlglot/pull/5345) by [@geooo109](https://github.com/geooo109))*:

  annotate type for STARTS_WITH (#5345)

- due to [`0337c4d`](https://github.com/tobymao/sqlglot/commit/0337c4d46e9e85d951fc9565a47e338106543711) - annotate type for SHA and SHA2 *(PR [#5346](https://github.com/tobymao/sqlglot/pull/5346) by [@geooo109](https://github.com/geooo109))*:

  annotate type for SHA and SHA2 (#5346)

- due to [`cc389fa`](https://github.com/tobymao/sqlglot/commit/cc389facb33f94a0d1f696f2ef9e92f298711894) - annotate type SHA1, SHA256, SHA512 for BigQuery *(PR [#5347](https://github.com/tobymao/sqlglot/pull/5347) by [@geooo109](https://github.com/geooo109))*:

  annotate type SHA1, SHA256, SHA512 for BigQuery (#5347)

- due to [`509b741`](https://github.com/tobymao/sqlglot/commit/509b74173f678842e7550c75c4d8d906c879fb12) - preserve multi-arg DECODE function instead of converting to CASE *(PR [#5352](https://github.com/tobymao/sqlglot/pull/5352) by [@georgesittas](https://github.com/georgesittas))*:

  preserve multi-arg DECODE function instead of converting to CASE (#5352)

- due to [`c1d3d61`](https://github.com/tobymao/sqlglot/commit/c1d3d61d00f00d2030107689d8704f7a488a80a7) - annotate type for CORR *(PR [#5364](https://github.com/tobymao/sqlglot/pull/5364) by [@geooo109](https://github.com/geooo109))*:

  annotate type for CORR (#5364)

- due to [`c1e8677`](https://github.com/tobymao/sqlglot/commit/c1e867767a006e774a2c200c10eb85b3fbd8a372) - annotate type for COVAR_POP *(PR [#5365](https://github.com/tobymao/sqlglot/pull/5365) by [@geooo109](https://github.com/geooo109))*:

  annotate type for COVAR_POP (#5365)

- due to [`e110ef4`](https://github.com/tobymao/sqlglot/commit/e110ef4f774e6ab8de6d4c86e5d306ab53fe895b) - annotate type for COVAR_SAMP *(PR [#5367](https://github.com/tobymao/sqlglot/pull/5367) by [@geooo109](https://github.com/geooo109))*:

  annotate type for COVAR_SAMP (#5367)

- due to [`5b59c16`](https://github.com/tobymao/sqlglot/commit/5b59c16528fb1904c64bef0ca6307bb6a95e5a2c) - annotate type for DATETIME *(PR [#5369](https://github.com/tobymao/sqlglot/pull/5369) by [@geooo109](https://github.com/geooo109))*:

  annotate type for DATETIME (#5369)

- due to [`47176ce`](https://github.com/tobymao/sqlglot/commit/47176ce6b9a4c1722f285034b08a6ae782129894) - annotate type for ENDS_WITH *(PR [#5370](https://github.com/tobymao/sqlglot/pull/5370) by [@geooo109](https://github.com/geooo109))*:

  annotate type for ENDS_WITH (#5370)

- due to [`2cce53d`](https://github.com/tobymao/sqlglot/commit/2cce53d59968f0a4bb3e9599ade93b0e6a140c68) - annotate type for LAG *(PR [#5371](https://github.com/tobymao/sqlglot/pull/5371) by [@geooo109](https://github.com/geooo109))*:

  annotate type for LAG (#5371)

- due to [`a3227de`](https://github.com/tobymao/sqlglot/commit/a3227de3fc57d559eb899dec08af01f85b470ce4) - improve transpilation of `ROUND(x, y)` to Postgres *(PR [#5368](https://github.com/tobymao/sqlglot/pull/5368) by [@blecourt-private](https://github.com/blecourt-private))*:

  improve transpilation of `ROUND(x, y)` to Postgres (#5368)

- due to [`d7ccb48`](https://github.com/tobymao/sqlglot/commit/d7ccb48e542c49258e31cc4df45f49beebc2e238) - week/quarter support *(PR [#5374](https://github.com/tobymao/sqlglot/pull/5374) by [@eakmanrq](https://github.com/eakmanrq))*:

  week/quarter support (#5374)

- due to [`b368fba`](https://github.com/tobymao/sqlglot/commit/b368fba59b606e038d445b2ca2d8436e115af3d6) - parse and annotate type for ASCII *(PR [#5377](https://github.com/tobymao/sqlglot/pull/5377) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for ASCII (#5377)

- due to [`7f19b31`](https://github.com/tobymao/sqlglot/commit/7f19b31ebd7981e53a8f8ba343b4f3222fe160c7) - annotate type for UNICODE *(PR [#5381](https://github.com/tobymao/sqlglot/pull/5381) by [@geooo109](https://github.com/geooo109))*:

  annotate type for UNICODE (#5381)

- due to [`9e8d3ab`](https://github.com/tobymao/sqlglot/commit/9e8d3abedcffb1c267ed0e6a8332af3b52105d41) - Preserve struct-column parentheses for RisingWave dialect *(PR [#5376](https://github.com/tobymao/sqlglot/pull/5376) by [@MisterWheatley](https://github.com/MisterWheatley))*:

  Added dialect as argument to `simplify_parens` function  
  * style: Ran formatter and tests. Fixed type annotation for simplify_parens  
  * Fix: Make dialect in `simplify_parens` optional.  
  Co-authored-by: Jo <46752250+georgesittas@users.noreply.github.com>  
  * Fix(optimizer): Tweaks to make simple non-nested star expand pass unit test for RW  
  * Fix(optimizer): Added test for deep nested unpacking for BigQuery and RisingWave  
  * style: Ran formatting check  
  * fix: Remove unuses function from RisingWave dialect test  
  * docs: updated docstring of new _expand_struct_stars_risingwave internal function  
  * fix: apply suggestions from code review 2  
  Co-authored-by: Jo <46752250+georgesittas@users.noreply.github.com>  
  * fix(optimizer,risingwave): Ensure that struct star-expansion to the correct level for RisingWave  
  Updated logic for expanding (struct_col).* expressions in RisingWave to correctly handle the level of nesting.  
  Moved struct expansion tests to tests/fixtures/qualify_columns.sql on behest of maintainers.  
  ---------

- due to [`3223e63`](https://github.com/tobymao/sqlglot/commit/3223e6394fdd3f8e48c68bbb940b661ff8e76fd8) - cast datetimeoffset to datetime2 *(PR [#5385](https://github.com/tobymao/sqlglot/pull/5385) by [@mattiasthalen](https://github.com/mattiasthalen))*:

  cast datetimeoffset to datetime2 (#5385)

- due to [`06cea31`](https://github.com/tobymao/sqlglot/commit/06cea310bd9fd3a9a9fa0ba008596e878a430df8) - support KEY related locks *(PR [#5397](https://github.com/tobymao/sqlglot/pull/5397) by [@geooo109](https://github.com/geooo109))*:

  support KEY related locks (#5397)

- due to [`1014a67`](https://github.com/tobymao/sqlglot/commit/1014a6759b0917ef1bf5af0dbbdcca72214a8dea) - remove redundant todate in dayofweek closes [#5398](https://github.com/tobymao/sqlglot/pull/5398) *(PR [#5399](https://github.com/tobymao/sqlglot/pull/5399) by [@tobymao](https://github.com/tobymao))*:

  remove redundant todate in dayofweek closes #5398 (#5399)

- due to [`b2631ae`](https://github.com/tobymao/sqlglot/commit/b2631aec8d1bdb08decb201b6bd2ba5d927bb121) - annotate type for bigquery BIT_AND, BIT_OR, BIT_XOR, BIT_COUNT *(PR [#5405](https://github.com/tobymao/sqlglot/pull/5405) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery BIT_AND, BIT_OR, BIT_XOR, BIT_COUNT (#5405)

- due to [`5835b8d`](https://github.com/tobymao/sqlglot/commit/5835b8d6c7fe77d9645691bb88021af137ed0bac) - make bracket parsing aware of duckdb MAP func *(PR [#5423](https://github.com/tobymao/sqlglot/pull/5423) by [@geooo109](https://github.com/geooo109))*:

  make bracket parsing aware of duckdb MAP func (#5423)

- due to [`489dc5c`](https://github.com/tobymao/sqlglot/commit/489dc5c2f7506e0fe4de549384dd0f816e9fd12f) - parse and annotate type support for JSON_ARRAY *(PR [#5424](https://github.com/tobymao/sqlglot/pull/5424) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type support for JSON_ARRAY (#5424)

- due to [`0ed518c`](https://github.com/tobymao/sqlglot/commit/0ed518c67042002ee0af91bee0b9e7093c85f926) - annotate type for bigquery JSON_VALUE *(PR [#5427](https://github.com/tobymao/sqlglot/pull/5427) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery JSON_VALUE (#5427)

- due to [`6091617`](https://github.com/tobymao/sqlglot/commit/6091617067c263e3e834e579b37aa1c601b1ddc7) - annotate type for bigquery JSON_VALUE_ARRAY *(PR [#5428](https://github.com/tobymao/sqlglot/pull/5428) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery JSON_VALUE_ARRAY (#5428)

- due to [`631c851`](https://github.com/tobymao/sqlglot/commit/631c851cbbfbf55cb66a79c2549aeeb443fcab83) - parse and annotate type support for bigquery JSON_TYPE *(PR [#5430](https://github.com/tobymao/sqlglot/pull/5430) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type support for bigquery JSON_TYPE (#5430)


### :sparkles: New Features
- [`ba7bf39`](https://github.com/tobymao/sqlglot/commit/ba7bf39966b519e11cde02a3c1f720598469e616) - **exasol**: implemented BIT_AND function with test *(PR [#5294](https://github.com/tobymao/sqlglot/pull/5294) by [@nnamdi16](https://github.com/nnamdi16))*
- [`fb4122e`](https://github.com/tobymao/sqlglot/commit/fb4122e80d1995bb87401e9ebe3749078c026a06) - **exasol**: add bitwiseOr function to exasol dialect *(PR [#5297](https://github.com/tobymao/sqlglot/pull/5297) by [@nnamdi16](https://github.com/nnamdi16))*
- [`c103b23`](https://github.com/tobymao/sqlglot/commit/c103b2304dca552ac8cf6733156db8b59d3614f3) - add support for `SUBSTRING_INDEX` *(PR [#5296](https://github.com/tobymao/sqlglot/pull/5296) by [@ankur334](https://github.com/ankur334))*
- [`4752f3a`](https://github.com/tobymao/sqlglot/commit/4752f3a6b715d8b6968c8f1f05f6ccdfb7351071) - **exasol**: added bit_xor built in exasol function to exasol dialect in sqlglot *(PR [#5298](https://github.com/tobymao/sqlglot/pull/5298) by [@nnamdi16](https://github.com/nnamdi16))*
- [`09bd610`](https://github.com/tobymao/sqlglot/commit/09bd6101de21ed86c9fd6df0f63e8bca2666dd81) - **parser**: annotate type of ARRAY_CONCAT_AGG *(PR [#5299](https://github.com/tobymao/sqlglot/pull/5299) by [@geooo109](https://github.com/geooo109))*
- [`ad0311a`](https://github.com/tobymao/sqlglot/commit/ad0311a7f8b0b3c5746c29d816b58578a892dd33) - **exasol**: added bit_not exasol built in function. *(PR [#5300](https://github.com/tobymao/sqlglot/pull/5300) by [@nnamdi16](https://github.com/nnamdi16))*
- [`a7bd823`](https://github.com/tobymao/sqlglot/commit/a7bd8234e0dd02abfe6fa56287e7bda14a549e5a) - **parser**: annotate type of ARRAY_TO_STRING *(PR [#5301](https://github.com/tobymao/sqlglot/pull/5301) by [@geooo109](https://github.com/geooo109))*
- [`2aa2182`](https://github.com/tobymao/sqlglot/commit/2aa21820f7d3a26cc4f47c1c757a9b7c97dd0382) - **exasol**: added BIT_LSHIFT built in function to exasol dialect *(PR [#5302](https://github.com/tobymao/sqlglot/pull/5302) by [@nnamdi16](https://github.com/nnamdi16))*
- [`c3d9ef2`](https://github.com/tobymao/sqlglot/commit/c3d9ef2cb2d004b57c64af4f3f1bac41f1890737) - **exasol**: added the bit_rshift built in exasol function *(PR [#5304](https://github.com/tobymao/sqlglot/pull/5304) by [@nnamdi16](https://github.com/nnamdi16))*
- [`6b42353`](https://github.com/tobymao/sqlglot/commit/6b4235340a2e432015c27b2aeadbdcb930bfa6b0) - **parser**: annotate type of ARRAY_FIRST, ARRAY_LAST *(PR [#5303](https://github.com/tobymao/sqlglot/pull/5303) by [@geooo109](https://github.com/geooo109))*
- [`f5b7cc6`](https://github.com/tobymao/sqlglot/commit/f5b7cc6d2f8d73bff4e42e242d3ad3db41d899cc) - **exasol**: added `EVERY` built in function *(PR [#5305](https://github.com/tobymao/sqlglot/pull/5305) by [@nnamdi16](https://github.com/nnamdi16))*
- [`d3f04d6`](https://github.com/tobymao/sqlglot/commit/d3f04d6766281ecb7ced9a5e812ab765d7b699be) - add Dremio dialect *(PR [#5277](https://github.com/tobymao/sqlglot/pull/5277) by [@mateuszpoleski](https://github.com/mateuszpoleski))*
- [`3d8e478`](https://github.com/tobymao/sqlglot/commit/3d8e478eac3df6a94c87cd610f96c5f19697a9bf) - **exasol**: added edit_distance built in function to exasol dialect *(PR [#5310](https://github.com/tobymao/sqlglot/pull/5310) by [@nnamdi16](https://github.com/nnamdi16))*
- [`db9b61e`](https://github.com/tobymao/sqlglot/commit/db9b61e4ecaa0600418eb90f637fb8b06b08c399) - **parser**: parse, annotate type for ARRAY_REVERSE *(PR [#5306](https://github.com/tobymao/sqlglot/pull/5306) by [@geooo109](https://github.com/geooo109))*
- [`5612a6d`](https://github.com/tobymao/sqlglot/commit/5612a6da6dee3545f3600db1e5b87c9450952eba) - add support for SPACE *(PR [#5308](https://github.com/tobymao/sqlglot/pull/5308) by [@ankur334](https://github.com/ankur334))*
- [`f148c9e`](https://github.com/tobymao/sqlglot/commit/f148c9e64ae0d4df96323271729fa6a6ca68a671) - **duckdb**: Transpile Spark's `exp.PosExplode` *(PR [#5311](https://github.com/tobymao/sqlglot/pull/5311) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5309](https://github.com/tobymao/sqlglot/issues/5309) opened by [@nimrodolev](https://github.com/nimrodolev)*
- [`179a278`](https://github.com/tobymao/sqlglot/commit/179a278c7fdbc29105e37f132e6f03e18627f769) - **exasol**: added the regexp_replace function *(PR [#5313](https://github.com/tobymao/sqlglot/pull/5313) by [@nnamdi16](https://github.com/nnamdi16))*
- [`8a2f65d`](https://github.com/tobymao/sqlglot/commit/8a2f65d6b2b68ad5ba45a5aed5e56c4dc0fea6fc) - **parser**: parse and annotate type for ARRAY_SLICE *(PR [#5312](https://github.com/tobymao/sqlglot/pull/5312) by [@geooo109](https://github.com/geooo109))*
- [`d2f7c41`](https://github.com/tobymao/sqlglot/commit/d2f7c41f9f30f4cf0c74782be9be0cc6e75565f3) - add TypeOf / toTypeName support *(PR [#5315](https://github.com/tobymao/sqlglot/pull/5315) by [@ankur334](https://github.com/ankur334))*
- [`950c15d`](https://github.com/tobymao/sqlglot/commit/950c15db5ff64b6f11036f8003db3e5b1fb3afc3) - **exasol**: add var_pop built in function to exasol dialect *(PR [#5328](https://github.com/tobymao/sqlglot/pull/5328) by [@nnamdi16](https://github.com/nnamdi16))*
- [`c4ca182`](https://github.com/tobymao/sqlglot/commit/c4ca182ad637b7a22b55d0ecf320c5a09ec5d56c) - **optimizer**: annotate type for FROM_BASE64 *(PR [#5329](https://github.com/tobymao/sqlglot/pull/5329) by [@geooo109](https://github.com/geooo109))*
- [`0992e99`](https://github.com/tobymao/sqlglot/commit/0992e99f99aeb4ecc97e6918a23b8fd524311ed9) - **exasol**: Add support  APPROXIMATE_COUNT_DISTINCT functions in exasol dialect *(PR [#5330](https://github.com/tobymao/sqlglot/pull/5330) by [@nnamdi16](https://github.com/nnamdi16))*
- [`7b72bbe`](https://github.com/tobymao/sqlglot/commit/7b72bbed3a0930e11ce4a0fdd9082de715326ac9) - **optimizer**: annotate type for ANY_VALUE *(PR [#5331](https://github.com/tobymao/sqlglot/pull/5331) by [@geooo109](https://github.com/geooo109))*
- [`c0d57e7`](https://github.com/tobymao/sqlglot/commit/c0d57e747bf5d2bed7ba2007ac2092d5797ee038) - **optimizer**: annotate type for CHR *(PR [#5332](https://github.com/tobymao/sqlglot/pull/5332) by [@geooo109](https://github.com/geooo109))*
- [`d65b5c2`](https://github.com/tobymao/sqlglot/commit/d65b5c22c29416007cca0154fd35f1d4b5efc929) - **optimizer**: annotate type for COUNTIF *(PR [#5334](https://github.com/tobymao/sqlglot/pull/5334) by [@geooo109](https://github.com/geooo109))*
- [`521b705`](https://github.com/tobymao/sqlglot/commit/521b7053213df8577f609409af2552c2ff4fd8c9) - **optimizer**: annotate type for GENERATE_ARRAY *(PR [#5335](https://github.com/tobymao/sqlglot/pull/5335) by [@geooo109](https://github.com/geooo109))*
- [`5fb26c5`](https://github.com/tobymao/sqlglot/commit/5fb26c58026018360f36a732394b612a3baac38b) - **optimizer**: annotate type for INT64 *(PR [#5339](https://github.com/tobymao/sqlglot/pull/5339) by [@geooo109](https://github.com/geooo109))*
- [`cff9b55`](https://github.com/tobymao/sqlglot/commit/cff9b55d70a3b85057e6385c93c0814eaa50f40b) - **optimizer**: annotate type for LOGICAL_AND and LOGICAL_OR *(PR [#5340](https://github.com/tobymao/sqlglot/pull/5340) by [@geooo109](https://github.com/geooo109))*
- [`b94a6f9`](https://github.com/tobymao/sqlglot/commit/b94a6f9228aa730296c3152179bfbf3503521063) - **optimizer**: annotate type for MAKE_INTERVAL *(PR [#5341](https://github.com/tobymao/sqlglot/pull/5341) by [@geooo109](https://github.com/geooo109))*
- [`2c9a7c6`](https://github.com/tobymao/sqlglot/commit/2c9a7c6f0b097a9e8514fc5e2af21c52f145920c) - **optimizer**: annotate type for LAST_VALUE *(PR [#5336](https://github.com/tobymao/sqlglot/pull/5336) by [@geooo109](https://github.com/geooo109))*
- [`d862a28`](https://github.com/tobymao/sqlglot/commit/d862a28b0a30f0c5774351f38a61f195120ad904) - **optimizer**: annoate type for TO_BASE64 *(PR [#5342](https://github.com/tobymao/sqlglot/pull/5342) by [@geooo109](https://github.com/geooo109))*
- [`85888c1`](https://github.com/tobymao/sqlglot/commit/85888c1b7cbbd0eee179d902a54fbd2a899cc16b) - **optimizer**: annotate type for UNIX_DATE *(PR [#5343](https://github.com/tobymao/sqlglot/pull/5343) by [@geooo109](https://github.com/geooo109))*
- [`8a214e0`](https://github.com/tobymao/sqlglot/commit/8a214e0859dfb715fcef0dd6b2d6392012b1f3fb) - **optimizer**: annotate type for UNIX_SECONDS *(PR [#5344](https://github.com/tobymao/sqlglot/pull/5344) by [@geooo109](https://github.com/geooo109))*
- [`625cb74`](https://github.com/tobymao/sqlglot/commit/625cb74b69e99ea1a707549366ea960d759848c9) - **optimizer**: annotate type for STARTS_WITH *(PR [#5345](https://github.com/tobymao/sqlglot/pull/5345) by [@geooo109](https://github.com/geooo109))*
- [`0337c4d`](https://github.com/tobymao/sqlglot/commit/0337c4d46e9e85d951fc9565a47e338106543711) - **optimizer**: annotate type for SHA and SHA2 *(PR [#5346](https://github.com/tobymao/sqlglot/pull/5346) by [@geooo109](https://github.com/geooo109))*
- [`835d9e6`](https://github.com/tobymao/sqlglot/commit/835d9e6c9ffc05de642113b566a1a4eb9cc38470) - add case-insensitive uppercase normalization strategy *(PR [#5349](https://github.com/tobymao/sqlglot/pull/5349) by [@georgesittas](https://github.com/georgesittas))*
- [`f80493e`](https://github.com/tobymao/sqlglot/commit/f80493efb168f600dc92da439d84e820f303e5aa) - **exasol**: Add TO_CHAR function support in exasol dialect *(PR [#5350](https://github.com/tobymao/sqlglot/pull/5350) by [@nnamdi16](https://github.com/nnamdi16))*
- [`cea6a24`](https://github.com/tobymao/sqlglot/commit/cea6a240292d6e31bc73179d433835483e65747a) - **teradata**: add FORMAT phrase parsing *(PR [#5348](https://github.com/tobymao/sqlglot/pull/5348) by [@readjfb](https://github.com/readjfb))*
- [`eae64e1`](https://github.com/tobymao/sqlglot/commit/eae64e1629a276bf3885749991869b6c6dea8a8b) - **duckdb**: support new lambda syntax *(PR [#5359](https://github.com/tobymao/sqlglot/pull/5359) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5357](https://github.com/tobymao/sqlglot/issues/5357) opened by [@aersam](https://github.com/aersam)*
- [`e77991d`](https://github.com/tobymao/sqlglot/commit/e77991d92fad56014ba2778c71e5e446d4dd090e) - **duckdb**: Add support for SET VARIABLE *(PR [#5360](https://github.com/tobymao/sqlglot/pull/5360) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5356](https://github.com/tobymao/sqlglot/issues/5356) opened by [@aersam](https://github.com/aersam)*
- [`c1d3d61`](https://github.com/tobymao/sqlglot/commit/c1d3d61d00f00d2030107689d8704f7a488a80a7) - **optimizer**: annotate type for CORR *(PR [#5364](https://github.com/tobymao/sqlglot/pull/5364) by [@geooo109](https://github.com/geooo109))*
- [`c1e8677`](https://github.com/tobymao/sqlglot/commit/c1e867767a006e774a2c200c10eb85b3fbd8a372) - **optimizer**: annotate type for COVAR_POP *(PR [#5365](https://github.com/tobymao/sqlglot/pull/5365) by [@geooo109](https://github.com/geooo109))*
- [`e110ef4`](https://github.com/tobymao/sqlglot/commit/e110ef4f774e6ab8de6d4c86e5d306ab53fe895b) - **optimizer**: annotate type for COVAR_SAMP *(PR [#5367](https://github.com/tobymao/sqlglot/pull/5367) by [@geooo109](https://github.com/geooo109))*
- [`5b59c16`](https://github.com/tobymao/sqlglot/commit/5b59c16528fb1904c64bef0ca6307bb6a95e5a2c) - **optimizer**: annotate type for DATETIME *(PR [#5369](https://github.com/tobymao/sqlglot/pull/5369) by [@geooo109](https://github.com/geooo109))*
- [`47176ce`](https://github.com/tobymao/sqlglot/commit/47176ce6b9a4c1722f285034b08a6ae782129894) - **optimizer**: annotate type for ENDS_WITH *(PR [#5370](https://github.com/tobymao/sqlglot/pull/5370) by [@geooo109](https://github.com/geooo109))*
- [`1fd757e`](https://github.com/tobymao/sqlglot/commit/1fd757e6279315f00e719974613313a6e43dfe55) - **fabric**: Ensure TIMESTAMPTZ is used with AT TIME ZONE *(PR [#5362](https://github.com/tobymao/sqlglot/pull/5362) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`2cce53d`](https://github.com/tobymao/sqlglot/commit/2cce53d59968f0a4bb3e9599ade93b0e6a140c68) - **optimizer**: annotate type for LAG *(PR [#5371](https://github.com/tobymao/sqlglot/pull/5371) by [@geooo109](https://github.com/geooo109))*
- [`a3227de`](https://github.com/tobymao/sqlglot/commit/a3227de3fc57d559eb899dec08af01f85b470ce4) - improve transpilation of `ROUND(x, y)` to Postgres *(PR [#5368](https://github.com/tobymao/sqlglot/pull/5368) by [@blecourt-private](https://github.com/blecourt-private))*
  - :arrow_lower_right: *addresses issue [#5366](https://github.com/tobymao/sqlglot/issues/5366) opened by [@blecourt-private](https://github.com/blecourt-private)*
- [`b368fba`](https://github.com/tobymao/sqlglot/commit/b368fba59b606e038d445b2ca2d8436e115af3d6) - **optimizer**: parse and annotate type for ASCII *(PR [#5377](https://github.com/tobymao/sqlglot/pull/5377) by [@geooo109](https://github.com/geooo109))*
- [`7f19b31`](https://github.com/tobymao/sqlglot/commit/7f19b31ebd7981e53a8f8ba343b4f3222fe160c7) - **optimizer**: annotate type for UNICODE *(PR [#5381](https://github.com/tobymao/sqlglot/pull/5381) by [@geooo109](https://github.com/geooo109))*
- [`f035bf0`](https://github.com/tobymao/sqlglot/commit/f035bf0eb582aa07d4ad79e0ed1958ce0d091ad9) - **dremio**: Add TIME_MAPPING for Dremio dialect *(PR [#5378](https://github.com/tobymao/sqlglot/pull/5378) by [@mateuszpoleski](https://github.com/mateuszpoleski))*
- [`31cfd0f`](https://github.com/tobymao/sqlglot/commit/31cfd0fc3309bc1080b7a2ba8d40b2aba5c098a3) - **exasol**: add to_date and refactored to_char functions with respect to time mapping *(PR [#5379](https://github.com/tobymao/sqlglot/pull/5379) by [@nnamdi16](https://github.com/nnamdi16))*
- [`bd3776e`](https://github.com/tobymao/sqlglot/commit/bd3776eaa26d40b44c4cebc2f3838b4055653548) - **doris**: add PROPERTIES_LOCATION mapping for Doris dialect *(PR [#5391](https://github.com/tobymao/sqlglot/pull/5391) by [@xinge-ji](https://github.com/xinge-ji))*
- [`7eaa67a`](https://github.com/tobymao/sqlglot/commit/7eaa67acb216501046c739f56839418b84f244c0) - **doris**: properly supported PROPERTIES and UNIQUE KEY table prop *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1e78163`](https://github.com/tobymao/sqlglot/commit/1e78163b829e910e7960c79e7ab118c07d1ecdc3) - **duckdb**: support column access via index *(PR [#5395](https://github.com/tobymao/sqlglot/pull/5395) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5392](https://github.com/tobymao/sqlglot/issues/5392) opened by [@tekumara](https://github.com/tekumara)*
- [`1014a67`](https://github.com/tobymao/sqlglot/commit/1014a6759b0917ef1bf5af0dbbdcca72214a8dea) - remove redundant todate in dayofweek closes [#5398](https://github.com/tobymao/sqlglot/pull/5398) *(PR [#5399](https://github.com/tobymao/sqlglot/pull/5399) by [@tobymao](https://github.com/tobymao))*
- [`be52f78`](https://github.com/tobymao/sqlglot/commit/be52f7866b03e436d103d9201d1a44c6632c643a) - **exasol**: add support for CONVERT_TZ function *(PR [#5401](https://github.com/tobymao/sqlglot/pull/5401) by [@nnamdi16](https://github.com/nnamdi16))*
- [`d637161`](https://github.com/tobymao/sqlglot/commit/d637161406faf623418f112162268bedb422213b) - **exasol**: add mapping to TIME_TO_STR in exasol dialect *(PR [#5403](https://github.com/tobymao/sqlglot/pull/5403) by [@nnamdi16](https://github.com/nnamdi16))*
- [`b2631ae`](https://github.com/tobymao/sqlglot/commit/b2631aec8d1bdb08decb201b6bd2ba5d927bb121) - **optimizer**: annotate type for bigquery BIT_AND, BIT_OR, BIT_XOR, BIT_COUNT *(PR [#5405](https://github.com/tobymao/sqlglot/pull/5405) by [@geooo109](https://github.com/geooo109))*
- [`b81ae62`](https://github.com/tobymao/sqlglot/commit/b81ae629bfb27760ddd832402a86dabe4e65072f) - **exasol**: map STR_TO_TIME to TO_DATE and *(PR [#5407](https://github.com/tobymao/sqlglot/pull/5407) by [@nnamdi16](https://github.com/nnamdi16))*
- [`c2fb9ab`](https://github.com/tobymao/sqlglot/commit/c2fb9abeb2f077f00278e46efd9573a3806cd218) - add `DateStrToTime` *(PR [#5409](https://github.com/tobymao/sqlglot/pull/5409) by [@betodealmeida](https://github.com/betodealmeida))*
- [`a95993a`](https://github.com/tobymao/sqlglot/commit/a95993ae4e8aa99969db059a534819a4f0b62b96) - **snowflake**: improve transpilation of queries with UNNEST sources *(PR [#5408](https://github.com/tobymao/sqlglot/pull/5408) by [@georgesittas](https://github.com/georgesittas))*
- [`7b69f54`](https://github.com/tobymao/sqlglot/commit/7b69f545bbcfeb1e1f2f3b7e0b9757cfd675e4a5) - **snowflake**: Support SEMANTIC_VIEW *(PR [#5414](https://github.com/tobymao/sqlglot/pull/5414) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5406](https://github.com/tobymao/sqlglot/issues/5406) opened by [@jkillian](https://github.com/jkillian)*
- [`7dba6f6`](https://github.com/tobymao/sqlglot/commit/7dba6f64d9a7945bbdef1b6e014d802014567a1e) - **exasol**: map AT TIME ZONE to CONVERT_TZ *(PR [#5416](https://github.com/tobymao/sqlglot/pull/5416) by [@nnamdi16](https://github.com/nnamdi16))*
- [`25f2c1b`](https://github.com/tobymao/sqlglot/commit/25f2c1bb18f9d073b128150566cb27c0c2da0865) - **postgres**: query placeholders *(PR [#5415](https://github.com/tobymao/sqlglot/pull/5415) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5412](https://github.com/tobymao/sqlglot/issues/5412) opened by [@aersam](https://github.com/aersam)*
- [`c309c87`](https://github.com/tobymao/sqlglot/commit/c309c8763a90bf0bce02e21f4088b38d85556cce) - **doris**: support range partitioning *(PR [#5402](https://github.com/tobymao/sqlglot/pull/5402) by [@xinge-ji](https://github.com/xinge-ji))*
- [`394d3a8`](https://github.com/tobymao/sqlglot/commit/394d3a81ef41d3052c0b0d6e48180c344b7db143) - **dremio**: Add support for DATE_ADD and DATE_SUB *(PR [#5411](https://github.com/tobymao/sqlglot/pull/5411) by [@mateuszpoleski](https://github.com/mateuszpoleski))*
- [`9cfac4f`](https://github.com/tobymao/sqlglot/commit/9cfac4fb04ce1fd038c3e8cbdb755cc24c052497) - **doris**: enhance partitioning support *(PR [#5421](https://github.com/tobymao/sqlglot/pull/5421) by [@xinge-ji](https://github.com/xinge-ji))*
- [`a018bea`](https://github.com/tobymao/sqlglot/commit/a018bea159261a3ad4ac082f29e30fe1153995b3) - **exasol**: mapped exp.CurrentUser to exasol CURRENT_USER *(PR [#5422](https://github.com/tobymao/sqlglot/pull/5422) by [@nnamdi16](https://github.com/nnamdi16))*
- [`489dc5c`](https://github.com/tobymao/sqlglot/commit/489dc5c2f7506e0fe4de549384dd0f816e9fd12f) - **optimizer**: parse and annotate type support for JSON_ARRAY *(PR [#5424](https://github.com/tobymao/sqlglot/pull/5424) by [@geooo109](https://github.com/geooo109))*
- [`0ed518c`](https://github.com/tobymao/sqlglot/commit/0ed518c67042002ee0af91bee0b9e7093c85f926) - **optimizer**: annotate type for bigquery JSON_VALUE *(PR [#5427](https://github.com/tobymao/sqlglot/pull/5427) by [@geooo109](https://github.com/geooo109))*
- [`6091617`](https://github.com/tobymao/sqlglot/commit/6091617067c263e3e834e579b37aa1c601b1ddc7) - **optimizer**: annotate type for bigquery JSON_VALUE_ARRAY *(PR [#5428](https://github.com/tobymao/sqlglot/pull/5428) by [@geooo109](https://github.com/geooo109))*
- [`631c851`](https://github.com/tobymao/sqlglot/commit/631c851cbbfbf55cb66a79c2549aeeb443fcab83) - **optimizer**: parse and annotate type support for bigquery JSON_TYPE *(PR [#5430](https://github.com/tobymao/sqlglot/pull/5430) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`5724538`](https://github.com/tobymao/sqlglot/commit/5724538f278b2178114b88850251afd7c3db0dda) - **bigquery**: ARRAY_CONCAT type annotation *(PR [#5293](https://github.com/tobymao/sqlglot/pull/5293) by [@geooo109](https://github.com/geooo109))*
- [`0a6afcd`](https://github.com/tobymao/sqlglot/commit/0a6afcd90c663aaef9b385fc12ccd19dbf6388cc) - use re-entrant lock in dialects/__init__ to avoid deadlocks *(PR [#5322](https://github.com/tobymao/sqlglot/pull/5322) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5321](https://github.com/tobymao/sqlglot/issues/5321) opened by [@jc-5s](https://github.com/jc-5s)*
- [`599ca81`](https://github.com/tobymao/sqlglot/commit/599ca8101f48805098cbdf808ac2923a8246066b) - **parser**: avoid CTE values ALIAS gen, when ALIAS exists *(PR [#5323](https://github.com/tobymao/sqlglot/pull/5323) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5318](https://github.com/tobymao/sqlglot/issues/5318) opened by [@ankur334](https://github.com/ankur334)*
- [`5a0f589`](https://github.com/tobymao/sqlglot/commit/5a0f589a0fdb6743c3be2f98b74a34780f51332b) - **spark**: distinguish STORED AS from USING *(PR [#5320](https://github.com/tobymao/sqlglot/pull/5320) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5317](https://github.com/tobymao/sqlglot/issues/5317) opened by [@cosinequanon](https://github.com/cosinequanon)*
- [`cbc79c2`](https://github.com/tobymao/sqlglot/commit/cbc79c2a47c46370de0378b8bae61f4f3c17ca82) - preserve ORDER BY comments fixes [#5326](https://github.com/tobymao/sqlglot/pull/5326) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fa69583`](https://github.com/tobymao/sqlglot/commit/fa69583d8b4f5801d05c21a92b43dea272a3ef49) - **optimizer**: avoid qualifying CTE *(PR [#5327](https://github.com/tobymao/sqlglot/pull/5327) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5319](https://github.com/tobymao/sqlglot/issues/5319) opened by [@naamamaoz](https://github.com/naamamaoz)*
- [`29cce43`](https://github.com/tobymao/sqlglot/commit/29cce43e72451feeb8788ac2660658075bf59093) - comment lost before GROUP, JOIN and HAVING *(PR [#5338](https://github.com/tobymao/sqlglot/pull/5338) by [@chiiips](https://github.com/chiiips))*
- [`509b741`](https://github.com/tobymao/sqlglot/commit/509b74173f678842e7550c75c4d8d906c879fb12) - preserve multi-arg DECODE function instead of converting to CASE *(PR [#5352](https://github.com/tobymao/sqlglot/pull/5352) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5351](https://github.com/tobymao/sqlglot/issues/5351) opened by [@kentmaxwell](https://github.com/kentmaxwell)*
- [`188d446`](https://github.com/tobymao/sqlglot/commit/188d446ca65125c63bbfff96d15d91078deb6b4a) - **optimizer**: downstream column for PIVOT *(PR [#5363](https://github.com/tobymao/sqlglot/pull/5363) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5354](https://github.com/tobymao/sqlglot/issues/5354) opened by [@suresh-summation](https://github.com/suresh-summation)*
- [`d7ccb48`](https://github.com/tobymao/sqlglot/commit/d7ccb48e542c49258e31cc4df45f49beebc2e238) - **duckdb**: week/quarter support *(PR [#5374](https://github.com/tobymao/sqlglot/pull/5374) by [@eakmanrq](https://github.com/eakmanrq))*
- [`252469d`](https://github.com/tobymao/sqlglot/commit/252469d2d0ed221dbb2fde86043506ad15dbe7e5) - **snowflake**: transpile bigquery CURRENT_DATE with timezone *(PR [#5387](https://github.com/tobymao/sqlglot/pull/5387) by [@geooo109](https://github.com/geooo109))*
- [`7511853`](https://github.com/tobymao/sqlglot/commit/751185325caf838107ecb4e8f35ad77bf3cc9bf2) - **postgres**: add XML type *(PR [#5396](https://github.com/tobymao/sqlglot/pull/5396) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5393](https://github.com/tobymao/sqlglot/issues/5393) opened by [@aersam](https://github.com/aersam)*
- [`9e8d3ab`](https://github.com/tobymao/sqlglot/commit/9e8d3abedcffb1c267ed0e6a8332af3b52105d41) - **optimizer**: Preserve struct-column parentheses for RisingWave dialect *(PR [#5376](https://github.com/tobymao/sqlglot/pull/5376) by [@MisterWheatley](https://github.com/MisterWheatley))*
- [`3223e63`](https://github.com/tobymao/sqlglot/commit/3223e6394fdd3f8e48c68bbb940b661ff8e76fd8) - **fabric**: cast datetimeoffset to datetime2 *(PR [#5385](https://github.com/tobymao/sqlglot/pull/5385) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`12b49dd`](https://github.com/tobymao/sqlglot/commit/12b49dd800951a48ea8bc0f01d7c35340236f559) - remove equal sign from CREATE TABLE comment (doris, starrocks) *(PR [#5390](https://github.com/tobymao/sqlglot/pull/5390) by [@xinge-ji](https://github.com/xinge-ji))*
- [`06cea31`](https://github.com/tobymao/sqlglot/commit/06cea310bd9fd3a9a9fa0ba008596e878a430df8) - **postgres**: support KEY related locks *(PR [#5397](https://github.com/tobymao/sqlglot/pull/5397) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5394](https://github.com/tobymao/sqlglot/issues/5394) opened by [@aurimasandriusaitis](https://github.com/aurimasandriusaitis)*
- [`92d93a6`](https://github.com/tobymao/sqlglot/commit/92d93a624b41df8bb4628c1f2d0cbb8c7844c927) - **parser**: do not consume modifier prefixes in group parser, fixes [#5400](https://github.com/tobymao/sqlglot/pull/5400) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`ba0c801`](https://github.com/tobymao/sqlglot/commit/ba0c801e3dab8e08d4b5f7f73247ec6cfdc667e5) - **tsql**: change READ_ONLY to READONLY *(PR [#5410](https://github.com/tobymao/sqlglot/pull/5410) by [@CrispinStichartFNSB](https://github.com/CrispinStichartFNSB))*
- [`63da895`](https://github.com/tobymao/sqlglot/commit/63da89563fddc13ee7aec06ee36d8a0f74227ee1) - **risingwave**: Fix RisingWave dialect SQL for MAP datatype declaration *(PR [#5418](https://github.com/tobymao/sqlglot/pull/5418) by [@MisterWheatley](https://github.com/MisterWheatley))*
- [`edacae1`](https://github.com/tobymao/sqlglot/commit/edacae183fe26ea25bffe1bccd335bf57ed34ecb) - **snowflake**: transpile bigquery GENERATE_DATE_ARRAY with column access *(PR [#5388](https://github.com/tobymao/sqlglot/pull/5388) by [@geooo109](https://github.com/geooo109))*
- [`5835b8d`](https://github.com/tobymao/sqlglot/commit/5835b8d6c7fe77d9645691bb88021af137ed0bac) - **duckdb**: make bracket parsing aware of duckdb MAP func *(PR [#5423](https://github.com/tobymao/sqlglot/pull/5423) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5417](https://github.com/tobymao/sqlglot/issues/5417) opened by [@MisterWheatley](https://github.com/MisterWheatley)*
- [`5c59816`](https://github.com/tobymao/sqlglot/commit/5c59816f5572f8adb1de9c97f0007d19091910ec) - **snowflake**: ALTER TABLE ADD with multiple columns *(PR [#5431](https://github.com/tobymao/sqlglot/pull/5431) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5426](https://github.com/tobymao/sqlglot/issues/5426) opened by [@ca0904](https://github.com/ca0904)*

### :recycle: Refactors
- [`8d118ea`](https://github.com/tobymao/sqlglot/commit/8d118ead9c15e7b2b4b51b7cf93cab94e61c2625) - **athena**: route statements to hive/trino depending on their type *(PR [#5314](https://github.com/tobymao/sqlglot/pull/5314) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5267](https://github.com/tobymao/sqlglot/issues/5267) opened by [@cpcloud](https://github.com/cpcloud)*

### :wrench: Chores
- [`cc389fa`](https://github.com/tobymao/sqlglot/commit/cc389facb33f94a0d1f696f2ef9e92f298711894) - **optimizer**: annotate type SHA1, SHA256, SHA512 for BigQuery *(PR [#5347](https://github.com/tobymao/sqlglot/pull/5347) by [@geooo109](https://github.com/geooo109))*
- [`194850a`](https://github.com/tobymao/sqlglot/commit/194850a52497300a8f1d47f2306b67cdd11ffab6) - **exasol**: clean up TO_CHAR *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1abd461`](https://github.com/tobymao/sqlglot/commit/1abd461295830807c52f24d25ac6938095f54831) - bump min. supported version to python 3.9 *(PR [#5353](https://github.com/tobymao/sqlglot/pull/5353) by [@georgesittas](https://github.com/georgesittas))*
- [`71b1349`](https://github.com/tobymao/sqlglot/commit/71b1349a26d2b9839899900ef8fdfb1ebc3d68fd) - **postgres, hive**: use ASCII node instead of UNICODE node *(PR [#5380](https://github.com/tobymao/sqlglot/pull/5380) by [@geooo109](https://github.com/geooo109))*
- [`a5c2245`](https://github.com/tobymao/sqlglot/commit/a5c2245c3e30f5bc3f410edacf3a077ce99f4a80) - improve error msg for PIVOT with missing aggregation *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v27.0.0] - 2025-07-07
### :boom: BREAKING CHANGES
- due to [`f2bf000`](https://github.com/tobymao/sqlglot/commit/f2bf000a410fb18531bb90ef1d767baf0e8bce7a) - avoid creating new alias for qualifying unpivot *(PR [#5121](https://github.com/tobymao/sqlglot/pull/5121) by [@geooo109](https://github.com/geooo109))*:

  avoid creating new alias for qualifying unpivot (#5121)

- due to [`a126ce8`](https://github.com/tobymao/sqlglot/commit/a126ce8a25287cf3531d815035fa3d567dc772fb) - make coalesce simplification optional, skip by default *(PR [#5123](https://github.com/tobymao/sqlglot/pull/5123) by [@barakalon](https://github.com/barakalon))*:

  make coalesce simplification optional, skip by default (#5123)

- due to [`6910744`](https://github.com/tobymao/sqlglot/commit/6910744e6260793b3f9190782cf60fbbd9adcd38) - update py03 version *(PR [#5136](https://github.com/tobymao/sqlglot/pull/5136) by [@benfdking](https://github.com/benfdking))*:

  update py03 version (#5136)

- due to [`a56deab`](https://github.com/tobymao/sqlglot/commit/a56deabc2b9543209fb5e41f19c3bef89177a577) - bump sqlglotrs to 0.5.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.5.0

- due to [`c484ca3`](https://github.com/tobymao/sqlglot/commit/c484ca39bad750a96b62e2edae85612cac66ba30) - recognize ARRAY_CONCAT_AGG as an aggregate function *(PR [#5141](https://github.com/tobymao/sqlglot/pull/5141) by [@georgesittas](https://github.com/georgesittas))*:

  recognize ARRAY_CONCAT_AGG as an aggregate function (#5141)

- due to [`72ce404`](https://github.com/tobymao/sqlglot/commit/72ce40405625239a0d6763d502e5af8b12abfe9b) - Refactor ALTER TABLE ADD parsing *(PR [#5144](https://github.com/tobymao/sqlglot/pull/5144) by [@VaggelisD](https://github.com/VaggelisD))*:

  Refactor ALTER TABLE ADD parsing (#5144)

- due to [`e73ddb7`](https://github.com/tobymao/sqlglot/commit/e73ddb733b7f120ae74054e6d4dc7d458f59ac50) - preserve TIMESTAMP on roundtrip *(PR [#5145](https://github.com/tobymao/sqlglot/pull/5145) by [@georgesittas](https://github.com/georgesittas))*:

  preserve TIMESTAMP on roundtrip (#5145)

- due to [`f6124c6`](https://github.com/tobymao/sqlglot/commit/f6124c6343f67563fc19f617891ecfc145a642db) - return token vector in `tokenize` even on failure *(PR [#5155](https://github.com/tobymao/sqlglot/pull/5155) by [@georgesittas](https://github.com/georgesittas))*:

  return token vector in `tokenize` even on failure (#5155)

- due to [`64c37f1`](https://github.com/tobymao/sqlglot/commit/64c37f147366fe87ae187996ecb3c9a5afa7c264) - bump sqlglotrs to 0.6.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.6.0

- due to [`434c45b`](https://github.com/tobymao/sqlglot/commit/434c45b547c3a5ea155dc8d7da2baab326eb6d4f) - improve support for ENDSWITH closes [#5170](https://github.com/tobymao/sqlglot/pull/5170) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  improve support for ENDSWITH closes #5170

- due to [`bc001ce`](https://github.com/tobymao/sqlglot/commit/bc001cef4c907d8fa421d3190b4fa91865d9ff6c) - Add support for ANY_VALUE for versions 16+ *(PR [#5179](https://github.com/tobymao/sqlglot/pull/5179) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for ANY_VALUE for versions 16+ (#5179)

- due to [`6a2cb39`](https://github.com/tobymao/sqlglot/commit/6a2cb39d0ceec091dc4fc228f26d4f457729a3cf) - virtual column with AS(expr) as ComputedColumnConstraint *(PR [#5180](https://github.com/tobymao/sqlglot/pull/5180) by [@geooo109](https://github.com/geooo109))*:

  virtual column with AS(expr) as ComputedColumnConstraint (#5180)

- due to [`29e2f1d`](https://github.com/tobymao/sqlglot/commit/29e2f1d89c095c9fab0944a6962c99bd745c2c91) - Array_intersection transpilation support *(PR [#5186](https://github.com/tobymao/sqlglot/pull/5186) by [@HarishRavi96](https://github.com/HarishRavi96))*:

  Array_intersection transpilation support (#5186)

- due to [`ac6555b`](https://github.com/tobymao/sqlglot/commit/ac6555b4d6c162ef7b14b63307d01fd560138ea0) - preserve DIV binary operator, fixes [#5198](https://github.com/tobymao/sqlglot/pull/5198) *(PR [#5199](https://github.com/tobymao/sqlglot/pull/5199) by [@georgesittas](https://github.com/georgesittas))*:

  preserve DIV binary operator, fixes #5198 (#5199)

- due to [`dfdd84b`](https://github.com/tobymao/sqlglot/commit/dfdd84bbc50da70f40a17b39935f8171d961f7d2) - CTEs instead of subqueries for pipe syntax *(PR [#5205](https://github.com/tobymao/sqlglot/pull/5205) by [@geooo109](https://github.com/geooo109))*:

  CTEs instead of subqueries for pipe syntax (#5205)

- due to [`5f95299`](https://github.com/tobymao/sqlglot/commit/5f9529940d83e89704f7d25eda63cd73fdb503ae) - support multi-part (>3) dotted functions *(PR [#5211](https://github.com/tobymao/sqlglot/pull/5211) by [@georgesittas](https://github.com/georgesittas))*:

  support multi-part (>3) dotted functions (#5211)

- due to [`02afa2a`](https://github.com/tobymao/sqlglot/commit/02afa2a1941fc67086d50dffac2857262f1c3c4f) - Preserve quoting for UDT *(PR [#5216](https://github.com/tobymao/sqlglot/pull/5216) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve quoting for UDT (#5216)

- due to [`44297f1`](https://github.com/tobymao/sqlglot/commit/44297f1c5c8c2cb16fe77c318312f417b4281708) - JOIN pipe syntax, Set Operators as CTEs *(PR [#5215](https://github.com/tobymao/sqlglot/pull/5215) by [@geooo109](https://github.com/geooo109))*:

  JOIN pipe syntax, Set Operators as CTEs (#5215)

- due to [`4f42d95`](https://github.com/tobymao/sqlglot/commit/4f42d951363f8c43a4c414dc21d0505d9c8e48bf) - Normalize date parts in `exp.Extract` generation *(PR [#5229](https://github.com/tobymao/sqlglot/pull/5229) by [@VaggelisD](https://github.com/VaggelisD))*:

  Normalize date parts in `exp.Extract` generation (#5229)

- due to [`e7e38fe`](https://github.com/tobymao/sqlglot/commit/e7e38fe0e09f9affbff4ffa7023d0161e3a1ee49) - resolve table "columns" in bigquery that produce structs *(PR [#5230](https://github.com/tobymao/sqlglot/pull/5230) by [@georgesittas](https://github.com/georgesittas))*:

  resolve table "columns" in bigquery that produce structs (#5230)

- due to [`d3dc761`](https://github.com/tobymao/sqlglot/commit/d3dc761393146357a5d20c4d7992fd2a1ae5e6e2) - change comma to cross join when precedence is the same for all join types *(PR [#5240](https://github.com/tobymao/sqlglot/pull/5240) by [@georgesittas](https://github.com/georgesittas))*:

  change comma to cross join when precedence is the same for all join types (#5240)

- due to [`e7c217e`](https://github.com/tobymao/sqlglot/commit/e7c217ef08e5811e7dad2b3d26dbaa9f02114e38) - transpile from/to dbms_random.value *(PR [#5242](https://github.com/tobymao/sqlglot/pull/5242) by [@georgesittas](https://github.com/georgesittas))*:

  transpile from/to dbms_random.value (#5242)

- due to [`31814cd`](https://github.com/tobymao/sqlglot/commit/31814cddb0cf65caf29fbc45a31a9c865b7991c7) - cast constructed timestamp literal to zone-aware type if needed *(PR [#5253](https://github.com/tobymao/sqlglot/pull/5253) by [@georgesittas](https://github.com/georgesittas))*:

  cast constructed timestamp literal to zone-aware type if needed (#5253)

- due to [`db4e0ec`](https://github.com/tobymao/sqlglot/commit/db4e0ece950a6a1f543d8ecad48a7d4b1d6872be) - convert information schema keywords to uppercase for consistency *(PR [#5263](https://github.com/tobymao/sqlglot/pull/5263) by [@mattiasthalen](https://github.com/mattiasthalen))*:

  convert information schema keywords to uppercase for consistency (#5263)

- due to [`eea1570`](https://github.com/tobymao/sqlglot/commit/eea1570ba530517a95699092ccd9ce6a856f5e84) - add support for SYSDATETIMEOFFSET closes [#5272](https://github.com/tobymao/sqlglot/pull/5272) *(PR [#5273](https://github.com/tobymao/sqlglot/pull/5273) by [@georgesittas](https://github.com/georgesittas))*:

  add support for SYSDATETIMEOFFSET closes #5272 (#5273)

- due to [`3d3ccc5`](https://github.com/tobymao/sqlglot/commit/3d3ccc52a40536b9ac4e974f1592dffe5a7568f9) - Transpile exp.PosExplode pos column alias *(PR [#5274](https://github.com/tobymao/sqlglot/pull/5274) by [@VaggelisD](https://github.com/VaggelisD))*:

  Transpile exp.PosExplode pos column alias (#5274)

- due to [`9a95af1`](https://github.com/tobymao/sqlglot/commit/9a95af1c725cd70ffa8206f1d88452a7faab93b2) - only cast strings to timestamp for TO_CHAR (TimeToStr) *(PR [#5283](https://github.com/tobymao/sqlglot/pull/5283) by [@georgesittas](https://github.com/georgesittas))*:

  only cast strings to timestamp for TO_CHAR (TimeToStr) (#5283)

- due to [`8af4790`](https://github.com/tobymao/sqlglot/commit/8af479017ccde16049c897ae5d322d4a69843b65) - Fix parsing of ADD CONSTRAINT *(PR [#5288](https://github.com/tobymao/sqlglot/pull/5288) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix parsing of ADD CONSTRAINT (#5288)

- due to [`18aea08`](https://github.com/tobymao/sqlglot/commit/18aea08f7dcaa887bcf29886cd3b3bc2850a3679) - include bigquery unnest aliases in selected sources *(PR [#5285](https://github.com/tobymao/sqlglot/pull/5285) by [@georgesittas](https://github.com/georgesittas))*:

  include bigquery unnest aliases in selected sources (#5285)

- due to [`0ff95c5`](https://github.com/tobymao/sqlglot/commit/0ff95c5903907c9ab30b7850bb3b962bc6da2bab) - add parsing/transpilation support for the REPLACE function *(PR [#5289](https://github.com/tobymao/sqlglot/pull/5289) by [@rahulj51](https://github.com/rahulj51))*:

  add parsing/transpilation support for the REPLACE function (#5289)

- due to [`dc03649`](https://github.com/tobymao/sqlglot/commit/dc03649bca0b7a090254976182a03c21dd2269ba) - only coerce time var -like units into strings for DATE_TRUNC *(PR [#5291](https://github.com/tobymao/sqlglot/pull/5291) by [@georgesittas](https://github.com/georgesittas))*:

  only coerce time var -like units into strings for DATE_TRUNC (#5291)

- due to [`5724538`](https://github.com/tobymao/sqlglot/commit/5724538f278b2178114b88850251afd7c3db0dda) - ARRAY_CONCAT type annotation *(PR [#5293](https://github.com/tobymao/sqlglot/pull/5293) by [@geooo109](https://github.com/geooo109))*:

  ARRAY_CONCAT type annotation (#5293)

- due to [`c103b23`](https://github.com/tobymao/sqlglot/commit/c103b2304dca552ac8cf6733156db8b59d3614f3) - add support for `SUBSTRING_INDEX` *(PR [#5296](https://github.com/tobymao/sqlglot/pull/5296) by [@ankur334](https://github.com/ankur334))*:

  add support for `SUBSTRING_INDEX` (#5296)

- due to [`a7bd823`](https://github.com/tobymao/sqlglot/commit/a7bd8234e0dd02abfe6fa56287e7bda14a549e5a) - annotate type of ARRAY_TO_STRING *(PR [#5301](https://github.com/tobymao/sqlglot/pull/5301) by [@geooo109](https://github.com/geooo109))*:

  annotate type of ARRAY_TO_STRING (#5301)

- due to [`6b42353`](https://github.com/tobymao/sqlglot/commit/6b4235340a2e432015c27b2aeadbdcb930bfa6b0) - annotate type of ARRAY_FIRST, ARRAY_LAST *(PR [#5303](https://github.com/tobymao/sqlglot/pull/5303) by [@geooo109](https://github.com/geooo109))*:

  annotate type of ARRAY_FIRST, ARRAY_LAST (#5303)

- due to [`db9b61e`](https://github.com/tobymao/sqlglot/commit/db9b61e4ecaa0600418eb90f637fb8b06b08c399) - parse, annotate type for ARRAY_REVERSE *(PR [#5306](https://github.com/tobymao/sqlglot/pull/5306) by [@geooo109](https://github.com/geooo109))*:

  parse, annotate type for ARRAY_REVERSE (#5306)

- due to [`5612a6d`](https://github.com/tobymao/sqlglot/commit/5612a6da6dee3545f3600db1e5b87c9450952eba) - add support for SPACE *(PR [#5308](https://github.com/tobymao/sqlglot/pull/5308) by [@ankur334](https://github.com/ankur334))*:

  add support for SPACE (#5308)

- due to [`8a2f65d`](https://github.com/tobymao/sqlglot/commit/8a2f65d6b2b68ad5ba45a5aed5e56c4dc0fea6fc) - parse and annotate type for ARRAY_SLICE *(PR [#5312](https://github.com/tobymao/sqlglot/pull/5312) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for ARRAY_SLICE (#5312)

- due to [`8d118ea`](https://github.com/tobymao/sqlglot/commit/8d118ead9c15e7b2b4b51b7cf93cab94e61c2625) - route statements to hive/trino depending on their type *(PR [#5314](https://github.com/tobymao/sqlglot/pull/5314) by [@georgesittas](https://github.com/georgesittas))*:

  route statements to hive/trino depending on their type (#5314)

- due to [`d2f7c41`](https://github.com/tobymao/sqlglot/commit/d2f7c41f9f30f4cf0c74782be9be0cc6e75565f3) - add TypeOf / toTypeName support *(PR [#5315](https://github.com/tobymao/sqlglot/pull/5315) by [@ankur334](https://github.com/ankur334))*:

  add TypeOf / toTypeName support (#5315)

- due to [`5a0f589`](https://github.com/tobymao/sqlglot/commit/5a0f589a0fdb6743c3be2f98b74a34780f51332b) - distinguish STORED AS from USING *(PR [#5320](https://github.com/tobymao/sqlglot/pull/5320) by [@geooo109](https://github.com/geooo109))*:

  distinguish STORED AS from USING (#5320)

- due to [`c4ca182`](https://github.com/tobymao/sqlglot/commit/c4ca182ad637b7a22b55d0ecf320c5a09ec5d56c) - annotate type for FROM_BASE64 *(PR [#5329](https://github.com/tobymao/sqlglot/pull/5329) by [@geooo109](https://github.com/geooo109))*:

  annotate type for FROM_BASE64 (#5329)

- due to [`7b72bbe`](https://github.com/tobymao/sqlglot/commit/7b72bbed3a0930e11ce4a0fdd9082de715326ac9) - annotate type for ANY_VALUE *(PR [#5331](https://github.com/tobymao/sqlglot/pull/5331) by [@geooo109](https://github.com/geooo109))*:

  annotate type for ANY_VALUE (#5331)

- due to [`c0d57e7`](https://github.com/tobymao/sqlglot/commit/c0d57e747bf5d2bed7ba2007ac2092d5797ee038) - annotate type for CHR *(PR [#5332](https://github.com/tobymao/sqlglot/pull/5332) by [@geooo109](https://github.com/geooo109))*:

  annotate type for CHR (#5332)

- due to [`d65b5c2`](https://github.com/tobymao/sqlglot/commit/d65b5c22c29416007cca0154fd35f1d4b5efc929) - annotate type for COUNTIF *(PR [#5334](https://github.com/tobymao/sqlglot/pull/5334) by [@geooo109](https://github.com/geooo109))*:

  annotate type for COUNTIF (#5334)

- due to [`521b705`](https://github.com/tobymao/sqlglot/commit/521b7053213df8577f609409af2552c2ff4fd8c9) - annotate type for GENERATE_ARRAY *(PR [#5335](https://github.com/tobymao/sqlglot/pull/5335) by [@geooo109](https://github.com/geooo109))*:

  annotate type for GENERATE_ARRAY (#5335)

- due to [`5fb26c5`](https://github.com/tobymao/sqlglot/commit/5fb26c58026018360f36a732394b612a3baac38b) - annotate type for INT64 *(PR [#5339](https://github.com/tobymao/sqlglot/pull/5339) by [@geooo109](https://github.com/geooo109))*:

  annotate type for INT64 (#5339)

- due to [`cff9b55`](https://github.com/tobymao/sqlglot/commit/cff9b55d70a3b85057e6385c93c0814eaa50f40b) - annotate type for LOGICAL_AND and LOGICAL_OR *(PR [#5340](https://github.com/tobymao/sqlglot/pull/5340) by [@geooo109](https://github.com/geooo109))*:

  annotate type for LOGICAL_AND and LOGICAL_OR (#5340)

- due to [`b94a6f9`](https://github.com/tobymao/sqlglot/commit/b94a6f9228aa730296c3152179bfbf3503521063) - annotate type for MAKE_INTERVAL *(PR [#5341](https://github.com/tobymao/sqlglot/pull/5341) by [@geooo109](https://github.com/geooo109))*:

  annotate type for MAKE_INTERVAL (#5341)

- due to [`2c9a7c6`](https://github.com/tobymao/sqlglot/commit/2c9a7c6f0b097a9e8514fc5e2af21c52f145920c) - annotate type for LAST_VALUE *(PR [#5336](https://github.com/tobymao/sqlglot/pull/5336) by [@geooo109](https://github.com/geooo109))*:

  annotate type for LAST_VALUE (#5336)

- due to [`d862a28`](https://github.com/tobymao/sqlglot/commit/d862a28b0a30f0c5774351f38a61f195120ad904) - annoate type for TO_BASE64 *(PR [#5342](https://github.com/tobymao/sqlglot/pull/5342) by [@geooo109](https://github.com/geooo109))*:

  annoate type for TO_BASE64 (#5342)

- due to [`85888c1`](https://github.com/tobymao/sqlglot/commit/85888c1b7cbbd0eee179d902a54fbd2a899cc16b) - annotate type for UNIX_DATE *(PR [#5343](https://github.com/tobymao/sqlglot/pull/5343) by [@geooo109](https://github.com/geooo109))*:

  annotate type for UNIX_DATE (#5343)

- due to [`8a214e0`](https://github.com/tobymao/sqlglot/commit/8a214e0859dfb715fcef0dd6b2d6392012b1f3fb) - annotate type for UNIX_SECONDS *(PR [#5344](https://github.com/tobymao/sqlglot/pull/5344) by [@geooo109](https://github.com/geooo109))*:

  annotate type for UNIX_SECONDS (#5344)

- due to [`625cb74`](https://github.com/tobymao/sqlglot/commit/625cb74b69e99ea1a707549366ea960d759848c9) - annotate type for STARTS_WITH *(PR [#5345](https://github.com/tobymao/sqlglot/pull/5345) by [@geooo109](https://github.com/geooo109))*:

  annotate type for STARTS_WITH (#5345)

- due to [`0337c4d`](https://github.com/tobymao/sqlglot/commit/0337c4d46e9e85d951fc9565a47e338106543711) - annotate type for SHA and SHA2 *(PR [#5346](https://github.com/tobymao/sqlglot/pull/5346) by [@geooo109](https://github.com/geooo109))*:

  annotate type for SHA and SHA2 (#5346)

- due to [`cc389fa`](https://github.com/tobymao/sqlglot/commit/cc389facb33f94a0d1f696f2ef9e92f298711894) - annotate type SHA1, SHA256, SHA512 for BigQuery *(PR [#5347](https://github.com/tobymao/sqlglot/pull/5347) by [@geooo109](https://github.com/geooo109))*:

  annotate type SHA1, SHA256, SHA512 for BigQuery (#5347)

- due to [`509b741`](https://github.com/tobymao/sqlglot/commit/509b74173f678842e7550c75c4d8d906c879fb12) - preserve multi-arg DECODE function instead of converting to CASE *(PR [#5352](https://github.com/tobymao/sqlglot/pull/5352) by [@georgesittas](https://github.com/georgesittas))*:

  preserve multi-arg DECODE function instead of converting to CASE (#5352)

- due to [`c1d3d61`](https://github.com/tobymao/sqlglot/commit/c1d3d61d00f00d2030107689d8704f7a488a80a7) - annotate type for CORR *(PR [#5364](https://github.com/tobymao/sqlglot/pull/5364) by [@geooo109](https://github.com/geooo109))*:

  annotate type for CORR (#5364)

- due to [`c1e8677`](https://github.com/tobymao/sqlglot/commit/c1e867767a006e774a2c200c10eb85b3fbd8a372) - annotate type for COVAR_POP *(PR [#5365](https://github.com/tobymao/sqlglot/pull/5365) by [@geooo109](https://github.com/geooo109))*:

  annotate type for COVAR_POP (#5365)

- due to [`e110ef4`](https://github.com/tobymao/sqlglot/commit/e110ef4f774e6ab8de6d4c86e5d306ab53fe895b) - annotate type for COVAR_SAMP *(PR [#5367](https://github.com/tobymao/sqlglot/pull/5367) by [@geooo109](https://github.com/geooo109))*:

  annotate type for COVAR_SAMP (#5367)

- due to [`5b59c16`](https://github.com/tobymao/sqlglot/commit/5b59c16528fb1904c64bef0ca6307bb6a95e5a2c) - annotate type for DATETIME *(PR [#5369](https://github.com/tobymao/sqlglot/pull/5369) by [@geooo109](https://github.com/geooo109))*:

  annotate type for DATETIME (#5369)

- due to [`47176ce`](https://github.com/tobymao/sqlglot/commit/47176ce6b9a4c1722f285034b08a6ae782129894) - annotate type for ENDS_WITH *(PR [#5370](https://github.com/tobymao/sqlglot/pull/5370) by [@geooo109](https://github.com/geooo109))*:

  annotate type for ENDS_WITH (#5370)

- due to [`2cce53d`](https://github.com/tobymao/sqlglot/commit/2cce53d59968f0a4bb3e9599ade93b0e6a140c68) - annotate type for LAG *(PR [#5371](https://github.com/tobymao/sqlglot/pull/5371) by [@geooo109](https://github.com/geooo109))*:

  annotate type for LAG (#5371)

- due to [`a3227de`](https://github.com/tobymao/sqlglot/commit/a3227de3fc57d559eb899dec08af01f85b470ce4) - improve transpilation of `ROUND(x, y)` to Postgres *(PR [#5368](https://github.com/tobymao/sqlglot/pull/5368) by [@blecourt-private](https://github.com/blecourt-private))*:

  improve transpilation of `ROUND(x, y)` to Postgres (#5368)


### :sparkles: New Features
- [`82c50ce`](https://github.com/tobymao/sqlglot/commit/82c50ce68d9a1ad25095086ae3645f5c4996c18b) - **duckdb**: extend time travel parsing to take VERSION into account *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`bb4f428`](https://github.com/tobymao/sqlglot/commit/bb4f4283b53bc060a8c7e0f12c1e7ef5b521c4e6) - bubble up comments nested under a Bracket, fixes [#5131](https://github.com/tobymao/sqlglot/pull/5131) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`9f318eb`](https://github.com/tobymao/sqlglot/commit/9f318ebe4502bb484a34873252cf4a40c7e440e4) - **snowflake**: Transpile BQ's `ARRAY(SELECT AS STRUCT ...)` *(PR [#5140](https://github.com/tobymao/sqlglot/pull/5140) by [@VaggelisD](https://github.com/VaggelisD))*
- [`93b402a`](https://github.com/tobymao/sqlglot/commit/93b402abc74e642ed312db585b33315674a450cd) - **parser**: support SELECT, FROM, WHERE with pipe syntax *(PR [#5128](https://github.com/tobymao/sqlglot/pull/5128) by [@geooo109](https://github.com/geooo109))*
- [`1a8e78b`](https://github.com/tobymao/sqlglot/commit/1a8e78bd84e006023d5d3ea561504587dfbb55a9) - **parser**: ORDER BY with pipe syntax *(PR [#5153](https://github.com/tobymao/sqlglot/pull/5153) by [@geooo109](https://github.com/geooo109))*
- [`966ad95`](https://github.com/tobymao/sqlglot/commit/966ad95432d5f8e29ade36d8271a5c489c207324) - **tsql**: add convert style 126 *(PR [#5157](https://github.com/tobymao/sqlglot/pull/5157) by [@pa1ch](https://github.com/pa1ch))*
- [`b7ac6ff`](https://github.com/tobymao/sqlglot/commit/b7ac6ff4680ff619be4b0ddb01f61f916ed09d58) - **parser**: LIMIT/OFFSET pipe syntax *(PR [#5159](https://github.com/tobymao/sqlglot/pull/5159) by [@geooo109](https://github.com/geooo109))*
- [`cfc158d`](https://github.com/tobymao/sqlglot/commit/cfc158d753d4f43d12c3b502633d29e43dcc5569) - **snowflake**: transpile STRTOK_TO_ARRAY to duckdb *(PR [#5165](https://github.com/tobymao/sqlglot/pull/5165) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5160](https://github.com/tobymao/sqlglot/issues/5160) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`ff0f30b`](https://github.com/tobymao/sqlglot/commit/ff0f30bcf7d0d74b26a703eaa632e1be15b3c001) - support ARRAY_REMOVE *(PR [#5163](https://github.com/tobymao/sqlglot/pull/5163) by [@geooo109](https://github.com/geooo109))*
- [`9cac01f`](https://github.com/tobymao/sqlglot/commit/9cac01f6b4a5c93b55f5b68f21cb104932880a0e) - **tsql**: support FOR XML syntax *(PR [#5167](https://github.com/tobymao/sqlglot/pull/5167) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5161](https://github.com/tobymao/sqlglot/issues/5161) opened by [@codykonior](https://github.com/codykonior)*
- [`8b5129f`](https://github.com/tobymao/sqlglot/commit/8b5129f288880032f0bf9d649984d82314039af1) - **postgres**: improve pretty-formatting of ARRAY[...] *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`964b4a1`](https://github.com/tobymao/sqlglot/commit/964b4a1e367e00e243b80edf677cd48d453ed31e) - add line/col position for Star *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`434c45b`](https://github.com/tobymao/sqlglot/commit/434c45b547c3a5ea155dc8d7da2baab326eb6d4f) - improve support for ENDSWITH closes [#5170](https://github.com/tobymao/sqlglot/pull/5170) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`63f9cb4`](https://github.com/tobymao/sqlglot/commit/63f9cb4b158b88574136b32241ee60254352c9e6) - **sqlglotrs**: match the Python implementation of __repr__ for tokens *(PR [#5172](https://github.com/tobymao/sqlglot/pull/5172) by [@georgesittas](https://github.com/georgesittas))*
- [`c007afa`](https://github.com/tobymao/sqlglot/commit/c007afa23831e9bd86f401d85260e15edf00328f) - support Star instance as first arg of exp.column helper *(PR [#5177](https://github.com/tobymao/sqlglot/pull/5177) by [@georgesittas](https://github.com/georgesittas))*
- [`bc001ce`](https://github.com/tobymao/sqlglot/commit/bc001cef4c907d8fa421d3190b4fa91865d9ff6c) - **postgres**: Add support for ANY_VALUE for versions 16+ *(PR [#5179](https://github.com/tobymao/sqlglot/pull/5179) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4674](https://github.com/TobikoData/sqlmesh/issues/4674) opened by [@petrikoro](https://github.com/petrikoro)*
- [`ba05ff6`](https://github.com/tobymao/sqlglot/commit/ba05ff67127e056d567fc2c1d3bcc8e3dcce7b7e) - **parser**: AGGREGATE with GROUP AND ORDER BY pipe syntax *(PR [#5171](https://github.com/tobymao/sqlglot/pull/5171) by [@geooo109](https://github.com/geooo109))*
- [`26077a4`](https://github.com/tobymao/sqlglot/commit/26077a47d9db750f44ab1baf9a434596b5bb613b) - make to_table more lenient *(PR [#5183](https://github.com/tobymao/sqlglot/pull/5183) by [@georgesittas](https://github.com/georgesittas))*
- [`29e2f1d`](https://github.com/tobymao/sqlglot/commit/29e2f1d89c095c9fab0944a6962c99bd745c2c91) - Array_intersection transpilation support *(PR [#5186](https://github.com/tobymao/sqlglot/pull/5186) by [@HarishRavi96](https://github.com/HarishRavi96))*
- [`d86a114`](https://github.com/tobymao/sqlglot/commit/d86a1147aeb866ed0ab2c342914ecf8cbfadac8a) - **sqlite**: implement RESPECT/IGNORE NULLS in first_value() *(PR [#5185](https://github.com/tobymao/sqlglot/pull/5185) by [@NickCrews](https://github.com/NickCrews))*
- [`1d50fca`](https://github.com/tobymao/sqlglot/commit/1d50fca8ffc34e4acbc1b791c4cdf5f184a748db) - improve transpilation of st_point and st_distance *(PR [#5194](https://github.com/tobymao/sqlglot/pull/5194) by [@georgesittas](https://github.com/georgesittas))*
- [`756ec3b`](https://github.com/tobymao/sqlglot/commit/756ec3b65db1eb2572d017a3ac12ece6bb44c726) - **parser**: SET OPERATORS with pipe syntax *(PR [#5184](https://github.com/tobymao/sqlglot/pull/5184) by [@geooo109](https://github.com/geooo109))*
- [`c20f85e`](https://github.com/tobymao/sqlglot/commit/c20f85e3e171e502fc51f74894d3313f0ad61535) - **spark**: support ALTER ADD PARTITION *(PR [#5208](https://github.com/tobymao/sqlglot/pull/5208) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5204](https://github.com/tobymao/sqlglot/issues/5204) opened by [@cosinequanon](https://github.com/cosinequanon)*
- [`44297f1`](https://github.com/tobymao/sqlglot/commit/44297f1c5c8c2cb16fe77c318312f417b4281708) - **parser**: JOIN pipe syntax, Set Operators as CTEs *(PR [#5215](https://github.com/tobymao/sqlglot/pull/5215) by [@geooo109](https://github.com/geooo109))*
- [`21cd3eb`](https://github.com/tobymao/sqlglot/commit/21cd3ebf5d0b57f5b102c5aadc3b24a598ebe918) - **parser**: PIVOT/UNPIVOT pipe syntax *(PR [#5222](https://github.com/tobymao/sqlglot/pull/5222) by [@geooo109](https://github.com/geooo109))*
- [`97f5822`](https://github.com/tobymao/sqlglot/commit/97f58226fc8815b23787b7b8699ea71f58268560) - **parser**: AS pipe syntax *(PR [#5224](https://github.com/tobymao/sqlglot/pull/5224) by [@geooo109](https://github.com/geooo109))*
- [`a7e7fee`](https://github.com/tobymao/sqlglot/commit/a7e7feef02a77fe8606f3f482bad91230fa637f4) - **parser**: EXTEND pipe syntax *(PR [#5225](https://github.com/tobymao/sqlglot/pull/5225) by [@geooo109](https://github.com/geooo109))*
- [`c1cb9f8`](https://github.com/tobymao/sqlglot/commit/c1cb9f8f682080f7a06c387219d79c6d068b6dbe) - **snowflake**: add autoincrement order clause support *(PR [#5223](https://github.com/tobymao/sqlglot/pull/5223) by [@dmaresma](https://github.com/dmaresma))*
- [`91afe4c`](https://github.com/tobymao/sqlglot/commit/91afe4cfd7b3f427e4c0b298075e867b8a1bbe55) - **parser**: TABLESAMPLE pipe syntax *(PR [#5231](https://github.com/tobymao/sqlglot/pull/5231) by [@geooo109](https://github.com/geooo109))*
- [`62da84a`](https://github.com/tobymao/sqlglot/commit/62da84acce7f44802dca26a9357a16115e21fabf) - **snowflake**: improve transpilation of unnested object lookup *(PR [#5234](https://github.com/tobymao/sqlglot/pull/5234) by [@georgesittas](https://github.com/georgesittas))*
- [`2c60453`](https://github.com/tobymao/sqlglot/commit/2c604537ba83dee74e9ced7e216673ecc70fe487) - **parser**: DROP pipe syntax *(PR [#5226](https://github.com/tobymao/sqlglot/pull/5226) by [@geooo109](https://github.com/geooo109))*
- [`9885729`](https://github.com/tobymao/sqlglot/commit/988572954135c68dc021b992c815024ce3debaff) - **parser**: SET pipe syntax *(PR [#5236](https://github.com/tobymao/sqlglot/pull/5236) by [@geooo109](https://github.com/geooo109))*
- [`e7c217e`](https://github.com/tobymao/sqlglot/commit/e7c217ef08e5811e7dad2b3d26dbaa9f02114e38) - **oracle**: transpile from/to dbms_random.value *(PR [#5242](https://github.com/tobymao/sqlglot/pull/5242) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5241](https://github.com/tobymao/sqlglot/issues/5241) opened by [@Akshat-2512](https://github.com/Akshat-2512)*
- [`0d19544`](https://github.com/tobymao/sqlglot/commit/0d19544317c1056b17fb089d4be9b5bddfe6feb3) - add Microsoft Fabric dialect, a case sensitive version of TSQL *(PR [#5247](https://github.com/tobymao/sqlglot/pull/5247) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`249dbc9`](https://github.com/tobymao/sqlglot/commit/249dbc906adc6b20932dc8efe83f6f4d23ef8c1e) - **parser**: start with SELECT and nested pipe syntax *(PR [#5248](https://github.com/tobymao/sqlglot/pull/5248) by [@geooo109](https://github.com/geooo109))*
- [`f5b5b93`](https://github.com/tobymao/sqlglot/commit/f5b5b9338eb92b7aa2c9b4c92c6138c2c05e1c40) - **fabric**: implement type mappings for unsupported Fabric types *(PR [#5249](https://github.com/tobymao/sqlglot/pull/5249) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`78fcea1`](https://github.com/tobymao/sqlglot/commit/78fcea13b5eb1734a15a254875bc80ad8063b0b0) - **spark, databricks**: parse brackets as placeholder *(PR [#5256](https://github.com/tobymao/sqlglot/pull/5256) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5251](https://github.com/tobymao/sqlglot/issues/5251) opened by [@aersam](https://github.com/aersam)*
- [`7d71387`](https://github.com/tobymao/sqlglot/commit/7d7138780db82e7a75949d29282b944e739ad99d) - **fabric**: Add precision cap to temporal data types *(PR [#5250](https://github.com/tobymao/sqlglot/pull/5250) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`e8cf793`](https://github.com/tobymao/sqlglot/commit/e8cf79305d398f25640ef3c07dd8b32997cb0167) - **duckdb**: Transpile Snowflake's TO_CHAR if format is in Snowflake.TIME_MAPPING *(PR [#5257](https://github.com/tobymao/sqlglot/pull/5257) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5255](https://github.com/tobymao/sqlglot/issues/5255) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`0cdfe64`](https://github.com/tobymao/sqlglot/commit/0cdfe642e3cb996c5ac48cc055af2862340dcf56) - add Exasol dialect (pass 1: string type mapping) *(PR [#5264](https://github.com/tobymao/sqlglot/pull/5264) by [@nnamdi16](https://github.com/nnamdi16))*
- [`eea1570`](https://github.com/tobymao/sqlglot/commit/eea1570ba530517a95699092ccd9ce6a856f5e84) - **tsql**: add support for SYSDATETIMEOFFSET closes [#5272](https://github.com/tobymao/sqlglot/pull/5272) *(PR [#5273](https://github.com/tobymao/sqlglot/pull/5273) by [@georgesittas](https://github.com/georgesittas))*
- [`3d3ccc5`](https://github.com/tobymao/sqlglot/commit/3d3ccc52a40536b9ac4e974f1592dffe5a7568f9) - **hive**: Transpile exp.PosExplode pos column alias *(PR [#5274](https://github.com/tobymao/sqlglot/pull/5274) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5271](https://github.com/tobymao/sqlglot/issues/5271) opened by [@charlie-liner](https://github.com/charlie-liner)*
- [`1c48c09`](https://github.com/tobymao/sqlglot/commit/1c48c09fd836db40bba6c46d0e9969937ce96587) - **exasol**: added datatype mappings and test for exasol dialect. *(PR [#5270](https://github.com/tobymao/sqlglot/pull/5270) by [@nnamdi16](https://github.com/nnamdi16))*
- [`883fcb1`](https://github.com/tobymao/sqlglot/commit/883fcb137583f6d36f3a70a1343780bb40bf6f81) - **databricks**: GROUP_CONCAT to LISTAGG *(PR [#5284](https://github.com/tobymao/sqlglot/pull/5284) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5281](https://github.com/tobymao/sqlglot/issues/5281) opened by [@wKollendorf](https://github.com/wKollendorf)*
- [`21ef897`](https://github.com/tobymao/sqlglot/commit/21ef8974426d9f3562ade0bd2c8448bb440bee27) - **fabric**: implement UnixToTime transformation to DATEADD syntax *(PR [#5269](https://github.com/tobymao/sqlglot/pull/5269) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`0ff95c5`](https://github.com/tobymao/sqlglot/commit/0ff95c5903907c9ab30b7850bb3b962bc6da2bab) - add parsing/transpilation support for the REPLACE function *(PR [#5289](https://github.com/tobymao/sqlglot/pull/5289) by [@rahulj51](https://github.com/rahulj51))*
- [`1b0631c`](https://github.com/tobymao/sqlglot/commit/1b0631c2b4516a9ceb81af6173790dd09269b635) - **exasol**: implemented the Mod function *(PR [#5292](https://github.com/tobymao/sqlglot/pull/5292) by [@nnamdi16](https://github.com/nnamdi16))*
- [`ba7bf39`](https://github.com/tobymao/sqlglot/commit/ba7bf39966b519e11cde02a3c1f720598469e616) - **exasol**: implemented BIT_AND function with test *(PR [#5294](https://github.com/tobymao/sqlglot/pull/5294) by [@nnamdi16](https://github.com/nnamdi16))*
- [`fb4122e`](https://github.com/tobymao/sqlglot/commit/fb4122e80d1995bb87401e9ebe3749078c026a06) - **exasol**: add bitwiseOr function to exasol dialect *(PR [#5297](https://github.com/tobymao/sqlglot/pull/5297) by [@nnamdi16](https://github.com/nnamdi16))*
- [`c103b23`](https://github.com/tobymao/sqlglot/commit/c103b2304dca552ac8cf6733156db8b59d3614f3) - add support for `SUBSTRING_INDEX` *(PR [#5296](https://github.com/tobymao/sqlglot/pull/5296) by [@ankur334](https://github.com/ankur334))*
- [`4752f3a`](https://github.com/tobymao/sqlglot/commit/4752f3a6b715d8b6968c8f1f05f6ccdfb7351071) - **exasol**: added bit_xor built in exasol function to exasol dialect in sqlglot *(PR [#5298](https://github.com/tobymao/sqlglot/pull/5298) by [@nnamdi16](https://github.com/nnamdi16))*
- [`09bd610`](https://github.com/tobymao/sqlglot/commit/09bd6101de21ed86c9fd6df0f63e8bca2666dd81) - **parser**: annotate type of ARRAY_CONCAT_AGG *(PR [#5299](https://github.com/tobymao/sqlglot/pull/5299) by [@geooo109](https://github.com/geooo109))*
- [`ad0311a`](https://github.com/tobymao/sqlglot/commit/ad0311a7f8b0b3c5746c29d816b58578a892dd33) - **exasol**: added bit_not exasol built in function. *(PR [#5300](https://github.com/tobymao/sqlglot/pull/5300) by [@nnamdi16](https://github.com/nnamdi16))*
- [`a7bd823`](https://github.com/tobymao/sqlglot/commit/a7bd8234e0dd02abfe6fa56287e7bda14a549e5a) - **parser**: annotate type of ARRAY_TO_STRING *(PR [#5301](https://github.com/tobymao/sqlglot/pull/5301) by [@geooo109](https://github.com/geooo109))*
- [`2aa2182`](https://github.com/tobymao/sqlglot/commit/2aa21820f7d3a26cc4f47c1c757a9b7c97dd0382) - **exasol**: added BIT_LSHIFT built in function to exasol dialect *(PR [#5302](https://github.com/tobymao/sqlglot/pull/5302) by [@nnamdi16](https://github.com/nnamdi16))*
- [`c3d9ef2`](https://github.com/tobymao/sqlglot/commit/c3d9ef2cb2d004b57c64af4f3f1bac41f1890737) - **exasol**: added the bit_rshift built in exasol function *(PR [#5304](https://github.com/tobymao/sqlglot/pull/5304) by [@nnamdi16](https://github.com/nnamdi16))*
- [`6b42353`](https://github.com/tobymao/sqlglot/commit/6b4235340a2e432015c27b2aeadbdcb930bfa6b0) - **parser**: annotate type of ARRAY_FIRST, ARRAY_LAST *(PR [#5303](https://github.com/tobymao/sqlglot/pull/5303) by [@geooo109](https://github.com/geooo109))*
- [`f5b7cc6`](https://github.com/tobymao/sqlglot/commit/f5b7cc6d2f8d73bff4e42e242d3ad3db41d899cc) - **exasol**: added `EVERY` built in function *(PR [#5305](https://github.com/tobymao/sqlglot/pull/5305) by [@nnamdi16](https://github.com/nnamdi16))*
- [`d3f04d6`](https://github.com/tobymao/sqlglot/commit/d3f04d6766281ecb7ced9a5e812ab765d7b699be) - add Dremio dialect *(PR [#5277](https://github.com/tobymao/sqlglot/pull/5277) by [@mateuszpoleski](https://github.com/mateuszpoleski))*
- [`3d8e478`](https://github.com/tobymao/sqlglot/commit/3d8e478eac3df6a94c87cd610f96c5f19697a9bf) - **exasol**: added edit_distance built in function to exasol dialect *(PR [#5310](https://github.com/tobymao/sqlglot/pull/5310) by [@nnamdi16](https://github.com/nnamdi16))*
- [`db9b61e`](https://github.com/tobymao/sqlglot/commit/db9b61e4ecaa0600418eb90f637fb8b06b08c399) - **parser**: parse, annotate type for ARRAY_REVERSE *(PR [#5306](https://github.com/tobymao/sqlglot/pull/5306) by [@geooo109](https://github.com/geooo109))*
- [`5612a6d`](https://github.com/tobymao/sqlglot/commit/5612a6da6dee3545f3600db1e5b87c9450952eba) - add support for SPACE *(PR [#5308](https://github.com/tobymao/sqlglot/pull/5308) by [@ankur334](https://github.com/ankur334))*
- [`f148c9e`](https://github.com/tobymao/sqlglot/commit/f148c9e64ae0d4df96323271729fa6a6ca68a671) - **duckdb**: Transpile Spark's `exp.PosExplode` *(PR [#5311](https://github.com/tobymao/sqlglot/pull/5311) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5309](https://github.com/tobymao/sqlglot/issues/5309) opened by [@nimrodolev](https://github.com/nimrodolev)*
- [`179a278`](https://github.com/tobymao/sqlglot/commit/179a278c7fdbc29105e37f132e6f03e18627f769) - **exasol**: added the regexp_replace function *(PR [#5313](https://github.com/tobymao/sqlglot/pull/5313) by [@nnamdi16](https://github.com/nnamdi16))*
- [`8a2f65d`](https://github.com/tobymao/sqlglot/commit/8a2f65d6b2b68ad5ba45a5aed5e56c4dc0fea6fc) - **parser**: parse and annotate type for ARRAY_SLICE *(PR [#5312](https://github.com/tobymao/sqlglot/pull/5312) by [@geooo109](https://github.com/geooo109))*
- [`d2f7c41`](https://github.com/tobymao/sqlglot/commit/d2f7c41f9f30f4cf0c74782be9be0cc6e75565f3) - add TypeOf / toTypeName support *(PR [#5315](https://github.com/tobymao/sqlglot/pull/5315) by [@ankur334](https://github.com/ankur334))*
- [`950c15d`](https://github.com/tobymao/sqlglot/commit/950c15db5ff64b6f11036f8003db3e5b1fb3afc3) - **exasol**: add var_pop built in function to exasol dialect *(PR [#5328](https://github.com/tobymao/sqlglot/pull/5328) by [@nnamdi16](https://github.com/nnamdi16))*
- [`c4ca182`](https://github.com/tobymao/sqlglot/commit/c4ca182ad637b7a22b55d0ecf320c5a09ec5d56c) - **optimizer**: annotate type for FROM_BASE64 *(PR [#5329](https://github.com/tobymao/sqlglot/pull/5329) by [@geooo109](https://github.com/geooo109))*
- [`0992e99`](https://github.com/tobymao/sqlglot/commit/0992e99f99aeb4ecc97e6918a23b8fd524311ed9) - **exasol**: Add support  APPROXIMATE_COUNT_DISTINCT functions in exasol dialect *(PR [#5330](https://github.com/tobymao/sqlglot/pull/5330) by [@nnamdi16](https://github.com/nnamdi16))*
- [`7b72bbe`](https://github.com/tobymao/sqlglot/commit/7b72bbed3a0930e11ce4a0fdd9082de715326ac9) - **optimizer**: annotate type for ANY_VALUE *(PR [#5331](https://github.com/tobymao/sqlglot/pull/5331) by [@geooo109](https://github.com/geooo109))*
- [`c0d57e7`](https://github.com/tobymao/sqlglot/commit/c0d57e747bf5d2bed7ba2007ac2092d5797ee038) - **optimizer**: annotate type for CHR *(PR [#5332](https://github.com/tobymao/sqlglot/pull/5332) by [@geooo109](https://github.com/geooo109))*
- [`d65b5c2`](https://github.com/tobymao/sqlglot/commit/d65b5c22c29416007cca0154fd35f1d4b5efc929) - **optimizer**: annotate type for COUNTIF *(PR [#5334](https://github.com/tobymao/sqlglot/pull/5334) by [@geooo109](https://github.com/geooo109))*
- [`521b705`](https://github.com/tobymao/sqlglot/commit/521b7053213df8577f609409af2552c2ff4fd8c9) - **optimizer**: annotate type for GENERATE_ARRAY *(PR [#5335](https://github.com/tobymao/sqlglot/pull/5335) by [@geooo109](https://github.com/geooo109))*
- [`5fb26c5`](https://github.com/tobymao/sqlglot/commit/5fb26c58026018360f36a732394b612a3baac38b) - **optimizer**: annotate type for INT64 *(PR [#5339](https://github.com/tobymao/sqlglot/pull/5339) by [@geooo109](https://github.com/geooo109))*
- [`cff9b55`](https://github.com/tobymao/sqlglot/commit/cff9b55d70a3b85057e6385c93c0814eaa50f40b) - **optimizer**: annotate type for LOGICAL_AND and LOGICAL_OR *(PR [#5340](https://github.com/tobymao/sqlglot/pull/5340) by [@geooo109](https://github.com/geooo109))*
- [`b94a6f9`](https://github.com/tobymao/sqlglot/commit/b94a6f9228aa730296c3152179bfbf3503521063) - **optimizer**: annotate type for MAKE_INTERVAL *(PR [#5341](https://github.com/tobymao/sqlglot/pull/5341) by [@geooo109](https://github.com/geooo109))*
- [`2c9a7c6`](https://github.com/tobymao/sqlglot/commit/2c9a7c6f0b097a9e8514fc5e2af21c52f145920c) - **optimizer**: annotate type for LAST_VALUE *(PR [#5336](https://github.com/tobymao/sqlglot/pull/5336) by [@geooo109](https://github.com/geooo109))*
- [`d862a28`](https://github.com/tobymao/sqlglot/commit/d862a28b0a30f0c5774351f38a61f195120ad904) - **optimizer**: annoate type for TO_BASE64 *(PR [#5342](https://github.com/tobymao/sqlglot/pull/5342) by [@geooo109](https://github.com/geooo109))*
- [`85888c1`](https://github.com/tobymao/sqlglot/commit/85888c1b7cbbd0eee179d902a54fbd2a899cc16b) - **optimizer**: annotate type for UNIX_DATE *(PR [#5343](https://github.com/tobymao/sqlglot/pull/5343) by [@geooo109](https://github.com/geooo109))*
- [`8a214e0`](https://github.com/tobymao/sqlglot/commit/8a214e0859dfb715fcef0dd6b2d6392012b1f3fb) - **optimizer**: annotate type for UNIX_SECONDS *(PR [#5344](https://github.com/tobymao/sqlglot/pull/5344) by [@geooo109](https://github.com/geooo109))*
- [`625cb74`](https://github.com/tobymao/sqlglot/commit/625cb74b69e99ea1a707549366ea960d759848c9) - **optimizer**: annotate type for STARTS_WITH *(PR [#5345](https://github.com/tobymao/sqlglot/pull/5345) by [@geooo109](https://github.com/geooo109))*
- [`0337c4d`](https://github.com/tobymao/sqlglot/commit/0337c4d46e9e85d951fc9565a47e338106543711) - **optimizer**: annotate type for SHA and SHA2 *(PR [#5346](https://github.com/tobymao/sqlglot/pull/5346) by [@geooo109](https://github.com/geooo109))*
- [`835d9e6`](https://github.com/tobymao/sqlglot/commit/835d9e6c9ffc05de642113b566a1a4eb9cc38470) - add case-insensitive uppercase normalization strategy *(PR [#5349](https://github.com/tobymao/sqlglot/pull/5349) by [@georgesittas](https://github.com/georgesittas))*
- [`f80493e`](https://github.com/tobymao/sqlglot/commit/f80493efb168f600dc92da439d84e820f303e5aa) - **exasol**: Add TO_CHAR function support in exasol dialect *(PR [#5350](https://github.com/tobymao/sqlglot/pull/5350) by [@nnamdi16](https://github.com/nnamdi16))*
- [`cea6a24`](https://github.com/tobymao/sqlglot/commit/cea6a240292d6e31bc73179d433835483e65747a) - **teradata**: add FORMAT phrase parsing *(PR [#5348](https://github.com/tobymao/sqlglot/pull/5348) by [@readjfb](https://github.com/readjfb))*
- [`eae64e1`](https://github.com/tobymao/sqlglot/commit/eae64e1629a276bf3885749991869b6c6dea8a8b) - **duckdb**: support new lambda syntax *(PR [#5359](https://github.com/tobymao/sqlglot/pull/5359) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5357](https://github.com/tobymao/sqlglot/issues/5357) opened by [@aersam](https://github.com/aersam)*
- [`e77991d`](https://github.com/tobymao/sqlglot/commit/e77991d92fad56014ba2778c71e5e446d4dd090e) - **duckdb**: Add support for SET VARIABLE *(PR [#5360](https://github.com/tobymao/sqlglot/pull/5360) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5356](https://github.com/tobymao/sqlglot/issues/5356) opened by [@aersam](https://github.com/aersam)*
- [`c1d3d61`](https://github.com/tobymao/sqlglot/commit/c1d3d61d00f00d2030107689d8704f7a488a80a7) - **optimizer**: annotate type for CORR *(PR [#5364](https://github.com/tobymao/sqlglot/pull/5364) by [@geooo109](https://github.com/geooo109))*
- [`c1e8677`](https://github.com/tobymao/sqlglot/commit/c1e867767a006e774a2c200c10eb85b3fbd8a372) - **optimizer**: annotate type for COVAR_POP *(PR [#5365](https://github.com/tobymao/sqlglot/pull/5365) by [@geooo109](https://github.com/geooo109))*
- [`e110ef4`](https://github.com/tobymao/sqlglot/commit/e110ef4f774e6ab8de6d4c86e5d306ab53fe895b) - **optimizer**: annotate type for COVAR_SAMP *(PR [#5367](https://github.com/tobymao/sqlglot/pull/5367) by [@geooo109](https://github.com/geooo109))*
- [`5b59c16`](https://github.com/tobymao/sqlglot/commit/5b59c16528fb1904c64bef0ca6307bb6a95e5a2c) - **optimizer**: annotate type for DATETIME *(PR [#5369](https://github.com/tobymao/sqlglot/pull/5369) by [@geooo109](https://github.com/geooo109))*
- [`47176ce`](https://github.com/tobymao/sqlglot/commit/47176ce6b9a4c1722f285034b08a6ae782129894) - **optimizer**: annotate type for ENDS_WITH *(PR [#5370](https://github.com/tobymao/sqlglot/pull/5370) by [@geooo109](https://github.com/geooo109))*
- [`1fd757e`](https://github.com/tobymao/sqlglot/commit/1fd757e6279315f00e719974613313a6e43dfe55) - **fabric**: Ensure TIMESTAMPTZ is used with AT TIME ZONE *(PR [#5362](https://github.com/tobymao/sqlglot/pull/5362) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`2cce53d`](https://github.com/tobymao/sqlglot/commit/2cce53d59968f0a4bb3e9599ade93b0e6a140c68) - **optimizer**: annotate type for LAG *(PR [#5371](https://github.com/tobymao/sqlglot/pull/5371) by [@geooo109](https://github.com/geooo109))*
- [`a3227de`](https://github.com/tobymao/sqlglot/commit/a3227de3fc57d559eb899dec08af01f85b470ce4) - improve transpilation of `ROUND(x, y)` to Postgres *(PR [#5368](https://github.com/tobymao/sqlglot/pull/5368) by [@blecourt-private](https://github.com/blecourt-private))*
  - :arrow_lower_right: *addresses issue [#5366](https://github.com/tobymao/sqlglot/issues/5366) opened by [@blecourt-private](https://github.com/blecourt-private)*

### :bug: Bug Fixes
- [`f2bf000`](https://github.com/tobymao/sqlglot/commit/f2bf000a410fb18531bb90ef1d767baf0e8bce7a) - **optimizer**: avoid creating new alias for qualifying unpivot *(PR [#5121](https://github.com/tobymao/sqlglot/pull/5121) by [@geooo109](https://github.com/geooo109))*
- [`a126ce8`](https://github.com/tobymao/sqlglot/commit/a126ce8a25287cf3531d815035fa3d567dc772fb) - **optimizer**: make coalesce simplification optional, skip by default *(PR [#5123](https://github.com/tobymao/sqlglot/pull/5123) by [@barakalon](https://github.com/barakalon))*
- [`f7401fd`](https://github.com/tobymao/sqlglot/commit/f7401fdc29a35738eb23f424ceba03463a4d8af9) - **bigquery**: avoid getting stuck in infinite loop when parsing tables *(PR [#5130](https://github.com/tobymao/sqlglot/pull/5130) by [@georgesittas](https://github.com/georgesittas))*
- [`e9b3156`](https://github.com/tobymao/sqlglot/commit/e9b3156aa1ed95fdee4c6b419134d8ca746964b6) - **athena**: Handle transpilation of FileFormatProperty from dialects that treat it as a variable and not a string literal *(PR [#5133](https://github.com/tobymao/sqlglot/pull/5133) by [@erindru](https://github.com/erindru))*
- [`a3fccd9`](https://github.com/tobymao/sqlglot/commit/a3fccd9be294499b53477da931f8b097cdbe09fc) - **snowflake**: generate SELECT for UNNEST without JOIN or FROM *(PR [#5138](https://github.com/tobymao/sqlglot/pull/5138) by [@geooo109](https://github.com/geooo109))*
- [`993919d`](https://github.com/tobymao/sqlglot/commit/993919d05d5d3c814471607b56831bb65d349eb4) - **snowflake**: Properly transpile ARRAY_AGG, IGNORE/RESPECT NULLS *(PR [#5137](https://github.com/tobymao/sqlglot/pull/5137) by [@VaggelisD](https://github.com/VaggelisD))*
- [`6e57619`](https://github.com/tobymao/sqlglot/commit/6e57619f85375e789bb39a6478aa01cd7c7758f0) - **snowflake**: Transpile ISOWEEK to WEEKISO *(PR [#5139](https://github.com/tobymao/sqlglot/pull/5139) by [@VaggelisD](https://github.com/VaggelisD))*
- [`c484ca3`](https://github.com/tobymao/sqlglot/commit/c484ca39bad750a96b62e2edae85612cac66ba30) - **bigquery**: recognize ARRAY_CONCAT_AGG as an aggregate function *(PR [#5141](https://github.com/tobymao/sqlglot/pull/5141) by [@georgesittas](https://github.com/georgesittas))*
- [`f3aeb37`](https://github.com/tobymao/sqlglot/commit/f3aeb374351a0b1b3c75945718d8ea42f8926b62) - **tsql**: properly parse and generate ALTER SET *(PR [#5143](https://github.com/tobymao/sqlglot/pull/5143) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5135](https://github.com/tobymao/sqlglot/issues/5135) opened by [@codykonior](https://github.com/codykonior)*
- [`72ce404`](https://github.com/tobymao/sqlglot/commit/72ce40405625239a0d6763d502e5af8b12abfe9b) - Refactor ALTER TABLE ADD parsing *(PR [#5144](https://github.com/tobymao/sqlglot/pull/5144) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5129](https://github.com/tobymao/sqlglot/issues/5129) opened by [@Mevrael](https://github.com/Mevrael)*
- [`e73ddb7`](https://github.com/tobymao/sqlglot/commit/e73ddb733b7f120ae74054e6d4dc7d458f59ac50) - **mysql**: preserve TIMESTAMP on roundtrip *(PR [#5145](https://github.com/tobymao/sqlglot/pull/5145) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5127](https://github.com/tobymao/sqlglot/issues/5127) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`4f8c73d`](https://github.com/tobymao/sqlglot/commit/4f8c73d60eecebc601c60ee8c7819458435e34b8) - **hive**: STRUCT column names and data type should be separated by ':' in hive *(PR [#5147](https://github.com/tobymao/sqlglot/pull/5147) by [@tsamaras](https://github.com/tsamaras))*
- [`e2a488f`](https://github.com/tobymao/sqlglot/commit/e2a488f48f3e036566462463bbc58cc6a1c7492e) - Error on columns mismatch in pushdown_projections ignores dialect *(PR [#5151](https://github.com/tobymao/sqlglot/pull/5151) by [@snovik75](https://github.com/snovik75))*
- [`1a35365`](https://github.com/tobymao/sqlglot/commit/1a35365a3bb1ef56e8da0023271cbe3108e0ccb1) - avoid generating nested comments when not supported *(PR [#5158](https://github.com/tobymao/sqlglot/pull/5158) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5132](https://github.com/tobymao/sqlglot/issues/5132) opened by [@patricksurry](https://github.com/patricksurry)*
- [`f6124c6`](https://github.com/tobymao/sqlglot/commit/f6124c6343f67563fc19f617891ecfc145a642db) - **rust-tokenizer**: return token vector in `tokenize` even on failure *(PR [#5155](https://github.com/tobymao/sqlglot/pull/5155) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5148](https://github.com/tobymao/sqlglot/issues/5148) opened by [@kamoser](https://github.com/kamoser)*
- [`760a606`](https://github.com/tobymao/sqlglot/commit/760a6062d5f259488e471af9c1d33e200066e9dc) - **postgres**: support decimal values in INTERVAL expressions fixes [#5168](https://github.com/tobymao/sqlglot/pull/5168) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6a2cb39`](https://github.com/tobymao/sqlglot/commit/6a2cb39d0ceec091dc4fc228f26d4f457729a3cf) - **parser**: virtual column with AS(expr) as ComputedColumnConstraint *(PR [#5180](https://github.com/tobymao/sqlglot/pull/5180) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5173](https://github.com/tobymao/sqlglot/issues/5173) opened by [@suyah](https://github.com/suyah)*
- [`c87ae02`](https://github.com/tobymao/sqlglot/commit/c87ae02aa263be8463ca7283ebd090385a4bfd59) - **sqlite**: Add REPLACE to command tokens *(PR [#5192](https://github.com/tobymao/sqlglot/pull/5192) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5187](https://github.com/tobymao/sqlglot/issues/5187) opened by [@stefanmalanik](https://github.com/stefanmalanik)*
- [`4b89afd`](https://github.com/tobymao/sqlglot/commit/4b89afdcc0063e70cbc64165c7f1f5102afaa87c) - **starrocks**: array_agg_transpilation_fix *(PR [#5190](https://github.com/tobymao/sqlglot/pull/5190) by [@Swathiraj23](https://github.com/Swathiraj23))*
- [`461b054`](https://github.com/tobymao/sqlglot/commit/461b0548832ab8d916c3a6638f27a49f681109fe) - **postgres**: support use_spheroid argument in ST_DISTANCE *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`99bbae3`](https://github.com/tobymao/sqlglot/commit/99bbae370329c5f5cd132b711c714359cf96ba58) - **sqlite**: allow ALTER RENAME without COLUMN keyword fixes [#5195](https://github.com/tobymao/sqlglot/pull/5195) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`ac6555b`](https://github.com/tobymao/sqlglot/commit/ac6555b4d6c162ef7b14b63307d01fd560138ea0) - **hive**: preserve DIV binary operator, fixes [#5198](https://github.com/tobymao/sqlglot/pull/5198) *(PR [#5199](https://github.com/tobymao/sqlglot/pull/5199) by [@georgesittas](https://github.com/georgesittas))*
- [`d0eeb26`](https://github.com/tobymao/sqlglot/commit/d0eeb2639e771e8f8b6feabd41c65f16ed5a9829) - eliminate_join_marks has multiple issues fixes [#5188](https://github.com/tobymao/sqlglot/pull/5188) *(PR [#5189](https://github.com/tobymao/sqlglot/pull/5189) by [@snovik75](https://github.com/snovik75))*
  - :arrow_lower_right: *fixes issue [#5188](https://github.com/tobymao/sqlglot/issues/5188) opened by [@snovik75](https://github.com/snovik75)*
- [`dfdd84b`](https://github.com/tobymao/sqlglot/commit/dfdd84bbc50da70f40a17b39935f8171d961f7d2) - **parser**: CTEs instead of subqueries for pipe syntax *(PR [#5205](https://github.com/tobymao/sqlglot/pull/5205) by [@geooo109](https://github.com/geooo109))*
- [`77e9d9a`](https://github.com/tobymao/sqlglot/commit/77e9d9a0269e2013379967cf2f46fbd79c036277) - **mysql**: properly parse STORED/VIRTUAL computed columns *(PR [#5210](https://github.com/tobymao/sqlglot/pull/5210) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5203](https://github.com/tobymao/sqlglot/issues/5203) opened by [@mdebski](https://github.com/mdebski)*
- [`5f95299`](https://github.com/tobymao/sqlglot/commit/5f9529940d83e89704f7d25eda63cd73fdb503ae) - **parser**: support multi-part (>3) dotted functions *(PR [#5211](https://github.com/tobymao/sqlglot/pull/5211) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5200](https://github.com/tobymao/sqlglot/issues/5200) opened by [@mateuszpoleski](https://github.com/mateuszpoleski)*
- [`02afa2a`](https://github.com/tobymao/sqlglot/commit/02afa2a1941fc67086d50dffac2857262f1c3c4f) - **postgres**: Preserve quoting for UDT *(PR [#5216](https://github.com/tobymao/sqlglot/pull/5216) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5212](https://github.com/tobymao/sqlglot/issues/5212) opened by [@NickCrews](https://github.com/NickCrews)*
- [`f37c0b1`](https://github.com/tobymao/sqlglot/commit/f37c0b1197321dd610648ce652a171ab063deeeb) - **snowflake**: ensure a standalone GET() expression can be parsed *(PR [#5219](https://github.com/tobymao/sqlglot/pull/5219) by [@georgesittas](https://github.com/georgesittas))*
- [`28fed58`](https://github.com/tobymao/sqlglot/commit/28fed586a39df83aade4792217743a1a859fd039) - **optimizer**: UnboundLocalError in scope module *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`809e05a`](https://github.com/tobymao/sqlglot/commit/809e05a743d5a2904a1d6f6813f24ca7549ac7ef) - **snowflake**: preserve STRTOK_TO_ARRAY roundtrip *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`df73a79`](https://github.com/tobymao/sqlglot/commit/df73a79a2ca3ba859b8aba5e3d0f6ed269874a63) - **tsql**: Retain limit clause in subquery expression. *(PR [#5227](https://github.com/tobymao/sqlglot/pull/5227) by [@MarcusRisanger](https://github.com/MarcusRisanger))*
- [`4f42d95`](https://github.com/tobymao/sqlglot/commit/4f42d951363f8c43a4c414dc21d0505d9c8e48bf) - **duckdb**: Normalize date parts in `exp.Extract` generation *(PR [#5229](https://github.com/tobymao/sqlglot/pull/5229) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5228](https://github.com/tobymao/sqlglot/issues/5228) opened by [@greybeam-bot](https://github.com/greybeam-bot)*
- [`1b4c083`](https://github.com/tobymao/sqlglot/commit/1b4c083fff8d7c44bf1dbba28c1225fa1e28c4d2) - **athena**: include Hive string escapes in the tokenizer *(PR [#5233](https://github.com/tobymao/sqlglot/pull/5233) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5232](https://github.com/tobymao/sqlglot/issues/5232) opened by [@ligfx](https://github.com/ligfx)*
- [`e7e38fe`](https://github.com/tobymao/sqlglot/commit/e7e38fe0e09f9affbff4ffa7023d0161e3a1ee49) - **optimizer**: resolve table "columns" in bigquery that produce structs *(PR [#5230](https://github.com/tobymao/sqlglot/pull/5230) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5207](https://github.com/tobymao/sqlglot/issues/5207) opened by [@Bladieblah](https://github.com/Bladieblah)*
- [`781539d`](https://github.com/tobymao/sqlglot/commit/781539d5cbe58142ed6688f1522fc4ed31da0a56) - **duckdb**: Generate correct DETACH syntax if IF EXISTS is set *(PR [#5235](https://github.com/tobymao/sqlglot/pull/5235) by [@erindru](https://github.com/erindru))*
- [`d3dc761`](https://github.com/tobymao/sqlglot/commit/d3dc761393146357a5d20c4d7992fd2a1ae5e6e2) - change comma to cross join when precedence is the same for all join types *(PR [#5240](https://github.com/tobymao/sqlglot/pull/5240) by [@georgesittas](https://github.com/georgesittas))*
- [`31814cd`](https://github.com/tobymao/sqlglot/commit/31814cddb0cf65caf29fbc45a31a9c865b7991c7) - **presto**: cast constructed timestamp literal to zone-aware type if needed *(PR [#5253](https://github.com/tobymao/sqlglot/pull/5253) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5252](https://github.com/tobymao/sqlglot/issues/5252) opened by [@agni-sairent](https://github.com/agni-sairent)*
- [`847248d`](https://github.com/tobymao/sqlglot/commit/847248dd1b66e3a8f60c23a4488be85dfdef4113) - format ADD CONSTRAINT clause properly fixes [#5260](https://github.com/tobymao/sqlglot/pull/5260) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`db4e0ec`](https://github.com/tobymao/sqlglot/commit/db4e0ece950a6a1f543d8ecad48a7d4b1d6872be) - **tsql**: convert information schema keywords to uppercase for consistency *(PR [#5263](https://github.com/tobymao/sqlglot/pull/5263) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`8de87e3`](https://github.com/tobymao/sqlglot/commit/8de87e3f755a40b600aa94ee2c30cf697ef7c43c) - **redshift**: handle scale parameter in to_timestamp *(PR [#5266](https://github.com/tobymao/sqlglot/pull/5266) by [@MatiasCasaliSplit](https://github.com/MatiasCasaliSplit))*
- [`e32f709`](https://github.com/tobymao/sqlglot/commit/e32f70992b5058efb93f5d2b6106fb00b810f576) - **hive**: Fix exp.PosExplode alias order *(PR [#5279](https://github.com/tobymao/sqlglot/pull/5279) by [@VaggelisD](https://github.com/VaggelisD))*
- [`3dd9f8e`](https://github.com/tobymao/sqlglot/commit/3dd9f8e78ecbfde0dd7fc6fefcc09c8cb99bcd7b) - **fabric**: Type mismatches and precision error *(PR [#5280](https://github.com/tobymao/sqlglot/pull/5280) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`9a95af1`](https://github.com/tobymao/sqlglot/commit/9a95af1c725cd70ffa8206f1d88452a7faab93b2) - **snowflake**: only cast strings to timestamp for TO_CHAR (TimeToStr) *(PR [#5283](https://github.com/tobymao/sqlglot/pull/5283) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5282](https://github.com/tobymao/sqlglot/issues/5282) opened by [@wedotech-ashley](https://github.com/wedotech-ashley)*
- [`8af4790`](https://github.com/tobymao/sqlglot/commit/8af479017ccde16049c897ae5d322d4a69843b65) - **tsql**: Fix parsing of ADD CONSTRAINT *(PR [#5288](https://github.com/tobymao/sqlglot/pull/5288) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4813](https://github.com/TobikoData/sqlmesh/issues/4813) opened by [@bnstewrt](https://github.com/bnstewrt)*
- [`18aea08`](https://github.com/tobymao/sqlglot/commit/18aea08f7dcaa887bcf29886cd3b3bc2850a3679) - **scope**: include bigquery unnest aliases in selected sources *(PR [#5285](https://github.com/tobymao/sqlglot/pull/5285) by [@georgesittas](https://github.com/georgesittas))*
- [`ba4a234`](https://github.com/tobymao/sqlglot/commit/ba4a234bfabdd8161b96a29436a50e0eb04c2dc2) - **fabric**: ignore Date cap *(PR [#5290](https://github.com/tobymao/sqlglot/pull/5290) by [@fresioAS](https://github.com/fresioAS))*
- [`dc03649`](https://github.com/tobymao/sqlglot/commit/dc03649bca0b7a090254976182a03c21dd2269ba) - **bigquery**: only coerce time var -like units into strings for DATE_TRUNC *(PR [#5291](https://github.com/tobymao/sqlglot/pull/5291) by [@georgesittas](https://github.com/georgesittas))*
- [`5724538`](https://github.com/tobymao/sqlglot/commit/5724538f278b2178114b88850251afd7c3db0dda) - **bigquery**: ARRAY_CONCAT type annotation *(PR [#5293](https://github.com/tobymao/sqlglot/pull/5293) by [@geooo109](https://github.com/geooo109))*
- [`0a6afcd`](https://github.com/tobymao/sqlglot/commit/0a6afcd90c663aaef9b385fc12ccd19dbf6388cc) - use re-entrant lock in dialects/__init__ to avoid deadlocks *(PR [#5322](https://github.com/tobymao/sqlglot/pull/5322) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5321](https://github.com/tobymao/sqlglot/issues/5321) opened by [@jc-5s](https://github.com/jc-5s)*
- [`599ca81`](https://github.com/tobymao/sqlglot/commit/599ca8101f48805098cbdf808ac2923a8246066b) - **parser**: avoid CTE values ALIAS gen, when ALIAS exists *(PR [#5323](https://github.com/tobymao/sqlglot/pull/5323) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5318](https://github.com/tobymao/sqlglot/issues/5318) opened by [@ankur334](https://github.com/ankur334)*
- [`5a0f589`](https://github.com/tobymao/sqlglot/commit/5a0f589a0fdb6743c3be2f98b74a34780f51332b) - **spark**: distinguish STORED AS from USING *(PR [#5320](https://github.com/tobymao/sqlglot/pull/5320) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5317](https://github.com/tobymao/sqlglot/issues/5317) opened by [@cosinequanon](https://github.com/cosinequanon)*
- [`cbc79c2`](https://github.com/tobymao/sqlglot/commit/cbc79c2a47c46370de0378b8bae61f4f3c17ca82) - preserve ORDER BY comments fixes [#5326](https://github.com/tobymao/sqlglot/pull/5326) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fa69583`](https://github.com/tobymao/sqlglot/commit/fa69583d8b4f5801d05c21a92b43dea272a3ef49) - **optimizer**: avoid qualifying CTE *(PR [#5327](https://github.com/tobymao/sqlglot/pull/5327) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5319](https://github.com/tobymao/sqlglot/issues/5319) opened by [@naamamaoz](https://github.com/naamamaoz)*
- [`29cce43`](https://github.com/tobymao/sqlglot/commit/29cce43e72451feeb8788ac2660658075bf59093) - comment lost before GROUP, JOIN and HAVING *(PR [#5338](https://github.com/tobymao/sqlglot/pull/5338) by [@chiiips](https://github.com/chiiips))*
- [`509b741`](https://github.com/tobymao/sqlglot/commit/509b74173f678842e7550c75c4d8d906c879fb12) - preserve multi-arg DECODE function instead of converting to CASE *(PR [#5352](https://github.com/tobymao/sqlglot/pull/5352) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5351](https://github.com/tobymao/sqlglot/issues/5351) opened by [@kentmaxwell](https://github.com/kentmaxwell)*
- [`188d446`](https://github.com/tobymao/sqlglot/commit/188d446ca65125c63bbfff96d15d91078deb6b4a) - **optimizer**: downstream column for PIVOT *(PR [#5363](https://github.com/tobymao/sqlglot/pull/5363) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5354](https://github.com/tobymao/sqlglot/issues/5354) opened by [@suresh-summation](https://github.com/suresh-summation)*

### :recycle: Refactors
- [`86c6b90`](https://github.com/tobymao/sqlglot/commit/86c6b90d21b204b4376639affa142e8cee509065) - **tsql**: XML_OPTIONS *(commit by [@geooo109](https://github.com/geooo109))*
- [`aac70aa`](https://github.com/tobymao/sqlglot/commit/aac70aaaa8d840c267129e2307ccb65058cef0c9) - **parser**: simpler _parse_pipe_syntax_select *(commit by [@geooo109](https://github.com/geooo109))*
- [`8d118ea`](https://github.com/tobymao/sqlglot/commit/8d118ead9c15e7b2b4b51b7cf93cab94e61c2625) - **athena**: route statements to hive/trino depending on their type *(PR [#5314](https://github.com/tobymao/sqlglot/pull/5314) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5267](https://github.com/tobymao/sqlglot/issues/5267) opened by [@cpcloud](https://github.com/cpcloud)*

### :wrench: Chores
- [`6910744`](https://github.com/tobymao/sqlglot/commit/6910744e6260793b3f9190782cf60fbbd9adcd38) - update py03 version *(PR [#5136](https://github.com/tobymao/sqlglot/pull/5136) by [@benfdking](https://github.com/benfdking))*
  - :arrow_lower_right: *addresses issue [#5134](https://github.com/tobymao/sqlglot/issues/5134) opened by [@mgorny](https://github.com/mgorny)*
- [`a56deab`](https://github.com/tobymao/sqlglot/commit/a56deabc2b9543209fb5e41f19c3bef89177a577) - bump sqlglotrs to 0.5.0 *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`5752a87`](https://github.com/tobymao/sqlglot/commit/5752a87406b736317e4dc5cce9ae05cbc5c19547) - udpate benchmarking framework *(PR [#5146](https://github.com/tobymao/sqlglot/pull/5146) by [@benfdking](https://github.com/benfdking))*
- [`0ae297a`](https://github.com/tobymao/sqlglot/commit/0ae297a01262cf323e225fe578bdeab2230c6fd5) - compare performance on main vs pr branch *(PR [#5149](https://github.com/tobymao/sqlglot/pull/5149) by [@georgesittas](https://github.com/georgesittas))*
- [`180963b`](https://github.com/tobymao/sqlglot/commit/180963b8cf25d9ff83d2347859b7f46398af5000) - handle pipe syntax unsupported operators more gracefully *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6c8d61a`](https://github.com/tobymao/sqlglot/commit/6c8d61ae1ef5b645835ccd683063845dd801e8d2) - include optimization benchmarks *(PR [#5152](https://github.com/tobymao/sqlglot/pull/5152) by [@georgesittas](https://github.com/georgesittas))*
- [`bc5c66c`](https://github.com/tobymao/sqlglot/commit/bc5c66c9210a472147d98a94c34b4bb582ade8b1) - Run benchmark job if /benchmark comment *(PR [#5164](https://github.com/tobymao/sqlglot/pull/5164) by [@VaggelisD](https://github.com/VaggelisD))*
- [`742b2b7`](https://github.com/tobymao/sqlglot/commit/742b2b770b88a2e901d2f84af00db821da441e4c) - Fix benchmark CI to include issue number *(PR [#5166](https://github.com/tobymao/sqlglot/pull/5166) by [@VaggelisD](https://github.com/VaggelisD))*
- [`64c37f1`](https://github.com/tobymao/sqlglot/commit/64c37f147366fe87ae187996ecb3c9a5afa7c264) - bump sqlglotrs to 0.6.0 *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`440590b`](https://github.com/tobymao/sqlglot/commit/440590bf92ab1281f50b96a1400cbca695d40f0c) - bump sqlglotrs to 0.6.1 *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`83de4e1`](https://github.com/tobymao/sqlglot/commit/83de4e11bc1547aa22b275b20c0326dfbe43b2b8) - improve benchmark result displaying *(PR [#5176](https://github.com/tobymao/sqlglot/pull/5176) by [@georgesittas](https://github.com/georgesittas))*
- [`5d5dc2f`](https://github.com/tobymao/sqlglot/commit/5d5dc2fa471bd53730e03ac8039804221949f843) - Clean up exp.ArrayIntersect PR *(PR [#5193](https://github.com/tobymao/sqlglot/pull/5193) by [@VaggelisD](https://github.com/VaggelisD))*
- [`ad8a4e7`](https://github.com/tobymao/sqlglot/commit/ad8a4e73e1a9e4234f0b711163fb49630acf736c) - refactor join mark elimination to use is_correlated_subquery *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`7dfb578`](https://github.com/tobymao/sqlglot/commit/7dfb5780fb242c82744dc1538077776ac624081e) - Refactor DETACH generation *(PR [#5237](https://github.com/tobymao/sqlglot/pull/5237) by [@VaggelisD](https://github.com/VaggelisD))*
- [`cc389fa`](https://github.com/tobymao/sqlglot/commit/cc389facb33f94a0d1f696f2ef9e92f298711894) - **optimizer**: annotate type SHA1, SHA256, SHA512 for BigQuery *(PR [#5347](https://github.com/tobymao/sqlglot/pull/5347) by [@geooo109](https://github.com/geooo109))*
- [`194850a`](https://github.com/tobymao/sqlglot/commit/194850a52497300a8f1d47f2306b67cdd11ffab6) - **exasol**: clean up TO_CHAR *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1abd461`](https://github.com/tobymao/sqlglot/commit/1abd461295830807c52f24d25ac6938095f54831) - bump min. supported version to python 3.9 *(PR [#5353](https://github.com/tobymao/sqlglot/pull/5353) by [@georgesittas](https://github.com/georgesittas))*


## [v26.33.0] - 2025-07-01
### :boom: BREAKING CHANGES
- due to [`d2f7c41`](https://github.com/tobymao/sqlglot/commit/d2f7c41f9f30f4cf0c74782be9be0cc6e75565f3) - add TypeOf / toTypeName support *(PR [#5315](https://github.com/tobymao/sqlglot/pull/5315) by [@ankur334](https://github.com/ankur334))*:

  add TypeOf / toTypeName support (#5315)


### :sparkles: New Features
- [`d2f7c41`](https://github.com/tobymao/sqlglot/commit/d2f7c41f9f30f4cf0c74782be9be0cc6e75565f3) - add TypeOf / toTypeName support *(PR [#5315](https://github.com/tobymao/sqlglot/pull/5315) by [@ankur334](https://github.com/ankur334))*

### :bug: Bug Fixes
- [`0a6afcd`](https://github.com/tobymao/sqlglot/commit/0a6afcd90c663aaef9b385fc12ccd19dbf6388cc) - use re-entrant lock in dialects/__init__ to avoid deadlocks *(PR [#5322](https://github.com/tobymao/sqlglot/pull/5322) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5321](https://github.com/tobymao/sqlglot/issues/5321) opened by [@jc-5s](https://github.com/jc-5s)*
- [`599ca81`](https://github.com/tobymao/sqlglot/commit/599ca8101f48805098cbdf808ac2923a8246066b) - **parser**: avoid CTE values ALIAS gen, when ALIAS exists *(PR [#5323](https://github.com/tobymao/sqlglot/pull/5323) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5318](https://github.com/tobymao/sqlglot/issues/5318) opened by [@ankur334](https://github.com/ankur334)*


## [v26.31.0] - 2025-06-26
### :boom: BREAKING CHANGES
- due to [`f2bf000`](https://github.com/tobymao/sqlglot/commit/f2bf000a410fb18531bb90ef1d767baf0e8bce7a) - avoid creating new alias for qualifying unpivot *(PR [#5121](https://github.com/tobymao/sqlglot/pull/5121) by [@geooo109](https://github.com/geooo109))*:

  avoid creating new alias for qualifying unpivot (#5121)

- due to [`a126ce8`](https://github.com/tobymao/sqlglot/commit/a126ce8a25287cf3531d815035fa3d567dc772fb) - make coalesce simplification optional, skip by default *(PR [#5123](https://github.com/tobymao/sqlglot/pull/5123) by [@barakalon](https://github.com/barakalon))*:

  make coalesce simplification optional, skip by default (#5123)

- due to [`6910744`](https://github.com/tobymao/sqlglot/commit/6910744e6260793b3f9190782cf60fbbd9adcd38) - update py03 version *(PR [#5136](https://github.com/tobymao/sqlglot/pull/5136) by [@benfdking](https://github.com/benfdking))*:

  update py03 version (#5136)

- due to [`a56deab`](https://github.com/tobymao/sqlglot/commit/a56deabc2b9543209fb5e41f19c3bef89177a577) - bump sqlglotrs to 0.5.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.5.0

- due to [`c484ca3`](https://github.com/tobymao/sqlglot/commit/c484ca39bad750a96b62e2edae85612cac66ba30) - recognize ARRAY_CONCAT_AGG as an aggregate function *(PR [#5141](https://github.com/tobymao/sqlglot/pull/5141) by [@georgesittas](https://github.com/georgesittas))*:

  recognize ARRAY_CONCAT_AGG as an aggregate function (#5141)

- due to [`72ce404`](https://github.com/tobymao/sqlglot/commit/72ce40405625239a0d6763d502e5af8b12abfe9b) - Refactor ALTER TABLE ADD parsing *(PR [#5144](https://github.com/tobymao/sqlglot/pull/5144) by [@VaggelisD](https://github.com/VaggelisD))*:

  Refactor ALTER TABLE ADD parsing (#5144)

- due to [`e73ddb7`](https://github.com/tobymao/sqlglot/commit/e73ddb733b7f120ae74054e6d4dc7d458f59ac50) - preserve TIMESTAMP on roundtrip *(PR [#5145](https://github.com/tobymao/sqlglot/pull/5145) by [@georgesittas](https://github.com/georgesittas))*:

  preserve TIMESTAMP on roundtrip (#5145)

- due to [`f6124c6`](https://github.com/tobymao/sqlglot/commit/f6124c6343f67563fc19f617891ecfc145a642db) - return token vector in `tokenize` even on failure *(PR [#5155](https://github.com/tobymao/sqlglot/pull/5155) by [@georgesittas](https://github.com/georgesittas))*:

  return token vector in `tokenize` even on failure (#5155)

- due to [`64c37f1`](https://github.com/tobymao/sqlglot/commit/64c37f147366fe87ae187996ecb3c9a5afa7c264) - bump sqlglotrs to 0.6.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.6.0

- due to [`434c45b`](https://github.com/tobymao/sqlglot/commit/434c45b547c3a5ea155dc8d7da2baab326eb6d4f) - improve support for ENDSWITH closes [#5170](https://github.com/tobymao/sqlglot/pull/5170) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  improve support for ENDSWITH closes #5170

- due to [`bc001ce`](https://github.com/tobymao/sqlglot/commit/bc001cef4c907d8fa421d3190b4fa91865d9ff6c) - Add support for ANY_VALUE for versions 16+ *(PR [#5179](https://github.com/tobymao/sqlglot/pull/5179) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for ANY_VALUE for versions 16+ (#5179)

- due to [`6a2cb39`](https://github.com/tobymao/sqlglot/commit/6a2cb39d0ceec091dc4fc228f26d4f457729a3cf) - virtual column with AS(expr) as ComputedColumnConstraint *(PR [#5180](https://github.com/tobymao/sqlglot/pull/5180) by [@geooo109](https://github.com/geooo109))*:

  virtual column with AS(expr) as ComputedColumnConstraint (#5180)

- due to [`29e2f1d`](https://github.com/tobymao/sqlglot/commit/29e2f1d89c095c9fab0944a6962c99bd745c2c91) - Array_intersection transpilation support *(PR [#5186](https://github.com/tobymao/sqlglot/pull/5186) by [@HarishRavi96](https://github.com/HarishRavi96))*:

  Array_intersection transpilation support (#5186)

- due to [`ac6555b`](https://github.com/tobymao/sqlglot/commit/ac6555b4d6c162ef7b14b63307d01fd560138ea0) - preserve DIV binary operator, fixes [#5198](https://github.com/tobymao/sqlglot/pull/5198) *(PR [#5199](https://github.com/tobymao/sqlglot/pull/5199) by [@georgesittas](https://github.com/georgesittas))*:

  preserve DIV binary operator, fixes #5198 (#5199)

- due to [`dfdd84b`](https://github.com/tobymao/sqlglot/commit/dfdd84bbc50da70f40a17b39935f8171d961f7d2) - CTEs instead of subqueries for pipe syntax *(PR [#5205](https://github.com/tobymao/sqlglot/pull/5205) by [@geooo109](https://github.com/geooo109))*:

  CTEs instead of subqueries for pipe syntax (#5205)

- due to [`5f95299`](https://github.com/tobymao/sqlglot/commit/5f9529940d83e89704f7d25eda63cd73fdb503ae) - support multi-part (>3) dotted functions *(PR [#5211](https://github.com/tobymao/sqlglot/pull/5211) by [@georgesittas](https://github.com/georgesittas))*:

  support multi-part (>3) dotted functions (#5211)

- due to [`02afa2a`](https://github.com/tobymao/sqlglot/commit/02afa2a1941fc67086d50dffac2857262f1c3c4f) - Preserve quoting for UDT *(PR [#5216](https://github.com/tobymao/sqlglot/pull/5216) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve quoting for UDT (#5216)

- due to [`44297f1`](https://github.com/tobymao/sqlglot/commit/44297f1c5c8c2cb16fe77c318312f417b4281708) - JOIN pipe syntax, Set Operators as CTEs *(PR [#5215](https://github.com/tobymao/sqlglot/pull/5215) by [@geooo109](https://github.com/geooo109))*:

  JOIN pipe syntax, Set Operators as CTEs (#5215)

- due to [`4f42d95`](https://github.com/tobymao/sqlglot/commit/4f42d951363f8c43a4c414dc21d0505d9c8e48bf) - Normalize date parts in `exp.Extract` generation *(PR [#5229](https://github.com/tobymao/sqlglot/pull/5229) by [@VaggelisD](https://github.com/VaggelisD))*:

  Normalize date parts in `exp.Extract` generation (#5229)

- due to [`e7e38fe`](https://github.com/tobymao/sqlglot/commit/e7e38fe0e09f9affbff4ffa7023d0161e3a1ee49) - resolve table "columns" in bigquery that produce structs *(PR [#5230](https://github.com/tobymao/sqlglot/pull/5230) by [@georgesittas](https://github.com/georgesittas))*:

  resolve table "columns" in bigquery that produce structs (#5230)

- due to [`d3dc761`](https://github.com/tobymao/sqlglot/commit/d3dc761393146357a5d20c4d7992fd2a1ae5e6e2) - change comma to cross join when precedence is the same for all join types *(PR [#5240](https://github.com/tobymao/sqlglot/pull/5240) by [@georgesittas](https://github.com/georgesittas))*:

  change comma to cross join when precedence is the same for all join types (#5240)

- due to [`e7c217e`](https://github.com/tobymao/sqlglot/commit/e7c217ef08e5811e7dad2b3d26dbaa9f02114e38) - transpile from/to dbms_random.value *(PR [#5242](https://github.com/tobymao/sqlglot/pull/5242) by [@georgesittas](https://github.com/georgesittas))*:

  transpile from/to dbms_random.value (#5242)

- due to [`31814cd`](https://github.com/tobymao/sqlglot/commit/31814cddb0cf65caf29fbc45a31a9c865b7991c7) - cast constructed timestamp literal to zone-aware type if needed *(PR [#5253](https://github.com/tobymao/sqlglot/pull/5253) by [@georgesittas](https://github.com/georgesittas))*:

  cast constructed timestamp literal to zone-aware type if needed (#5253)

- due to [`db4e0ec`](https://github.com/tobymao/sqlglot/commit/db4e0ece950a6a1f543d8ecad48a7d4b1d6872be) - convert information schema keywords to uppercase for consistency *(PR [#5263](https://github.com/tobymao/sqlglot/pull/5263) by [@mattiasthalen](https://github.com/mattiasthalen))*:

  convert information schema keywords to uppercase for consistency (#5263)

- due to [`eea1570`](https://github.com/tobymao/sqlglot/commit/eea1570ba530517a95699092ccd9ce6a856f5e84) - add support for SYSDATETIMEOFFSET closes [#5272](https://github.com/tobymao/sqlglot/pull/5272) *(PR [#5273](https://github.com/tobymao/sqlglot/pull/5273) by [@georgesittas](https://github.com/georgesittas))*:

  add support for SYSDATETIMEOFFSET closes #5272 (#5273)

- due to [`3d3ccc5`](https://github.com/tobymao/sqlglot/commit/3d3ccc52a40536b9ac4e974f1592dffe5a7568f9) - Transpile exp.PosExplode pos column alias *(PR [#5274](https://github.com/tobymao/sqlglot/pull/5274) by [@VaggelisD](https://github.com/VaggelisD))*:

  Transpile exp.PosExplode pos column alias (#5274)

- due to [`9a95af1`](https://github.com/tobymao/sqlglot/commit/9a95af1c725cd70ffa8206f1d88452a7faab93b2) - only cast strings to timestamp for TO_CHAR (TimeToStr) *(PR [#5283](https://github.com/tobymao/sqlglot/pull/5283) by [@georgesittas](https://github.com/georgesittas))*:

  only cast strings to timestamp for TO_CHAR (TimeToStr) (#5283)

- due to [`8af4790`](https://github.com/tobymao/sqlglot/commit/8af479017ccde16049c897ae5d322d4a69843b65) - Fix parsing of ADD CONSTRAINT *(PR [#5288](https://github.com/tobymao/sqlglot/pull/5288) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix parsing of ADD CONSTRAINT (#5288)

- due to [`18aea08`](https://github.com/tobymao/sqlglot/commit/18aea08f7dcaa887bcf29886cd3b3bc2850a3679) - include bigquery unnest aliases in selected sources *(PR [#5285](https://github.com/tobymao/sqlglot/pull/5285) by [@georgesittas](https://github.com/georgesittas))*:

  include bigquery unnest aliases in selected sources (#5285)

- due to [`0ff95c5`](https://github.com/tobymao/sqlglot/commit/0ff95c5903907c9ab30b7850bb3b962bc6da2bab) - add parsing/transpilation support for the REPLACE function *(PR [#5289](https://github.com/tobymao/sqlglot/pull/5289) by [@rahulj51](https://github.com/rahulj51))*:

  add parsing/transpilation support for the REPLACE function (#5289)

- due to [`dc03649`](https://github.com/tobymao/sqlglot/commit/dc03649bca0b7a090254976182a03c21dd2269ba) - only coerce time var -like units into strings for DATE_TRUNC *(PR [#5291](https://github.com/tobymao/sqlglot/pull/5291) by [@georgesittas](https://github.com/georgesittas))*:

  only coerce time var -like units into strings for DATE_TRUNC (#5291)


### :sparkles: New Features
- [`82c50ce`](https://github.com/tobymao/sqlglot/commit/82c50ce68d9a1ad25095086ae3645f5c4996c18b) - **duckdb**: extend time travel parsing to take VERSION into account *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`bb4f428`](https://github.com/tobymao/sqlglot/commit/bb4f4283b53bc060a8c7e0f12c1e7ef5b521c4e6) - bubble up comments nested under a Bracket, fixes [#5131](https://github.com/tobymao/sqlglot/pull/5131) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`9f318eb`](https://github.com/tobymao/sqlglot/commit/9f318ebe4502bb484a34873252cf4a40c7e440e4) - **snowflake**: Transpile BQ's `ARRAY(SELECT AS STRUCT ...)` *(PR [#5140](https://github.com/tobymao/sqlglot/pull/5140) by [@VaggelisD](https://github.com/VaggelisD))*
- [`93b402a`](https://github.com/tobymao/sqlglot/commit/93b402abc74e642ed312db585b33315674a450cd) - **parser**: support SELECT, FROM, WHERE with pipe syntax *(PR [#5128](https://github.com/tobymao/sqlglot/pull/5128) by [@geooo109](https://github.com/geooo109))*
- [`1a8e78b`](https://github.com/tobymao/sqlglot/commit/1a8e78bd84e006023d5d3ea561504587dfbb55a9) - **parser**: ORDER BY with pipe syntax *(PR [#5153](https://github.com/tobymao/sqlglot/pull/5153) by [@geooo109](https://github.com/geooo109))*
- [`966ad95`](https://github.com/tobymao/sqlglot/commit/966ad95432d5f8e29ade36d8271a5c489c207324) - **tsql**: add convert style 126 *(PR [#5157](https://github.com/tobymao/sqlglot/pull/5157) by [@pa1ch](https://github.com/pa1ch))*
- [`b7ac6ff`](https://github.com/tobymao/sqlglot/commit/b7ac6ff4680ff619be4b0ddb01f61f916ed09d58) - **parser**: LIMIT/OFFSET pipe syntax *(PR [#5159](https://github.com/tobymao/sqlglot/pull/5159) by [@geooo109](https://github.com/geooo109))*
- [`cfc158d`](https://github.com/tobymao/sqlglot/commit/cfc158d753d4f43d12c3b502633d29e43dcc5569) - **snowflake**: transpile STRTOK_TO_ARRAY to duckdb *(PR [#5165](https://github.com/tobymao/sqlglot/pull/5165) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5160](https://github.com/tobymao/sqlglot/issues/5160) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`ff0f30b`](https://github.com/tobymao/sqlglot/commit/ff0f30bcf7d0d74b26a703eaa632e1be15b3c001) - support ARRAY_REMOVE *(PR [#5163](https://github.com/tobymao/sqlglot/pull/5163) by [@geooo109](https://github.com/geooo109))*
- [`9cac01f`](https://github.com/tobymao/sqlglot/commit/9cac01f6b4a5c93b55f5b68f21cb104932880a0e) - **tsql**: support FOR XML syntax *(PR [#5167](https://github.com/tobymao/sqlglot/pull/5167) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5161](https://github.com/tobymao/sqlglot/issues/5161) opened by [@codykonior](https://github.com/codykonior)*
- [`8b5129f`](https://github.com/tobymao/sqlglot/commit/8b5129f288880032f0bf9d649984d82314039af1) - **postgres**: improve pretty-formatting of ARRAY[...] *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`964b4a1`](https://github.com/tobymao/sqlglot/commit/964b4a1e367e00e243b80edf677cd48d453ed31e) - add line/col position for Star *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`434c45b`](https://github.com/tobymao/sqlglot/commit/434c45b547c3a5ea155dc8d7da2baab326eb6d4f) - improve support for ENDSWITH closes [#5170](https://github.com/tobymao/sqlglot/pull/5170) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`63f9cb4`](https://github.com/tobymao/sqlglot/commit/63f9cb4b158b88574136b32241ee60254352c9e6) - **sqlglotrs**: match the Python implementation of __repr__ for tokens *(PR [#5172](https://github.com/tobymao/sqlglot/pull/5172) by [@georgesittas](https://github.com/georgesittas))*
- [`c007afa`](https://github.com/tobymao/sqlglot/commit/c007afa23831e9bd86f401d85260e15edf00328f) - support Star instance as first arg of exp.column helper *(PR [#5177](https://github.com/tobymao/sqlglot/pull/5177) by [@georgesittas](https://github.com/georgesittas))*
- [`bc001ce`](https://github.com/tobymao/sqlglot/commit/bc001cef4c907d8fa421d3190b4fa91865d9ff6c) - **postgres**: Add support for ANY_VALUE for versions 16+ *(PR [#5179](https://github.com/tobymao/sqlglot/pull/5179) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4674](https://github.com/TobikoData/sqlmesh/issues/4674) opened by [@petrikoro](https://github.com/petrikoro)*
- [`ba05ff6`](https://github.com/tobymao/sqlglot/commit/ba05ff67127e056d567fc2c1d3bcc8e3dcce7b7e) - **parser**: AGGREGATE with GROUP AND ORDER BY pipe syntax *(PR [#5171](https://github.com/tobymao/sqlglot/pull/5171) by [@geooo109](https://github.com/geooo109))*
- [`26077a4`](https://github.com/tobymao/sqlglot/commit/26077a47d9db750f44ab1baf9a434596b5bb613b) - make to_table more lenient *(PR [#5183](https://github.com/tobymao/sqlglot/pull/5183) by [@georgesittas](https://github.com/georgesittas))*
- [`29e2f1d`](https://github.com/tobymao/sqlglot/commit/29e2f1d89c095c9fab0944a6962c99bd745c2c91) - Array_intersection transpilation support *(PR [#5186](https://github.com/tobymao/sqlglot/pull/5186) by [@HarishRavi96](https://github.com/HarishRavi96))*
- [`d86a114`](https://github.com/tobymao/sqlglot/commit/d86a1147aeb866ed0ab2c342914ecf8cbfadac8a) - **sqlite**: implement RESPECT/IGNORE NULLS in first_value() *(PR [#5185](https://github.com/tobymao/sqlglot/pull/5185) by [@NickCrews](https://github.com/NickCrews))*
- [`1d50fca`](https://github.com/tobymao/sqlglot/commit/1d50fca8ffc34e4acbc1b791c4cdf5f184a748db) - improve transpilation of st_point and st_distance *(PR [#5194](https://github.com/tobymao/sqlglot/pull/5194) by [@georgesittas](https://github.com/georgesittas))*
- [`756ec3b`](https://github.com/tobymao/sqlglot/commit/756ec3b65db1eb2572d017a3ac12ece6bb44c726) - **parser**: SET OPERATORS with pipe syntax *(PR [#5184](https://github.com/tobymao/sqlglot/pull/5184) by [@geooo109](https://github.com/geooo109))*
- [`c20f85e`](https://github.com/tobymao/sqlglot/commit/c20f85e3e171e502fc51f74894d3313f0ad61535) - **spark**: support ALTER ADD PARTITION *(PR [#5208](https://github.com/tobymao/sqlglot/pull/5208) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5204](https://github.com/tobymao/sqlglot/issues/5204) opened by [@cosinequanon](https://github.com/cosinequanon)*
- [`44297f1`](https://github.com/tobymao/sqlglot/commit/44297f1c5c8c2cb16fe77c318312f417b4281708) - **parser**: JOIN pipe syntax, Set Operators as CTEs *(PR [#5215](https://github.com/tobymao/sqlglot/pull/5215) by [@geooo109](https://github.com/geooo109))*
- [`21cd3eb`](https://github.com/tobymao/sqlglot/commit/21cd3ebf5d0b57f5b102c5aadc3b24a598ebe918) - **parser**: PIVOT/UNPIVOT pipe syntax *(PR [#5222](https://github.com/tobymao/sqlglot/pull/5222) by [@geooo109](https://github.com/geooo109))*
- [`97f5822`](https://github.com/tobymao/sqlglot/commit/97f58226fc8815b23787b7b8699ea71f58268560) - **parser**: AS pipe syntax *(PR [#5224](https://github.com/tobymao/sqlglot/pull/5224) by [@geooo109](https://github.com/geooo109))*
- [`a7e7fee`](https://github.com/tobymao/sqlglot/commit/a7e7feef02a77fe8606f3f482bad91230fa637f4) - **parser**: EXTEND pipe syntax *(PR [#5225](https://github.com/tobymao/sqlglot/pull/5225) by [@geooo109](https://github.com/geooo109))*
- [`c1cb9f8`](https://github.com/tobymao/sqlglot/commit/c1cb9f8f682080f7a06c387219d79c6d068b6dbe) - **snowflake**: add autoincrement order clause support *(PR [#5223](https://github.com/tobymao/sqlglot/pull/5223) by [@dmaresma](https://github.com/dmaresma))*
- [`91afe4c`](https://github.com/tobymao/sqlglot/commit/91afe4cfd7b3f427e4c0b298075e867b8a1bbe55) - **parser**: TABLESAMPLE pipe syntax *(PR [#5231](https://github.com/tobymao/sqlglot/pull/5231) by [@geooo109](https://github.com/geooo109))*
- [`62da84a`](https://github.com/tobymao/sqlglot/commit/62da84acce7f44802dca26a9357a16115e21fabf) - **snowflake**: improve transpilation of unnested object lookup *(PR [#5234](https://github.com/tobymao/sqlglot/pull/5234) by [@georgesittas](https://github.com/georgesittas))*
- [`2c60453`](https://github.com/tobymao/sqlglot/commit/2c604537ba83dee74e9ced7e216673ecc70fe487) - **parser**: DROP pipe syntax *(PR [#5226](https://github.com/tobymao/sqlglot/pull/5226) by [@geooo109](https://github.com/geooo109))*
- [`9885729`](https://github.com/tobymao/sqlglot/commit/988572954135c68dc021b992c815024ce3debaff) - **parser**: SET pipe syntax *(PR [#5236](https://github.com/tobymao/sqlglot/pull/5236) by [@geooo109](https://github.com/geooo109))*
- [`e7c217e`](https://github.com/tobymao/sqlglot/commit/e7c217ef08e5811e7dad2b3d26dbaa9f02114e38) - **oracle**: transpile from/to dbms_random.value *(PR [#5242](https://github.com/tobymao/sqlglot/pull/5242) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5241](https://github.com/tobymao/sqlglot/issues/5241) opened by [@Akshat-2512](https://github.com/Akshat-2512)*
- [`0d19544`](https://github.com/tobymao/sqlglot/commit/0d19544317c1056b17fb089d4be9b5bddfe6feb3) - add Microsoft Fabric dialect, a case sensitive version of TSQL *(PR [#5247](https://github.com/tobymao/sqlglot/pull/5247) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`249dbc9`](https://github.com/tobymao/sqlglot/commit/249dbc906adc6b20932dc8efe83f6f4d23ef8c1e) - **parser**: start with SELECT and nested pipe syntax *(PR [#5248](https://github.com/tobymao/sqlglot/pull/5248) by [@geooo109](https://github.com/geooo109))*
- [`f5b5b93`](https://github.com/tobymao/sqlglot/commit/f5b5b9338eb92b7aa2c9b4c92c6138c2c05e1c40) - **fabric**: implement type mappings for unsupported Fabric types *(PR [#5249](https://github.com/tobymao/sqlglot/pull/5249) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`78fcea1`](https://github.com/tobymao/sqlglot/commit/78fcea13b5eb1734a15a254875bc80ad8063b0b0) - **spark, databricks**: parse brackets as placeholder *(PR [#5256](https://github.com/tobymao/sqlglot/pull/5256) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5251](https://github.com/tobymao/sqlglot/issues/5251) opened by [@aersam](https://github.com/aersam)*
- [`7d71387`](https://github.com/tobymao/sqlglot/commit/7d7138780db82e7a75949d29282b944e739ad99d) - **fabric**: Add precision cap to temporal data types *(PR [#5250](https://github.com/tobymao/sqlglot/pull/5250) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`e8cf793`](https://github.com/tobymao/sqlglot/commit/e8cf79305d398f25640ef3c07dd8b32997cb0167) - **duckdb**: Transpile Snowflake's TO_CHAR if format is in Snowflake.TIME_MAPPING *(PR [#5257](https://github.com/tobymao/sqlglot/pull/5257) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5255](https://github.com/tobymao/sqlglot/issues/5255) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`0cdfe64`](https://github.com/tobymao/sqlglot/commit/0cdfe642e3cb996c5ac48cc055af2862340dcf56) - add Exasol dialect (pass 1: string type mapping) *(PR [#5264](https://github.com/tobymao/sqlglot/pull/5264) by [@nnamdi16](https://github.com/nnamdi16))*
- [`eea1570`](https://github.com/tobymao/sqlglot/commit/eea1570ba530517a95699092ccd9ce6a856f5e84) - **tsql**: add support for SYSDATETIMEOFFSET closes [#5272](https://github.com/tobymao/sqlglot/pull/5272) *(PR [#5273](https://github.com/tobymao/sqlglot/pull/5273) by [@georgesittas](https://github.com/georgesittas))*
- [`3d3ccc5`](https://github.com/tobymao/sqlglot/commit/3d3ccc52a40536b9ac4e974f1592dffe5a7568f9) - **hive**: Transpile exp.PosExplode pos column alias *(PR [#5274](https://github.com/tobymao/sqlglot/pull/5274) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5271](https://github.com/tobymao/sqlglot/issues/5271) opened by [@charlie-liner](https://github.com/charlie-liner)*
- [`1c48c09`](https://github.com/tobymao/sqlglot/commit/1c48c09fd836db40bba6c46d0e9969937ce96587) - **exasol**: added datatype mappings and test for exasol dialect. *(PR [#5270](https://github.com/tobymao/sqlglot/pull/5270) by [@nnamdi16](https://github.com/nnamdi16))*
- [`883fcb1`](https://github.com/tobymao/sqlglot/commit/883fcb137583f6d36f3a70a1343780bb40bf6f81) - **databricks**: GROUP_CONCAT to LISTAGG *(PR [#5284](https://github.com/tobymao/sqlglot/pull/5284) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5281](https://github.com/tobymao/sqlglot/issues/5281) opened by [@wKollendorf](https://github.com/wKollendorf)*
- [`21ef897`](https://github.com/tobymao/sqlglot/commit/21ef8974426d9f3562ade0bd2c8448bb440bee27) - **fabric**: implement UnixToTime transformation to DATEADD syntax *(PR [#5269](https://github.com/tobymao/sqlglot/pull/5269) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`0ff95c5`](https://github.com/tobymao/sqlglot/commit/0ff95c5903907c9ab30b7850bb3b962bc6da2bab) - add parsing/transpilation support for the REPLACE function *(PR [#5289](https://github.com/tobymao/sqlglot/pull/5289) by [@rahulj51](https://github.com/rahulj51))*
- [`1b0631c`](https://github.com/tobymao/sqlglot/commit/1b0631c2b4516a9ceb81af6173790dd09269b635) - **exasol**: implemented the Mod function *(PR [#5292](https://github.com/tobymao/sqlglot/pull/5292) by [@nnamdi16](https://github.com/nnamdi16))*

### :bug: Bug Fixes
- [`f2bf000`](https://github.com/tobymao/sqlglot/commit/f2bf000a410fb18531bb90ef1d767baf0e8bce7a) - **optimizer**: avoid creating new alias for qualifying unpivot *(PR [#5121](https://github.com/tobymao/sqlglot/pull/5121) by [@geooo109](https://github.com/geooo109))*
- [`a126ce8`](https://github.com/tobymao/sqlglot/commit/a126ce8a25287cf3531d815035fa3d567dc772fb) - **optimizer**: make coalesce simplification optional, skip by default *(PR [#5123](https://github.com/tobymao/sqlglot/pull/5123) by [@barakalon](https://github.com/barakalon))*
- [`f7401fd`](https://github.com/tobymao/sqlglot/commit/f7401fdc29a35738eb23f424ceba03463a4d8af9) - **bigquery**: avoid getting stuck in infinite loop when parsing tables *(PR [#5130](https://github.com/tobymao/sqlglot/pull/5130) by [@georgesittas](https://github.com/georgesittas))*
- [`e9b3156`](https://github.com/tobymao/sqlglot/commit/e9b3156aa1ed95fdee4c6b419134d8ca746964b6) - **athena**: Handle transpilation of FileFormatProperty from dialects that treat it as a variable and not a string literal *(PR [#5133](https://github.com/tobymao/sqlglot/pull/5133) by [@erindru](https://github.com/erindru))*
- [`a3fccd9`](https://github.com/tobymao/sqlglot/commit/a3fccd9be294499b53477da931f8b097cdbe09fc) - **snowflake**: generate SELECT for UNNEST without JOIN or FROM *(PR [#5138](https://github.com/tobymao/sqlglot/pull/5138) by [@geooo109](https://github.com/geooo109))*
- [`993919d`](https://github.com/tobymao/sqlglot/commit/993919d05d5d3c814471607b56831bb65d349eb4) - **snowflake**: Properly transpile ARRAY_AGG, IGNORE/RESPECT NULLS *(PR [#5137](https://github.com/tobymao/sqlglot/pull/5137) by [@VaggelisD](https://github.com/VaggelisD))*
- [`6e57619`](https://github.com/tobymao/sqlglot/commit/6e57619f85375e789bb39a6478aa01cd7c7758f0) - **snowflake**: Transpile ISOWEEK to WEEKISO *(PR [#5139](https://github.com/tobymao/sqlglot/pull/5139) by [@VaggelisD](https://github.com/VaggelisD))*
- [`c484ca3`](https://github.com/tobymao/sqlglot/commit/c484ca39bad750a96b62e2edae85612cac66ba30) - **bigquery**: recognize ARRAY_CONCAT_AGG as an aggregate function *(PR [#5141](https://github.com/tobymao/sqlglot/pull/5141) by [@georgesittas](https://github.com/georgesittas))*
- [`f3aeb37`](https://github.com/tobymao/sqlglot/commit/f3aeb374351a0b1b3c75945718d8ea42f8926b62) - **tsql**: properly parse and generate ALTER SET *(PR [#5143](https://github.com/tobymao/sqlglot/pull/5143) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5135](https://github.com/tobymao/sqlglot/issues/5135) opened by [@codykonior](https://github.com/codykonior)*
- [`72ce404`](https://github.com/tobymao/sqlglot/commit/72ce40405625239a0d6763d502e5af8b12abfe9b) - Refactor ALTER TABLE ADD parsing *(PR [#5144](https://github.com/tobymao/sqlglot/pull/5144) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5129](https://github.com/tobymao/sqlglot/issues/5129) opened by [@Mevrael](https://github.com/Mevrael)*
- [`e73ddb7`](https://github.com/tobymao/sqlglot/commit/e73ddb733b7f120ae74054e6d4dc7d458f59ac50) - **mysql**: preserve TIMESTAMP on roundtrip *(PR [#5145](https://github.com/tobymao/sqlglot/pull/5145) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5127](https://github.com/tobymao/sqlglot/issues/5127) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`4f8c73d`](https://github.com/tobymao/sqlglot/commit/4f8c73d60eecebc601c60ee8c7819458435e34b8) - **hive**: STRUCT column names and data type should be separated by ':' in hive *(PR [#5147](https://github.com/tobymao/sqlglot/pull/5147) by [@tsamaras](https://github.com/tsamaras))*
- [`e2a488f`](https://github.com/tobymao/sqlglot/commit/e2a488f48f3e036566462463bbc58cc6a1c7492e) - Error on columns mismatch in pushdown_projections ignores dialect *(PR [#5151](https://github.com/tobymao/sqlglot/pull/5151) by [@snovik75](https://github.com/snovik75))*
- [`1a35365`](https://github.com/tobymao/sqlglot/commit/1a35365a3bb1ef56e8da0023271cbe3108e0ccb1) - avoid generating nested comments when not supported *(PR [#5158](https://github.com/tobymao/sqlglot/pull/5158) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5132](https://github.com/tobymao/sqlglot/issues/5132) opened by [@patricksurry](https://github.com/patricksurry)*
- [`f6124c6`](https://github.com/tobymao/sqlglot/commit/f6124c6343f67563fc19f617891ecfc145a642db) - **rust-tokenizer**: return token vector in `tokenize` even on failure *(PR [#5155](https://github.com/tobymao/sqlglot/pull/5155) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5148](https://github.com/tobymao/sqlglot/issues/5148) opened by [@kamoser](https://github.com/kamoser)*
- [`760a606`](https://github.com/tobymao/sqlglot/commit/760a6062d5f259488e471af9c1d33e200066e9dc) - **postgres**: support decimal values in INTERVAL expressions fixes [#5168](https://github.com/tobymao/sqlglot/pull/5168) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6a2cb39`](https://github.com/tobymao/sqlglot/commit/6a2cb39d0ceec091dc4fc228f26d4f457729a3cf) - **parser**: virtual column with AS(expr) as ComputedColumnConstraint *(PR [#5180](https://github.com/tobymao/sqlglot/pull/5180) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5173](https://github.com/tobymao/sqlglot/issues/5173) opened by [@suyah](https://github.com/suyah)*
- [`c87ae02`](https://github.com/tobymao/sqlglot/commit/c87ae02aa263be8463ca7283ebd090385a4bfd59) - **sqlite**: Add REPLACE to command tokens *(PR [#5192](https://github.com/tobymao/sqlglot/pull/5192) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5187](https://github.com/tobymao/sqlglot/issues/5187) opened by [@stefanmalanik](https://github.com/stefanmalanik)*
- [`4b89afd`](https://github.com/tobymao/sqlglot/commit/4b89afdcc0063e70cbc64165c7f1f5102afaa87c) - **starrocks**: array_agg_transpilation_fix *(PR [#5190](https://github.com/tobymao/sqlglot/pull/5190) by [@Swathiraj23](https://github.com/Swathiraj23))*
- [`461b054`](https://github.com/tobymao/sqlglot/commit/461b0548832ab8d916c3a6638f27a49f681109fe) - **postgres**: support use_spheroid argument in ST_DISTANCE *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`99bbae3`](https://github.com/tobymao/sqlglot/commit/99bbae370329c5f5cd132b711c714359cf96ba58) - **sqlite**: allow ALTER RENAME without COLUMN keyword fixes [#5195](https://github.com/tobymao/sqlglot/pull/5195) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`ac6555b`](https://github.com/tobymao/sqlglot/commit/ac6555b4d6c162ef7b14b63307d01fd560138ea0) - **hive**: preserve DIV binary operator, fixes [#5198](https://github.com/tobymao/sqlglot/pull/5198) *(PR [#5199](https://github.com/tobymao/sqlglot/pull/5199) by [@georgesittas](https://github.com/georgesittas))*
- [`d0eeb26`](https://github.com/tobymao/sqlglot/commit/d0eeb2639e771e8f8b6feabd41c65f16ed5a9829) - eliminate_join_marks has multiple issues fixes [#5188](https://github.com/tobymao/sqlglot/pull/5188) *(PR [#5189](https://github.com/tobymao/sqlglot/pull/5189) by [@snovik75](https://github.com/snovik75))*
  - :arrow_lower_right: *fixes issue [#5188](https://github.com/tobymao/sqlglot/issues/5188) opened by [@snovik75](https://github.com/snovik75)*
- [`dfdd84b`](https://github.com/tobymao/sqlglot/commit/dfdd84bbc50da70f40a17b39935f8171d961f7d2) - **parser**: CTEs instead of subqueries for pipe syntax *(PR [#5205](https://github.com/tobymao/sqlglot/pull/5205) by [@geooo109](https://github.com/geooo109))*
- [`77e9d9a`](https://github.com/tobymao/sqlglot/commit/77e9d9a0269e2013379967cf2f46fbd79c036277) - **mysql**: properly parse STORED/VIRTUAL computed columns *(PR [#5210](https://github.com/tobymao/sqlglot/pull/5210) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5203](https://github.com/tobymao/sqlglot/issues/5203) opened by [@mdebski](https://github.com/mdebski)*
- [`5f95299`](https://github.com/tobymao/sqlglot/commit/5f9529940d83e89704f7d25eda63cd73fdb503ae) - **parser**: support multi-part (>3) dotted functions *(PR [#5211](https://github.com/tobymao/sqlglot/pull/5211) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5200](https://github.com/tobymao/sqlglot/issues/5200) opened by [@mateuszpoleski](https://github.com/mateuszpoleski)*
- [`02afa2a`](https://github.com/tobymao/sqlglot/commit/02afa2a1941fc67086d50dffac2857262f1c3c4f) - **postgres**: Preserve quoting for UDT *(PR [#5216](https://github.com/tobymao/sqlglot/pull/5216) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5212](https://github.com/tobymao/sqlglot/issues/5212) opened by [@NickCrews](https://github.com/NickCrews)*
- [`f37c0b1`](https://github.com/tobymao/sqlglot/commit/f37c0b1197321dd610648ce652a171ab063deeeb) - **snowflake**: ensure a standalone GET() expression can be parsed *(PR [#5219](https://github.com/tobymao/sqlglot/pull/5219) by [@georgesittas](https://github.com/georgesittas))*
- [`28fed58`](https://github.com/tobymao/sqlglot/commit/28fed586a39df83aade4792217743a1a859fd039) - **optimizer**: UnboundLocalError in scope module *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`809e05a`](https://github.com/tobymao/sqlglot/commit/809e05a743d5a2904a1d6f6813f24ca7549ac7ef) - **snowflake**: preserve STRTOK_TO_ARRAY roundtrip *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`df73a79`](https://github.com/tobymao/sqlglot/commit/df73a79a2ca3ba859b8aba5e3d0f6ed269874a63) - **tsql**: Retain limit clause in subquery expression. *(PR [#5227](https://github.com/tobymao/sqlglot/pull/5227) by [@MarcusRisanger](https://github.com/MarcusRisanger))*
- [`4f42d95`](https://github.com/tobymao/sqlglot/commit/4f42d951363f8c43a4c414dc21d0505d9c8e48bf) - **duckdb**: Normalize date parts in `exp.Extract` generation *(PR [#5229](https://github.com/tobymao/sqlglot/pull/5229) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5228](https://github.com/tobymao/sqlglot/issues/5228) opened by [@greybeam-bot](https://github.com/greybeam-bot)*
- [`1b4c083`](https://github.com/tobymao/sqlglot/commit/1b4c083fff8d7c44bf1dbba28c1225fa1e28c4d2) - **athena**: include Hive string escapes in the tokenizer *(PR [#5233](https://github.com/tobymao/sqlglot/pull/5233) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5232](https://github.com/tobymao/sqlglot/issues/5232) opened by [@ligfx](https://github.com/ligfx)*
- [`e7e38fe`](https://github.com/tobymao/sqlglot/commit/e7e38fe0e09f9affbff4ffa7023d0161e3a1ee49) - **optimizer**: resolve table "columns" in bigquery that produce structs *(PR [#5230](https://github.com/tobymao/sqlglot/pull/5230) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5207](https://github.com/tobymao/sqlglot/issues/5207) opened by [@Bladieblah](https://github.com/Bladieblah)*
- [`781539d`](https://github.com/tobymao/sqlglot/commit/781539d5cbe58142ed6688f1522fc4ed31da0a56) - **duckdb**: Generate correct DETACH syntax if IF EXISTS is set *(PR [#5235](https://github.com/tobymao/sqlglot/pull/5235) by [@erindru](https://github.com/erindru))*
- [`d3dc761`](https://github.com/tobymao/sqlglot/commit/d3dc761393146357a5d20c4d7992fd2a1ae5e6e2) - change comma to cross join when precedence is the same for all join types *(PR [#5240](https://github.com/tobymao/sqlglot/pull/5240) by [@georgesittas](https://github.com/georgesittas))*
- [`31814cd`](https://github.com/tobymao/sqlglot/commit/31814cddb0cf65caf29fbc45a31a9c865b7991c7) - **presto**: cast constructed timestamp literal to zone-aware type if needed *(PR [#5253](https://github.com/tobymao/sqlglot/pull/5253) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5252](https://github.com/tobymao/sqlglot/issues/5252) opened by [@agni-sairent](https://github.com/agni-sairent)*
- [`847248d`](https://github.com/tobymao/sqlglot/commit/847248dd1b66e3a8f60c23a4488be85dfdef4113) - format ADD CONSTRAINT clause properly fixes [#5260](https://github.com/tobymao/sqlglot/pull/5260) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`db4e0ec`](https://github.com/tobymao/sqlglot/commit/db4e0ece950a6a1f543d8ecad48a7d4b1d6872be) - **tsql**: convert information schema keywords to uppercase for consistency *(PR [#5263](https://github.com/tobymao/sqlglot/pull/5263) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`8de87e3`](https://github.com/tobymao/sqlglot/commit/8de87e3f755a40b600aa94ee2c30cf697ef7c43c) - **redshift**: handle scale parameter in to_timestamp *(PR [#5266](https://github.com/tobymao/sqlglot/pull/5266) by [@MatiasCasaliSplit](https://github.com/MatiasCasaliSplit))*
- [`e32f709`](https://github.com/tobymao/sqlglot/commit/e32f70992b5058efb93f5d2b6106fb00b810f576) - **hive**: Fix exp.PosExplode alias order *(PR [#5279](https://github.com/tobymao/sqlglot/pull/5279) by [@VaggelisD](https://github.com/VaggelisD))*
- [`3dd9f8e`](https://github.com/tobymao/sqlglot/commit/3dd9f8e78ecbfde0dd7fc6fefcc09c8cb99bcd7b) - **fabric**: Type mismatches and precision error *(PR [#5280](https://github.com/tobymao/sqlglot/pull/5280) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`9a95af1`](https://github.com/tobymao/sqlglot/commit/9a95af1c725cd70ffa8206f1d88452a7faab93b2) - **snowflake**: only cast strings to timestamp for TO_CHAR (TimeToStr) *(PR [#5283](https://github.com/tobymao/sqlglot/pull/5283) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5282](https://github.com/tobymao/sqlglot/issues/5282) opened by [@wedotech-ashley](https://github.com/wedotech-ashley)*
- [`8af4790`](https://github.com/tobymao/sqlglot/commit/8af479017ccde16049c897ae5d322d4a69843b65) - **tsql**: Fix parsing of ADD CONSTRAINT *(PR [#5288](https://github.com/tobymao/sqlglot/pull/5288) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4813](https://github.com/TobikoData/sqlmesh/issues/4813) opened by [@bnstewrt](https://github.com/bnstewrt)*
- [`18aea08`](https://github.com/tobymao/sqlglot/commit/18aea08f7dcaa887bcf29886cd3b3bc2850a3679) - **scope**: include bigquery unnest aliases in selected sources *(PR [#5285](https://github.com/tobymao/sqlglot/pull/5285) by [@georgesittas](https://github.com/georgesittas))*
- [`ba4a234`](https://github.com/tobymao/sqlglot/commit/ba4a234bfabdd8161b96a29436a50e0eb04c2dc2) - **fabric**: ignore Date cap *(PR [#5290](https://github.com/tobymao/sqlglot/pull/5290) by [@fresioAS](https://github.com/fresioAS))*
- [`dc03649`](https://github.com/tobymao/sqlglot/commit/dc03649bca0b7a090254976182a03c21dd2269ba) - **bigquery**: only coerce time var -like units into strings for DATE_TRUNC *(PR [#5291](https://github.com/tobymao/sqlglot/pull/5291) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`86c6b90`](https://github.com/tobymao/sqlglot/commit/86c6b90d21b204b4376639affa142e8cee509065) - **tsql**: XML_OPTIONS *(commit by [@geooo109](https://github.com/geooo109))*
- [`aac70aa`](https://github.com/tobymao/sqlglot/commit/aac70aaaa8d840c267129e2307ccb65058cef0c9) - **parser**: simpler _parse_pipe_syntax_select *(commit by [@geooo109](https://github.com/geooo109))*

### :wrench: Chores
- [`6910744`](https://github.com/tobymao/sqlglot/commit/6910744e6260793b3f9190782cf60fbbd9adcd38) - update py03 version *(PR [#5136](https://github.com/tobymao/sqlglot/pull/5136) by [@benfdking](https://github.com/benfdking))*
  - :arrow_lower_right: *addresses issue [#5134](https://github.com/tobymao/sqlglot/issues/5134) opened by [@mgorny](https://github.com/mgorny)*
- [`a56deab`](https://github.com/tobymao/sqlglot/commit/a56deabc2b9543209fb5e41f19c3bef89177a577) - bump sqlglotrs to 0.5.0 *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`5752a87`](https://github.com/tobymao/sqlglot/commit/5752a87406b736317e4dc5cce9ae05cbc5c19547) - udpate benchmarking framework *(PR [#5146](https://github.com/tobymao/sqlglot/pull/5146) by [@benfdking](https://github.com/benfdking))*
- [`0ae297a`](https://github.com/tobymao/sqlglot/commit/0ae297a01262cf323e225fe578bdeab2230c6fd5) - compare performance on main vs pr branch *(PR [#5149](https://github.com/tobymao/sqlglot/pull/5149) by [@georgesittas](https://github.com/georgesittas))*
- [`180963b`](https://github.com/tobymao/sqlglot/commit/180963b8cf25d9ff83d2347859b7f46398af5000) - handle pipe syntax unsupported operators more gracefully *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6c8d61a`](https://github.com/tobymao/sqlglot/commit/6c8d61ae1ef5b645835ccd683063845dd801e8d2) - include optimization benchmarks *(PR [#5152](https://github.com/tobymao/sqlglot/pull/5152) by [@georgesittas](https://github.com/georgesittas))*
- [`bc5c66c`](https://github.com/tobymao/sqlglot/commit/bc5c66c9210a472147d98a94c34b4bb582ade8b1) - Run benchmark job if /benchmark comment *(PR [#5164](https://github.com/tobymao/sqlglot/pull/5164) by [@VaggelisD](https://github.com/VaggelisD))*
- [`742b2b7`](https://github.com/tobymao/sqlglot/commit/742b2b770b88a2e901d2f84af00db821da441e4c) - Fix benchmark CI to include issue number *(PR [#5166](https://github.com/tobymao/sqlglot/pull/5166) by [@VaggelisD](https://github.com/VaggelisD))*
- [`64c37f1`](https://github.com/tobymao/sqlglot/commit/64c37f147366fe87ae187996ecb3c9a5afa7c264) - bump sqlglotrs to 0.6.0 *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`440590b`](https://github.com/tobymao/sqlglot/commit/440590bf92ab1281f50b96a1400cbca695d40f0c) - bump sqlglotrs to 0.6.1 *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`83de4e1`](https://github.com/tobymao/sqlglot/commit/83de4e11bc1547aa22b275b20c0326dfbe43b2b8) - improve benchmark result displaying *(PR [#5176](https://github.com/tobymao/sqlglot/pull/5176) by [@georgesittas](https://github.com/georgesittas))*
- [`5d5dc2f`](https://github.com/tobymao/sqlglot/commit/5d5dc2fa471bd53730e03ac8039804221949f843) - Clean up exp.ArrayIntersect PR *(PR [#5193](https://github.com/tobymao/sqlglot/pull/5193) by [@VaggelisD](https://github.com/VaggelisD))*
- [`ad8a4e7`](https://github.com/tobymao/sqlglot/commit/ad8a4e73e1a9e4234f0b711163fb49630acf736c) - refactor join mark elimination to use is_correlated_subquery *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`7dfb578`](https://github.com/tobymao/sqlglot/commit/7dfb5780fb242c82744dc1538077776ac624081e) - Refactor DETACH generation *(PR [#5237](https://github.com/tobymao/sqlglot/pull/5237) by [@VaggelisD](https://github.com/VaggelisD))*


## [v26.30.0] - 2025-06-21
### :boom: BREAKING CHANGES
- due to [`d3dc761`](https://github.com/tobymao/sqlglot/commit/d3dc761393146357a5d20c4d7992fd2a1ae5e6e2) - change comma to cross join when precedence is the same for all join types *(PR [#5240](https://github.com/tobymao/sqlglot/pull/5240) by [@georgesittas](https://github.com/georgesittas))*:

  change comma to cross join when precedence is the same for all join types (#5240)

- due to [`e7c217e`](https://github.com/tobymao/sqlglot/commit/e7c217ef08e5811e7dad2b3d26dbaa9f02114e38) - transpile from/to dbms_random.value *(PR [#5242](https://github.com/tobymao/sqlglot/pull/5242) by [@georgesittas](https://github.com/georgesittas))*:

  transpile from/to dbms_random.value (#5242)

- due to [`31814cd`](https://github.com/tobymao/sqlglot/commit/31814cddb0cf65caf29fbc45a31a9c865b7991c7) - cast constructed timestamp literal to zone-aware type if needed *(PR [#5253](https://github.com/tobymao/sqlglot/pull/5253) by [@georgesittas](https://github.com/georgesittas))*:

  cast constructed timestamp literal to zone-aware type if needed (#5253)


### :sparkles: New Features
- [`e7c217e`](https://github.com/tobymao/sqlglot/commit/e7c217ef08e5811e7dad2b3d26dbaa9f02114e38) - **oracle**: transpile from/to dbms_random.value *(PR [#5242](https://github.com/tobymao/sqlglot/pull/5242) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5241](https://github.com/tobymao/sqlglot/issues/5241) opened by [@Akshat-2512](https://github.com/Akshat-2512)*
- [`0d19544`](https://github.com/tobymao/sqlglot/commit/0d19544317c1056b17fb089d4be9b5bddfe6feb3) - add Microsoft Fabric dialect, a case sensitive version of TSQL *(PR [#5247](https://github.com/tobymao/sqlglot/pull/5247) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`249dbc9`](https://github.com/tobymao/sqlglot/commit/249dbc906adc6b20932dc8efe83f6f4d23ef8c1e) - **parser**: start with SELECT and nested pipe syntax *(PR [#5248](https://github.com/tobymao/sqlglot/pull/5248) by [@geooo109](https://github.com/geooo109))*
- [`f5b5b93`](https://github.com/tobymao/sqlglot/commit/f5b5b9338eb92b7aa2c9b4c92c6138c2c05e1c40) - **fabric**: implement type mappings for unsupported Fabric types *(PR [#5249](https://github.com/tobymao/sqlglot/pull/5249) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`78fcea1`](https://github.com/tobymao/sqlglot/commit/78fcea13b5eb1734a15a254875bc80ad8063b0b0) - **spark, databricks**: parse brackets as placeholder *(PR [#5256](https://github.com/tobymao/sqlglot/pull/5256) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5251](https://github.com/tobymao/sqlglot/issues/5251) opened by [@aersam](https://github.com/aersam)*
- [`7d71387`](https://github.com/tobymao/sqlglot/commit/7d7138780db82e7a75949d29282b944e739ad99d) - **fabric**: Add precision cap to temporal data types *(PR [#5250](https://github.com/tobymao/sqlglot/pull/5250) by [@mattiasthalen](https://github.com/mattiasthalen))*
- [`e8cf793`](https://github.com/tobymao/sqlglot/commit/e8cf79305d398f25640ef3c07dd8b32997cb0167) - **duckdb**: Transpile Snowflake's TO_CHAR if format is in Snowflake.TIME_MAPPING *(PR [#5257](https://github.com/tobymao/sqlglot/pull/5257) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#5255](https://github.com/tobymao/sqlglot/issues/5255) opened by [@kyle-cheung](https://github.com/kyle-cheung)*

### :bug: Bug Fixes
- [`d3dc761`](https://github.com/tobymao/sqlglot/commit/d3dc761393146357a5d20c4d7992fd2a1ae5e6e2) - change comma to cross join when precedence is the same for all join types *(PR [#5240](https://github.com/tobymao/sqlglot/pull/5240) by [@georgesittas](https://github.com/georgesittas))*
- [`31814cd`](https://github.com/tobymao/sqlglot/commit/31814cddb0cf65caf29fbc45a31a9c865b7991c7) - **presto**: cast constructed timestamp literal to zone-aware type if needed *(PR [#5253](https://github.com/tobymao/sqlglot/pull/5253) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5252](https://github.com/tobymao/sqlglot/issues/5252) opened by [@agni-sairent](https://github.com/agni-sairent)*


## [v26.29.0] - 2025-06-17
### :boom: BREAKING CHANGES
- due to [`4f42d95`](https://github.com/tobymao/sqlglot/commit/4f42d951363f8c43a4c414dc21d0505d9c8e48bf) - Normalize date parts in `exp.Extract` generation *(PR [#5229](https://github.com/tobymao/sqlglot/pull/5229) by [@VaggelisD](https://github.com/VaggelisD))*:

  Normalize date parts in `exp.Extract` generation (#5229)

- due to [`e7e38fe`](https://github.com/tobymao/sqlglot/commit/e7e38fe0e09f9affbff4ffa7023d0161e3a1ee49) - resolve table "columns" in bigquery that produce structs *(PR [#5230](https://github.com/tobymao/sqlglot/pull/5230) by [@georgesittas](https://github.com/georgesittas))*:

  resolve table "columns" in bigquery that produce structs (#5230)


### :sparkles: New Features
- [`97f5822`](https://github.com/tobymao/sqlglot/commit/97f58226fc8815b23787b7b8699ea71f58268560) - **parser**: AS pipe syntax *(PR [#5224](https://github.com/tobymao/sqlglot/pull/5224) by [@geooo109](https://github.com/geooo109))*
- [`a7e7fee`](https://github.com/tobymao/sqlglot/commit/a7e7feef02a77fe8606f3f482bad91230fa637f4) - **parser**: EXTEND pipe syntax *(PR [#5225](https://github.com/tobymao/sqlglot/pull/5225) by [@geooo109](https://github.com/geooo109))*
- [`c1cb9f8`](https://github.com/tobymao/sqlglot/commit/c1cb9f8f682080f7a06c387219d79c6d068b6dbe) - **snowflake**: add autoincrement order clause support *(PR [#5223](https://github.com/tobymao/sqlglot/pull/5223) by [@dmaresma](https://github.com/dmaresma))*
- [`91afe4c`](https://github.com/tobymao/sqlglot/commit/91afe4cfd7b3f427e4c0b298075e867b8a1bbe55) - **parser**: TABLESAMPLE pipe syntax *(PR [#5231](https://github.com/tobymao/sqlglot/pull/5231) by [@geooo109](https://github.com/geooo109))*
- [`62da84a`](https://github.com/tobymao/sqlglot/commit/62da84acce7f44802dca26a9357a16115e21fabf) - **snowflake**: improve transpilation of unnested object lookup *(PR [#5234](https://github.com/tobymao/sqlglot/pull/5234) by [@georgesittas](https://github.com/georgesittas))*
- [`2c60453`](https://github.com/tobymao/sqlglot/commit/2c604537ba83dee74e9ced7e216673ecc70fe487) - **parser**: DROP pipe syntax *(PR [#5226](https://github.com/tobymao/sqlglot/pull/5226) by [@geooo109](https://github.com/geooo109))*
- [`9885729`](https://github.com/tobymao/sqlglot/commit/988572954135c68dc021b992c815024ce3debaff) - **parser**: SET pipe syntax *(PR [#5236](https://github.com/tobymao/sqlglot/pull/5236) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`df73a79`](https://github.com/tobymao/sqlglot/commit/df73a79a2ca3ba859b8aba5e3d0f6ed269874a63) - **tsql**: Retain limit clause in subquery expression. *(PR [#5227](https://github.com/tobymao/sqlglot/pull/5227) by [@MarcusRisanger](https://github.com/MarcusRisanger))*
- [`4f42d95`](https://github.com/tobymao/sqlglot/commit/4f42d951363f8c43a4c414dc21d0505d9c8e48bf) - **duckdb**: Normalize date parts in `exp.Extract` generation *(PR [#5229](https://github.com/tobymao/sqlglot/pull/5229) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5228](https://github.com/tobymao/sqlglot/issues/5228) opened by [@greybeam-bot](https://github.com/greybeam-bot)*
- [`1b4c083`](https://github.com/tobymao/sqlglot/commit/1b4c083fff8d7c44bf1dbba28c1225fa1e28c4d2) - **athena**: include Hive string escapes in the tokenizer *(PR [#5233](https://github.com/tobymao/sqlglot/pull/5233) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5232](https://github.com/tobymao/sqlglot/issues/5232) opened by [@ligfx](https://github.com/ligfx)*
- [`e7e38fe`](https://github.com/tobymao/sqlglot/commit/e7e38fe0e09f9affbff4ffa7023d0161e3a1ee49) - **optimizer**: resolve table "columns" in bigquery that produce structs *(PR [#5230](https://github.com/tobymao/sqlglot/pull/5230) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5207](https://github.com/tobymao/sqlglot/issues/5207) opened by [@Bladieblah](https://github.com/Bladieblah)*
- [`781539d`](https://github.com/tobymao/sqlglot/commit/781539d5cbe58142ed6688f1522fc4ed31da0a56) - **duckdb**: Generate correct DETACH syntax if IF EXISTS is set *(PR [#5235](https://github.com/tobymao/sqlglot/pull/5235) by [@erindru](https://github.com/erindru))*

### :wrench: Chores
- [`7dfb578`](https://github.com/tobymao/sqlglot/commit/7dfb5780fb242c82744dc1538077776ac624081e) - Refactor DETACH generation *(PR [#5237](https://github.com/tobymao/sqlglot/pull/5237) by [@VaggelisD](https://github.com/VaggelisD))*


## [v26.28.1] - 2025-06-13
### :boom: BREAKING CHANGES
- due to [`44297f1`](https://github.com/tobymao/sqlglot/commit/44297f1c5c8c2cb16fe77c318312f417b4281708) - JOIN pipe syntax, Set Operators as CTEs *(PR [#5215](https://github.com/tobymao/sqlglot/pull/5215) by [@geooo109](https://github.com/geooo109))*:

  JOIN pipe syntax, Set Operators as CTEs (#5215)


### :sparkles: New Features
- [`44297f1`](https://github.com/tobymao/sqlglot/commit/44297f1c5c8c2cb16fe77c318312f417b4281708) - **parser**: JOIN pipe syntax, Set Operators as CTEs *(PR [#5215](https://github.com/tobymao/sqlglot/pull/5215) by [@geooo109](https://github.com/geooo109))*
- [`21cd3eb`](https://github.com/tobymao/sqlglot/commit/21cd3ebf5d0b57f5b102c5aadc3b24a598ebe918) - **parser**: PIVOT/UNPIVOT pipe syntax *(PR [#5222](https://github.com/tobymao/sqlglot/pull/5222) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`28fed58`](https://github.com/tobymao/sqlglot/commit/28fed586a39df83aade4792217743a1a859fd039) - **optimizer**: UnboundLocalError in scope module *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`809e05a`](https://github.com/tobymao/sqlglot/commit/809e05a743d5a2904a1d6f6813f24ca7549ac7ef) - **snowflake**: preserve STRTOK_TO_ARRAY roundtrip *(commit by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`aac70aa`](https://github.com/tobymao/sqlglot/commit/aac70aaaa8d840c267129e2307ccb65058cef0c9) - **parser**: simpler _parse_pipe_syntax_select *(commit by [@geooo109](https://github.com/geooo109))*


## [v26.27.0] - 2025-06-12
### :boom: BREAKING CHANGES
- due to [`ac6555b`](https://github.com/tobymao/sqlglot/commit/ac6555b4d6c162ef7b14b63307d01fd560138ea0) - preserve DIV binary operator, fixes [#5198](https://github.com/tobymao/sqlglot/pull/5198) *(PR [#5199](https://github.com/tobymao/sqlglot/pull/5199) by [@georgesittas](https://github.com/georgesittas))*:

  preserve DIV binary operator, fixes #5198 (#5199)

- due to [`dfdd84b`](https://github.com/tobymao/sqlglot/commit/dfdd84bbc50da70f40a17b39935f8171d961f7d2) - CTEs instead of subqueries for pipe syntax *(PR [#5205](https://github.com/tobymao/sqlglot/pull/5205) by [@geooo109](https://github.com/geooo109))*:

  CTEs instead of subqueries for pipe syntax (#5205)

- due to [`5f95299`](https://github.com/tobymao/sqlglot/commit/5f9529940d83e89704f7d25eda63cd73fdb503ae) - support multi-part (>3) dotted functions *(PR [#5211](https://github.com/tobymao/sqlglot/pull/5211) by [@georgesittas](https://github.com/georgesittas))*:

  support multi-part (>3) dotted functions (#5211)

- due to [`02afa2a`](https://github.com/tobymao/sqlglot/commit/02afa2a1941fc67086d50dffac2857262f1c3c4f) - Preserve quoting for UDT *(PR [#5216](https://github.com/tobymao/sqlglot/pull/5216) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve quoting for UDT (#5216)


### :sparkles: New Features
- [`c20f85e`](https://github.com/tobymao/sqlglot/commit/c20f85e3e171e502fc51f74894d3313f0ad61535) - **spark**: support ALTER ADD PARTITION *(PR [#5208](https://github.com/tobymao/sqlglot/pull/5208) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5204](https://github.com/tobymao/sqlglot/issues/5204) opened by [@cosinequanon](https://github.com/cosinequanon)*

### :bug: Bug Fixes
- [`99bbae3`](https://github.com/tobymao/sqlglot/commit/99bbae370329c5f5cd132b711c714359cf96ba58) - **sqlite**: allow ALTER RENAME without COLUMN keyword fixes [#5195](https://github.com/tobymao/sqlglot/pull/5195) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`ac6555b`](https://github.com/tobymao/sqlglot/commit/ac6555b4d6c162ef7b14b63307d01fd560138ea0) - **hive**: preserve DIV binary operator, fixes [#5198](https://github.com/tobymao/sqlglot/pull/5198) *(PR [#5199](https://github.com/tobymao/sqlglot/pull/5199) by [@georgesittas](https://github.com/georgesittas))*
- [`d0eeb26`](https://github.com/tobymao/sqlglot/commit/d0eeb2639e771e8f8b6feabd41c65f16ed5a9829) - eliminate_join_marks has multiple issues fixes [#5188](https://github.com/tobymao/sqlglot/pull/5188) *(PR [#5189](https://github.com/tobymao/sqlglot/pull/5189) by [@snovik75](https://github.com/snovik75))*
  - :arrow_lower_right: *fixes issue [#5188](https://github.com/tobymao/sqlglot/issues/5188) opened by [@snovik75](https://github.com/snovik75)*
- [`dfdd84b`](https://github.com/tobymao/sqlglot/commit/dfdd84bbc50da70f40a17b39935f8171d961f7d2) - **parser**: CTEs instead of subqueries for pipe syntax *(PR [#5205](https://github.com/tobymao/sqlglot/pull/5205) by [@geooo109](https://github.com/geooo109))*
- [`77e9d9a`](https://github.com/tobymao/sqlglot/commit/77e9d9a0269e2013379967cf2f46fbd79c036277) - **mysql**: properly parse STORED/VIRTUAL computed columns *(PR [#5210](https://github.com/tobymao/sqlglot/pull/5210) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5203](https://github.com/tobymao/sqlglot/issues/5203) opened by [@mdebski](https://github.com/mdebski)*
- [`5f95299`](https://github.com/tobymao/sqlglot/commit/5f9529940d83e89704f7d25eda63cd73fdb503ae) - **parser**: support multi-part (>3) dotted functions *(PR [#5211](https://github.com/tobymao/sqlglot/pull/5211) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5200](https://github.com/tobymao/sqlglot/issues/5200) opened by [@mateuszpoleski](https://github.com/mateuszpoleski)*
- [`02afa2a`](https://github.com/tobymao/sqlglot/commit/02afa2a1941fc67086d50dffac2857262f1c3c4f) - **postgres**: Preserve quoting for UDT *(PR [#5216](https://github.com/tobymao/sqlglot/pull/5216) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5212](https://github.com/tobymao/sqlglot/issues/5212) opened by [@NickCrews](https://github.com/NickCrews)*
- [`f37c0b1`](https://github.com/tobymao/sqlglot/commit/f37c0b1197321dd610648ce652a171ab063deeeb) - **snowflake**: ensure a standalone GET() expression can be parsed *(PR [#5219](https://github.com/tobymao/sqlglot/pull/5219) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`ad8a4e7`](https://github.com/tobymao/sqlglot/commit/ad8a4e73e1a9e4234f0b711163fb49630acf736c) - refactor join mark elimination to use is_correlated_subquery *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.26.0] - 2025-06-09
### :boom: BREAKING CHANGES
- due to [`434c45b`](https://github.com/tobymao/sqlglot/commit/434c45b547c3a5ea155dc8d7da2baab326eb6d4f) - improve support for ENDSWITH closes [#5170](https://github.com/tobymao/sqlglot/pull/5170) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  improve support for ENDSWITH closes #5170

- due to [`bc001ce`](https://github.com/tobymao/sqlglot/commit/bc001cef4c907d8fa421d3190b4fa91865d9ff6c) - Add support for ANY_VALUE for versions 16+ *(PR [#5179](https://github.com/tobymao/sqlglot/pull/5179) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for ANY_VALUE for versions 16+ (#5179)

- due to [`6a2cb39`](https://github.com/tobymao/sqlglot/commit/6a2cb39d0ceec091dc4fc228f26d4f457729a3cf) - virtual column with AS(expr) as ComputedColumnConstraint *(PR [#5180](https://github.com/tobymao/sqlglot/pull/5180) by [@geooo109](https://github.com/geooo109))*:

  virtual column with AS(expr) as ComputedColumnConstraint (#5180)

- due to [`29e2f1d`](https://github.com/tobymao/sqlglot/commit/29e2f1d89c095c9fab0944a6962c99bd745c2c91) - Array_intersection transpilation support *(PR [#5186](https://github.com/tobymao/sqlglot/pull/5186) by [@HarishRavi96](https://github.com/HarishRavi96))*:

  Array_intersection transpilation support (#5186)


### :sparkles: New Features
- [`434c45b`](https://github.com/tobymao/sqlglot/commit/434c45b547c3a5ea155dc8d7da2baab326eb6d4f) - improve support for ENDSWITH closes [#5170](https://github.com/tobymao/sqlglot/pull/5170) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`63f9cb4`](https://github.com/tobymao/sqlglot/commit/63f9cb4b158b88574136b32241ee60254352c9e6) - **sqlglotrs**: match the Python implementation of __repr__ for tokens *(PR [#5172](https://github.com/tobymao/sqlglot/pull/5172) by [@georgesittas](https://github.com/georgesittas))*
- [`c007afa`](https://github.com/tobymao/sqlglot/commit/c007afa23831e9bd86f401d85260e15edf00328f) - support Star instance as first arg of exp.column helper *(PR [#5177](https://github.com/tobymao/sqlglot/pull/5177) by [@georgesittas](https://github.com/georgesittas))*
- [`bc001ce`](https://github.com/tobymao/sqlglot/commit/bc001cef4c907d8fa421d3190b4fa91865d9ff6c) - **postgres**: Add support for ANY_VALUE for versions 16+ *(PR [#5179](https://github.com/tobymao/sqlglot/pull/5179) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4674](https://github.com/TobikoData/sqlmesh/issues/4674) opened by [@petrikoro](https://github.com/petrikoro)*
- [`ba05ff6`](https://github.com/tobymao/sqlglot/commit/ba05ff67127e056d567fc2c1d3bcc8e3dcce7b7e) - **parser**: AGGREGATE with GROUP AND ORDER BY pipe syntax *(PR [#5171](https://github.com/tobymao/sqlglot/pull/5171) by [@geooo109](https://github.com/geooo109))*
- [`26077a4`](https://github.com/tobymao/sqlglot/commit/26077a47d9db750f44ab1baf9a434596b5bb613b) - make to_table more lenient *(PR [#5183](https://github.com/tobymao/sqlglot/pull/5183) by [@georgesittas](https://github.com/georgesittas))*
- [`29e2f1d`](https://github.com/tobymao/sqlglot/commit/29e2f1d89c095c9fab0944a6962c99bd745c2c91) - Array_intersection transpilation support *(PR [#5186](https://github.com/tobymao/sqlglot/pull/5186) by [@HarishRavi96](https://github.com/HarishRavi96))*
- [`d86a114`](https://github.com/tobymao/sqlglot/commit/d86a1147aeb866ed0ab2c342914ecf8cbfadac8a) - **sqlite**: implement RESPECT/IGNORE NULLS in first_value() *(PR [#5185](https://github.com/tobymao/sqlglot/pull/5185) by [@NickCrews](https://github.com/NickCrews))*
- [`1d50fca`](https://github.com/tobymao/sqlglot/commit/1d50fca8ffc34e4acbc1b791c4cdf5f184a748db) - improve transpilation of st_point and st_distance *(PR [#5194](https://github.com/tobymao/sqlglot/pull/5194) by [@georgesittas](https://github.com/georgesittas))*
- [`756ec3b`](https://github.com/tobymao/sqlglot/commit/756ec3b65db1eb2572d017a3ac12ece6bb44c726) - **parser**: SET OPERATORS with pipe syntax *(PR [#5184](https://github.com/tobymao/sqlglot/pull/5184) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`6a2cb39`](https://github.com/tobymao/sqlglot/commit/6a2cb39d0ceec091dc4fc228f26d4f457729a3cf) - **parser**: virtual column with AS(expr) as ComputedColumnConstraint *(PR [#5180](https://github.com/tobymao/sqlglot/pull/5180) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5173](https://github.com/tobymao/sqlglot/issues/5173) opened by [@suyah](https://github.com/suyah)*
- [`c87ae02`](https://github.com/tobymao/sqlglot/commit/c87ae02aa263be8463ca7283ebd090385a4bfd59) - **sqlite**: Add REPLACE to command tokens *(PR [#5192](https://github.com/tobymao/sqlglot/pull/5192) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5187](https://github.com/tobymao/sqlglot/issues/5187) opened by [@stefanmalanik](https://github.com/stefanmalanik)*
- [`4b89afd`](https://github.com/tobymao/sqlglot/commit/4b89afdcc0063e70cbc64165c7f1f5102afaa87c) - **starrocks**: array_agg_transpilation_fix *(PR [#5190](https://github.com/tobymao/sqlglot/pull/5190) by [@Swathiraj23](https://github.com/Swathiraj23))*
- [`461b054`](https://github.com/tobymao/sqlglot/commit/461b0548832ab8d916c3a6638f27a49f681109fe) - **postgres**: support use_spheroid argument in ST_DISTANCE *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`83de4e1`](https://github.com/tobymao/sqlglot/commit/83de4e11bc1547aa22b275b20c0326dfbe43b2b8) - improve benchmark result displaying *(PR [#5176](https://github.com/tobymao/sqlglot/pull/5176) by [@georgesittas](https://github.com/georgesittas))*
- [`5d5dc2f`](https://github.com/tobymao/sqlglot/commit/5d5dc2fa471bd53730e03ac8039804221949f843) - Clean up exp.ArrayIntersect PR *(PR [#5193](https://github.com/tobymao/sqlglot/pull/5193) by [@VaggelisD](https://github.com/VaggelisD))*


## [v26.25.3] - 2025-06-04
### :sparkles: New Features
- [`964b4a1`](https://github.com/tobymao/sqlglot/commit/964b4a1e367e00e243b80edf677cd48d453ed31e) - add line/col position for Star *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.25.2] - 2025-06-04
### :sparkles: New Features
- [`8b5129f`](https://github.com/tobymao/sqlglot/commit/8b5129f288880032f0bf9d649984d82314039af1) - **postgres**: improve pretty-formatting of ARRAY[...] *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.25.1] - 2025-06-04
### :wrench: Chores
- [`440590b`](https://github.com/tobymao/sqlglot/commit/440590bf92ab1281f50b96a1400cbca695d40f0c) - bump sqlglotrs to 0.6.1 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.25.0] - 2025-06-03
### :boom: BREAKING CHANGES
- due to [`72ce404`](https://github.com/tobymao/sqlglot/commit/72ce40405625239a0d6763d502e5af8b12abfe9b) - Refactor ALTER TABLE ADD parsing *(PR [#5144](https://github.com/tobymao/sqlglot/pull/5144) by [@VaggelisD](https://github.com/VaggelisD))*:

  Refactor ALTER TABLE ADD parsing (#5144)

- due to [`e73ddb7`](https://github.com/tobymao/sqlglot/commit/e73ddb733b7f120ae74054e6d4dc7d458f59ac50) - preserve TIMESTAMP on roundtrip *(PR [#5145](https://github.com/tobymao/sqlglot/pull/5145) by [@georgesittas](https://github.com/georgesittas))*:

  preserve TIMESTAMP on roundtrip (#5145)

- due to [`f6124c6`](https://github.com/tobymao/sqlglot/commit/f6124c6343f67563fc19f617891ecfc145a642db) - return token vector in `tokenize` even on failure *(PR [#5155](https://github.com/tobymao/sqlglot/pull/5155) by [@georgesittas](https://github.com/georgesittas))*:

  return token vector in `tokenize` even on failure (#5155)

- due to [`64c37f1`](https://github.com/tobymao/sqlglot/commit/64c37f147366fe87ae187996ecb3c9a5afa7c264) - bump sqlglotrs to 0.6.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.6.0


### :sparkles: New Features
- [`93b402a`](https://github.com/tobymao/sqlglot/commit/93b402abc74e642ed312db585b33315674a450cd) - **parser**: support SELECT, FROM, WHERE with pipe syntax *(PR [#5128](https://github.com/tobymao/sqlglot/pull/5128) by [@geooo109](https://github.com/geooo109))*
- [`1a8e78b`](https://github.com/tobymao/sqlglot/commit/1a8e78bd84e006023d5d3ea561504587dfbb55a9) - **parser**: ORDER BY with pipe syntax *(PR [#5153](https://github.com/tobymao/sqlglot/pull/5153) by [@geooo109](https://github.com/geooo109))*
- [`966ad95`](https://github.com/tobymao/sqlglot/commit/966ad95432d5f8e29ade36d8271a5c489c207324) - **tsql**: add convert style 126 *(PR [#5157](https://github.com/tobymao/sqlglot/pull/5157) by [@pa1ch](https://github.com/pa1ch))*
- [`b7ac6ff`](https://github.com/tobymao/sqlglot/commit/b7ac6ff4680ff619be4b0ddb01f61f916ed09d58) - **parser**: LIMIT/OFFSET pipe syntax *(PR [#5159](https://github.com/tobymao/sqlglot/pull/5159) by [@geooo109](https://github.com/geooo109))*
- [`cfc158d`](https://github.com/tobymao/sqlglot/commit/cfc158d753d4f43d12c3b502633d29e43dcc5569) - **snowflake**: transpile STRTOK_TO_ARRAY to duckdb *(PR [#5165](https://github.com/tobymao/sqlglot/pull/5165) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5160](https://github.com/tobymao/sqlglot/issues/5160) opened by [@kyle-cheung](https://github.com/kyle-cheung)*
- [`ff0f30b`](https://github.com/tobymao/sqlglot/commit/ff0f30bcf7d0d74b26a703eaa632e1be15b3c001) - support ARRAY_REMOVE *(PR [#5163](https://github.com/tobymao/sqlglot/pull/5163) by [@geooo109](https://github.com/geooo109))*
- [`9cac01f`](https://github.com/tobymao/sqlglot/commit/9cac01f6b4a5c93b55f5b68f21cb104932880a0e) - **tsql**: support FOR XML syntax *(PR [#5167](https://github.com/tobymao/sqlglot/pull/5167) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5161](https://github.com/tobymao/sqlglot/issues/5161) opened by [@codykonior](https://github.com/codykonior)*

### :bug: Bug Fixes
- [`f3aeb37`](https://github.com/tobymao/sqlglot/commit/f3aeb374351a0b1b3c75945718d8ea42f8926b62) - **tsql**: properly parse and generate ALTER SET *(PR [#5143](https://github.com/tobymao/sqlglot/pull/5143) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5135](https://github.com/tobymao/sqlglot/issues/5135) opened by [@codykonior](https://github.com/codykonior)*
- [`72ce404`](https://github.com/tobymao/sqlglot/commit/72ce40405625239a0d6763d502e5af8b12abfe9b) - Refactor ALTER TABLE ADD parsing *(PR [#5144](https://github.com/tobymao/sqlglot/pull/5144) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5129](https://github.com/tobymao/sqlglot/issues/5129) opened by [@Mevrael](https://github.com/Mevrael)*
- [`e73ddb7`](https://github.com/tobymao/sqlglot/commit/e73ddb733b7f120ae74054e6d4dc7d458f59ac50) - **mysql**: preserve TIMESTAMP on roundtrip *(PR [#5145](https://github.com/tobymao/sqlglot/pull/5145) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5127](https://github.com/tobymao/sqlglot/issues/5127) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`4f8c73d`](https://github.com/tobymao/sqlglot/commit/4f8c73d60eecebc601c60ee8c7819458435e34b8) - **hive**: STRUCT column names and data type should be separated by ':' in hive *(PR [#5147](https://github.com/tobymao/sqlglot/pull/5147) by [@tsamaras](https://github.com/tsamaras))*
- [`e2a488f`](https://github.com/tobymao/sqlglot/commit/e2a488f48f3e036566462463bbc58cc6a1c7492e) - Error on columns mismatch in pushdown_projections ignores dialect *(PR [#5151](https://github.com/tobymao/sqlglot/pull/5151) by [@snovik75](https://github.com/snovik75))*
- [`1a35365`](https://github.com/tobymao/sqlglot/commit/1a35365a3bb1ef56e8da0023271cbe3108e0ccb1) - avoid generating nested comments when not supported *(PR [#5158](https://github.com/tobymao/sqlglot/pull/5158) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5132](https://github.com/tobymao/sqlglot/issues/5132) opened by [@patricksurry](https://github.com/patricksurry)*
- [`f6124c6`](https://github.com/tobymao/sqlglot/commit/f6124c6343f67563fc19f617891ecfc145a642db) - **rust-tokenizer**: return token vector in `tokenize` even on failure *(PR [#5155](https://github.com/tobymao/sqlglot/pull/5155) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5148](https://github.com/tobymao/sqlglot/issues/5148) opened by [@kamoser](https://github.com/kamoser)*
- [`760a606`](https://github.com/tobymao/sqlglot/commit/760a6062d5f259488e471af9c1d33e200066e9dc) - **postgres**: support decimal values in INTERVAL expressions fixes [#5168](https://github.com/tobymao/sqlglot/pull/5168) *(commit by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`86c6b90`](https://github.com/tobymao/sqlglot/commit/86c6b90d21b204b4376639affa142e8cee509065) - **tsql**: XML_OPTIONS *(commit by [@geooo109](https://github.com/geooo109))*

### :wrench: Chores
- [`5752a87`](https://github.com/tobymao/sqlglot/commit/5752a87406b736317e4dc5cce9ae05cbc5c19547) - udpate benchmarking framework *(PR [#5146](https://github.com/tobymao/sqlglot/pull/5146) by [@benfdking](https://github.com/benfdking))*
- [`0ae297a`](https://github.com/tobymao/sqlglot/commit/0ae297a01262cf323e225fe578bdeab2230c6fd5) - compare performance on main vs pr branch *(PR [#5149](https://github.com/tobymao/sqlglot/pull/5149) by [@georgesittas](https://github.com/georgesittas))*
- [`180963b`](https://github.com/tobymao/sqlglot/commit/180963b8cf25d9ff83d2347859b7f46398af5000) - handle pipe syntax unsupported operators more gracefully *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6c8d61a`](https://github.com/tobymao/sqlglot/commit/6c8d61ae1ef5b645835ccd683063845dd801e8d2) - include optimization benchmarks *(PR [#5152](https://github.com/tobymao/sqlglot/pull/5152) by [@georgesittas](https://github.com/georgesittas))*
- [`bc5c66c`](https://github.com/tobymao/sqlglot/commit/bc5c66c9210a472147d98a94c34b4bb582ade8b1) - Run benchmark job if /benchmark comment *(PR [#5164](https://github.com/tobymao/sqlglot/pull/5164) by [@VaggelisD](https://github.com/VaggelisD))*
- [`742b2b7`](https://github.com/tobymao/sqlglot/commit/742b2b770b88a2e901d2f84af00db821da441e4c) - Fix benchmark CI to include issue number *(PR [#5166](https://github.com/tobymao/sqlglot/pull/5166) by [@VaggelisD](https://github.com/VaggelisD))*
- [`64c37f1`](https://github.com/tobymao/sqlglot/commit/64c37f147366fe87ae187996ecb3c9a5afa7c264) - bump sqlglotrs to 0.6.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.24.0] - 2025-05-30
### :boom: BREAKING CHANGES
- due to [`c484ca3`](https://github.com/tobymao/sqlglot/commit/c484ca39bad750a96b62e2edae85612cac66ba30) - recognize ARRAY_CONCAT_AGG as an aggregate function *(PR [#5141](https://github.com/tobymao/sqlglot/pull/5141) by [@georgesittas](https://github.com/georgesittas))*:

  recognize ARRAY_CONCAT_AGG as an aggregate function (#5141)


### :sparkles: New Features
- [`bb4f428`](https://github.com/tobymao/sqlglot/commit/bb4f4283b53bc060a8c7e0f12c1e7ef5b521c4e6) - bubble up comments nested under a Bracket, fixes [#5131](https://github.com/tobymao/sqlglot/pull/5131) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`9f318eb`](https://github.com/tobymao/sqlglot/commit/9f318ebe4502bb484a34873252cf4a40c7e440e4) - **snowflake**: Transpile BQ's `ARRAY(SELECT AS STRUCT ...)` *(PR [#5140](https://github.com/tobymao/sqlglot/pull/5140) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`a3fccd9`](https://github.com/tobymao/sqlglot/commit/a3fccd9be294499b53477da931f8b097cdbe09fc) - **snowflake**: generate SELECT for UNNEST without JOIN or FROM *(PR [#5138](https://github.com/tobymao/sqlglot/pull/5138) by [@geooo109](https://github.com/geooo109))*
- [`993919d`](https://github.com/tobymao/sqlglot/commit/993919d05d5d3c814471607b56831bb65d349eb4) - **snowflake**: Properly transpile ARRAY_AGG, IGNORE/RESPECT NULLS *(PR [#5137](https://github.com/tobymao/sqlglot/pull/5137) by [@VaggelisD](https://github.com/VaggelisD))*
- [`6e57619`](https://github.com/tobymao/sqlglot/commit/6e57619f85375e789bb39a6478aa01cd7c7758f0) - **snowflake**: Transpile ISOWEEK to WEEKISO *(PR [#5139](https://github.com/tobymao/sqlglot/pull/5139) by [@VaggelisD](https://github.com/VaggelisD))*
- [`c484ca3`](https://github.com/tobymao/sqlglot/commit/c484ca39bad750a96b62e2edae85612cac66ba30) - **bigquery**: recognize ARRAY_CONCAT_AGG as an aggregate function *(PR [#5141](https://github.com/tobymao/sqlglot/pull/5141) by [@georgesittas](https://github.com/georgesittas))*


## [v26.23.0] - 2025-05-29
### :boom: BREAKING CHANGES
- due to [`6910744`](https://github.com/tobymao/sqlglot/commit/6910744e6260793b3f9190782cf60fbbd9adcd38) - update py03 version *(PR [#5136](https://github.com/tobymao/sqlglot/pull/5136) by [@benfdking](https://github.com/benfdking))*:

  update py03 version (#5136)

- due to [`a56deab`](https://github.com/tobymao/sqlglot/commit/a56deabc2b9543209fb5e41f19c3bef89177a577) - bump sqlglotrs to 0.5.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.5.0


### :bug: Bug Fixes
- [`e9b3156`](https://github.com/tobymao/sqlglot/commit/e9b3156aa1ed95fdee4c6b419134d8ca746964b6) - **athena**: Handle transpilation of FileFormatProperty from dialects that treat it as a variable and not a string literal *(PR [#5133](https://github.com/tobymao/sqlglot/pull/5133) by [@erindru](https://github.com/erindru))*

### :wrench: Chores
- [`6910744`](https://github.com/tobymao/sqlglot/commit/6910744e6260793b3f9190782cf60fbbd9adcd38) - update py03 version *(PR [#5136](https://github.com/tobymao/sqlglot/pull/5136) by [@benfdking](https://github.com/benfdking))*
  - :arrow_lower_right: *addresses issue [#5134](https://github.com/tobymao/sqlglot/issues/5134) opened by [@mgorny](https://github.com/mgorny)*
- [`a56deab`](https://github.com/tobymao/sqlglot/commit/a56deabc2b9543209fb5e41f19c3bef89177a577) - bump sqlglotrs to 0.5.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.22.1] - 2025-05-28
### :bug: Bug Fixes
- [`f7401fd`](https://github.com/tobymao/sqlglot/commit/f7401fdc29a35738eb23f424ceba03463a4d8af9) - **bigquery**: avoid getting stuck in infinite loop when parsing tables *(PR [#5130](https://github.com/tobymao/sqlglot/pull/5130) by [@georgesittas](https://github.com/georgesittas))*


## [v26.22.0] - 2025-05-27
### :boom: BREAKING CHANGES
- due to [`f2bf000`](https://github.com/tobymao/sqlglot/commit/f2bf000a410fb18531bb90ef1d767baf0e8bce7a) - avoid creating new alias for qualifying unpivot *(PR [#5121](https://github.com/tobymao/sqlglot/pull/5121) by [@geooo109](https://github.com/geooo109))*:

  avoid creating new alias for qualifying unpivot (#5121)

- due to [`a126ce8`](https://github.com/tobymao/sqlglot/commit/a126ce8a25287cf3531d815035fa3d567dc772fb) - make coalesce simplification optional, skip by default *(PR [#5123](https://github.com/tobymao/sqlglot/pull/5123) by [@barakalon](https://github.com/barakalon))*:

  make coalesce simplification optional, skip by default (#5123)


### :sparkles: New Features
- [`82c50ce`](https://github.com/tobymao/sqlglot/commit/82c50ce68d9a1ad25095086ae3645f5c4996c18b) - **duckdb**: extend time travel parsing to take VERSION into account *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`f2bf000`](https://github.com/tobymao/sqlglot/commit/f2bf000a410fb18531bb90ef1d767baf0e8bce7a) - **optimizer**: avoid creating new alias for qualifying unpivot *(PR [#5121](https://github.com/tobymao/sqlglot/pull/5121) by [@geooo109](https://github.com/geooo109))*
- [`a126ce8`](https://github.com/tobymao/sqlglot/commit/a126ce8a25287cf3531d815035fa3d567dc772fb) - **optimizer**: make coalesce simplification optional, skip by default *(PR [#5123](https://github.com/tobymao/sqlglot/pull/5123) by [@barakalon](https://github.com/barakalon))*


## [v26.21.0] - 2025-05-26
### :boom: BREAKING CHANGES
- due to [`de67d3c`](https://github.com/tobymao/sqlglot/commit/de67d3c953191d77ecf8cf57e375e7d203cd8857) - error on unsupported dialect settings *(PR [#5119](https://github.com/tobymao/sqlglot/pull/5119) by [@georgesittas](https://github.com/georgesittas))*:

  error on unsupported dialect settings (#5119)


### :sparkles: New Features
- [`344f2f1`](https://github.com/tobymao/sqlglot/commit/344f2f12b6ed02d3cfd265c33fe4428741bcf6d6) - store line/col position for Anonymous functions *(PR [#5120](https://github.com/tobymao/sqlglot/pull/5120) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`de67d3c`](https://github.com/tobymao/sqlglot/commit/de67d3c953191d77ecf8cf57e375e7d203cd8857) - error on unsupported dialect settings *(PR [#5119](https://github.com/tobymao/sqlglot/pull/5119) by [@georgesittas](https://github.com/georgesittas))*


## [v26.20.0] - 2025-05-25
### :sparkles: New Features
- [`a51744f`](https://github.com/tobymao/sqlglot/commit/a51744f84945dbb99a2ab3b576eccf1543e21e17) - **optimizer**: annotate SORT_ARRAY *(PR [#5110](https://github.com/tobymao/sqlglot/pull/5110) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5107](https://github.com/tobymao/sqlglot/issues/5107) opened by [@fernandomorato](https://github.com/fernandomorato)*

### :bug: Bug Fixes
- [`ef93832`](https://github.com/tobymao/sqlglot/commit/ef938328ea18dede07ea4be7425a203770b4ca7d) - **tsql**: separate ISNULL from COALESCE *(PR [#5105](https://github.com/tobymao/sqlglot/pull/5105) by [@geooo109](https://github.com/geooo109))*
- [`10fe4c0`](https://github.com/tobymao/sqlglot/commit/10fe4c039a15f12c97bdf74e2e4cf547691f8546) - Return parameterized `type2` in `_maybe_coerce` *(PR [#5106](https://github.com/tobymao/sqlglot/pull/5106) by [@aninhalbuquerque](https://github.com/aninhalbuquerque))*
- [`2a95777`](https://github.com/tobymao/sqlglot/commit/2a957772fb2d95442604cf19451bf8cb58be0aeb) - **snowflake**: Put COPY GRANTS in the right place for materialized views *(PR [#5109](https://github.com/tobymao/sqlglot/pull/5109) by [@erindru](https://github.com/erindru))*
- [`8ba0eca`](https://github.com/tobymao/sqlglot/commit/8ba0ecaa4c3b81594a0bc0a6a88f205dc64fb9aa) - **optimizer**: avoid creating extra ARRAY for annotate SORT_ARRAY *(commit by [@geooo109](https://github.com/geooo109))*
- [`a2ba1aa`](https://github.com/tobymao/sqlglot/commit/a2ba1aa14891db9edb853296501fac6995f8d802) - **optimizer**: annotate DPipe with VARCHAR *(PR [#5111](https://github.com/tobymao/sqlglot/pull/5111) by [@geooo109](https://github.com/geooo109))*
- [`57db62a`](https://github.com/tobymao/sqlglot/commit/57db62ac9cf115b699076af2fb951188b54639be) - ignore/respect nulls generation edge case *(PR [#5117](https://github.com/tobymao/sqlglot/pull/5117) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`a330093`](https://github.com/tobymao/sqlglot/commit/a33009349b49f244f857976dad72bee3943b80c0) - **executor**: add type hints in table module *(PR [#5113](https://github.com/tobymao/sqlglot/pull/5113) by [@esadek](https://github.com/esadek))*


## [v26.19.0] - 2025-05-22
### :boom: BREAKING CHANGES
- due to [`886f85b`](https://github.com/tobymao/sqlglot/commit/886f85bf61d23ef968b9bcfd98ab606c8a590526) - pass dialect to ensure_schema *(PR [#5100](https://github.com/tobymao/sqlglot/pull/5100) by [@georgesittas](https://github.com/georgesittas))*:

  pass dialect to ensure_schema (#5100)

- due to [`7570f8a`](https://github.com/tobymao/sqlglot/commit/7570f8a8e77b045b5fd97dde8b4112b901df7e15) - hive, spark2, spark, databricks type coercion for IF and COALESCE functions *(PR [#5096](https://github.com/tobymao/sqlglot/pull/5096) by [@geooo109](https://github.com/geooo109))*:

  hive, spark2, spark, databricks type coercion for IF and COALESCE functions (#5096)


### :sparkles: New Features
- [`f5f4ca1`](https://github.com/tobymao/sqlglot/commit/f5f4ca195b57007afa80fd3d9ef69953e36536ea) - **starrocks**: Support parsing "NONE" as security option *(PR [#5099](https://github.com/tobymao/sqlglot/pull/5099) by [@alpolishchuk](https://github.com/alpolishchuk))*
- [`2b928e2`](https://github.com/tobymao/sqlglot/commit/2b928e238cba63e5e043207dae1bfe2f140a1c2b) - improve pretty-printing of MERGE statement *(PR [#5102](https://github.com/tobymao/sqlglot/pull/5102) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5101](https://github.com/tobymao/sqlglot/issues/5101) opened by [@maoxingda](https://github.com/maoxingda)*
- [`69ce6b4`](https://github.com/tobymao/sqlglot/commit/69ce6b4e5d597288e4001f9696713aee083617be) - **duckdb**: add support for TRY *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`886f85b`](https://github.com/tobymao/sqlglot/commit/886f85bf61d23ef968b9bcfd98ab606c8a590526) - **optimizer**: pass dialect to ensure_schema *(PR [#5100](https://github.com/tobymao/sqlglot/pull/5100) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5098](https://github.com/tobymao/sqlglot/issues/5098) opened by [@sh-rp](https://github.com/sh-rp)*
- [`7570f8a`](https://github.com/tobymao/sqlglot/commit/7570f8a8e77b045b5fd97dde8b4112b901df7e15) - **optimizer**: hive, spark2, spark, databricks type coercion for IF and COALESCE functions *(PR [#5096](https://github.com/tobymao/sqlglot/pull/5096) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5067](https://github.com/tobymao/sqlglot/issues/5067) opened by [@fernandomorato](https://github.com/fernandomorato)*

### :wrench: Chores
- [`cb96a0c`](https://github.com/tobymao/sqlglot/commit/cb96a0c57d94b172e6a46f8498d726cec65cfb3f) - **duckdb**: add test for UUIDV7 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.18.1] - 2025-05-20
### :wrench: Chores
- [`db2af6f`](https://github.com/tobymao/sqlglot/commit/db2af6fa1e2c2bf0f4cebb272287d0b2e8e69f76) - bump sqlglotrs to 0.4.2 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.18.0] - 2025-05-20
### :boom: BREAKING CHANGES
- due to [`1df7f61`](https://github.com/tobymao/sqlglot/commit/1df7f611bc96616cb07950a80f6669d0bc331b0e) - refactor length_sql so it handles any type, not just varchar/blob *(PR [#4935](https://github.com/tobymao/sqlglot/pull/4935) by [@tekumara](https://github.com/tekumara))*:

  refactor length_sql so it handles any type, not just varchar/blob (#4935)

- due to [`52719f3`](https://github.com/tobymao/sqlglot/commit/52719f37f6541e8ec9f66642ac23ed9015048092) - parse CREATE STAGE *(PR [#4947](https://github.com/tobymao/sqlglot/pull/4947) by [@tekumara](https://github.com/tekumara))*:

  parse CREATE STAGE (#4947)

- due to [`fd39b30`](https://github.com/tobymao/sqlglot/commit/fd39b30209d068b787619b8137a105aca9c3e607) - parse CREATE FILE FORMAT *(PR [#4948](https://github.com/tobymao/sqlglot/pull/4948) by [@tekumara](https://github.com/tekumara))*:

  parse CREATE FILE FORMAT (#4948)

- due to [`f835756`](https://github.com/tobymao/sqlglot/commit/f835756257f735643584b89e93693e8577744731) - Fix CREATE EXTERNAL TABLE properties *(PR [#4951](https://github.com/tobymao/sqlglot/pull/4951) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix CREATE EXTERNAL TABLE properties (#4951)

- due to [`44b955b`](https://github.com/tobymao/sqlglot/commit/44b955bd537bfb8f5b6e84ecbcd5f6e3da852260) - Fix generation of exp.Values *(PR [#4930](https://github.com/tobymao/sqlglot/pull/4930) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix generation of exp.Values (#4930)

- due to [`1f506b1`](https://github.com/tobymao/sqlglot/commit/1f506b186f1b954829195eefda318e231d474208) - support SHOW (ALL) TABLES *(PR [#4961](https://github.com/tobymao/sqlglot/pull/4961) by [@mscolnick](https://github.com/mscolnick))*:

  support SHOW (ALL) TABLES (#4961)

- due to [`72cf4a4`](https://github.com/tobymao/sqlglot/commit/72cf4a4501a8d122041a28b71be5a41ffb53602a) - Add support for PIVOT multiple IN clauses *(PR [#4964](https://github.com/tobymao/sqlglot/pull/4964) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for PIVOT multiple IN clauses (#4964)

- due to [`400ea54`](https://github.com/tobymao/sqlglot/commit/400ea54d3a9cab256bfa5e496439bb9be6072d0b) - ensure JSON_FORMAT type is JSON when targeting Presto *(PR [#4968](https://github.com/tobymao/sqlglot/pull/4968) by [@georgesittas](https://github.com/georgesittas))*:

  ensure JSON_FORMAT type is JSON when targeting Presto (#4968)

- due to [`cb20038`](https://github.com/tobymao/sqlglot/commit/cb2003875fc6e149bd4a631e99c312a04435a46b) - treat GO as command *(PR [#4978](https://github.com/tobymao/sqlglot/pull/4978) by [@georgesittas](https://github.com/georgesittas))*:

  treat GO as command (#4978)

- due to [`60e26b8`](https://github.com/tobymao/sqlglot/commit/60e26b868242a05a7fdc2725bd21a127910a6fb7) - improve transpilability of GET_JSON_OBJECT by parsing json path *(PR [#4980](https://github.com/tobymao/sqlglot/pull/4980) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilability of GET_JSON_OBJECT by parsing json path (#4980)

- due to [`2b7845a`](https://github.com/tobymao/sqlglot/commit/2b7845a3a821d366ae90ba9ef5e7d61194a34874) - Add support for Athena's Iceberg partitioning transforms *(PR [#4976](https://github.com/tobymao/sqlglot/pull/4976) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for Athena's Iceberg partitioning transforms (#4976)

- due to [`ee794e9`](https://github.com/tobymao/sqlglot/commit/ee794e9c6a3b2fdb142114327d904b6c94a16cd0) - use the standard POWER function instead of ^ fixes [#4982](https://github.com/tobymao/sqlglot/pull/4982) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  use the standard POWER function instead of ^ fixes #4982

- due to [`2369195`](https://github.com/tobymao/sqlglot/commit/2369195635e25dabd5ce26c13e402076508bba04) - consistently parse INTERVAL value as a string *(PR [#4986](https://github.com/tobymao/sqlglot/pull/4986) by [@georgesittas](https://github.com/georgesittas))*:

  consistently parse INTERVAL value as a string (#4986)

- due to [`e866cff`](https://github.com/tobymao/sqlglot/commit/e866cffbaac3b62255d0d5c8be043ab2394af619) - support RELY option for PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints *(PR [#4987](https://github.com/tobymao/sqlglot/pull/4987) by [@geooo109](https://github.com/geooo109))*:

  support RELY option for PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints (#4987)

- due to [`510984f`](https://github.com/tobymao/sqlglot/commit/510984f2ddc6ff13b8a8030f698aed9ad0e6f46b) - stop generating redundant TO_DATE calls *(PR [#4990](https://github.com/tobymao/sqlglot/pull/4990) by [@georgesittas](https://github.com/georgesittas))*:

  stop generating redundant TO_DATE calls (#4990)

- due to [`da9ec61`](https://github.com/tobymao/sqlglot/commit/da9ec61e8edd5049e246390e1b638cf14d50fa2d) - Fix pretty generation of exp.Window *(PR [#4994](https://github.com/tobymao/sqlglot/pull/4994) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix pretty generation of exp.Window (#4994)

- due to [`fb83fac`](https://github.com/tobymao/sqlglot/commit/fb83fac2d097d8d3e8e2556c072792857609bd94) - remove recursion from `simplify` *(PR [#4988](https://github.com/tobymao/sqlglot/pull/4988) by [@georgesittas](https://github.com/georgesittas))*:

  remove recursion from `simplify` (#4988)

- due to [`890b24a`](https://github.com/tobymao/sqlglot/commit/890b24a5cec269f5595743d0a86024a23217a3f1) - remove `connector_depth` as it is now dead code *(commit by [@georgesittas](https://github.com/georgesittas))*:

  remove `connector_depth` as it is now dead code

- due to [`1dc501b`](https://github.com/tobymao/sqlglot/commit/1dc501b8ed68638375d869e11f3bf188948a4990) - remove `max_depth` argument in simplify as it is now dead code *(commit by [@georgesittas](https://github.com/georgesittas))*:

  remove `max_depth` argument in simplify as it is now dead code

- due to [`f5358d8`](https://github.com/tobymao/sqlglot/commit/f5358d8a3e2743b5ac0d540f10502d333ad4e082) - add support for GET statements *(PR [#5019](https://github.com/tobymao/sqlglot/pull/5019) by [@eruditmorina](https://github.com/eruditmorina))*:

  add support for GET statements (#5019)

- due to [`bafa7f3`](https://github.com/tobymao/sqlglot/commit/bafa7f3a03c57e573b793ed2c83c3a549dfb789c) - parse DOW and DOY *(PR [#5037](https://github.com/tobymao/sqlglot/pull/5037) by [@geooo109](https://github.com/geooo109))*:

  parse DOW and DOY (#5037)

- due to [`eb0a989`](https://github.com/tobymao/sqlglot/commit/eb0a989a7f3bbddb49c66ad5cd42043532568e25) - support udf environment property *(PR [#5045](https://github.com/tobymao/sqlglot/pull/5045) by [@geooo109](https://github.com/geooo109))*:

  support udf environment property (#5045)

- due to [`807fbbc`](https://github.com/tobymao/sqlglot/commit/807fbbc5a89925fd3c98e823003a9dc929fcaff6) - transpile timestamp without time zone *(PR [#5047](https://github.com/tobymao/sqlglot/pull/5047) by [@geooo109](https://github.com/geooo109))*:

  transpile timestamp without time zone (#5047)

- due to [`c48fc8f`](https://github.com/tobymao/sqlglot/commit/c48fc8fefc13becff92d0546cec1730f038af6b2) - support translate with error *(PR [#5052](https://github.com/tobymao/sqlglot/pull/5052) by [@geooo109](https://github.com/geooo109))*:

  support translate with error (#5052)

- due to [`2e9704e`](https://github.com/tobymao/sqlglot/commit/2e9704ede255ef17b412c6905aad69afd70ccbf3) - Change `COLLATE` expression to `Var` for `ALTER TABLE` *(PR [#5055](https://github.com/tobymao/sqlglot/pull/5055) by [@MarcusRisanger](https://github.com/MarcusRisanger))*:

  Change `COLLATE` expression to `Var` for `ALTER TABLE` (#5055)

- due to [`63f505e`](https://github.com/tobymao/sqlglot/commit/63f505e036928ed94df61a8b213bf84198e33d35) - unqualify UNNEST only the left most part of a column *(PR [#5069](https://github.com/tobymao/sqlglot/pull/5069) by [@geooo109](https://github.com/geooo109))*:

  unqualify UNNEST only the left most part of a column (#5069)

- due to [`56da962`](https://github.com/tobymao/sqlglot/commit/56da9629899e72ab1e15cfc45ede838c4c38c16e) - to_timestamp without format *(PR [#5070](https://github.com/tobymao/sqlglot/pull/5070) by [@geooo109](https://github.com/geooo109))*:

  to_timestamp without format (#5070)

- due to [`1ddfcbe`](https://github.com/tobymao/sqlglot/commit/1ddfcbe6c1d30d70533774da38d842bb3af6c205) - support CONVERT function *(PR [#5074](https://github.com/tobymao/sqlglot/pull/5074) by [@geooo109](https://github.com/geooo109))*:

  support CONVERT function (#5074)

- due to [`ba52f01`](https://github.com/tobymao/sqlglot/commit/ba52f014f0d53ce8a179f1b140876274a01b38ac) - respect normalization strategy overrides *(PR [#5080](https://github.com/tobymao/sqlglot/pull/5080) by [@georgesittas](https://github.com/georgesittas))*:

  respect normalization strategy overrides (#5080)


### :sparkles: New Features
- [`52719f3`](https://github.com/tobymao/sqlglot/commit/52719f37f6541e8ec9f66642ac23ed9015048092) - **snowflake**: parse CREATE STAGE *(PR [#4947](https://github.com/tobymao/sqlglot/pull/4947) by [@tekumara](https://github.com/tekumara))*
- [`fd39b30`](https://github.com/tobymao/sqlglot/commit/fd39b30209d068b787619b8137a105aca9c3e607) - **snowflake**: parse CREATE FILE FORMAT *(PR [#4948](https://github.com/tobymao/sqlglot/pull/4948) by [@tekumara](https://github.com/tekumara))*
- [`da9a6a1`](https://github.com/tobymao/sqlglot/commit/da9a6a1d56323319b87e9b193d12ad1c644b9239) - **snowflake**: parse SHOW STAGES *(PR [#4949](https://github.com/tobymao/sqlglot/pull/4949) by [@tekumara](https://github.com/tekumara))*
- [`bfdcdf0`](https://github.com/tobymao/sqlglot/commit/bfdcdf0afc0f4af3dacdfc3e8dca243793552b74) - **snowflake**: parse SHOW FILE FORMATS *(PR [#4950](https://github.com/tobymao/sqlglot/pull/4950) by [@tekumara](https://github.com/tekumara))*
- [`c591443`](https://github.com/tobymao/sqlglot/commit/c591443b6b2328780e08179144557e181db0cbb6) - **duckdb**: add support for GROUP clause in standard PIVOT syntax *(PR [#4953](https://github.com/tobymao/sqlglot/pull/4953) by [@georgesittas](https://github.com/georgesittas))*
- [`b011ee2`](https://github.com/tobymao/sqlglot/commit/b011ee2df0beaac75b982261a25d3e787dead54a) - **bigquery**: Add support for side & kind on set operators *(PR [#4959](https://github.com/tobymao/sqlglot/pull/4959) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4942](https://github.com/tobymao/sqlglot/issues/4942) opened by [@z3z1ma](https://github.com/z3z1ma)*
- [`1f506b1`](https://github.com/tobymao/sqlglot/commit/1f506b186f1b954829195eefda318e231d474208) - **duckdb**: support SHOW (ALL) TABLES *(PR [#4961](https://github.com/tobymao/sqlglot/pull/4961) by [@mscolnick](https://github.com/mscolnick))*
  - :arrow_lower_right: *addresses issue [#4956](https://github.com/tobymao/sqlglot/issues/4956) opened by [@mscolnick](https://github.com/mscolnick)*
- [`ad5b595`](https://github.com/tobymao/sqlglot/commit/ad5b595049a16a27a7f249afea43dbcfcf43b5f4) - allow explicit aliasing in if(...) expressions *(PR [#4963](https://github.com/tobymao/sqlglot/pull/4963) by [@georgesittas](https://github.com/georgesittas))*
- [`72cf4a4`](https://github.com/tobymao/sqlglot/commit/72cf4a4501a8d122041a28b71be5a41ffb53602a) - **duckdb**: Add support for PIVOT multiple IN clauses *(PR [#4964](https://github.com/tobymao/sqlglot/pull/4964) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4944](https://github.com/tobymao/sqlglot/issues/4944) opened by [@nph](https://github.com/nph)*
- [`7bc5a21`](https://github.com/tobymao/sqlglot/commit/7bc5a217c3cc68d0cb1eaedc0c18f5188de80bf1) - **postgres**: support laterals with ordinality fixes [#4965](https://github.com/tobymao/sqlglot/pull/4965) *(PR [#4966](https://github.com/tobymao/sqlglot/pull/4966) by [@georgesittas](https://github.com/georgesittas))*
- [`400ea54`](https://github.com/tobymao/sqlglot/commit/400ea54d3a9cab256bfa5e496439bb9be6072d0b) - ensure JSON_FORMAT type is JSON when targeting Presto *(PR [#4968](https://github.com/tobymao/sqlglot/pull/4968) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4967](https://github.com/tobymao/sqlglot/issues/4967) opened by [@jmsmdy](https://github.com/jmsmdy)*
- [`a762993`](https://github.com/tobymao/sqlglot/commit/a762993c53d7ae91a831a8be448010e17e60f497) - **generator**: unsupported warning for T-SQL query option *(PR [#4972](https://github.com/tobymao/sqlglot/pull/4972) by [@geooo109](https://github.com/geooo109))*
- [`e866cff`](https://github.com/tobymao/sqlglot/commit/e866cffbaac3b62255d0d5c8be043ab2394af619) - **parser**: support RELY option for PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints *(PR [#4987](https://github.com/tobymao/sqlglot/pull/4987) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#4983](https://github.com/tobymao/sqlglot/issues/4983) opened by [@ggadon](https://github.com/ggadon)*
- [`76535ce`](https://github.com/tobymao/sqlglot/commit/76535ce9487186d2eb7071fac2f224238de7a9ba) - **optimizer**: add support for Spark's TRANSFORM clause *(PR [#4993](https://github.com/tobymao/sqlglot/pull/4993) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4991](https://github.com/tobymao/sqlglot/issues/4991) opened by [@karta0807913](https://github.com/karta0807913)*
- [`27a9fb2`](https://github.com/tobymao/sqlglot/commit/27a9fb26a1936512a09b8b09ed2656e22918f2c6) - **clickhouse**: Support parsing CTAS with alias *(PR [#5003](https://github.com/tobymao/sqlglot/pull/5003) by [@dorranh](https://github.com/dorranh))*
- [`45cd165`](https://github.com/tobymao/sqlglot/commit/45cd165eaca96b33f1de753a147bdc352b9d56d0) - **clickhouse**: Support ClickHouse Nothing type *(PR [#5004](https://github.com/tobymao/sqlglot/pull/5004) by [@dorranh](https://github.com/dorranh))*
- [`ca61a61`](https://github.com/tobymao/sqlglot/commit/ca61a617fa67082bc0fc94853dee4d70b8ca5c59) - Support exp.PartitionByProperty for parse_into() *(PR [#5006](https://github.com/tobymao/sqlglot/pull/5006) by [@erindru](https://github.com/erindru))*
- [`a6d4c3c`](https://github.com/tobymao/sqlglot/commit/a6d4c3c901f828cdd96a16a0e55eac1b244f63be) - **snowflake**: Add numeric parameter support *(PR [#5008](https://github.com/tobymao/sqlglot/pull/5008) by [@hovaesco](https://github.com/hovaesco))*
- [`5feae00`](https://github.com/tobymao/sqlglot/commit/5feae00ec7a4826285e7fd0be85d377cc0de09b5) - **databricks**: add support for the VOID type *(PR [#5012](https://github.com/tobymao/sqlglot/pull/5012) by [@georgesittas](https://github.com/georgesittas))*
- [`6010302`](https://github.com/tobymao/sqlglot/commit/60103020879db5f23a6c4a1775848e31cce13415) - **postgres**: transpile QUARTER interval unit *(PR [#5015](https://github.com/tobymao/sqlglot/pull/5015) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5013](https://github.com/tobymao/sqlglot/issues/5013) opened by [@Wiill007](https://github.com/Wiill007)*
- [`f5358d8`](https://github.com/tobymao/sqlglot/commit/f5358d8a3e2743b5ac0d540f10502d333ad4e082) - **snowflake**: add support for GET statements *(PR [#5019](https://github.com/tobymao/sqlglot/pull/5019) by [@eruditmorina](https://github.com/eruditmorina))*
- [`df5ecdb`](https://github.com/tobymao/sqlglot/commit/df5ecdbebcdce491031538f6baa0f87ec7eefee8) - Include token refereces in the meta of identifier expressions *(PR [#5022](https://github.com/tobymao/sqlglot/pull/5022) by [@izeigerman](https://github.com/izeigerman))*
- [`bafa7f3`](https://github.com/tobymao/sqlglot/commit/bafa7f3a03c57e573b793ed2c83c3a549dfb789c) - **presto**: parse DOW and DOY *(PR [#5037](https://github.com/tobymao/sqlglot/pull/5037) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5036](https://github.com/tobymao/sqlglot/issues/5036) opened by [@baruchoxman](https://github.com/baruchoxman)*
- [`eb0a989`](https://github.com/tobymao/sqlglot/commit/eb0a989a7f3bbddb49c66ad5cd42043532568e25) - support udf environment property *(PR [#5045](https://github.com/tobymao/sqlglot/pull/5045) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5043](https://github.com/tobymao/sqlglot/issues/5043) opened by [@aersam](https://github.com/aersam)*
- [`c48fc8f`](https://github.com/tobymao/sqlglot/commit/c48fc8fefc13becff92d0546cec1730f038af6b2) - **teradata**: support translate with error *(PR [#5052](https://github.com/tobymao/sqlglot/pull/5052) by [@geooo109](https://github.com/geooo109))*
- [`6791849`](https://github.com/tobymao/sqlglot/commit/679184943f7ffa79a2a466546f9bdfccd69034a3) - **executor**: support conversion from table to pylist *(PR [#5053](https://github.com/tobymao/sqlglot/pull/5053) by [@esadek](https://github.com/esadek))*
- [`07bf71b`](https://github.com/tobymao/sqlglot/commit/07bf71bae5d2a5c381104a86bb52c06809c21174) - **parser**: FK REFERENCES without specifying column *(PR [#5064](https://github.com/tobymao/sqlglot/pull/5064) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5057](https://github.com/tobymao/sqlglot/issues/5057) opened by [@Steven-Wright](https://github.com/Steven-Wright)*
- [`1ddfcbe`](https://github.com/tobymao/sqlglot/commit/1ddfcbe6c1d30d70533774da38d842bb3af6c205) - **oracle**: support CONVERT function *(PR [#5074](https://github.com/tobymao/sqlglot/pull/5074) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5071](https://github.com/tobymao/sqlglot/issues/5071) opened by [@tchamwam](https://github.com/tchamwam)*
- [`2cca655`](https://github.com/tobymao/sqlglot/commit/2cca655430ccf4542dcb3fd0e95b776739ef91eb) - allow PIVOT to follow a JOIN *(PR [#5075](https://github.com/tobymao/sqlglot/pull/5075) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5073](https://github.com/tobymao/sqlglot/issues/5073) opened by [@tchamwam](https://github.com/tchamwam)*
- [`c7a56d7`](https://github.com/tobymao/sqlglot/commit/c7a56d7616cfb99de942d527e80ccec36cfc5cc3) - **oracle**: PRIOR in SELECT *(PR [#5077](https://github.com/tobymao/sqlglot/pull/5077) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#5072](https://github.com/tobymao/sqlglot/issues/5072) opened by [@tchamwam](https://github.com/tchamwam)*
- [`5c66679`](https://github.com/tobymao/sqlglot/commit/5c66679208b34b480b9a0a0c538a15ab98f872b6) - **clickhouse**: allow EXCHANGE to be parsed as Command *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`3bcf989`](https://github.com/tobymao/sqlglot/commit/3bcf9899bbdac54bf8923ab3aa13ec66c65f0c44) - **snowflake**: Transpile DataType.BIGDECIMAL to DOUBLE *(PR [#5092](https://github.com/tobymao/sqlglot/pull/5092) by [@VaggelisD](https://github.com/VaggelisD))*
- [`b63b60e`](https://github.com/tobymao/sqlglot/commit/b63b60ebd10ca51f05e3f54532767bd98ccc34e3) - treat `CHAR[ACTER] VARYING` as `VARCHAR` for all dialects *(PR [#5093](https://github.com/tobymao/sqlglot/pull/5093) by [@ewhitley](https://github.com/ewhitley))*
- [`aa26aad`](https://github.com/tobymao/sqlglot/commit/aa26aad2608cd55b8bbd1d9e268444307a7224dc) - transpile WINDOW clause *(PR [#5097](https://github.com/tobymao/sqlglot/pull/5097) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`7042603`](https://github.com/tobymao/sqlglot/commit/7042603ecb5693795b15219ec9cebf2f76032c03) - **optimizer**: Merge subqueries when inner query has name conflict with outer query *(PR [#4931](https://github.com/tobymao/sqlglot/pull/4931) by [@barakalon](https://github.com/barakalon))*
- [`1df7f61`](https://github.com/tobymao/sqlglot/commit/1df7f611bc96616cb07950a80f6669d0bc331b0e) - **duckdb**: refactor length_sql so it handles any type, not just varchar/blob *(PR [#4935](https://github.com/tobymao/sqlglot/pull/4935) by [@tekumara](https://github.com/tekumara))*
  - :arrow_lower_right: *fixes issue [#4934](https://github.com/tobymao/sqlglot/issues/4934) opened by [@tekumara](https://github.com/tekumara)*
- [`09882e3`](https://github.com/tobymao/sqlglot/commit/09882e32f057670a9cbd97c1e5cf1a00c774b5d2) - **tsql**: remove assert call from _build_formatted_time *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`bf39a95`](https://github.com/tobymao/sqlglot/commit/bf39a95426ed6637e424da1be070cc9a8affc358) - **sqlite**: transpile double quoted PRIMARY KEY *(PR [#4941](https://github.com/tobymao/sqlglot/pull/4941) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4938](https://github.com/tobymao/sqlglot/issues/4938) opened by [@rgeronimi](https://github.com/rgeronimi)*
- [`f835756`](https://github.com/tobymao/sqlglot/commit/f835756257f735643584b89e93693e8577744731) - **snowflake**: Fix CREATE EXTERNAL TABLE properties *(PR [#4951](https://github.com/tobymao/sqlglot/pull/4951) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4945](https://github.com/tobymao/sqlglot/issues/4945) opened by [@tekumara](https://github.com/tekumara)*
- [`61ed971`](https://github.com/tobymao/sqlglot/commit/61ed971213c979c3777e57853bd6989bc169adb1) - **athena**: Correctly handle CTAS queries that contain Union's *(PR [#4955](https://github.com/tobymao/sqlglot/pull/4955) by [@erindru](https://github.com/erindru))*
- [`44b955b`](https://github.com/tobymao/sqlglot/commit/44b955bd537bfb8f5b6e84ecbcd5f6e3da852260) - **clickhouse**: Fix generation of exp.Values *(PR [#4930](https://github.com/tobymao/sqlglot/pull/4930) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4056](https://github.com/TobikoData/sqlmesh/issues/4056) opened by [@dnbnero](https://github.com/dnbnero)*
- [`61bc01c`](https://github.com/tobymao/sqlglot/commit/61bc01ceec2f801490f3f1a571aee655c5109962) - **clickhouse**: allow string literal for clickhouse ON CLUSTER clause *(PR [#4971](https://github.com/tobymao/sqlglot/pull/4971) by [@lepfhty](https://github.com/lepfhty))*
- [`1353b79`](https://github.com/tobymao/sqlglot/commit/1353b79bd9810788a02163928b044fe038267078) - **Snowflake**: Enhance parity for FILE_FORMAT & CREDENTIALS in CREATE STAGE *(PR [#4969](https://github.com/tobymao/sqlglot/pull/4969) by [@whummer](https://github.com/whummer))*
- [`9693dbd`](https://github.com/tobymao/sqlglot/commit/9693dbd18b98b2699cade738a254f71f2ee8ce74) - **clickhouse**: avoid superfluous parentheses in DISTINCT ON (...) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`cb20038`](https://github.com/tobymao/sqlglot/commit/cb2003875fc6e149bd4a631e99c312a04435a46b) - **tsql**: treat GO as command *(PR [#4978](https://github.com/tobymao/sqlglot/pull/4978) by [@georgesittas](https://github.com/georgesittas))*
- [`60e26b8`](https://github.com/tobymao/sqlglot/commit/60e26b868242a05a7fdc2725bd21a127910a6fb7) - **hive**: improve transpilability of GET_JSON_OBJECT by parsing json path *(PR [#4980](https://github.com/tobymao/sqlglot/pull/4980) by [@georgesittas](https://github.com/georgesittas))*
- [`2b7845a`](https://github.com/tobymao/sqlglot/commit/2b7845a3a821d366ae90ba9ef5e7d61194a34874) - Add support for Athena's Iceberg partitioning transforms *(PR [#4976](https://github.com/tobymao/sqlglot/pull/4976) by [@VaggelisD](https://github.com/VaggelisD))*
- [`fa6af23`](https://github.com/tobymao/sqlglot/commit/fa6af2302f8482c5d89ead481afe4195aaa41a9c) - **optimizer**: compare the whole type to determine if a cast can be removed *(PR [#4981](https://github.com/tobymao/sqlglot/pull/4981) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4977](https://github.com/tobymao/sqlglot/issues/4977) opened by [@MeinAccount](https://github.com/MeinAccount)*
- [`830c9b8`](https://github.com/tobymao/sqlglot/commit/830c9b8bbf906cf5d4fa8028b67dadda73fc58a9) - **unnest_subqueries**: avoid adding GROUP BY on aggregate projections in lateral subqueries *(PR [#4970](https://github.com/tobymao/sqlglot/pull/4970) by [@skadel](https://github.com/skadel))*
- [`ee794e9`](https://github.com/tobymao/sqlglot/commit/ee794e9c6a3b2fdb142114327d904b6c94a16cd0) - **postgres**: use the standard POWER function instead of ^ fixes [#4982](https://github.com/tobymao/sqlglot/pull/4982) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`85e62b8`](https://github.com/tobymao/sqlglot/commit/85e62b88df2822797f527dce4eaa230c778cbe9e) - **bigquery**: Do not consume JOIN keywords after WITH OFFSET *(PR [#4984](https://github.com/tobymao/sqlglot/pull/4984) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2369195`](https://github.com/tobymao/sqlglot/commit/2369195635e25dabd5ce26c13e402076508bba04) - consistently parse INTERVAL value as a string *(PR [#4986](https://github.com/tobymao/sqlglot/pull/4986) by [@georgesittas](https://github.com/georgesittas))*
- [`510984f`](https://github.com/tobymao/sqlglot/commit/510984f2ddc6ff13b8a8030f698aed9ad0e6f46b) - **hive**: stop generating redundant TO_DATE calls *(PR [#4990](https://github.com/tobymao/sqlglot/pull/4990) by [@georgesittas](https://github.com/georgesittas))*
- [`da9ec61`](https://github.com/tobymao/sqlglot/commit/da9ec61e8edd5049e246390e1b638cf14d50fa2d) - **generator**: Fix pretty generation of exp.Window *(PR [#4994](https://github.com/tobymao/sqlglot/pull/4994) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4098](https://github.com/TobikoData/sqlmesh/issues/4098) opened by [@tanghyd](https://github.com/tanghyd)*
- [`aae9aa8`](https://github.com/tobymao/sqlglot/commit/aae9aa8f96ccaa7686cda3cdabec208ae4c3d60a) - **optimizer**: ensure there are no shared refs after qualify_tables *(PR [#4995](https://github.com/tobymao/sqlglot/pull/4995) by [@georgesittas](https://github.com/georgesittas))*
- [`adaef42`](https://github.com/tobymao/sqlglot/commit/adaef42234d8f1c9c331f53bee2c42686f29bdec) - **trino**: Dont quote identifiers in string literals for the partitioned_by property *(PR [#4998](https://github.com/tobymao/sqlglot/pull/4998) by [@erindru](https://github.com/erindru))*
- [`a547f8d`](https://github.com/tobymao/sqlglot/commit/a547f8d4292f3b3a4c85f9d6466ead2ad976dfd2) - **postgres**: Capture optional minus sign in interval regex *(PR [#5000](https://github.com/tobymao/sqlglot/pull/5000) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4999](https://github.com/tobymao/sqlglot/issues/4999) opened by [@cpimhoff](https://github.com/cpimhoff)*
- [`8e9dbd4`](https://github.com/tobymao/sqlglot/commit/8e9dbd491b9516c614554e05f05cc1cb976838e3) - **duckdb**: warn on unsupported IGNORE/RESPECT NULLS *(PR [#5002](https://github.com/tobymao/sqlglot/pull/5002) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5001](https://github.com/tobymao/sqlglot/issues/5001) opened by [@MarcoGorelli](https://github.com/MarcoGorelli)*
- [`10b02bc`](https://github.com/tobymao/sqlglot/commit/10b02bce304042fea09e9cb2369db3c873452245) - **clickhouse**: Support optional timezone argument in date_diff() *(PR [#5005](https://github.com/tobymao/sqlglot/pull/5005) by [@dorranh](https://github.com/dorranh))*
- [`c594b63`](https://github.com/tobymao/sqlglot/commit/c594b630c1c940e9a47abfce1633b435a2607f13) - Add MAX_BY & MIN_BY to FUNCTION_PARSER *(PR [#5021](https://github.com/tobymao/sqlglot/pull/5021) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5020](https://github.com/tobymao/sqlglot/issues/5020) opened by [@omerhadari](https://github.com/omerhadari)*
- [`c1c892c`](https://github.com/tobymao/sqlglot/commit/c1c892cebb89ddf29369ff3c7647f96d217acb71) - **parser**: parse column ops after no-paren type casting *(PR [#5025](https://github.com/tobymao/sqlglot/pull/5025) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5024](https://github.com/tobymao/sqlglot/issues/5024) opened by [@MagdaSousa](https://github.com/MagdaSousa)*
- [`52e068f`](https://github.com/tobymao/sqlglot/commit/52e068f74bd6844d0273ddcc7637d249e6ed51c1) - **databricks**: Preserve colon operators in TRY_CAST *(PR [#5028](https://github.com/tobymao/sqlglot/pull/5028) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5027](https://github.com/tobymao/sqlglot/issues/5027) opened by [@aersam](https://github.com/aersam)*
- [`91e5036`](https://github.com/tobymao/sqlglot/commit/91e5036831b87fd4670424e6a49e81efead432f2) - **parser**: Do not parse set ops if input expr is None *(PR [#5030](https://github.com/tobymao/sqlglot/pull/5030) by [@VaggelisD](https://github.com/VaggelisD))*
- [`8f77b30`](https://github.com/tobymao/sqlglot/commit/8f77b301a267eadb4c4792201e112159db554d1c) - **snowflake**: get function *(commit by [@tobymao](https://github.com/tobymao))*
- [`281ab21`](https://github.com/tobymao/sqlglot/commit/281ab21969d3937cef55adc3032f74b00173e948) - **snowflake**: generate expression DayOfWeekIso using DAYOFWEEKISO *(PR [#5034](https://github.com/tobymao/sqlglot/pull/5034) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5032](https://github.com/tobymao/sqlglot/issues/5032) opened by [@baruchoxman](https://github.com/baruchoxman)*
- [`2fa9684`](https://github.com/tobymao/sqlglot/commit/2fa96843a29323b97229842f7cf993b72bc86677) - preserve non-participating joins in eliminate_join_marks rule fixes [#5039](https://github.com/tobymao/sqlglot/pull/5039) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`d10fdf5`](https://github.com/tobymao/sqlglot/commit/d10fdf5f9388dc3848617cfbf4e6f7b1aa73be1a) - **optimizer**: prevent incorrect predicate pushdown into RHS of CROSS JOIN UNNEST *(PR [#5033](https://github.com/tobymao/sqlglot/pull/5033) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5023](https://github.com/tobymao/sqlglot/issues/5023) opened by [@schelip](https://github.com/schelip)*
- [`7c55c48`](https://github.com/tobymao/sqlglot/commit/7c55c48ec2088e776fd4ec5b6c0f4989450a39c6) - prevent redundant backslash escapes in rawstring generator *(PR [#5040](https://github.com/tobymao/sqlglot/pull/5040) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5038](https://github.com/tobymao/sqlglot/issues/5038) opened by [@ihvol-freenome](https://github.com/ihvol-freenome)*
- [`167c547`](https://github.com/tobymao/sqlglot/commit/167c547171fa3f2de1c2fdd64ca51bb9ccb3ee52) - **tsql**: ALTER COLUMN syntax *(PR [#5051](https://github.com/tobymao/sqlglot/pull/5051) by [@MarcusRisanger](https://github.com/MarcusRisanger))*
  - :arrow_lower_right: *fixes issue [#5050](https://github.com/tobymao/sqlglot/issues/5050) opened by [@MarcusRisanger](https://github.com/MarcusRisanger)*
- [`807fbbc`](https://github.com/tobymao/sqlglot/commit/807fbbc5a89925fd3c98e823003a9dc929fcaff6) - **duckdb**: transpile timestamp without time zone *(PR [#5047](https://github.com/tobymao/sqlglot/pull/5047) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4859](https://github.com/tobymao/sqlglot/issues/4859) opened by [@eakmanrq](https://github.com/eakmanrq)*
- [`2e9704e`](https://github.com/tobymao/sqlglot/commit/2e9704ede255ef17b412c6905aad69afd70ccbf3) - **tsql**: Change `COLLATE` expression to `Var` for `ALTER TABLE` *(PR [#5055](https://github.com/tobymao/sqlglot/pull/5055) by [@MarcusRisanger](https://github.com/MarcusRisanger))*
- [`60f9420`](https://github.com/tobymao/sqlglot/commit/60f9420660d8d48bd98560a9bf8aec1f497fdeff) - **druid**: preserve MOD function fixes [#5060](https://github.com/tobymao/sqlglot/pull/5060) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`7866b48`](https://github.com/tobymao/sqlglot/commit/7866b48275c830aeb51592e1888c751bcb58a361) - **druid**: support current_timestamp *(PR [#5061](https://github.com/tobymao/sqlglot/pull/5061) by [@ALongJohnson](https://github.com/ALongJohnson))*
  - :arrow_lower_right: *fixes issue [#5059](https://github.com/tobymao/sqlglot/issues/5059) opened by [@ALongJohnson](https://github.com/ALongJohnson)*
- [`626f3a3`](https://github.com/tobymao/sqlglot/commit/626f3a3987c2a96a8fd6e329d237c0c7bc8bf264) - Support EXCLUDE in window definition *(PR [#5058](https://github.com/tobymao/sqlglot/pull/5058) by [@rafasofizada](https://github.com/rafasofizada))*
- [`63f505e`](https://github.com/tobymao/sqlglot/commit/63f505e036928ed94df61a8b213bf84198e33d35) - unqualify UNNEST only the left most part of a column *(PR [#5069](https://github.com/tobymao/sqlglot/pull/5069) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5062](https://github.com/tobymao/sqlglot/issues/5062) opened by [@goldmedal](https://github.com/goldmedal)*
- [`56da962`](https://github.com/tobymao/sqlglot/commit/56da9629899e72ab1e15cfc45ede838c4c38c16e) - **oracle**: to_timestamp without format *(PR [#5070](https://github.com/tobymao/sqlglot/pull/5070) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5068](https://github.com/tobymao/sqlglot/issues/5068) opened by [@kosta-foundational](https://github.com/kosta-foundational)*
- [`ba52f01`](https://github.com/tobymao/sqlglot/commit/ba52f014f0d53ce8a179f1b140876274a01b38ac) - **bigquery**: respect normalization strategy overrides *(PR [#5080](https://github.com/tobymao/sqlglot/pull/5080) by [@georgesittas](https://github.com/georgesittas))*
- [`03ace87`](https://github.com/tobymao/sqlglot/commit/03ace877e3f9e5d56fcbcbe260849f5d1247e5d9) - **optimizer**: keep ORDER BY when merging subqueries *(PR [#5084](https://github.com/tobymao/sqlglot/pull/5084) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#5065](https://github.com/tobymao/sqlglot/issues/5065) opened by [@udaykrishna-eng](https://github.com/udaykrishna-eng)*
- [`ba7b5a8`](https://github.com/tobymao/sqlglot/commit/ba7b5a8566dc15f438dcd0c03397b2e93e9c75cb) - **bigquery**: respect normalization strategy patching *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`4558bb7`](https://github.com/tobymao/sqlglot/commit/4558bb7a3a00629194f969d05d4b151f9ccd6172) - **bigquery**: always infer concat type as either bytes or string *(PR [#5085](https://github.com/tobymao/sqlglot/pull/5085) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5083](https://github.com/tobymao/sqlglot/issues/5083) opened by [@tobymao](https://github.com/tobymao)*
- [`612a2da`](https://github.com/tobymao/sqlglot/commit/612a2daeb0e93c5cc77b3c78c0b53905f4bee19c) - **tokenizer**: fix token col attribute when there is leading whitespace after a newline *(PR [#5094](https://github.com/tobymao/sqlglot/pull/5094) by [@chgiff](https://github.com/chgiff))*
- [`9d3a929`](https://github.com/tobymao/sqlglot/commit/9d3a929ba9006ebac67ff315c55da74a724ec975) - preserve `ARRAY_JOIN` for StarRocks, Doris (fixes [#5095](https://github.com/tobymao/sqlglot/pull/5095)) *(commit by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`fb83fac`](https://github.com/tobymao/sqlglot/commit/fb83fac2d097d8d3e8e2556c072792857609bd94) - **optimizer**: remove recursion from `simplify` *(PR [#4988](https://github.com/tobymao/sqlglot/pull/4988) by [@georgesittas](https://github.com/georgesittas))*
- [`1b3ea34`](https://github.com/tobymao/sqlglot/commit/1b3ea344af1d71d3eee239a5c4996a0aecd091de) - **clickhouse**: override _parse_property_assignment to handle null engine *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`890b24a`](https://github.com/tobymao/sqlglot/commit/890b24a5cec269f5595743d0a86024a23217a3f1) - remove `connector_depth` as it is now dead code *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1dc501b`](https://github.com/tobymao/sqlglot/commit/1dc501b8ed68638375d869e11f3bf188948a4990) - remove `max_depth` argument in simplify as it is now dead code *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6572517`](https://github.com/tobymao/sqlglot/commit/6572517c1ec76f14cbd661aacc15c84bef065284) - improve tooling around benchmarks *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1d4d906`](https://github.com/tobymao/sqlglot/commit/1d4d906abc60d29b6606bc8eee50c92cef21d3fd) - use _try_parse for parsing ClickHouse's CREATE TABLE .. AS <table> *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fc58c27`](https://github.com/tobymao/sqlglot/commit/fc58c273690734263b971b138ec8f0186f524672) - Refactor placeholder parsing for TokenType.COLON *(PR [#5009](https://github.com/tobymao/sqlglot/pull/5009) by [@VaggelisD](https://github.com/VaggelisD))*
- [`da90228`](https://github.com/tobymao/sqlglot/commit/da90228f1550715646106dd6f9a170d0973f138f) - put a lock around the lazy dialect module loading call *(PR [#5011](https://github.com/tobymao/sqlglot/pull/5011) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5010](https://github.com/tobymao/sqlglot/issues/5010) opened by [@NickCrews](https://github.com/NickCrews)*
- [`abbcf26`](https://github.com/tobymao/sqlglot/commit/abbcf26b2101b2d806466353dcd29b79d1af5219) - bump sqlglotrs to 0.4.1 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.16.4] - 2025-05-02
### :bug: Bug Fixes
- [`52e068f`](https://github.com/tobymao/sqlglot/commit/52e068f74bd6844d0273ddcc7637d249e6ed51c1) - **databricks**: Preserve colon operators in TRY_CAST *(PR [#5028](https://github.com/tobymao/sqlglot/pull/5028) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5027](https://github.com/tobymao/sqlglot/issues/5027) opened by [@aersam](https://github.com/aersam)*
- [`91e5036`](https://github.com/tobymao/sqlglot/commit/91e5036831b87fd4670424e6a49e81efead432f2) - **parser**: Do not parse set ops if input expr is None *(PR [#5030](https://github.com/tobymao/sqlglot/pull/5030) by [@VaggelisD](https://github.com/VaggelisD))*
- [`8f77b30`](https://github.com/tobymao/sqlglot/commit/8f77b301a267eadb4c4792201e112159db554d1c) - **snowflake**: get function *(commit by [@tobymao](https://github.com/tobymao))*


## [v26.16.3] - 2025-05-01
### :boom: BREAKING CHANGES
- due to [`f5358d8`](https://github.com/tobymao/sqlglot/commit/f5358d8a3e2743b5ac0d540f10502d333ad4e082) - add support for GET statements *(PR [#5019](https://github.com/tobymao/sqlglot/pull/5019) by [@eruditmorina](https://github.com/eruditmorina))*:

  add support for GET statements (#5019)


### :sparkles: New Features
- [`6010302`](https://github.com/tobymao/sqlglot/commit/60103020879db5f23a6c4a1775848e31cce13415) - **postgres**: transpile QUARTER interval unit *(PR [#5015](https://github.com/tobymao/sqlglot/pull/5015) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5013](https://github.com/tobymao/sqlglot/issues/5013) opened by [@Wiill007](https://github.com/Wiill007)*
- [`f5358d8`](https://github.com/tobymao/sqlglot/commit/f5358d8a3e2743b5ac0d540f10502d333ad4e082) - **snowflake**: add support for GET statements *(PR [#5019](https://github.com/tobymao/sqlglot/pull/5019) by [@eruditmorina](https://github.com/eruditmorina))*
- [`df5ecdb`](https://github.com/tobymao/sqlglot/commit/df5ecdbebcdce491031538f6baa0f87ec7eefee8) - Include token refereces in the meta of identifier expressions *(PR [#5022](https://github.com/tobymao/sqlglot/pull/5022) by [@izeigerman](https://github.com/izeigerman))*

### :bug: Bug Fixes
- [`c594b63`](https://github.com/tobymao/sqlglot/commit/c594b630c1c940e9a47abfce1633b435a2607f13) - Add MAX_BY & MIN_BY to FUNCTION_PARSER *(PR [#5021](https://github.com/tobymao/sqlglot/pull/5021) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#5020](https://github.com/tobymao/sqlglot/issues/5020) opened by [@omerhadari](https://github.com/omerhadari)*
- [`c1c892c`](https://github.com/tobymao/sqlglot/commit/c1c892cebb89ddf29369ff3c7647f96d217acb71) - **parser**: parse column ops after no-paren type casting *(PR [#5025](https://github.com/tobymao/sqlglot/pull/5025) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5024](https://github.com/tobymao/sqlglot/issues/5024) opened by [@MagdaSousa](https://github.com/MagdaSousa)*


## [v26.16.2] - 2025-04-24
### :sparkles: New Features
- [`5feae00`](https://github.com/tobymao/sqlglot/commit/5feae00ec7a4826285e7fd0be85d377cc0de09b5) - **databricks**: add support for the VOID type *(PR [#5012](https://github.com/tobymao/sqlglot/pull/5012) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`da90228`](https://github.com/tobymao/sqlglot/commit/da90228f1550715646106dd6f9a170d0973f138f) - put a lock around the lazy dialect module loading call *(PR [#5011](https://github.com/tobymao/sqlglot/pull/5011) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#5010](https://github.com/tobymao/sqlglot/issues/5010) opened by [@NickCrews](https://github.com/NickCrews)*


## [v26.16.1] - 2025-04-24
### :sparkles: New Features
- [`27a9fb2`](https://github.com/tobymao/sqlglot/commit/27a9fb26a1936512a09b8b09ed2656e22918f2c6) - **clickhouse**: Support parsing CTAS with alias *(PR [#5003](https://github.com/tobymao/sqlglot/pull/5003) by [@dorranh](https://github.com/dorranh))*
- [`45cd165`](https://github.com/tobymao/sqlglot/commit/45cd165eaca96b33f1de753a147bdc352b9d56d0) - **clickhouse**: Support ClickHouse Nothing type *(PR [#5004](https://github.com/tobymao/sqlglot/pull/5004) by [@dorranh](https://github.com/dorranh))*
- [`ca61a61`](https://github.com/tobymao/sqlglot/commit/ca61a617fa67082bc0fc94853dee4d70b8ca5c59) - Support exp.PartitionByProperty for parse_into() *(PR [#5006](https://github.com/tobymao/sqlglot/pull/5006) by [@erindru](https://github.com/erindru))*
- [`a6d4c3c`](https://github.com/tobymao/sqlglot/commit/a6d4c3c901f828cdd96a16a0e55eac1b244f63be) - **snowflake**: Add numeric parameter support *(PR [#5008](https://github.com/tobymao/sqlglot/pull/5008) by [@hovaesco](https://github.com/hovaesco))*

### :bug: Bug Fixes
- [`8e9dbd4`](https://github.com/tobymao/sqlglot/commit/8e9dbd491b9516c614554e05f05cc1cb976838e3) - **duckdb**: warn on unsupported IGNORE/RESPECT NULLS *(PR [#5002](https://github.com/tobymao/sqlglot/pull/5002) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#5001](https://github.com/tobymao/sqlglot/issues/5001) opened by [@MarcoGorelli](https://github.com/MarcoGorelli)*
- [`10b02bc`](https://github.com/tobymao/sqlglot/commit/10b02bce304042fea09e9cb2369db3c873452245) - **clickhouse**: Support optional timezone argument in date_diff() *(PR [#5005](https://github.com/tobymao/sqlglot/pull/5005) by [@dorranh](https://github.com/dorranh))*

### :wrench: Chores
- [`1d4d906`](https://github.com/tobymao/sqlglot/commit/1d4d906abc60d29b6606bc8eee50c92cef21d3fd) - use _try_parse for parsing ClickHouse's CREATE TABLE .. AS <table> *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fc58c27`](https://github.com/tobymao/sqlglot/commit/fc58c273690734263b971b138ec8f0186f524672) - Refactor placeholder parsing for TokenType.COLON *(PR [#5009](https://github.com/tobymao/sqlglot/pull/5009) by [@VaggelisD](https://github.com/VaggelisD))*


## [v26.16.0] - 2025-04-22
### :boom: BREAKING CHANGES
- due to [`510984f`](https://github.com/tobymao/sqlglot/commit/510984f2ddc6ff13b8a8030f698aed9ad0e6f46b) - stop generating redundant TO_DATE calls *(PR [#4990](https://github.com/tobymao/sqlglot/pull/4990) by [@georgesittas](https://github.com/georgesittas))*:

  stop generating redundant TO_DATE calls (#4990)

- due to [`da9ec61`](https://github.com/tobymao/sqlglot/commit/da9ec61e8edd5049e246390e1b638cf14d50fa2d) - Fix pretty generation of exp.Window *(PR [#4994](https://github.com/tobymao/sqlglot/pull/4994) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix pretty generation of exp.Window (#4994)

- due to [`fb83fac`](https://github.com/tobymao/sqlglot/commit/fb83fac2d097d8d3e8e2556c072792857609bd94) - remove recursion from `simplify` *(PR [#4988](https://github.com/tobymao/sqlglot/pull/4988) by [@georgesittas](https://github.com/georgesittas))*:

  remove recursion from `simplify` (#4988)

- due to [`890b24a`](https://github.com/tobymao/sqlglot/commit/890b24a5cec269f5595743d0a86024a23217a3f1) - remove `connector_depth` as it is now dead code *(commit by [@georgesittas](https://github.com/georgesittas))*:

  remove `connector_depth` as it is now dead code

- due to [`1dc501b`](https://github.com/tobymao/sqlglot/commit/1dc501b8ed68638375d869e11f3bf188948a4990) - remove `max_depth` argument in simplify as it is now dead code *(commit by [@georgesittas](https://github.com/georgesittas))*:

  remove `max_depth` argument in simplify as it is now dead code


### :sparkles: New Features
- [`76535ce`](https://github.com/tobymao/sqlglot/commit/76535ce9487186d2eb7071fac2f224238de7a9ba) - **optimizer**: add support for Spark's TRANSFORM clause *(PR [#4993](https://github.com/tobymao/sqlglot/pull/4993) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4991](https://github.com/tobymao/sqlglot/issues/4991) opened by [@karta0807913](https://github.com/karta0807913)*

### :bug: Bug Fixes
- [`510984f`](https://github.com/tobymao/sqlglot/commit/510984f2ddc6ff13b8a8030f698aed9ad0e6f46b) - **hive**: stop generating redundant TO_DATE calls *(PR [#4990](https://github.com/tobymao/sqlglot/pull/4990) by [@georgesittas](https://github.com/georgesittas))*
- [`da9ec61`](https://github.com/tobymao/sqlglot/commit/da9ec61e8edd5049e246390e1b638cf14d50fa2d) - **generator**: Fix pretty generation of exp.Window *(PR [#4994](https://github.com/tobymao/sqlglot/pull/4994) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4098](https://github.com/TobikoData/sqlmesh/issues/4098) opened by [@tanghyd](https://github.com/tanghyd)*
- [`aae9aa8`](https://github.com/tobymao/sqlglot/commit/aae9aa8f96ccaa7686cda3cdabec208ae4c3d60a) - **optimizer**: ensure there are no shared refs after qualify_tables *(PR [#4995](https://github.com/tobymao/sqlglot/pull/4995) by [@georgesittas](https://github.com/georgesittas))*
- [`adaef42`](https://github.com/tobymao/sqlglot/commit/adaef42234d8f1c9c331f53bee2c42686f29bdec) - **trino**: Dont quote identifiers in string literals for the partitioned_by property *(PR [#4998](https://github.com/tobymao/sqlglot/pull/4998) by [@erindru](https://github.com/erindru))*
- [`a547f8d`](https://github.com/tobymao/sqlglot/commit/a547f8d4292f3b3a4c85f9d6466ead2ad976dfd2) - **postgres**: Capture optional minus sign in interval regex *(PR [#5000](https://github.com/tobymao/sqlglot/pull/5000) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4999](https://github.com/tobymao/sqlglot/issues/4999) opened by [@cpimhoff](https://github.com/cpimhoff)*

### :recycle: Refactors
- [`fb83fac`](https://github.com/tobymao/sqlglot/commit/fb83fac2d097d8d3e8e2556c072792857609bd94) - **optimizer**: remove recursion from `simplify` *(PR [#4988](https://github.com/tobymao/sqlglot/pull/4988) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`890b24a`](https://github.com/tobymao/sqlglot/commit/890b24a5cec269f5595743d0a86024a23217a3f1) - remove `connector_depth` as it is now dead code *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1dc501b`](https://github.com/tobymao/sqlglot/commit/1dc501b8ed68638375d869e11f3bf188948a4990) - remove `max_depth` argument in simplify as it is now dead code *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6572517`](https://github.com/tobymao/sqlglot/commit/6572517c1ec76f14cbd661aacc15c84bef065284) - improve tooling around benchmarks *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.15.0] - 2025-04-17
### :boom: BREAKING CHANGES
- due to [`2b7845a`](https://github.com/tobymao/sqlglot/commit/2b7845a3a821d366ae90ba9ef5e7d61194a34874) - Add support for Athena's Iceberg partitioning transforms *(PR [#4976](https://github.com/tobymao/sqlglot/pull/4976) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for Athena's Iceberg partitioning transforms (#4976)

- due to [`ee794e9`](https://github.com/tobymao/sqlglot/commit/ee794e9c6a3b2fdb142114327d904b6c94a16cd0) - use the standard POWER function instead of ^ fixes [#4982](https://github.com/tobymao/sqlglot/pull/4982) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  use the standard POWER function instead of ^ fixes #4982

- due to [`2369195`](https://github.com/tobymao/sqlglot/commit/2369195635e25dabd5ce26c13e402076508bba04) - consistently parse INTERVAL value as a string *(PR [#4986](https://github.com/tobymao/sqlglot/pull/4986) by [@georgesittas](https://github.com/georgesittas))*:

  consistently parse INTERVAL value as a string (#4986)

- due to [`e866cff`](https://github.com/tobymao/sqlglot/commit/e866cffbaac3b62255d0d5c8be043ab2394af619) - support RELY option for PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints *(PR [#4987](https://github.com/tobymao/sqlglot/pull/4987) by [@geooo109](https://github.com/geooo109))*:

  support RELY option for PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints (#4987)


### :sparkles: New Features
- [`e866cff`](https://github.com/tobymao/sqlglot/commit/e866cffbaac3b62255d0d5c8be043ab2394af619) - **parser**: support RELY option for PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints *(PR [#4987](https://github.com/tobymao/sqlglot/pull/4987) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#4983](https://github.com/tobymao/sqlglot/issues/4983) opened by [@ggadon](https://github.com/ggadon)*

### :bug: Bug Fixes
- [`2b7845a`](https://github.com/tobymao/sqlglot/commit/2b7845a3a821d366ae90ba9ef5e7d61194a34874) - Add support for Athena's Iceberg partitioning transforms *(PR [#4976](https://github.com/tobymao/sqlglot/pull/4976) by [@VaggelisD](https://github.com/VaggelisD))*
- [`fa6af23`](https://github.com/tobymao/sqlglot/commit/fa6af2302f8482c5d89ead481afe4195aaa41a9c) - **optimizer**: compare the whole type to determine if a cast can be removed *(PR [#4981](https://github.com/tobymao/sqlglot/pull/4981) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4977](https://github.com/tobymao/sqlglot/issues/4977) opened by [@MeinAccount](https://github.com/MeinAccount)*
- [`830c9b8`](https://github.com/tobymao/sqlglot/commit/830c9b8bbf906cf5d4fa8028b67dadda73fc58a9) - **unnest_subqueries**: avoid adding GROUP BY on aggregate projections in lateral subqueries *(PR [#4970](https://github.com/tobymao/sqlglot/pull/4970) by [@skadel](https://github.com/skadel))*
- [`ee794e9`](https://github.com/tobymao/sqlglot/commit/ee794e9c6a3b2fdb142114327d904b6c94a16cd0) - **postgres**: use the standard POWER function instead of ^ fixes [#4982](https://github.com/tobymao/sqlglot/pull/4982) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`85e62b8`](https://github.com/tobymao/sqlglot/commit/85e62b88df2822797f527dce4eaa230c778cbe9e) - **bigquery**: Do not consume JOIN keywords after WITH OFFSET *(PR [#4984](https://github.com/tobymao/sqlglot/pull/4984) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2369195`](https://github.com/tobymao/sqlglot/commit/2369195635e25dabd5ce26c13e402076508bba04) - consistently parse INTERVAL value as a string *(PR [#4986](https://github.com/tobymao/sqlglot/pull/4986) by [@georgesittas](https://github.com/georgesittas))*


## [v26.14.0] - 2025-04-15
### :boom: BREAKING CHANGES
- due to [`cb20038`](https://github.com/tobymao/sqlglot/commit/cb2003875fc6e149bd4a631e99c312a04435a46b) - treat GO as command *(PR [#4978](https://github.com/tobymao/sqlglot/pull/4978) by [@georgesittas](https://github.com/georgesittas))*:

  treat GO as command (#4978)

- due to [`60e26b8`](https://github.com/tobymao/sqlglot/commit/60e26b868242a05a7fdc2725bd21a127910a6fb7) - improve transpilability of GET_JSON_OBJECT by parsing json path *(PR [#4980](https://github.com/tobymao/sqlglot/pull/4980) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilability of GET_JSON_OBJECT by parsing json path (#4980)


### :bug: Bug Fixes
- [`cb20038`](https://github.com/tobymao/sqlglot/commit/cb2003875fc6e149bd4a631e99c312a04435a46b) - **tsql**: treat GO as command *(PR [#4978](https://github.com/tobymao/sqlglot/pull/4978) by [@georgesittas](https://github.com/georgesittas))*
- [`60e26b8`](https://github.com/tobymao/sqlglot/commit/60e26b868242a05a7fdc2725bd21a127910a6fb7) - **hive**: improve transpilability of GET_JSON_OBJECT by parsing json path *(PR [#4980](https://github.com/tobymao/sqlglot/pull/4980) by [@georgesittas](https://github.com/georgesittas))*


## [v26.13.2] - 2025-04-14
### :bug: Bug Fixes
- [`9693dbd`](https://github.com/tobymao/sqlglot/commit/9693dbd18b98b2699cade738a254f71f2ee8ce74) - **clickhouse**: avoid superfluous parentheses in DISTINCT ON (...) *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.13.1] - 2025-04-14
### :sparkles: New Features
- [`a762993`](https://github.com/tobymao/sqlglot/commit/a762993c53d7ae91a831a8be448010e17e60f497) - **generator**: unsupported warning for T-SQL query option *(PR [#4972](https://github.com/tobymao/sqlglot/pull/4972) by [@geooo109](https://github.com/geooo109))*

### :bug: Bug Fixes
- [`61bc01c`](https://github.com/tobymao/sqlglot/commit/61bc01ceec2f801490f3f1a571aee655c5109962) - **clickhouse**: allow string literal for clickhouse ON CLUSTER clause *(PR [#4971](https://github.com/tobymao/sqlglot/pull/4971) by [@lepfhty](https://github.com/lepfhty))*
- [`1353b79`](https://github.com/tobymao/sqlglot/commit/1353b79bd9810788a02163928b044fe038267078) - **Snowflake**: Enhance parity for FILE_FORMAT & CREDENTIALS in CREATE STAGE *(PR [#4969](https://github.com/tobymao/sqlglot/pull/4969) by [@whummer](https://github.com/whummer))*


## [v26.13.0] - 2025-04-11
### :boom: BREAKING CHANGES
- due to [`1df7f61`](https://github.com/tobymao/sqlglot/commit/1df7f611bc96616cb07950a80f6669d0bc331b0e) - refactor length_sql so it handles any type, not just varchar/blob *(PR [#4935](https://github.com/tobymao/sqlglot/pull/4935) by [@tekumara](https://github.com/tekumara))*:

  refactor length_sql so it handles any type, not just varchar/blob (#4935)

- due to [`52719f3`](https://github.com/tobymao/sqlglot/commit/52719f37f6541e8ec9f66642ac23ed9015048092) - parse CREATE STAGE *(PR [#4947](https://github.com/tobymao/sqlglot/pull/4947) by [@tekumara](https://github.com/tekumara))*:

  parse CREATE STAGE (#4947)

- due to [`fd39b30`](https://github.com/tobymao/sqlglot/commit/fd39b30209d068b787619b8137a105aca9c3e607) - parse CREATE FILE FORMAT *(PR [#4948](https://github.com/tobymao/sqlglot/pull/4948) by [@tekumara](https://github.com/tekumara))*:

  parse CREATE FILE FORMAT (#4948)

- due to [`f835756`](https://github.com/tobymao/sqlglot/commit/f835756257f735643584b89e93693e8577744731) - Fix CREATE EXTERNAL TABLE properties *(PR [#4951](https://github.com/tobymao/sqlglot/pull/4951) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix CREATE EXTERNAL TABLE properties (#4951)

- due to [`44b955b`](https://github.com/tobymao/sqlglot/commit/44b955bd537bfb8f5b6e84ecbcd5f6e3da852260) - Fix generation of exp.Values *(PR [#4930](https://github.com/tobymao/sqlglot/pull/4930) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix generation of exp.Values (#4930)

- due to [`1f506b1`](https://github.com/tobymao/sqlglot/commit/1f506b186f1b954829195eefda318e231d474208) - support SHOW (ALL) TABLES *(PR [#4961](https://github.com/tobymao/sqlglot/pull/4961) by [@mscolnick](https://github.com/mscolnick))*:

  support SHOW (ALL) TABLES (#4961)

- due to [`72cf4a4`](https://github.com/tobymao/sqlglot/commit/72cf4a4501a8d122041a28b71be5a41ffb53602a) - Add support for PIVOT multiple IN clauses *(PR [#4964](https://github.com/tobymao/sqlglot/pull/4964) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for PIVOT multiple IN clauses (#4964)

- due to [`400ea54`](https://github.com/tobymao/sqlglot/commit/400ea54d3a9cab256bfa5e496439bb9be6072d0b) - ensure JSON_FORMAT type is JSON when targeting Presto *(PR [#4968](https://github.com/tobymao/sqlglot/pull/4968) by [@georgesittas](https://github.com/georgesittas))*:

  ensure JSON_FORMAT type is JSON when targeting Presto (#4968)


### :sparkles: New Features
- [`52719f3`](https://github.com/tobymao/sqlglot/commit/52719f37f6541e8ec9f66642ac23ed9015048092) - **snowflake**: parse CREATE STAGE *(PR [#4947](https://github.com/tobymao/sqlglot/pull/4947) by [@tekumara](https://github.com/tekumara))*
- [`fd39b30`](https://github.com/tobymao/sqlglot/commit/fd39b30209d068b787619b8137a105aca9c3e607) - **snowflake**: parse CREATE FILE FORMAT *(PR [#4948](https://github.com/tobymao/sqlglot/pull/4948) by [@tekumara](https://github.com/tekumara))*
- [`da9a6a1`](https://github.com/tobymao/sqlglot/commit/da9a6a1d56323319b87e9b193d12ad1c644b9239) - **snowflake**: parse SHOW STAGES *(PR [#4949](https://github.com/tobymao/sqlglot/pull/4949) by [@tekumara](https://github.com/tekumara))*
- [`bfdcdf0`](https://github.com/tobymao/sqlglot/commit/bfdcdf0afc0f4af3dacdfc3e8dca243793552b74) - **snowflake**: parse SHOW FILE FORMATS *(PR [#4950](https://github.com/tobymao/sqlglot/pull/4950) by [@tekumara](https://github.com/tekumara))*
- [`c591443`](https://github.com/tobymao/sqlglot/commit/c591443b6b2328780e08179144557e181db0cbb6) - **duckdb**: add support for GROUP clause in standard PIVOT syntax *(PR [#4953](https://github.com/tobymao/sqlglot/pull/4953) by [@georgesittas](https://github.com/georgesittas))*
- [`b011ee2`](https://github.com/tobymao/sqlglot/commit/b011ee2df0beaac75b982261a25d3e787dead54a) - **bigquery**: Add support for side & kind on set operators *(PR [#4959](https://github.com/tobymao/sqlglot/pull/4959) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4942](https://github.com/tobymao/sqlglot/issues/4942) opened by [@z3z1ma](https://github.com/z3z1ma)*
- [`1f506b1`](https://github.com/tobymao/sqlglot/commit/1f506b186f1b954829195eefda318e231d474208) - **duckdb**: support SHOW (ALL) TABLES *(PR [#4961](https://github.com/tobymao/sqlglot/pull/4961) by [@mscolnick](https://github.com/mscolnick))*
  - :arrow_lower_right: *addresses issue [#4956](https://github.com/tobymao/sqlglot/issues/4956) opened by [@mscolnick](https://github.com/mscolnick)*
- [`ad5b595`](https://github.com/tobymao/sqlglot/commit/ad5b595049a16a27a7f249afea43dbcfcf43b5f4) - allow explicit aliasing in if(...) expressions *(PR [#4963](https://github.com/tobymao/sqlglot/pull/4963) by [@georgesittas](https://github.com/georgesittas))*
- [`72cf4a4`](https://github.com/tobymao/sqlglot/commit/72cf4a4501a8d122041a28b71be5a41ffb53602a) - **duckdb**: Add support for PIVOT multiple IN clauses *(PR [#4964](https://github.com/tobymao/sqlglot/pull/4964) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4944](https://github.com/tobymao/sqlglot/issues/4944) opened by [@nph](https://github.com/nph)*
- [`7bc5a21`](https://github.com/tobymao/sqlglot/commit/7bc5a217c3cc68d0cb1eaedc0c18f5188de80bf1) - **postgres**: support laterals with ordinality fixes [#4965](https://github.com/tobymao/sqlglot/pull/4965) *(PR [#4966](https://github.com/tobymao/sqlglot/pull/4966) by [@georgesittas](https://github.com/georgesittas))*
- [`400ea54`](https://github.com/tobymao/sqlglot/commit/400ea54d3a9cab256bfa5e496439bb9be6072d0b) - ensure JSON_FORMAT type is JSON when targeting Presto *(PR [#4968](https://github.com/tobymao/sqlglot/pull/4968) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4967](https://github.com/tobymao/sqlglot/issues/4967) opened by [@jmsmdy](https://github.com/jmsmdy)*

### :bug: Bug Fixes
- [`7042603`](https://github.com/tobymao/sqlglot/commit/7042603ecb5693795b15219ec9cebf2f76032c03) - **optimizer**: Merge subqueries when inner query has name conflict with outer query *(PR [#4931](https://github.com/tobymao/sqlglot/pull/4931) by [@barakalon](https://github.com/barakalon))*
- [`1df7f61`](https://github.com/tobymao/sqlglot/commit/1df7f611bc96616cb07950a80f6669d0bc331b0e) - **duckdb**: refactor length_sql so it handles any type, not just varchar/blob *(PR [#4935](https://github.com/tobymao/sqlglot/pull/4935) by [@tekumara](https://github.com/tekumara))*
  - :arrow_lower_right: *fixes issue [#4934](https://github.com/tobymao/sqlglot/issues/4934) opened by [@tekumara](https://github.com/tekumara)*
- [`09882e3`](https://github.com/tobymao/sqlglot/commit/09882e32f057670a9cbd97c1e5cf1a00c774b5d2) - **tsql**: remove assert call from _build_formatted_time *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`bf39a95`](https://github.com/tobymao/sqlglot/commit/bf39a95426ed6637e424da1be070cc9a8affc358) - **sqlite**: transpile double quoted PRIMARY KEY *(PR [#4941](https://github.com/tobymao/sqlglot/pull/4941) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4938](https://github.com/tobymao/sqlglot/issues/4938) opened by [@rgeronimi](https://github.com/rgeronimi)*
- [`f835756`](https://github.com/tobymao/sqlglot/commit/f835756257f735643584b89e93693e8577744731) - **snowflake**: Fix CREATE EXTERNAL TABLE properties *(PR [#4951](https://github.com/tobymao/sqlglot/pull/4951) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4945](https://github.com/tobymao/sqlglot/issues/4945) opened by [@tekumara](https://github.com/tekumara)*
- [`61ed971`](https://github.com/tobymao/sqlglot/commit/61ed971213c979c3777e57853bd6989bc169adb1) - **athena**: Correctly handle CTAS queries that contain Union's *(PR [#4955](https://github.com/tobymao/sqlglot/pull/4955) by [@erindru](https://github.com/erindru))*
- [`44b955b`](https://github.com/tobymao/sqlglot/commit/44b955bd537bfb8f5b6e84ecbcd5f6e3da852260) - **clickhouse**: Fix generation of exp.Values *(PR [#4930](https://github.com/tobymao/sqlglot/pull/4930) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4056](https://github.com/TobikoData/sqlmesh/issues/4056) opened by [@dnbnero](https://github.com/dnbnero)*


## [v26.12.0] - 2025-03-27
### :boom: BREAKING CHANGES
- due to [`8a692b9`](https://github.com/tobymao/sqlglot/commit/8a692b9b5b7982ed54444bddfe974e5f629183ff) - support select...into #temp_table syntax *(PR [#4893](https://github.com/tobymao/sqlglot/pull/4893) by [@hhubbell](https://github.com/hhubbell))*:

  support select...into #temp_table syntax (#4893)

- due to [`bcf311a`](https://github.com/tobymao/sqlglot/commit/bcf311a4af4b1a95e038befc0bc84627c4851e5f) - Preserve PARSE_JSON() *(PR [#4901](https://github.com/tobymao/sqlglot/pull/4901) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve PARSE_JSON() (#4901)

- due to [`937b7bd`](https://github.com/tobymao/sqlglot/commit/937b7bdd5b06daffee379390796c76ffb07c2588) - handle string interval values in DATE ADD/SUB *(PR [#4902](https://github.com/tobymao/sqlglot/pull/4902) by [@georgesittas](https://github.com/georgesittas))*:

  handle string interval values in DATE ADD/SUB (#4902)

- due to [`96749c1`](https://github.com/tobymao/sqlglot/commit/96749c144832b491f01de387cd2f7a9b769af626) - improve LATERAL VIEW EXPLODE transpilation *(PR [#4905](https://github.com/tobymao/sqlglot/pull/4905) by [@georgesittas](https://github.com/georgesittas))*:

  improve LATERAL VIEW EXPLODE transpilation (#4905)

- due to [`71c529a`](https://github.com/tobymao/sqlglot/commit/71c529a13db1690412829ac03b82ff72d44ce6c2) - disable lateral alias expansion for Oracle fixes [#4910](https://github.com/tobymao/sqlglot/pull/4910) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  disable lateral alias expansion for Oracle fixes #4910

- due to [`f17004e`](https://github.com/tobymao/sqlglot/commit/f17004e1691c9d834e295452a960a6e3a2830e88) - only use ARRAY[...] syntax for Schema if parent is partitioned by prop *(PR [#4913](https://github.com/tobymao/sqlglot/pull/4913) by [@georgesittas](https://github.com/georgesittas))*:

  only use ARRAY[...] syntax for Schema if parent is partitioned by prop (#4913)

- due to [`2fbbf6a`](https://github.com/tobymao/sqlglot/commit/2fbbf6a8525385f53bcb3e588d665208ac6811c1) - infer timestamp function types as TIMESTAMPTZ for bigquery *(PR [#4914](https://github.com/tobymao/sqlglot/pull/4914) by [@georgesittas](https://github.com/georgesittas))*:

  infer timestamp function types as TIMESTAMPTZ for bigquery (#4914)

- due to [`c0b3448`](https://github.com/tobymao/sqlglot/commit/c0b3448e7a4ec46485dd65b7498855ab57e029ef) - parse at sign as ABS function *(PR [#4915](https://github.com/tobymao/sqlglot/pull/4915) by [@geooo109](https://github.com/geooo109))*:

  parse at sign as ABS function (#4915)

- due to [`aa9734d`](https://github.com/tobymao/sqlglot/commit/aa9734df473d1aed8e5a53a7ef8e4d3208c8296d) - improve pretty-formatting of IN (...) *(PR [#4920](https://github.com/tobymao/sqlglot/pull/4920) by [@georgesittas](https://github.com/georgesittas))*:

  improve pretty-formatting of IN (...) (#4920)


### :sparkles: New Features
- [`1d6218e`](https://github.com/tobymao/sqlglot/commit/1d6218e386b3b1a5081272e179b8e48ec57153b4) - **snowflake**: add support for CREATE USING TEMPLATE closes [#4883](https://github.com/tobymao/sqlglot/pull/4883) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fa9f9bd`](https://github.com/tobymao/sqlglot/commit/fa9f9bde626ddb3b8b5ad3dedc6aa3399b8c1716) - **tsql**: allow MERGE to be used in place of a subquery *(PR [#4890](https://github.com/tobymao/sqlglot/pull/4890) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4884](https://github.com/tobymao/sqlglot/issues/4884) opened by [@AndysonEjvind](https://github.com/AndysonEjvind)*
- [`0de4503`](https://github.com/tobymao/sqlglot/commit/0de4503655ae9169ae02fdc8c48fb1edcd868cc8) - add a check and error message for set operations in pushdown_projections *(PR [#4897](https://github.com/tobymao/sqlglot/pull/4897) by [@snovik75](https://github.com/snovik75))*
- [`96749c1`](https://github.com/tobymao/sqlglot/commit/96749c144832b491f01de387cd2f7a9b769af626) - **presto**: improve LATERAL VIEW EXPLODE transpilation *(PR [#4905](https://github.com/tobymao/sqlglot/pull/4905) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4879](https://github.com/tobymao/sqlglot/issues/4879) opened by [@Juanpeterjuanpa](https://github.com/Juanpeterjuanpa)*
- [`c0b3448`](https://github.com/tobymao/sqlglot/commit/c0b3448e7a4ec46485dd65b7498855ab57e029ef) - **duckdb**: parse at sign as ABS function *(PR [#4915](https://github.com/tobymao/sqlglot/pull/4915) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#4912](https://github.com/tobymao/sqlglot/issues/4912) opened by [@suresh-summation](https://github.com/suresh-summation)*
- [`aa9734d`](https://github.com/tobymao/sqlglot/commit/aa9734df473d1aed8e5a53a7ef8e4d3208c8296d) - **generator**: improve pretty-formatting of IN (...) *(PR [#4920](https://github.com/tobymao/sqlglot/pull/4920) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4015](https://github.com/TobikoData/sqlmesh/issues/4015) opened by [@petrikoro](https://github.com/petrikoro)*

### :bug: Bug Fixes
- [`1617509`](https://github.com/tobymao/sqlglot/commit/1617509d44124ffaba7eaf139023df07c3ad1636) - **bigquery**: preserve time zone info in FORMAT_TIMESTAMP roundtrip *(PR [#4895](https://github.com/tobymao/sqlglot/pull/4895) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4894](https://github.com/tobymao/sqlglot/issues/4894) opened by [@lenare1K5](https://github.com/lenare1K5)*
- [`8a692b9`](https://github.com/tobymao/sqlglot/commit/8a692b9b5b7982ed54444bddfe974e5f629183ff) - **tsql**: support select...into #temp_table syntax *(PR [#4893](https://github.com/tobymao/sqlglot/pull/4893) by [@hhubbell](https://github.com/hhubbell))*
- [`bcf311a`](https://github.com/tobymao/sqlglot/commit/bcf311a4af4b1a95e038befc0bc84627c4851e5f) - **databricks**: Preserve PARSE_JSON() *(PR [#4901](https://github.com/tobymao/sqlglot/pull/4901) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4898](https://github.com/tobymao/sqlglot/issues/4898) opened by [@h2o1](https://github.com/h2o1)*
- [`3040a5e`](https://github.com/tobymao/sqlglot/commit/3040a5e4ebc778795251a74cf3de2169337aca55) - preserve whitespace in quoted identifiers and strings *(PR [#4903](https://github.com/tobymao/sqlglot/pull/4903) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4900](https://github.com/tobymao/sqlglot/issues/4900) opened by [@npochhi](https://github.com/npochhi)*
- [`937b7bd`](https://github.com/tobymao/sqlglot/commit/937b7bdd5b06daffee379390796c76ffb07c2588) - **hive**: handle string interval values in DATE ADD/SUB *(PR [#4902](https://github.com/tobymao/sqlglot/pull/4902) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4899](https://github.com/tobymao/sqlglot/issues/4899) opened by [@ricardo-rolo](https://github.com/ricardo-rolo)*
- [`71c529a`](https://github.com/tobymao/sqlglot/commit/71c529a13db1690412829ac03b82ff72d44ce6c2) - **optimizer**: disable lateral alias expansion for Oracle fixes [#4910](https://github.com/tobymao/sqlglot/pull/4910) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`3b7c699`](https://github.com/tobymao/sqlglot/commit/3b7c699267bf4f041a033017a894f0c1e2ae4068) - **snowflake**: quote identifiers in stage references *(PR [#4906](https://github.com/tobymao/sqlglot/pull/4906) by [@whummer](https://github.com/whummer))*
- [`f17004e`](https://github.com/tobymao/sqlglot/commit/f17004e1691c9d834e295452a960a6e3a2830e88) - **presto**: only use ARRAY[...] syntax for Schema if parent is partitioned by prop *(PR [#4913](https://github.com/tobymao/sqlglot/pull/4913) by [@georgesittas](https://github.com/georgesittas))*
- [`2fbbf6a`](https://github.com/tobymao/sqlglot/commit/2fbbf6a8525385f53bcb3e588d665208ac6811c1) - **optimizer**: infer timestamp function types as TIMESTAMPTZ for bigquery *(PR [#4914](https://github.com/tobymao/sqlglot/pull/4914) by [@georgesittas](https://github.com/georgesittas))*
- [`ff6be71`](https://github.com/tobymao/sqlglot/commit/ff6be715b7d44700b595bbd5c83f65c28b52e191) - **optimizer**: avoid merging window function nested under a projection *(PR [#4919](https://github.com/tobymao/sqlglot/pull/4919) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4907](https://github.com/tobymao/sqlglot/issues/4907) opened by [@Rejudge-F](https://github.com/Rejudge-F)*

### :recycle: Refactors
- [`d386f37`](https://github.com/tobymao/sqlglot/commit/d386f374a6108ecce4e48324fe487c0955ab63b3) - **sqlite**: move generator methods within SQLite class *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.11.1] - 2025-03-18
### :bug: Bug Fixes
- [`d7b3b3e`](https://github.com/tobymao/sqlglot/commit/d7b3b3e89720d1783d092a2c60a9c2209d9984a2) - **optimizer**: handle TableFromRows properly in annotate_types *(PR [#4889](https://github.com/tobymao/sqlglot/pull/4889) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4004](https://github.com/TobikoData/sqlmesh/issues/4004) opened by [@hmeng-taproot](https://github.com/hmeng-taproot)*


## [v26.11.0] - 2025-03-17
### :boom: BREAKING CHANGES
- due to [`ac3d311`](https://github.com/tobymao/sqlglot/commit/ac3d311c4184ca2ced603a100588e3e7435ce352) - do not expand having expressions if they conflict with a projection *(PR [#4881](https://github.com/tobymao/sqlglot/pull/4881) by [@tobymao](https://github.com/tobymao))*:

  do not expand having expressions if they conflict with a projection (#4881)

- due to [`081994e`](https://github.com/tobymao/sqlglot/commit/081994ea85c7aa1cbbbc40a24857dba4fd6c1c61) - Fix parsing multi-part format name *(PR [#4885](https://github.com/tobymao/sqlglot/pull/4885) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix parsing multi-part format name (#4885)

- due to [`491c407`](https://github.com/tobymao/sqlglot/commit/491c407d48a24b6d4093e9c9bfdc3d8c27c29e4c) - parse parameter key as Var instead of Identifier *(PR [#4888](https://github.com/tobymao/sqlglot/pull/4888) by [@georgesittas](https://github.com/georgesittas))*:

  parse parameter key as Var instead of Identifier (#4888)


### :bug: Bug Fixes
- [`ac3d311`](https://github.com/tobymao/sqlglot/commit/ac3d311c4184ca2ced603a100588e3e7435ce352) - do not expand having expressions if they conflict with a projection *(PR [#4881](https://github.com/tobymao/sqlglot/pull/4881) by [@tobymao](https://github.com/tobymao))*
- [`44b7b09`](https://github.com/tobymao/sqlglot/commit/44b7b09deca881e274ad03068eee5d4d594c8ca8) - **parser**: Fix separator generation for STRING_AGG *(PR [#4887](https://github.com/tobymao/sqlglot/pull/4887) by [@VaggelisD](https://github.com/VaggelisD))*
- [`081994e`](https://github.com/tobymao/sqlglot/commit/081994ea85c7aa1cbbbc40a24857dba4fd6c1c61) - **snowflake**: Fix parsing multi-part format name *(PR [#4885](https://github.com/tobymao/sqlglot/pull/4885) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4882](https://github.com/tobymao/sqlglot/issues/4882) opened by [@kharigardner](https://github.com/kharigardner)*
- [`38111a5`](https://github.com/tobymao/sqlglot/commit/38111a5eaa6bde640e25aa408ff7ea9ea6864c0b) - apply unpivot alias string conversion only for UNPIVOT *(PR [#4886](https://github.com/tobymao/sqlglot/pull/4886) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4003](https://github.com/TobikoData/sqlmesh/issues/4003) opened by [@lucargir](https://github.com/lucargir)*
- [`491c407`](https://github.com/tobymao/sqlglot/commit/491c407d48a24b6d4093e9c9bfdc3d8c27c29e4c) - **clickhouse**: parse parameter key as Var instead of Identifier *(PR [#4888](https://github.com/tobymao/sqlglot/pull/4888) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4002](https://github.com/TobikoData/sqlmesh/issues/4002) opened by [@petrjanda](https://github.com/petrjanda)*


## [v26.10.1] - 2025-03-13
### :bug: Bug Fixes
- [`2b3824f`](https://github.com/tobymao/sqlglot/commit/2b3824f0bac5dae48ea7eecbe2168afe79038d06) - **duckdb**: revert timestamp/datetime -> timestampntz parsing temporarily *(PR [#4878](https://github.com/tobymao/sqlglot/pull/4878) by [@georgesittas](https://github.com/georgesittas))*


## [v26.10.0] - 2025-03-13
### :boom: BREAKING CHANGES
- due to [`c0bfcc6`](https://github.com/tobymao/sqlglot/commit/c0bfcc66b97ce667a1ead608c4fbbee69db633fa) - postgres case insesitive formats closes [#4860](https://github.com/tobymao/sqlglot/pull/4860) *(commit by [@tobymao](https://github.com/tobymao))*:

  postgres case insesitive formats closes #4860

- due to [`6914684`](https://github.com/tobymao/sqlglot/commit/69146842d005ae0edecbd7f6f842f648ae0622e7) - duckdb defaults timestampntz closes [#4859](https://github.com/tobymao/sqlglot/pull/4859) *(commit by [@tobymao](https://github.com/tobymao))*:

  duckdb defaults timestampntz closes #4859

- due to [`ceb1f02`](https://github.com/tobymao/sqlglot/commit/ceb1f026dd04926a6a210de9d16da4dffef4717c) - support TO_CHAR to duckdb STRFTIME *(PR [#4866](https://github.com/tobymao/sqlglot/pull/4866) by [@geooo109](https://github.com/geooo109))*:

  support TO_CHAR to duckdb STRFTIME (#4866)

- due to [`d748e53`](https://github.com/tobymao/sqlglot/commit/d748e53f6a77196bef6550b6d9fddf41076c01fa) - Introduce pyproject.toml and switch to packaging via build *(PR [#4865](https://github.com/tobymao/sqlglot/pull/4865) by [@erindru](https://github.com/erindru))*:

  Introduce pyproject.toml and switch to packaging via build (#4865)

- due to [`038da09`](https://github.com/tobymao/sqlglot/commit/038da09f620cf057e4576b719c4e2f6712cbb804) - treat TABLE(...) as a UDTF *(PR [#4875](https://github.com/tobymao/sqlglot/pull/4875) by [@georgesittas](https://github.com/georgesittas))*:

  treat TABLE(...) as a UDTF (#4875)

- due to [`92e479e`](https://github.com/tobymao/sqlglot/commit/92e479ea7d70efc4bdccd17cb12b719aec603830) - support STRUCT(*) and MAP(*) *(PR [#4876](https://github.com/tobymao/sqlglot/pull/4876) by [@geooo109](https://github.com/geooo109))*:

  support STRUCT(*) and MAP(*) (#4876)

- due to [`87c94fe`](https://github.com/tobymao/sqlglot/commit/87c94fe91aa2a4bc2c255191d92aed450f3c7998) - turn off multi-arg coalesce simplification *(PR [#4877](https://github.com/tobymao/sqlglot/pull/4877) by [@georgesittas](https://github.com/georgesittas))*:

  turn off multi-arg coalesce simplification (#4877)


### :sparkles: New Features
- [`54be278`](https://github.com/tobymao/sqlglot/commit/54be278361496367fb2f7d380634d3390879e58d) - **snowflake**: add support for HEX_DECODE_BINARY *(PR [#4855](https://github.com/tobymao/sqlglot/pull/4855) by [@sk-](https://github.com/sk-))*
  - :arrow_lower_right: *addresses issue [#4852](https://github.com/tobymao/sqlglot/issues/4852) opened by [@sk-](https://github.com/sk-)*
- [`47959a9`](https://github.com/tobymao/sqlglot/commit/47959a94a4693cb904cfb2e50ce8cc8ca5c2e22f) - **duckdb**: add support for prefix aliases *(PR [#4869](https://github.com/tobymao/sqlglot/pull/4869) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`e942391`](https://github.com/tobymao/sqlglot/commit/e942391edcefb40f927887450765b4365b0e980d) - spark zone offset closes [#4858](https://github.com/tobymao/sqlglot/pull/4858) *(commit by [@tobymao](https://github.com/tobymao))*
- [`c0bfcc6`](https://github.com/tobymao/sqlglot/commit/c0bfcc66b97ce667a1ead608c4fbbee69db633fa) - postgres case insesitive formats closes [#4860](https://github.com/tobymao/sqlglot/pull/4860) *(commit by [@tobymao](https://github.com/tobymao))*
- [`6914684`](https://github.com/tobymao/sqlglot/commit/69146842d005ae0edecbd7f6f842f648ae0622e7) - duckdb defaults timestampntz closes [#4859](https://github.com/tobymao/sqlglot/pull/4859) *(commit by [@tobymao](https://github.com/tobymao))*
- [`ceb1f02`](https://github.com/tobymao/sqlglot/commit/ceb1f026dd04926a6a210de9d16da4dffef4717c) - **snowflake**: support TO_CHAR to duckdb STRFTIME *(PR [#4866](https://github.com/tobymao/sqlglot/pull/4866) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4857](https://github.com/tobymao/sqlglot/issues/4857) opened by [@asarama](https://github.com/asarama)*
- [`80466f1`](https://github.com/tobymao/sqlglot/commit/80466f16aa081860bc9e65f425924a0620840cdf) - expand util - align normalization behaviour with lazy and non-lazy source providers. *(PR [#4874](https://github.com/tobymao/sqlglot/pull/4874) by [@omerhadari](https://github.com/omerhadari))*
- [`038da09`](https://github.com/tobymao/sqlglot/commit/038da09f620cf057e4576b719c4e2f6712cbb804) - **snowflake**: treat TABLE(...) as a UDTF *(PR [#4875](https://github.com/tobymao/sqlglot/pull/4875) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4861](https://github.com/tobymao/sqlglot/issues/4861) opened by [@mattijsdp](https://github.com/mattijsdp)*
- [`92e479e`](https://github.com/tobymao/sqlglot/commit/92e479ea7d70efc4bdccd17cb12b719aec603830) - **hive**: support STRUCT(*) and MAP(*) *(PR [#4876](https://github.com/tobymao/sqlglot/pull/4876) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4871](https://github.com/tobymao/sqlglot/issues/4871) opened by [@btyuhas](https://github.com/btyuhas)*
- [`87c94fe`](https://github.com/tobymao/sqlglot/commit/87c94fe91aa2a4bc2c255191d92aed450f3c7998) - **redshift**: turn off multi-arg coalesce simplification *(PR [#4877](https://github.com/tobymao/sqlglot/pull/4877) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`d748e53`](https://github.com/tobymao/sqlglot/commit/d748e53f6a77196bef6550b6d9fddf41076c01fa) - Introduce pyproject.toml and switch to packaging via build *(PR [#4865](https://github.com/tobymao/sqlglot/pull/4865) by [@erindru](https://github.com/erindru))*


## [v26.9.0] - 2025-03-07
### :boom: BREAKING CHANGES
- due to [`6a3973b`](https://github.com/tobymao/sqlglot/commit/6a3973b7da639a19634bc352ea76f75735114c38) - Refactor exp.GroupConcat generation *(PR [#4823](https://github.com/tobymao/sqlglot/pull/4823) by [@VaggelisD](https://github.com/VaggelisD))*:

  Refactor exp.GroupConcat generation (#4823)

- due to [`813d2ad`](https://github.com/tobymao/sqlglot/commit/813d2ada7afd653b2aaff75cbddd7f011750f861) - use _parse_table_parts for udf parsing *(PR [#4829](https://github.com/tobymao/sqlglot/pull/4829) by [@geooo109](https://github.com/geooo109))*:

  use _parse_table_parts for udf parsing (#4829)

- due to [`7cdbad6`](https://github.com/tobymao/sqlglot/commit/7cdbad688cad7e7ce40df99802e93deb6a4d7abf) - add initial support for PUT statements *(PR [#4818](https://github.com/tobymao/sqlglot/pull/4818) by [@whummer](https://github.com/whummer))*:

  add initial support for PUT statements (#4818)

- due to [`8c0a6be`](https://github.com/tobymao/sqlglot/commit/8c0a6bec6e38f3f6ce9a90b6a9b6457de70c7228) - BLOB transpilation *(PR [#4844](https://github.com/tobymao/sqlglot/pull/4844) by [@geooo109](https://github.com/geooo109))*:

  BLOB transpilation (#4844)


### :sparkles: New Features
- [`7e8975e`](https://github.com/tobymao/sqlglot/commit/7e8975efce0af350142f8fb437cf46dd46f2b8d9) - **oracle**: add FORCE property *(PR [#4828](https://github.com/tobymao/sqlglot/pull/4828) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#4826](https://github.com/tobymao/sqlglot/issues/4826) opened by [@Duchyna1](https://github.com/Duchyna1)*
- [`7cdbad6`](https://github.com/tobymao/sqlglot/commit/7cdbad688cad7e7ce40df99802e93deb6a4d7abf) - **snowflake**: add initial support for PUT statements *(PR [#4818](https://github.com/tobymao/sqlglot/pull/4818) by [@whummer](https://github.com/whummer))*
  - :arrow_lower_right: *addresses issue [#4813](https://github.com/tobymao/sqlglot/issues/4813) opened by [@whummer](https://github.com/whummer)*
- [`f4d1a1f`](https://github.com/tobymao/sqlglot/commit/f4d1a1f4d8104b2efd56f568ca99c7e768466d19) - **hive**: add support for STORED BY syntax for storage handlers *(PR [#4832](https://github.com/tobymao/sqlglot/pull/4832) by [@tsamaras](https://github.com/tsamaras))*
- [`b7a0df1`](https://github.com/tobymao/sqlglot/commit/b7a0df1b9a9cff2cd57db77ac0095c189b9d67ab) - **parser**: Support trailing commas after from *(PR [#4854](https://github.com/tobymao/sqlglot/pull/4854) by [@omerhadari](https://github.com/omerhadari))*

### :bug: Bug Fixes
- [`6a3973b`](https://github.com/tobymao/sqlglot/commit/6a3973b7da639a19634bc352ea76f75735114c38) - **duckdb, snowflake**: Refactor exp.GroupConcat generation *(PR [#4823](https://github.com/tobymao/sqlglot/pull/4823) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4821](https://github.com/tobymao/sqlglot/issues/4821) opened by [@asarama](https://github.com/asarama)*
- [`08eb7f2`](https://github.com/tobymao/sqlglot/commit/08eb7f2032957c2fe3119963f344538b90d8f631) - **snowflake**: clean up PUT implementation *(PR [#4830](https://github.com/tobymao/sqlglot/pull/4830) by [@georgesittas](https://github.com/georgesittas))*
- [`adf2fef`](https://github.com/tobymao/sqlglot/commit/adf2fef27dc341508c3b9c710da0f835277094a1) - **mysql**: Support for USING BTREE/HASH in PK *(PR [#4837](https://github.com/tobymao/sqlglot/pull/4837) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4833](https://github.com/tobymao/sqlglot/issues/4833) opened by [@Gohoy](https://github.com/Gohoy)*
- [`8c0a6be`](https://github.com/tobymao/sqlglot/commit/8c0a6bec6e38f3f6ce9a90b6a9b6457de70c7228) - **mysql**: BLOB transpilation *(PR [#4844](https://github.com/tobymao/sqlglot/pull/4844) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4839](https://github.com/tobymao/sqlglot/issues/4839) opened by [@Gohoy](https://github.com/Gohoy)*
- [`0cb7a71`](https://github.com/tobymao/sqlglot/commit/0cb7a719de33ab1f6cfedf0833df7c79324b21f9) - **postgres**: Fix arrow extraction for string keys representing numbers *(PR [#4842](https://github.com/tobymao/sqlglot/pull/4842) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4840](https://github.com/tobymao/sqlglot/issues/4840) opened by [@superkashyap](https://github.com/superkashyap)*
- [`2e223cb`](https://github.com/tobymao/sqlglot/commit/2e223cb3e0bc946b8aa97e115e4c0dc02e58d1c9) - **parser**: properly parse qualified columns when parsing "columns ops" *(PR [#4847](https://github.com/tobymao/sqlglot/pull/4847) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4845](https://github.com/tobymao/sqlglot/issues/4845) opened by [@maudlel](https://github.com/maudlel)*

### :recycle: Refactors
- [`813d2ad`](https://github.com/tobymao/sqlglot/commit/813d2ada7afd653b2aaff75cbddd7f011750f861) - use _parse_table_parts for udf parsing *(PR [#4829](https://github.com/tobymao/sqlglot/pull/4829) by [@geooo109](https://github.com/geooo109))*

### :wrench: Chores
- [`e4fd354`](https://github.com/tobymao/sqlglot/commit/e4fd354c8fb55752cb883eb3912950c17020a1df) - Simplify Hive's STORED BY property *(PR [#4838](https://github.com/tobymao/sqlglot/pull/4838) by [@VaggelisD](https://github.com/VaggelisD))*
- [`8115b58`](https://github.com/tobymao/sqlglot/commit/8115b5853e621423eb2697b7253b17ef709dbdf0) - (duckdb): treat auto-increment DDL property as unsupported *(PR [#4849](https://github.com/tobymao/sqlglot/pull/4849) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4841](https://github.com/tobymao/sqlglot/issues/4841) opened by [@sk-](https://github.com/sk-)*
- [`b05dddb`](https://github.com/tobymao/sqlglot/commit/b05dddbe5a7d45dfebefc3e04cb95d8c4d9802e9) - fix pdoc deployment *(PR [#4856](https://github.com/tobymao/sqlglot/pull/4856) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4853](https://github.com/tobymao/sqlglot/issues/4853) opened by [@tsamaras](https://github.com/tsamaras)*


## [v26.8.0] - 2025-03-03
### :boom: BREAKING CHANGES
- due to [`596b66f`](https://github.com/tobymao/sqlglot/commit/596b66fc289140109db8f689c6e84264d643a47a) - add support for and/2 and or/2 functions *(PR [#4806](https://github.com/tobymao/sqlglot/pull/4806) by [@georgesittas](https://github.com/georgesittas))*:

  add support for and/2 and or/2 functions (#4806)

- due to [`eae860c`](https://github.com/tobymao/sqlglot/commit/eae860ce5b59b9e0b791fe79686899efb83df1dd) - expand DISTINCT ON expressions like we do for GROUP/ORDER by *(PR [#4807](https://github.com/tobymao/sqlglot/pull/4807) by [@georgesittas](https://github.com/georgesittas))*:

  expand DISTINCT ON expressions like we do for GROUP/ORDER by (#4807)

- due to [`83e6a87`](https://github.com/tobymao/sqlglot/commit/83e6a87f8d233eac6d3bcd3a49451a14dc10e06e) - Parse SHA256 *(PR [#4816](https://github.com/tobymao/sqlglot/pull/4816) by [@VaggelisD](https://github.com/VaggelisD))*:

  Parse SHA256 (#4816)


### :sparkles: New Features
- [`50539ce`](https://github.com/tobymao/sqlglot/commit/50539ced46de3949f6a70acdab86129fb50c9385) - **trino**: add support for ON ... ERROR/NULL syntax for JSON_QUERY *(PR [#4805](https://github.com/tobymao/sqlglot/pull/4805) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3905](https://github.com/TobikoData/sqlmesh/issues/3905) opened by [@darkcofy](https://github.com/darkcofy)*
- [`596b66f`](https://github.com/tobymao/sqlglot/commit/596b66fc289140109db8f689c6e84264d643a47a) - **clickhouse**: add support for and/2 and or/2 functions *(PR [#4806](https://github.com/tobymao/sqlglot/pull/4806) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4803](https://github.com/tobymao/sqlglot/issues/4803) opened by [@xtess16](https://github.com/xtess16)*
- [`c5bf122`](https://github.com/tobymao/sqlglot/commit/c5bf122a6aa7ca315ad726e6ea3d4a98eebd68d0) - **mysql**: support setting visibility on `ALTER COLUMN`. *(PR [#4809](https://github.com/tobymao/sqlglot/pull/4809) by [@burnison](https://github.com/burnison))*

### :bug: Bug Fixes
- [`6441d00`](https://github.com/tobymao/sqlglot/commit/6441d0041ccec7f1c28763f5775b6195d2049dc6) - orphan node(s) in eliminate_join_marks *(PR [#4808](https://github.com/tobymao/sqlglot/pull/4808) by [@snovik75](https://github.com/snovik75))*
- [`eae860c`](https://github.com/tobymao/sqlglot/commit/eae860ce5b59b9e0b791fe79686899efb83df1dd) - **optimizer**: expand DISTINCT ON expressions like we do for GROUP/ORDER by *(PR [#4807](https://github.com/tobymao/sqlglot/pull/4807) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4791](https://github.com/tobymao/sqlglot/issues/4791) opened by [@Fosly](https://github.com/Fosly)*
- [`5ef35f2`](https://github.com/tobymao/sqlglot/commit/5ef35f2dc622d96b013a2651c71e1a32933f51cb) - **clickhouse**: unparseable `AggregateFunction(count)` *(PR [#4812](https://github.com/tobymao/sqlglot/pull/4812) by [@pkit](https://github.com/pkit))*
- [`83e6a87`](https://github.com/tobymao/sqlglot/commit/83e6a87f8d233eac6d3bcd3a49451a14dc10e06e) - **duckdb**: Parse SHA256 *(PR [#4816](https://github.com/tobymao/sqlglot/pull/4816) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4815](https://github.com/tobymao/sqlglot/issues/4815) opened by [@muuuuwa](https://github.com/muuuuwa)*
- [`faf6d41`](https://github.com/tobymao/sqlglot/commit/faf6d416afe30bf0bc24649fcceccf79fbfb8ca1) - allow duplicate nodes in matchings *(PR [#4817](https://github.com/tobymao/sqlglot/pull/4817) by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`e34f3bc`](https://github.com/tobymao/sqlglot/commit/e34f3bc99f832ce2affb3a0297329f3d1cd7244e) - **optimizer**: refactor DISTINCT ON qualification to better match ORDER BY *(PR [#4811](https://github.com/tobymao/sqlglot/pull/4811) by [@georgesittas](https://github.com/georgesittas))*


## [v26.7.0] - 2025-02-26
### :boom: BREAKING CHANGES
- due to [`466c839`](https://github.com/tobymao/sqlglot/commit/466c839c2cfc94b398dd619b738df165f2876cdb) - Remove extra MAP bracket and ARRAY wrap *(PR [#4712](https://github.com/tobymao/sqlglot/pull/4712) by [@VaggelisD](https://github.com/VaggelisD))*:

  Remove extra MAP bracket and ARRAY wrap (#4712)

- due to [`79ab311`](https://github.com/tobymao/sqlglot/commit/79ab3116758c240786ab4353a26f1646e242a61b) - add generate_series table column alias *(PR [#4741](https://github.com/tobymao/sqlglot/pull/4741) by [@georgesittas](https://github.com/georgesittas))*:

  add generate_series table column alias (#4741)

- due to [`66b3ea9`](https://github.com/tobymao/sqlglot/commit/66b3ea905af34cfedc961c68ba738ba90d16221d) - respect type nullability when casting in strtodate_sql *(PR [#4744](https://github.com/tobymao/sqlglot/pull/4744) by [@sleshJdev](https://github.com/sleshJdev))*:

  respect type nullability when casting in strtodate_sql (#4744)

- due to [`91f47fe`](https://github.com/tobymao/sqlglot/commit/91f47fec2a8c727f7e4c93fa54b6f06a36a6b42f) - Support for exp.HexString in DuckDB/Presto/Trino *(PR [#4743](https://github.com/tobymao/sqlglot/pull/4743) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support for exp.HexString in DuckDB/Presto/Trino (#4743)

- due to [`0596176`](https://github.com/tobymao/sqlglot/commit/0596176dd59737f945624d6453259072917e2fee) - generate CASE expression instead of COUNT_IF for <v1.2 *(PR [#4755](https://github.com/tobymao/sqlglot/pull/4755) by [@georgesittas](https://github.com/georgesittas))*:

  generate CASE expression instead of COUNT_IF for <v1.2 (#4755)

- due to [`01008a9`](https://github.com/tobymao/sqlglot/commit/01008a91144a20e830df3783ff04beba7d029fec) - enable transpilation of CURDATE fixes [#4758](https://github.com/tobymao/sqlglot/pull/4758) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  enable transpilation of CURDATE fixes #4758

- due to [`dc69192`](https://github.com/tobymao/sqlglot/commit/dc691923655d022f2a4b23e9df24a9d1518e7048) - column qualify not applying in an order by inside within group when it also the alias *(PR [#4780](https://github.com/tobymao/sqlglot/pull/4780) by [@ran-lakeway](https://github.com/ran-lakeway))*:

  column qualify not applying in an order by inside within group when it also the alias (#4780)

- due to [`ae00c92`](https://github.com/tobymao/sqlglot/commit/ae00c9203197a436bdb81289580eba0e7fc374ec) - properly alias generate series *(PR [#4789](https://github.com/tobymao/sqlglot/pull/4789) by [@georgesittas](https://github.com/georgesittas))*:

  properly alias generate series (#4789)

- due to [`14c3de9`](https://github.com/tobymao/sqlglot/commit/14c3de91072435a68dd8d0f024dff0a0fd236cb8) - parse unqualified names in DISTINCT ON (...) as identifiers *(PR [#4795](https://github.com/tobymao/sqlglot/pull/4795) by [@georgesittas](https://github.com/georgesittas))*:

  parse unqualified names in DISTINCT ON (...) as identifiers (#4795)

- due to [`00c7f05`](https://github.com/tobymao/sqlglot/commit/00c7f05f348eeb6b60d4e70d7e29e564caf38f64) - do not coerce parameterized types *(PR [#4796](https://github.com/tobymao/sqlglot/pull/4796) by [@georgesittas](https://github.com/georgesittas))*:

  do not coerce parameterized types (#4796)

- due to [`513fad4`](https://github.com/tobymao/sqlglot/commit/513fad47cd321fe8431e673351bdd5f0674d5af1) - show databases, functions, procedures and warehouses  *(PR [#4787](https://github.com/tobymao/sqlglot/pull/4787) by [@tekumara](https://github.com/tekumara))*:

  show databases, functions, procedures and warehouses  (#4787)

- due to [`a9ae2d2`](https://github.com/tobymao/sqlglot/commit/a9ae2d229640f22134410ca5826c6f19df7ef523) - support TOP with PERCENT and WITH TIES *(PR [#4801](https://github.com/tobymao/sqlglot/pull/4801) by [@geooo109](https://github.com/geooo109))*:

  support TOP with PERCENT and WITH TIES (#4801)

- due to [`e05983a`](https://github.com/tobymao/sqlglot/commit/e05983a62ee9b3689e8a277924f2b1e36744a2a9) - make hex literal tokenization more robust *(PR [#4802](https://github.com/tobymao/sqlglot/pull/4802) by [@georgesittas](https://github.com/georgesittas))*:

  make hex literal tokenization more robust (#4802)

- due to [`e9c8eae`](https://github.com/tobymao/sqlglot/commit/e9c8eae060c05d02f6f65916d59f1792098679c8) - bump sqlglotrs to 0.4.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.4.0


### :sparkles: New Features
- [`5c59943`](https://github.com/tobymao/sqlglot/commit/5c599434fbe4d4f1059196b1530d02d7d204d3a9) - **mysql**: add unsigned double type *(PR [#4734](https://github.com/tobymao/sqlglot/pull/4734) by [@GabrielVSMachado](https://github.com/GabrielVSMachado))*
- [`31b2139`](https://github.com/tobymao/sqlglot/commit/31b21391cc521cf15a098c335d4202378ad96c0b) - **duckdb**: transpile JSONB into JSON *(PR [#4753](https://github.com/tobymao/sqlglot/pull/4753) by [@georgesittas](https://github.com/georgesittas))*
- [`0596176`](https://github.com/tobymao/sqlglot/commit/0596176dd59737f945624d6453259072917e2fee) - **duckdb**: generate CASE expression instead of COUNT_IF for <v1.2 *(PR [#4755](https://github.com/tobymao/sqlglot/pull/4755) by [@georgesittas](https://github.com/georgesittas))*
- [`2b3ec0f`](https://github.com/tobymao/sqlglot/commit/2b3ec0fbd6bd84dd893c244892fc1a63e2eab7bd) - **snowflake**: add support for USE SECONDARY ROLES *(PR [#4761](https://github.com/tobymao/sqlglot/pull/4761) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4760](https://github.com/tobymao/sqlglot/issues/4760) opened by [@merlindso](https://github.com/merlindso)*
- [`2079bdb`](https://github.com/tobymao/sqlglot/commit/2079bdbb5d72d7fe02019a9ba6d1684403e30b00) - **postgres**: Add support for WITH RECURSIVE search options *(PR [#4773](https://github.com/tobymao/sqlglot/pull/4773) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4763](https://github.com/tobymao/sqlglot/issues/4763) opened by [@psanjeev-cellino](https://github.com/psanjeev-cellino)*
- [`70eefc6`](https://github.com/tobymao/sqlglot/commit/70eefc694c20600cdd99d682393cae81812d0b00) - **duckdb**: add support for underscored numbers fixes [#4777](https://github.com/tobymao/sqlglot/pull/4777) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`f6be1d4`](https://github.com/tobymao/sqlglot/commit/f6be1d4b2c34ea4e3e98c3ae5e272e2f2b1912f3) - **clickhouse**: add support for dynamic json casting *(PR [#4784](https://github.com/tobymao/sqlglot/pull/4784) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4776](https://github.com/tobymao/sqlglot/issues/4776) opened by [@pkit](https://github.com/pkit)*
- [`513fad4`](https://github.com/tobymao/sqlglot/commit/513fad47cd321fe8431e673351bdd5f0674d5af1) - **snowflake**: show databases, functions, procedures and warehouses  *(PR [#4787](https://github.com/tobymao/sqlglot/pull/4787) by [@tekumara](https://github.com/tekumara))*
- [`575c979`](https://github.com/tobymao/sqlglot/commit/575c979b77831bd356cc7e9b39e78fcca2b344be) - **clickhouse**: support `GLOBAL NOT IN` *(PR [#4798](https://github.com/tobymao/sqlglot/pull/4798) by [@hdhoang](https://github.com/hdhoang))*
- [`856faa9`](https://github.com/tobymao/sqlglot/commit/856faa9773dea3b214571932c7d65e5773c5dc76) - **mysql**: add support for `ALTER INDEX` *(PR [#4800](https://github.com/tobymao/sqlglot/pull/4800) by [@burnison](https://github.com/burnison))*

### :bug: Bug Fixes
- [`7cad7f0`](https://github.com/tobymao/sqlglot/commit/7cad7f06d676bf230238bf08aea91701e1fa1d95) - Add additional allowed tokens for parsing aggregate functions in DDL *(PR [#4727](https://github.com/tobymao/sqlglot/pull/4727) by [@dorranh](https://github.com/dorranh))*
  - :arrow_lower_right: *fixes issue [#4723](https://github.com/tobymao/sqlglot/issues/4723) opened by [@dorranh](https://github.com/dorranh)*
- [`466c839`](https://github.com/tobymao/sqlglot/commit/466c839c2cfc94b398dd619b738df165f2876cdb) - **duckdb**: Remove extra MAP bracket and ARRAY wrap *(PR [#4712](https://github.com/tobymao/sqlglot/pull/4712) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4710](https://github.com/tobymao/sqlglot/issues/4710) opened by [@eakmanrq](https://github.com/eakmanrq)*
- [`2c7dbe2`](https://github.com/tobymao/sqlglot/commit/2c7dbe2f84fcba044881776ebdeed6ee980fe0dd) - **duckdb**: Fix INTERVAL roundtrip of DATE_ADD *(PR [#4732](https://github.com/tobymao/sqlglot/pull/4732) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4728](https://github.com/tobymao/sqlglot/issues/4728) opened by [@whao89](https://github.com/whao89)*
- [`cbad48d`](https://github.com/tobymao/sqlglot/commit/cbad48d4102412ea3fd20272cf75974a8c53bb34) - infinite traversal in eliminate_qualify closes [#4735](https://github.com/tobymao/sqlglot/pull/4735) *(commit by [@tobymao](https://github.com/tobymao))*
- [`79ab311`](https://github.com/tobymao/sqlglot/commit/79ab3116758c240786ab4353a26f1646e242a61b) - **clickhouse**: add generate_series table column alias *(PR [#4741](https://github.com/tobymao/sqlglot/pull/4741) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4740](https://github.com/tobymao/sqlglot/issues/4740) opened by [@martijnthe](https://github.com/martijnthe)*
- [`f0ce5ce`](https://github.com/tobymao/sqlglot/commit/f0ce5ce80d5a0ac24da4295a2f660bad9d7934be) - **snowflake**: unqualify ANY ORDER BY columns *(PR [#4737](https://github.com/tobymao/sqlglot/pull/4737) by [@georgesittas](https://github.com/georgesittas))*
- [`66b3ea9`](https://github.com/tobymao/sqlglot/commit/66b3ea905af34cfedc961c68ba738ba90d16221d) - **clickhouse**: respect type nullability when casting in strtodate_sql *(PR [#4744](https://github.com/tobymao/sqlglot/pull/4744) by [@sleshJdev](https://github.com/sleshJdev))*
- [`fda40a9`](https://github.com/tobymao/sqlglot/commit/fda40a9e670f30822dee05651d2336494c0d4f3b) - **clickhouse**: add BOTH if trim type is unspecified fixes [#4746](https://github.com/tobymao/sqlglot/pull/4746) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`91f47fe`](https://github.com/tobymao/sqlglot/commit/91f47fec2a8c727f7e4c93fa54b6f06a36a6b42f) - Support for exp.HexString in DuckDB/Presto/Trino *(PR [#4743](https://github.com/tobymao/sqlglot/pull/4743) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4739](https://github.com/tobymao/sqlglot/issues/4739) opened by [@sk-](https://github.com/sk-)*
- [`f94656d`](https://github.com/tobymao/sqlglot/commit/f94656d06b118cee69ec8305fde948caa250cb69) - **clickhouse**: Generation of exp.ArrayConcat *(PR [#4754](https://github.com/tobymao/sqlglot/pull/4754) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4751](https://github.com/tobymao/sqlglot/issues/4751) opened by [@maximlipiev](https://github.com/maximlipiev)*
- [`cf33711`](https://github.com/tobymao/sqlglot/commit/cf337114a2259058b2a8cbde22d0a560001df9a5) - **tokenizer-rs**: use is_whitespace in token scan loop *(PR [#4756](https://github.com/tobymao/sqlglot/pull/4756) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4748](https://github.com/tobymao/sqlglot/issues/4748) opened by [@gesoges0](https://github.com/gesoges0)*
- [`01008a9`](https://github.com/tobymao/sqlglot/commit/01008a91144a20e830df3783ff04beba7d029fec) - **mysql**: enable transpilation of CURDATE fixes [#4758](https://github.com/tobymao/sqlglot/pull/4758) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1fbd108`](https://github.com/tobymao/sqlglot/commit/1fbd108e8e9b85ae873bb702ff8f73c5ae94b392) - **sqlite**: correct json key value pair separator *(PR [#4774](https://github.com/tobymao/sqlglot/pull/4774) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4770](https://github.com/tobymao/sqlglot/issues/4770) opened by [@ori-n](https://github.com/ori-n)*
- [`ccdb92a`](https://github.com/tobymao/sqlglot/commit/ccdb92a7b676d3454cf9f6a18a2b0becec676169) - **optimizer**: do not qualify ctes that are pivoted *(PR [#4775](https://github.com/tobymao/sqlglot/pull/4775) by [@georgesittas](https://github.com/georgesittas))*
- [`ec4e97c`](https://github.com/tobymao/sqlglot/commit/ec4e97c31c16a1c8b5eb1acfba0ef77f064505b4) - **parser**: Do not preemptively create chunks for comment only statements *(PR [#4772](https://github.com/tobymao/sqlglot/pull/4772) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4762](https://github.com/tobymao/sqlglot/issues/4762) opened by [@dbittenbender](https://github.com/dbittenbender)*
- [`fc8aec3`](https://github.com/tobymao/sqlglot/commit/fc8aec35b211395ca603b2feabd1e5a5006fd677) - **parser**: Revert [#4772](https://github.com/tobymao/sqlglot/pull/4772) *(PR [#4783](https://github.com/tobymao/sqlglot/pull/4783) by [@VaggelisD](https://github.com/VaggelisD))*
- [`dc69192`](https://github.com/tobymao/sqlglot/commit/dc691923655d022f2a4b23e9df24a9d1518e7048) - **optimizer**: column qualify not applying in an order by inside within group when it also the alias *(PR [#4780](https://github.com/tobymao/sqlglot/pull/4780) by [@ran-lakeway](https://github.com/ran-lakeway))*
- [`ae00c92`](https://github.com/tobymao/sqlglot/commit/ae00c9203197a436bdb81289580eba0e7fc374ec) - **postgres**: properly alias generate series *(PR [#4789](https://github.com/tobymao/sqlglot/pull/4789) by [@georgesittas](https://github.com/georgesittas))*
- [`14c3de9`](https://github.com/tobymao/sqlglot/commit/14c3de91072435a68dd8d0f024dff0a0fd236cb8) - **optimizer**: parse unqualified names in DISTINCT ON (...) as identifiers *(PR [#4795](https://github.com/tobymao/sqlglot/pull/4795) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4791](https://github.com/tobymao/sqlglot/issues/4791) opened by [@Fosly](https://github.com/Fosly)*
- [`00c7f05`](https://github.com/tobymao/sqlglot/commit/00c7f05f348eeb6b60d4e70d7e29e564caf38f64) - **optimizer**: do not coerce parameterized types *(PR [#4796](https://github.com/tobymao/sqlglot/pull/4796) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4792](https://github.com/tobymao/sqlglot/issues/4792) opened by [@georgesittas](https://github.com/georgesittas)*
- [`9f6b4f7`](https://github.com/tobymao/sqlglot/commit/9f6b4f7dd6ea8eb80d8d5d3b75eeff9e262ceba2) - **snowflake**: add missing clauses in SHOW statements *(PR [#4797](https://github.com/tobymao/sqlglot/pull/4797) by [@georgesittas](https://github.com/georgesittas))*
- [`a9ae2d2`](https://github.com/tobymao/sqlglot/commit/a9ae2d229640f22134410ca5826c6f19df7ef523) - **tsql**: support TOP with PERCENT and WITH TIES *(PR [#4801](https://github.com/tobymao/sqlglot/pull/4801) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4793](https://github.com/tobymao/sqlglot/issues/4793) opened by [@gleb-daax](https://github.com/gleb-daax)*
- [`e05983a`](https://github.com/tobymao/sqlglot/commit/e05983a62ee9b3689e8a277924f2b1e36744a2a9) - **tokenizer-rs**: make hex literal tokenization more robust *(PR [#4802](https://github.com/tobymao/sqlglot/pull/4802) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`6c4c69e`](https://github.com/tobymao/sqlglot/commit/6c4c69e95d504ec839f7abd571eb2f614ea3c9ca) - move t-sql generators to base, clean up set_sql *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`4bf897d`](https://github.com/tobymao/sqlglot/commit/4bf897da6332175c22b772beb0b0cb6e6e941ebb) - remove redundant "parts" arg from CH ast, move gen methods to base *(PR [#4779](https://github.com/tobymao/sqlglot/pull/4779) by [@georgesittas](https://github.com/georgesittas))*
- [`e9c8eae`](https://github.com/tobymao/sqlglot/commit/e9c8eae060c05d02f6f65916d59f1792098679c8) - bump sqlglotrs to 0.4.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.6.0] - 2025-02-10
### :boom: BREAKING CHANGES
- due to [`a790e41`](https://github.com/tobymao/sqlglot/commit/a790e41736884bde7a8172f458db92d80064556f) - avoid redundant casts in FROM/TO_UTC_TIMESTAMP *(PR [#4725](https://github.com/tobymao/sqlglot/pull/4725) by [@georgesittas](https://github.com/georgesittas))*:

  avoid redundant casts in FROM/TO_UTC_TIMESTAMP (#4725)


### :bug: Bug Fixes
- [`a790e41`](https://github.com/tobymao/sqlglot/commit/a790e41736884bde7a8172f458db92d80064556f) - **spark**: avoid redundant casts in FROM/TO_UTC_TIMESTAMP *(PR [#4725](https://github.com/tobymao/sqlglot/pull/4725) by [@georgesittas](https://github.com/georgesittas))*


## [v26.5.0] - 2025-02-10
### :boom: BREAKING CHANGES
- due to [`da52181`](https://github.com/tobymao/sqlglot/commit/da52181f1cd3ec22e5ac597de50036278d2e66e5) - TO_DATE parsing with safe flag true *(PR [#4713](https://github.com/tobymao/sqlglot/pull/4713) by [@geooo109](https://github.com/geooo109))*:

  TO_DATE parsing with safe flag true (#4713)

- due to [`b12aba9`](https://github.com/tobymao/sqlglot/commit/b12aba9be6043053f79ff50f7bdcdfdff19ddf52) - Improve UUID support *(PR [#4718](https://github.com/tobymao/sqlglot/pull/4718) by [@amachanic](https://github.com/amachanic))*:

  Improve UUID support (#4718)

- due to [`27ec74b`](https://github.com/tobymao/sqlglot/commit/27ec74bab67afba930c4ea66130bcba5e9bb5ba1) - Properly set 'this' when parsing IDENTITY *(PR [#4719](https://github.com/tobymao/sqlglot/pull/4719) by [@amachanic](https://github.com/amachanic))*:

  Properly set 'this' when parsing IDENTITY (#4719)


### :sparkles: New Features
- [`c31947b`](https://github.com/tobymao/sqlglot/commit/c31947b2386f579d9d12d2d4053461a75855b9be) - **postgres**: Support generation of exp.CountIf *(PR [#4709](https://github.com/tobymao/sqlglot/pull/4709) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`b842d93`](https://github.com/tobymao/sqlglot/commit/b842d9383827d18482e36d6ea3041180a74d0abf) - **postgres**: enable qualification of queries using the ROWS FROM syntax *(PR [#4699](https://github.com/tobymao/sqlglot/pull/4699) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3777](https://github.com/TobikoData/sqlmesh/issues/3777) opened by [@simon-pactum](https://github.com/simon-pactum)*
- [`5f90307`](https://github.com/tobymao/sqlglot/commit/5f9030786f8489e70aee70acabee440ecf23699c) - **duckdb**: enable support for user-defined types *(PR [#4702](https://github.com/tobymao/sqlglot/pull/4702) by [@georgesittas](https://github.com/georgesittas))*
- [`23283ca`](https://github.com/tobymao/sqlglot/commit/23283cacda3c4d6e4f6453cdef1a9e73e3bc8d24) - avoid concealing dialect module exception in _try_load *(PR [#4708](https://github.com/tobymao/sqlglot/pull/4708) by [@georgesittas](https://github.com/georgesittas))*
- [`707d45e`](https://github.com/tobymao/sqlglot/commit/707d45ecc7e233f57ada8d6dfaf6c621d6ee3f51) - **tsql**: support default values on definitons and the OUTPUT/OUT/READ_ONLY syntax *(PR [#4704](https://github.com/tobymao/sqlglot/pull/4704) by [@geooo109](https://github.com/geooo109))*
- [`da52181`](https://github.com/tobymao/sqlglot/commit/da52181f1cd3ec22e5ac597de50036278d2e66e5) - **hive**: TO_DATE parsing with safe flag true *(PR [#4713](https://github.com/tobymao/sqlglot/pull/4713) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4707](https://github.com/tobymao/sqlglot/issues/4707) opened by [@jiangli001](https://github.com/jiangli001)*
- [`b12aba9`](https://github.com/tobymao/sqlglot/commit/b12aba9be6043053f79ff50f7bdcdfdff19ddf52) - **tsql, postgres**: Improve UUID support *(PR [#4718](https://github.com/tobymao/sqlglot/pull/4718) by [@amachanic](https://github.com/amachanic))*
- [`27ec74b`](https://github.com/tobymao/sqlglot/commit/27ec74bab67afba930c4ea66130bcba5e9bb5ba1) - **tsql**: Properly set 'this' when parsing IDENTITY *(PR [#4719](https://github.com/tobymao/sqlglot/pull/4719) by [@amachanic](https://github.com/amachanic))*
- [`f7e22d4`](https://github.com/tobymao/sqlglot/commit/f7e22d40cddfdee4a3d4912aef3161546528d400) - don't change query if no join marks in eliminate_join_marks, fixes [#4721](https://github.com/tobymao/sqlglot/pull/4721) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b918ff1`](https://github.com/tobymao/sqlglot/commit/b918ff1bc4e256bd1b84802327ffe4acf36d2d45) - **bigquery**: type-annotated array literal logic edge case *(PR [#4724](https://github.com/tobymao/sqlglot/pull/4724) by [@georgesittas](https://github.com/georgesittas))*


## [v26.4.1] - 2025-02-03
### :bug: Bug Fixes
- [`dd1cdb0`](https://github.com/tobymao/sqlglot/commit/dd1cdb0b91ac597a9cb1f1f517a616c264f5b654) - **redshift**: generate proper syntax for column type alteration *(PR [#4698](https://github.com/tobymao/sqlglot/pull/4698) by [@georgesittas](https://github.com/georgesittas))*


## [v26.4.0] - 2025-02-03
### :boom: BREAKING CHANGES
- due to [`48145a3`](https://github.com/tobymao/sqlglot/commit/48145a399b076bd3189af8ed8187ca45767d018d) - CurrentSchema for SQLite, MySQL, Postgres, and TSQL *(PR [#4658](https://github.com/tobymao/sqlglot/pull/4658) by [@pruzko](https://github.com/pruzko))*:

  CurrentSchema for SQLite, MySQL, Postgres, and TSQL (#4658)

- due to [`1a91913`](https://github.com/tobymao/sqlglot/commit/1a91913eea97e2008a0fe4282d60d7c693a79fc3) - Parse empty bracketed ARRAY with cast *(PR [#4679](https://github.com/tobymao/sqlglot/pull/4679) by [@VaggelisD](https://github.com/VaggelisD))*:

  Parse empty bracketed ARRAY with cast (#4679)

- due to [`f6482fb`](https://github.com/tobymao/sqlglot/commit/f6482fbb782b13a0f180f67b8b5eb3149eba0251) - transpile postgres DATE_BIN function to duckdb TIME_BUCKET *(PR [#4681](https://github.com/tobymao/sqlglot/pull/4681) by [@dor-bernstein](https://github.com/dor-bernstein))*:

  transpile postgres DATE_BIN function to duckdb TIME_BUCKET (#4681)

- due to [`ade8b82`](https://github.com/tobymao/sqlglot/commit/ade8b826541ecfb00e218d16d995d34adab0335a) - load dialects lazily *(PR [#4687](https://github.com/tobymao/sqlglot/pull/4687) by [@georgesittas](https://github.com/georgesittas))*:

  load dialects lazily (#4687)


### :sparkles: New Features
- [`48145a3`](https://github.com/tobymao/sqlglot/commit/48145a399b076bd3189af8ed8187ca45767d018d) - CurrentSchema for SQLite, MySQL, Postgres, and TSQL *(PR [#4658](https://github.com/tobymao/sqlglot/pull/4658) by [@pruzko](https://github.com/pruzko))*
  - :arrow_lower_right: *addresses issue [#4655](https://github.com/tobymao/sqlglot/issues/4655) opened by [@pruzko](https://github.com/pruzko)*
- [`f6482fb`](https://github.com/tobymao/sqlglot/commit/f6482fbb782b13a0f180f67b8b5eb3149eba0251) - transpile postgres DATE_BIN function to duckdb TIME_BUCKET *(PR [#4681](https://github.com/tobymao/sqlglot/pull/4681) by [@dor-bernstein](https://github.com/dor-bernstein))*
- [`b2a6041`](https://github.com/tobymao/sqlglot/commit/b2a6041ace9a97fab947364b22d5ddf0e842e278) - **oracle**: add support for CAST(... DEFAULT <value> ON CONVERSION FAILURE) *(PR [#4683](https://github.com/tobymao/sqlglot/pull/4683) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4682](https://github.com/tobymao/sqlglot/issues/4682) opened by [@jaredschwartz-ofs](https://github.com/jaredschwartz-ofs)*
- [`1ad656e`](https://github.com/tobymao/sqlglot/commit/1ad656e4572bf7b3d38805e92f7202f4dcc4f9f8) - enable parsing of (u)int128,256 types for all dialects *(PR [#4685](https://github.com/tobymao/sqlglot/pull/4685) by [@georgesittas](https://github.com/georgesittas))*
- [`6f5fb04`](https://github.com/tobymao/sqlglot/commit/6f5fb0423e970920fa5abda3f7e4356e2fb441e1) - implement Dune dialect *(PR [#4686](https://github.com/tobymao/sqlglot/pull/4686) by [@georgesittas](https://github.com/georgesittas))*
- [`9ea15c7`](https://github.com/tobymao/sqlglot/commit/9ea15c732d76e0d6a393e553a42e6b9ed30ef286) - **bigquery**: add EXPORT DATA statement support *(PR [#4688](https://github.com/tobymao/sqlglot/pull/4688) by [@ArnoldHueteG](https://github.com/ArnoldHueteG))*

### :bug: Bug Fixes
- [`cd53f7e`](https://github.com/tobymao/sqlglot/commit/cd53f7ec03e99129b430c435d23907ef7d0e0c34) - **clickhouse**: Generate bracket notation for exp.VarMap *(PR [#4664](https://github.com/tobymao/sqlglot/pull/4664) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4662](https://github.com/tobymao/sqlglot/issues/4662) opened by [@martijnthe](https://github.com/martijnthe)*
- [`0920f77`](https://github.com/tobymao/sqlglot/commit/0920f778b2d94d94f3c8cccf280a87a6a14b12f7) - use utf-8 encoding in open calls, fixes [#4676](https://github.com/tobymao/sqlglot/pull/4676) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`e71c4c0`](https://github.com/tobymao/sqlglot/commit/e71c4c0b60811f26828d7719fe941dfbc3693be1) - **trino**: Add more JSON_QUERY options *(PR [#4673](https://github.com/tobymao/sqlglot/pull/4673) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4672](https://github.com/tobymao/sqlglot/issues/4672) opened by [@JustGui](https://github.com/JustGui)*
- [`1a91913`](https://github.com/tobymao/sqlglot/commit/1a91913eea97e2008a0fe4282d60d7c693a79fc3) - **postgres**: Parse empty bracketed ARRAY with cast *(PR [#4679](https://github.com/tobymao/sqlglot/pull/4679) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4674](https://github.com/tobymao/sqlglot/issues/4674) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`c45f174`](https://github.com/tobymao/sqlglot/commit/c45f17455477790f53ef7e347a7e85cfdb82c4ab) - **bigquery**: Inline type-annotated ARRAY literals *(PR [#4671](https://github.com/tobymao/sqlglot/pull/4671) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4670](https://github.com/tobymao/sqlglot/issues/4670) opened by [@sean-rose](https://github.com/sean-rose)*
- [`df75edd`](https://github.com/tobymao/sqlglot/commit/df75eddf698af9fe36e7121a63cc2b9fdd468363) - **duckdb**: support postgres JSON/JSONB_OBJECT_AGG to duckdb JSON_GROUP_OBJECT *(PR [#4677](https://github.com/tobymao/sqlglot/pull/4677) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4667](https://github.com/tobymao/sqlglot/issues/4667) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`69680c1`](https://github.com/tobymao/sqlglot/commit/69680c146f67175ab6e4c4d9898b0991033a4188) - **tsql**: Transpile exp.Fetch limits *(PR [#4680](https://github.com/tobymao/sqlglot/pull/4680) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4665](https://github.com/tobymao/sqlglot/issues/4665) opened by [@WillAyd](https://github.com/WillAyd)*
- [`b3b0962`](https://github.com/tobymao/sqlglot/commit/b3b09624cdefb1baa46ddbb888b24648f330a963) - **hive**: Simplify DATE_FORMAT roundtrip *(PR [#4689](https://github.com/tobymao/sqlglot/pull/4689) by [@VaggelisD](https://github.com/VaggelisD))*
- [`ade8b82`](https://github.com/tobymao/sqlglot/commit/ade8b826541ecfb00e218d16d995d34adab0335a) - load dialects lazily *(PR [#4687](https://github.com/tobymao/sqlglot/pull/4687) by [@georgesittas](https://github.com/georgesittas))*
- [`47c0236`](https://github.com/tobymao/sqlglot/commit/47c023650dad8b0091248c608a211018b841042a) - **bigquery**: Refactor EXPORT DATA statement *(PR [#4693](https://github.com/tobymao/sqlglot/pull/4693) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1904b76`](https://github.com/tobymao/sqlglot/commit/1904b7605a7308608ac64e5cfb3c8424d3e55c17) - **tsql**: remove BEGIN from identifiers *(PR [#4695](https://github.com/tobymao/sqlglot/pull/4695) by [@geooo109](https://github.com/geooo109))*
- [`a688b6c`](https://github.com/tobymao/sqlglot/commit/a688b6cff01b9cd828c0467b0aa09fba728d751a) - **snowflake**: support correct AUTO INCREMENT transpilation *(PR [#4696](https://github.com/tobymao/sqlglot/pull/4696) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4694](https://github.com/tobymao/sqlglot/issues/4694) opened by [@sfc-gh-tdwojak](https://github.com/sfc-gh-tdwojak)*


## [v26.3.9] - 2025-01-27
### :bug: Bug Fixes
- [`b091f2f`](https://github.com/tobymao/sqlglot/commit/b091f2f4e4779fb9a4187d1665ca40e1648d9ccb) - **trino**: Correctly render exp.LocationProperty in CREATE TABLE / CREATE SCHEMA *(PR [#4659](https://github.com/tobymao/sqlglot/pull/4659) by [@erindru](https://github.com/erindru))*
- [`c4de945`](https://github.com/tobymao/sqlglot/commit/c4de94538cd69540f772b9b13e968ee16ffbbe67) - **Trino**: Prevent first_value and last_value from being converted *(PR [#4661](https://github.com/tobymao/sqlglot/pull/4661) by [@MikeWallis42](https://github.com/MikeWallis42))*
  - :arrow_lower_right: *fixes issue [#4660](https://github.com/tobymao/sqlglot/issues/4660) opened by [@MikeWallis42](https://github.com/MikeWallis42)*

### :wrench: Chores
- [`bae0489`](https://github.com/tobymao/sqlglot/commit/bae0489044a1368556f03f637c171a1873b6f05c) - reduce sdist size *(commit by [@tobymao](https://github.com/tobymao))*


## [v26.3.8] - 2025-01-24
### :wrench: Chores
- [`5f54f16`](https://github.com/tobymao/sqlglot/commit/5f54f168ee75c5a344747a035e63e1df70fe652c) - bump sqlglotrs to 0.3.14 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.3.7] - 2025-01-24
### :wrench: Chores
- [`14ad1a0`](https://github.com/tobymao/sqlglot/commit/14ad1a04e86fea5ea88f99948e4cc283692e72a2) - bump sqlglotrs to 0.3.13 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.3.6] - 2025-01-24
### :wrench: Chores
- [`085fef6`](https://github.com/tobymao/sqlglot/commit/085fef6971a4ebd43b5c7013c6bbcb0d00dfdc30) - bump sqlglotrs to 0.3.12 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.3.5] - 2025-01-24
### :wrench: Chores
- [`acb7217`](https://github.com/tobymao/sqlglot/commit/acb7217d89e12de549663b67af4687a08512993f) - bump sqlglotrs to 0.3.11 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.3.4] - 2025-01-24
### :wrench: Chores
- [`bb7548d`](https://github.com/tobymao/sqlglot/commit/bb7548d1e9f371d3ce931fcbd86c65c895f159d1) - bump sqlglotrs to 0.3.10 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.3.3] - 2025-01-23
### :wrench: Chores
- [`3a188ef`](https://github.com/tobymao/sqlglot/commit/3a188ef0d42a6313625b25003c27195156e7e753) - fix sqlglotrs deployment job *(PR [#4657](https://github.com/tobymao/sqlglot/pull/4657) by [@georgesittas](https://github.com/georgesittas))*
- [`7e55533`](https://github.com/tobymao/sqlglot/commit/7e55533d9bb06783803f275415640217c89085d0) - bump sqlglotrs to 0.3.9 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.3.2] - 2025-01-23
### :wrench: Chores
- [`28f56cb`](https://github.com/tobymao/sqlglot/commit/28f56cb7d9805ce898e7bf6bb884cccb1bd32c52) - fix sqlglotrs deployment job *(PR [#4656](https://github.com/tobymao/sqlglot/pull/4656) by [@georgesittas](https://github.com/georgesittas))*
- [`846b141`](https://github.com/tobymao/sqlglot/commit/846b1414183e3d193b4aacc82f3861378adb9ec9) - bump sqlglotrs to 0.3.8 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.3.1] - 2025-01-23
### :wrench: Chores
- [`ff9ea0c`](https://github.com/tobymao/sqlglot/commit/ff9ea0c4554ef0fa46b3460d01374d4a3f9c36ff) - change upload-artifact to v4 *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`61c4784`](https://github.com/tobymao/sqlglot/commit/61c4784033940e34e91732e2464e4baba77e6b7c) - bump sqlglotrs to 0.3.7 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.3.0] - 2025-01-23
### :boom: BREAKING CHANGES
- due to [`8b465d4`](https://github.com/tobymao/sqlglot/commit/8b465d498e0aa9feee53306f631e258443ee3060) - expand single VALUES clause in CTE into a SELECT * *(PR [#4617](https://github.com/tobymao/sqlglot/pull/4617) by [@georgesittas](https://github.com/georgesittas))*:

  expand single VALUES clause in CTE into a SELECT * (#4617)

- due to [`59d886d`](https://github.com/tobymao/sqlglot/commit/59d886d6abfc00726b785a4d468f6b2e0f9d3b1a) - treat LEVEL column in CONNECT BY queries as an identifier *(PR [#4627](https://github.com/tobymao/sqlglot/pull/4627) by [@georgesittas](https://github.com/georgesittas))*:

  treat LEVEL column in CONNECT BY queries as an identifier (#4627)

- due to [`9db09ff`](https://github.com/tobymao/sqlglot/commit/9db09ff91931802c675a219951f28afee1d4019d) - support more compact SAFE_DIVIDE transpilation [#4634](https://github.com/tobymao/sqlglot/pull/4634) *(PR [#4641](https://github.com/tobymao/sqlglot/pull/4641) by [@geooo109](https://github.com/geooo109))*:

  support more compact SAFE_DIVIDE transpilation #4634 (#4641)

- due to [`94af80b`](https://github.com/tobymao/sqlglot/commit/94af80b8bc3c44aa9770d6503f4e07ad4e37e314) - Do not remove parens on bracketed expressions *(PR [#4645](https://github.com/tobymao/sqlglot/pull/4645) by [@VaggelisD](https://github.com/VaggelisD))*:

  Do not remove parens on bracketed expressions (#4645)

- due to [`35923e9`](https://github.com/tobymao/sqlglot/commit/35923e959ff934093a7b82c58f13c5a89a768f5e) - POSITION and all their variants for all dialects *(PR [#4606](https://github.com/tobymao/sqlglot/pull/4606) by [@pruzko](https://github.com/pruzko))*:

  POSITION and all their variants for all dialects (#4606)


### :sparkles: New Features
- [`e47a7c9`](https://github.com/tobymao/sqlglot/commit/e47a7c943b0beef37e30cd7c71ea98c27b82c11b) - Fix Oracle Integer Type Mapping *(PR [#4616](https://github.com/tobymao/sqlglot/pull/4616) by [@pruzko](https://github.com/pruzko))*
- [`d8ade83`](https://github.com/tobymao/sqlglot/commit/d8ade830bbca4d2893a7e406868a0bd3a654057e) - **clickhouse**: Dynamic data type *(PR [#4624](https://github.com/tobymao/sqlglot/pull/4624) by [@pkit](https://github.com/pkit))*
- [`f7628ad`](https://github.com/tobymao/sqlglot/commit/f7628adf12e03a09ec89fe883d5b710a0f7e0151) - **optimizer**: Fix qualify for SEMI/ANTI joins *(PR [#4622](https://github.com/tobymao/sqlglot/pull/4622) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3557](https://github.com/TobikoData/sqlmesh/issues/3557) opened by [@Bilbottom](https://github.com/Bilbottom)*
- [`a20b663`](https://github.com/tobymao/sqlglot/commit/a20b663964a9845d3eb3c43def5880a531dab4a4) - improve rs tokenizer performance *(PR [#4638](https://github.com/tobymao/sqlglot/pull/4638) by [@benfdking](https://github.com/benfdking))*
- [`ffa0df7`](https://github.com/tobymao/sqlglot/commit/ffa0df72e36c6a08f1fc707d9c83e98eccc214c1) - **parser**: Support Oracle/Postgres XMLNAMESPACES in XMLTABLE *(PR [#4643](https://github.com/tobymao/sqlglot/pull/4643) by [@rbreejen](https://github.com/rbreejen))*
  - :arrow_lower_right: *addresses issue [#4642](https://github.com/tobymao/sqlglot/issues/4642) opened by [@rbreejen](https://github.com/rbreejen)*
- [`35923e9`](https://github.com/tobymao/sqlglot/commit/35923e959ff934093a7b82c58f13c5a89a768f5e) - POSITION and all their variants for all dialects *(PR [#4606](https://github.com/tobymao/sqlglot/pull/4606) by [@pruzko](https://github.com/pruzko))*

### :bug: Bug Fixes
- [`14474ee`](https://github.com/tobymao/sqlglot/commit/14474ee689025cc67b1f9a07e51d2f414ec5ab49) - **tsql**: support TSQL PRIMARY KEY constraint with DESC, ASC *(PR [#4618](https://github.com/tobymao/sqlglot/pull/4618) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4610](https://github.com/tobymao/sqlglot/issues/4610) opened by [@cchambers-rdi](https://github.com/cchambers-rdi)*
- [`8b465d4`](https://github.com/tobymao/sqlglot/commit/8b465d498e0aa9feee53306f631e258443ee3060) - **parser**: expand single VALUES clause in CTE into a SELECT * *(PR [#4617](https://github.com/tobymao/sqlglot/pull/4617) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3556](https://github.com/TobikoData/sqlmesh/issues/3556) opened by [@Bilbottom](https://github.com/Bilbottom)*
- [`647d986`](https://github.com/tobymao/sqlglot/commit/647d98650a3d6ba6aa7d57560555832548dd89aa) - **snowflake**: get rid of incorrect time mappings *(PR [#4629](https://github.com/tobymao/sqlglot/pull/4629) by [@georgesittas](https://github.com/georgesittas))*
- [`9cbd5ef`](https://github.com/tobymao/sqlglot/commit/9cbd5ef798d1f34d4eebe501cead8295564fc15c) - **trino**: generate ArrayUniqueAgg as ARRAY_AGG(DISTINCT ...) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`59d886d`](https://github.com/tobymao/sqlglot/commit/59d886d6abfc00726b785a4d468f6b2e0f9d3b1a) - **optimizer**: treat LEVEL column in CONNECT BY queries as an identifier *(PR [#4627](https://github.com/tobymao/sqlglot/pull/4627) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4620](https://github.com/tobymao/sqlglot/issues/4620) opened by [@snovik75](https://github.com/snovik75)*
- [`6107661`](https://github.com/tobymao/sqlglot/commit/6107661424622651447da09fb9d7e456ff453bff) - **snowflake**: Allow parsing of TO_TIME *(PR [#4631](https://github.com/tobymao/sqlglot/pull/4631) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4625](https://github.com/tobymao/sqlglot/issues/4625) opened by [@aletheavilla](https://github.com/aletheavilla)*
- [`9fdfd4d`](https://github.com/tobymao/sqlglot/commit/9fdfd4d6824702f019223536ba4013a966170ff6) - **trino**: support QUOTES option for JSON_QUERY [#4623](https://github.com/tobymao/sqlglot/pull/4623) *(PR [#4628](https://github.com/tobymao/sqlglot/pull/4628) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4623](https://github.com/tobymao/sqlglot/issues/4623) opened by [@betodealmeida](https://github.com/betodealmeida)*
- [`43eb0d9`](https://github.com/tobymao/sqlglot/commit/43eb0d9360f3154039e9eb71ee8818b6590d220a) - **tsql**: create schema ast access fixup fixes [#4632](https://github.com/tobymao/sqlglot/pull/4632) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`59f6525`](https://github.com/tobymao/sqlglot/commit/59f652572037940f136508ee60b8e0a137ce18f0) - **duckdb**: Transpile exp.RegexpILike *(PR [#4640](https://github.com/tobymao/sqlglot/pull/4640) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4639](https://github.com/tobymao/sqlglot/issues/4639) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`9db09ff`](https://github.com/tobymao/sqlglot/commit/9db09ff91931802c675a219951f28afee1d4019d) - **bigquery**: support more compact SAFE_DIVIDE transpilation [#4634](https://github.com/tobymao/sqlglot/pull/4634) *(PR [#4641](https://github.com/tobymao/sqlglot/pull/4641) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4634](https://github.com/tobymao/sqlglot/issues/4634) opened by [@bbernst](https://github.com/bbernst)*
- [`94af80b`](https://github.com/tobymao/sqlglot/commit/94af80b8bc3c44aa9770d6503f4e07ad4e37e314) - **optimizer**: Do not remove parens on bracketed expressions *(PR [#4645](https://github.com/tobymao/sqlglot/pull/4645) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3672](https://github.com/TobikoData/sqlmesh/issues/3672) opened by [@simon-pactum](https://github.com/simon-pactum)*
- [`761e835`](https://github.com/tobymao/sqlglot/commit/761e835e39fa819ef478b8086bfd814dbecc7927) - qualify using *(PR [#4646](https://github.com/tobymao/sqlglot/pull/4646) by [@tobymao](https://github.com/tobymao))*
- [`8b0b8ac`](https://github.com/tobymao/sqlglot/commit/8b0b8ac4ccbaf54d5fa948d9900ca53ccca9115b) - **sqlite**: allow 2-arg version of UNHEX closes [#4648](https://github.com/tobymao/sqlglot/pull/4648) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`2f12bd9`](https://github.com/tobymao/sqlglot/commit/2f12bd94d8583ddf9af808dda4df1690179ee592) - **athena**: Generate PartitionedByProperty correctly on CTAS for an Iceberg table *(PR [#4654](https://github.com/tobymao/sqlglot/pull/4654) by [@erindru](https://github.com/erindru))*
- [`1ea0dc2`](https://github.com/tobymao/sqlglot/commit/1ea0dc296ca2e47d466ddce162ad64945c532586) - **postgres**: Support WITHIN GROUP ( order_by_clause ) FILTER for Postgres *(PR [#4652](https://github.com/tobymao/sqlglot/pull/4652) by [@gl3nnleblanc](https://github.com/gl3nnleblanc))*
  - :arrow_lower_right: *fixes issue [#4651](https://github.com/tobymao/sqlglot/issues/4651) opened by [@gl3nnleblanc](https://github.com/gl3nnleblanc)*

### :recycle: Refactors
- [`284a936`](https://github.com/tobymao/sqlglot/commit/284a9360c5d43301da34d8d5199f101423ade289) - simplify WITHIN GROUP ... FILTER support *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`73512f9`](https://github.com/tobymao/sqlglot/commit/73512f9dde03b632b5f9eff0331713f9b44996d7) - set default properly for use_rs_tokenizer *(PR [#4619](https://github.com/tobymao/sqlglot/pull/4619) by [@georgesittas](https://github.com/georgesittas))*
- [`9ba1db3`](https://github.com/tobymao/sqlglot/commit/9ba1db3436d2afba5821b853cb3c573aada370e7) - add bench command *(PR [#4621](https://github.com/tobymao/sqlglot/pull/4621) by [@benfdking](https://github.com/benfdking))*
- [`0aa1516`](https://github.com/tobymao/sqlglot/commit/0aa1516cd8bf5f7d77e6d743f30f1526ccf15633) - move to string new *(PR [#4637](https://github.com/tobymao/sqlglot/pull/4637) by [@benfdking](https://github.com/benfdking))*
- [`2355a91`](https://github.com/tobymao/sqlglot/commit/2355a914752f3add75457849ae8f8ec00754f888) - clean up unnecessary mut *(PR [#4636](https://github.com/tobymao/sqlglot/pull/4636) by [@benfdking](https://github.com/benfdking))*
- [`0b68af5`](https://github.com/tobymao/sqlglot/commit/0b68af545bc82317ee16903d525e7b47f273d92d) - bump sqlglotrs to 0.3.6 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.2.1] - 2025-01-15
### :wrench: Chores
- [`b447322`](https://github.com/tobymao/sqlglot/commit/b4473220f0f50a9ce2463b3a98a77bf2fdd897af) - parser accepts ctes without as keyword again, except for clickhouse *(PR [#4612](https://github.com/tobymao/sqlglot/pull/4612) by [@georgesittas](https://github.com/georgesittas))*


## [v26.2.0] - 2025-01-14
### :boom: BREAKING CHANGES
- due to [`f3fcc10`](https://github.com/tobymao/sqlglot/commit/f3fcc1013dfcfdaa388ba3426ed82c4fe0eefab1) - allow limit, offset to be used as both modifiers and aliases *(PR [#4589](https://github.com/tobymao/sqlglot/pull/4589) by [@georgesittas](https://github.com/georgesittas))*:

  allow limit, offset to be used as both modifiers and aliases (#4589)

- due to [`b7ab3f1`](https://github.com/tobymao/sqlglot/commit/b7ab3f1697bda3d67a1183e6cd78dbd13777112b) - exp.Merge condition for Trino/Postgres *(PR [#4596](https://github.com/tobymao/sqlglot/pull/4596) by [@MikeWallis42](https://github.com/MikeWallis42))*:

  exp.Merge condition for Trino/Postgres (#4596)

- due to [`e617d40`](https://github.com/tobymao/sqlglot/commit/e617d407ece96d3c3311c95936ccdca6ecd35a70) - extend ANALYZE common syntax to cover multiple dialects *(PR [#4591](https://github.com/tobymao/sqlglot/pull/4591) by [@zashroof](https://github.com/zashroof))*:

  extend ANALYZE common syntax to cover multiple dialects (#4591)


### :sparkles: New Features
- [`c75016a`](https://github.com/tobymao/sqlglot/commit/c75016a83cda5eb328f854a8628884b90dec10e4) - parse analyze compute statistics *(PR [#4547](https://github.com/tobymao/sqlglot/pull/4547) by [@zashroof](https://github.com/zashroof))*
- [`986a1da`](https://github.com/tobymao/sqlglot/commit/986a1da98fa5648bc3e364ae436dc4168a1b33ed) - Druid dialect *(PR [#4579](https://github.com/tobymao/sqlglot/pull/4579) by [@betodealmeida](https://github.com/betodealmeida))*
- [`bc9975f`](https://github.com/tobymao/sqlglot/commit/bc9975fe80d66b0c25b8755f1757f049edb4d0be) - move to rustc fx hashmap *(PR [#4588](https://github.com/tobymao/sqlglot/pull/4588) by [@benfdking](https://github.com/benfdking))*
- [`853cbe6`](https://github.com/tobymao/sqlglot/commit/853cbe655f2aa3fa4debb8091b335eb6f9530390) - cleaner IS_ASCII for TSQL *(PR [#4592](https://github.com/tobymao/sqlglot/pull/4592) by [@pruzko](https://github.com/pruzko))*
- [`3ebd879`](https://github.com/tobymao/sqlglot/commit/3ebd87919a4a9947c077c657c03ba2d2b3799620) - LOGICAL_AND and LOGICAL_OR for Oracle *(PR [#4593](https://github.com/tobymao/sqlglot/pull/4593) by [@pruzko](https://github.com/pruzko))*
- [`e617d40`](https://github.com/tobymao/sqlglot/commit/e617d407ece96d3c3311c95936ccdca6ecd35a70) - extend ANALYZE common syntax to cover multiple dialects *(PR [#4591](https://github.com/tobymao/sqlglot/pull/4591) by [@zashroof](https://github.com/zashroof))*

### :bug: Bug Fixes
- [`766d698`](https://github.com/tobymao/sqlglot/commit/766d69886ac088de7dd9a22d71124ffa1b36d003) - **postgres**: Revert exp.StrPos generation *(PR [#4586](https://github.com/tobymao/sqlglot/pull/4586) by [@VaggelisD](https://github.com/VaggelisD))*
- [`f3fcc10`](https://github.com/tobymao/sqlglot/commit/f3fcc1013dfcfdaa388ba3426ed82c4fe0eefab1) - **parser**: allow limit, offset to be used as both modifiers and aliases *(PR [#4589](https://github.com/tobymao/sqlglot/pull/4589) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4575](https://github.com/tobymao/sqlglot/issues/4575) opened by [@baruchoxman](https://github.com/baruchoxman)*
- [`2bea466`](https://github.com/tobymao/sqlglot/commit/2bea466cbef3adfc09185176ee38ddf820b3f7ab) - **optimizer**: unions on nested subqueries *(PR [#4603](https://github.com/tobymao/sqlglot/pull/4603) by [@barakalon](https://github.com/barakalon))*
- [`199508a`](https://github.com/tobymao/sqlglot/commit/199508a77c62f75b5e12fee47828d34e4903c706) - **snowflake**: treat $ as part of the json path key identifier *(PR [#4604](https://github.com/tobymao/sqlglot/pull/4604) by [@georgesittas](https://github.com/georgesittas))*
- [`b7ab3f1`](https://github.com/tobymao/sqlglot/commit/b7ab3f1697bda3d67a1183e6cd78dbd13777112b) - exp.Merge condition for Trino/Postgres *(PR [#4596](https://github.com/tobymao/sqlglot/pull/4596) by [@MikeWallis42](https://github.com/MikeWallis42))*
  - :arrow_lower_right: *fixes issue [#4595](https://github.com/tobymao/sqlglot/issues/4595) opened by [@MikeWallis42](https://github.com/MikeWallis42)*

### :recycle: Refactors
- [`c0f7309`](https://github.com/tobymao/sqlglot/commit/c0f7309327e21204a0a0f273712d3097f02f6796) - simplify `trie_filter` closure in `Tokenizer` initialization *(PR [#4599](https://github.com/tobymao/sqlglot/pull/4599) by [@gvozdvmozgu](https://github.com/gvozdvmozgu))*
- [`fb93219`](https://github.com/tobymao/sqlglot/commit/fb932198087e5e3aa1a42e65ac30f28e24c6d84f) - replace `std::mem::replace` with `std::mem::take` and `Vec::drain` *(PR [#4600](https://github.com/tobymao/sqlglot/pull/4600) by [@gvozdvmozgu](https://github.com/gvozdvmozgu))*

### :wrench: Chores
- [`672d656`](https://github.com/tobymao/sqlglot/commit/672d656eb5a014ba42492ba2c2a9a33ebd145bd8) - clean up ANALYZE implementation *(PR [#4607](https://github.com/tobymao/sqlglot/pull/4607) by [@georgesittas](https://github.com/georgesittas))*
- [`e58a8cb`](https://github.com/tobymao/sqlglot/commit/e58a8cb4d388d22eff8fd2cca08f38e4c42075d6) - apply clippy fixes *(PR [#4608](https://github.com/tobymao/sqlglot/pull/4608) by [@benfdking](https://github.com/benfdking))*
- [`5502c94`](https://github.com/tobymao/sqlglot/commit/5502c94d665a2ed354e44beb145e767bab00adfa) - bump sqlglotrs to 0.3.5 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.1.3] - 2025-01-09
### :bug: Bug Fixes
- [`d250846`](https://github.com/tobymao/sqlglot/commit/d250846d05711ac62a45efd4930f0ca712841b11) - **snowflake**: generate LIMIT when OFFSET exists [#4575](https://github.com/tobymao/sqlglot/pull/4575) *(PR [#4581](https://github.com/tobymao/sqlglot/pull/4581) by [@geooo109](https://github.com/geooo109))*

### :wrench: Chores
- [`ffbb935`](https://github.com/tobymao/sqlglot/commit/ffbb9350f8d0decab4555471ec2e468fa2741f5f) - install python 3.7 when building windows wheel for sqlglotrs *(PR [#4585](https://github.com/tobymao/sqlglot/pull/4585) by [@georgesittas](https://github.com/georgesittas))*
- [`1ea05c0`](https://github.com/tobymao/sqlglot/commit/1ea05c0b4e3cf53482058b22ecac7ec7c1de525d) - bump sqlglotrs to 0.3.4 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.1.2] - 2025-01-08
### :wrench: Chores
- [`e33af0b`](https://github.com/tobymao/sqlglot/commit/e33af0bcd859571dab68aef3a1fc9ecbf5c49e71) - try setup-python@v5 in the publish job *(PR [#4582](https://github.com/tobymao/sqlglot/pull/4582) by [@georgesittas](https://github.com/georgesittas))*
- [`3259f84`](https://github.com/tobymao/sqlglot/commit/3259f84f1faa6f1135ecca7d0f5fcd4b187b4da7) - bump sqlglotrs to 0.3.3 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.1.1] - 2025-01-08
### :wrench: Chores
- [`e51d1cf`](https://github.com/tobymao/sqlglot/commit/e51d1cfb0aa1028bb116851b03b759282305217b) - release sqlglotrs for Python 3.13 on windows *(PR [#4580](https://github.com/tobymao/sqlglot/pull/4580) by [@VaggelisD](https://github.com/VaggelisD))*
- [`975ffa0`](https://github.com/tobymao/sqlglot/commit/975ffa0e10f08243375e5e83384fd0e134730d14) - bump sqlglotrs to 0.3.2 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.1.0] - 2025-01-08
### :boom: BREAKING CHANGES
- due to [`07d05da`](https://github.com/tobymao/sqlglot/commit/07d05da95c7d3882a7032dade3cbeefbd96628b7) - normalize before qualifying tables *(PR [#4539](https://github.com/tobymao/sqlglot/pull/4539) by [@tobymao](https://github.com/tobymao))*:

  normalize before qualifying tables (#4539)

- due to [`cead7c3`](https://github.com/tobymao/sqlglot/commit/cead7c32bef44c0efaf48c2038976c7c7f2b709c) - require AS token in CTEs for all dialects except spark, databricks *(PR [#4546](https://github.com/tobymao/sqlglot/pull/4546) by [@georgesittas](https://github.com/georgesittas))*:

  require AS token in CTEs for all dialects except spark, databricks (#4546)

- due to [`231d032`](https://github.com/tobymao/sqlglot/commit/231d03202e4338ee097662d59770dae1a9958617) - support Unicode in sqlite, mysql, tsql, postgres, oracle *(PR [#4554](https://github.com/tobymao/sqlglot/pull/4554) by [@pruzko](https://github.com/pruzko))*:

  support Unicode in sqlite, mysql, tsql, postgres, oracle (#4554)

- due to [`83595b6`](https://github.com/tobymao/sqlglot/commit/83595b67f0aa4cafdfcf4bace7b92c17f9e9f5f3) - parse ASCII into Unicode to facilitate transpilation *(commit by [@georgesittas](https://github.com/georgesittas))*:

  parse ASCII into Unicode to facilitate transpilation

- due to [`e141960`](https://github.com/tobymao/sqlglot/commit/e1419607981cd8fe597781faeae429069b13d5fb) - improve transpilation of CHAR[ACTER]_LENGTH *(PR [#4555](https://github.com/tobymao/sqlglot/pull/4555) by [@pruzko](https://github.com/pruzko))*:

  improve transpilation of CHAR[ACTER]_LENGTH (#4555)


### :sparkles: New Features
- [`7a517d7`](https://github.com/tobymao/sqlglot/commit/7a517d71dbcab4b46538263cac604ac38e714e6b) - Introduce meta comment to parse known functions as exp.Anonymous *(PR [#4532](https://github.com/tobymao/sqlglot/pull/4532) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4522](https://github.com/tobymao/sqlglot/issues/4522) opened by [@github-christophe-oudar](https://github.com/github-christophe-oudar)*
- [`6992c18`](https://github.com/tobymao/sqlglot/commit/6992c1855f343a5d0120a3b4c993d8c406dd29ba) - **tokenizer**: Allow underscore separated number literals *(PR [#4536](https://github.com/tobymao/sqlglot/pull/4536) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4530](https://github.com/tobymao/sqlglot/issues/4530) opened by [@martijnthe](https://github.com/martijnthe)*
- [`7fe9f7f`](https://github.com/tobymao/sqlglot/commit/7fe9f7f6dbc701580ca17318400203245331704e) - **tsql**: add support for DATETRUNC [#4531](https://github.com/tobymao/sqlglot/pull/4531) *(PR [#4537](https://github.com/tobymao/sqlglot/pull/4537) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#4531](https://github.com/tobymao/sqlglot/issues/4531) opened by [@rajat-wisdom](https://github.com/rajat-wisdom)*
- [`931eef6`](https://github.com/tobymao/sqlglot/commit/931eef6958be87ef88f4ff5311441e7a7004b8c5) - **duckdb**: Support simplified UNPIVOT statement *(PR [#4545](https://github.com/tobymao/sqlglot/pull/4545) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4542](https://github.com/tobymao/sqlglot/issues/4542) opened by [@Bilbottom](https://github.com/Bilbottom)*
- [`923a1f7`](https://github.com/tobymao/sqlglot/commit/923a1f7fda66f3dc61ee12755fe8960f8aeb3cd8) - treat NAMESPACE as a db creatable *(PR [#4556](https://github.com/tobymao/sqlglot/pull/4556) by [@TanviPardeshi](https://github.com/TanviPardeshi))*
- [`b0cc7d0`](https://github.com/tobymao/sqlglot/commit/b0cc7d029a78c7929daff9b30dc072608d9c80b0) - add support for IS_ASCII *(PR [#4557](https://github.com/tobymao/sqlglot/pull/4557) by [@pruzko](https://github.com/pruzko))*
- [`231d032`](https://github.com/tobymao/sqlglot/commit/231d03202e4338ee097662d59770dae1a9958617) - support Unicode in sqlite, mysql, tsql, postgres, oracle *(PR [#4554](https://github.com/tobymao/sqlglot/pull/4554) by [@pruzko](https://github.com/pruzko))*
- [`83595b6`](https://github.com/tobymao/sqlglot/commit/83595b67f0aa4cafdfcf4bace7b92c17f9e9f5f3) - **hive**: parse ASCII into Unicode to facilitate transpilation *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`095fb1e`](https://github.com/tobymao/sqlglot/commit/095fb1e834153eeeea33885dc20e1ba05f8bf814) - generate POSITION instead of STRPOS for Postgres *(PR [#4577](https://github.com/tobymao/sqlglot/pull/4577) by [@pruzko](https://github.com/pruzko))*
- [`9def0b7`](https://github.com/tobymao/sqlglot/commit/9def0b79ee623a07d8c367e0ec575ed8e63c83c6) - add support for Chr in tsql and sqlite *(PR [#4566](https://github.com/tobymao/sqlglot/pull/4566) by [@pruzko](https://github.com/pruzko))*
- [`d32d26a`](https://github.com/tobymao/sqlglot/commit/d32d26affaa7b0639abc107505db234aeb7386d4) - **postgres**: add support for XMLTABLE *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`e141960`](https://github.com/tobymao/sqlglot/commit/e1419607981cd8fe597781faeae429069b13d5fb) - improve transpilation of CHAR[ACTER]_LENGTH *(PR [#4555](https://github.com/tobymao/sqlglot/pull/4555) by [@pruzko](https://github.com/pruzko))*

### :bug: Bug Fixes
- [`07d05da`](https://github.com/tobymao/sqlglot/commit/07d05da95c7d3882a7032dade3cbeefbd96628b7) - normalize before qualifying tables *(PR [#4539](https://github.com/tobymao/sqlglot/pull/4539) by [@tobymao](https://github.com/tobymao))*
  - :arrow_lower_right: *fixes issue [#4538](https://github.com/tobymao/sqlglot/issues/4538) opened by [@karakanb](https://github.com/karakanb)*
- [`1ed3235`](https://github.com/tobymao/sqlglot/commit/1ed32358adc6b578e8b8af265ac8afe37aae9ad8) - allow When in exp.merge fixes [#4543](https://github.com/tobymao/sqlglot/pull/4543) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`cead7c3`](https://github.com/tobymao/sqlglot/commit/cead7c32bef44c0efaf48c2038976c7c7f2b709c) - **parser**: require AS token in CTEs for all dialects except spark, databricks *(PR [#4546](https://github.com/tobymao/sqlglot/pull/4546) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4544](https://github.com/tobymao/sqlglot/issues/4544) opened by [@xtess16](https://github.com/xtess16)*
- [`006b384`](https://github.com/tobymao/sqlglot/commit/006b3842f90186f8932f0dbf02453f138129608b) - **postgres**: add support for WHERE clause in INSERT DML *(PR [#4550](https://github.com/tobymao/sqlglot/pull/4550) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4549](https://github.com/tobymao/sqlglot/issues/4549) opened by [@VigneshChennai](https://github.com/VigneshChennai)*
- [`795e7e0`](https://github.com/tobymao/sqlglot/commit/795e7e0e857486417ce98246389849fc09ccb60a) - Pin ubuntu to 22.04 for Python 3.7 *(PR [#4571](https://github.com/tobymao/sqlglot/pull/4571) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2495508`](https://github.com/tobymao/sqlglot/commit/2495508fa7b3931d466c36b5ed225b2e1510b01c) - **tsql**: generate correct DateFromParts naming *(PR [#4563](https://github.com/tobymao/sqlglot/pull/4563) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4558](https://github.com/tobymao/sqlglot/issues/4558) opened by [@rajat-wisdom](https://github.com/rajat-wisdom)*
- [`2aff4ae`](https://github.com/tobymao/sqlglot/commit/2aff4ae861dc5225a616f5e3980cf04805e5b339) - **duckdb**: support parentheses with FROM-First syntax *(PR [#4569](https://github.com/tobymao/sqlglot/pull/4569) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4561](https://github.com/tobymao/sqlglot/issues/4561) opened by [@LennartH](https://github.com/LennartH)*
- [`51ac9a7`](https://github.com/tobymao/sqlglot/commit/51ac9a7b8a91d1bb5b3b6b396e1083c03573a708) - **rust-tokenizer**: increase integer width when converting hex literals *(PR [#4573](https://github.com/tobymao/sqlglot/pull/4573) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4560](https://github.com/tobymao/sqlglot/issues/4560) opened by [@whummer](https://github.com/whummer)*
- [`94ffdb7`](https://github.com/tobymao/sqlglot/commit/94ffdb7b790c6c2235a0586c6df23c3155c184b1) - addressing mismatch in STR_POSITION argument order in executor. *(PR [#4574](https://github.com/tobymao/sqlglot/pull/4574) by [@cecilycarver](https://github.com/cecilycarver))*
- [`139b699`](https://github.com/tobymao/sqlglot/commit/139b699f61326bdf9700f0ba9bea9a44e594cf6d) - **tsql**: transpile snowflake TIMESTAMP_NTZ to DATETIME2 *(PR [#4576](https://github.com/tobymao/sqlglot/pull/4576) by [@geooo109](https://github.com/geooo109))*
- [`ceb42fa`](https://github.com/tobymao/sqlglot/commit/ceb42fabad60312699e4b15936aeebac00e22e4d) - parse & generate Length properly in clickhouse *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`9921528`](https://github.com/tobymao/sqlglot/commit/992152840ffad5fb85315e0bead2c498d4310cc5) - introduce benchmarking for rust *(PR [#4552](https://github.com/tobymao/sqlglot/pull/4552) by [@benfdking](https://github.com/benfdking))*
- [`0ffe8f9`](https://github.com/tobymao/sqlglot/commit/0ffe8f91eb8295ab8171e029aa4ccbf071028a4a) - temporarily disable sqlglotrs benchmarking *(PR [#4578](https://github.com/tobymao/sqlglot/pull/4578) by [@georgesittas](https://github.com/georgesittas))*
- [`7a0dbcf`](https://github.com/tobymao/sqlglot/commit/7a0dbcfda26ff7cf20371c84b31f454e63260959) - bump sqlglotrs to 0.3.1 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.0.1] - 2024-12-18
### :sparkles: New Features
- [`5d3ee4c`](https://github.com/tobymao/sqlglot/commit/5d3ee4cac1c5c9e45cbf6263c32c87fda78f9854) - **snowflake**: transpile date subtraction *(PR [#4506](https://github.com/tobymao/sqlglot/pull/4506) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4485](https://github.com/tobymao/sqlglot/issues/4485) opened by [@cisenbe](https://github.com/cisenbe)*
- [`efeb4bd`](https://github.com/tobymao/sqlglot/commit/efeb4bd870dd5c017b31d6b95c9bd6311c75b9ae) - **postgres**: add support for XMLELEMENT *(PR [#4513](https://github.com/tobymao/sqlglot/pull/4513) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4512](https://github.com/tobymao/sqlglot/issues/4512) opened by [@fresioAS](https://github.com/fresioAS)*
- [`e495777`](https://github.com/tobymao/sqlglot/commit/e495777b8612866041050c96d3df700cd829dc9c) - **clickhouse**: add support for bracket map syntax *(PR [#4528](https://github.com/tobymao/sqlglot/pull/4528) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4527](https://github.com/tobymao/sqlglot/issues/4527) opened by [@mrcljx](https://github.com/mrcljx)*
- [`cc44ed7`](https://github.com/tobymao/sqlglot/commit/cc44ed73fa4489e0bcb457b7eae8a9772415db65) - **mysql**: Support SERIAL data type *(PR [#4533](https://github.com/tobymao/sqlglot/pull/4533) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4529](https://github.com/tobymao/sqlglot/issues/4529) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`ee7dc96`](https://github.com/tobymao/sqlglot/commit/ee7dc966d533228756c3294c66422c27eceae503) - **starrocks**: add partition by range and unique key *(PR [#4509](https://github.com/tobymao/sqlglot/pull/4509) by [@pickfire](https://github.com/pickfire))*
- [`84ec478`](https://github.com/tobymao/sqlglot/commit/84ec47810e0a5c9e71a2b48e686656f9c2eafb39) - **lineage**: Extend lineage function to work with pivot operation *(PR [#4471](https://github.com/tobymao/sqlglot/pull/4471) by [@step4](https://github.com/step4))*
- [`52c8374`](https://github.com/tobymao/sqlglot/commit/52c8374876bc4037dcb81a50301fdd62cb14bb2a) - include comments in gen *(PR [#4535](https://github.com/tobymao/sqlglot/pull/4535) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`8f8e84a`](https://github.com/tobymao/sqlglot/commit/8f8e84ae81d60bea224e35b9ca88b0bb4a59512b) - **snowflake**: bitxor third parameter(padside) issue *(PR [#4501](https://github.com/tobymao/sqlglot/pull/4501) by [@ankur334](https://github.com/ankur334))*
- [`4760246`](https://github.com/tobymao/sqlglot/commit/476024653e5b942faaaaa2b3bce30a3ea1873190) - **snowflake**: generate only one INPUT => clause in unnest_sql *(PR [#4505](https://github.com/tobymao/sqlglot/pull/4505) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4503](https://github.com/tobymao/sqlglot/issues/4503) opened by [@harounp](https://github.com/harounp)*
- [`7649d50`](https://github.com/tobymao/sqlglot/commit/7649d5053e3305dadb83769bb5cec52ed8235a19) - **optimizer**: only expand stars for select scopes *(PR [#4515](https://github.com/tobymao/sqlglot/pull/4515) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4514](https://github.com/tobymao/sqlglot/issues/4514) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*
- [`2b68b9b`](https://github.com/tobymao/sqlglot/commit/2b68b9b7967b68465042a1b8c2ee21bb30007712) - **snowflake**: Allow alias expansion inside JOIN statements *(PR [#4504](https://github.com/tobymao/sqlglot/pull/4504) by [@florian-ernst-alan](https://github.com/florian-ernst-alan))*
  - :arrow_lower_right: *fixes issue [#4502](https://github.com/tobymao/sqlglot/issues/4502) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*
- [`e15cd0b`](https://github.com/tobymao/sqlglot/commit/e15cd0be1c66e0e72d9815575fa9b210e66cf7c9) - **postgres**: generate float if the type has precision *(PR [#4516](https://github.com/tobymao/sqlglot/pull/4516) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4508](https://github.com/tobymao/sqlglot/issues/4508) opened by [@RedTailedHawk](https://github.com/RedTailedHawk)*
- [`98906d4`](https://github.com/tobymao/sqlglot/commit/98906d4520a0c582a0534384ee3d0c1449846ee6) - another interval parsing edge case *(PR [#4519](https://github.com/tobymao/sqlglot/pull/4519) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4490](https://github.com/tobymao/sqlglot/issues/4490) opened by [@fuglaeff](https://github.com/fuglaeff)*
- [`992f6e9`](https://github.com/tobymao/sqlglot/commit/992f6e9fc867aa5ad60a255be593b8982a0fbcba) - **tsql**: Convert exp.Neg literal to number through to_py() *(PR [#4523](https://github.com/tobymao/sqlglot/pull/4523) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4520](https://github.com/tobymao/sqlglot/issues/4520) opened by [@DzianisKryvasheya](https://github.com/DzianisKryvasheya)*
- [`946cd42`](https://github.com/tobymao/sqlglot/commit/946cd4234a2ca403785b7c6a026a39ef604e8754) - **optimizer**: qualify snowflake queries with `level` pseudocolumn *(PR [#4524](https://github.com/tobymao/sqlglot/pull/4524) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4518](https://github.com/tobymao/sqlglot/issues/4518) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*
- [`bc68289`](https://github.com/tobymao/sqlglot/commit/bc68289d4d368b29241e56b8f0aefc36db65ad47) - **planner**: ensure aggregate variable is bound *(PR [#4526](https://github.com/tobymao/sqlglot/pull/4526) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4525](https://github.com/tobymao/sqlglot/issues/4525) opened by [@EyalDlph](https://github.com/EyalDlph)*

### :recycle: Refactors
- [`cd6e00f`](https://github.com/tobymao/sqlglot/commit/cd6e00f55195e26c3d02e255e66b45ab781addad) - clean up pivot lineage *(PR [#4534](https://github.com/tobymao/sqlglot/pull/4534) by [@georgesittas](https://github.com/georgesittas))*


## [v26.0.0] - 2024-12-10
### :boom: BREAKING CHANGES
- due to [`1d3c9aa`](https://github.com/tobymao/sqlglot/commit/1d3c9aa604c7bf60166a0e5587f1a8d88b89bea6) - Transpile support for bitor/bit_or snowflake function *(PR [#4486](https://github.com/tobymao/sqlglot/pull/4486) by [@ankur334](https://github.com/ankur334))*:

  Transpile support for bitor/bit_or snowflake function (#4486)

- due to [`ab10851`](https://github.com/tobymao/sqlglot/commit/ab108518c53173ddf71ac1dfd9e45df6ac621b81) - Preserve roundtrips of DATETIME/DATETIME2 *(PR [#4491](https://github.com/tobymao/sqlglot/pull/4491) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve roundtrips of DATETIME/DATETIME2 (#4491)


### :sparkles: New Features
- [`1d3c9aa`](https://github.com/tobymao/sqlglot/commit/1d3c9aa604c7bf60166a0e5587f1a8d88b89bea6) - **snowflake**: Transpile support for bitor/bit_or snowflake function *(PR [#4486](https://github.com/tobymao/sqlglot/pull/4486) by [@ankur334](https://github.com/ankur334))*
- [`822aea0`](https://github.com/tobymao/sqlglot/commit/822aea0826f09fa773193004acb2af99e495fddd) - **snowflake**: Support for inline FOREIGN KEY *(PR [#4493](https://github.com/tobymao/sqlglot/pull/4493) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4489](https://github.com/tobymao/sqlglot/issues/4489) opened by [@kylekarpack](https://github.com/kylekarpack)*

### :bug: Bug Fixes
- [`ab10851`](https://github.com/tobymao/sqlglot/commit/ab108518c53173ddf71ac1dfd9e45df6ac621b81) - **tsql**: Preserve roundtrips of DATETIME/DATETIME2 *(PR [#4491](https://github.com/tobymao/sqlglot/pull/4491) by [@VaggelisD](https://github.com/VaggelisD))*
- [`43975e4`](https://github.com/tobymao/sqlglot/commit/43975e4b7abcd640cd5a0f91aea1fbda8dd893cb) - **duckdb**: Allow escape strings similar to Postgres *(PR [#4497](https://github.com/tobymao/sqlglot/pull/4497) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4496](https://github.com/tobymao/sqlglot/issues/4496) opened by [@LennartH](https://github.com/LennartH)*


## [v25.34.1] - 2024-12-10
### :boom: BREAKING CHANGES
- due to [`f70f124`](https://github.com/tobymao/sqlglot/commit/f70f12408fbaf021dd105f2eac957b9e6fac045d) - transpile MySQL FORMAT to DuckDB *(PR [#4488](https://github.com/tobymao/sqlglot/pull/4488) by [@georgesittas](https://github.com/georgesittas))*:

  transpile MySQL FORMAT to DuckDB (#4488)


### :sparkles: New Features
- [`f70f124`](https://github.com/tobymao/sqlglot/commit/f70f12408fbaf021dd105f2eac957b9e6fac045d) - transpile MySQL FORMAT to DuckDB *(PR [#4488](https://github.com/tobymao/sqlglot/pull/4488) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4445](https://github.com/tobymao/sqlglot/issues/4445) opened by [@fanyang01](https://github.com/fanyang01)*
- [`5a276f3`](https://github.com/tobymao/sqlglot/commit/5a276f33df48dab96e77c560c4b193f9634974f7) - add parse into tuple *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`ddf7483`](https://github.com/tobymao/sqlglot/commit/ddf74833c3e033067e731eab387db658a9a803be) - enable python 3.13 in CI *(PR [#4483](https://github.com/tobymao/sqlglot/pull/4483) by [@simon-pactum](https://github.com/simon-pactum))*


## [v25.34.0] - 2024-12-06
### :boom: BREAKING CHANGES
- due to [`41c6d24`](https://github.com/tobymao/sqlglot/commit/41c6d24c99e130b3c8e35e348a25a59e9e3d5553) - Alias expanded USING STRUCT fields *(PR [#4474](https://github.com/tobymao/sqlglot/pull/4474) by [@VaggelisD](https://github.com/VaggelisD))*:

  Alias expanded USING STRUCT fields (#4474)


### :sparkles: New Features
- [`41c6d24`](https://github.com/tobymao/sqlglot/commit/41c6d24c99e130b3c8e35e348a25a59e9e3d5553) - **optimizer**: Alias expanded USING STRUCT fields *(PR [#4474](https://github.com/tobymao/sqlglot/pull/4474) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3465](https://github.com/TobikoData/sqlmesh/issues/3465) opened by [@esciara](https://github.com/esciara)*

### :bug: Bug Fixes
- [`a34bcde`](https://github.com/tobymao/sqlglot/commit/a34bcde1f7b4b2974a0132555477fa5a788126b4) - **bigquery**: properly consume dashed table parts *(PR [#4477](https://github.com/tobymao/sqlglot/pull/4477) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4476](https://github.com/tobymao/sqlglot/issues/4476) opened by [@matthewcyy](https://github.com/matthewcyy)*
- [`438ae4c`](https://github.com/tobymao/sqlglot/commit/438ae4c0691fb3ad43ef95e613118a116cb7924c) - **bigquery**: Do not generate NULL ordering on Windows *(PR [#4480](https://github.com/tobymao/sqlglot/pull/4480) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4478](https://github.com/tobymao/sqlglot/issues/4478) opened by [@goldmedal](https://github.com/goldmedal)*


## [v25.33.0] - 2024-12-04
### :boom: BREAKING CHANGES
- due to [`07fa69d`](https://github.com/tobymao/sqlglot/commit/07fa69dcb8970167ba0c55fff39175ab856ea9f3) - Make TIMESTAMP map to Type.TIMESTAMPTZ *(PR [#4451](https://github.com/tobymao/sqlglot/pull/4451) by [@VaggelisD](https://github.com/VaggelisD))*:

  Make TIMESTAMP map to Type.TIMESTAMPTZ (#4451)

- due to [`63d8f41`](https://github.com/tobymao/sqlglot/commit/63d8f41794b2e9d22f87d0a8fbfbd83125889ca2) - treat NEXT as a func keyword, parse NEXT VALUE FOR in tsql, oracle *(PR [#4467](https://github.com/tobymao/sqlglot/pull/4467) by [@georgesittas](https://github.com/georgesittas))*:

  treat NEXT as a func keyword, parse NEXT VALUE FOR in tsql, oracle (#4467)


### :sparkles: New Features
- [`3945acc`](https://github.com/tobymao/sqlglot/commit/3945acc4a0dfd58147de929c9a2c71734d8f1ade) - allow tables to be preserved in replace_table *(PR [#4468](https://github.com/tobymao/sqlglot/pull/4468) by [@georgesittas](https://github.com/georgesittas))*
- [`a9dca8d`](https://github.com/tobymao/sqlglot/commit/a9dca8dd1b523efd703003694d4389f9af9d1a12) - **postgres**: Support generated columns *(PR [#4472](https://github.com/tobymao/sqlglot/pull/4472) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4463](https://github.com/tobymao/sqlglot/issues/4463) opened by [@AKST](https://github.com/AKST)*

### :bug: Bug Fixes
- [`380dad2`](https://github.com/tobymao/sqlglot/commit/380dad2f5826caa820a69442c42805c7b3c23ada) - **bigquery**: Rename CONTAINS_SUBSTRING to CONTAINS_SUBSTR *(PR [#4457](https://github.com/tobymao/sqlglot/pull/4457) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4456](https://github.com/tobymao/sqlglot/issues/4456) opened by [@romanhaa](https://github.com/romanhaa)*
- [`ca5023d`](https://github.com/tobymao/sqlglot/commit/ca5023db5ea2a2ece804f6e389640e0bd4987598) - **presto**: Remove parentheses from CURRENT_USER *(PR [#4459](https://github.com/tobymao/sqlglot/pull/4459) by [@MikeWallis42](https://github.com/MikeWallis42))*
  - :arrow_lower_right: *fixes issue [#4458](https://github.com/tobymao/sqlglot/issues/4458) opened by [@MikeWallis42](https://github.com/MikeWallis42)*
- [`07fa69d`](https://github.com/tobymao/sqlglot/commit/07fa69dcb8970167ba0c55fff39175ab856ea9f3) - **spark**: Make TIMESTAMP map to Type.TIMESTAMPTZ *(PR [#4451](https://github.com/tobymao/sqlglot/pull/4451) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4442](https://github.com/tobymao/sqlglot/issues/4442) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`63d8f41`](https://github.com/tobymao/sqlglot/commit/63d8f41794b2e9d22f87d0a8fbfbd83125889ca2) - **parser**: treat NEXT as a func keyword, parse NEXT VALUE FOR in tsql, oracle *(PR [#4467](https://github.com/tobymao/sqlglot/pull/4467) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4466](https://github.com/tobymao/sqlglot/issues/4466) opened by [@Harmuth94](https://github.com/Harmuth94)*


## [v25.32.1] - 2024-11-27
### :bug: Bug Fixes
- [`954d8fd`](https://github.com/tobymao/sqlglot/commit/954d8fd12740071e0951d1df3a405a4b9634868d) - parse DEFAULT in VALUES clause into a Var *(PR [#4448](https://github.com/tobymao/sqlglot/pull/4448) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4446](https://github.com/tobymao/sqlglot/issues/4446) opened by [@ddh-5230](https://github.com/ddh-5230)*
- [`73afd0f`](https://github.com/tobymao/sqlglot/commit/73afd0f435b7e7ccde831ee311c9a76c14797fdc) - **bigquery**: Make JSONPathTokenizer more lenient for new standards *(PR [#4447](https://github.com/tobymao/sqlglot/pull/4447) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4441](https://github.com/tobymao/sqlglot/issues/4441) opened by [@patricksurry](https://github.com/patricksurry)*


## [v25.32.0] - 2024-11-22
### :boom: BREAKING CHANGES
- due to [`0eed45c`](https://github.com/tobymao/sqlglot/commit/0eed45cce82681bfbafc8bfb78eb2a1bce86ae53) - Add support for ATTACH/DETACH statements *(PR [#4419](https://github.com/tobymao/sqlglot/pull/4419) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for ATTACH/DETACH statements (#4419)

- due to [`da48b68`](https://github.com/tobymao/sqlglot/commit/da48b68a4f1fa6a754fa2a0a789564675d59546f) - Tokenize hints as comments *(PR [#4426](https://github.com/tobymao/sqlglot/pull/4426) by [@VaggelisD](https://github.com/VaggelisD))*:

  Tokenize hints as comments (#4426)

- due to [`fe35394`](https://github.com/tobymao/sqlglot/commit/fe3539464a153b1c0bf46975d6221dee48a48f02) - fix datetime coercion in the canonicalize rule *(PR [#4431](https://github.com/tobymao/sqlglot/pull/4431) by [@georgesittas](https://github.com/georgesittas))*:

  fix datetime coercion in the canonicalize rule (#4431)

- due to [`fddcd3d`](https://github.com/tobymao/sqlglot/commit/fddcd3dfc264a645909686c201d2288c0adf9047) - bump sqlglotrs to 0.3.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.3.0


### :sparkles: New Features
- [`0eed45c`](https://github.com/tobymao/sqlglot/commit/0eed45cce82681bfbafc8bfb78eb2a1bce86ae53) - **duckdb**: Add support for ATTACH/DETACH statements *(PR [#4419](https://github.com/tobymao/sqlglot/pull/4419) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2db757d`](https://github.com/tobymao/sqlglot/commit/2db757dfec9ded26572b8e9a71dcc8ea8a2382fe) - **bigquery**: Support FEATURES_AT_TIME *(PR [#4430](https://github.com/tobymao/sqlglot/pull/4430) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4428](https://github.com/tobymao/sqlglot/issues/4428) opened by [@YuvrajSoni-Ksolves](https://github.com/YuvrajSoni-Ksolves)*
- [`fc591ae`](https://github.com/tobymao/sqlglot/commit/fc591ae2fa80be5821cb53d78906afe8e5505654) - **risingwave**: add support for SINK, SOURCE & other DDL properties *(PR [#4387](https://github.com/tobymao/sqlglot/pull/4387) by [@lin0303-siyuan](https://github.com/lin0303-siyuan))*
- [`a2bde2e`](https://github.com/tobymao/sqlglot/commit/a2bde2e03e9ef8650756bf304db35b4876746d1f) - **mysql**: improve transpilability of CHAR[ACTER]_LENGTH *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`0acc248`](https://github.com/tobymao/sqlglot/commit/0acc248361f49f68f17d799cbaf6b3de06c57f7e) - **snowflake**: Support CREATE ... WITH TAG *(PR [#4434](https://github.com/tobymao/sqlglot/pull/4434) by [@asikowitz](https://github.com/asikowitz))*
  - :arrow_lower_right: *addresses issue [#4427](https://github.com/tobymao/sqlglot/issues/4427) opened by [@asikowitz](https://github.com/asikowitz)*
- [`37863ff`](https://github.com/tobymao/sqlglot/commit/37863ffd747cad9c2b9bed60119cc1551faeffda) - **snowflake**: Transpile non-UNNEST exp.GenerateDateArray refs *(PR [#4433](https://github.com/tobymao/sqlglot/pull/4433) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`83ee97b`](https://github.com/tobymao/sqlglot/commit/83ee97b34cd0fe269b4820f15147d1ed7523612e) - **parser**: Do not parse window function arg as exp.Column *(PR [#4415](https://github.com/tobymao/sqlglot/pull/4415) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4410](https://github.com/tobymao/sqlglot/issues/4410) opened by [@merlindso](https://github.com/merlindso)*
- [`b22e0c8`](https://github.com/tobymao/sqlglot/commit/b22e0c8680b0ee5a382e57904b698bf21a94f782) - **parser**: Extend DESCRIBE parser for MySQL FORMAT & statements *(PR [#4417](https://github.com/tobymao/sqlglot/pull/4417) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4414](https://github.com/tobymao/sqlglot/issues/4414) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`d1d2ae7`](https://github.com/tobymao/sqlglot/commit/d1d2ae7d1514abc9477d275352e5e126509157c6) - **duckdb**: Allow count arg on exp.ArgMax & exp.ArgMin *(PR [#4413](https://github.com/tobymao/sqlglot/pull/4413) by [@aersam](https://github.com/aersam))*
  - :arrow_lower_right: *fixes issue [#4412](https://github.com/tobymao/sqlglot/issues/4412) opened by [@aersam](https://github.com/aersam)*
- [`e3c45d5`](https://github.com/tobymao/sqlglot/commit/e3c45d5ec0ae6827e4b0bcfb047aeac131379732) - presto reset session closes [#4421](https://github.com/tobymao/sqlglot/pull/4421) *(commit by [@tobymao](https://github.com/tobymao))*
- [`fd81f1b`](https://github.com/tobymao/sqlglot/commit/fd81f1bfee9a566b8df8bb501828c20bd72ac481) - more presto commands *(commit by [@tobymao](https://github.com/tobymao))*
- [`da48b68`](https://github.com/tobymao/sqlglot/commit/da48b68a4f1fa6a754fa2a0a789564675d59546f) - Tokenize hints as comments *(PR [#4426](https://github.com/tobymao/sqlglot/pull/4426) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4425](https://github.com/tobymao/sqlglot/issues/4425) opened by [@mkmoisen](https://github.com/mkmoisen)*
- [`69d4a8c`](https://github.com/tobymao/sqlglot/commit/69d4a8ccdf5954f293acbdf61c420b72dde5b8af) - **tsql**: Map weekday to %w *(PR [#4438](https://github.com/tobymao/sqlglot/pull/4438) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4435](https://github.com/tobymao/sqlglot/issues/4435) opened by [@travispaice](https://github.com/travispaice)*
- [`41d6a13`](https://github.com/tobymao/sqlglot/commit/41d6a13ccfb28fbcf772fd43ea17da3b36567e67) - add return type *(PR [#4440](https://github.com/tobymao/sqlglot/pull/4440) by [@etonlels](https://github.com/etonlels))*
- [`fe35394`](https://github.com/tobymao/sqlglot/commit/fe3539464a153b1c0bf46975d6221dee48a48f02) - **optimizer**: fix datetime coercion in the canonicalize rule *(PR [#4431](https://github.com/tobymao/sqlglot/pull/4431) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4429](https://github.com/tobymao/sqlglot/issues/4429) opened by [@Ca1ypso](https://github.com/Ca1ypso)*
- [`6aea9f3`](https://github.com/tobymao/sqlglot/commit/6aea9f346ef8f91467e1d5da5a3f94cf862b44fe) - Refactor NORMALIZE_FUNCTIONS flag usage *(PR [#4437](https://github.com/tobymao/sqlglot/pull/4437) by [@VaggelisD](https://github.com/VaggelisD))*

### :recycle: Refactors
- [`f32a435`](https://github.com/tobymao/sqlglot/commit/f32a435205ec288f310ad57748ac66805e27f7f5) - **risingwave**: clean up SINK/SOURCE logic *(PR [#4432](https://github.com/tobymao/sqlglot/pull/4432) by [@georgesittas](https://github.com/georgesittas))*
- [`b24aced`](https://github.com/tobymao/sqlglot/commit/b24aced2dbb7e471d2dd0eb830ea4f2e24f9d267) - **snowflake**: clean up [WITH] TAG property / constraint *(PR [#4439](https://github.com/tobymao/sqlglot/pull/4439) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`fddcd3d`](https://github.com/tobymao/sqlglot/commit/fddcd3dfc264a645909686c201d2288c0adf9047) - bump sqlglotrs to 0.3.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.4] - 2024-11-17
### :bug: Bug Fixes
- [`59b8b6d`](https://github.com/tobymao/sqlglot/commit/59b8b6d1409b4112d425cc31db45519d5936b6fa) - preserve column quoting in DISTINCT ON elimination *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.3] - 2024-11-17
### :sparkles: New Features
- [`835e717`](https://github.com/tobymao/sqlglot/commit/835e71795f994599dbc19f1a5969b464154926e1) - **clickhouse**: transform function support *(PR [#4408](https://github.com/tobymao/sqlglot/pull/4408) by [@GaliFFun](https://github.com/GaliFFun))*

### :bug: Bug Fixes
- [`0479743`](https://github.com/tobymao/sqlglot/commit/047974393cebbddbbfb878071d159a3e538b0e4d) - **snowflake**: cast to TimeToStr arg to TIMESTAMP more conservatively *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.2] - 2024-11-17
### :bug: Bug Fixes
- [`d851269`](https://github.com/tobymao/sqlglot/commit/d851269780c7f0a0c756289c3dea9b1aa58d2a69) - use existing aliases in DISTINCT ON elimination, if any *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.1] - 2024-11-17
### :sparkles: New Features
- [`b00d857`](https://github.com/tobymao/sqlglot/commit/b00d857cd8a6d2452c2170077cbfa82352f708dd) - add support for specifying column in row_number function *(PR [#4406](https://github.com/tobymao/sqlglot/pull/4406) by [@GaliFFun](https://github.com/GaliFFun))*

### :bug: Bug Fixes
- [`0e46cc7`](https://github.com/tobymao/sqlglot/commit/0e46cc7fa2d80ba4e92182b3fa5f1075a63f4754) - refactor DISTINCT ON elimination transformation *(PR [#4407](https://github.com/tobymao/sqlglot/pull/4407) by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.0] - 2024-11-16
### :boom: BREAKING CHANGES
- due to [`f4abfd5`](https://github.com/tobymao/sqlglot/commit/f4abfd59b8255cf8c39bf51028ee5f6ed704927f) - Support FORMAT_TIMESTAMP *(PR [#4383](https://github.com/tobymao/sqlglot/pull/4383) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support FORMAT_TIMESTAMP (#4383)

- due to [`45eef60`](https://github.com/tobymao/sqlglot/commit/45eef600064ad024b34e32e7acc3aca409fbd9c4) - use select star when eliminating distinct on *(PR [#4401](https://github.com/tobymao/sqlglot/pull/4401) by [@agrigoroi-palantir](https://github.com/agrigoroi-palantir))*:

  use select star when eliminating distinct on (#4401)


### :sparkles: New Features
- [`72ffdcb`](https://github.com/tobymao/sqlglot/commit/72ffdcb631bf7afdeda2ce96911442a94b7f11eb) - **bigquery**: Add parsing support for STRPOS(...) *(PR [#4378](https://github.com/tobymao/sqlglot/pull/4378) by [@VaggelisD](https://github.com/VaggelisD))*
- [`e7b67e0`](https://github.com/tobymao/sqlglot/commit/e7b67e0c280179188ce1bca650735978b758dca1) - **bigquery**: Support MAKE_INTERVAL *(PR [#4384](https://github.com/tobymao/sqlglot/pull/4384) by [@VaggelisD](https://github.com/VaggelisD))*
- [`37c4809`](https://github.com/tobymao/sqlglot/commit/37c4809dfda48224fd982ea8a48d3dbc5c17f9ae) - **bigquery**: Support INT64(...) *(PR [#4391](https://github.com/tobymao/sqlglot/pull/4391) by [@VaggelisD](https://github.com/VaggelisD))*
- [`9694999`](https://github.com/tobymao/sqlglot/commit/96949999d394e27df8b0287a14e9ac82d52bc0f9) - Add support for CONTAINS(...) *(PR [#4399](https://github.com/tobymao/sqlglot/pull/4399) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`f4abfd5`](https://github.com/tobymao/sqlglot/commit/f4abfd59b8255cf8c39bf51028ee5f6ed704927f) - **bigquery**: Support FORMAT_TIMESTAMP *(PR [#4383](https://github.com/tobymao/sqlglot/pull/4383) by [@VaggelisD](https://github.com/VaggelisD))*
- [`bb46ee3`](https://github.com/tobymao/sqlglot/commit/bb46ee33d481a888882cbbb26a9240dd2dbb10ee) - **parser**: Parse exp.Column for DROP COLUMN *(PR [#4390](https://github.com/tobymao/sqlglot/pull/4390) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4388](https://github.com/tobymao/sqlglot/issues/4388) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`79f6783`](https://github.com/tobymao/sqlglot/commit/79f67830d7d3ba92bff91eeb95b4dc8bdfa6c44e) - **snowflake**: Wrap DIV0 operands if they're binary expressions *(PR [#4393](https://github.com/tobymao/sqlglot/pull/4393) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4392](https://github.com/tobymao/sqlglot/issues/4392) opened by [@diogo-fernan](https://github.com/diogo-fernan)*
- [`647b98d`](https://github.com/tobymao/sqlglot/commit/647b98d84643b88a41218fb67f6a2bd83ca4c702) - **starrocks**: Add RESERVED_KEYWORDS specific to starrocks *(PR [#4402](https://github.com/tobymao/sqlglot/pull/4402) by [@notexistence](https://github.com/notexistence))*
- [`45eef60`](https://github.com/tobymao/sqlglot/commit/45eef600064ad024b34e32e7acc3aca409fbd9c4) - use select star when eliminating distinct on *(PR [#4401](https://github.com/tobymao/sqlglot/pull/4401) by [@agrigoroi-palantir](https://github.com/agrigoroi-palantir))*

### :recycle: Refactors
- [`a3af2af`](https://github.com/tobymao/sqlglot/commit/a3af2af3a893dfd6c6946b732aa086d1f1d91570) - attach stamement comments consistently *(PR [#4377](https://github.com/tobymao/sqlglot/pull/4377) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4376](https://github.com/tobymao/sqlglot/issues/4376) opened by [@YieldRay](https://github.com/YieldRay)*

### :wrench: Chores
- [`858c5b1`](https://github.com/tobymao/sqlglot/commit/858c5b1a43f74e11b8c357986c78b5068792b3af) - improve contribution guide *(PR [#4379](https://github.com/tobymao/sqlglot/pull/4379) by [@georgesittas](https://github.com/georgesittas))*
- [`160e688`](https://github.com/tobymao/sqlglot/commit/160e6883225cd6ad41a218213f73aa9f91b5fc5e) - fix relative benchmark import, comment out sqltree *(PR [#4403](https://github.com/tobymao/sqlglot/pull/4403) by [@georgesittas](https://github.com/georgesittas))*
- [`8d78add`](https://github.com/tobymao/sqlglot/commit/8d78addccaaffa4ea2dcfe1de002f8a653f137b7) - bump PYO3 to v"0.22.6" *(PR [#4400](https://github.com/tobymao/sqlglot/pull/4400) by [@MartinSahlen](https://github.com/MartinSahlen))*
- [`f78e755`](https://github.com/tobymao/sqlglot/commit/f78e755adaf52823642d2b0e1cae54da835ec653) - bump sqlglotrs to v0.2.14 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.30.0] - 2024-11-11
### :boom: BREAKING CHANGES
- due to [`60625ea`](https://github.com/tobymao/sqlglot/commit/60625eae34deb6a6fc36c0f3996f1281eae0ef6f) - Fix STRUCT cast generation *(PR [#4366](https://github.com/tobymao/sqlglot/pull/4366) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix STRUCT cast generation (#4366)


### :sparkles: New Features
- [`87ab8fe`](https://github.com/tobymao/sqlglot/commit/87ab8fe9cc4d6d060d8fe8a9c3faf8c47c2c9ed6) - **spark, bigquery**: Add support for UNIX_SECONDS(...) *(PR [#4350](https://github.com/tobymao/sqlglot/pull/4350) by [@VaggelisD](https://github.com/VaggelisD))*
- [`42da638`](https://github.com/tobymao/sqlglot/commit/42da63812ed489d1d8bbef0fc14c7dfa5ce57b7a) - **bigquery**: Support JSON_VALUE_ARRAY(...) *(PR [#4356](https://github.com/tobymao/sqlglot/pull/4356) by [@VaggelisD](https://github.com/VaggelisD))*
- [`e337a42`](https://github.com/tobymao/sqlglot/commit/e337a42dd56f5358e617750e7a70a0d4b7eab3f9) - **bigquery**: Parse REGEXP_SUBSTR as exp.RegexpExtract *(PR [#4358](https://github.com/tobymao/sqlglot/pull/4358) by [@VaggelisD](https://github.com/VaggelisD))*
- [`602dbf8`](https://github.com/tobymao/sqlglot/commit/602dbf84ce23f41fba6a87db70ecec6113044bac) - Support REGEXP_EXTRACT_ALL *(PR [#4359](https://github.com/tobymao/sqlglot/pull/4359) by [@VaggelisD](https://github.com/VaggelisD))*
- [`27a44a2`](https://github.com/tobymao/sqlglot/commit/27a44a22ff78cc35e8ab7c91b94311ef93d86c5a) - improve Levenshtein expression transpilation *(PR [#4360](https://github.com/tobymao/sqlglot/pull/4360) by [@krzysztof-kwitt](https://github.com/krzysztof-kwitt))*
- [`79c675a`](https://github.com/tobymao/sqlglot/commit/79c675a49fb44a6a7a97ea0de79822d8571724be) - **bigquery**: Support JSON_QUERY_ARRAY & JSON_EXTRACT_ARRAY *(PR [#4361](https://github.com/tobymao/sqlglot/pull/4361) by [@VaggelisD](https://github.com/VaggelisD))*
- [`57722db`](https://github.com/tobymao/sqlglot/commit/57722db90394d9a102c0e76a3e4d32a9f72f9ff9) - optionally wrap connectors when using builders *(PR [#4369](https://github.com/tobymao/sqlglot/pull/4369) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4362](https://github.com/tobymao/sqlglot/issues/4362) opened by [@gabrielteotonio](https://github.com/gabrielteotonio)*
  - :arrow_lower_right: *addresses issue [#4367](https://github.com/tobymao/sqlglot/issues/4367) opened by [@gabrielteotonio](https://github.com/gabrielteotonio)*

### :bug: Bug Fixes
- [`eb8e2fe`](https://github.com/tobymao/sqlglot/commit/eb8e2fe3ab3fb4b88f72843a5bd21f4a3c1d895c) - bubble up comments in qualified column refs fixes [#4353](https://github.com/tobymao/sqlglot/pull/4353) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`efcbfdb`](https://github.com/tobymao/sqlglot/commit/efcbfdb67b12853581fbfc0d4c4a450c0281849b) - **clickhouse**: Generate exp.Median as lowercase *(PR [#4355](https://github.com/tobymao/sqlglot/pull/4355) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4354](https://github.com/tobymao/sqlglot/issues/4354) opened by [@cpcloud](https://github.com/cpcloud)*
- [`60625ea`](https://github.com/tobymao/sqlglot/commit/60625eae34deb6a6fc36c0f3996f1281eae0ef6f) - **duckdb**: Fix STRUCT cast generation *(PR [#4366](https://github.com/tobymao/sqlglot/pull/4366) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4365](https://github.com/tobymao/sqlglot/issues/4365) opened by [@NickCrews](https://github.com/NickCrews)*
- [`a665030`](https://github.com/tobymao/sqlglot/commit/a665030323b200f3bed241bb928993b9807c4100) - safe removal while iterating expression list for multiple UNNEST expressions *(PR [#4364](https://github.com/tobymao/sqlglot/pull/4364) by [@gauravsagar483](https://github.com/gauravsagar483))*
- [`a71cee4`](https://github.com/tobymao/sqlglot/commit/a71cee4b4eafad9988b945c69dc75583ae105ec7) - Transpilation of exp.ArraySize from Postgres (read) *(PR [#4370](https://github.com/tobymao/sqlglot/pull/4370) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4368](https://github.com/tobymao/sqlglot/issues/4368) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`702fe31`](https://github.com/tobymao/sqlglot/commit/702fe318dadbe6cb83676e2a23ee830774697bb0) - Remove flaky timing test *(PR [#4371](https://github.com/tobymao/sqlglot/pull/4371) by [@VaggelisD](https://github.com/VaggelisD))*
- [`4d3904e`](https://github.com/tobymao/sqlglot/commit/4d3904e8906f0573f3352ad82282ea09c571daa8) - **spark**: Support DB's TIMESTAMP_DIFF *(PR [#4373](https://github.com/tobymao/sqlglot/pull/4373) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4372](https://github.com/tobymao/sqlglot/issues/4372) opened by [@nikmalviya](https://github.com/nikmalviya)*
- [`060ecfc`](https://github.com/tobymao/sqlglot/commit/060ecfc75fd8a07ffbc19f34959155a0fce317b6) - don't generate comments in table_name *(PR [#4375](https://github.com/tobymao/sqlglot/pull/4375) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`e19fb62`](https://github.com/tobymao/sqlglot/commit/e19fb620dbe6e405518aee381183e4640b638aa4) - improve error handling for unnest_to_explode *(PR [#4339](https://github.com/tobymao/sqlglot/pull/4339) by [@gauravsagar483](https://github.com/gauravsagar483))*


## [v25.29.0] - 2024-11-05
### :boom: BREAKING CHANGES
- due to [`e92904e`](https://github.com/tobymao/sqlglot/commit/e92904e61ab3b14fe18d472df19311f9b014f6cc) - Transpile ANY to EXISTS *(PR [#4305](https://github.com/tobymao/sqlglot/pull/4305) by [@VaggelisD](https://github.com/VaggelisD))*:

  Transpile ANY to EXISTS (#4305)

- due to [`23e620f`](https://github.com/tobymao/sqlglot/commit/23e620f7cd2860fbce45a5377a75ae0c8f031ce0) - Support MEDIAN() function *(PR [#4317](https://github.com/tobymao/sqlglot/pull/4317) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support MEDIAN() function (#4317)

- due to [`a093ae7`](https://github.com/tobymao/sqlglot/commit/a093ae750af8a351e54f1431deba1f2ce6843666) - always wrap value in NOT value IS ... *(PR [#4331](https://github.com/tobymao/sqlglot/pull/4331) by [@georgesittas](https://github.com/georgesittas))*:

  always wrap value in NOT value IS ... (#4331)

- due to [`84f78aa`](https://github.com/tobymao/sqlglot/commit/84f78aafd5d7e74da407167cd394d2bff0718cfb) - parse information schema views into a single identifier *(PR [#4336](https://github.com/tobymao/sqlglot/pull/4336) by [@georgesittas](https://github.com/georgesittas))*:

  parse information schema views into a single identifier (#4336)


### :sparkles: New Features
- [`efd9b4e`](https://github.com/tobymao/sqlglot/commit/efd9b4ed5a761a2ebfc47a1582e9d1b2eb7cb277) - **postgres**: Support JSONB_EXISTS *(PR [#4302](https://github.com/tobymao/sqlglot/pull/4302) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4299](https://github.com/tobymao/sqlglot/issues/4299) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`e92904e`](https://github.com/tobymao/sqlglot/commit/e92904e61ab3b14fe18d472df19311f9b014f6cc) - **spark**: Transpile ANY to EXISTS *(PR [#4305](https://github.com/tobymao/sqlglot/pull/4305) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4298](https://github.com/tobymao/sqlglot/issues/4298) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`2af4936`](https://github.com/tobymao/sqlglot/commit/2af4936bd9b318c695aae249324ff67bcd1292f6) - **snowflake**: Transpile BQ's TIMESTAMP() function *(PR [#4309](https://github.com/tobymao/sqlglot/pull/4309) by [@VaggelisD](https://github.com/VaggelisD))*
- [`50a1c91`](https://github.com/tobymao/sqlglot/commit/50a1c919d0d46384e3bd9ba1d45c24dd07efe6d2) - **snowflake**: Transpile exp.TimestampAdd *(PR [#4320](https://github.com/tobymao/sqlglot/pull/4320) by [@VaggelisD](https://github.com/VaggelisD))*
- [`01671ce`](https://github.com/tobymao/sqlglot/commit/01671ce137c9cf8d0f12dadc66e0db141f797d16) - **teradata**: add support for hexadecimal literals *(PR [#4323](https://github.com/tobymao/sqlglot/pull/4323) by [@thomascjohnson](https://github.com/thomascjohnson))*
- [`23e620f`](https://github.com/tobymao/sqlglot/commit/23e620f7cd2860fbce45a5377a75ae0c8f031ce0) - Support MEDIAN() function *(PR [#4317](https://github.com/tobymao/sqlglot/pull/4317) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4315](https://github.com/tobymao/sqlglot/issues/4315) opened by [@cpcloud](https://github.com/cpcloud)*
- [`9faef8d`](https://github.com/tobymao/sqlglot/commit/9faef8d1ceff91dd88db46b2c187d64f15490bf4) - **snowflake**: Transpile exp.TimestampSub *(PR [#4329](https://github.com/tobymao/sqlglot/pull/4329) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2d98cac`](https://github.com/tobymao/sqlglot/commit/2d98cacc723bc0c0df8ce11895983fb7cb9f5237) - **BigQuery**: Support JSON_VALUE() *(PR [#4332](https://github.com/tobymao/sqlglot/pull/4332) by [@VaggelisD](https://github.com/VaggelisD))*
- [`f8fec0a`](https://github.com/tobymao/sqlglot/commit/f8fec0ab098df37c3b54d91c24e5d8ec84f7cdbe) - **snowflake**: Transpile exp.DatetimeDiff *(PR [#4334](https://github.com/tobymao/sqlglot/pull/4334) by [@VaggelisD](https://github.com/VaggelisD))*
- [`16fd1ea`](https://github.com/tobymao/sqlglot/commit/16fd1ea2653a602bdc0d8b81e971fb1acadee585) - **BigQuery**: Support JSON_QUERY *(PR [#4333](https://github.com/tobymao/sqlglot/pull/4333) by [@VaggelisD](https://github.com/VaggelisD))*
- [`c09b6a2`](https://github.com/tobymao/sqlglot/commit/c09b6a2a37807795ead251f4fb81a9ba144cce27) - **duckdb**: support flags for RegexpExtract *(PR [#4326](https://github.com/tobymao/sqlglot/pull/4326) by [@NickCrews](https://github.com/NickCrews))*
- [`536973c`](https://github.com/tobymao/sqlglot/commit/536973cfc9d00110e388e8af1ed91d73607e07c2) - **trino**: add support for the ON OVERFLOW clause in LISTAGG *(PR [#4340](https://github.com/tobymao/sqlglot/pull/4340) by [@georgesittas](https://github.com/georgesittas))*
- [`4584935`](https://github.com/tobymao/sqlglot/commit/4584935cab328eced61c62a998cc013cab5cc3e3) - **snowflake**: Transpile exp.StrToDate *(PR [#4348](https://github.com/tobymao/sqlglot/pull/4348) by [@VaggelisD](https://github.com/VaggelisD))*
- [`71f4a47`](https://github.com/tobymao/sqlglot/commit/71f4a47910d5db97fa1a286891d72b5c4694d294) - **snowflake**: Transpile exp.DatetimeAdd *(PR [#4349](https://github.com/tobymao/sqlglot/pull/4349) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`551afff`](https://github.com/tobymao/sqlglot/commit/551afff58ea7bc1047775bfcd5d80b812fb3f682) - handle a Move edge case in the semantic differ *(PR [#4295](https://github.com/tobymao/sqlglot/pull/4295) by [@georgesittas](https://github.com/georgesittas))*
- [`a66e721`](https://github.com/tobymao/sqlglot/commit/a66e721dcd63488f7f3b427569a2115ae044c71b) - **generator**: Add NULL FILTER on ARRAY_AGG only for columns *(PR [#4301](https://github.com/tobymao/sqlglot/pull/4301) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4300](https://github.com/tobymao/sqlglot/issues/4300) opened by [@elad-sachs](https://github.com/elad-sachs)*
- [`b4ea602`](https://github.com/tobymao/sqlglot/commit/b4ea602ab17b0e8e85ddb090156c7bd2c6354de4) - **clickhouse**: improve parsing of WITH FILL ... INTERPOLATE *(PR [#4311](https://github.com/tobymao/sqlglot/pull/4311) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4310](https://github.com/tobymao/sqlglot/issues/4310) opened by [@brunorpinho](https://github.com/brunorpinho)*
- [`749886b`](https://github.com/tobymao/sqlglot/commit/749886b574a5dfa03aeb78b76d9cc097aa0f3e65) - **tsql**: Generate LOG(...) for exp.Ln *(PR [#4318](https://github.com/tobymao/sqlglot/pull/4318) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4316](https://github.com/tobymao/sqlglot/issues/4316) opened by [@cpcloud](https://github.com/cpcloud)*
- [`5c1b1f4`](https://github.com/tobymao/sqlglot/commit/5c1b1f43014967f6853752ba8d0899757a3efcd5) - **parser**: optionally parse a Stream expression *(PR [#4325](https://github.com/tobymao/sqlglot/pull/4325) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4324](https://github.com/tobymao/sqlglot/issues/4324) opened by [@lancewl](https://github.com/lancewl)*
- [`bb49a00`](https://github.com/tobymao/sqlglot/commit/bb49a00b16487356369bbb77aff9c2ff3f9cda52) - **oracle**: Do not normalize time units for exp.DateTrunc *(PR [#4328](https://github.com/tobymao/sqlglot/pull/4328) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4321](https://github.com/tobymao/sqlglot/issues/4321) opened by [@cpcloud](https://github.com/cpcloud)*
- [`a093ae7`](https://github.com/tobymao/sqlglot/commit/a093ae750af8a351e54f1431deba1f2ce6843666) - **clickhouse**: always wrap value in NOT value IS ... *(PR [#4331](https://github.com/tobymao/sqlglot/pull/4331) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4330](https://github.com/tobymao/sqlglot/issues/4330) opened by [@elchyn-cheliabiyeu](https://github.com/elchyn-cheliabiyeu)*
- [`def4f1e`](https://github.com/tobymao/sqlglot/commit/def4f1e3a9eac7545dfad223a5d49cee4fb7eeb8) - Refactor exp.RegexpExtract (follow up 4326) *(PR [#4341](https://github.com/tobymao/sqlglot/pull/4341) by [@VaggelisD](https://github.com/VaggelisD))*
- [`c1456d0`](https://github.com/tobymao/sqlglot/commit/c1456d07097c42a2ba2078ad30a8afe4cc89597d) - presto/trino current_time closes [#4344](https://github.com/tobymao/sqlglot/pull/4344) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8e16abe`](https://github.com/tobymao/sqlglot/commit/8e16abe2fed324b7ed6c718753cc623a8eb37814) - **duckdb**: we ALWAYS need to render group if params is present for RegexpExtract *(PR [#4343](https://github.com/tobymao/sqlglot/pull/4343) by [@NickCrews](https://github.com/NickCrews))*
- [`1689dc7`](https://github.com/tobymao/sqlglot/commit/1689dc7adbb913fe603b5e37eba29cc10d344cd2) - **bigquery**: Parse timezone for DATE_TRUNC *(PR [#4347](https://github.com/tobymao/sqlglot/pull/4347) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4346](https://github.com/tobymao/sqlglot/issues/4346) opened by [@CYFish](https://github.com/CYFish)*
- [`84f78aa`](https://github.com/tobymao/sqlglot/commit/84f78aafd5d7e74da407167cd394d2bff0718cfb) - **bigquery**: parse information schema views into a single identifier *(PR [#4336](https://github.com/tobymao/sqlglot/pull/4336) by [@georgesittas](https://github.com/georgesittas))*


## [v25.28.0] - 2024-10-25
### :boom: BREAKING CHANGES
- due to [`1691388`](https://github.com/tobymao/sqlglot/commit/16913887f5573f01eb8cd2b9336d4b37b84a449a) - Fix chained exp.SetOperation type annotation *(PR [#4274](https://github.com/tobymao/sqlglot/pull/4274) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix chained exp.SetOperation type annotation (#4274)

- due to [`c3c1997`](https://github.com/tobymao/sqlglot/commit/c3c199714df04edfe3698594680bac06575ca285) - Add support for STRING function *(PR [#4284](https://github.com/tobymao/sqlglot/pull/4284) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for STRING function (#4284)


### :sparkles: New Features
- [`379f487`](https://github.com/tobymao/sqlglot/commit/379f487080d95ef6e87cbbae8003541cde381ac0) - **bigquery**: transpile EDIT_DISTANCE, closes [#4283](https://github.com/tobymao/sqlglot/pull/4283) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`c3c1997`](https://github.com/tobymao/sqlglot/commit/c3c199714df04edfe3698594680bac06575ca285) - **bigquery**: Add support for STRING function *(PR [#4284](https://github.com/tobymao/sqlglot/pull/4284) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1a26bff`](https://github.com/tobymao/sqlglot/commit/1a26bff619315a6e9dc3eab4dec07746b4820796) - **snowflake**: Transpile exp.SafeDivide *(PR [#4294](https://github.com/tobymao/sqlglot/pull/4294) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`ac66d2f`](https://github.com/tobymao/sqlglot/commit/ac66d2f4b94e6a984adbf3df01139b6378248158) - **clickhouse**: properly parse CREATE FUNCTION DDLs *(PR [#4282](https://github.com/tobymao/sqlglot/pull/4282) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3276](https://github.com/TobikoData/sqlmesh/issues/3276) opened by [@jwhitaker-gridcog](https://github.com/jwhitaker-gridcog)*
- [`1691388`](https://github.com/tobymao/sqlglot/commit/16913887f5573f01eb8cd2b9336d4b37b84a449a) - **optimizer**: Fix chained exp.SetOperation type annotation *(PR [#4274](https://github.com/tobymao/sqlglot/pull/4274) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4261](https://github.com/tobymao/sqlglot/issues/4261) opened by [@gabrielteotonio](https://github.com/gabrielteotonio)*
- [`559e7bc`](https://github.com/tobymao/sqlglot/commit/559e7bc5bbc77e94dea6de0470659b3c3fa6851f) - **clickhouse**: Wrap subquery if it's LHS of IS NOT NULL *(PR [#4287](https://github.com/tobymao/sqlglot/pull/4287) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4285](https://github.com/tobymao/sqlglot/issues/4285) opened by [@EugeneTorap](https://github.com/EugeneTorap)*
- [`47bc09a`](https://github.com/tobymao/sqlglot/commit/47bc09a85a3781682f5e58bfde5f453fb1a7c50b) - **sqlite**: Fix UNIQUE parsing *(PR [#4293](https://github.com/tobymao/sqlglot/pull/4293) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4291](https://github.com/tobymao/sqlglot/issues/4291) opened by [@tshu-w](https://github.com/tshu-w)*
- [`ee266ef`](https://github.com/tobymao/sqlglot/commit/ee266ef8f92fe72252eea36b56e8825715644a4f) - improve support for identifier delimiter escaping *(PR [#4288](https://github.com/tobymao/sqlglot/pull/4288) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c6ff7f1`](https://github.com/tobymao/sqlglot/commit/c6ff7f1a0b6e443d80bc0f0ad1086d5c7b13b9f4) - bump sqlglotrs to v0.2.13 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.27.0] - 2024-10-22
### :boom: BREAKING CHANGES
- due to [`4d86499`](https://github.com/tobymao/sqlglot/commit/4d8649940d02ac319f2fec372a52674488f01de5) - include the target node for Move edits *(PR [#4277](https://github.com/tobymao/sqlglot/pull/4277) by [@georgesittas](https://github.com/georgesittas))*:

  include the target node for Move edits (#4277)

- due to [`9771965`](https://github.com/tobymao/sqlglot/commit/97719657d1b2074dabfbe54af0e1ea3acd6d4744) - Add support for TIMESTAMP_NTZ_FROM_PARTS *(PR [#4280](https://github.com/tobymao/sqlglot/pull/4280) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for TIMESTAMP_NTZ_FROM_PARTS (#4280)

- due to [`768adb3`](https://github.com/tobymao/sqlglot/commit/768adb3d85ed88931d761e5ecc8fb4a3a40d0dc5) - time string literals containing fractional seconds *(PR [#4269](https://github.com/tobymao/sqlglot/pull/4269) by [@treysp](https://github.com/treysp))*:

  time string literals containing fractional seconds (#4269)


### :sparkles: New Features
- [`9771965`](https://github.com/tobymao/sqlglot/commit/97719657d1b2074dabfbe54af0e1ea3acd6d4744) - **snowflake**: Add support for TIMESTAMP_NTZ_FROM_PARTS *(PR [#4280](https://github.com/tobymao/sqlglot/pull/4280) by [@VaggelisD](https://github.com/VaggelisD))*
- [`9e11654`](https://github.com/tobymao/sqlglot/commit/9e11654c6ebf7451f14d46c006070effe452519a) - **clickhouse**: add geometry types *(PR [#4278](https://github.com/tobymao/sqlglot/pull/4278) by [@jwhitaker-gridcog](https://github.com/jwhitaker-gridcog))*

### :bug: Bug Fixes
- [`c25a9ab`](https://github.com/tobymao/sqlglot/commit/c25a9ab577d7f0a1056e8afab680ca7801c47fff) - **tsql**: Keep CTE's attached to the query when emulating IF NOT EXISTS *(PR [#4279](https://github.com/tobymao/sqlglot/pull/4279) by [@erindru](https://github.com/erindru))*
- [`768adb3`](https://github.com/tobymao/sqlglot/commit/768adb3d85ed88931d761e5ecc8fb4a3a40d0dc5) - **clickhouse**: time string literals containing fractional seconds *(PR [#4269](https://github.com/tobymao/sqlglot/pull/4269) by [@treysp](https://github.com/treysp))*

### :recycle: Refactors
- [`4d86499`](https://github.com/tobymao/sqlglot/commit/4d8649940d02ac319f2fec372a52674488f01de5) - **diff**: include the target node for Move edits *(PR [#4277](https://github.com/tobymao/sqlglot/pull/4277) by [@georgesittas](https://github.com/georgesittas))*


## [v25.26.0] - 2024-10-21
### :boom: BREAKING CHANGES
- due to [`142c3e7`](https://github.com/tobymao/sqlglot/commit/142c3e75b25374ba9259f21b51cd728bbeb280ef) - Support TO_DOUBLE function *(PR [#4255](https://github.com/tobymao/sqlglot/pull/4255) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support TO_DOUBLE function (#4255)

- due to [`13d0696`](https://github.com/tobymao/sqlglot/commit/13d06966a2ca9264f35d5a58e1eaff1baa7dc66e) - Support TRY_TO_TIMESTAMP function *(PR [#4257](https://github.com/tobymao/sqlglot/pull/4257) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support TRY_TO_TIMESTAMP function (#4257)

- due to [`7fc0055`](https://github.com/tobymao/sqlglot/commit/7fc0055fb04713ba047baa5eda1ce0baf1cc79e2) - dont parse right-hand side operands of ARRAY JOIN as Tables *(PR [#4258](https://github.com/tobymao/sqlglot/pull/4258) by [@georgesittas](https://github.com/georgesittas))*:

  dont parse right-hand side operands of ARRAY JOIN as Tables (#4258)

- due to [`222152e`](https://github.com/tobymao/sqlglot/commit/222152e32521dbc6de3384b18ab4c677239c6088) - Add type hints for optimizer rules eliminate & merge subqueries *(PR [#4267](https://github.com/tobymao/sqlglot/pull/4267) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add type hints for optimizer rules eliminate & merge subqueries (#4267)


### :sparkles: New Features
- [`6f32e53`](https://github.com/tobymao/sqlglot/commit/6f32e5348d9aeba9c5d51a892023b2e14e072119) - support non-strict qualify_columns *(PR [#4243](https://github.com/tobymao/sqlglot/pull/4243) by [@hsheth2](https://github.com/hsheth2))*
- [`ed97954`](https://github.com/tobymao/sqlglot/commit/ed97954ecd7c2d7d4fe1bbf2ec0ecc000dd02b32) - **duckdb**: Transpile Spark's LATERAL VIEW EXPLODE *(PR [#4252](https://github.com/tobymao/sqlglot/pull/4252) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4247](https://github.com/tobymao/sqlglot/issues/4247) opened by [@idanyadgar-clutch](https://github.com/idanyadgar-clutch)*
- [`8f5efc7`](https://github.com/tobymao/sqlglot/commit/8f5efc7bc01ba5923584cd6ef38a4d81e763ccae) - **oracle**: parse hints  *(PR [#4249](https://github.com/tobymao/sqlglot/pull/4249) by [@mkmoisen](https://github.com/mkmoisen))*
- [`8b7ff5e`](https://github.com/tobymao/sqlglot/commit/8b7ff5ee8713a3ba50c48addd3700927a0240cf5) - **starrocks**: support for ALTER TABLE SWAP WITH *(PR [#4256](https://github.com/tobymao/sqlglot/pull/4256) by [@mrhamburg](https://github.com/mrhamburg))*
- [`1c43348`](https://github.com/tobymao/sqlglot/commit/1c433487a45379298ef27b3688723df2bd740fd1) - **trino**: Support for LISTAGG function *(PR [#4253](https://github.com/tobymao/sqlglot/pull/4253) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4250](https://github.com/tobymao/sqlglot/issues/4250) opened by [@npochhi](https://github.com/npochhi)*
- [`142c3e7`](https://github.com/tobymao/sqlglot/commit/142c3e75b25374ba9259f21b51cd728bbeb280ef) - **snowflake**: Support TO_DOUBLE function *(PR [#4255](https://github.com/tobymao/sqlglot/pull/4255) by [@VaggelisD](https://github.com/VaggelisD))*
- [`13d0696`](https://github.com/tobymao/sqlglot/commit/13d06966a2ca9264f35d5a58e1eaff1baa7dc66e) - **snowflake**: Support TRY_TO_TIMESTAMP function *(PR [#4257](https://github.com/tobymao/sqlglot/pull/4257) by [@VaggelisD](https://github.com/VaggelisD))*
- [`04dccf3`](https://github.com/tobymao/sqlglot/commit/04dccf3cdaf1c3a0466dda113aba5439f1639ae0) - **tsql**: Support for stored procedure options *(PR [#4260](https://github.com/tobymao/sqlglot/pull/4260) by [@rsanchez-xtillion](https://github.com/rsanchez-xtillion))*
- [`36f6841`](https://github.com/tobymao/sqlglot/commit/36f68416b3dd0d9ac703dd926d1f74bc43566e0d) - **bigquery**: support EDIT_DISTANCE (Levinshtein) function *(PR [#4276](https://github.com/tobymao/sqlglot/pull/4276) by [@esciara](https://github.com/esciara))*
  - :arrow_lower_right: *addresses issue [#4275](https://github.com/tobymao/sqlglot/issues/4275) opened by [@esciara](https://github.com/esciara)*

### :bug: Bug Fixes
- [`fcc05c9`](https://github.com/tobymao/sqlglot/commit/fcc05c9daa31c7a51474ec9c72ceafd682359f90) - **bigquery**: Early expand only aliased names in GROUP BY *(PR [#4246](https://github.com/tobymao/sqlglot/pull/4246) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5655cfb`](https://github.com/tobymao/sqlglot/commit/5655cfba7afdf8f95dea53d5ededfde209b77c30) - add support for negative intervals in to_interval *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`51f4d26`](https://github.com/tobymao/sqlglot/commit/51f4d26ed8694365c61fdefd810a420fcfefdeca) - generate single argument ArrayConcat without trailing comma fixes [#4259](https://github.com/tobymao/sqlglot/pull/4259) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`7fc0055`](https://github.com/tobymao/sqlglot/commit/7fc0055fb04713ba047baa5eda1ce0baf1cc79e2) - **clickhouse**: dont parse right-hand side operands of ARRAY JOIN as Tables *(PR [#4258](https://github.com/tobymao/sqlglot/pull/4258) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4254](https://github.com/tobymao/sqlglot/issues/4254) opened by [@xtess16](https://github.com/xtess16)*
- [`8f49ad8`](https://github.com/tobymao/sqlglot/commit/8f49ad87fa795349183d13129110ad59387bfe11) - **clickhouse**: traverse_scope with FINAL modifier *(PR [#4263](https://github.com/tobymao/sqlglot/pull/4263) by [@pkit](https://github.com/pkit))*
  - :arrow_lower_right: *fixes issue [#4262](https://github.com/tobymao/sqlglot/issues/4262) opened by [@obazna](https://github.com/obazna)*
- [`83167ea`](https://github.com/tobymao/sqlglot/commit/83167eaa3039195f756c7b1ad95fc9162f19b1b1) - hive dialect hierarchy has no CURRENT_TIME func *(PR [#4264](https://github.com/tobymao/sqlglot/pull/4264) by [@georgesittas](https://github.com/georgesittas))*
- [`7a5c7e0`](https://github.com/tobymao/sqlglot/commit/7a5c7e036fa84eb30bcae75829f3cb94503fa99e) - **presto**: transpile BIT to BOOLEAN *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`48be3d8`](https://github.com/tobymao/sqlglot/commit/48be3d89b1df96c7b8d81536862f53a98e414f11) - make the semantic diffing aware of changes to non-expression leaves *(PR [#4268](https://github.com/tobymao/sqlglot/pull/4268) by [@georgesittas](https://github.com/georgesittas))*
- [`4543fb3`](https://github.com/tobymao/sqlglot/commit/4543fb3cd052dfb20428f5a6254b38def9e756ee) - **optimizer**: Fix merge_subqueries.py::rename_inner_sources() *(PR [#4266](https://github.com/tobymao/sqlglot/pull/4266) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4245](https://github.com/tobymao/sqlglot/issues/4245) opened by [@daniel769](https://github.com/daniel769)*
- [`222152e`](https://github.com/tobymao/sqlglot/commit/222152e32521dbc6de3384b18ab4c677239c6088) - **optimizer**: Add type hints for optimizer rules eliminate & merge subqueries *(PR [#4267](https://github.com/tobymao/sqlglot/pull/4267) by [@VaggelisD](https://github.com/VaggelisD))*

### :recycle: Refactors
- [`94013a2`](https://github.com/tobymao/sqlglot/commit/94013a21ca69b90da78dc47b16cd86503736597a) - simplify _expression_only_args helper in diff module *(PR [#4251](https://github.com/tobymao/sqlglot/pull/4251) by [@georgesittas](https://github.com/georgesittas))*
- [`41e2eba`](https://github.com/tobymao/sqlglot/commit/41e2eba1a01c1a5b784ad9dc6c5191f3d3bc0d74) - **Oracle**: simplify hint arg formatting *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`cfd692f`](https://github.com/tobymao/sqlglot/commit/cfd692ff28a59f413671aafbc8dcd61eab3558c3) - move SwapTable logic to the base Parser/Generator classes *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.25.1] - 2024-10-15
### :bug: Bug Fixes
- [`e6567ae`](https://github.com/tobymao/sqlglot/commit/e6567ae11650834874808a844a19836fbb9ee753) - small overload fix for ensure list taking None *(PR [#4248](https://github.com/tobymao/sqlglot/pull/4248) by [@benfdking](https://github.com/benfdking))*


## [v25.25.0] - 2024-10-14
### :boom: BREAKING CHANGES
- due to [`275b64b`](https://github.com/tobymao/sqlglot/commit/275b64b6a28722232a24870e443b249994220d54) - refactor set operation builders so they can work with N expressions *(PR [#4226](https://github.com/tobymao/sqlglot/pull/4226) by [@georgesittas](https://github.com/georgesittas))*:

  refactor set operation builders so they can work with N expressions (#4226)

- due to [`aee76da`](https://github.com/tobymao/sqlglot/commit/aee76da1cadec242f7428d23999f1752cb0708ca) - Native annotations for string functions *(PR [#4231](https://github.com/tobymao/sqlglot/pull/4231) by [@VaggelisD](https://github.com/VaggelisD))*:

  Native annotations for string functions (#4231)

- due to [`202aaa0`](https://github.com/tobymao/sqlglot/commit/202aaa0e7390142ee3ade41c28e2e77cde31f295) - Native annotations for string functions *(PR [#4234](https://github.com/tobymao/sqlglot/pull/4234) by [@VaggelisD](https://github.com/VaggelisD))*:

  Native annotations for string functions (#4234)

- due to [`5741180`](https://github.com/tobymao/sqlglot/commit/5741180e895eaaa75a07af388d36a0d2df97b28c) - produce exp.Column for the RHS of <value> IN <name> *(PR [#4239](https://github.com/tobymao/sqlglot/pull/4239) by [@georgesittas](https://github.com/georgesittas))*:

  produce exp.Column for the RHS of <value> IN <name> (#4239)

- due to [`4da2502`](https://github.com/tobymao/sqlglot/commit/4da25029b1c6f1425b4602f42da4fa1bcd3fccdb) - make Explode a UDTF subclass *(PR [#4242](https://github.com/tobymao/sqlglot/pull/4242) by [@georgesittas](https://github.com/georgesittas))*:

  make Explode a UDTF subclass (#4242)


### :sparkles: New Features
- [`163e943`](https://github.com/tobymao/sqlglot/commit/163e943cdaf449599640c198f69e73d2398eb323) - **tsql**: SPLIT_PART function and conversion to PARSENAME in tsql *(PR [#4211](https://github.com/tobymao/sqlglot/pull/4211) by [@daihuynh](https://github.com/daihuynh))*
- [`275b64b`](https://github.com/tobymao/sqlglot/commit/275b64b6a28722232a24870e443b249994220d54) - refactor set operation builders so they can work with N expressions *(PR [#4226](https://github.com/tobymao/sqlglot/pull/4226) by [@georgesittas](https://github.com/georgesittas))*
- [`3f6ba3e`](https://github.com/tobymao/sqlglot/commit/3f6ba3e69c9ba92429d2b3b00cac33f45518aa56) - **clickhouse**: Support varlen arrays for ARRAY JOIN *(PR [#4229](https://github.com/tobymao/sqlglot/pull/4229) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4227](https://github.com/tobymao/sqlglot/issues/4227) opened by [@brunorpinho](https://github.com/brunorpinho)*
- [`aee76da`](https://github.com/tobymao/sqlglot/commit/aee76da1cadec242f7428d23999f1752cb0708ca) - **bigquery**: Native annotations for string functions *(PR [#4231](https://github.com/tobymao/sqlglot/pull/4231) by [@VaggelisD](https://github.com/VaggelisD))*
- [`202aaa0`](https://github.com/tobymao/sqlglot/commit/202aaa0e7390142ee3ade41c28e2e77cde31f295) - **bigquery**: Native annotations for string functions *(PR [#4234](https://github.com/tobymao/sqlglot/pull/4234) by [@VaggelisD](https://github.com/VaggelisD))*
- [`eeae25e`](https://github.com/tobymao/sqlglot/commit/eeae25e03a883671f9d5e514f9bd3021fb6c0d32) - support EXPLAIN in mysql *(PR [#4235](https://github.com/tobymao/sqlglot/pull/4235) by [@xiaoyu-meng-mxy](https://github.com/xiaoyu-meng-mxy))*
- [`06748d9`](https://github.com/tobymao/sqlglot/commit/06748d93ccd232528003c37fdda25ae8163f3c18) - **mysql**: add support for operation modifiers like HIGH_PRIORITY *(PR [#4238](https://github.com/tobymao/sqlglot/pull/4238) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4236](https://github.com/tobymao/sqlglot/issues/4236) opened by [@asdfsx](https://github.com/asdfsx)*

### :bug: Bug Fixes
- [`dcdec95`](https://github.com/tobymao/sqlglot/commit/dcdec95f986426ae90469baca993b47ac390081b) - Make exp.Update a DML node *(PR [#4223](https://github.com/tobymao/sqlglot/pull/4223) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4221](https://github.com/tobymao/sqlglot/issues/4221) opened by [@rahul-ve](https://github.com/rahul-ve)*
- [`79caf51`](https://github.com/tobymao/sqlglot/commit/79caf519987718390a086bee19fdc89f6094496c) - **clickhouse**: rename BOOLEAN type to Bool fixes [#4230](https://github.com/tobymao/sqlglot/pull/4230) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b26a3f6`](https://github.com/tobymao/sqlglot/commit/b26a3f67b7113802ba1b4b3b211431e98258dc15) - satisfy mypy *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`5741180`](https://github.com/tobymao/sqlglot/commit/5741180e895eaaa75a07af388d36a0d2df97b28c) - **parser**: produce exp.Column for the RHS of <value> IN <name> *(PR [#4239](https://github.com/tobymao/sqlglot/pull/4239) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4237](https://github.com/tobymao/sqlglot/issues/4237) opened by [@rustyconover](https://github.com/rustyconover)*
- [`daa6e78`](https://github.com/tobymao/sqlglot/commit/daa6e78e4b810eff826f995aa52f9e38197f1b7e) - **optimizer**: handle subquery predicate substitution correctly in de morgan's rule *(PR [#4240](https://github.com/tobymao/sqlglot/pull/4240) by [@georgesittas](https://github.com/georgesittas))*
- [`c0a8355`](https://github.com/tobymao/sqlglot/commit/c0a83556acffcd77521f69bf51503a07310f749d) - **parser**: parse a column reference for the RHS of the IN clause *(PR [#4241](https://github.com/tobymao/sqlglot/pull/4241) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`0882f03`](https://github.com/tobymao/sqlglot/commit/0882f03d526f593b2d415e85b7d7a7c113721806) - Rename exp.RenameTable to exp.AlterRename *(PR [#4224](https://github.com/tobymao/sqlglot/pull/4224) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4222](https://github.com/tobymao/sqlglot/issues/4222) opened by [@s1101010110](https://github.com/s1101010110)*
- [`fd42b5c`](https://github.com/tobymao/sqlglot/commit/fd42b5cdaf9421abb11e71d82726536af09e3ae3) - Simplify PARSENAME <-> SPLIT_PART transpilation *(PR [#4225](https://github.com/tobymao/sqlglot/pull/4225) by [@VaggelisD](https://github.com/VaggelisD))*
- [`4da2502`](https://github.com/tobymao/sqlglot/commit/4da25029b1c6f1425b4602f42da4fa1bcd3fccdb) - make Explode a UDTF subclass *(PR [#4242](https://github.com/tobymao/sqlglot/pull/4242) by [@georgesittas](https://github.com/georgesittas))*


## [v25.24.5] - 2024-10-08
### :sparkles: New Features
- [`22a1684`](https://github.com/tobymao/sqlglot/commit/22a16848d80a2fa6d310f99d21f7d81f90eb9440) - **bigquery**: Native annotations for more math functions *(PR [#4212](https://github.com/tobymao/sqlglot/pull/4212) by [@VaggelisD](https://github.com/VaggelisD))*
- [`354cfff`](https://github.com/tobymao/sqlglot/commit/354cfff13ab30d01c6123fca74eed0669d238aa0) - add builder methods to exp.Update and add with_ arg to exp.update *(PR [#4217](https://github.com/tobymao/sqlglot/pull/4217) by [@brdbry](https://github.com/brdbry))*

### :bug: Bug Fixes
- [`2c513b7`](https://github.com/tobymao/sqlglot/commit/2c513b71c7d4b1ff5c7c4e12d6c38694210b1a12) - Attach CTE comments before commas *(PR [#4218](https://github.com/tobymao/sqlglot/pull/4218) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4216](https://github.com/tobymao/sqlglot/issues/4216) opened by [@ajfriend](https://github.com/ajfriend)*


## [v25.24.4] - 2024-10-04
### :bug: Bug Fixes
- [`484df7d`](https://github.com/tobymao/sqlglot/commit/484df7d50df5cb314943e1810db18a7d7d5bb3eb) - tsql union with limit *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.24.3] - 2024-10-03
### :sparkles: New Features
- [`25b18d2`](https://github.com/tobymao/sqlglot/commit/25b18d28e5ad7b3687e2848ff92a0a1fc17b06fa) - **trino**: Support JSON_QUERY *(PR [#4206](https://github.com/tobymao/sqlglot/pull/4206) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4200](https://github.com/tobymao/sqlglot/issues/4200) opened by [@Harmuth94](https://github.com/Harmuth94)*
- [`5781b45`](https://github.com/tobymao/sqlglot/commit/5781b455fa3ec495b65f3f3f4a959192389bd816) - **duckdb**: Add more Postgres operators *(PR [#4199](https://github.com/tobymao/sqlglot/pull/4199) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4189](https://github.com/tobymao/sqlglot/issues/4189) opened by [@rustyconover](https://github.com/rustyconover)*
- [`89c0703`](https://github.com/tobymao/sqlglot/commit/89c07039da402fb2ad77e00edb4f09079ecbb41d) - **bigquery**: Native math function annotations *(PR [#4201](https://github.com/tobymao/sqlglot/pull/4201) by [@VaggelisD](https://github.com/VaggelisD))*
- [`977d9e5`](https://github.com/tobymao/sqlglot/commit/977d9e5a854b58b4469be1af6aa14a5bf5a4b8c6) - allow supplying dialect in diff, conditionally copy ASTs *(PR [#4208](https://github.com/tobymao/sqlglot/pull/4208) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4203](https://github.com/tobymao/sqlglot/issues/4203) opened by [@mkmoisen](https://github.com/mkmoisen)*

### :bug: Bug Fixes
- [`332c74b`](https://github.com/tobymao/sqlglot/commit/332c74b881487cd9ce711ca3bd065a8992872098) - attach comments to subquery predicates properly, fix comment case *(PR [#4207](https://github.com/tobymao/sqlglot/pull/4207) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4205](https://github.com/tobymao/sqlglot/issues/4205) opened by [@mkmoisen](https://github.com/mkmoisen)*
- [`55da21d`](https://github.com/tobymao/sqlglot/commit/55da21dd043dfcbefa3653fe168eb9cae5dc5bf5) - Unexpected row deduplication using eliminate_full_outer_join *(PR [#4178](https://github.com/tobymao/sqlglot/pull/4178) by [@liaco](https://github.com/liaco))*


## [v25.24.2] - 2024-10-02
### :sparkles: New Features
- [`c8b7c1e`](https://github.com/tobymao/sqlglot/commit/c8b7c1ef7c6070a51638af18833c649a77e735cb) - **optimizer**: Fixture file for function annotations *(PR [#4182](https://github.com/tobymao/sqlglot/pull/4182) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0adbbf7`](https://github.com/tobymao/sqlglot/commit/0adbbf7ad8f16700adc48c6757c07768199860d9) - **duckdb**: Parse ** and ^ operators as POW *(PR [#4193](https://github.com/tobymao/sqlglot/pull/4193) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4186](https://github.com/tobymao/sqlglot/issues/4186) opened by [@rustyconover](https://github.com/rustyconover)*
- [`4949906`](https://github.com/tobymao/sqlglot/commit/4949906e9dd0c3039a161e06ddb970f37067b88f) - **duckdb**: Parse ~~~ as GLOB *(PR [#4194](https://github.com/tobymao/sqlglot/pull/4194) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4187](https://github.com/tobymao/sqlglot/issues/4187) opened by [@rustyconover](https://github.com/rustyconover)*
- [`6ba2bb0`](https://github.com/tobymao/sqlglot/commit/6ba2bb03f973c30788508768c3ba716aa94b0299) - **oracle**: Add support for BULK COLLECT INTO *(PR [#4181](https://github.com/tobymao/sqlglot/pull/4181) by [@mkmoisen](https://github.com/mkmoisen))*
- [`0de59ce`](https://github.com/tobymao/sqlglot/commit/0de59cebe550b33ac34a92c1ded1d3f9b8f679c4) - mark `expressions` as unsupported in Into generator *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`475f7a3`](https://github.com/tobymao/sqlglot/commit/475f7a3c639c7b8c5f3af1b2e5fcce9174be39ec) - **redshift**: Add unsupported warnings for UNNEST *(PR [#4173](https://github.com/tobymao/sqlglot/pull/4173) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4169](https://github.com/tobymao/sqlglot/issues/4169) opened by [@bjabes](https://github.com/bjabes)*
- [`d38e023`](https://github.com/tobymao/sqlglot/commit/d38e023966c32b208fe5ae9843bbd716e2181521) - **spark**: Offset TRY_ELEMENT_AT by one *(PR [#4183](https://github.com/tobymao/sqlglot/pull/4183) by [@VaggelisD](https://github.com/VaggelisD))*
- [`3d1c643`](https://github.com/tobymao/sqlglot/commit/3d1c6430791dcce05f1a71f17311e294d9fc9d3d) - rename SHA function to SHA1 for DuckDB *(PR [#4191](https://github.com/tobymao/sqlglot/pull/4191) by [@rustyconover](https://github.com/rustyconover))*
- [`0388a51`](https://github.com/tobymao/sqlglot/commit/0388a519dba63636a9aac3e3272cdea0f0b8312d) - add support for UHUGEINT for duckdb *(PR [#4190](https://github.com/tobymao/sqlglot/pull/4190) by [@rustyconover](https://github.com/rustyconover))*
  - :arrow_lower_right: *fixes issue [#4184](https://github.com/tobymao/sqlglot/issues/4184) opened by [@rustyconover](https://github.com/rustyconover)*
- [`9eba00d`](https://github.com/tobymao/sqlglot/commit/9eba00dca517efe7df171b09ed916af3ea5e350d) - **duckdb**: Parse ~~ as LIKE *(PR [#4195](https://github.com/tobymao/sqlglot/pull/4195) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4188](https://github.com/tobymao/sqlglot/issues/4188) opened by [@rustyconover](https://github.com/rustyconover)*
- [`6a65973`](https://github.com/tobymao/sqlglot/commit/6a659736f3a176e335c68fdd07d8265c3d0421dc) - expand UPDATABLE_EXPRESSION_TYPES to account for Identifier changes *(PR [#4197](https://github.com/tobymao/sqlglot/pull/4197) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4192](https://github.com/tobymao/sqlglot/issues/4192) opened by [@mkmoisen](https://github.com/mkmoisen)*
- [`a6c28c6`](https://github.com/tobymao/sqlglot/commit/a6c28c63f4e44bb62ba8df30f1407c728eb215f2) - **sqlite**: generate StrPosition as INSTR *(PR [#4198](https://github.com/tobymao/sqlglot/pull/4198) by [@pruzko](https://github.com/pruzko))*
  - :arrow_lower_right: *fixes issue [#4196](https://github.com/tobymao/sqlglot/issues/4196) opened by [@pruzko](https://github.com/pruzko)*
- [`5a123a5`](https://github.com/tobymao/sqlglot/commit/5a123a54ecd033c0a104e33476b17d816a09caac) - **oracle**: retreat properly when parsing BULK COLLECT INTO *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`f935e42`](https://github.com/tobymao/sqlglot/commit/f935e42130724e032b294074f3b552f21e20bc57) - properly escape closing identifier delimiters *(PR [#4202](https://github.com/tobymao/sqlglot/pull/4202) by [@georgesittas](https://github.com/georgesittas))*


## [v25.24.1] - 2024-10-01
### :sparkles: New Features
- [`7af33a2`](https://github.com/tobymao/sqlglot/commit/7af33a2f74dd1300bcd45f1974b7fd28abe66b8e) - **spark**: Custom annotation for more string functions *(PR [#4156](https://github.com/tobymao/sqlglot/pull/4156) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`81df4e1`](https://github.com/tobymao/sqlglot/commit/81df4e104ff3d60e3c23d3ac321e719b1f0962c0) - **athena**: Case sensitivity in CTAS property names *(PR [#4171](https://github.com/tobymao/sqlglot/pull/4171) by [@erindru](https://github.com/erindru))*
- [`0703152`](https://github.com/tobymao/sqlglot/commit/0703152a25afced183dc5efd5f62311a48545420) - **bigquery**: Do not generate null ordering on agg funcs *(PR [#4172](https://github.com/tobymao/sqlglot/pull/4172) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4170](https://github.com/tobymao/sqlglot/issues/4170) opened by [@yjabri](https://github.com/yjabri)*


## [v25.24.0] - 2024-09-26
### :boom: BREAKING CHANGES
- due to [`3ab6dfb`](https://github.com/tobymao/sqlglot/commit/3ab6dfb486f18d036bfac6a90d5f81b0ce7a91ea) - Generalize COLUMNS(...) APPLY *(PR [#4161](https://github.com/tobymao/sqlglot/pull/4161) by [@VaggelisD](https://github.com/VaggelisD))*:

  Generalize COLUMNS(...) APPLY (#4161)


### :sparkles: New Features
- [`93cef30`](https://github.com/tobymao/sqlglot/commit/93cef30bc534a155bce06f35d441d20e5dd78cf6) - **postgres**: Support OVERLAY function *(PR [#4165](https://github.com/tobymao/sqlglot/pull/4165) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4159](https://github.com/tobymao/sqlglot/issues/4159) opened by [@s1101010110](https://github.com/s1101010110)*
- [`0a5444d`](https://github.com/tobymao/sqlglot/commit/0a5444dc822b7c53c008bc946eb3b54ca2147f3c) - expose a flag to automatically exclude Keep diff nodes *(PR [#4168](https://github.com/tobymao/sqlglot/pull/4168) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`9c17264`](https://github.com/tobymao/sqlglot/commit/9c172643aa3f3f0ffcc2e62242b62ba9c6141925) - **hive**: Enclose exp.Split with \E *(PR [#4163](https://github.com/tobymao/sqlglot/pull/4163) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4158](https://github.com/tobymao/sqlglot/issues/4158) opened by [@cpcloud](https://github.com/cpcloud)*
- [`3ab6dfb`](https://github.com/tobymao/sqlglot/commit/3ab6dfb486f18d036bfac6a90d5f81b0ce7a91ea) - **clickhouse**: Generalize COLUMNS(...) APPLY *(PR [#4161](https://github.com/tobymao/sqlglot/pull/4161) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4157](https://github.com/tobymao/sqlglot/issues/4157) opened by [@elchyn-cheliabiyeu](https://github.com/elchyn-cheliabiyeu)*

### :recycle: Refactors
- [`2540e50`](https://github.com/tobymao/sqlglot/commit/2540e50d2b0df12f940c68acc574e540d19546cf) - simplify check_deploy job *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`f6d3bdd`](https://github.com/tobymao/sqlglot/commit/f6d3bdd740d0fe128d4d5dd99833a6f71c890ed3) - update supported dialect count (21 -> 23) *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.23.2] - 2024-09-25
### :wrench: Chores
- [`eca05d3`](https://github.com/tobymao/sqlglot/commit/eca05d3b08645d7a984ee65b438282b35cb41960) - tweak should_deploy_rs script to avoid marking CI as failed *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.23.1] - 2024-09-25
### :wrench: Chores
- [`349b8f8`](https://github.com/tobymao/sqlglot/commit/349b8f81ed69a3708e1afd15816b3b58e2bf8b3f) - fetch all history to allow workflow script to skip sqlglotrs deployments *(PR [#4162](https://github.com/tobymao/sqlglot/pull/4162) by [@georgesittas](https://github.com/georgesittas))*


## [v25.23.0] - 2024-09-25
### :boom: BREAKING CHANGES
- due to [`da51ea5`](https://github.com/tobymao/sqlglot/commit/da51ea5c8f405859d877a25176e8e48ef8b4b112) - refactor exp.Chr *(PR [#4081](https://github.com/tobymao/sqlglot/pull/4081) by [@georgesittas](https://github.com/georgesittas))*:

  refactor exp.Chr (#4081)

- due to [`9c527b5`](https://github.com/tobymao/sqlglot/commit/9c527b549cc56db9d8f44579397d9f9fe1440573) - treat Nullable as an arg instead of a DataType.TYPE *(PR [#4094](https://github.com/tobymao/sqlglot/pull/4094) by [@georgesittas](https://github.com/georgesittas))*:

  treat Nullable as an arg instead of a DataType.TYPE (#4094)

- due to [`ba015dc`](https://github.com/tobymao/sqlglot/commit/ba015dc1102a4fe0c35cbfe6e3d23dc24263c20f) - add `returning` to merge expression builder *(PR [#4125](https://github.com/tobymao/sqlglot/pull/4125) by [@max-muoto](https://github.com/max-muoto))*:

  add `returning` to merge expression builder (#4125)

- due to [`77a514d`](https://github.com/tobymao/sqlglot/commit/77a514dd7cfa9feb847c429411809092e5578bad) - Parse VALUES & query modifiers in wrapped FROM clause *(PR [#4135](https://github.com/tobymao/sqlglot/pull/4135) by [@VaggelisD](https://github.com/VaggelisD))*:

  Parse VALUES & query modifiers in wrapped FROM clause (#4135)


### :sparkles: New Features
- [`5771d8d`](https://github.com/tobymao/sqlglot/commit/5771d8da94aff104206f93482e7b248d725f1843) - add merge expression builder *(PR [#4084](https://github.com/tobymao/sqlglot/pull/4084) by [@max-muoto](https://github.com/max-muoto))*
- [`1d52709`](https://github.com/tobymao/sqlglot/commit/1d5270915d14f3f92341d5057b88b58fff6c0d97) - **postgres**: Parse DO NOTHING and RETURNING in MERGE statement *(PR [#4087](https://github.com/tobymao/sqlglot/pull/4087) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4083](https://github.com/tobymao/sqlglot/issues/4083) opened by [@max-muoto](https://github.com/max-muoto)*
- [`1615bad`](https://github.com/tobymao/sqlglot/commit/1615bad98eeddc8e67e8002c3b1fe93bd7c3b690) - Add support for UUID function *(PR [#4089](https://github.com/tobymao/sqlglot/pull/4089) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5733600`](https://github.com/tobymao/sqlglot/commit/57336006795d32e9253a9df4813d3029d1d32ef1) - **bigquery**: transpile UUID type to STRING *(PR [#4093](https://github.com/tobymao/sqlglot/pull/4093) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4091](https://github.com/tobymao/sqlglot/issues/4091) opened by [@gigatexal](https://github.com/gigatexal)*
- [`75230f5`](https://github.com/tobymao/sqlglot/commit/75230f5970c240add1cff7349fc65fb67541fa34) - **bigquery**: add support for the MERGE ... THEN INSERT ROW syntax *(PR [#4096](https://github.com/tobymao/sqlglot/pull/4096) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4095](https://github.com/tobymao/sqlglot/issues/4095) opened by [@ericist](https://github.com/ericist)*
- [`f8d4dc4`](https://github.com/tobymao/sqlglot/commit/f8d4dc4bab90cd369eef090c23b81160a7ae78fc) - **parser**: add support for ALTER INDEX closes [#4105](https://github.com/tobymao/sqlglot/pull/4105) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`28c6f27`](https://github.com/tobymao/sqlglot/commit/28c6f27291d57d85917c62b387b86a598ee3c1d6) - **duckdb**: Support *COLUMNS() function *(PR [#4106](https://github.com/tobymao/sqlglot/pull/4106) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4101](https://github.com/tobymao/sqlglot/issues/4101) opened by [@aersam](https://github.com/aersam)*
- [`3cb0041`](https://github.com/tobymao/sqlglot/commit/3cb00417f45624f012e5ce8ababfe3250e813b80) - **snowflake**: Fix exp.Pivot FOR IN clause *(PR [#4109](https://github.com/tobymao/sqlglot/pull/4109) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4108](https://github.com/tobymao/sqlglot/issues/4108) opened by [@kharigardner](https://github.com/kharigardner)*
- [`7ac21a0`](https://github.com/tobymao/sqlglot/commit/7ac21a07c73cdf156ae4dc6a848b9f781b265d16) - **athena**: Improve DDL query support *(PR [#4099](https://github.com/tobymao/sqlglot/pull/4099) by [@erindru](https://github.com/erindru))*
- [`a34f8b6`](https://github.com/tobymao/sqlglot/commit/a34f8b6ff9b0aa8595214da75fe7cbfbc8285476) - **oracle**: support TRUNC without fmt argument fixes [#4116](https://github.com/tobymao/sqlglot/pull/4116) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`bd8e050`](https://github.com/tobymao/sqlglot/commit/bd8e050e7df905082fd32e26c4ee0c6e2d36c897) - **clickhouse**: support ON CLUSTER clause in DELETE *(PR [#4119](https://github.com/tobymao/sqlglot/pull/4119) by [@treysp](https://github.com/treysp))*
- [`1fac6a9`](https://github.com/tobymao/sqlglot/commit/1fac6a9f46e147a5583042d6f82deffd04cd58c9) - expose sqlglot.expressions.delete as a sqlglot module function *(PR [#4126](https://github.com/tobymao/sqlglot/pull/4126) by [@max-muoto](https://github.com/max-muoto))*
- [`4506b3b`](https://github.com/tobymao/sqlglot/commit/4506b3b58fde8e8fe711df8fd0c9c245a98ca86b) - **duckdb**: add support for the UNION type *(PR [#4128](https://github.com/tobymao/sqlglot/pull/4128) by [@georgesittas](https://github.com/georgesittas))*
- [`ba015dc`](https://github.com/tobymao/sqlglot/commit/ba015dc1102a4fe0c35cbfe6e3d23dc24263c20f) - add `returning` to merge expression builder *(PR [#4125](https://github.com/tobymao/sqlglot/pull/4125) by [@max-muoto](https://github.com/max-muoto))*
- [`3ec96ab`](https://github.com/tobymao/sqlglot/commit/3ec96ab7318dc5fc07802d31c825a95db7f5b303) - **clickhouse**: Add support for APPLY query modifier *(PR [#4141](https://github.com/tobymao/sqlglot/pull/4141) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4139](https://github.com/tobymao/sqlglot/issues/4139) opened by [@elchyn-cheliabiyeu](https://github.com/elchyn-cheliabiyeu)*
- [`04ddc54`](https://github.com/tobymao/sqlglot/commit/04ddc543159bc55e2cf8098cd96b2a5c881ebbc6) - **bigquery**: Support RANGE<T> type *(PR [#4148](https://github.com/tobymao/sqlglot/pull/4148) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4146](https://github.com/tobymao/sqlglot/issues/4146) opened by [@plaflamme](https://github.com/plaflamme)*
- [`17533ee`](https://github.com/tobymao/sqlglot/commit/17533ee6361c30731a9a14666ac952b44982b69d) - Add support for GRANT DDL *(PR [#4138](https://github.com/tobymao/sqlglot/pull/4138) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1a240ec`](https://github.com/tobymao/sqlglot/commit/1a240ec1cbdaf15abab8df642e189b89de239e84) - Add SUBSTR Support *(PR [#4153](https://github.com/tobymao/sqlglot/pull/4153) by [@mwc360](https://github.com/mwc360))*

### :bug: Bug Fixes
- [`da51ea5`](https://github.com/tobymao/sqlglot/commit/da51ea5c8f405859d877a25176e8e48ef8b4b112) - **parser**: refactor exp.Chr *(PR [#4081](https://github.com/tobymao/sqlglot/pull/4081) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4080](https://github.com/tobymao/sqlglot/issues/4080) opened by [@EugeneTorap](https://github.com/EugeneTorap)*
- [`6294f9e`](https://github.com/tobymao/sqlglot/commit/6294f9e6d08111c6088f5ed9846e7a64f7724801) - **starrocks**: generate ARRAY_FILTER for exp.ArrayFilter *(PR [#4088](https://github.com/tobymao/sqlglot/pull/4088) by [@gauravsagar483](https://github.com/gauravsagar483))*
- [`1e02c02`](https://github.com/tobymao/sqlglot/commit/1e02c0221ea8445ccb5537b0a77e120c0b2c108c) - **mysql**: convert VARCHAR without size to TEXT for DDLs *(PR [#4092](https://github.com/tobymao/sqlglot/pull/4092) by [@georgesittas](https://github.com/georgesittas))*
- [`cb5bcff`](https://github.com/tobymao/sqlglot/commit/cb5bcfff1f96972e75681bb2411bca8b60a4bff1) - **clickhouse**: generate formatDateTime instead of DATE_FORMAT fixes [#4098](https://github.com/tobymao/sqlglot/pull/4098) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b10255e`](https://github.com/tobymao/sqlglot/commit/b10255eb8b6b73bf5084fdf6bffd5a7fa351b1ec) - **snowflake**: Manually escape single quotes in colon operator *(PR [#4104](https://github.com/tobymao/sqlglot/pull/4104) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4090](https://github.com/tobymao/sqlglot/issues/4090) opened by [@jussihe-rec](https://github.com/jussihe-rec)*
- [`67a9ad8`](https://github.com/tobymao/sqlglot/commit/67a9ad89abfce84f78dd1a34caa9dc8143233609) - Move JSON path escape to generation *(PR [#4110](https://github.com/tobymao/sqlglot/pull/4110) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4090](https://github.com/tobymao/sqlglot/issues/4090) opened by [@jussihe-rec](https://github.com/jussihe-rec)*
- [`06c76f7`](https://github.com/tobymao/sqlglot/commit/06c76f7471cdb679a7a7a35064204d94841fd929) - calling interval without unit *(commit by [@tobymao](https://github.com/tobymao))*
- [`66c3295`](https://github.com/tobymao/sqlglot/commit/66c32958a9e46642077813adf90079098e41c87e) - **optimizer**: Enable USING expansion with multiple joins *(PR [#4113](https://github.com/tobymao/sqlglot/pull/4113) by [@dg-hellotwin](https://github.com/dg-hellotwin))*
  - :arrow_lower_right: *fixes issue [#4112](https://github.com/tobymao/sqlglot/issues/4112) opened by [@dg-hellotwin](https://github.com/dg-hellotwin)*
- [`21f5bcd`](https://github.com/tobymao/sqlglot/commit/21f5bcd13eb9c567c711cec5879c4d08a052b91c) - parse struct(...)[] type properly *(PR [#4123](https://github.com/tobymao/sqlglot/pull/4123) by [@georgesittas](https://github.com/georgesittas))*
- [`22c456d`](https://github.com/tobymao/sqlglot/commit/22c456d032b457244b397598d4480ae22ad316bd) - Do not generate DISTINCT keyword in FILTER *(PR [#4130](https://github.com/tobymao/sqlglot/pull/4130) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4129](https://github.com/tobymao/sqlglot/issues/4129) opened by [@tekumara](https://github.com/tekumara)*
- [`089b77e`](https://github.com/tobymao/sqlglot/commit/089b77ec7efcd6decbf9a7be500e73dc88ba4dec) - **athena**: DDL fixes *(PR [#4132](https://github.com/tobymao/sqlglot/pull/4132) by [@erindru](https://github.com/erindru))*
- [`e6c9902`](https://github.com/tobymao/sqlglot/commit/e6c990225e2685c617dfd1594c83778036405f6b) - invalid regex *(commit by [@tobymao](https://github.com/tobymao))*
- [`77a514d`](https://github.com/tobymao/sqlglot/commit/77a514dd7cfa9feb847c429411809092e5578bad) - **parser**: Parse VALUES & query modifiers in wrapped FROM clause *(PR [#4135](https://github.com/tobymao/sqlglot/pull/4135) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4133](https://github.com/tobymao/sqlglot/issues/4133) opened by [@danxmoran](https://github.com/danxmoran)*
- [`8822d6c`](https://github.com/tobymao/sqlglot/commit/8822d6c1b3b62cfd76fd481db473bf8ea1c12b1a) - **parser**: handle brackets in column op json extract arrow parser *(PR [#4140](https://github.com/tobymao/sqlglot/pull/4140) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3151](https://github.com/TobikoData/sqlmesh/issues/3151) opened by [@markgraphene](https://github.com/markgraphene)*
- [`be0a4a8`](https://github.com/tobymao/sqlglot/commit/be0a4a85d41bfb617360bd9a59aa9e631e4d6d6c) - **bigquery**: Consume dashed identifiers only if they're connected *(PR [#4144](https://github.com/tobymao/sqlglot/pull/4144) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0444819`](https://github.com/tobymao/sqlglot/commit/044481926d4b008027a2c7fb20501514ef507811) - **optimizer**: don't reorder subquery predicates in simplify *(PR [#4147](https://github.com/tobymao/sqlglot/pull/4147) by [@georgesittas](https://github.com/georgesittas))*
- [`89519bb`](https://github.com/tobymao/sqlglot/commit/89519bba99fc11f17e8e00bf8e3f6dde213e99be) - **clickhouse**: make ToTableProperty appear right after the DDL name *(PR [#4151](https://github.com/tobymao/sqlglot/pull/4151) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4150](https://github.com/tobymao/sqlglot/issues/4150) opened by [@tzinyama](https://github.com/tzinyama)*

### :recycle: Refactors
- [`9c527b5`](https://github.com/tobymao/sqlglot/commit/9c527b549cc56db9d8f44579397d9f9fe1440573) - treat Nullable as an arg instead of a DataType.TYPE *(PR [#4094](https://github.com/tobymao/sqlglot/pull/4094) by [@georgesittas](https://github.com/georgesittas))*
- [`2961049`](https://github.com/tobymao/sqlglot/commit/296104950f2e679aea37810a48eb490e170518d3) - implement decorator to easily mark args as unsupported *(PR [#4111](https://github.com/tobymao/sqlglot/pull/4111) by [@georgesittas](https://github.com/georgesittas))*
- [`7cf1d70`](https://github.com/tobymao/sqlglot/commit/7cf1d70e909ae319ff659e1455e6fcad1e8cf905) - **optimizer**: Optimize USING expansion *(PR [#4115](https://github.com/tobymao/sqlglot/pull/4115) by [@VaggelisD](https://github.com/VaggelisD))*

### :wrench: Chores
- [`75e6406`](https://github.com/tobymao/sqlglot/commit/75e640672eef0ddf752ee36dbc6f904f8e06510f) - **prql**: rewrite tests to use `validate_all()` *(PR [#4097](https://github.com/tobymao/sqlglot/pull/4097) by [@JJHCool](https://github.com/JJHCool))*
- [`e1f6ae3`](https://github.com/tobymao/sqlglot/commit/e1f6ae393fa2857dbbb9a14b03cbe39910207233) - **prql**: use validate_all instead of validate_identity *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`2dc0b86`](https://github.com/tobymao/sqlglot/commit/2dc0b8620e16b3cdf85f6bb6bea10c2527497933) - **optimizer**: rename helper function in expand_using *(PR [#4117](https://github.com/tobymao/sqlglot/pull/4117) by [@georgesittas](https://github.com/georgesittas))*
- [`fd8b8ba`](https://github.com/tobymao/sqlglot/commit/fd8b8ba7dedaee5d237b080db5c4f7e83ba079e9) - create ARRAY_TYPES set under DataType *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.22.0] - 2024-09-19
### :boom: BREAKING CHANGES
- due to [`ba015dc`](https://github.com/tobymao/sqlglot/commit/ba015dc1102a4fe0c35cbfe6e3d23dc24263c20f) - add `returning` to merge expression builder *(PR [#4125](https://github.com/tobymao/sqlglot/pull/4125) by [@max-muoto](https://github.com/max-muoto))*:

  add `returning` to merge expression builder (#4125)


### :sparkles: New Features
- [`1fac6a9`](https://github.com/tobymao/sqlglot/commit/1fac6a9f46e147a5583042d6f82deffd04cd58c9) - expose sqlglot.expressions.delete as a sqlglot module function *(PR [#4126](https://github.com/tobymao/sqlglot/pull/4126) by [@max-muoto](https://github.com/max-muoto))*
- [`4506b3b`](https://github.com/tobymao/sqlglot/commit/4506b3b58fde8e8fe711df8fd0c9c245a98ca86b) - **duckdb**: add support for the UNION type *(PR [#4128](https://github.com/tobymao/sqlglot/pull/4128) by [@georgesittas](https://github.com/georgesittas))*
- [`ba015dc`](https://github.com/tobymao/sqlglot/commit/ba015dc1102a4fe0c35cbfe6e3d23dc24263c20f) - add `returning` to merge expression builder *(PR [#4125](https://github.com/tobymao/sqlglot/pull/4125) by [@max-muoto](https://github.com/max-muoto))*

### :bug: Bug Fixes
- [`22c456d`](https://github.com/tobymao/sqlglot/commit/22c456d032b457244b397598d4480ae22ad316bd) - Do not generate DISTINCT keyword in FILTER *(PR [#4130](https://github.com/tobymao/sqlglot/pull/4130) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4129](https://github.com/tobymao/sqlglot/issues/4129) opened by [@tekumara](https://github.com/tekumara)*
- [`089b77e`](https://github.com/tobymao/sqlglot/commit/089b77ec7efcd6decbf9a7be500e73dc88ba4dec) - **athena**: DDL fixes *(PR [#4132](https://github.com/tobymao/sqlglot/pull/4132) by [@erindru](https://github.com/erindru))*
- [`e6c9902`](https://github.com/tobymao/sqlglot/commit/e6c990225e2685c617dfd1594c83778036405f6b) - invalid regex *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.21.3] - 2024-09-14
### :sparkles: New Features
- [`bd8e050`](https://github.com/tobymao/sqlglot/commit/bd8e050e7df905082fd32e26c4ee0c6e2d36c897) - **clickhouse**: support ON CLUSTER clause in DELETE *(PR [#4119](https://github.com/tobymao/sqlglot/pull/4119) by [@treysp](https://github.com/treysp))*

### :bug: Bug Fixes
- [`21f5bcd`](https://github.com/tobymao/sqlglot/commit/21f5bcd13eb9c567c711cec5879c4d08a052b91c) - parse struct(...)[] type properly *(PR [#4123](https://github.com/tobymao/sqlglot/pull/4123) by [@georgesittas](https://github.com/georgesittas))*


## [v25.21.2] - 2024-09-13
### :sparkles: New Features
- [`a34f8b6`](https://github.com/tobymao/sqlglot/commit/a34f8b6ff9b0aa8595214da75fe7cbfbc8285476) - **oracle**: support TRUNC without fmt argument fixes [#4116](https://github.com/tobymao/sqlglot/pull/4116) *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`66c3295`](https://github.com/tobymao/sqlglot/commit/66c32958a9e46642077813adf90079098e41c87e) - **optimizer**: Enable USING expansion with multiple joins *(PR [#4113](https://github.com/tobymao/sqlglot/pull/4113) by [@dg-hellotwin](https://github.com/dg-hellotwin))*
  - :arrow_lower_right: *fixes issue [#4112](https://github.com/tobymao/sqlglot/issues/4112) opened by [@dg-hellotwin](https://github.com/dg-hellotwin)*

### :recycle: Refactors
- [`7cf1d70`](https://github.com/tobymao/sqlglot/commit/7cf1d70e909ae319ff659e1455e6fcad1e8cf905) - **optimizer**: Optimize USING expansion *(PR [#4115](https://github.com/tobymao/sqlglot/pull/4115) by [@VaggelisD](https://github.com/VaggelisD))*

### :wrench: Chores
- [`2dc0b86`](https://github.com/tobymao/sqlglot/commit/2dc0b8620e16b3cdf85f6bb6bea10c2527497933) - **optimizer**: rename helper function in expand_using *(PR [#4117](https://github.com/tobymao/sqlglot/pull/4117) by [@georgesittas](https://github.com/georgesittas))*
- [`fd8b8ba`](https://github.com/tobymao/sqlglot/commit/fd8b8ba7dedaee5d237b080db5c4f7e83ba079e9) - create ARRAY_TYPES set under DataType *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.21.1] - 2024-09-13
### :bug: Bug Fixes
- [`06c76f7`](https://github.com/tobymao/sqlglot/commit/06c76f7471cdb679a7a7a35064204d94841fd929) - calling interval without unit *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.21.0] - 2024-09-12
### :boom: BREAKING CHANGES
- due to [`da51ea5`](https://github.com/tobymao/sqlglot/commit/da51ea5c8f405859d877a25176e8e48ef8b4b112) - refactor exp.Chr *(PR [#4081](https://github.com/tobymao/sqlglot/pull/4081) by [@georgesittas](https://github.com/georgesittas))*:

  refactor exp.Chr (#4081)

- due to [`9c527b5`](https://github.com/tobymao/sqlglot/commit/9c527b549cc56db9d8f44579397d9f9fe1440573) - treat Nullable as an arg instead of a DataType.TYPE *(PR [#4094](https://github.com/tobymao/sqlglot/pull/4094) by [@georgesittas](https://github.com/georgesittas))*:

  treat Nullable as an arg instead of a DataType.TYPE (#4094)


### :sparkles: New Features
- [`5771d8d`](https://github.com/tobymao/sqlglot/commit/5771d8da94aff104206f93482e7b248d725f1843) - add merge expression builder *(PR [#4084](https://github.com/tobymao/sqlglot/pull/4084) by [@max-muoto](https://github.com/max-muoto))*
- [`1d52709`](https://github.com/tobymao/sqlglot/commit/1d5270915d14f3f92341d5057b88b58fff6c0d97) - **postgres**: Parse DO NOTHING and RETURNING in MERGE statement *(PR [#4087](https://github.com/tobymao/sqlglot/pull/4087) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4083](https://github.com/tobymao/sqlglot/issues/4083) opened by [@max-muoto](https://github.com/max-muoto)*
- [`1615bad`](https://github.com/tobymao/sqlglot/commit/1615bad98eeddc8e67e8002c3b1fe93bd7c3b690) - Add support for UUID function *(PR [#4089](https://github.com/tobymao/sqlglot/pull/4089) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5733600`](https://github.com/tobymao/sqlglot/commit/57336006795d32e9253a9df4813d3029d1d32ef1) - **bigquery**: transpile UUID type to STRING *(PR [#4093](https://github.com/tobymao/sqlglot/pull/4093) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4091](https://github.com/tobymao/sqlglot/issues/4091) opened by [@gigatexal](https://github.com/gigatexal)*
- [`75230f5`](https://github.com/tobymao/sqlglot/commit/75230f5970c240add1cff7349fc65fb67541fa34) - **bigquery**: add support for the MERGE ... THEN INSERT ROW syntax *(PR [#4096](https://github.com/tobymao/sqlglot/pull/4096) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4095](https://github.com/tobymao/sqlglot/issues/4095) opened by [@ericist](https://github.com/ericist)*
- [`f8d4dc4`](https://github.com/tobymao/sqlglot/commit/f8d4dc4bab90cd369eef090c23b81160a7ae78fc) - **parser**: add support for ALTER INDEX closes [#4105](https://github.com/tobymao/sqlglot/pull/4105) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`28c6f27`](https://github.com/tobymao/sqlglot/commit/28c6f27291d57d85917c62b387b86a598ee3c1d6) - **duckdb**: Support *COLUMNS() function *(PR [#4106](https://github.com/tobymao/sqlglot/pull/4106) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4101](https://github.com/tobymao/sqlglot/issues/4101) opened by [@aersam](https://github.com/aersam)*
- [`3cb0041`](https://github.com/tobymao/sqlglot/commit/3cb00417f45624f012e5ce8ababfe3250e813b80) - **snowflake**: Fix exp.Pivot FOR IN clause *(PR [#4109](https://github.com/tobymao/sqlglot/pull/4109) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4108](https://github.com/tobymao/sqlglot/issues/4108) opened by [@kharigardner](https://github.com/kharigardner)*
- [`7ac21a0`](https://github.com/tobymao/sqlglot/commit/7ac21a07c73cdf156ae4dc6a848b9f781b265d16) - **athena**: Improve DDL query support *(PR [#4099](https://github.com/tobymao/sqlglot/pull/4099) by [@erindru](https://github.com/erindru))*

### :bug: Bug Fixes
- [`da51ea5`](https://github.com/tobymao/sqlglot/commit/da51ea5c8f405859d877a25176e8e48ef8b4b112) - **parser**: refactor exp.Chr *(PR [#4081](https://github.com/tobymao/sqlglot/pull/4081) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4080](https://github.com/tobymao/sqlglot/issues/4080) opened by [@EugeneTorap](https://github.com/EugeneTorap)*
- [`6294f9e`](https://github.com/tobymao/sqlglot/commit/6294f9e6d08111c6088f5ed9846e7a64f7724801) - **starrocks**: generate ARRAY_FILTER for exp.ArrayFilter *(PR [#4088](https://github.com/tobymao/sqlglot/pull/4088) by [@gauravsagar483](https://github.com/gauravsagar483))*
- [`1e02c02`](https://github.com/tobymao/sqlglot/commit/1e02c0221ea8445ccb5537b0a77e120c0b2c108c) - **mysql**: convert VARCHAR without size to TEXT for DDLs *(PR [#4092](https://github.com/tobymao/sqlglot/pull/4092) by [@georgesittas](https://github.com/georgesittas))*
- [`cb5bcff`](https://github.com/tobymao/sqlglot/commit/cb5bcfff1f96972e75681bb2411bca8b60a4bff1) - **clickhouse**: generate formatDateTime instead of DATE_FORMAT fixes [#4098](https://github.com/tobymao/sqlglot/pull/4098) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b10255e`](https://github.com/tobymao/sqlglot/commit/b10255eb8b6b73bf5084fdf6bffd5a7fa351b1ec) - **snowflake**: Manually escape single quotes in colon operator *(PR [#4104](https://github.com/tobymao/sqlglot/pull/4104) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4090](https://github.com/tobymao/sqlglot/issues/4090) opened by [@jussihe-rec](https://github.com/jussihe-rec)*
- [`67a9ad8`](https://github.com/tobymao/sqlglot/commit/67a9ad89abfce84f78dd1a34caa9dc8143233609) - Move JSON path escape to generation *(PR [#4110](https://github.com/tobymao/sqlglot/pull/4110) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4090](https://github.com/tobymao/sqlglot/issues/4090) opened by [@jussihe-rec](https://github.com/jussihe-rec)*

### :recycle: Refactors
- [`9c527b5`](https://github.com/tobymao/sqlglot/commit/9c527b549cc56db9d8f44579397d9f9fe1440573) - treat Nullable as an arg instead of a DataType.TYPE *(PR [#4094](https://github.com/tobymao/sqlglot/pull/4094) by [@georgesittas](https://github.com/georgesittas))*
- [`2961049`](https://github.com/tobymao/sqlglot/commit/296104950f2e679aea37810a48eb490e170518d3) - implement decorator to easily mark args as unsupported *(PR [#4111](https://github.com/tobymao/sqlglot/pull/4111) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`75e6406`](https://github.com/tobymao/sqlglot/commit/75e640672eef0ddf752ee36dbc6f904f8e06510f) - **prql**: rewrite tests to use `validate_all()` *(PR [#4097](https://github.com/tobymao/sqlglot/pull/4097) by [@JJHCool](https://github.com/JJHCool))*
- [`e1f6ae3`](https://github.com/tobymao/sqlglot/commit/e1f6ae393fa2857dbbb9a14b03cbe39910207233) - **prql**: use validate_all instead of validate_identity *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.20.1] - 2024-09-06
### :wrench: Chores
- [`8a357cc`](https://github.com/tobymao/sqlglot/commit/8a357ccbcf2301f6a8d60c237a6397bf6547de14) - bump sqlglotrs to 0.2.12 -- remove relative readme path *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.20.0] - 2024-09-06
### :boom: BREAKING CHANGES
- due to [`3e1af21`](https://github.com/tobymao/sqlglot/commit/3e1af21787ee81df5cbb5eb8b4b7f808b404c870) - Canonicalize exp.RegexpExtract group default value *(PR [#4051](https://github.com/tobymao/sqlglot/pull/4051) by [@VaggelisD](https://github.com/VaggelisD))*:

  Canonicalize exp.RegexpExtract group default value (#4051)

- due to [`c8e2eae`](https://github.com/tobymao/sqlglot/commit/c8e2eaecad0b3b0fff725512ef571de41c5be0a1) - do not canonicalize INTERVAL values to number literals *(commit by [@VaggelisD](https://github.com/VaggelisD))*:

  do not canonicalize INTERVAL values to number literals


### :sparkles: New Features
- [`d3ee5ea`](https://github.com/tobymao/sqlglot/commit/d3ee5ea6abd0dfb6e5216bf212e9e737c163eeb9) - **oracle**: parse TRUNC to facilitate transpilation closes [#4054](https://github.com/tobymao/sqlglot/pull/4054) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`143f176`](https://github.com/tobymao/sqlglot/commit/143f176a893f060eecc1fbf4a5b5c54d35a3acc7) - **clickhouse**: transpile oracle functions chr, lag, lead *(PR [#4053](https://github.com/tobymao/sqlglot/pull/4053) by [@sleshJdev](https://github.com/sleshJdev))*
- [`d89757e`](https://github.com/tobymao/sqlglot/commit/d89757e21665913d49a3ccc19deeea86ab59820c) - **postgres**: add support for the NOT VALID clause in ALTER TABLE fixes [#4077](https://github.com/tobymao/sqlglot/pull/4077) *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`3e1af21`](https://github.com/tobymao/sqlglot/commit/3e1af21787ee81df5cbb5eb8b4b7f808b404c870) - Canonicalize exp.RegexpExtract group default value *(PR [#4051](https://github.com/tobymao/sqlglot/pull/4051) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4039](https://github.com/tobymao/sqlglot/issues/4039) opened by [@hellozepp](https://github.com/hellozepp)*
- [`75cafad`](https://github.com/tobymao/sqlglot/commit/75cafad45fec02eb27f42676ddca4d1777e800f7) - **starrocks**: Move the parse distribute and duplicate to Parser *(PR [#4062](https://github.com/tobymao/sqlglot/pull/4062) by [@hellozepp](https://github.com/hellozepp))*
- [`74352d5`](https://github.com/tobymao/sqlglot/commit/74352d523333e5eff464f97b49a1bcfb11ec291b) - **tsql**: use plus operator for string concat to support more systems that use tsql *(PR [#4067](https://github.com/tobymao/sqlglot/pull/4067) by [@cpcloud](https://github.com/cpcloud))*
  - :arrow_lower_right: *fixes issue [#4066](https://github.com/tobymao/sqlglot/issues/4066) opened by [@cpcloud](https://github.com/cpcloud)*
- [`c8e2eae`](https://github.com/tobymao/sqlglot/commit/c8e2eaecad0b3b0fff725512ef571de41c5be0a1) - do not canonicalize INTERVAL values to number literals *(commit by [@VaggelisD](https://github.com/VaggelisD))*
- [`532a024`](https://github.com/tobymao/sqlglot/commit/532a024e1a1dbc422e603dc0336149362c5763df) - **snowflake, bigquery**: Remove exp.Trim generation *(PR [#4070](https://github.com/tobymao/sqlglot/pull/4070) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3095](https://github.com/TobikoData/sqlmesh/issues/3095) opened by [@plaflamme](https://github.com/plaflamme)*
- [`ad7e582`](https://github.com/tobymao/sqlglot/commit/ad7e582504955c5dba84566994e81873a46d1c28) - **athena**: Apply correct quoting to queries depending on type (DML or DDL) *(PR [#4073](https://github.com/tobymao/sqlglot/pull/4073) by [@erindru](https://github.com/erindru))*
- [`cc5b877`](https://github.com/tobymao/sqlglot/commit/cc5b8774c469250fd403ca3379f1c2dcab9d4017) - **parser**: Wrap column constraints in _parse_column_def() *(PR [#4078](https://github.com/tobymao/sqlglot/pull/4078) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4065](https://github.com/tobymao/sqlglot/issues/4065) opened by [@ajuszczak](https://github.com/ajuszczak)*
- [`4eb384a`](https://github.com/tobymao/sqlglot/commit/4eb384a799b3ad0f152893eb6217131a3a698ff1) - **clickhouse**: Remove CURRENT_TIMESTAMP from NO_PAREN_FUNCTIONS *(PR [#4079](https://github.com/tobymao/sqlglot/pull/4079) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4076](https://github.com/tobymao/sqlglot/issues/4076) opened by [@hellozepp](https://github.com/hellozepp)*

### :recycle: Refactors
- [`534f882`](https://github.com/tobymao/sqlglot/commit/534f88280a895d9f7503e48eedf600628d34aa82) - **clickhouse**: clean up chr, lag, lead generation *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`b986ebe`](https://github.com/tobymao/sqlglot/commit/b986ebe99c95ce6f76a76bfd0ea79ee4ac3757f0) - fill in more details for sqlglotrs pypi page *(PR [#4071](https://github.com/tobymao/sqlglot/pull/4071) by [@georgesittas](https://github.com/georgesittas))*
- [`0310926`](https://github.com/tobymao/sqlglot/commit/0310926297b18714a02873b649061b50c7080ac9) - bump sqlglotrs to 0.2.11 (update pypi details) *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.19.0] - 2024-09-03
### :boom: BREAKING CHANGES
- due to [`6da9328`](https://github.com/tobymao/sqlglot/commit/6da932889c2d60c82be118842f4edee031009f8a) - refactor SET OPERATION handling to set correct defaults *(PR [#4009](https://github.com/tobymao/sqlglot/pull/4009) by [@georgesittas](https://github.com/georgesittas))*:

  refactor SET OPERATION handling to set correct defaults (#4009)

- due to [`4b69d18`](https://github.com/tobymao/sqlglot/commit/4b69d18e8e23c9ba2b0a886be497df9c1071f26c) - use TO_GEOGRAPHY, TO_GEOMETRY instead of casts *(PR [#4017](https://github.com/tobymao/sqlglot/pull/4017) by [@georgesittas](https://github.com/georgesittas))*:

  use TO_GEOGRAPHY, TO_GEOMETRY instead of casts (#4017)

- due to [`0985907`](https://github.com/tobymao/sqlglot/commit/098590718104b9a6e9c0340fbe05fd89759c142b) - Add UnsupportedError to unnest_to_explode transform *(PR [#4016](https://github.com/tobymao/sqlglot/pull/4016) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add UnsupportedError to unnest_to_explode transform (#4016)

- due to [`7d63d23`](https://github.com/tobymao/sqlglot/commit/7d63d235c1f8ce7a76db3d31f11050dc65c0fef1) - Support JSON_EXISTS, refactor ON handling *(PR [#4032](https://github.com/tobymao/sqlglot/pull/4032) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support JSON_EXISTS, refactor ON handling (#4032)


### :sparkles: New Features
- [`f550ba1`](https://github.com/tobymao/sqlglot/commit/f550ba1068eaa4be45c19b4a3ea11baad48b27c1) - **presto**: support [ SECURITY { DEFINER | INVOKER } ] *(PR [#4008](https://github.com/tobymao/sqlglot/pull/4008) by [@usmanovbf](https://github.com/usmanovbf))*
- [`dedd757`](https://github.com/tobymao/sqlglot/commit/dedd75790ecc2549fa7b28b3612125ca2aaeb762) - **oracle**: Parse multitable inserts *(PR [#4000](https://github.com/tobymao/sqlglot/pull/4000) by [@usefulalgorithm](https://github.com/usefulalgorithm))*
- [`0985907`](https://github.com/tobymao/sqlglot/commit/098590718104b9a6e9c0340fbe05fd89759c142b) - Add UnsupportedError to unnest_to_explode transform *(PR [#4016](https://github.com/tobymao/sqlglot/pull/4016) by [@VaggelisD](https://github.com/VaggelisD))*
- [`8f5fccf`](https://github.com/tobymao/sqlglot/commit/8f5fccfca8502e0fe00420662825845cc640a1cb) - **presto**: generate non-iso DayOfWeek *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`7d63d23`](https://github.com/tobymao/sqlglot/commit/7d63d235c1f8ce7a76db3d31f11050dc65c0fef1) - **oracle**: Support JSON_EXISTS, refactor ON handling *(PR [#4032](https://github.com/tobymao/sqlglot/pull/4032) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4026](https://github.com/tobymao/sqlglot/issues/4026) opened by [@ashishshukla19](https://github.com/ashishshukla19)*
- [`85cc7ad`](https://github.com/tobymao/sqlglot/commit/85cc7ad68599dde59ffab460d49010f167cab85d) - **duckdb**: Transpile BQ's exp.ArrayToString *(PR [#4034](https://github.com/tobymao/sqlglot/pull/4034) by [@VaggelisD](https://github.com/VaggelisD))*
- [`7f2c7f1`](https://github.com/tobymao/sqlglot/commit/7f2c7f17f5d79c3ea93b43cdeacdb5339955c9a8) - Support for NORMALIZE() function *(PR [#4041](https://github.com/tobymao/sqlglot/pull/4041) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4037](https://github.com/tobymao/sqlglot/issues/4037) opened by [@jasonrosendale](https://github.com/jasonrosendale)*
- [`a1b9803`](https://github.com/tobymao/sqlglot/commit/a1b980327ff94519a4cba1e0e48066c0ea51d359) - support COMPRESS column constraint wihout a value *(PR [#4045](https://github.com/tobymao/sqlglot/pull/4045) by [@thomascjohnson](https://github.com/thomascjohnson))*

### :bug: Bug Fixes
- [`8583772`](https://github.com/tobymao/sqlglot/commit/85837729e746743755294727d0394534834f4c4c) - **tsql**: Use count_big instead of count *(PR [#3996](https://github.com/tobymao/sqlglot/pull/3996) by [@colin-ho](https://github.com/colin-ho))*
  - :arrow_lower_right: *fixes issue [#3995](https://github.com/tobymao/sqlglot/issues/3995) opened by [@colin-ho](https://github.com/colin-ho)*
- [`4b7ca2b`](https://github.com/tobymao/sqlglot/commit/4b7ca2be353e7432b84384ff9cfd43f3c43438e0) - **spark**: Custom annotation for SUBSTRING() *(PR [#4004](https://github.com/tobymao/sqlglot/pull/4004) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4002](https://github.com/tobymao/sqlglot/issues/4002) opened by [@racevedoo](https://github.com/racevedoo)*
- [`cb172db`](https://github.com/tobymao/sqlglot/commit/cb172dbe4d13fd3badad352ea79d2fd6e5271576) - **clickhouse**: ensure that ALL and DISTINCT are rendered for except and intersect *(PR [#4007](https://github.com/tobymao/sqlglot/pull/4007) by [@cpcloud](https://github.com/cpcloud))*
  - :arrow_lower_right: *fixes issue [#4005](https://github.com/tobymao/sqlglot/issues/4005) opened by [@cpcloud](https://github.com/cpcloud)*
- [`829fdcb`](https://github.com/tobymao/sqlglot/commit/829fdcb1dbe52710269823bda93e3e49c02dbf63) - **starrocks**: exp.Unnest transpilation *(PR [#3999](https://github.com/tobymao/sqlglot/pull/3999) by [@hellozepp](https://github.com/hellozepp))*
  - :arrow_lower_right: *fixes issue [#3962](https://github.com/tobymao/sqlglot/issues/3962) opened by [@hellozepp](https://github.com/hellozepp)*
- [`6da9328`](https://github.com/tobymao/sqlglot/commit/6da932889c2d60c82be118842f4edee031009f8a) - refactor SET OPERATION handling to set correct defaults *(PR [#4009](https://github.com/tobymao/sqlglot/pull/4009) by [@georgesittas](https://github.com/georgesittas))*
- [`23a928e`](https://github.com/tobymao/sqlglot/commit/23a928edc1d204a13516e0db38336774962a135e) - **mysql**: Preserve roundtrip of %a, %W time formats *(PR [#4014](https://github.com/tobymao/sqlglot/pull/4014) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4011](https://github.com/tobymao/sqlglot/issues/4011) opened by [@hellozepp](https://github.com/hellozepp)*
- [`2d4483c`](https://github.com/tobymao/sqlglot/commit/2d4483c0f79c5c72438a7093c938b1f178e5d48a) - don't log warning in to_json_path conditionally *(PR [#4015](https://github.com/tobymao/sqlglot/pull/4015) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4013](https://github.com/tobymao/sqlglot/issues/4013) opened by [@hellozepp](https://github.com/hellozepp)*
- [`4b69d18`](https://github.com/tobymao/sqlglot/commit/4b69d18e8e23c9ba2b0a886be497df9c1071f26c) - **snowflake**: use TO_GEOGRAPHY, TO_GEOMETRY instead of casts *(PR [#4017](https://github.com/tobymao/sqlglot/pull/4017) by [@georgesittas](https://github.com/georgesittas))*
- [`1108426`](https://github.com/tobymao/sqlglot/commit/1108426a0eb23bbcaec8bed946f1dae6682bc1dd) - **optimizer**: annotate unary expressions correctly *(PR [#4019](https://github.com/tobymao/sqlglot/pull/4019) by [@georgesittas](https://github.com/georgesittas))*
- [`5fad18c`](https://github.com/tobymao/sqlglot/commit/5fad18c6cb5f62630cdfa2616231436586c41d67) - **presto**: exp.DayOfWeek *(PR [#4024](https://github.com/tobymao/sqlglot/pull/4024) by [@hellozepp](https://github.com/hellozepp))*
- [`ea9a494`](https://github.com/tobymao/sqlglot/commit/ea9a4948a3e1619b885fc0b1522a7382d68c9cbe) - **parser**: consume STREAM in _parse_select only if it's a VAR, closes [#4029](https://github.com/tobymao/sqlglot/pull/4029) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`c239a74`](https://github.com/tobymao/sqlglot/commit/c239a741233bf858ce686d2d32a657cbedb49699) - transpile null exclusion for ARRAY_AGG *(PR [#4033](https://github.com/tobymao/sqlglot/pull/4033) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4031](https://github.com/tobymao/sqlglot/issues/4031) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`60a8f16`](https://github.com/tobymao/sqlglot/commit/60a8f16b5386fe334b6e15afa967ad7bdd2a83de) - **parser**: don't consume strings in match_text_seq *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a726583`](https://github.com/tobymao/sqlglot/commit/a726583995716f815946b4c81c07a916ade727b7) - **parser**: don't consume strings in match_texts *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`551d32f`](https://github.com/tobymao/sqlglot/commit/551d32fdebfc78506b77e4f6e6882d2a8cbd457c) - **postgres**: Support for DROP INDEX CONCURRENTLY. *(PR [#4040](https://github.com/tobymao/sqlglot/pull/4040) by [@EdgyEdgemond](https://github.com/EdgyEdgemond))*
  - :arrow_lower_right: *fixes issue [#3783](https://github.com/tobymao/sqlglot/issues/3783) opened by [@EdgyEdgemond](https://github.com/EdgyEdgemond)*
- [`f55647d`](https://github.com/tobymao/sqlglot/commit/f55647d9d9c088880c0c16efff23ef8d22c2be44) - **starrocks**: exp.Create transpilation *(PR [#4023](https://github.com/tobymao/sqlglot/pull/4023) by [@hellozepp](https://github.com/hellozepp))*
  - :arrow_lower_right: *fixes issue [#3997](https://github.com/tobymao/sqlglot/issues/3997) opened by [@hellozepp](https://github.com/hellozepp)*
- [`bf0f5fa`](https://github.com/tobymao/sqlglot/commit/bf0f5fa1ab44daa74102b0f16ae16f905b175fbc) - **parser**: Ensure exp.Coalesce expressions is a list *(PR [#4050](https://github.com/tobymao/sqlglot/pull/4050) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3080](https://github.com/TobikoData/sqlmesh/issues/3080) opened by [@Ziemin](https://github.com/Ziemin)*

### :recycle: Refactors
- [`6494776`](https://github.com/tobymao/sqlglot/commit/6494776a45ae4975cee21f70b5f383d29530d155) - simplify multi-insert generation, fix pretty mode *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b11c73e`](https://github.com/tobymao/sqlglot/commit/b11c73e38aa495715c327f44586714e19f699c9c) - clean up starrocks DISTRIBUTED BY property generation *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c35a62c`](https://github.com/tobymao/sqlglot/commit/c35a62cdbc29c796bf0728d3f26e5ae5474881a8) - set the license for sqlglotrs *(PR [#4048](https://github.com/tobymao/sqlglot/pull/4048) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4047](https://github.com/tobymao/sqlglot/issues/4047) opened by [@chriscc2](https://github.com/chriscc2)*
- [`9b7eb2e`](https://github.com/tobymao/sqlglot/commit/9b7eb2e40e4bec4d18664f09e01c1165122dd43f) - bump sqlglotrs to v0.2.10 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.18.0] - 2024-08-28
### :boom: BREAKING CHANGES
- due to [`22bb9a0`](https://github.com/tobymao/sqlglot/commit/22bb9a0e5c64ae344c9e25ed34200ed743e7b8f0) - stop normalizing qualified anonymous functions *(PR [#3969](https://github.com/tobymao/sqlglot/pull/3969) by [@georgesittas](https://github.com/georgesittas))*:

  stop normalizing qualified anonymous functions (#3969)

- due to [`8aec682`](https://github.com/tobymao/sqlglot/commit/8aec68253b10dcbfe7cc5b3d6e1145ae714ca346) - mysql/tsql datetime precision, formatting, exp.AtTimeZone *(PR [#3951](https://github.com/tobymao/sqlglot/pull/3951) by [@erindru](https://github.com/erindru))*:

  mysql/tsql datetime precision, formatting, exp.AtTimeZone (#3951)

- due to [`2f3626a`](https://github.com/tobymao/sqlglot/commit/2f3626a4fc20c46411cd91bf8beda2bdd103ca4a) - Generation of exp.SHA2, exp.Transform, exp.IgnoreNulls *(PR [#3980](https://github.com/tobymao/sqlglot/pull/3980) by [@VaggelisD](https://github.com/VaggelisD))*:

  Generation of exp.SHA2, exp.Transform, exp.IgnoreNulls (#3980)

- due to [`905b722`](https://github.com/tobymao/sqlglot/commit/905b7226ae4a6dc505fe303bb4df3818cb586826) - preserve each distinct CUBE/ROLLUP/GROUPING SET clause *(PR [#3985](https://github.com/tobymao/sqlglot/pull/3985) by [@georgesittas](https://github.com/georgesittas))*:

  preserve each distinct CUBE/ROLLUP/GROUPING SET clause (#3985)


### :sparkles: New Features
- [`48b214d`](https://github.com/tobymao/sqlglot/commit/48b214da7e39d36938d12059deb827d0a5f6a5a2) - **postgres**: Support for IS JSON predicate *(PR [#3971](https://github.com/tobymao/sqlglot/pull/3971) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3965](https://github.com/tobymao/sqlglot/issues/3965) opened by [@faisal-ksolves](https://github.com/faisal-ksolves)*
- [`f7e4e4a`](https://github.com/tobymao/sqlglot/commit/f7e4e4adc64aaef73d23c2550a4bfa9958d4851b) - **duckdb**: add support for the GLOB table function closes [#3973](https://github.com/tobymao/sqlglot/pull/3973) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a0d1377`](https://github.com/tobymao/sqlglot/commit/a0d137787885627aae07f11a9c18a4cc133baa0a) - **spark**: add support for table statement in INSERT *(PR [#3986](https://github.com/tobymao/sqlglot/pull/3986) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3984](https://github.com/tobymao/sqlglot/issues/3984) opened by [@madeirak](https://github.com/madeirak)*
- [`f5bfd67`](https://github.com/tobymao/sqlglot/commit/f5bfd67341518d0ecb1c3693e0b41ed5c1cf0596) - **mysql**: Parse JSON_VALUE() *(PR [#3987](https://github.com/tobymao/sqlglot/pull/3987) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3983](https://github.com/tobymao/sqlglot/issues/3983) opened by [@ashishshukla19](https://github.com/ashishshukla19)*
- [`79e92ad`](https://github.com/tobymao/sqlglot/commit/79e92ad565c42098ff7b7921fe04e6aac7859dd8) - **spark**: Default naming of STRUCT fields *(PR [#3991](https://github.com/tobymao/sqlglot/pull/3991) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3988](https://github.com/tobymao/sqlglot/issues/3988) opened by [@dor-bernstein](https://github.com/dor-bernstein)*

### :bug: Bug Fixes
- [`22bb9a0`](https://github.com/tobymao/sqlglot/commit/22bb9a0e5c64ae344c9e25ed34200ed743e7b8f0) - stop normalizing qualified anonymous functions *(PR [#3969](https://github.com/tobymao/sqlglot/pull/3969) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3967](https://github.com/tobymao/sqlglot/issues/3967) opened by [@cpcloud](https://github.com/cpcloud)*
- [`8aec682`](https://github.com/tobymao/sqlglot/commit/8aec68253b10dcbfe7cc5b3d6e1145ae714ca346) - mysql/tsql datetime precision, formatting, exp.AtTimeZone *(PR [#3951](https://github.com/tobymao/sqlglot/pull/3951) by [@erindru](https://github.com/erindru))*
- [`d37a5bb`](https://github.com/tobymao/sqlglot/commit/d37a5bbfcd5732aa64a24bd83dde4abcac8b0bed) - **snowflake**: handle DIV0 case where divident is null *(PR [#3975](https://github.com/tobymao/sqlglot/pull/3975) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3974](https://github.com/tobymao/sqlglot/issues/3974) opened by [@Nathan-Fenner](https://github.com/Nathan-Fenner)*
- [`b2f877b`](https://github.com/tobymao/sqlglot/commit/b2f877ba5fc9ec9fdafad74196dda1631fdfc0c1) - **oracle**: Use LTRIM/RTRIM unless BOTH is specified *(PR [#3977](https://github.com/tobymao/sqlglot/pull/3977) by [@VaggelisD](https://github.com/VaggelisD))*
- [`201b51a`](https://github.com/tobymao/sqlglot/commit/201b51a860d4db2b2e49e04f6534b7ad22ae287c) - **sqlite**: Make IS parser more lenient *(PR [#3981](https://github.com/tobymao/sqlglot/pull/3981) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3978](https://github.com/tobymao/sqlglot/issues/3978) opened by [@focafull](https://github.com/focafull)*
- [`2f3626a`](https://github.com/tobymao/sqlglot/commit/2f3626a4fc20c46411cd91bf8beda2bdd103ca4a) - **duckdb**: Generation of exp.SHA2, exp.Transform, exp.IgnoreNulls *(PR [#3980](https://github.com/tobymao/sqlglot/pull/3980) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3972](https://github.com/tobymao/sqlglot/issues/3972) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`905b722`](https://github.com/tobymao/sqlglot/commit/905b7226ae4a6dc505fe303bb4df3818cb586826) - **parser**: preserve each distinct CUBE/ROLLUP/GROUPING SET clause *(PR [#3985](https://github.com/tobymao/sqlglot/pull/3985) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3979](https://github.com/tobymao/sqlglot/issues/3979) opened by [@cpcloud](https://github.com/cpcloud)*
- [`ee9dc39`](https://github.com/tobymao/sqlglot/commit/ee9dc399134ad86720abe480ee2565de822336cf) - Fix binding of TABLESAMPLE to exp.Subquery instead of top-level exp.Select *(PR [#3994](https://github.com/tobymao/sqlglot/pull/3994) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3992](https://github.com/tobymao/sqlglot/issues/3992) opened by [@cpcloud](https://github.com/cpcloud)*


## [v25.17.0] - 2024-08-26
### :boom: BREAKING CHANGES
- due to [`0a9ba05`](https://github.com/tobymao/sqlglot/commit/0a9ba0536235e10aed02d4ff5e571e435a00febc) - 0 is falsey *(commit by [@tobymao](https://github.com/tobymao))*:

  0 is falsey


### :bug: Bug Fixes
- [`42b725e`](https://github.com/tobymao/sqlglot/commit/42b725e4821a1426fe7c93f9fecbd4ec372accc9) - flaky test closes [#3961](https://github.com/tobymao/sqlglot/pull/3961) *(commit by [@tobymao](https://github.com/tobymao))*
- [`cc29921`](https://github.com/tobymao/sqlglot/commit/cc299217f5d31a0406ba3c4778bb1ce581fe3f4a) - Parse LTRIM/RTRIM functions as positional exp.Trim *(PR [#3958](https://github.com/tobymao/sqlglot/pull/3958) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3957](https://github.com/tobymao/sqlglot/issues/3957) opened by [@sleshJdev](https://github.com/sleshJdev)*
- [`678e692`](https://github.com/tobymao/sqlglot/commit/678e6926fdbefb16efbbcaef9cd6c5ca284af54a) - make sample an arg of table, not a wrapper *(PR [#3963](https://github.com/tobymao/sqlglot/pull/3963) by [@barakalon](https://github.com/barakalon))*
- [`0a9ba05`](https://github.com/tobymao/sqlglot/commit/0a9ba0536235e10aed02d4ff5e571e435a00febc) - 0 is falsey *(commit by [@tobymao](https://github.com/tobymao))*
- [`c1ac987`](https://github.com/tobymao/sqlglot/commit/c1ac9872a6f77acd52546edbc9da53e350ebf080) - **starrocks**: exp.Array generation, exp.Unnest alias *(PR [#3964](https://github.com/tobymao/sqlglot/pull/3964) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3962](https://github.com/tobymao/sqlglot/issues/3962) opened by [@hellozepp](https://github.com/hellozepp)*


## [v25.16.1] - 2024-08-23
### :bug: Bug Fixes
- [`c4e5be7`](https://github.com/tobymao/sqlglot/commit/c4e5be7d3f4d7a9075d11dc56ece02774f32e749) - include dialect when parsing inside cast *(PR [#3960](https://github.com/tobymao/sqlglot/pull/3960) by [@eakmanrq](https://github.com/eakmanrq))*

### :wrench: Chores
- [`794dc4c`](https://github.com/tobymao/sqlglot/commit/794dc4cea3c4298c8986ade8e0fee88479851b34) - update readme to include onboarding doc *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.16.0] - 2024-08-22
### :boom: BREAKING CHANGES
- due to [`f68d155`](https://github.com/tobymao/sqlglot/commit/f68d155c38a79a6527685c37f8de8773ce790bca) - exp.Merge, for Trino and Postgres, dont strip the target alias from then WHEN MATCHED condition to prevent an ambiguous column error *(PR [#3940](https://github.com/tobymao/sqlglot/pull/3940) by [@erindru](https://github.com/erindru))*:

  exp.Merge, for Trino and Postgres, dont strip the target alias from then WHEN MATCHED condition to prevent an ambiguous column error (#3940)

- due to [`667f7d9`](https://github.com/tobymao/sqlglot/commit/667f7d9e94e14ff619998d2001b6116d363f2a1f) - attach INTERPOLATE expressions to WithFill *(PR [#3944](https://github.com/tobymao/sqlglot/pull/3944) by [@georgesittas](https://github.com/georgesittas))*:

  attach INTERPOLATE expressions to WithFill (#3944)

- due to [`145fdbf`](https://github.com/tobymao/sqlglot/commit/145fdbf6bb02fa1c55087bfd9f6b3a15fbd4b684) - Redshift date format *(PR [#3942](https://github.com/tobymao/sqlglot/pull/3942) by [@erindru](https://github.com/erindru))*:

  Redshift date format (#3942)

- due to [`a84a21a`](https://github.com/tobymao/sqlglot/commit/a84a21aaef0e65754e67ecebdfcbf7136c77acc7) - Add timezone support to exp.TimeStrToTime *(PR [#3938](https://github.com/tobymao/sqlglot/pull/3938) by [@erindru](https://github.com/erindru))*:

  Add timezone support to exp.TimeStrToTime (#3938)


### :sparkles: New Features
- [`a84a21a`](https://github.com/tobymao/sqlglot/commit/a84a21aaef0e65754e67ecebdfcbf7136c77acc7) - Add timezone support to exp.TimeStrToTime *(PR [#3938](https://github.com/tobymao/sqlglot/pull/3938) by [@erindru](https://github.com/erindru))*
- [`70a052a`](https://github.com/tobymao/sqlglot/commit/70a052a672d0c72a3e53b19316defb01144f2907) - transpile from_iso8601_timestamp from presto/trino to duckdb *(PR [#3956](https://github.com/tobymao/sqlglot/pull/3956) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`f68d155`](https://github.com/tobymao/sqlglot/commit/f68d155c38a79a6527685c37f8de8773ce790bca) - exp.Merge, for Trino and Postgres, dont strip the target alias from then WHEN MATCHED condition to prevent an ambiguous column error *(PR [#3940](https://github.com/tobymao/sqlglot/pull/3940) by [@erindru](https://github.com/erindru))*
- [`0458dc0`](https://github.com/tobymao/sqlglot/commit/0458dc0fa1978388336b9fa459b28508d7b40f9e) - **optimizer**: expand alias refs recursive CTE edge case patch *(PR [#3943](https://github.com/tobymao/sqlglot/pull/3943) by [@georgesittas](https://github.com/georgesittas))*
- [`145fdbf`](https://github.com/tobymao/sqlglot/commit/145fdbf6bb02fa1c55087bfd9f6b3a15fbd4b684) - Redshift date format *(PR [#3942](https://github.com/tobymao/sqlglot/pull/3942) by [@erindru](https://github.com/erindru))*
- [`6233c2c`](https://github.com/tobymao/sqlglot/commit/6233c2c75ab3a3bc0dfbf28d3fa8adc1be719281) - **parser**: Support sqls with DESCRIBE partition  *(PR [#3945](https://github.com/tobymao/sqlglot/pull/3945) by [@gp1105739](https://github.com/gp1105739))*
  - :arrow_lower_right: *fixes issue [#3941](https://github.com/tobymao/sqlglot/issues/3941) opened by [@gp1105739](https://github.com/gp1105739)*
- [`85cd6e5`](https://github.com/tobymao/sqlglot/commit/85cd6e507b73be89d2d9b2c88c7370a14b813b5c) - **bigquery**: Map %e to %-d *(PR [#3946](https://github.com/tobymao/sqlglot/pull/3946) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1ba0f03`](https://github.com/tobymao/sqlglot/commit/1ba0f03fbfe5dadc3411c7ff26e6dfbef852491a) - **duckdb**: TIME does not support modifiers *(PR [#3947](https://github.com/tobymao/sqlglot/pull/3947) by [@georgesittas](https://github.com/georgesittas))*
- [`d5d3615`](https://github.com/tobymao/sqlglot/commit/d5d361571cd463869e2243d257f9b6ad0615c070) - **optimizer**: convert TsOrDsToDate to Cast more conservatively *(PR [#3949](https://github.com/tobymao/sqlglot/pull/3949) by [@barakalon](https://github.com/barakalon))*
- [`fb6edc7`](https://github.com/tobymao/sqlglot/commit/fb6edc774539704b48e7d2805ef3211636af18aa) - oracle/snowflake comments closes [#3950](https://github.com/tobymao/sqlglot/pull/3950) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1284fd0`](https://github.com/tobymao/sqlglot/commit/1284fd0a64890d3548af7ed0a0cc05bb6166ccb2) - **oracle**: Revert NVL() being parsed into exp.Anonymous *(PR [#3954](https://github.com/tobymao/sqlglot/pull/3954) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3952](https://github.com/tobymao/sqlglot/issues/3952) opened by [@sleshJdev](https://github.com/sleshJdev)*
- [`c99f8d5`](https://github.com/tobymao/sqlglot/commit/c99f8d5bda79f16fb0d71ae73127cc826860e104) - **duckdb**: Fix exp.Unnest generation for BQ's nested arrays *(PR [#3931](https://github.com/tobymao/sqlglot/pull/3931) by [@VaggelisD](https://github.com/VaggelisD))*

### :recycle: Refactors
- [`f16b0e7`](https://github.com/tobymao/sqlglot/commit/f16b0e7203ad60f0ce50861c4d78176ca53eb2cf) - iteratively generate binary expressions *(PR [#3926](https://github.com/tobymao/sqlglot/pull/3926) by [@MatMoore](https://github.com/MatMoore))*
- [`667f7d9`](https://github.com/tobymao/sqlglot/commit/667f7d9e94e14ff619998d2001b6116d363f2a1f) - **clickhouse**: attach INTERPOLATE expressions to WithFill *(PR [#3944](https://github.com/tobymao/sqlglot/pull/3944) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c697357`](https://github.com/tobymao/sqlglot/commit/c6973572dfd953b5539bb4e9dcba402c0c3c6acf) - slightly refactor Generator.binary, add stress test *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6a5f619`](https://github.com/tobymao/sqlglot/commit/6a5f6199f6da0053fa4564e71a17e3b9f91f0496) - New doc - Onboarding Doc *(PR [#3902](https://github.com/tobymao/sqlglot/pull/3902) by [@VaggelisD](https://github.com/VaggelisD))*


## [v25.15.0] - 2024-08-19
### :boom: BREAKING CHANGES
- due to [`a668655`](https://github.com/tobymao/sqlglot/commit/a668655440815605a566c52b65b28decdfb551eb) - preserve SYSDATE *(PR [#3935](https://github.com/tobymao/sqlglot/pull/3935) by [@georgesittas](https://github.com/georgesittas))*:

  preserve SYSDATE (#3935)


### :sparkles: New Features
- [`be11f4c`](https://github.com/tobymao/sqlglot/commit/be11f4c57c7842f69950bafc3225fb9c139af014) - **clickhouse**: add support for "@"-style parameters *(PR [#3939](https://github.com/tobymao/sqlglot/pull/3939) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`a668655`](https://github.com/tobymao/sqlglot/commit/a668655440815605a566c52b65b28decdfb551eb) - **oracle**: preserve SYSDATE *(PR [#3935](https://github.com/tobymao/sqlglot/pull/3935) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3934](https://github.com/tobymao/sqlglot/issues/3934) opened by [@Hal-H2Apps](https://github.com/Hal-H2Apps)*
- [`b824f8a`](https://github.com/tobymao/sqlglot/commit/b824f8a4148ace01750db301daf4a663dc03b580) - **parser**: allow complex expressions for UNPIVOT alias *(PR [#3937](https://github.com/tobymao/sqlglot/pull/3937) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3936](https://github.com/tobymao/sqlglot/issues/3936) opened by [@dbittenbender](https://github.com/dbittenbender)*

### :recycle: Refactors
- [`f4c34d3`](https://github.com/tobymao/sqlglot/commit/f4c34d37c5773c37a13437c7e0e7eb27b4e98877) - move "MINUS": TokenType.EXCEPT to hive instead of spark *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.14.0] - 2024-08-19
### :boom: BREAKING CHANGES
- due to [`605f1b2`](https://github.com/tobymao/sqlglot/commit/605f1b217d5d1de654cfe2fa1b51435a1a71ae62) - use creatable kind mapping dict for schema<-->database substitution *(PR [#3924](https://github.com/tobymao/sqlglot/pull/3924) by [@treysp](https://github.com/treysp))*:

  use creatable kind mapping dict for schema<-->database substitution (#3924)

- due to [`f418caa`](https://github.com/tobymao/sqlglot/commit/f418caafa8ed317f9e360c6c8f01bdac596258e5) - skip nullable comparison in is_type by default *(PR [#3927](https://github.com/tobymao/sqlglot/pull/3927) by [@georgesittas](https://github.com/georgesittas))*:

  skip nullable comparison in is_type by default (#3927)


### :sparkles: New Features
- [`f418caa`](https://github.com/tobymao/sqlglot/commit/f418caafa8ed317f9e360c6c8f01bdac596258e5) - skip nullable comparison in is_type by default *(PR [#3927](https://github.com/tobymao/sqlglot/pull/3927) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`605f1b2`](https://github.com/tobymao/sqlglot/commit/605f1b217d5d1de654cfe2fa1b51435a1a71ae62) - **clickhouse**: use creatable kind mapping dict for schema<-->database substitution *(PR [#3924](https://github.com/tobymao/sqlglot/pull/3924) by [@treysp](https://github.com/treysp))*


## [v25.13.0] - 2024-08-17
### :boom: BREAKING CHANGES
- due to [`102f5d4`](https://github.com/tobymao/sqlglot/commit/102f5d48279ac1a7a1851737f55a13bd08512f3d) - infer set op types more accurately *(PR [#3918](https://github.com/tobymao/sqlglot/pull/3918) by [@georgesittas](https://github.com/georgesittas))*:

  infer set op types more accurately (#3918)

- due to [`46496a6`](https://github.com/tobymao/sqlglot/commit/46496a6af80bd49d36ef8d265800679d2b07c4db) - improve transpilation of nullable/non-nullable data types *(PR [#3921](https://github.com/tobymao/sqlglot/pull/3921) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilation of nullable/non-nullable data types (#3921)


### :bug: Bug Fixes
- [`c74a8fd`](https://github.com/tobymao/sqlglot/commit/c74a8fd2acd859f5947f27a8f091f13fba1d39e4) - **clickhouse**: make try_cast toXXXOrNull() functions case-specific *(PR [#3917](https://github.com/tobymao/sqlglot/pull/3917) by [@treysp](https://github.com/treysp))*
- [`102f5d4`](https://github.com/tobymao/sqlglot/commit/102f5d48279ac1a7a1851737f55a13bd08512f3d) - **optimizer**: infer set op types more accurately *(PR [#3918](https://github.com/tobymao/sqlglot/pull/3918) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3916](https://github.com/tobymao/sqlglot/issues/3916) opened by [@racevedoo](https://github.com/racevedoo)*

### :recycle: Refactors
- [`1d436d4`](https://github.com/tobymao/sqlglot/commit/1d436d45b4469bb8195dd3597319b6fc5c3f2344) - **clickhouse**: transpile TRY_CAST(x AS T) to CAST(x AS Nullable(T)) *(PR [#3919](https://github.com/tobymao/sqlglot/pull/3919) by [@georgesittas](https://github.com/georgesittas))*
- [`46496a6`](https://github.com/tobymao/sqlglot/commit/46496a6af80bd49d36ef8d265800679d2b07c4db) - **clickhouse**: improve transpilation of nullable/non-nullable data types *(PR [#3921](https://github.com/tobymao/sqlglot/pull/3921) by [@georgesittas](https://github.com/georgesittas))*


## [v25.12.0] - 2024-08-15
### :boom: BREAKING CHANGES
- due to [`e8e70f3`](https://github.com/tobymao/sqlglot/commit/e8e70f3a6cc2ca24de2afe622bbcbccb1ac8aeb3) - treat DATABASE kind as SCHEMA (and conversely) in exp.Create *(PR [#3912](https://github.com/tobymao/sqlglot/pull/3912) by [@georgesittas](https://github.com/georgesittas))*:

  treat DATABASE kind as SCHEMA (and conversely) in exp.Create (#3912)


### :sparkles: New Features
- [`9a66903`](https://github.com/tobymao/sqlglot/commit/9a66903975f16a09d84337a8405bf70945706412) - **clickhouse**: add support for TryCast generation *(PR [#3913](https://github.com/tobymao/sqlglot/pull/3913) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`7965cac`](https://github.com/tobymao/sqlglot/commit/7965cace1d9632c865cae257781072b0932b709d) - **clickhouse**: wrap query in CTAS when COMMENT prop is present *(PR [#3911](https://github.com/tobymao/sqlglot/pull/3911) by [@georgesittas](https://github.com/georgesittas))*
- [`e8e70f3`](https://github.com/tobymao/sqlglot/commit/e8e70f3a6cc2ca24de2afe622bbcbccb1ac8aeb3) - **clickhouse**: treat DATABASE kind as SCHEMA (and conversely) in exp.Create *(PR [#3912](https://github.com/tobymao/sqlglot/pull/3912) by [@georgesittas](https://github.com/georgesittas))*


## [v25.11.3] - 2024-08-14
### :bug: Bug Fixes
- [`57f7aa9`](https://github.com/tobymao/sqlglot/commit/57f7aa9108ed38c0e83ef5bf4fac900434fac777) - **clickhouse**: COMMENT property in CTAS needs to come last *(PR [#3910](https://github.com/tobymao/sqlglot/pull/3910) by [@georgesittas](https://github.com/georgesittas))*


## [v25.11.2] - 2024-08-14
### :bug: Bug Fixes
- [`c22f411`](https://github.com/tobymao/sqlglot/commit/c22f41129985ecfd3b3906b9594ca1692b91708c) - **clickhouse**: ensure we generate the Table in creatable_sql if it represents a db ref *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`19eee93`](https://github.com/tobymao/sqlglot/commit/19eee93c8027e6c612611d3b54980e193e0b6f49) - various fixups for unnest(generatedatearray) transpilation *(PR [#3906](https://github.com/tobymao/sqlglot/pull/3906) by [@georgesittas](https://github.com/georgesittas))*


## [v25.11.1] - 2024-08-13
### :sparkles: New Features
- [`790c1b1`](https://github.com/tobymao/sqlglot/commit/790c1b141d4bc2206df017c70416b589932886a4) - **clickhouse**: support PARTITION BY, SETTINGS in Insert expression *(PR [#3904](https://github.com/tobymao/sqlglot/pull/3904) by [@georgesittas](https://github.com/georgesittas))*


## [v25.11.0] - 2024-08-13
### :boom: BREAKING CHANGES
- due to [`0428c37`](https://github.com/tobymao/sqlglot/commit/0428c37e11f42be8eba352e69c1d2e7425824d38) - Support ALTER VIEW AS SELECT *(PR [#3873](https://github.com/tobymao/sqlglot/pull/3873) by [@xiaohui-sun](https://github.com/xiaohui-sun))*:

  Support ALTER VIEW AS SELECT (#3873)

- due to [`a666117`](https://github.com/tobymao/sqlglot/commit/a666117dcb887031f5995c50d687405b9c145fbd) - parse v NOT IN (subquery) as v <> ALL (subquery) *(PR [#3891](https://github.com/tobymao/sqlglot/pull/3891) by [@georgesittas](https://github.com/georgesittas))*:

  parse v NOT IN (subquery) as v <> ALL (subquery) (#3891)

- due to [`d968932`](https://github.com/tobymao/sqlglot/commit/d968932ef742e97ccf3ec6cdca0bc3319830f0a9) - treat identifiers as case-sensitive, handle EMPTY table property, generate DateStrToDate *(PR [#3895](https://github.com/tobymao/sqlglot/pull/3895) by [@jwhitaker-gridcog](https://github.com/jwhitaker-gridcog))*:

  treat identifiers as case-sensitive, handle EMPTY table property, generate DateStrToDate (#3895)

- due to [`1d7319a`](https://github.com/tobymao/sqlglot/commit/1d7319a8425aace6c11f59552fdd19bdbf5efd03) - transpile Unnest(GenerateDateArray(...)) to various dialects *(PR [#3899](https://github.com/tobymao/sqlglot/pull/3899) by [@georgesittas](https://github.com/georgesittas))*:

  transpile Unnest(GenerateDateArray(...)) to various dialects (#3899)


### :sparkles: New Features
- [`0428c37`](https://github.com/tobymao/sqlglot/commit/0428c37e11f42be8eba352e69c1d2e7425824d38) - **parser**: Support ALTER VIEW AS SELECT *(PR [#3873](https://github.com/tobymao/sqlglot/pull/3873) by [@xiaohui-sun](https://github.com/xiaohui-sun))*
- [`8a48458`](https://github.com/tobymao/sqlglot/commit/8a48458e20e6d0833638e750565da138bdcd5d55) - **athena**: parse UNLOAD into exp.Command closes [#3896](https://github.com/tobymao/sqlglot/pull/3896) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6f1527f`](https://github.com/tobymao/sqlglot/commit/6f1527fd3ebd16f49edb351f050a1db687824530) - **bigquery**: transpile format_datetime, datetime_trunc to duckdb *(PR [#3894](https://github.com/tobymao/sqlglot/pull/3894) by [@skadel](https://github.com/skadel))*
- [`1d7319a`](https://github.com/tobymao/sqlglot/commit/1d7319a8425aace6c11f59552fdd19bdbf5efd03) - transpile Unnest(GenerateDateArray(...)) to various dialects *(PR [#3899](https://github.com/tobymao/sqlglot/pull/3899) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`2cac14f`](https://github.com/tobymao/sqlglot/commit/2cac14f480dcaf458b1eb36b694770ce24f56e61) - generate set ops in ALTER VIEW AS statement *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a666117`](https://github.com/tobymao/sqlglot/commit/a666117dcb887031f5995c50d687405b9c145fbd) - **snowflake**: parse v NOT IN (subquery) as v <> ALL (subquery) *(PR [#3891](https://github.com/tobymao/sqlglot/pull/3891) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3890](https://github.com/tobymao/sqlglot/issues/3890) opened by [@ajuszczak](https://github.com/ajuszczak)*
- [`924a4af`](https://github.com/tobymao/sqlglot/commit/924a4af146952e84688fdccb7b63883fcd7fb255) - **oracle**: preserve function-style MOD syntax fixes [#3897](https://github.com/tobymao/sqlglot/pull/3897) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`d968932`](https://github.com/tobymao/sqlglot/commit/d968932ef742e97ccf3ec6cdca0bc3319830f0a9) - **clickhouse**: treat identifiers as case-sensitive, handle EMPTY table property, generate DateStrToDate *(PR [#3895](https://github.com/tobymao/sqlglot/pull/3895) by [@jwhitaker-gridcog](https://github.com/jwhitaker-gridcog))*
- [`3e5e730`](https://github.com/tobymao/sqlglot/commit/3e5e7300ec184024f871669db48d68476b3fa4df) - **clickhouse**: generate exp.Values correctly, handle `FORMAT Values` *(PR [#3900](https://github.com/tobymao/sqlglot/pull/3900) by [@georgesittas](https://github.com/georgesittas))*
- [`bea3c08`](https://github.com/tobymao/sqlglot/commit/bea3c08e46a020d8545b702c77f0db18c99f1c55) - **parser**: improve performance of OUTER/CROSS APPLY parsing *(PR [#3901](https://github.com/tobymao/sqlglot/pull/3901) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3898](https://github.com/tobymao/sqlglot/issues/3898) opened by [@ewhitley](https://github.com/ewhitley)*


## [v25.10.0] - 2024-08-08
### :boom: BREAKING CHANGES
- due to [`3eb46db`](https://github.com/tobymao/sqlglot/commit/3eb46db5c429f50b5bb6c0c5517a5f7c1084b5ea) - switch off CSV file schema inference by default *(PR [#3879](https://github.com/tobymao/sqlglot/pull/3879) by [@georgesittas](https://github.com/georgesittas))*:

  switch off CSV file schema inference by default (#3879)


### :sparkles: New Features
- [`3e4fcf7`](https://github.com/tobymao/sqlglot/commit/3e4fcf7e8f6a322c14470de6c5dbba152bc9b2fe) - **databricks**: Add support for STREAMING tables *(PR [#3878](https://github.com/tobymao/sqlglot/pull/3878) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3876](https://github.com/tobymao/sqlglot/issues/3876) opened by [@ericvergnaud](https://github.com/ericvergnaud)*
- [`528f690`](https://github.com/tobymao/sqlglot/commit/528f6908001db2f132edfa3c61c21815f7e9dc2f) - **duckdb**: Transpile Snowflake's CONVERT_TIMEZONE 3-arg version *(PR [#3883](https://github.com/tobymao/sqlglot/pull/3883) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3875](https://github.com/tobymao/sqlglot/issues/3875) opened by [@milonimrod](https://github.com/milonimrod)*
- [`411f62a`](https://github.com/tobymao/sqlglot/commit/411f62ad27f8cbe0d9a429e0cafdf4bd9eb2749f) - **bigquery**: Support for GENERATE_TIMESTAMP_ARRAY, DDB transpilation *(PR [#3888](https://github.com/tobymao/sqlglot/pull/3888) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`7169e6e`](https://github.com/tobymao/sqlglot/commit/7169e6ef52d24754059b9ee4324398d22ddff0da) - **bigquery**: ensure Funcs are preserved when used as Tables *(PR [#3877](https://github.com/tobymao/sqlglot/pull/3877) by [@georgesittas](https://github.com/georgesittas))*
- [`62ceed2`](https://github.com/tobymao/sqlglot/commit/62ceed2fa3cd7b41919839d837b860f3814fa769) - **redshift**: parse first arg in DATE_PART into a Var fixes [#3882](https://github.com/tobymao/sqlglot/pull/3882) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`2ad9bfe`](https://github.com/tobymao/sqlglot/commit/2ad9bfef71ae707b83f604f16b47aa583d082c3b) - **snowflake**: support table qualification in USING clause *(PR [#3885](https://github.com/tobymao/sqlglot/pull/3885) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3881](https://github.com/tobymao/sqlglot/issues/3881) opened by [@dlahyani](https://github.com/dlahyani)*
- [`ef16b1d`](https://github.com/tobymao/sqlglot/commit/ef16b1da6b43647a0ca08d69eaf3610e3b72671f) - Fix COLLATE's RHS parsing *(PR [#3887](https://github.com/tobymao/sqlglot/pull/3887) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3880](https://github.com/tobymao/sqlglot/issues/3880) opened by [@ewhitley](https://github.com/ewhitley)*

### :recycle: Refactors
- [`3eb46db`](https://github.com/tobymao/sqlglot/commit/3eb46db5c429f50b5bb6c0c5517a5f7c1084b5ea) - **optimizer**: switch off CSV file schema inference by default *(PR [#3879](https://github.com/tobymao/sqlglot/pull/3879) by [@georgesittas](https://github.com/georgesittas))*


## [v25.9.0] - 2024-08-05
### :boom: BREAKING CHANGES
- due to [`64e187c`](https://github.com/tobymao/sqlglot/commit/64e187c52cd9725ba79e6afbd444382eba9e5827) - transpile postgres impliclitly exploding GENERATE_SERIES proje… *(PR [#3853](https://github.com/tobymao/sqlglot/pull/3853) by [@georgesittas](https://github.com/georgesittas))*:

  transpile postgres impliclitly exploding GENERATE_SERIES proje… (#3853)

- due to [`e53e7cc`](https://github.com/tobymao/sqlglot/commit/e53e7cc02a224563d0a61b0a39298d606b9bac80) - Generation of exp.ArrayConcat for 2-arg based dialects *(PR [#3864](https://github.com/tobymao/sqlglot/pull/3864) by [@VaggelisD](https://github.com/VaggelisD))*:

  Generation of exp.ArrayConcat for 2-arg based dialects (#3864)

- due to [`659b8bf`](https://github.com/tobymao/sqlglot/commit/659b8bf12e396856d1562ee4678b4f687629e081) - Support for BQ's exp.GenerateDateArray generation *(PR [#3865](https://github.com/tobymao/sqlglot/pull/3865) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support for BQ's exp.GenerateDateArray generation (#3865)


### :sparkles: New Features
- [`6afed2a`](https://github.com/tobymao/sqlglot/commit/6afed2aecc0ce186ff6c484b1ad32ac6a2fb61bc) - **duckdb**: Support for exp.TimeDiff generation *(PR [#3856](https://github.com/tobymao/sqlglot/pull/3856) by [@VaggelisD](https://github.com/VaggelisD))*
- [`64e187c`](https://github.com/tobymao/sqlglot/commit/64e187c52cd9725ba79e6afbd444382eba9e5827) - transpile postgres impliclitly exploding GENERATE_SERIES proje… *(PR [#3853](https://github.com/tobymao/sqlglot/pull/3853) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3818](https://github.com/tobymao/sqlglot/issues/3818) opened by [@wojciechowski-p](https://github.com/wojciechowski-p)*
- [`8a948c8`](https://github.com/tobymao/sqlglot/commit/8a948c805f7534e266557e1aa08bee0982340685) - **teradata**: Parse RENAME TABLE as Command *(PR [#3863](https://github.com/tobymao/sqlglot/pull/3863) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3861](https://github.com/tobymao/sqlglot/issues/3861) opened by [@EdouardW](https://github.com/EdouardW)*
- [`659b8bf`](https://github.com/tobymao/sqlglot/commit/659b8bf12e396856d1562ee4678b4f687629e081) - **duckdb**: Support for BQ's exp.GenerateDateArray generation *(PR [#3865](https://github.com/tobymao/sqlglot/pull/3865) by [@VaggelisD](https://github.com/VaggelisD))*
- [`734f54b`](https://github.com/tobymao/sqlglot/commit/734f54bb6ec697a5213f046fbb1e8174b2c31115) - **snowflake**: add support for a a couple of missing clauses in PIVOT clause *(PR [#3867](https://github.com/tobymao/sqlglot/pull/3867) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`8710763`](https://github.com/tobymao/sqlglot/commit/87107631378b0972115a01cc0bb99dbfc44a66d7) - **presto**: map %W to %A in the TIME_MAPPING *(PR [#3855](https://github.com/tobymao/sqlglot/pull/3855) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3854](https://github.com/tobymao/sqlglot/issues/3854) opened by [@ddelzell](https://github.com/ddelzell)*
- [`532f3c8`](https://github.com/tobymao/sqlglot/commit/532f3c8714220058170790b13977cc66760841dc) - **duckdb**: Add implicit casts to DATE_DIFF *(PR [#3857](https://github.com/tobymao/sqlglot/pull/3857) by [@VaggelisD](https://github.com/VaggelisD))*
- [`299c4a5`](https://github.com/tobymao/sqlglot/commit/299c4a559dd04047d5a4c4691f8965972842fe7d) - **clickhouse**: Fix SETTINGS parsing *(PR [#3859](https://github.com/tobymao/sqlglot/pull/3859) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3858](https://github.com/tobymao/sqlglot/issues/3858) opened by [@obazna](https://github.com/obazna)*
- [`810d23d`](https://github.com/tobymao/sqlglot/commit/810d23d4e42f9a7de83015ec425dff9223598219) - **parser**: make assignment parsing more lenient by allowing keyword in LHS *(PR [#3866](https://github.com/tobymao/sqlglot/pull/3866) by [@georgesittas](https://github.com/georgesittas))*
- [`e53e7cc`](https://github.com/tobymao/sqlglot/commit/e53e7cc02a224563d0a61b0a39298d606b9bac80) - Generation of exp.ArrayConcat for 2-arg based dialects *(PR [#3864](https://github.com/tobymao/sqlglot/pull/3864) by [@VaggelisD](https://github.com/VaggelisD))*
- [`813f127`](https://github.com/tobymao/sqlglot/commit/813f127b293e7087d174f3f632b65ba7b24bc9e3) - **duckdb**: Allow DESCRIBE as a _parse_select() path *(PR [#3871](https://github.com/tobymao/sqlglot/pull/3871) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3869](https://github.com/tobymao/sqlglot/issues/3869) opened by [@cpcloud](https://github.com/cpcloud)*
- [`6ff0c01`](https://github.com/tobymao/sqlglot/commit/6ff0c01a5b8b19e3090b8cf08aabbb4b27425abb) - Fixed size array parsing *(PR [#3870](https://github.com/tobymao/sqlglot/pull/3870) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3868](https://github.com/tobymao/sqlglot/issues/3868) opened by [@tekumara](https://github.com/tekumara)*


## [v25.8.1] - 2024-07-30
### :bug: Bug Fixes
- [`a295b3a`](https://github.com/tobymao/sqlglot/commit/a295b3adbef0eff0b3f6c3b8b97b1eaa8c13f144) - **tsql**: regression related to CTEs in CREATE VIEW AS statements *(PR [#3852](https://github.com/tobymao/sqlglot/pull/3852) by [@georgesittas](https://github.com/georgesittas))*


## [v25.8.0] - 2024-07-29
### :sparkles: New Features
- [`e37d63a`](https://github.com/tobymao/sqlglot/commit/e37d63a17d4709135c1de7876b2898cf7bd2e641) - **bigquery**: add support for BYTEINT closes [#3838](https://github.com/tobymao/sqlglot/pull/3838) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`4c912cd`](https://github.com/tobymao/sqlglot/commit/4c912cd2302874b8abeed3cafa93ff3771b8dcba) - **clickhouse**: improve parsing/transpilation of StrToDate *(PR [#3839](https://github.com/tobymao/sqlglot/pull/3839) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3837](https://github.com/tobymao/sqlglot/issues/3837) opened by [@ace-xc](https://github.com/ace-xc)*
- [`45f45ea`](https://github.com/tobymao/sqlglot/commit/45f45eaaac5a9130168dddaef4713542886a83cb) - **duckdb**: add support for SUMMARIZE *(PR [#3840](https://github.com/tobymao/sqlglot/pull/3840) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3823](https://github.com/tobymao/sqlglot/issues/3823) opened by [@cpcloud](https://github.com/cpcloud)*

### :bug: Bug Fixes
- [`57ecc84`](https://github.com/tobymao/sqlglot/commit/57ecc8465a3c4d1e0ab1db71dc185c80efc5d0aa) - **duckdb**: wrap left IN clause json extract arrow operand fixes [#3836](https://github.com/tobymao/sqlglot/pull/3836) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`2ffb070`](https://github.com/tobymao/sqlglot/commit/2ffb07070952cde7ac9a1883cbf9b4c477c55abb) - **duckdb**: allow fixed length array casts closes [#3841](https://github.com/tobymao/sqlglot/pull/3841) *(PR [#3842](https://github.com/tobymao/sqlglot/pull/3842) by [@tobymao](https://github.com/tobymao))*
- [`d71eb4e`](https://github.com/tobymao/sqlglot/commit/d71eb4ebc2a0f82c567b32de51298f0d82f400a1) - pretty gen for tuples *(commit by [@tobymao](https://github.com/tobymao))*
- [`12ae9cd`](https://github.com/tobymao/sqlglot/commit/12ae9cdc1c1f52735f8c60488b5d98a4872bf764) - **tsql**: handle JSON_QUERY with a single argument *(PR [#3847](https://github.com/tobymao/sqlglot/pull/3847) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3843](https://github.com/tobymao/sqlglot/issues/3843) opened by [@zachary62](https://github.com/zachary62)*
- [`f8ca6b4`](https://github.com/tobymao/sqlglot/commit/f8ca6b4048ee22585cd7635f83b25fe2df9bd748) - **tsql**: bubble up exp.Create CTEs to improve transpilability *(PR [#3848](https://github.com/tobymao/sqlglot/pull/3848) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3844](https://github.com/tobymao/sqlglot/issues/3844) opened by [@zachary62](https://github.com/zachary62)*
- [`89976c1`](https://github.com/tobymao/sqlglot/commit/89976c1dbb61bdfe3bbb98702b18365e90a69acb) - **parser**: allow 'cube' to be used for identifiers *(PR [#3850](https://github.com/tobymao/sqlglot/pull/3850) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`d00ea9c`](https://github.com/tobymao/sqlglot/commit/d00ea9c4d39f686fabbe864e88cfe5c071fd4f66) - exclude boolean args in Generator.format_args *(PR [#3849](https://github.com/tobymao/sqlglot/pull/3849) by [@georgesittas](https://github.com/georgesittas))*


## [v25.7.1] - 2024-07-25
### :bug: Bug Fixes
- [`ae95c18`](https://github.com/tobymao/sqlglot/commit/ae95c18f636d34c7f92b48cd5970f4fa6ad81b08) - alter table add columns closes [#3835](https://github.com/tobymao/sqlglot/pull/3835) *(commit by [@tobymao](https://github.com/tobymao))*
- [`9b5839d`](https://github.com/tobymao/sqlglot/commit/9b5839d7fb04f78c9ef50b112cd9d4d24558c912) - make ast consistent *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.7.0] - 2024-07-25
### :sparkles: New Features
- [`ba0aa50`](https://github.com/tobymao/sqlglot/commit/ba0aa50072f623c299eb4d2dbb69993541fff27b) - **duckdb**: Transpile BQ's exp.DatetimeAdd, exp.DatetimeSub *(PR [#3777](https://github.com/tobymao/sqlglot/pull/3777) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5da91fb`](https://github.com/tobymao/sqlglot/commit/5da91fb50d0f8029ddda16040ebd316c1a651e2d) - **postgres**: Support for CREATE INDEX CONCURRENTLY *(PR [#3787](https://github.com/tobymao/sqlglot/pull/3787) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3783](https://github.com/tobymao/sqlglot/issues/3783) opened by [@EdgyEdgemond](https://github.com/EdgyEdgemond)*
- [`00722eb`](https://github.com/tobymao/sqlglot/commit/00722eb41795e7454d0ecb4c3d0e1caf96a19465) - Move ANNOTATORS to Dialect for dialect-aware annotation *(PR [#3786](https://github.com/tobymao/sqlglot/pull/3786) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3778](https://github.com/tobymao/sqlglot/issues/3778) opened by [@ddelzell](https://github.com/ddelzell)*
- [`a6d84fb`](https://github.com/tobymao/sqlglot/commit/a6d84fbd9b4120f42b31bb01d4bf3e6258e51562) - **postgres**: Parse TO_DATE as exp.StrToDate *(PR [#3799](https://github.com/tobymao/sqlglot/pull/3799) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3797](https://github.com/tobymao/sqlglot/issues/3797) opened by [@dioptre](https://github.com/dioptre)*
- [`3582644`](https://github.com/tobymao/sqlglot/commit/358264478e5449b7e4ebddce1cc463d140f266f5) - **hive, spark, db**: Support for exp.GenerateSeries *(PR [#3798](https://github.com/tobymao/sqlglot/pull/3798) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3793](https://github.com/tobymao/sqlglot/issues/3793) opened by [@wojciechowski-p](https://github.com/wojciechowski-p)*
- [`80b4a12`](https://github.com/tobymao/sqlglot/commit/80b4a12b779b661e42d31cf75ead8aff25257f8a) - **tsql**: Support for COLUMNSTORE option on CREATE INDEX *(PR [#3805](https://github.com/tobymao/sqlglot/pull/3805) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3801](https://github.com/tobymao/sqlglot/issues/3801) opened by [@na399](https://github.com/na399)*
- [`bf6c126`](https://github.com/tobymao/sqlglot/commit/bf6c12687f3ed032ea7be40875c19fc00e5927ad) - **databricks**: Support USE CATALOG *(PR [#3812](https://github.com/tobymao/sqlglot/pull/3812) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3811](https://github.com/tobymao/sqlglot/issues/3811) opened by [@grusin-db](https://github.com/grusin-db)*
- [`624d411`](https://github.com/tobymao/sqlglot/commit/624d4115e3ee4b8db2dbf2970bf0047e14b23e92) - **snowflake**: Support for OBJECT_INSERT, transpile to DDB *(PR [#3807](https://github.com/tobymao/sqlglot/pull/3807) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3802](https://github.com/tobymao/sqlglot/issues/3802) opened by [@buremba](https://github.com/buremba)*
- [`5b393fb`](https://github.com/tobymao/sqlglot/commit/5b393fb4d2db47b9229ca12a03aba82cdd510615) - **postgres**: Add missing constraint options *(PR [#3816](https://github.com/tobymao/sqlglot/pull/3816) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3814](https://github.com/tobymao/sqlglot/issues/3814) opened by [@DTovstohan](https://github.com/DTovstohan)*

### :bug: Bug Fixes
- [`898f523`](https://github.com/tobymao/sqlglot/commit/898f523a8db9f73b59055f1e38cf4acb07157f00) - **duckdb**: Wrap JSON_EXTRACT if it's subscripted *(PR [#3785](https://github.com/tobymao/sqlglot/pull/3785) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3782](https://github.com/tobymao/sqlglot/issues/3782) opened by [@egan8888](https://github.com/egan8888)*
- [`db3748d`](https://github.com/tobymao/sqlglot/commit/db3748d56b138a6427d6f4fc3e32c895ffb993fa) - **mysql**: don't wrap VALUES clause *(PR [#3792](https://github.com/tobymao/sqlglot/pull/3792) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3789](https://github.com/tobymao/sqlglot/issues/3789) opened by [@stephenprater](https://github.com/stephenprater)*
- [`44d6506`](https://github.com/tobymao/sqlglot/commit/44d650637d5d7a662b57ec1d8ca74dffe0f7ad73) - with as comments closes [#3794](https://github.com/tobymao/sqlglot/pull/3794) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8ca6a61`](https://github.com/tobymao/sqlglot/commit/8ca6a613692e7339717c449ba6966d7c2911b584) - **tsql**: Fix roundtrip of exp.Stddev *(PR [#3806](https://github.com/tobymao/sqlglot/pull/3806) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3804](https://github.com/tobymao/sqlglot/issues/3804) opened by [@JonaGeishauser](https://github.com/JonaGeishauser)*
- [`8551063`](https://github.com/tobymao/sqlglot/commit/855106377c97ee313b45046041fafabb2810dab2) - **duckdb**: Fix STRUCT_PACK -> ROW due to is_struct_cast *(PR [#3809](https://github.com/tobymao/sqlglot/pull/3809) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3808](https://github.com/tobymao/sqlglot/issues/3808) opened by [@aersam](https://github.com/aersam)*
- [`98f80ed`](https://github.com/tobymao/sqlglot/commit/98f80eda3863b5ff40d566330e6ab35a99f569ca) - **clickhouse**: allow like as an identifier closes [#3813](https://github.com/tobymao/sqlglot/pull/3813) *(commit by [@tobymao](https://github.com/tobymao))*
- [`556ba35`](https://github.com/tobymao/sqlglot/commit/556ba35e4ce9efa51561ef0578bfb24a51ce4dcd) - allow parse_identifier to handle single quotes *(commit by [@tobymao](https://github.com/tobymao))*
- [`f9810d2`](https://github.com/tobymao/sqlglot/commit/f9810d213f3992881fc13291a681da6553701083) - **snowflake**: Don't consume LPAREN when parsing staged file path *(PR [#3815](https://github.com/tobymao/sqlglot/pull/3815) by [@VaggelisD](https://github.com/VaggelisD))*
- [`416f4a1`](https://github.com/tobymao/sqlglot/commit/416f4a1b6a04b858ff8ed94509aacd9bacca145b) - **postgres**: Fix COLLATE column constraint *(PR [#3820](https://github.com/tobymao/sqlglot/pull/3820) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3817](https://github.com/tobymao/sqlglot/issues/3817) opened by [@DTovstohan](https://github.com/DTovstohan)*
- [`69b9395`](https://github.com/tobymao/sqlglot/commit/69b93953c35bd7f1d53cf15d9937117edb38f512) - Do not preemptively consume SELECT [ALL] if ALL is connected *(PR [#3822](https://github.com/tobymao/sqlglot/pull/3822) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3819](https://github.com/tobymao/sqlglot/issues/3819) opened by [@nfx](https://github.com/nfx)*
- [`1c19abe`](https://github.com/tobymao/sqlglot/commit/1c19abe5b3f3187a2e0ba420cf8c5e5b5ecc788e) - **presto, trino**: Fix StrToUnix transpilation *(PR [#3824](https://github.com/tobymao/sqlglot/pull/3824) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3796](https://github.com/tobymao/sqlglot/issues/3796) opened by [@ddelzell](https://github.com/ddelzell)*


## [v25.6.1] - 2024-07-18
### :bug: Bug Fixes
- [`19370d5`](https://github.com/tobymao/sqlglot/commit/19370d5d16b555e25def503323ec3dc4e5d40e6c) - **postgres**: Decouple UNIQUE from DEFAULT constraints *(PR [#3775](https://github.com/tobymao/sqlglot/pull/3775) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3774](https://github.com/tobymao/sqlglot/issues/3774) opened by [@EdgyEdgemond](https://github.com/EdgyEdgemond)*
- [`e99146b`](https://github.com/tobymao/sqlglot/commit/e99146b0989599772c020905f69496ea80e7e2e5) - make copy a dml statement for qualify_tables *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.6.0] - 2024-07-17
### :boom: BREAKING CHANGES
- due to [`89fc63c`](https://github.com/tobymao/sqlglot/commit/89fc63c5831dc5d63feff9e39fea1e90d65e9a09) - QUALIFY comes after WINDOW clause in queries *(PR [#3745](https://github.com/tobymao/sqlglot/pull/3745) by [@georgesittas](https://github.com/georgesittas))*:

  QUALIFY comes after WINDOW clause in queries (#3745)

- due to [`a2a6efb`](https://github.com/tobymao/sqlglot/commit/a2a6efb45dc0f380747aa4afdaa19122389f3c28) - Canonicalize struct & array inline constructor *(PR [#3751](https://github.com/tobymao/sqlglot/pull/3751) by [@VaggelisD](https://github.com/VaggelisD))*:

  Canonicalize struct & array inline constructor (#3751)


### :sparkles: New Features
- [`e9c4bbb`](https://github.com/tobymao/sqlglot/commit/e9c4bbbb0d0a03d1b1efaad9abe0068b3b7efa9d) - Support for ORDER BY ALL *(PR [#3756](https://github.com/tobymao/sqlglot/pull/3756) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3755](https://github.com/tobymao/sqlglot/issues/3755) opened by [@Hunterlige](https://github.com/Hunterlige)*
- [`4a843e6`](https://github.com/tobymao/sqlglot/commit/4a843e6cca7bcc0d9956fe975dbc77e67038f1b8) - **postgres**: Support FROM ROWS FROM (...) *(PR [#3753](https://github.com/tobymao/sqlglot/pull/3753) by [@VaggelisD](https://github.com/VaggelisD))*
- [`321051a`](https://github.com/tobymao/sqlglot/commit/321051aef30f11f2778444040a2078633e617144) - **presto, trino**: Add support for exp.TimestampAdd *(PR [#3765](https://github.com/tobymao/sqlglot/pull/3765) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3762](https://github.com/tobymao/sqlglot/issues/3762) opened by [@ddelzell](https://github.com/ddelzell)*
- [`82a1bb4`](https://github.com/tobymao/sqlglot/commit/82a1bb42856d628651bb5f1ef9aa8f440736c450) - Support for RPAD & LPAD functions *(PR [#3757](https://github.com/tobymao/sqlglot/pull/3757) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`89fc63c`](https://github.com/tobymao/sqlglot/commit/89fc63c5831dc5d63feff9e39fea1e90d65e9a09) - **duckdb, clickhouse**: QUALIFY comes after WINDOW clause in queries *(PR [#3745](https://github.com/tobymao/sqlglot/pull/3745) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3744](https://github.com/tobymao/sqlglot/issues/3744) opened by [@taylorbarstow](https://github.com/taylorbarstow)*
- [`15ca924`](https://github.com/tobymao/sqlglot/commit/15ca924ac6e8a72396a882c394856e466cae9ac3) - **optimizer**: Fix expansion of SELECT * REPLACE, RENAME *(PR [#3742](https://github.com/tobymao/sqlglot/pull/3742) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0363fef`](https://github.com/tobymao/sqlglot/commit/0363fefd3ddd490ddddae47f7eb0192f0ff3cc5e) - attach comments to Commands *(PR [#3758](https://github.com/tobymao/sqlglot/pull/3758) by [@georgesittas](https://github.com/georgesittas))*
- [`a2a6efb`](https://github.com/tobymao/sqlglot/commit/a2a6efb45dc0f380747aa4afdaa19122389f3c28) - **bigquery**: Canonicalize struct & array inline constructor *(PR [#3751](https://github.com/tobymao/sqlglot/pull/3751) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5df3f52`](https://github.com/tobymao/sqlglot/commit/5df3f5292488df6a8e21abf3b49086c823797e78) - Remove number matching from COLON placeholder parser *(PR [#3761](https://github.com/tobymao/sqlglot/pull/3761) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3759](https://github.com/tobymao/sqlglot/issues/3759) opened by [@egan8888](https://github.com/egan8888)*
- [`0606af6`](https://github.com/tobymao/sqlglot/commit/0606af66dba7c290fee65926dcb74baad82c84ac) - **duckdb**: Transpile UDFs from Databricks *(PR [#3768](https://github.com/tobymao/sqlglot/pull/3768) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3764](https://github.com/tobymao/sqlglot/issues/3764) opened by [@aersam](https://github.com/aersam)*
- [`dcc783a`](https://github.com/tobymao/sqlglot/commit/dcc783aad7c2e7184224e90fed7710eb08ddc76a) - **clickhouse**: Allow TokenType.SELECT as a Tuple field identifier *(PR [#3766](https://github.com/tobymao/sqlglot/pull/3766) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3763](https://github.com/tobymao/sqlglot/issues/3763) opened by [@cpcloud](https://github.com/cpcloud)*
- [`b42b7ac`](https://github.com/tobymao/sqlglot/commit/b42b7ac5bb1785a9028235c1557b9842ea1d7524) - extract from time/date *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.5.1] - 2024-07-05
### :bug: Bug Fixes
- [`2bdde22`](https://github.com/tobymao/sqlglot/commit/2bdde2221b8017791ce4cc619abb2706464ca408) - **optimizer**: only qualify coalesced USING columns if they exist in table schemas *(PR [#3740](https://github.com/tobymao/sqlglot/pull/3740) by [@georgesittas](https://github.com/georgesittas))*


## [v25.5.0] - 2024-07-04
### :boom: BREAKING CHANGES
- due to [`8335ba1`](https://github.com/tobymao/sqlglot/commit/8335ba10e60c7c63881d7559a6f1fada11b0e55d) - preserve EXTRACT(date_part FROM datetime) calls *(PR [#3729](https://github.com/tobymao/sqlglot/pull/3729) by [@georgesittas](https://github.com/georgesittas))*:

  preserve EXTRACT(date_part FROM datetime) calls (#3729)

- due to [`fb066a6`](https://github.com/tobymao/sqlglot/commit/fb066a6167e1f887bd8c1a1369d063fe70f36a8a) - Decouple NVL() from COALESCE() *(PR [#3734](https://github.com/tobymao/sqlglot/pull/3734) by [@VaggelisD](https://github.com/VaggelisD))*:

  Decouple NVL() from COALESCE() (#3734)


### :sparkles: New Features
- [`0c03299`](https://github.com/tobymao/sqlglot/commit/0c032992fac622ebaee114cd9f6e405be1820054) - **teradata**: random lower upper closes [#3721](https://github.com/tobymao/sqlglot/pull/3721) *(commit by [@tobymao](https://github.com/tobymao))*
- [`37b6e2d`](https://github.com/tobymao/sqlglot/commit/37b6e2d806f6da1338c75803919c602f8705acac) - **snowflake**: add support for VECTOR(type, size) *(PR [#3724](https://github.com/tobymao/sqlglot/pull/3724) by [@georgesittas](https://github.com/georgesittas))*
- [`1e07c4d`](https://github.com/tobymao/sqlglot/commit/1e07c4d29a43192fb57c120f3b9c1c2fa27d0fa6) - **presto, trino**: Configurable transpilation of Snowflake VARIANT *(PR [#3725](https://github.com/tobymao/sqlglot/pull/3725) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3713](https://github.com/tobymao/sqlglot/issues/3713) opened by [@Leonti](https://github.com/Leonti)*
- [`e5a53aa`](https://github.com/tobymao/sqlglot/commit/e5a53aaa015806574cd3c4bbe46b5788e960903e) - **snowflake**: Support for FROM CHANGES *(PR [#3731](https://github.com/tobymao/sqlglot/pull/3731) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3730](https://github.com/tobymao/sqlglot/issues/3730) opened by [@achicoine-coveo](https://github.com/achicoine-coveo)*
- [`820d664`](https://github.com/tobymao/sqlglot/commit/820d66430bb23bff88d0057b22842d313e1431c5) - **presto**: wrap md5 string arguments in to_utf8 *(PR [#3732](https://github.com/tobymao/sqlglot/pull/3732) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2855](https://github.com/TobikoData/sqlmesh/issues/2855) opened by [@MikeWallis42](https://github.com/MikeWallis42)*
- [`912bc84`](https://github.com/tobymao/sqlglot/commit/912bc84791008ecce545cfbd3b0c9d4362131eb3) - **spark, databricks**: Support view schema binding options *(PR [#3739](https://github.com/tobymao/sqlglot/pull/3739) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3738](https://github.com/tobymao/sqlglot/issues/3738) opened by [@aersam](https://github.com/aersam)*

### :bug: Bug Fixes
- [`3454f86`](https://github.com/tobymao/sqlglot/commit/3454f861f22f680f6b8c18cca466154d3b9fe8d1) - **teradata**: use timestamp with time zone over timestamptz *(PR [#3723](https://github.com/tobymao/sqlglot/pull/3723) by [@mtagle](https://github.com/mtagle))*
  - :arrow_lower_right: *fixes issue [#3722](https://github.com/tobymao/sqlglot/issues/3722) opened by [@mtagle](https://github.com/mtagle)*
- [`f4a2872`](https://github.com/tobymao/sqlglot/commit/f4a28721fd33edb3178c1d99746209dadfbba487) - **clickhouse**: switch off table alias columns generation *(PR [#3727](https://github.com/tobymao/sqlglot/pull/3727) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3726](https://github.com/tobymao/sqlglot/issues/3726) opened by [@cpcloud](https://github.com/cpcloud)*
- [`8335ba1`](https://github.com/tobymao/sqlglot/commit/8335ba10e60c7c63881d7559a6f1fada11b0e55d) - **clickhouse**: preserve EXTRACT(date_part FROM datetime) calls *(PR [#3729](https://github.com/tobymao/sqlglot/pull/3729) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3728](https://github.com/tobymao/sqlglot/issues/3728) opened by [@cpcloud](https://github.com/cpcloud)*
- [`fb066a6`](https://github.com/tobymao/sqlglot/commit/fb066a6167e1f887bd8c1a1369d063fe70f36a8a) - **oracle**: Decouple NVL() from COALESCE() *(PR [#3734](https://github.com/tobymao/sqlglot/pull/3734) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3733](https://github.com/tobymao/sqlglot/issues/3733) opened by [@Hal-H2Apps](https://github.com/Hal-H2Apps)*
- [`c790c3b`](https://github.com/tobymao/sqlglot/commit/c790c3b1fa274d7b0faf9f75e7dbc62bc4f55c67) - **tsql**: parse rhs of x::varchar(max) into a type *(PR [#3737](https://github.com/tobymao/sqlglot/pull/3737) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`84416d2`](https://github.com/tobymao/sqlglot/commit/84416d207a2e397aba12a4138fcbd1fab382c22d) - **teradata**: clean up CurrentTimestamp generation logic *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.4.1] - 2024-06-29
### :bug: Bug Fixes
- [`6bf9853`](https://github.com/tobymao/sqlglot/commit/6bf9853fd0b26d5a4e93e37447c3b275cd108872) - **tsql**: cast shorthand closes [#3760](https://github.com/tobymao/sqlglot/pull/3760) *(PR [#3720](https://github.com/tobymao/sqlglot/pull/3720) by [@tobymao](https://github.com/tobymao))*


## [v25.4.0] - 2024-06-28
### :boom: BREAKING CHANGES
- due to [`9fb1d79`](https://github.com/tobymao/sqlglot/commit/9fb1d79398769edb452e075eb3b6416e69f239bf) - extract unit should be a var, not a column *(PR [#3712](https://github.com/tobymao/sqlglot/pull/3712) by [@tobymao](https://github.com/tobymao))*:

  extract unit should be a var, not a column (#3712)

- due to [`ae1816f`](https://github.com/tobymao/sqlglot/commit/ae1816fc71a5a164d1aae6644a9c3bc4cec484d2) - simplify no longer removes neg, add to_py *(PR [#3714](https://github.com/tobymao/sqlglot/pull/3714) by [@tobymao](https://github.com/tobymao))*:

  simplify no longer removes neg, add to_py (#3714)

- due to [`beaf9cc`](https://github.com/tobymao/sqlglot/commit/beaf9cc1f07ff4223f99c84ad6645d3f29af5801) - coalesce left-hand side of join conditions produced by expanding USING *(PR [#3715](https://github.com/tobymao/sqlglot/pull/3715) by [@georgesittas](https://github.com/georgesittas))*:

  coalesce left-hand side of join conditions produced by expanding USING (#3715)


### :sparkles: New Features
- [`97739fe`](https://github.com/tobymao/sqlglot/commit/97739fe692a883a45247d92b2a3efaed33c4b5bf) - add Select expression parser *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1c2279c`](https://github.com/tobymao/sqlglot/commit/1c2279c0659d5cbe30c19afee85308ef7bf4c9c5) - **duckdb**: Transpile exp.Length from other dialects *(PR [#3708](https://github.com/tobymao/sqlglot/pull/3708) by [@VaggelisD](https://github.com/VaggelisD))*
- [`23dac71`](https://github.com/tobymao/sqlglot/commit/23dac7147883d559acca7d21e3600c28576ec950) - **snowflake**: add support for CONNECT_BY_ROOT expression *(PR [#3717](https://github.com/tobymao/sqlglot/pull/3717) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3716](https://github.com/tobymao/sqlglot/issues/3716) opened by [@niklaslorenzatalligator](https://github.com/niklaslorenzatalligator)*
- [`4f050e0`](https://github.com/tobymao/sqlglot/commit/4f050e0aefcde8fb3c65abaf49c6aa4e2bbe5e2b) - transpile BigQuery's SAFE_CAST with FORMAT to DuckDB *(PR [#3718](https://github.com/tobymao/sqlglot/pull/3718) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2837](https://github.com/TobikoData/sqlmesh/issues/2837) opened by [@hustic](https://github.com/hustic)*

### :bug: Bug Fixes
- [`3a86d7e`](https://github.com/tobymao/sqlglot/commit/3a86d7e4ec02e96326021c417dc972b64076567f) - non deterministic aggs in planner closes [#3709](https://github.com/tobymao/sqlglot/pull/3709) *(commit by [@tobymao](https://github.com/tobymao))*
- [`3b8568d`](https://github.com/tobymao/sqlglot/commit/3b8568d37792c1916f05faf5df8af1841144b338) - **clickhouse**: extract closes [#3711](https://github.com/tobymao/sqlglot/pull/3711) *(commit by [@tobymao](https://github.com/tobymao))*
- [`9fb1d79`](https://github.com/tobymao/sqlglot/commit/9fb1d79398769edb452e075eb3b6416e69f239bf) - extract unit should be a var, not a column *(PR [#3712](https://github.com/tobymao/sqlglot/pull/3712) by [@tobymao](https://github.com/tobymao))*
- [`ae1816f`](https://github.com/tobymao/sqlglot/commit/ae1816fc71a5a164d1aae6644a9c3bc4cec484d2) - simplify no longer removes neg, add to_py *(PR [#3714](https://github.com/tobymao/sqlglot/pull/3714) by [@tobymao](https://github.com/tobymao))*
- [`beaf9cc`](https://github.com/tobymao/sqlglot/commit/beaf9cc1f07ff4223f99c84ad6645d3f29af5801) - **optimizer**: coalesce left-hand side of join conditions produced by expanding USING *(PR [#3715](https://github.com/tobymao/sqlglot/pull/3715) by [@georgesittas](https://github.com/georgesittas))*


## [v25.3.3] - 2024-06-26
### :recycle: Refactors
- [`972ce7d`](https://github.com/tobymao/sqlglot/commit/972ce7d27d9f083d8ef02ded9278e320da3aa0b6) - control ParseJSON generation logic with a flag *(PR [#3707](https://github.com/tobymao/sqlglot/pull/3707) by [@georgesittas](https://github.com/georgesittas))*


## [v25.3.2] - 2024-06-26
### :sparkles: New Features
- [`a1327c7`](https://github.com/tobymao/sqlglot/commit/a1327c7f4ae74ae25617cd448448ae89c915c744) - **tsql**: Add support for scope qualifier operator *(PR [#3703](https://github.com/tobymao/sqlglot/pull/3703) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#2817](https://github.com/TobikoData/sqlmesh/issues/2817) opened by [@elauser](https://github.com/elauser)*

### :bug: Bug Fixes
- [`842a9f0`](https://github.com/tobymao/sqlglot/commit/842a9f0cf6fd49cf1d6ed31a5ad9b40eaa483bff) - **parser**: preserve Cast expression when it's 'safe' and has a format *(PR [#3705](https://github.com/tobymao/sqlglot/pull/3705) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2827](https://github.com/TobikoData/sqlmesh/issues/2827) opened by [@hustic](https://github.com/hustic)*
- [`fc0411d`](https://github.com/tobymao/sqlglot/commit/fc0411dc6236c040ce12c036e1ce1165a5143fa1) - **parser**: explicitly check for identifiers in _parse_types *(PR [#3704](https://github.com/tobymao/sqlglot/pull/3704) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2826](https://github.com/TobikoData/sqlmesh/issues/2826) opened by [@plaflamme](https://github.com/plaflamme)*

### :recycle: Refactors
- [`e9236e3`](https://github.com/tobymao/sqlglot/commit/e9236e36c94464af21c7e2f35a083eef316feab1) - add EXPAND_ALIAS_REFS_ONLY_IN_GROUP_BY dialect constant *(PR [#3702](https://github.com/tobymao/sqlglot/pull/3702) by [@georgesittas](https://github.com/georgesittas))*
- [`92c6ebb`](https://github.com/tobymao/sqlglot/commit/92c6ebb8f703486cf3132c9d2c3c58568c10aea4) - **tsql**: make ScopeResolution round-trippable *(PR [#3706](https://github.com/tobymao/sqlglot/pull/3706) by [@georgesittas](https://github.com/georgesittas))*


## [v25.3.1] - 2024-06-25
### :sparkles: New Features
- [`4ed02b0`](https://github.com/tobymao/sqlglot/commit/4ed02b0a24eeabf813525ba09d646763970dd33b) - transpile TRY_PARSE_JSON Snowflake -> DuckDB *(PR [#3696](https://github.com/tobymao/sqlglot/pull/3696) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3690](https://github.com/tobymao/sqlglot/issues/3690) opened by [@achicoine-coveo](https://github.com/achicoine-coveo)*
- [`60fa5e3`](https://github.com/tobymao/sqlglot/commit/60fa5e3f8a6eab3abb12064366a6bde907d9e9de) - **snowflake**: add support for dynamic table DDL *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`442c61d`](https://github.com/tobymao/sqlglot/commit/442c61defe05f4c168a7909d0a5fc5c043a2d2b4) - **tokenizer**: don't treat escapes in raw strings as such for some dialects *(PR [#3689](https://github.com/tobymao/sqlglot/pull/3689) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3686](https://github.com/tobymao/sqlglot/issues/3686) opened by [@aersam](https://github.com/aersam)*
- [`f3e928e`](https://github.com/tobymao/sqlglot/commit/f3e928e771e1973a13afe09e4dc295ad492b783f) - **parser**: make parse_var_or_string more lenient *(PR [#3695](https://github.com/tobymao/sqlglot/pull/3695) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3693](https://github.com/tobymao/sqlglot/issues/3693) opened by [@WSKINGS](https://github.com/WSKINGS)*
- [`806a7e4`](https://github.com/tobymao/sqlglot/commit/806a7e421a9b5a54a2859d7bb4c3ea131a4a8640) - remove tokenizer cache for multi-threading *(commit by [@tobymao](https://github.com/tobymao))*
- [`3fba603`](https://github.com/tobymao/sqlglot/commit/3fba6035ac0263beab73ab62013a64a56dea9165) - don't treat /*+ as a HINT token in dialects that don't support hints *(PR [#3697](https://github.com/tobymao/sqlglot/pull/3697) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3692](https://github.com/tobymao/sqlglot/issues/3692) opened by [@sandband](https://github.com/sandband)*
- [`e5d534c`](https://github.com/tobymao/sqlglot/commit/e5d534ce96381f42f26d43c4fcab7eff23946c90) - **optimizer**: Force early alias expansion in BQ & CH *(PR [#3699](https://github.com/tobymao/sqlglot/pull/3699) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3687](https://github.com/tobymao/sqlglot/issues/3687) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`1cfb1ff`](https://github.com/tobymao/sqlglot/commit/1cfb1ff850fb4fcf69fc5962e01c879ce51bec8b) - proper parsing of unit in spark/databricks date_diff *(PR [#3701](https://github.com/tobymao/sqlglot/pull/3701) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3700](https://github.com/tobymao/sqlglot/issues/3700) opened by [@cheesefactory](https://github.com/cheesefactory)*

### :wrench: Chores
- [`8b16199`](https://github.com/tobymao/sqlglot/commit/8b16199af3743aee292df5429e1f0087704e1cbc) - bump sqlglotrs to v0.2.8 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.3.0] - 2024-06-21
### :boom: BREAKING CHANGES
- due to [`84d820f`](https://github.com/tobymao/sqlglot/commit/84d820f96b161fdd5b00f265890b5c75c65a36f0) - Time/Datetime/Timestamp function additions *(PR [#3666](https://github.com/tobymao/sqlglot/pull/3666) by [@VaggelisD](https://github.com/VaggelisD))*:

  Time/Datetime/Timestamp function additions (#3666)

- due to [`acbc81d`](https://github.com/tobymao/sqlglot/commit/acbc81d47a2e721c4334ac86b5e17177429cd1c6) - Preserve JSON/VARIANT path with operators *(PR [#3678](https://github.com/tobymao/sqlglot/pull/3678) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve JSON/VARIANT path with operators (#3678)


### :sparkles: New Features
- [`84d820f`](https://github.com/tobymao/sqlglot/commit/84d820f96b161fdd5b00f265890b5c75c65a36f0) - **bigquery**: Time/Datetime/Timestamp function additions *(PR [#3666](https://github.com/tobymao/sqlglot/pull/3666) by [@VaggelisD](https://github.com/VaggelisD))*
- [`d46ad95`](https://github.com/tobymao/sqlglot/commit/d46ad95bb623f1931d9e373d8444d9ed947362c5) - **tokenizer**: add support for nested comments *(PR [#3670](https://github.com/tobymao/sqlglot/pull/3670) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3668](https://github.com/tobymao/sqlglot/issues/3668) opened by [@aersam](https://github.com/aersam)*
- [`ac0e89c`](https://github.com/tobymao/sqlglot/commit/ac0e89c4401f2f278d32c3e956670b262ab21ce7) - **snowflake**: add SECURE post table property fixes [#3677](https://github.com/tobymao/sqlglot/pull/3677) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`acbc81d`](https://github.com/tobymao/sqlglot/commit/acbc81d47a2e721c4334ac86b5e17177429cd1c6) - **databricks**: Preserve JSON/VARIANT path with operators *(PR [#3678](https://github.com/tobymao/sqlglot/pull/3678) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3673](https://github.com/tobymao/sqlglot/issues/3673) opened by [@aersam](https://github.com/aersam)*
- [`07158c7`](https://github.com/tobymao/sqlglot/commit/07158c77ae7879aa83b7982cefb4ec9d01c11857) - **clickhouse**: Fix roundtrips of DATE/TIMESTAMP functions *(PR [#3683](https://github.com/tobymao/sqlglot/pull/3683) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3679](https://github.com/tobymao/sqlglot/issues/3679) opened by [@TacoBel42](https://github.com/TacoBel42)*

### :bug: Bug Fixes
- [`79aea2a`](https://github.com/tobymao/sqlglot/commit/79aea2affece72acfac52b3ac85cf740d55ccff0) - **doris**: ensure LAG/LEAD are generated with three arguments *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`08fb2ec`](https://github.com/tobymao/sqlglot/commit/08fb2ecf808f25eae74b579f8e5c4369edc7c604) - **parser**: check if FROM exists when making implicit unnest explicit fixes [#3671](https://github.com/tobymao/sqlglot/pull/3671) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`049fc5a`](https://github.com/tobymao/sqlglot/commit/049fc5a430ad6fa2998fd94d6e20b23da3b545c3) - **bigquery**: handle the case-sensitive strategy in normalize_identifier *(PR [#3667](https://github.com/tobymao/sqlglot/pull/3667) by [@georgesittas](https://github.com/georgesittas))*
- [`9e1b6aa`](https://github.com/tobymao/sqlglot/commit/9e1b6aa5d9e2abb141143327c835c8f3b4bbcb0f) - **parser**: handle another edge case in struct field type parser *(PR [#3682](https://github.com/tobymao/sqlglot/pull/3682) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3680](https://github.com/tobymao/sqlglot/issues/3680) opened by [@plaflamme](https://github.com/plaflamme)*
- [`a1a0278`](https://github.com/tobymao/sqlglot/commit/a1a02782f22b471ee3c896d57f15237dc86565d1) - jsonbcontains default gen *(commit by [@tobymao](https://github.com/tobymao))*
- [`bf44942`](https://github.com/tobymao/sqlglot/commit/bf44942a7d35eb83685ad3aa2b360c7105a9f5b7) - **oracle**: Fix default NULL_ORDERING *(PR [#3688](https://github.com/tobymao/sqlglot/pull/3688) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3684](https://github.com/tobymao/sqlglot/issues/3684) opened by [@ncclementi](https://github.com/ncclementi)*

### :wrench: Chores
- [`7ae99fe`](https://github.com/tobymao/sqlglot/commit/7ae99fe8284cf2e60819b3992bc79a020dfd00c5) - bump sqlglotrs to 0.2.7 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.2.0] - 2024-06-17
### :boom: BREAKING CHANGES
- due to [`d331e56`](https://github.com/tobymao/sqlglot/commit/d331e56aad7784a122dc36d7bffe5cf0565e38d1) - Normalize time units in their full singular form *(PR [#3652](https://github.com/tobymao/sqlglot/pull/3652) by [@VaggelisD](https://github.com/VaggelisD))*:

  Normalize time units in their full singular form (#3652)

- due to [`468123e`](https://github.com/tobymao/sqlglot/commit/468123e4b7612287e128529de62f3a88f4e1d579) - create SetOperation class *(PR [#3661](https://github.com/tobymao/sqlglot/pull/3661) by [@georgesittas](https://github.com/georgesittas))*:

  create SetOperation class (#3661)


### :sparkles: New Features
- [`e7a158b`](https://github.com/tobymao/sqlglot/commit/e7a158b6f0990db00a4890dfb456de6112f50fd2) - set misc. dialect settings if available *(PR [#3649](https://github.com/tobymao/sqlglot/pull/3649) by [@georgesittas](https://github.com/georgesittas))*
- [`ff3dabc`](https://github.com/tobymao/sqlglot/commit/ff3dabc75f9a03627caa988b85f88be04a6c70a4) - **tsql**: index on closes [#3658](https://github.com/tobymao/sqlglot/pull/3658) *(commit by [@tobymao](https://github.com/tobymao))*
- [`fb4d908`](https://github.com/tobymao/sqlglot/commit/fb4d9080a042d40455bcf631ca6a0afaacb19683) - **tsql**: clustered index closes [#3659](https://github.com/tobymao/sqlglot/pull/3659) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`88c4965`](https://github.com/tobymao/sqlglot/commit/88c49651ecc9d55967f5c8056352de0f0981989f) - **mysql**: delete redundant keywords *(PR [#3646](https://github.com/tobymao/sqlglot/pull/3646) by [@Toms1999](https://github.com/Toms1999))*
- [`4c82c0d`](https://github.com/tobymao/sqlglot/commit/4c82c0d01086e0a622a1448d25f51b0e760d053f) - Parse UNNEST as a function in base dialect *(PR [#3650](https://github.com/tobymao/sqlglot/pull/3650) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3647](https://github.com/tobymao/sqlglot/issues/3647) opened by [@ronnix](https://github.com/ronnix)*
- [`d331e56`](https://github.com/tobymao/sqlglot/commit/d331e56aad7784a122dc36d7bffe5cf0565e38d1) - **redshift**: Normalize time units in their full singular form *(PR [#3652](https://github.com/tobymao/sqlglot/pull/3652) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3651](https://github.com/tobymao/sqlglot/issues/3651) opened by [@vidit-wisdom](https://github.com/vidit-wisdom)*
- [`a06ee36`](https://github.com/tobymao/sqlglot/commit/a06ee3695d4d23626c1ef0700b373fc84600d374) - **parser**: edge case in _parse_types *(PR [#3656](https://github.com/tobymao/sqlglot/pull/3656) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3655](https://github.com/tobymao/sqlglot/issues/3655) opened by [@dangoldin](https://github.com/dangoldin)*
- [`a739741`](https://github.com/tobymao/sqlglot/commit/a739741dca5eefd7d4a2c450dd4506cb951d7efb) - teradata warning *(commit by [@tobymao](https://github.com/tobymao))*
- [`868f30d`](https://github.com/tobymao/sqlglot/commit/868f30d1ff46ec9b8a048bb79fbb511f458fd769) - improve schema error handling *(PR [#3663](https://github.com/tobymao/sqlglot/pull/3663) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3662](https://github.com/tobymao/sqlglot/issues/3662) opened by [@dexhorthy](https://github.com/dexhorthy)*

### :recycle: Refactors
- [`e8cab58`](https://github.com/tobymao/sqlglot/commit/e8cab58c4c44e84ee21d11e8554ee7aed5dc5901) - clean up join mark elimination rule *(PR [#3653](https://github.com/tobymao/sqlglot/pull/3653) by [@georgesittas](https://github.com/georgesittas))*
- [`468123e`](https://github.com/tobymao/sqlglot/commit/468123e4b7612287e128529de62f3a88f4e1d579) - create SetOperation class *(PR [#3661](https://github.com/tobymao/sqlglot/pull/3661) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3660](https://github.com/tobymao/sqlglot/issues/3660) opened by [@sorgfresser](https://github.com/sorgfresser)*


## [v25.1.0] - 2024-06-12
### :boom: BREAKING CHANGES
- due to [`d6cfb41`](https://github.com/tobymao/sqlglot/commit/d6cfb41d63893eadf23a81adf413952f3bd4f0ad) - Support for DATE_ADD functions *(PR [#3609](https://github.com/tobymao/sqlglot/pull/3609) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support for DATE_ADD functions (#3609)


### :sparkles: New Features
- [`d6cfb41`](https://github.com/tobymao/sqlglot/commit/d6cfb41d63893eadf23a81adf413952f3bd4f0ad) - **spark, databricks**: Support for DATE_ADD functions *(PR [#3609](https://github.com/tobymao/sqlglot/pull/3609) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3607](https://github.com/tobymao/sqlglot/issues/3607) opened by [@justinbt21](https://github.com/justinbt21)*
- [`4b30b87`](https://github.com/tobymao/sqlglot/commit/4b30b872b6db73da51e81ef72e1f3bf8763b652b) - **postgres**: Support DIV() func for integer division *(PR [#3602](https://github.com/tobymao/sqlglot/pull/3602) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3601](https://github.com/tobymao/sqlglot/issues/3601) opened by [@andrrreasss](https://github.com/andrrreasss)*
- [`ee9b01d`](https://github.com/tobymao/sqlglot/commit/ee9b01d5631f8f0942b61dfaf0632ae0ac2543bb) - **mysql**: support ADD INDEX/KEY/UNIQUE in ALTER TABLE *(PR [#3621](https://github.com/tobymao/sqlglot/pull/3621) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3615](https://github.com/tobymao/sqlglot/issues/3615) opened by [@hubg398](https://github.com/hubg398)*
- [`c49cefa`](https://github.com/tobymao/sqlglot/commit/c49cefafaf5e9e51778ab85499fde29600d66ed7) - **mysql**: support STRAIGHT_JOIN *(PR [#3623](https://github.com/tobymao/sqlglot/pull/3623) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3622](https://github.com/tobymao/sqlglot/issues/3622) opened by [@gabocic](https://github.com/gabocic)*
- [`e998308`](https://github.com/tobymao/sqlglot/commit/e998308be079bca343af053b99e3826606811df5) - eliminate join marks *(PR [#3580](https://github.com/tobymao/sqlglot/pull/3580) by [@mrhopko](https://github.com/mrhopko))*
- [`227e054`](https://github.com/tobymao/sqlglot/commit/227e0544ede5dfe3063f3497e865be6e383db524) - **oracle**: support unicode strings u'...' *(PR [#3641](https://github.com/tobymao/sqlglot/pull/3641) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3638](https://github.com/tobymao/sqlglot/issues/3638) opened by [@deebify](https://github.com/deebify)*
- [`6df5757`](https://github.com/tobymao/sqlglot/commit/6df5757d7714269c035b3a3a015c81bde436f2bb) - bq datetime -> timestampfromparts *(PR [#3642](https://github.com/tobymao/sqlglot/pull/3642) by [@tobymao](https://github.com/tobymao))*
- [`6abd2c9`](https://github.com/tobymao/sqlglot/commit/6abd2c943896e65b6c9bb5343304dcd8f01b425e) - **oracle**: Support for WITH READ ONLY / CHECK OPTION *(PR [#3639](https://github.com/tobymao/sqlglot/pull/3639) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3635](https://github.com/tobymao/sqlglot/issues/3635) opened by [@deebify](https://github.com/deebify)*

### :bug: Bug Fixes
- [`514b3a5`](https://github.com/tobymao/sqlglot/commit/514b3a52384fc9164bc5c63fda6b779d68e427b0) - **redshift**: add support for Oracle style outer join markers [#3611](https://github.com/tobymao/sqlglot/pull/3611) *(PR [#3612](https://github.com/tobymao/sqlglot/pull/3612) by [@sandband](https://github.com/sandband))*
  - :arrow_lower_right: *fixes issue [#3611](https://github.com/tobymao/sqlglot/issues/3611) opened by [@sandband](https://github.com/sandband)*
- [`6a607d3`](https://github.com/tobymao/sqlglot/commit/6a607d3fa604be7fdbd51e7de06aeedae73039b7) - unnest should also be a function *(commit by [@tobymao](https://github.com/tobymao))*
- [`0e1a1fb`](https://github.com/tobymao/sqlglot/commit/0e1a1fb31de5fefc16a978162d6c6dd4141e1c4d) - **optimizer**: don't use datetrunc type for right side *(PR [#3614](https://github.com/tobymao/sqlglot/pull/3614) by [@barakalon](https://github.com/barakalon))*
- [`d96459f`](https://github.com/tobymao/sqlglot/commit/d96459f18b308466fbbfd9fcbe658e33ec931f1e) - **postgres**: sha256 support *(commit by [@tobymao](https://github.com/tobymao))*
- [`05fe847`](https://github.com/tobymao/sqlglot/commit/05fe847aeb6525836d4eadb908c65a50755dc0c5) - **snowflake**: support fqns in masking/projection policy constraint *(PR [#3620](https://github.com/tobymao/sqlglot/pull/3620) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3617](https://github.com/tobymao/sqlglot/issues/3617) opened by [@kosta-foundational](https://github.com/kosta-foundational)*
- [`caa3051`](https://github.com/tobymao/sqlglot/commit/caa305161893079f87d4d51d9042b5103a850be4) - **snowflake**: Allow SELECT keyword as JSON path key *(PR [#3627](https://github.com/tobymao/sqlglot/pull/3627) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3619](https://github.com/tobymao/sqlglot/issues/3619) opened by [@kosta-foundational](https://github.com/kosta-foundational)*
- [`96efb64`](https://github.com/tobymao/sqlglot/commit/96efb6458ad5c6b92990d8ea69545e60b2eaa8a5) - **tokenizer**: properly handle tags that need to be identifiers in heredocs *(PR [#3630](https://github.com/tobymao/sqlglot/pull/3630) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3618](https://github.com/tobymao/sqlglot/issues/3618) opened by [@bigluck](https://github.com/bigluck)*
- [`4f8edba`](https://github.com/tobymao/sqlglot/commit/4f8edba78d070e2d4b50da56ddb5ed139120c587) - **oracle**: Allow optional format in TO_DATE *(PR [#3637](https://github.com/tobymao/sqlglot/pull/3637) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3636](https://github.com/tobymao/sqlglot/issues/3636) opened by [@deebify](https://github.com/deebify)*
- [`d8c6153`](https://github.com/tobymao/sqlglot/commit/d8c61534f2b11287af22eb70948dfb735cd778bc) - **oracle**: don't apply eliminate_join_markers at parse time *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1afe6ac`](https://github.com/tobymao/sqlglot/commit/1afe6ac62b9c827a001c5a6ab917304c5756fb09) - don't generate neq(0) if subquery predicate in ensure_bools *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fc050bd`](https://github.com/tobymao/sqlglot/commit/fc050bddf937509961cfd83e9fa86ed7e931da11) - **sqlite**: Fix transpilation of GENERATED AS IDENTITY *(PR [#3634](https://github.com/tobymao/sqlglot/pull/3634) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3632](https://github.com/tobymao/sqlglot/issues/3632) opened by [@lelandbatey](https://github.com/lelandbatey)*
- [`47472d9`](https://github.com/tobymao/sqlglot/commit/47472d9c0a27070fd5f4f9b8c12a8bd8c86b1de1) - **duckdb**: get rid of TEXT length to facilitate transpilation *(PR [#3633](https://github.com/tobymao/sqlglot/pull/3633) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`6194c0f`](https://github.com/tobymao/sqlglot/commit/6194c0f37fd322ee2c33ebe30dcee6c836a66943) - clean up logic related to join marker parsing/generation *(PR [#3613](https://github.com/tobymao/sqlglot/pull/3613) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`5396a8e`](https://github.com/tobymao/sqlglot/commit/5396a8e6ea29876c824b741c2812ad15f4768e4c) - fix SQLFrame casing *(PR [#3616](https://github.com/tobymao/sqlglot/pull/3616) by [@eakmanrq](https://github.com/eakmanrq))*
- [`0397d6f`](https://github.com/tobymao/sqlglot/commit/0397d6f7638c658528cdfef3c85f89afc7fc8952) - bump sqlglotrs to v0.2.6 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.0.3] - 2024-06-06
### :sparkles: New Features
- [`97f8d1a`](https://github.com/tobymao/sqlglot/commit/97f8d1a05801bcd7fd237dac0470c232d3106ca4) - add materialize dialect *(PR [#3577](https://github.com/tobymao/sqlglot/pull/3577) by [@bobbyiliev](https://github.com/bobbyiliev))*
- [`bde5a8d`](https://github.com/tobymao/sqlglot/commit/bde5a8de346125704f757ed6a2de444905fe146e) - add risingwave dialect *(PR [#3598](https://github.com/tobymao/sqlglot/pull/3598) by [@neverchanje](https://github.com/neverchanje))*

### :recycle: Refactors
- [`5140817`](https://github.com/tobymao/sqlglot/commit/51408172ce940b6ab0ad783d98e632d972da6a0a) - **risingwave**: clean up initial implementation of RisingWave *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`f920014`](https://github.com/tobymao/sqlglot/commit/f920014709c2d3ccb7ec18fb622ecd6b6ee0afcd) - **materialize**: clean up initial implementation of Materialize *(PR [#3608](https://github.com/tobymao/sqlglot/pull/3608) by [@georgesittas](https://github.com/georgesittas))*


## [v25.0.2] - 2024-06-05
### :sparkles: New Features
- [`472058d`](https://github.com/tobymao/sqlglot/commit/472058daccf8dc2a7f7f4b7082309a06802017a5) - **bigquery**: add support for GAP_FILL function *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.0.0] - 2024-06-05
### :bug: Bug Fixes
- [`f7081c4`](https://github.com/tobymao/sqlglot/commit/f7081c455cf2f61af61dcfd0859a1bf272b84258) - builder other props closes [#3588](https://github.com/tobymao/sqlglot/pull/3588) *(commit by [@tobymao](https://github.com/tobymao))*
- [`13009ca`](https://github.com/tobymao/sqlglot/commit/13009ca5c14d81b7a07311a38f329b967f909926) - **doris**: use CSV syntax for GROUP_CONCAT *(PR [#3592](https://github.com/tobymao/sqlglot/pull/3592) by [@Toms1999](https://github.com/Toms1999))*
- [`bf7fd12`](https://github.com/tobymao/sqlglot/commit/bf7fd12f9a19bf91dd89f76cf376bf6004d83dc0) - no_ilike_sql to lower both sides *(PR [#3593](https://github.com/tobymao/sqlglot/pull/3593) by [@barakalon](https://github.com/barakalon))*
- [`8d87568`](https://github.com/tobymao/sqlglot/commit/8d875681403a43282e1f414ca90f3cf955f26027) - stop normalization_distance early *(PR [#3594](https://github.com/tobymao/sqlglot/pull/3594) by [@barakalon](https://github.com/barakalon))*
- [`3e38912`](https://github.com/tobymao/sqlglot/commit/3e38912cd0de2e3939221b6ad8ae194e68cfe288) - **duckdb**: add reserved keywords *(PR [#3597](https://github.com/tobymao/sqlglot/pull/3597) by [@georgesittas](https://github.com/georgesittas))*
- [`5683d5f`](https://github.com/tobymao/sqlglot/commit/5683d5fe7eeae8f70751de962644c0981c21c7fc) - **hive**: generate TRUNC for TimestampTrunc *(PR [#3600](https://github.com/tobymao/sqlglot/pull/3600) by [@Toms1999](https://github.com/Toms1999))*
- [`ff55ec1`](https://github.com/tobymao/sqlglot/commit/ff55ec1ca8c259f3c304aa7f6039c033f1fe728c) - **hive**: generate string unit for TRUNC, parse it into TimestampTrunc too *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`12b6aa7`](https://github.com/tobymao/sqlglot/commit/12b6aa7006bbf005c750070d9e266153057ff281) - **snowflake**: Fix COPY INTO with subquery *(PR [#3605](https://github.com/tobymao/sqlglot/pull/3605) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3604](https://github.com/tobymao/sqlglot/issues/3604) opened by [@ajuszczak](https://github.com/ajuszczak)*
- [`061be9b`](https://github.com/tobymao/sqlglot/commit/061be9bda9e03b17590a0ac58fa2fec0540e2e77) - optimize absorb_and_eliminate and remove_complements *(PR [#3595](https://github.com/tobymao/sqlglot/pull/3595) by [@barakalon](https://github.com/barakalon))*

### :wrench: Chores
- [`7dd244b`](https://github.com/tobymao/sqlglot/commit/7dd244b6a57e4e8cc9d07cbaf3e89c60fa665a69) - **hive**: test TRUNC roundtrip *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.1.2] - 2024-06-04
### :sparkles: New Features
- [`158ca97`](https://github.com/tobymao/sqlglot/commit/158ca9724c23e7a58f6782719b477f2adb57acae) - **duckdb**: transpile TIMESTAMPNTZ into TIMESTAMP *(PR [#3587](https://github.com/tobymao/sqlglot/pull/3587) by [@georgesittas](https://github.com/georgesittas))*


## [v24.1.1] - 2024-06-03
### :bug: Bug Fixes
- [`60d9085`](https://github.com/tobymao/sqlglot/commit/60d9085a4ec2d0c39aa904bf81b7e15b5bac8ea5) - **postgres**: collate with identifier closes [#3578](https://github.com/tobymao/sqlglot/pull/3578) *(commit by [@tobymao](https://github.com/tobymao))*
- [`317e3a9`](https://github.com/tobymao/sqlglot/commit/317e3a96a49f439aa06af31abb06990f9a1b0d63) - **bigquery**: expanding positional refs with ambiguous references *(PR [#3585](https://github.com/tobymao/sqlglot/pull/3585) by [@tobymao](https://github.com/tobymao))*
- [`5e321f1`](https://github.com/tobymao/sqlglot/commit/5e321f15ac4e54c78b9f90475e1bac4a94eaa48d) - div aliases closes [#3583](https://github.com/tobymao/sqlglot/pull/3583) *(PR [#3586](https://github.com/tobymao/sqlglot/pull/3586) by [@tobymao](https://github.com/tobymao))*


## [v24.1.0] - 2024-05-30
### :boom: BREAKING CHANGES
- due to [`0788c94`](https://github.com/tobymao/sqlglot/commit/0788c944a85d7323b61109ee1ccb5859e3d08404) - Expand stars on BigQuery's tbl.struct_col.* selections *(PR [#3531](https://github.com/tobymao/sqlglot/pull/3531) by [@VaggelisD](https://github.com/VaggelisD))*:

  Expand stars on BigQuery's tbl.struct_col.* selections (#3531)

- due to [`3e71393`](https://github.com/tobymao/sqlglot/commit/3e71393cb8e201a75321fbc179289eb15b1dc6ce) - Refactor struct star expansion in BQ *(PR [#3576](https://github.com/tobymao/sqlglot/pull/3576) by [@VaggelisD](https://github.com/VaggelisD))*:

  Refactor struct star expansion in BQ (#3576)


### :sparkles: New Features
- [`0788c94`](https://github.com/tobymao/sqlglot/commit/0788c944a85d7323b61109ee1ccb5859e3d08404) - **optimizer**: Expand stars on BigQuery's tbl.struct_col.* selections *(PR [#3531](https://github.com/tobymao/sqlglot/pull/3531) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3484](https://github.com/tobymao/sqlglot/issues/3484) opened by [@Bladieblah](https://github.com/Bladieblah)*

### :bug: Bug Fixes
- [`14d63ee`](https://github.com/tobymao/sqlglot/commit/14d63ee8172ddc972d6677071cae3880c748c3aa) - bubble up Identifier comments to TableAliases *(PR [#3571](https://github.com/tobymao/sqlglot/pull/3571) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3570](https://github.com/tobymao/sqlglot/issues/3570) opened by [@fangxingli](https://github.com/fangxingli)*
- [`ba90c22`](https://github.com/tobymao/sqlglot/commit/ba90c22921448ef6b5a0497a9a48918d0e8a9654) - **snowflake**: COPY Postfix *(PR [#3564](https://github.com/tobymao/sqlglot/pull/3564) by [@VaggelisD](https://github.com/VaggelisD))*
- [`3e71393`](https://github.com/tobymao/sqlglot/commit/3e71393cb8e201a75321fbc179289eb15b1dc6ce) - **optimizer**: Refactor struct star expansion in BQ *(PR [#3576](https://github.com/tobymao/sqlglot/pull/3576) by [@VaggelisD](https://github.com/VaggelisD))*

### :recycle: Refactors
- [`1e1dc3f`](https://github.com/tobymao/sqlglot/commit/1e1dc3fea8c5fc1f86fefe6af384e38c8531f2d2) - **optimizer**: minor improvements in the struct star expansion *(PR [#3568](https://github.com/tobymao/sqlglot/pull/3568) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`b12ea8c`](https://github.com/tobymao/sqlglot/commit/b12ea8c126d5debef59e9d9bcbbc6fd5ecf56682) - minor style changes related to COPY INTO *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.0.3] - 2024-05-29
### :bug: Bug Fixes
- [`fb8db9f`](https://github.com/tobymao/sqlglot/commit/fb8db9f2219cfd578fda5c3f51737c180d5aecc6) - **parser**: edge case where TYPE_CONVERTERS leads to type instead of column *(PR [#3566](https://github.com/tobymao/sqlglot/pull/3566) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3565](https://github.com/tobymao/sqlglot/issues/3565) opened by [@galunto](https://github.com/galunto)*
- [`aac8570`](https://github.com/tobymao/sqlglot/commit/aac85705c43edfcd1ebb552573f496c14dce519b) - use index2 instead of self._index in _parse_type index difference *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.0.2] - 2024-05-28
### :sparkles: New Features
- [`078471d`](https://github.com/tobymao/sqlglot/commit/078471d3643da418c91b71dc7bfce5453b924028) - **mysql,doris**: improve transpilation of INTERVAL (plural to singular) *(PR [#3543](https://github.com/tobymao/sqlglot/pull/3543) by [@Toms1999](https://github.com/Toms1999))*
- [`fe56e64`](https://github.com/tobymao/sqlglot/commit/fe56e64aff775002c52843b6b9df973d96349400) - **postgres**: add support for col int[size] column def syntax *(PR [#3548](https://github.com/tobymao/sqlglot/pull/3548) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3544](https://github.com/tobymao/sqlglot/issues/3544) opened by [@judahrand](https://github.com/judahrand)*
  - :arrow_lower_right: *addresses issue [#3545](https://github.com/tobymao/sqlglot/issues/3545) opened by [@judahrand](https://github.com/judahrand)*
- [`188dce8`](https://github.com/tobymao/sqlglot/commit/188dce8ae98f23b5741882c698109563445f11f6) - **snowflake**: add support for WITH-prefixed column constraints *(PR [#3549](https://github.com/tobymao/sqlglot/pull/3549) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3537](https://github.com/tobymao/sqlglot/issues/3537) opened by [@barino86](https://github.com/barino86)*
- [`712d247`](https://github.com/tobymao/sqlglot/commit/712d24704f1be9e54fd6385d6fdbd05173b007aa) - add support for ALTER COLUMN DROP NOT NULL *(PR [#3550](https://github.com/tobymao/sqlglot/pull/3550) by [@noklam](https://github.com/noklam))*
  - :arrow_lower_right: *addresses issue [#3534](https://github.com/tobymao/sqlglot/issues/3534) opened by [@barino86](https://github.com/barino86)*
- [`7c323bd`](https://github.com/tobymao/sqlglot/commit/7c323bde83f1804d7a1e98fcf94e6832385a03d6) - add option in schema's find method to ensure types are DataTypes *(PR [#3560](https://github.com/tobymao/sqlglot/pull/3560) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`1a8a16b`](https://github.com/tobymao/sqlglot/commit/1a8a16b459c7fe20fc2c689ad601b5beac57a206) - **clickhouse**: improve struct type parsing *(PR [#3547](https://github.com/tobymao/sqlglot/pull/3547) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3546](https://github.com/tobymao/sqlglot/issues/3546) opened by [@cpcloud](https://github.com/cpcloud)*
- [`970d3b0`](https://github.com/tobymao/sqlglot/commit/970d3b03750d58ec236ce205bc250616e1fb1349) - **postgres**: setting un-suffixed FLOAT as DOUBLE ([#3551](https://github.com/tobymao/sqlglot/pull/3551)) *(PR [#3552](https://github.com/tobymao/sqlglot/pull/3552) by [@sandband](https://github.com/sandband))*
  - :arrow_lower_right: *fixes issue [#3551](https://github.com/tobymao/sqlglot/issues/3551) opened by [@sandband](https://github.com/sandband)*
- [`e1a9a8b`](https://github.com/tobymao/sqlglot/commit/e1a9a8b6b7fbd44e62cee626540f90425d22d50c) - **redshift**: add support for MINUS operator [#3553](https://github.com/tobymao/sqlglot/pull/3553) *(PR [#3555](https://github.com/tobymao/sqlglot/pull/3555) by [@sandband](https://github.com/sandband))*
- [`beb0269`](https://github.com/tobymao/sqlglot/commit/beb0269b39e848897eaf56e1966d342db72e5c7c) - **tsql**: adapt TimeStrToTime to avoid superfluous casts *(PR [#3558](https://github.com/tobymao/sqlglot/pull/3558) by [@Themiscodes](https://github.com/Themiscodes))*
- [`eae3c51`](https://github.com/tobymao/sqlglot/commit/eae3c5165c16b61c7b524a55776bdb1127005c7d) - use regex to split interval strings *(PR [#3556](https://github.com/tobymao/sqlglot/pull/3556) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3554](https://github.com/tobymao/sqlglot/issues/3554) opened by [@kevinjqiu](https://github.com/kevinjqiu)*

### :recycle: Refactors
- [`a67de5f`](https://github.com/tobymao/sqlglot/commit/a67de5faaa88c1fb5d9857a69c9df06506520cbc) - get rid of redundant dict_depth check in schema find *(PR [#3561](https://github.com/tobymao/sqlglot/pull/3561) by [@georgesittas](https://github.com/georgesittas))*
- [`89a8984`](https://github.com/tobymao/sqlglot/commit/89a8984b8db3817d934b4395e190f3848b1ee77a) - move UNESCAPED_SEQUENCES out of the _Dialect metaclass *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`893addf`](https://github.com/tobymao/sqlglot/commit/893addf9d07602ec3a77097f38d696b6760c6038) - add SET NOT NULL test *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.0.1] - 2024-05-23
### :boom: BREAKING CHANGES
- due to [`80c622e`](https://github.com/tobymao/sqlglot/commit/80c622e0c252ef3be9e469c1cf116c1cd4eaef94) - add reserved keywords fixes [#3526](https://github.com/tobymao/sqlglot/pull/3526) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  add reserved keywords fixes #3526


### :sparkles: New Features
- [`a255610`](https://github.com/tobymao/sqlglot/commit/a2556101c8d04907ae49252def84c55d2daf78b2) - add StringToArray expression (postgres), improve its transpilation *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`8f46d48`](https://github.com/tobymao/sqlglot/commit/8f46d48d4ef4e6be022aff5739992f149519c19d) - **redshift**: transpile SPLIT_TO_STRING *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`80c622e`](https://github.com/tobymao/sqlglot/commit/80c622e0c252ef3be9e469c1cf116c1cd4eaef94) - **doris**: add reserved keywords fixes [#3526](https://github.com/tobymao/sqlglot/pull/3526) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`ebf5fc7`](https://github.com/tobymao/sqlglot/commit/ebf5fc70d8936b5e1522a3ae1b9e231cefe49623) - **hive**: generate correct names for weekofyear, dayofmonth, dayofweek *(PR [#3533](https://github.com/tobymao/sqlglot/pull/3533) by [@oshyun](https://github.com/oshyun))*
  - :arrow_lower_right: *fixes issue [#3532](https://github.com/tobymao/sqlglot/issues/3532) opened by [@oshyun](https://github.com/oshyun)*
- [`3fe3c2c`](https://github.com/tobymao/sqlglot/commit/3fe3c2c0a3e5f465a0c62261c5a0ba6faf8f0846) - **parser**: make _parse_type less aggressive, only parse column as last resort *(PR [#3541](https://github.com/tobymao/sqlglot/pull/3541) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3539](https://github.com/tobymao/sqlglot/issues/3539) opened by [@crash-g](https://github.com/crash-g)*
  - :arrow_lower_right: *fixes issue [#3540](https://github.com/tobymao/sqlglot/issues/3540) opened by [@crash-g](https://github.com/crash-g)*
- [`8afff02`](https://github.com/tobymao/sqlglot/commit/8afff028977593789abe31c6168a93b7e32ac890) - **tsql**: preserve REPLICATE roundtrip *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.0.0] - 2024-05-21
### :boom: BREAKING CHANGES
- due to [`a077f17`](https://github.com/tobymao/sqlglot/commit/a077f17d10200980769ff69dd9044c95d6d718f2) - add reserved keywords *(PR [#3525](https://github.com/tobymao/sqlglot/pull/3525) by [@georgesittas](https://github.com/georgesittas))*:

  add reserved keywords (#3525)


### :sparkles: New Features
- [`d958bba`](https://github.com/tobymao/sqlglot/commit/d958bba8494b8bca9cf3ffef0384690bafd78393) - **snowflake**: add support for CREATE WAREHOUSE *(PR [#3510](https://github.com/tobymao/sqlglot/pull/3510) by [@yingw787](https://github.com/yingw787))*
  - :arrow_lower_right: *addresses issue [#3502](https://github.com/tobymao/sqlglot/issues/3502) opened by [@yingw787](https://github.com/yingw787)*
- [`2105300`](https://github.com/tobymao/sqlglot/commit/21053004dbb4c6dc3bcb078c4ab93f267e2c63b2) - **databricks**: Enable hex string literals *(PR [#3522](https://github.com/tobymao/sqlglot/pull/3522) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3521](https://github.com/tobymao/sqlglot/issues/3521) opened by [@aersam](https://github.com/aersam)*
- [`1ef3bb6`](https://github.com/tobymao/sqlglot/commit/1ef3bb6ab49eff66a50c4d3983f19292b6979e98) - **snowflake**: Add support for `CREATE STREAMLIT` *(PR [#3519](https://github.com/tobymao/sqlglot/pull/3519) by [@yingw787](https://github.com/yingw787))*
  - :arrow_lower_right: *addresses issue [#3516](https://github.com/tobymao/sqlglot/issues/3516) opened by [@yingw787](https://github.com/yingw787)*

### :bug: Bug Fixes
- [`5cecbfa`](https://github.com/tobymao/sqlglot/commit/5cecbfa63a770c4d623f4a5f76d1a7a5f59d087d) - unnest identifier closes [#3512](https://github.com/tobymao/sqlglot/pull/3512) *(commit by [@tobymao](https://github.com/tobymao))*
- [`33ab353`](https://github.com/tobymao/sqlglot/commit/33ab3536d68203f4fceee63507b5c73076d48ed7) - **snowflake**: parse certain DB_CREATABLES as identifiers *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`d468f92`](https://github.com/tobymao/sqlglot/commit/d468f92a16decabdf847d7de19f82d65d1939d92) - **doris**: dont generate arrows for JSONExtract* closes [#3513](https://github.com/tobymao/sqlglot/pull/3513) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`bfb9f98`](https://github.com/tobymao/sqlglot/commit/bfb9f983d35e080ec1f8c171a65d576af873c0ea) - **postgres**: parse @> into ArrayContainsAll, improve transpilation *(PR [#3515](https://github.com/tobymao/sqlglot/pull/3515) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3511](https://github.com/tobymao/sqlglot/issues/3511) opened by [@Toms1999](https://github.com/Toms1999)*
- [`4def45b`](https://github.com/tobymao/sqlglot/commit/4def45bb553f6fbc65dcf0fa3d6e8c3f5ec000ea) - make UDF DDL property parsing more lenient closes [#3517](https://github.com/tobymao/sqlglot/pull/3517) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a077f17`](https://github.com/tobymao/sqlglot/commit/a077f17d10200980769ff69dd9044c95d6d718f2) - **mysql**: add reserved keywords *(PR [#3525](https://github.com/tobymao/sqlglot/pull/3525) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3520](https://github.com/tobymao/sqlglot/issues/3520) opened by [@Toms1999](https://github.com/Toms1999)*
  - :arrow_lower_right: *fixes issue [#3524](https://github.com/tobymao/sqlglot/issues/3524) opened by [@Toms1999](https://github.com/Toms1999)*

### :wrench: Chores
- [`358f30c`](https://github.com/tobymao/sqlglot/commit/358f30cc02959275c53a2ee9eccde04ddc6a74a5) - remove redundant postgres JSONB token mapping *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.17.0] - 2024-05-19
### :boom: BREAKING CHANGES
- due to [`77d21d9`](https://github.com/tobymao/sqlglot/commit/77d21d9379c3f130b803ea651ec3d36256bb84a4) - parse : operator as JSONExtract (similar to Snowflake) *(PR [#3508](https://github.com/tobymao/sqlglot/pull/3508) by [@georgesittas](https://github.com/georgesittas))*:

  parse : operator as JSONExtract (similar to Snowflake) (#3508)


### :sparkles: New Features
- [`1125662`](https://github.com/tobymao/sqlglot/commit/11256629d74c4721ed13ed534509d266e260dde6) - add support for snowflake lambdas with type annotations closes … *(PR [#3506](https://github.com/tobymao/sqlglot/pull/3506) by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`77d21d9`](https://github.com/tobymao/sqlglot/commit/77d21d9379c3f130b803ea651ec3d36256bb84a4) - **databricks**: parse : operator as JSONExtract (similar to Snowflake) *(PR [#3508](https://github.com/tobymao/sqlglot/pull/3508) by [@georgesittas](https://github.com/georgesittas))*


## [v23.16.0] - 2024-05-18
### :boom: BREAKING CHANGES
- due to [`e281db8`](https://github.com/tobymao/sqlglot/commit/e281db8784682649be305e9a05c45211402f107c) - Add ALTER TABLE SET *(PR [#3485](https://github.com/tobymao/sqlglot/pull/3485) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add ALTER TABLE SET (#3485)


### :sparkles: New Features
- [`e281db8`](https://github.com/tobymao/sqlglot/commit/e281db8784682649be305e9a05c45211402f107c) - Add ALTER TABLE SET *(PR [#3485](https://github.com/tobymao/sqlglot/pull/3485) by [@VaggelisD](https://github.com/VaggelisD))*
- [`9aee21b`](https://github.com/tobymao/sqlglot/commit/9aee21b88e73809e2cdc4e48f04e16edcf1141d7) - add RETURNS NULL ON NULL and STRICT properties *(PR [#3504](https://github.com/tobymao/sqlglot/pull/3504) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3503](https://github.com/tobymao/sqlglot/issues/3503) opened by [@krzysztof-kwitt](https://github.com/krzysztof-kwitt)*

### :wrench: Chores
- [`0896d11`](https://github.com/tobymao/sqlglot/commit/0896d113b94aaea82e90dd04cdf917dfa546d08e) - lint *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.10] - 2024-05-17
### :sparkles: New Features
- [`89c1d3a`](https://github.com/tobymao/sqlglot/commit/89c1d3a4dd3387576c384413b3a8991a2dd030de) - **clickhouse**: support generate TimestampTrunc, Variance, Stddev *(PR [#3489](https://github.com/tobymao/sqlglot/pull/3489) by [@longxiaofei](https://github.com/longxiaofei))*

### :bug: Bug Fixes
- [`03879bb`](https://github.com/tobymao/sqlglot/commit/03879bb3249ee83cce34d629f1016575d3b932e3) - **postgres**: date_trunc supports time zone *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6e7f37a`](https://github.com/tobymao/sqlglot/commit/6e7f37af86a4f36ec47ea4ef3519e5c97376e090) - copy into pretty printing and default dialect *(PR [#3496](https://github.com/tobymao/sqlglot/pull/3496) by [@tobymao](https://github.com/tobymao))*
- [`e8600e2`](https://github.com/tobymao/sqlglot/commit/e8600e24370a131a0b375a1a9943fdf590968198) - property eq needs highest precedence *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.9] - 2024-05-17
### :boom: BREAKING CHANGES
- due to [`846d5cd`](https://github.com/tobymao/sqlglot/commit/846d5cd2fe85f836f5ad888e783fedfa2108d579) - set default precision / width for DECIMAL type *(PR [#3472](https://github.com/tobymao/sqlglot/pull/3472) by [@georgesittas](https://github.com/georgesittas))*:

  set default precision / width for DECIMAL type (#3472)

- due to [`e3ff67b`](https://github.com/tobymao/sqlglot/commit/e3ff67b0327a217a0523f82e6a11940feab1a8ac) - preserve star clauses (EXCLUDE, RENAME, REPLACE) *(PR [#3477](https://github.com/tobymao/sqlglot/pull/3477) by [@georgesittas](https://github.com/georgesittas))*:

  preserve star clauses (EXCLUDE, RENAME, REPLACE) (#3477)

- due to [`b417c80`](https://github.com/tobymao/sqlglot/commit/b417c80b4208df1b97363db53af42158aa97bbd6) - parse TININT into UTINYINT to improve transpilation *(PR [#3486](https://github.com/tobymao/sqlglot/pull/3486) by [@georgesittas](https://github.com/georgesittas))*:

  parse TININT into UTINYINT to improve transpilation (#3486)

- due to [`54e31af`](https://github.com/tobymao/sqlglot/commit/54e31af7d86138662c9619d50b4ae2e68e04942b) - add DECLARE statement parsing *(PR [#3462](https://github.com/tobymao/sqlglot/pull/3462) by [@jlucas-fsp](https://github.com/jlucas-fsp))*:

  add DECLARE statement parsing (#3462)

- due to [`7287bb9`](https://github.com/tobymao/sqlglot/commit/7287bb9bf578b2b3afaf25647f505b9d73040dc7) - nested cte ordering closes [#3488](https://github.com/tobymao/sqlglot/pull/3488) *(commit by [@tobymao](https://github.com/tobymao))*:

  nested cte ordering closes #3488


### :sparkles: New Features
- [`2c29bf3`](https://github.com/tobymao/sqlglot/commit/2c29bf3b7a163b88754c4593996bbba9b3c791b6) - **snowflake**: add support for CREATE TAG DDL statement *(PR [#3473](https://github.com/tobymao/sqlglot/pull/3473) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3468](https://github.com/tobymao/sqlglot/issues/3468) opened by [@tekumara](https://github.com/tekumara)*
- [`2433993`](https://github.com/tobymao/sqlglot/commit/24339934167e651f2afd6966024e4d96ef55c677) - **transpiler**: handle different hex behavior for dialects *(PR [#3463](https://github.com/tobymao/sqlglot/pull/3463) by [@viplazylmht](https://github.com/viplazylmht))*
  - :arrow_lower_right: *addresses issue [#3460](https://github.com/tobymao/sqlglot/issues/3460) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`0009e09`](https://github.com/tobymao/sqlglot/commit/0009e09b1a7f94f85985670a09bb0be92c673b46) - add epoch_ms of duckdb to other dialects *(PR [#3471](https://github.com/tobymao/sqlglot/pull/3471) by [@longxiaofei](https://github.com/longxiaofei))*
- [`461215b`](https://github.com/tobymao/sqlglot/commit/461215b259de98125ea6b09d7bd875edb3ccce75) - **clickhouse**: add support for PROJECTION in CREATE TABLE statement *(PR [#3465](https://github.com/tobymao/sqlglot/pull/3465) by [@GaliFFun](https://github.com/GaliFFun))*
- [`54e31af`](https://github.com/tobymao/sqlglot/commit/54e31af7d86138662c9619d50b4ae2e68e04942b) - **tsql**: add DECLARE statement parsing *(PR [#3462](https://github.com/tobymao/sqlglot/pull/3462) by [@jlucas-fsp](https://github.com/jlucas-fsp))*
- [`c811adb`](https://github.com/tobymao/sqlglot/commit/c811adb73e6f83265fedc26274c7d4b40f8a1c85) - snowflake array_construct_compact to spark *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`58d5f2b`](https://github.com/tobymao/sqlglot/commit/58d5f2bece42acdda5f8c08d30e6f61a5e538d4c) - **presto**: fix parsing and generating hash functions presto/trino *(PR [#3459](https://github.com/tobymao/sqlglot/pull/3459) by [@viplazylmht](https://github.com/viplazylmht))*
  - :arrow_lower_right: *fixes issue [#3458](https://github.com/tobymao/sqlglot/issues/3458) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`065281e`](https://github.com/tobymao/sqlglot/commit/065281e28be75597f3f97cee22995423ed483660) - **optimizer**: fix multiple bugs in unnest_subqueries, clean up test suite *(PR [#3464](https://github.com/tobymao/sqlglot/pull/3464) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3448](https://github.com/tobymao/sqlglot/issues/3448) opened by [@yesemsanthoshkumar](https://github.com/yesemsanthoshkumar)*
- [`80ba1e8`](https://github.com/tobymao/sqlglot/commit/80ba1e8786a6347b8f20f340c185a0b41d017c73) - preserve quotes for projections produced by the eliminate_qualify rule *(PR [#3470](https://github.com/tobymao/sqlglot/pull/3470) by [@aersam](https://github.com/aersam))*
  - :arrow_lower_right: *fixes issue [#3467](https://github.com/tobymao/sqlglot/issues/3467) opened by [@aersam](https://github.com/aersam)*
- [`3bc1fbe`](https://github.com/tobymao/sqlglot/commit/3bc1fbed40d9d0d05f189ca60fdc7af19b815e8b) - make quoting of alias_or_name in eliminate_qualify more robust *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1843e9b`](https://github.com/tobymao/sqlglot/commit/1843e9b825da6e97bda8c7b4fffce40baf199af1) - allow parameters in user-defined types *(PR [#3474](https://github.com/tobymao/sqlglot/pull/3474) by [@georgesittas](https://github.com/georgesittas))*
- [`e004d2a`](https://github.com/tobymao/sqlglot/commit/e004d2a3d88ea77d34ecdb8290df1e73511e6b6c) - **duckdb**: preserve precedence of json extraction when converting to arrow syntax *(PR [#3478](https://github.com/tobymao/sqlglot/pull/3478) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3476](https://github.com/tobymao/sqlglot/issues/3476) opened by [@asiunov](https://github.com/asiunov)*
- [`e3ff67b`](https://github.com/tobymao/sqlglot/commit/e3ff67b0327a217a0523f82e6a11940feab1a8ac) - **snowflake**: preserve star clauses (EXCLUDE, RENAME, REPLACE) *(PR [#3477](https://github.com/tobymao/sqlglot/pull/3477) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3475](https://github.com/tobymao/sqlglot/issues/3475) opened by [@asiunov](https://github.com/asiunov)*
- [`428fd61`](https://github.com/tobymao/sqlglot/commit/428fd61574e10be9afab23ac711758b229cc174f) - **mysql**: generate CONCAT for DPipe *(PR [#3482](https://github.com/tobymao/sqlglot/pull/3482) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3481](https://github.com/tobymao/sqlglot/issues/3481) opened by [@Toms1999](https://github.com/Toms1999)*
- [`b417c80`](https://github.com/tobymao/sqlglot/commit/b417c80b4208df1b97363db53af42158aa97bbd6) - **tsql**: parse TININT into UTINYINT to improve transpilation *(PR [#3486](https://github.com/tobymao/sqlglot/pull/3486) by [@georgesittas](https://github.com/georgesittas))*
- [`a3ff49e`](https://github.com/tobymao/sqlglot/commit/a3ff49e93f2c6752f512192ca8b6b6ad18fc925a) - **presto**: fix DELETE DML statement for presto/trino *(PR [#3466](https://github.com/tobymao/sqlglot/pull/3466) by [@viplazylmht](https://github.com/viplazylmht))*
- [`7287bb9`](https://github.com/tobymao/sqlglot/commit/7287bb9bf578b2b3afaf25647f505b9d73040dc7) - nested cte ordering closes [#3488](https://github.com/tobymao/sqlglot/pull/3488) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5b64475`](https://github.com/tobymao/sqlglot/commit/5b64475bfd2d6a0ddcb3d0adb60d06dca62421a0) - allow rollup to be used as an identifier *(PR [#3495](https://github.com/tobymao/sqlglot/pull/3495) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3494](https://github.com/tobymao/sqlglot/issues/3494) opened by [@dangoldin](https://github.com/dangoldin)*

### :recycle: Refactors
- [`846d5cd`](https://github.com/tobymao/sqlglot/commit/846d5cd2fe85f836f5ad888e783fedfa2108d579) - **snowflake**: set default precision / width for DECIMAL type *(PR [#3472](https://github.com/tobymao/sqlglot/pull/3472) by [@georgesittas](https://github.com/georgesittas))*
- [`930f923`](https://github.com/tobymao/sqlglot/commit/930f923c6da182be33ad4c912b64ec052a63af30) - clean up Hex / LowerHex implementation *(PR [#3483](https://github.com/tobymao/sqlglot/pull/3483) by [@georgesittas](https://github.com/georgesittas))*
- [`883fcd7`](https://github.com/tobymao/sqlglot/commit/883fcd78645539a275b66472f0bd1dfe1d3d4401) - **presto**: make DELETE transpilation more robust *(PR [#3487](https://github.com/tobymao/sqlglot/pull/3487) by [@georgesittas](https://github.com/georgesittas))*
- [`49f7f85`](https://github.com/tobymao/sqlglot/commit/49f7f857634ae85547c805ac53911895407dd7cb) - **tsql**: handle TABLE <schema> more gracefully for DeclareItem *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.15.8] - 2024-05-11
### :boom: BREAKING CHANGES
- due to [`510f8b5`](https://github.com/tobymao/sqlglot/commit/510f8b5726c59a13284e9482dc47d488559e6c9e) - improve transpilation of TABLESAMPLE clause *(PR [#3457](https://github.com/tobymao/sqlglot/pull/3457) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilation of TABLESAMPLE clause (#3457)


### :sparkles: New Features
- [`510f8b5`](https://github.com/tobymao/sqlglot/commit/510f8b5726c59a13284e9482dc47d488559e6c9e) - improve transpilation of TABLESAMPLE clause *(PR [#3457](https://github.com/tobymao/sqlglot/pull/3457) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3456](https://github.com/tobymao/sqlglot/issues/3456) opened by [@whummer](https://github.com/whummer)*
- [`e28c959`](https://github.com/tobymao/sqlglot/commit/e28c959bf44208bdb3821b38c13fde59f1944fbb) - make create table cmd parsing less aggressive so that they can be used in sqlmesh @if macros *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.7] - 2024-05-11
### :wrench: Chores
- [`c3bb3da`](https://github.com/tobymao/sqlglot/commit/c3bb3da670d06cb2eef545a909635224b6e7c205) - change python-version to 3.11 for build-rs *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.15.6] - 2024-05-11
### :wrench: Chores
- [`cd8f568`](https://github.com/tobymao/sqlglot/commit/cd8f568dba53efe6b9883035c48a67134016e612) - fix rust deployment workflow bug *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.15.3] - 2024-05-10
### :wrench: Chores
- [`130255e`](https://github.com/tobymao/sqlglot/commit/130255ebc927c48b3d3e479e17c38269bd7d8056) - update rust *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.2] - 2024-05-10
### :sparkles: New Features
- [`116172a`](https://github.com/tobymao/sqlglot/commit/116172a41119e72aaf618a83761f73d52f0440d2) - add support for ON property in ALTER and DROP statements *(PR [#3450](https://github.com/tobymao/sqlglot/pull/3450) by [@GaliFFun](https://github.com/GaliFFun))*
- [`aa104fd`](https://github.com/tobymao/sqlglot/commit/aa104fd2ccd73a13ca60fa3de3296ed4c007e8da) - add semi colon comments *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`2c62267`](https://github.com/tobymao/sqlglot/commit/2c62267e2ae908d10d8164f080bc66a133596bf6) - **bigquery**: fix SHA1 generator *(PR [#3453](https://github.com/tobymao/sqlglot/pull/3453) by [@viplazylmht](https://github.com/viplazylmht))*
  - :arrow_lower_right: *fixes issue [#3451](https://github.com/tobymao/sqlglot/issues/3451) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`fb3dea9`](https://github.com/tobymao/sqlglot/commit/fb3dea9a803157b4684cd62e2ef0b6a6b612f7e1) - **clickhouse**: fix parsing and generating hash functions *(PR [#3454](https://github.com/tobymao/sqlglot/pull/3454) by [@viplazylmht](https://github.com/viplazylmht))*
  - :arrow_lower_right: *fixes issue [#3452](https://github.com/tobymao/sqlglot/issues/3452) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`b76dfda`](https://github.com/tobymao/sqlglot/commit/b76dfda7b4122a59c52bcbb445cffc6617e68b8c) - **snowflake**: COPY Subquery postfix *(PR [#3449](https://github.com/tobymao/sqlglot/pull/3449) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3434](https://github.com/tobymao/sqlglot/issues/3434) opened by [@whummer](https://github.com/whummer)*

### :wrench: Chores
- [`684df5f`](https://github.com/tobymao/sqlglot/commit/684df5f7e11bb89def9ff71da0913de222bdaf3c) - remove unnecessary set_op *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.1] - 2024-05-10
### :bug: Bug Fixes
- [`33ac4fc`](https://github.com/tobymao/sqlglot/commit/33ac4fca3e5f162500ddde529cd69c338a6fecc5) - add create view tsql *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.0] - 2024-05-09
### :boom: BREAKING CHANGES
- due to [`9338ebc`](https://github.com/tobymao/sqlglot/commit/9338ebc6dc9635f12639b562ee2af140cf708b6b) - tsql drop view no catalog *(commit by [@tobymao](https://github.com/tobymao))*:

  tsql drop view no catalog


### :sparkles: New Features
- [`80670bb`](https://github.com/tobymao/sqlglot/commit/80670bbd1e062cc476dcee17d0b9972ff7dc0424) - **snowflake**: Support for APPROX_PERCENTILE *(PR [#3426](https://github.com/tobymao/sqlglot/pull/3426) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3424](https://github.com/tobymao/sqlglot/issues/3424) opened by [@baruchoxman](https://github.com/baruchoxman)*
- [`b46c5b3`](https://github.com/tobymao/sqlglot/commit/b46c5b3ddaed359fb59264f00d7033c7b36bd9a4) - **clickhouse**: add support for partition expression *(PR [#3428](https://github.com/tobymao/sqlglot/pull/3428) by [@GaliFFun](https://github.com/GaliFFun))*
- [`07badc9`](https://github.com/tobymao/sqlglot/commit/07badc9d155cfd6d0c70e4419ed763b8c52b4973) - **clickhouse**: add support for ALTER TABLE REPLACE PARTITION statement *(PR [#3441](https://github.com/tobymao/sqlglot/pull/3441) by [@GaliFFun](https://github.com/GaliFFun))*
- [`baf39e7`](https://github.com/tobymao/sqlglot/commit/baf39e78cdebf5478b59f83120c43b39b27d1a31) - **redshift**: improve ALTER TABLE .. ALTER .. support *(PR [#3444](https://github.com/tobymao/sqlglot/pull/3444) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`e8014e2`](https://github.com/tobymao/sqlglot/commit/e8014e2a479c37ef75510e7d5ca90ed30522ce60) - **mysql**: Parse REPLACE statement as Command *(PR [#3425](https://github.com/tobymao/sqlglot/pull/3425) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3423](https://github.com/tobymao/sqlglot/issues/3423) opened by [@DyCheer](https://github.com/DyCheer)*
- [`273731f`](https://github.com/tobymao/sqlglot/commit/273731fd8cba4d6bda0d7ce109f25c49de0ec95c) - **snowflake**: parse CREATE SEQUENCE with commas *(PR [#3436](https://github.com/tobymao/sqlglot/pull/3436) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3435](https://github.com/tobymao/sqlglot/issues/3435) opened by [@whummer](https://github.com/whummer)*
- [`761ba6f`](https://github.com/tobymao/sqlglot/commit/761ba6fb507158d4e5ea51ca396809be91c11ebf) - don't generate connector comments when comments=False closes [#3439](https://github.com/tobymao/sqlglot/pull/3439) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a2a6eaa`](https://github.com/tobymao/sqlglot/commit/a2a6eaa5d7ace2879ded7c3a4cf4192b75c07f26) - handle empty string in connector comment padding *(PR [#3437](https://github.com/tobymao/sqlglot/pull/3437) by [@uncledata](https://github.com/uncledata))*
- [`1bc0ce5`](https://github.com/tobymao/sqlglot/commit/1bc0ce57eca5e401a4c39237b52ee722bdfb46af) - func to binary MOD generation *(PR [#3440](https://github.com/tobymao/sqlglot/pull/3440) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3431](https://github.com/tobymao/sqlglot/issues/3431) opened by [@daniel769](https://github.com/daniel769)*
- [`5cfb29c`](https://github.com/tobymao/sqlglot/commit/5cfb29c7ff6015e39d7fd5b94ed2aa66436e33ae) - **bigquery**: MOD edge case *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`9825c4c`](https://github.com/tobymao/sqlglot/commit/9825c4cb616af07a048109c499666081bc6e4eba) - improve error handling for nested schema levels *(PR [#3445](https://github.com/tobymao/sqlglot/pull/3445) by [@tobymao](https://github.com/tobymao))*
- [`c309def`](https://github.com/tobymao/sqlglot/commit/c309defa450f755dbed1d1b6f276b4b1765166e2) - **duckdb**: use name sequence instead of single _t for unnest alias *(PR [#3446](https://github.com/tobymao/sqlglot/pull/3446) by [@georgesittas](https://github.com/georgesittas))*
- [`0927ae3`](https://github.com/tobymao/sqlglot/commit/0927ae3c448ebf068b89bfa5e46b8f135121b470) - **executor**: use timezone-aware object to represent datetime in UTC *(PR [#3447](https://github.com/tobymao/sqlglot/pull/3447) by [@georgesittas](https://github.com/georgesittas))*
- [`9338ebc`](https://github.com/tobymao/sqlglot/commit/9338ebc6dc9635f12639b562ee2af140cf708b6b) - tsql drop view no catalog *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`30f9d30`](https://github.com/tobymao/sqlglot/commit/30f9d30d8ab3727a43b1e6f363f28631cbfa7f92) - bump ruff to 0.4.3 *(PR [#3430](https://github.com/tobymao/sqlglot/pull/3430) by [@georgesittas](https://github.com/georgesittas))*
- [`91bed56`](https://github.com/tobymao/sqlglot/commit/91bed5607e442d416021a1f93e4a457fb47b6a1f) - test 3.12 *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.14.0] - 2024-05-07
### :boom: BREAKING CHANGES
- due to [`258ad3b`](https://github.com/tobymao/sqlglot/commit/258ad3bbf73f55d02ed78a93fa0f16d4630159e3) - parse column instead of identifier for SET assignment LHS *(PR [#3417](https://github.com/tobymao/sqlglot/pull/3417) by [@georgesittas](https://github.com/georgesittas))*:

  parse column instead of identifier for SET assignment LHS (#3417)


### :bug: Bug Fixes
- [`258ad3b`](https://github.com/tobymao/sqlglot/commit/258ad3bbf73f55d02ed78a93fa0f16d4630159e3) - parse column instead of identifier for SET assignment LHS *(PR [#3417](https://github.com/tobymao/sqlglot/pull/3417) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3415](https://github.com/tobymao/sqlglot/issues/3415) opened by [@tekumara](https://github.com/tekumara)*
- [`17c31da`](https://github.com/tobymao/sqlglot/commit/17c31da9e159dc1cdd91bd6df38c43606bdc48c9) - **lineage**: get rid of comments in Node names *(PR [#3418](https://github.com/tobymao/sqlglot/pull/3418) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3414](https://github.com/tobymao/sqlglot/issues/3414) opened by [@jaspev123](https://github.com/jaspev123)*
- [`ea197ea`](https://github.com/tobymao/sqlglot/commit/ea197eae2fcdbeba395b53cf4864fc2e44134c71) - **snowflake**: ensure OBJECT_CONSTRUCT is not generated inside of VALUES *(PR [#3419](https://github.com/tobymao/sqlglot/pull/3419) by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.7] - 2024-05-04
### :wrench: Chores
- [`4dbcd4f`](https://github.com/tobymao/sqlglot/commit/4dbcd4f7147204b7bafa32d14dfe615882562b6b) - refactor publish workflow for sqlglotrs releasing *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.6] - 2024-05-04
### :wrench: Chores
- [`aa4f90a`](https://github.com/tobymao/sqlglot/commit/aa4f90acde9c022fb7f984b30763c732977c1b4c) - refactor publish workflow for sqlglotrs releasing *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.5] - 2024-05-04
### :wrench: Chores
- [`0deffd8`](https://github.com/tobymao/sqlglot/commit/0deffd89a8c6d2da90c9a654c22b78dd4c7dd8f6) - refactor publish workflow for sqlglotrs releasing *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.4] - 2024-05-04
### :wrench: Chores
- [`5125732`](https://github.com/tobymao/sqlglot/commit/5125732f05408750aceefba99b48aeb4def89557) - refactor publish workflow for sqlglotrs releasing *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.3] - 2024-05-04
### :wrench: Chores
- [`0a36dd8`](https://github.com/tobymao/sqlglot/commit/0a36dd85cd7de544a509f7e4ccdddf0cb0c1f697) - fix should-deploy-rs bash condition *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.2] - 2024-05-04
### :bug: Bug Fixes
- [`fc979a0`](https://github.com/tobymao/sqlglot/commit/fc979a0055c0f402cda77448d9c7dfecf45a901f) - **snowflake**: make FILE_FORMAT option always be uppercase in COPY INTO *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`1b5b77d`](https://github.com/tobymao/sqlglot/commit/1b5b77d849260589a2f7d3593c4472e47cae0280) - improve unsupported error documentation *(PR [#3406](https://github.com/tobymao/sqlglot/pull/3406) by [@georgesittas](https://github.com/georgesittas))*
- [`fcb51af`](https://github.com/tobymao/sqlglot/commit/fcb51afc4631cfc5f494c9114d4aba667aa46087) - release sqlglotrs only when Cargo.toml is updated *(PR [#3408](https://github.com/tobymao/sqlglot/pull/3408) by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.1] - 2024-05-04
### :bug: Bug Fixes
- [`2c2a788`](https://github.com/tobymao/sqlglot/commit/2c2a788bb3a5a46e7729a117a6e6b62d33beb020) - **snowflake**: COPY postfix *(PR [#3398](https://github.com/tobymao/sqlglot/pull/3398) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3388](https://github.com/tobymao/sqlglot/issues/3388) opened by [@dangoldin](https://github.com/dangoldin)*


## [v23.13.0] - 2024-05-03
### :boom: BREAKING CHANGES
- due to [`cc6259d`](https://github.com/tobymao/sqlglot/commit/cc6259de3d68831ded31bfb7fafe1ce654aa89dd) - Mark UDTF child scopes as ScopeType.SUBQUERY *(PR [#3390](https://github.com/tobymao/sqlglot/pull/3390) by [@VaggelisD](https://github.com/VaggelisD))*:

  Mark UDTF child scopes as ScopeType.SUBQUERY (#3390)

- due to [`33bae9b`](https://github.com/tobymao/sqlglot/commit/33bae9b527b27f02dfafff3f45534f85aa9e0d9d) - get rid of superfluous "parameters" arg in RegexpReplace *(PR [#3394](https://github.com/tobymao/sqlglot/pull/3394) by [@georgesittas](https://github.com/georgesittas))*:

  get rid of superfluous "parameters" arg in RegexpReplace (#3394)

- due to [`3768514`](https://github.com/tobymao/sqlglot/commit/3768514e3b2f256b69553e173b40f17180744ab0) - snowflake optional merge insert *(commit by [@tobymao](https://github.com/tobymao))*:

  snowflake optional merge insert

- due to [`d1b4f1f`](https://github.com/tobymao/sqlglot/commit/d1b4f1f256cd772bec366d6bf13d9589e1fdfc4b) - Introducing TIMESTAMP_NTZ token and data type *(PR [#3386](https://github.com/tobymao/sqlglot/pull/3386) by [@VaggelisD](https://github.com/VaggelisD))*:

  Introducing TIMESTAMP_NTZ token and data type (#3386)


### :sparkles: New Features
- [`d1b4f1f`](https://github.com/tobymao/sqlglot/commit/d1b4f1f256cd772bec366d6bf13d9589e1fdfc4b) - Introducing TIMESTAMP_NTZ token and data type *(PR [#3386](https://github.com/tobymao/sqlglot/pull/3386) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3379](https://github.com/tobymao/sqlglot/issues/3379) opened by [@aersam](https://github.com/aersam)*
- [`16691f9`](https://github.com/tobymao/sqlglot/commit/16691f962822a132e233d61c2b67ec0fc3da51eb) - **prql**: add support for AGGREGATE *(PR [#3395](https://github.com/tobymao/sqlglot/pull/3395) by [@fool1280](https://github.com/fool1280))*
- [`534fb80`](https://github.com/tobymao/sqlglot/commit/534fb80462370b5236061472496c35a16e9bab4a) - **postgres**: add support for anonymos index DDL syntax *(PR [#3403](https://github.com/tobymao/sqlglot/pull/3403) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`a2afcca`](https://github.com/tobymao/sqlglot/commit/a2afccafd300939eaa5a3b075820f3bf8e8dcaac) - **mysql**: don't cast into invalid numeric/text types *(PR [#3375](https://github.com/tobymao/sqlglot/pull/3375) by [@georgesittas](https://github.com/georgesittas))*
- [`60b5c3b`](https://github.com/tobymao/sqlglot/commit/60b5c3b1b5dfb4aa00754f4b2473ad054b8dd14a) - **spark**: transpile presto TRY, fix JSON casting issue *(PR [#3376](https://github.com/tobymao/sqlglot/pull/3376) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3374](https://github.com/tobymao/sqlglot/issues/3374) opened by [@cploonker](https://github.com/cploonker)*
- [`3e8de71`](https://github.com/tobymao/sqlglot/commit/3e8de7124b735a6ab52971a3e51702c4e7b74be5) - **postgres**: allow FOR clause without FROM in SUBSTRING closes [#3377](https://github.com/tobymao/sqlglot/pull/3377) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b2a7e55`](https://github.com/tobymao/sqlglot/commit/b2a7e550b25fd95eb0abba63228c9e285be168e0) - **optimizer**: Remove XOR from connector simplifications *(PR [#3380](https://github.com/tobymao/sqlglot/pull/3380) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3372](https://github.com/tobymao/sqlglot/issues/3372) opened by [@colincointe](https://github.com/colincointe)*
- [`477754c`](https://github.com/tobymao/sqlglot/commit/477754c72c47b6dc9dd01463b8f6fae6686cb1ac) - **trino**: bring back TRIM parsing *(PR [#3385](https://github.com/tobymao/sqlglot/pull/3385) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3384](https://github.com/tobymao/sqlglot/issues/3384) opened by [@dmelchor-stripe](https://github.com/dmelchor-stripe)*
- [`cc6259d`](https://github.com/tobymao/sqlglot/commit/cc6259de3d68831ded31bfb7fafe1ce654aa89dd) - **optimizer**: Mark UDTF child scopes as ScopeType.SUBQUERY *(PR [#3390](https://github.com/tobymao/sqlglot/pull/3390) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0d23b20`](https://github.com/tobymao/sqlglot/commit/0d23b20352a8931adf8224d322da324b18e8282d) - allow joins in FROM expression parser *(PR [#3389](https://github.com/tobymao/sqlglot/pull/3389) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3387](https://github.com/tobymao/sqlglot/issues/3387) opened by [@MikeWallis42](https://github.com/MikeWallis42)*
- [`e7021df`](https://github.com/tobymao/sqlglot/commit/e7021df397a1dc5e726d1e391ef6428a3190856d) - **duckdb**: Preserve DATE_SUB roundtrip *(PR [#3382](https://github.com/tobymao/sqlglot/pull/3382) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3373](https://github.com/tobymao/sqlglot/issues/3373) opened by [@zergar](https://github.com/zergar)*
- [`641b296`](https://github.com/tobymao/sqlglot/commit/641b296017591b65ffc223d28b37e51886789ca7) - **postgres**: tokenize INT8 as BIGINT *(PR [#3392](https://github.com/tobymao/sqlglot/pull/3392) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3391](https://github.com/tobymao/sqlglot/issues/3391) opened by [@fuzi1996](https://github.com/fuzi1996)*
- [`33bae9b`](https://github.com/tobymao/sqlglot/commit/33bae9b527b27f02dfafff3f45534f85aa9e0d9d) - get rid of superfluous "parameters" arg in RegexpReplace *(PR [#3394](https://github.com/tobymao/sqlglot/pull/3394) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3393](https://github.com/tobymao/sqlglot/issues/3393) opened by [@rzykov](https://github.com/rzykov)*
- [`3768514`](https://github.com/tobymao/sqlglot/commit/3768514e3b2f256b69553e173b40f17180744ab0) - snowflake optional merge insert *(commit by [@tobymao](https://github.com/tobymao))*
- [`f44cd24`](https://github.com/tobymao/sqlglot/commit/f44cd248a82f5519afd0edba5112a499b804fe8f) - make generated constraint parsing more lenient fixes [#3397](https://github.com/tobymao/sqlglot/pull/3397) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`00ff877`](https://github.com/tobymao/sqlglot/commit/00ff87719ab4d6e3a407334c8d811366d0c7ead5) - **tsql**: quote hash sign as well for quoted temporary tables *(PR [#3401](https://github.com/tobymao/sqlglot/pull/3401) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3399](https://github.com/tobymao/sqlglot/issues/3399) opened by [@gforsyth](https://github.com/gforsyth)*
- [`84b7026`](https://github.com/tobymao/sqlglot/commit/84b7026e2fbc4e73c3b4c0c86cb764b95541841e) - **trino**: support for data type 'tdigest' *(PR [#3402](https://github.com/tobymao/sqlglot/pull/3402) by [@suryaiyer95](https://github.com/suryaiyer95))*
- [`24e1115`](https://github.com/tobymao/sqlglot/commit/24e1115c957d42a5511c1c428516e3ce5426cd88) - **trino|presto**: adding cast support for "hyperloglog" column type *(PR [#3405](https://github.com/tobymao/sqlglot/pull/3405) by [@uncledata](https://github.com/uncledata))*


## [v23.12.2] - 2024-04-30
### :sparkles: New Features
- [`d2a6f16`](https://github.com/tobymao/sqlglot/commit/d2a6f16c35cbe355932d0e0eab2fc6ba096d8a97) - COPY TO/FROM statement *(PR [#3359](https://github.com/tobymao/sqlglot/pull/3359) by [@VaggelisD](https://github.com/VaggelisD))*
- [`f034ea0`](https://github.com/tobymao/sqlglot/commit/f034ea0fdd7429bf6694e07b4aff06c665c10951) - **mysql**: Transpile TimestampTrunc *(PR [#3367](https://github.com/tobymao/sqlglot/pull/3367) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3366](https://github.com/tobymao/sqlglot/issues/3366) opened by [@sivpr2000](https://github.com/sivpr2000)*

### :bug: Bug Fixes
- [`f697cb1`](https://github.com/tobymao/sqlglot/commit/f697cb16b6d744253febb2f83476853e63e06f88) - duckdb describe query closes [#3353](https://github.com/tobymao/sqlglot/pull/3353) *(commit by [@tobymao](https://github.com/tobymao))*
- [`6e0fc5d`](https://github.com/tobymao/sqlglot/commit/6e0fc5dd8e1921aac1e3f9834dd6a1c0e30b9e50) - export optimizer functions explicitly in init *(PR [#3358](https://github.com/tobymao/sqlglot/pull/3358) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3354](https://github.com/tobymao/sqlglot/issues/3354) opened by [@tekumara](https://github.com/tekumara)*
- [`23d45ee`](https://github.com/tobymao/sqlglot/commit/23d45eefb8b5f650d2e723499a12ac6801d5cd14) - **postgres**: don't generate CommentColumnConstraint *(PR [#3357](https://github.com/tobymao/sqlglot/pull/3357) by [@georgesittas](https://github.com/georgesittas))*
- [`e87685b`](https://github.com/tobymao/sqlglot/commit/e87685b6971d6ddb7d222993b38aa224c39c5154) - **lineage**: use source names of derived table sources for laterals *(PR [#3360](https://github.com/tobymao/sqlglot/pull/3360) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3356](https://github.com/tobymao/sqlglot/issues/3356) opened by [@eliaperantoni](https://github.com/eliaperantoni)*
- [`e82a30b`](https://github.com/tobymao/sqlglot/commit/e82a30b6563547daea0bb087e1b6b5bf3b0532d3) - **postgres**: don't generate SchemaCommentProperty *(PR [#3364](https://github.com/tobymao/sqlglot/pull/3364) by [@georgesittas](https://github.com/georgesittas))*
- [`47dc52c`](https://github.com/tobymao/sqlglot/commit/47dc52c99ea50b55d08f2b57885eebbd577b8b46) - **mysql**: convert epoch extraction into UNIX_TIMESTAMP call *(PR [#3369](https://github.com/tobymao/sqlglot/pull/3369) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3368](https://github.com/tobymao/sqlglot/issues/3368) opened by [@FaizelK](https://github.com/FaizelK)*
- [`b8f0979`](https://github.com/tobymao/sqlglot/commit/b8f0979537cf3ad9ef83f2c30d6cfb23cd4d2d1e) - **mysql**: generate GROUP_CONCAT for ArrayAgg *(PR [#3370](https://github.com/tobymao/sqlglot/pull/3370) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3368](https://github.com/tobymao/sqlglot/issues/3368) opened by [@FaizelK](https://github.com/FaizelK)*

### :recycle: Refactors
- [`b928f54`](https://github.com/tobymao/sqlglot/commit/b928f542a81d299311d01bd8f1eb762a13adf5c8) - don't mutate the AST when creating DDL scopes *(PR [#3371](https://github.com/tobymao/sqlglot/pull/3371) by [@georgesittas](https://github.com/georgesittas))*


## [v23.12.1] - 2024-04-25
### :wrench: Chores
- [`719d394`](https://github.com/tobymao/sqlglot/commit/719d3949b75bcdac0d19b86d7398c5d9c4b5bdc3) - add a test for quoted aliases *(commit by [@tobymao](https://github.com/tobymao))*
- [`6d7a9f4`](https://github.com/tobymao/sqlglot/commit/6d7a9f4ec0cd87efe19128dc9e55967172bf324e) - use unknown token types *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.12.0] - 2024-04-25
### :boom: BREAKING CHANGES
- due to [`c5ce47b`](https://github.com/tobymao/sqlglot/commit/c5ce47ba7863e0c536e076ea78ec27cb52324493) - Combine aggregate functions with orderby from WITHIN GROUP *(PR [#3352](https://github.com/tobymao/sqlglot/pull/3352) by [@VaggelisD](https://github.com/VaggelisD))*:

  Combine aggregate functions with orderby from WITHIN GROUP (#3352)


### :sparkles: New Features
- [`80793cc`](https://github.com/tobymao/sqlglot/commit/80793ccdb52b1975d93c64a20380047bc6cf4479) - parse (a,) as a tuple instead of a paren *(PR [#3341](https://github.com/tobymao/sqlglot/pull/3341) by [@georgesittas](https://github.com/georgesittas))*
- [`b3826f8`](https://github.com/tobymao/sqlglot/commit/b3826f873dc81adbfe4fbe35e83b71f4c37c3b16) - allow comments to be attached for identifiers used in definitions *(PR [#3340](https://github.com/tobymao/sqlglot/pull/3340) by [@georgesittas](https://github.com/georgesittas))*
- [`ce7d893`](https://github.com/tobymao/sqlglot/commit/ce7d893c7e0d627b94e9225a06b83b863bd61a40) - **clickhouse**: Parse window functions in ParameterizedAggFuncs *(PR [#3347](https://github.com/tobymao/sqlglot/pull/3347) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3344](https://github.com/tobymao/sqlglot/issues/3344) opened by [@alesk](https://github.com/alesk)*

### :bug: Bug Fixes
- [`0e54975`](https://github.com/tobymao/sqlglot/commit/0e54975bf27f8d765378f47872d372ba3817088e) - **tsql**: only use target table name when generating sp_rename *(PR [#3342](https://github.com/tobymao/sqlglot/pull/3342) by [@georgesittas](https://github.com/georgesittas))*
- [`52bdd0c`](https://github.com/tobymao/sqlglot/commit/52bdd0ce104606c520ad4edf8c781ccc502d5a0e) - **tsql**: Convert TIMESTAMP to ROWVERSION, transpile both to BINARY *(PR [#3348](https://github.com/tobymao/sqlglot/pull/3348) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3345](https://github.com/tobymao/sqlglot/issues/3345) opened by [@aersam](https://github.com/aersam)*
- [`c5ce47b`](https://github.com/tobymao/sqlglot/commit/c5ce47ba7863e0c536e076ea78ec27cb52324493) - **duckdb**: Combine aggregate functions with orderby from WITHIN GROUP *(PR [#3352](https://github.com/tobymao/sqlglot/pull/3352) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3350](https://github.com/tobymao/sqlglot/issues/3350) opened by [@btyuhas](https://github.com/btyuhas)*

### :wrench: Chores
- [`eae2f6b`](https://github.com/tobymao/sqlglot/commit/eae2f6be8f13eb44c404dc638ec50d08f203b094) - update sqlglot logo *(commit by [@tobymao](https://github.com/tobymao))*
- [`fb9a7ad`](https://github.com/tobymao/sqlglot/commit/fb9a7ad8f2af98a248e4576677b7b615b9d4c3e7) - copy png *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.11.2] - 2024-04-19
### :bug: Bug Fixes
- [`68595eb`](https://github.com/tobymao/sqlglot/commit/68595eba02ca9f3a01359566104b4315a313ec0a) - edge case *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.11.1] - 2024-04-19
### :bug: Bug Fixes
- [`9cf6f4e`](https://github.com/tobymao/sqlglot/commit/9cf6f4e49208d5a41bca1bd437d31b1ed894e6eb) - don't allow any_token on reserved keywords *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.11.0] - 2024-04-19
### :boom: BREAKING CHANGES
- due to [`290e408`](https://github.com/tobymao/sqlglot/commit/290e408ccf0d0eeec767d4b58bc1293878a3a3ae) - Preserve DPipe in simplify_concat *(PR [#3317](https://github.com/tobymao/sqlglot/pull/3317) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve DPipe in simplify_concat (#3317)

- due to [`83cff79`](https://github.com/tobymao/sqlglot/commit/83cff79633225fe3d8606ec3a5a9e8c1081edd0c) - add comprehensive reserved keywords for presto and redshift *(PR [#3322](https://github.com/tobymao/sqlglot/pull/3322) by [@tobymao](https://github.com/tobymao))*:

  add comprehensive reserved keywords for presto and redshift (#3322)

- due to [`61f5b12`](https://github.com/tobymao/sqlglot/commit/61f5b1274cc1f3d68f0f16d4b3efcdc082f67257) - Introduce partition in exp.Table *(PR [#3327](https://github.com/tobymao/sqlglot/pull/3327) by [@VaggelisD](https://github.com/VaggelisD))*:

  Introduce partition in exp.Table (#3327)

- due to [`1832ff1`](https://github.com/tobymao/sqlglot/commit/1832ff130da06ec905835583f101c031dc4faf1d) - dynamic styling for inline arrays *(commit by [@tobymao](https://github.com/tobymao))*:

  dynamic styling for inline arrays

- due to [`5fb7f5b`](https://github.com/tobymao/sqlglot/commit/5fb7f5b21bc441af8d6fabaff7c3d542d96d3811) - dont double indent comments *(commit by [@tobymao](https://github.com/tobymao))*:

  dont double indent comments


### :sparkles: New Features
- [`4f1691a`](https://github.com/tobymao/sqlglot/commit/4f1691a221f3d7395774f8c131a656a3ec531534) - allow qualify to also annotate on the fly for unnest support *(PR [#3316](https://github.com/tobymao/sqlglot/pull/3316) by [@tobymao](https://github.com/tobymao))*
- [`83cff79`](https://github.com/tobymao/sqlglot/commit/83cff79633225fe3d8606ec3a5a9e8c1081edd0c) - add comprehensive reserved keywords for presto and redshift *(PR [#3322](https://github.com/tobymao/sqlglot/pull/3322) by [@tobymao](https://github.com/tobymao))*
- [`ef3311a`](https://github.com/tobymao/sqlglot/commit/ef3311a8ece67e6300e5ff121660dea8cfd80480) - **hive**: Add 'STORED AS' option in INSERT DIRECTORY *(PR [#3326](https://github.com/tobymao/sqlglot/pull/3326) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3320](https://github.com/tobymao/sqlglot/issues/3320) opened by [@bkyryliuk](https://github.com/bkyryliuk)*
- [`7f9cb2d`](https://github.com/tobymao/sqlglot/commit/7f9cb2d2fe2c09e94f9dbaafcc0a808428b5b21c) - **clickhouse**: Add support for DATE_FORMAT / formatDateTime *(PR [#3329](https://github.com/tobymao/sqlglot/pull/3329) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3324](https://github.com/tobymao/sqlglot/issues/3324) opened by [@PaienNate](https://github.com/PaienNate)*
- [`61f5b12`](https://github.com/tobymao/sqlglot/commit/61f5b1274cc1f3d68f0f16d4b3efcdc082f67257) - Introduce partition in exp.Table *(PR [#3327](https://github.com/tobymao/sqlglot/pull/3327) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3319](https://github.com/tobymao/sqlglot/issues/3319) opened by [@bkyryliuk](https://github.com/bkyryliuk)*
- [`31744b2`](https://github.com/tobymao/sqlglot/commit/31744b26ed97c12fd3cb1e3a0661695fac4c0736) - **prql**: handle NULL *(PR [#3331](https://github.com/tobymao/sqlglot/pull/3331) by [@fool1280](https://github.com/fool1280))*
- [`1105044`](https://github.com/tobymao/sqlglot/commit/1105044fa8c5af8269eeddfe8e160f0c52de913c) - **tsql**: add alter table rename *(commit by [@tobymao](https://github.com/tobymao))*
- [`1832ff1`](https://github.com/tobymao/sqlglot/commit/1832ff130da06ec905835583f101c031dc4faf1d) - dynamic styling for inline arrays *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`ef84f17`](https://github.com/tobymao/sqlglot/commit/ef84f177b7d76b7bf43d6ef38a89cfbe47f4e13b) - **optimizer**: don't simplify parentheses when parent is SubqueryPredicate *(PR [#3315](https://github.com/tobymao/sqlglot/pull/3315) by [@georgesittas](https://github.com/georgesittas))*
- [`290e408`](https://github.com/tobymao/sqlglot/commit/290e408ccf0d0eeec767d4b58bc1293878a3a3ae) - **optimizer**: Preserve DPipe in simplify_concat *(PR [#3317](https://github.com/tobymao/sqlglot/pull/3317) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#2439](https://github.com/TobikoData/sqlmesh/issues/2439) opened by [@ma1f](https://github.com/ma1f)*
- [`52b957a`](https://github.com/tobymao/sqlglot/commit/52b957a0691b09dc43628703b9b3633d7238df5b) - transform eliminate_qualify on generated columns *(PR [#3307](https://github.com/tobymao/sqlglot/pull/3307) by [@viplazylmht](https://github.com/viplazylmht))*
- [`eb8d7b8`](https://github.com/tobymao/sqlglot/commit/eb8d7b80c74850d791ac51a117ed5381b3431b3b) - remove e*s mapping because it's not equivalent to %f *(commit by [@tobymao](https://github.com/tobymao))*
- [`9de1494`](https://github.com/tobymao/sqlglot/commit/9de1494899bfc9ad13270a38054a8deab2fc926e) - allow bigquery udf with resered keyword closes [#3332](https://github.com/tobymao/sqlglot/pull/3332) *(PR [#3333](https://github.com/tobymao/sqlglot/pull/3333) by [@tobymao](https://github.com/tobymao))*
- [`e2b6213`](https://github.com/tobymao/sqlglot/commit/e2b62133add5a39e3a2df1d0c8e634fcab3487ff) - don't double comment unions *(commit by [@tobymao](https://github.com/tobymao))*
- [`5fb7f5b`](https://github.com/tobymao/sqlglot/commit/5fb7f5b21bc441af8d6fabaff7c3d542d96d3811) - dont double indent comments *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`81b28c2`](https://github.com/tobymao/sqlglot/commit/81b28c2a7882b642069afb80cee16991542f84e3) - fix tests with latest duckdb *(commit by [@tobymao](https://github.com/tobymao))*
- [`17f7eaf`](https://github.com/tobymao/sqlglot/commit/17f7eaff564790b1fe7faa414143accf362f550e) - add test *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.10.0] - 2024-04-12
### :bug: Bug Fixes
- [`506760d`](https://github.com/tobymao/sqlglot/commit/506760d2597779e287be4fffdeb1b375994320b1) - **redshift**: unqualify unnest columns *(PR [#3314](https://github.com/tobymao/sqlglot/pull/3314) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`0450521`](https://github.com/tobymao/sqlglot/commit/0450521a4470633be26ad5399247d5c9083e2afc) - get rid of 1st projection pad for leading comma formatting *(PR [#3308](https://github.com/tobymao/sqlglot/pull/3308) by [@georgesittas](https://github.com/georgesittas))*


## [v23.9.0] - 2024-04-12
### :boom: BREAKING CHANGES
- due to [`32cdc36`](https://github.com/tobymao/sqlglot/commit/32cdc3635b22e3e5d0cd5caf5a6ad171ca7c34fb) - allow unions to be limited directly and stop subquerying since… *(PR [#3301](https://github.com/tobymao/sqlglot/pull/3301) by [@tobymao](https://github.com/tobymao))*:

  allow unions to be limited directly and stop subquerying since… (#3301)

- due to [`3c97d34`](https://github.com/tobymao/sqlglot/commit/3c97d3437ea573fd3764eab05ed619353fced580) - parse right-hand side of <value> IN (<query>) as a Subquery *(PR [#3304](https://github.com/tobymao/sqlglot/pull/3304) by [@georgesittas](https://github.com/georgesittas))*:

  parse right-hand side of <value> IN (<query>) as a Subquery (#3304)

- due to [`75e0c69`](https://github.com/tobymao/sqlglot/commit/75e0c69e33922168fcadb4e457ae93815bf533e1) - cast less aggressively *(PR [#3302](https://github.com/tobymao/sqlglot/pull/3302) by [@georgesittas](https://github.com/georgesittas))*:

  cast less aggressively (#3302)


### :sparkles: New Features
- [`a721923`](https://github.com/tobymao/sqlglot/commit/a72192306c8fad6253ad9a03661edcfaa15757c7) - **prql**: Add support for SORT *(PR [#3297](https://github.com/tobymao/sqlglot/pull/3297) by [@fool1280](https://github.com/fool1280))*
- [`2ea438b`](https://github.com/tobymao/sqlglot/commit/2ea438b89f76a357390d657fe3f9e01d2a79e7e4) - is_negative helper method *(commit by [@tobymao](https://github.com/tobymao))*
- [`b28cd89`](https://github.com/tobymao/sqlglot/commit/b28cd89823a38f3a90c57344a44719364d66d723) - improve transpilation of datetime functions to Teradata *(PR [#3295](https://github.com/tobymao/sqlglot/pull/3295) by [@maureen-daum](https://github.com/maureen-daum))*
- [`32cdc36`](https://github.com/tobymao/sqlglot/commit/32cdc3635b22e3e5d0cd5caf5a6ad171ca7c34fb) - allow unions to be limited directly and stop subquerying since… *(PR [#3301](https://github.com/tobymao/sqlglot/pull/3301) by [@tobymao](https://github.com/tobymao))*
  - :arrow_lower_right: *addresses issue [#3300](https://github.com/tobymao/sqlglot/issues/3300) opened by [@williaster](https://github.com/williaster)*
- [`1bc51df`](https://github.com/tobymao/sqlglot/commit/1bc51dfa9d8fd5d7dbea42d3d55aa1db66776ce5) - **teradata**: handle transpile of quarter function *(PR [#3303](https://github.com/tobymao/sqlglot/pull/3303) by [@maureen-daum](https://github.com/maureen-daum))*
- [`4790414`](https://github.com/tobymao/sqlglot/commit/4790414b887b347cb94d810eeb3fe4713970984e) - **prql**: Handle DESC with sort *(PR [#3299](https://github.com/tobymao/sqlglot/pull/3299) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`3c97d34`](https://github.com/tobymao/sqlglot/commit/3c97d3437ea573fd3764eab05ed619353fced580) - parse right-hand side of <value> IN (<query>) as a Subquery *(PR [#3304](https://github.com/tobymao/sqlglot/pull/3304) by [@georgesittas](https://github.com/georgesittas))*
- [`75e0c69`](https://github.com/tobymao/sqlglot/commit/75e0c69e33922168fcadb4e457ae93815bf533e1) - cast less aggressively *(PR [#3302](https://github.com/tobymao/sqlglot/pull/3302) by [@georgesittas](https://github.com/georgesittas))*
- [`d3472c6`](https://github.com/tobymao/sqlglot/commit/d3472c664fdfb7c9cfa9a54c6b0491b605cf4913) - Add postgres transpilation for TIME_TO_UNIX *(PR [#3305](https://github.com/tobymao/sqlglot/pull/3305) by [@crericha](https://github.com/crericha))*
- [`2224881`](https://github.com/tobymao/sqlglot/commit/2224881ed378abe075ebcd3bfbc3eee901f89d71) - case when / if should ignore null types *(commit by [@tobymao](https://github.com/tobymao))*
- [`5b2feb7`](https://github.com/tobymao/sqlglot/commit/5b2feb760ecd4c8ee64f8c464518e7e874f9b9bb) - allow unnesting to bring struct fields into scope *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`b523bb6`](https://github.com/tobymao/sqlglot/commit/b523bb630b28866ebab581d43e99f0b2b821ec12) - cleanup teradata to simplify first *(commit by [@tobymao](https://github.com/tobymao))*
- [`6f73186`](https://github.com/tobymao/sqlglot/commit/6f73186681e8eb9f100a1fe4104c82cbae9d0f61) - refactor to use inline lambda *(commit by [@tobymao](https://github.com/tobymao))*
- [`6b21bba`](https://github.com/tobymao/sqlglot/commit/6b21bba378e411797a57d3de8bd06d3efb6afa8c) - make test runnable *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.8.2] - 2024-04-10
### :sparkles: New Features
- [`eabb708`](https://github.com/tobymao/sqlglot/commit/eabb708db9ce7255f947542d57c31a6c93103985) - **prql**: add filter, set operations  *(PR [#3291](https://github.com/tobymao/sqlglot/pull/3291) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`94c188d`](https://github.com/tobymao/sqlglot/commit/94c188d4920fd03e978253ed98711de259d6acb2) - **optimizer**: propagate recursive CTE source to children scopes early *(PR [#3294](https://github.com/tobymao/sqlglot/pull/3294) by [@georgesittas](https://github.com/georgesittas))*
- [`281db61`](https://github.com/tobymao/sqlglot/commit/281db61009ee01d10690dcc1f2039062b2a1a58c) - replace fully qualified columns with generated table aliases since they become invalid *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.8.1] - 2024-04-09
### :sparkles: New Features
- [`942856d`](https://github.com/tobymao/sqlglot/commit/942856d1bdae43b13114a15a66c84467a0f90e75) - **postgres**: add COMMENT ON MATERIALIZED VIEW *(PR [#3293](https://github.com/tobymao/sqlglot/pull/3293) by [@l-vincent-l](https://github.com/l-vincent-l))*

### :bug: Bug Fixes
- [`fd24b27`](https://github.com/tobymao/sqlglot/commit/fd24b2779fa962077e84d234b6821e67f3815551) - make exp.to_column more lenient *(PR [#3292](https://github.com/tobymao/sqlglot/pull/3292) by [@georgesittas](https://github.com/georgesittas))*


## [v23.8.0] - 2024-04-08
### :boom: BREAKING CHANGES
- due to [`6bba030`](https://github.com/tobymao/sqlglot/commit/6bba0308b590aed73e454c2c40d600c670e0ad7f) - transpile map retrieval to duckdb, transpile TRY_ELEMENT_AT *(PR [#3277](https://github.com/tobymao/sqlglot/pull/3277) by [@georgesittas](https://github.com/georgesittas))*:

  transpile map retrieval to duckdb, transpile TRY_ELEMENT_AT (#3277)

- due to [`02218fc`](https://github.com/tobymao/sqlglot/commit/02218fc4f75d22487976572f51bd131170a728e5) - allow to_column to properly parse quoted column paths, make types simpler *(PR [#3289](https://github.com/tobymao/sqlglot/pull/3289) by [@tobymao](https://github.com/tobymao))*:

  allow to_column to properly parse quoted column paths, make types simpler (#3289)


### :sparkles: New Features
- [`08222c2`](https://github.com/tobymao/sqlglot/commit/08222c2c626353be108347b95644660fe04dfcd1) - **clickhouse**: add support for MATERIALIZED, EPHEMERAL column constraints *(PR [#3275](https://github.com/tobymao/sqlglot/pull/3275) by [@pkit](https://github.com/pkit))*
- [`6bba030`](https://github.com/tobymao/sqlglot/commit/6bba0308b590aed73e454c2c40d600c670e0ad7f) - transpile map retrieval to duckdb, transpile TRY_ELEMENT_AT *(PR [#3277](https://github.com/tobymao/sqlglot/pull/3277) by [@georgesittas](https://github.com/georgesittas))*
- [`1726923`](https://github.com/tobymao/sqlglot/commit/17269232ea7f1f2ebf6daae7a49d55ccadc31798) - desc history databricks closes [#3280](https://github.com/tobymao/sqlglot/pull/3280) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0690cbc`](https://github.com/tobymao/sqlglot/commit/0690cbc14f023589f38bcceea443642c5a9cc586) - **snowflake**: FINAL/RUNNING keywords in MATCH_RECOGNIZE MEASURES *(PR [#3284](https://github.com/tobymao/sqlglot/pull/3284) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3282](https://github.com/tobymao/sqlglot/issues/3282) opened by [@galunto](https://github.com/galunto)*
- [`1311ba3`](https://github.com/tobymao/sqlglot/commit/1311ba3da3b5e05f148d602885fcc34cc73c3c6f) - **presto**: add support for DISTINCT / ALL after GROUP BY *(PR [#3290](https://github.com/tobymao/sqlglot/pull/3290) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3286](https://github.com/tobymao/sqlglot/issues/3286) opened by [@bkyryliuk](https://github.com/bkyryliuk)*

### :bug: Bug Fixes
- [`f65d812`](https://github.com/tobymao/sqlglot/commit/f65d8129b0ae887ff882cf5117f04f64b7e7db6f) - move EphemeralColumnConstraint generation to base generator *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6d1c44d`](https://github.com/tobymao/sqlglot/commit/6d1c44d5b7ac9e3e929de84a761906ad42a07aee) - **optimizer**: unnest union subqueries *(PR [#3278](https://github.com/tobymao/sqlglot/pull/3278) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3276](https://github.com/tobymao/sqlglot/issues/3276) opened by [@khabiri](https://github.com/khabiri)*
- [`a37d231`](https://github.com/tobymao/sqlglot/commit/a37d231200af1dc99fc45fc40627671ee82f6d5e) - **presto**: allow qualify to be an alias closes [#3287](https://github.com/tobymao/sqlglot/pull/3287) *(commit by [@tobymao](https://github.com/tobymao))*
- [`02218fc`](https://github.com/tobymao/sqlglot/commit/02218fc4f75d22487976572f51bd131170a728e5) - allow to_column to properly parse quoted column paths, make types simpler *(PR [#3289](https://github.com/tobymao/sqlglot/pull/3289) by [@tobymao](https://github.com/tobymao))*
- [`fe0eb57`](https://github.com/tobymao/sqlglot/commit/fe0eb57feecce413e3e2992db73424c8cf585599) - pass quoted to the identifier *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`c793629`](https://github.com/tobymao/sqlglot/commit/c79362953a5bf12278f861b8b5d39e6847b22e3b) - another test case *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.7.0] - 2024-04-04
### :sparkles: New Features
- [`e33fb01`](https://github.com/tobymao/sqlglot/commit/e33fb012b47892fab03fab7de896495951f23174) - **prql**: Add support for TAKE  *(PR [#3258](https://github.com/tobymao/sqlglot/pull/3258) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`19302ab`](https://github.com/tobymao/sqlglot/commit/19302abe17a6828e1928075de45c1e2a4f3008ce) - **optimizer**: preserve the original type when creating a date literal *(PR [#3273](https://github.com/tobymao/sqlglot/pull/3273) by [@georgesittas](https://github.com/georgesittas))*


## [v23.6.4] - 2024-04-03
### :bug: Bug Fixes
- [`803fc9e`](https://github.com/tobymao/sqlglot/commit/803fc9e8f245e48e8b0e13760c5fa60cd596a464) - allow placeholders in units closes [#3265](https://github.com/tobymao/sqlglot/pull/3265) *(PR [#3267](https://github.com/tobymao/sqlglot/pull/3267) by [@tobymao](https://github.com/tobymao))*
- [`64ae85b`](https://github.com/tobymao/sqlglot/commit/64ae85ba1344b293ba01dfa300d100ff144cdd7b) - nested cte ordering closes [#3266](https://github.com/tobymao/sqlglot/pull/3266) *(commit by [@tobymao](https://github.com/tobymao))*
- [`09287d9`](https://github.com/tobymao/sqlglot/commit/09287d9b2a39d2476d1f72880f9d2dccfdb210ec) - amend interval unit parsing regression *(PR [#3269](https://github.com/tobymao/sqlglot/pull/3269) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3268](https://github.com/tobymao/sqlglot/issues/3268) opened by [@LilyFoote](https://github.com/LilyFoote)*
- [`bc26e84`](https://github.com/tobymao/sqlglot/commit/bc26e840d171dd03e6053f22ecd785d59cbd4f80) - **optimizer**: tweaks to date simplification *(PR [#3270](https://github.com/tobymao/sqlglot/pull/3270) by [@barakalon](https://github.com/barakalon))*


## [v23.6.0] - 2024-04-02
### :wrench: Chores
- [`4eec748`](https://github.com/tobymao/sqlglot/commit/4eec748d7fd0c73d9593cb3da2b9ebc1d2440436) - deploy sqlglot and then sqlglotrs *(PR [#3264](https://github.com/tobymao/sqlglot/pull/3264) by [@georgesittas](https://github.com/georgesittas))*


## [v23.4.0] - 2024-04-02
### :boom: BREAKING CHANGES
- due to [`e148fe1`](https://github.com/tobymao/sqlglot/commit/e148fe1ace1fe647369c14f2649f428307686a2f) - describe formatted closes [#3244](https://github.com/tobymao/sqlglot/pull/3244) *(commit by [@tobymao](https://github.com/tobymao))*:

  describe formatted closes #3244

- due to [`2c359e7`](https://github.com/tobymao/sqlglot/commit/2c359e790a58e4df9008282401a5578d3ce9d3a4) - properly transpile escape sequences *(PR [#3256](https://github.com/tobymao/sqlglot/pull/3256) by [@georgesittas](https://github.com/georgesittas))*:

  properly transpile escape sequences (#3256)

- due to [`9787567`](https://github.com/tobymao/sqlglot/commit/978756783b639e174f3f614f3e39382fef296640) - bump sqlglotrs to 0.2.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.2.0


### :sparkles: New Features
- [`8dba8e2`](https://github.com/tobymao/sqlglot/commit/8dba8e2508f04ccdf9b10eaa8a456478190a53a5) - **optimizer**: Support for small integer CAST elimination *(PR [#3234](https://github.com/tobymao/sqlglot/pull/3234) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3229](https://github.com/tobymao/sqlglot/issues/3229) opened by [@NickCrews](https://github.com/NickCrews)*
- [`e148fe1`](https://github.com/tobymao/sqlglot/commit/e148fe1ace1fe647369c14f2649f428307686a2f) - describe formatted closes [#3244](https://github.com/tobymao/sqlglot/pull/3244) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a48d7eb`](https://github.com/tobymao/sqlglot/commit/a48d7eb9f3d1f9b3d1ffb9b3ec99b1024b7c3da9) - allow non func hints closes [#3248](https://github.com/tobymao/sqlglot/pull/3248) *(commit by [@tobymao](https://github.com/tobymao))*
- [`d90ec95`](https://github.com/tobymao/sqlglot/commit/d90ec95001a9747d6066d1872c5a9402e2837f62) - add conversion of named tuples and classes to structs *(PR [#3245](https://github.com/tobymao/sqlglot/pull/3245) by [@tobymao](https://github.com/tobymao))*
- [`f88640b`](https://github.com/tobymao/sqlglot/commit/f88640b8df22e29ad2fa845b580cf78ad4fb2262) - **clickhouse**: CREATE TABLE computed columns, column compression, index *(PR [#3252](https://github.com/tobymao/sqlglot/pull/3252) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3243](https://github.com/tobymao/sqlglot/issues/3243) opened by [@lksv](https://github.com/lksv)*
- [`a64ec1b`](https://github.com/tobymao/sqlglot/commit/a64ec1bf60fda00e6dd7122a338c6dac80d005e4) - **snowflake**: MATCH_CONDITION in ASOF JOIN *(PR [#3255](https://github.com/tobymao/sqlglot/pull/3255) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3254](https://github.com/tobymao/sqlglot/issues/3254) opened by [@Bilbottom](https://github.com/Bilbottom)*

### :bug: Bug Fixes
- [`a630c50`](https://github.com/tobymao/sqlglot/commit/a630c50737bb9deb6f44e1afd374b113612a1d24) - allow interval spans closes [#3246](https://github.com/tobymao/sqlglot/pull/3246) *(commit by [@tobymao](https://github.com/tobymao))*
- [`28c5ee7`](https://github.com/tobymao/sqlglot/commit/28c5ee7243af9fb8aa5abf2d5d36d6fa4ef47681) - **mysql**: Duplicate parsing of ENGINE_ATTRIBUTE *(PR [#3253](https://github.com/tobymao/sqlglot/pull/3253) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2c359e7`](https://github.com/tobymao/sqlglot/commit/2c359e790a58e4df9008282401a5578d3ce9d3a4) - properly transpile escape sequences *(PR [#3256](https://github.com/tobymao/sqlglot/pull/3256) by [@georgesittas](https://github.com/georgesittas))*
- [`6badfd1`](https://github.com/tobymao/sqlglot/commit/6badfd17b416380a4077f2ef48f1efcbed3c78d3) - Fix STRPOS for Presto & Trino *(PR [#3261](https://github.com/tobymao/sqlglot/pull/3261) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3259](https://github.com/tobymao/sqlglot/issues/3259) opened by [@amitgilad3](https://github.com/amitgilad3)*

### :wrench: Chores
- [`9787567`](https://github.com/tobymao/sqlglot/commit/978756783b639e174f3f614f3e39382fef296640) - bump sqlglotrs to 0.2.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.3.0] - 2024-03-29
### :boom: BREAKING CHANGES
- due to [`0919be5`](https://github.com/tobymao/sqlglot/commit/0919be5eea7aba175e173dbfc0b6547e5c9473a8) - StrToUnix Hive parsing, Presto generation fixes *(PR [#3225](https://github.com/tobymao/sqlglot/pull/3225) by [@georgesittas](https://github.com/georgesittas))*:

  StrToUnix Hive parsing, Presto generation fixes (#3225)

- due to [`163c85c`](https://github.com/tobymao/sqlglot/commit/163c85c8ed327150a6e5c79f1a4b52a8848d4408) - convert dt with isoformat sep space for better compat, trino doesnt accept T *(commit by [@tobymao](https://github.com/tobymao))*:

  convert dt with isoformat sep space for better compat, trino doesnt accept T


### :sparkles: New Features
- [`59f1d13`](https://github.com/tobymao/sqlglot/commit/59f1d13bc5e37ebe6636b05e0381facc9725f7b0) - **oracle**: Support for CONNECT BY [NOCYCLE] *(PR [#3238](https://github.com/tobymao/sqlglot/pull/3238) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3237](https://github.com/tobymao/sqlglot/issues/3237) opened by [@Hal-H2Apps](https://github.com/Hal-H2Apps)*
- [`12563ae`](https://github.com/tobymao/sqlglot/commit/12563ae0645487d5e63343224e1016cce4be447b) - mvp for transpling sqlite's STRFTIME *(PR [#3242](https://github.com/tobymao/sqlglot/pull/3242) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3240](https://github.com/tobymao/sqlglot/issues/3240) opened by [@markhalonen](https://github.com/markhalonen)*

### :bug: Bug Fixes
- [`0919be5`](https://github.com/tobymao/sqlglot/commit/0919be5eea7aba175e173dbfc0b6547e5c9473a8) - StrToUnix Hive parsing, Presto generation fixes *(PR [#3225](https://github.com/tobymao/sqlglot/pull/3225) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3221](https://github.com/tobymao/sqlglot/issues/3221) opened by [@luhea](https://github.com/luhea)*
- [`163c85c`](https://github.com/tobymao/sqlglot/commit/163c85c8ed327150a6e5c79f1a4b52a8848d4408) - convert dt with isoformat sep space for better compat, trino doesnt accept T *(commit by [@tobymao](https://github.com/tobymao))*
- [`555647d`](https://github.com/tobymao/sqlglot/commit/555647d5541c2e52b40d098ee42f6454518e8401) - make property value parsing more lenient *(PR [#3230](https://github.com/tobymao/sqlglot/pull/3230) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3228](https://github.com/tobymao/sqlglot/issues/3228) opened by [@hsheth2](https://github.com/hsheth2)*
- [`8325039`](https://github.com/tobymao/sqlglot/commit/83250398c7804863a6b3f339305600df39515ccc) - **duckdb**: wrap columns inside of INTERVAL expressions *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fd5783f`](https://github.com/tobymao/sqlglot/commit/fd5783f34cb0cb7052477f25a9847a5efd61c04f) - don't evaluate Rand twice when ordering by it *(PR [#3233](https://github.com/tobymao/sqlglot/pull/3233) by [@georgesittas](https://github.com/georgesittas))*
- [`b097da5`](https://github.com/tobymao/sqlglot/commit/b097da5a624fa467830464427ec57bf3b303de3f) - index error when comment sql is none *(commit by [@tobymao](https://github.com/tobymao))*
- [`bf94ce3`](https://github.com/tobymao/sqlglot/commit/bf94ce317497ab92e9fe0562b3034f3482601072) - > 1 nested joins closes [#3231](https://github.com/tobymao/sqlglot/pull/3231) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2a3a5cd`](https://github.com/tobymao/sqlglot/commit/2a3a5cdcffe39d42153b3e960a580d084a27c0eb) - properly parse/generate duckdb MAP {..} syntax, annotate MAPs *(PR [#3241](https://github.com/tobymao/sqlglot/pull/3241) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`647611e`](https://github.com/tobymao/sqlglot/commit/647611e16bdb5ecfc2eec30111cc6689200836b7) - only set vars with necessary *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.1.0] - 2024-03-26
### :boom: BREAKING CHANGES
- due to [`71b82b4`](https://github.com/tobymao/sqlglot/commit/71b82b424b9c336072b011785a0e3e9650ae1380) - allow transformations that mutate the visited node's parent *(PR [#3182](https://github.com/tobymao/sqlglot/pull/3182) by [@georgesittas](https://github.com/georgesittas))*:

  allow transformations that mutate the visited node's parent (#3182)


### :sparkles: New Features
- [`c19878a`](https://github.com/tobymao/sqlglot/commit/c19878a329078ce6ebfbb4337316ff5e43b8c924) - transpile Snowflake's ADDTIME *(PR [#3180](https://github.com/tobymao/sqlglot/pull/3180) by [@georgesittas](https://github.com/georgesittas))*
- [`66e2e49`](https://github.com/tobymao/sqlglot/commit/66e2e497626a77540b9addd35f2edb287c7b62fe) - improve lineage perf *(commit by [@tobymao](https://github.com/tobymao))*
- [`ad23608`](https://github.com/tobymao/sqlglot/commit/ad23608f9f3724f0c35e5d517bba51f77a84f6cb) - **mysql**: Parse MODIFY COLUMN *(PR [#3189](https://github.com/tobymao/sqlglot/pull/3189) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3186](https://github.com/tobymao/sqlglot/issues/3186) opened by [@kosti-hokkanen-supermetrics](https://github.com/kosti-hokkanen-supermetrics)*
- [`a18444d`](https://github.com/tobymao/sqlglot/commit/a18444dbd7ccfc05b189dcb2005c85a1048cc8de) - add expressions for CORR, COVAR_SAMP, COVAR_POP *(PR [#3193](https://github.com/tobymao/sqlglot/pull/3193) by [@ttzhou](https://github.com/ttzhou))*
- [`3620b99`](https://github.com/tobymao/sqlglot/commit/3620b9974c28df7d4d189ebd5fdcb675f41a275d) - add support for converting `bytes` to sqlglot AST *(PR [#3198](https://github.com/tobymao/sqlglot/pull/3198) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3195](https://github.com/tobymao/sqlglot/issues/3195) opened by [@aersam](https://github.com/aersam)*
- [`648c819`](https://github.com/tobymao/sqlglot/commit/648c819071082f7a1f2f6587336ae765d4915034) - redshift starts with support *(PR [#3194](https://github.com/tobymao/sqlglot/pull/3194) by [@eakmanrq](https://github.com/eakmanrq))*
- [`c355a4a`](https://github.com/tobymao/sqlglot/commit/c355a4a821c7eaf76df510020d825a9f326068de) - **tsql**: add support for WITH <view_attribute> in view DDL *(PR [#3203](https://github.com/tobymao/sqlglot/pull/3203) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3202](https://github.com/tobymao/sqlglot/issues/3202) opened by [@mr-miles](https://github.com/mr-miles)*
- [`8622eb2`](https://github.com/tobymao/sqlglot/commit/8622eb21c89a0d7569e27b3c739592cb96946a3a) - **duckdb**: add support for heredoc string syntax *(PR [#3212](https://github.com/tobymao/sqlglot/pull/3212) by [@georgesittas](https://github.com/georgesittas))*
- [`b50dc5e`](https://github.com/tobymao/sqlglot/commit/b50dc5ecc7d29bce43229d050da8c4e37951853c) - Support for MySQL & Redshift UnixTotime *(PR [#3223](https://github.com/tobymao/sqlglot/pull/3223) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3214](https://github.com/tobymao/sqlglot/issues/3214) opened by [@exgalibas](https://github.com/exgalibas)*
- [`2f6a2f1`](https://github.com/tobymao/sqlglot/commit/2f6a2f13bbd40f3d5348b0ed1b8cf6736ef9d1c5) - **optimizer**: Support for UNION BY NAME *(PR [#3224](https://github.com/tobymao/sqlglot/pull/3224) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3222](https://github.com/tobymao/sqlglot/issues/3222) opened by [@yiyuanliu](https://github.com/yiyuanliu)*

### :bug: Bug Fixes
- [`71b82b4`](https://github.com/tobymao/sqlglot/commit/71b82b424b9c336072b011785a0e3e9650ae1380) - allow transformations that mutate the visited node's parent *(PR [#3182](https://github.com/tobymao/sqlglot/pull/3182) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3181](https://github.com/tobymao/sqlglot/issues/3181) opened by [@l-vincent-l](https://github.com/l-vincent-l)*
- [`6827edd`](https://github.com/tobymao/sqlglot/commit/6827edd108bbc6ecfcc0f03495f00c08022efb3b) - **postgres**: Fix ARROW/DARROW column operators *(PR [#3191](https://github.com/tobymao/sqlglot/pull/3191) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3185](https://github.com/tobymao/sqlglot/issues/3185) opened by [@ZipBrandon](https://github.com/ZipBrandon)*
- [`0dd9ba5`](https://github.com/tobymao/sqlglot/commit/0dd9ba5ef57d29b6406a5d2a7e381eb6e6f56221) - Fix backtracking through try/catch exceptions *(PR [#3190](https://github.com/tobymao/sqlglot/pull/3190) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3175](https://github.com/tobymao/sqlglot/issues/3175) opened by [@herry13](https://github.com/herry13)*
- [`5cdd874`](https://github.com/tobymao/sqlglot/commit/5cdd8749bacb101711a477798ff96bace44ccfb1) - **generator**: compute csv leading comma pad length correctly *(PR [#3201](https://github.com/tobymao/sqlglot/pull/3201) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3199](https://github.com/tobymao/sqlglot/issues/3199) opened by [@giovannipcarvalho](https://github.com/giovannipcarvalho)*
- [`73fc807`](https://github.com/tobymao/sqlglot/commit/73fc807a48bfadc5bbe5594b55ba45480e93be3c) - **tokenizer**: don't increment array cursor by 2 on CRLF *(PR [#3204](https://github.com/tobymao/sqlglot/pull/3204) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3200](https://github.com/tobymao/sqlglot/issues/3200) opened by [@mr-miles](https://github.com/mr-miles)*
- [`af1b026`](https://github.com/tobymao/sqlglot/commit/af1b02697303160050ee32b1c89ff80f14d9d0fa) - **snowflake**: convert VALUES with invalid expressions into UNION ALL *(PR [#3213](https://github.com/tobymao/sqlglot/pull/3213) by [@georgesittas](https://github.com/georgesittas))*
- [`ec4648f`](https://github.com/tobymao/sqlglot/commit/ec4648f7eb982bf48b5bf09271c1955b892867fa) - **optimizer**: don't merge ORDER BY into UNION *(PR [#3215](https://github.com/tobymao/sqlglot/pull/3215) by [@barakalon](https://github.com/barakalon))*
  - :arrow_lower_right: *fixes issue [#3211](https://github.com/tobymao/sqlglot/issues/3211) opened by [@rorynormaness](https://github.com/rorynormaness)*
- [`e4dd052`](https://github.com/tobymao/sqlglot/commit/e4dd0526031591179156a1eea45089213b23cdf7) - allow snowflake object_construct with string keys to transpile to sqlglot dialect *(commit by [@tobymao](https://github.com/tobymao))*
- [`9e39076`](https://github.com/tobymao/sqlglot/commit/9e39076b7f581dc68e10c558ff8f6c9809bfe841) - **tsql**: datestrtodate for tsql closes [#3216](https://github.com/tobymao/sqlglot/pull/3216) *(commit by [@tobymao](https://github.com/tobymao))*
- [`e7c9158`](https://github.com/tobymao/sqlglot/commit/e7c91584ac7fb35082ebd1d4873f13307ea848af) - bq datetime to timestamp *(PR [#3220](https://github.com/tobymao/sqlglot/pull/3220) by [@eakmanrq](https://github.com/eakmanrq))*
- [`e6b8d1f`](https://github.com/tobymao/sqlglot/commit/e6b8d1f0061d55bf434d1a838f858b9fa412e312) - **optimizer**: constrain UDTF scope boundary *(PR [#3226](https://github.com/tobymao/sqlglot/pull/3226) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3209](https://github.com/tobymao/sqlglot/issues/3209) opened by [@rorynormaness](https://github.com/rorynormaness)*

### :recycle: Refactors
- [`4cd0e17`](https://github.com/tobymao/sqlglot/commit/4cd0e1719a55a75dac1114736fbbe48a8aa8f294) - get rid of redundant condition in Expression.replace *(PR [#3192](https://github.com/tobymao/sqlglot/pull/3192) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`3879518`](https://github.com/tobymao/sqlglot/commit/3879518f951233fed3434c493a5786573ee814fd) - bump sqlglotrs to 0.1.3 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.0.5] - 2024-03-20
### :bug: Bug Fixes
- [`ed2c9e1`](https://github.com/tobymao/sqlglot/commit/ed2c9e126cc7e679c543adaa2827c1f5c47b96d7) - move varchar max conversion to base *(commit by [@tobymao](https://github.com/tobymao))*
- [`e3b6139`](https://github.com/tobymao/sqlglot/commit/e3b61392b1d050447f77fcf1b04efd6dcbfc311e) - move comment from window function to Window expression *(PR [#3178](https://github.com/tobymao/sqlglot/pull/3178) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2299](https://github.com/TobikoData/sqlmesh/issues/2299) opened by [@georgesittas](https://github.com/georgesittas)*
- [`a452276`](https://github.com/tobymao/sqlglot/commit/a452276da4daaa436a9ac95566bcbb2954d149e3) - **clickhouse**: Fixing FORMAT being parsed as implicit alias *(PR [#3179](https://github.com/tobymao/sqlglot/pull/3179) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3176](https://github.com/tobymao/sqlglot/issues/3176) opened by [@mlipiev](https://github.com/mlipiev)*


## [v23.0.4] - 2024-03-20
### :bug: Bug Fixes
- [`42cf703`](https://github.com/tobymao/sqlglot/commit/42cf70351e7811a077da29af42b28662ede203ac) - redshift varchar(max) catch lower case *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`20cd803`](https://github.com/tobymao/sqlglot/commit/20cd8038268b162af7bae63d54ed2f349502042a) - cleanup redundant check *(commit by [@tobymao](https://github.com/tobymao))*
- [`7e12342`](https://github.com/tobymao/sqlglot/commit/7e12342029d33ff139a3566243789f54e36f4525) - add superset to readme *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.0.3] - 2024-03-19
### :sparkles: New Features
- [`bc8bc7f`](https://github.com/tobymao/sqlglot/commit/bc8bc7f8c9a6a20a35bab8ea7b34cf6431616b50) - replace a nested child node with a list convenience *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`eaaeab0`](https://github.com/tobymao/sqlglot/commit/eaaeab088010f55ccc221a9a4968f0d4ff67d8b1) - **snowflake**: Allow non-literal expressions too in DATE functions *(PR [#3167](https://github.com/tobymao/sqlglot/pull/3167) by [@VaggelisD](https://github.com/VaggelisD))*


## [v23.0.2] - 2024-03-19
### :sparkles: New Features
- [`32cc2be`](https://github.com/tobymao/sqlglot/commit/32cc2be1b19ade551b42cc70a96f1675ac8773f4) - **postgres**: add support for materialized CTEs *(PR [#3171](https://github.com/tobymao/sqlglot/pull/3171) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3170](https://github.com/tobymao/sqlglot/issues/3170) opened by [@betodealmeida](https://github.com/betodealmeida)*

### :bug: Bug Fixes
- [`df4ce17`](https://github.com/tobymao/sqlglot/commit/df4ce17f24bbb16a64172e351f4e27ac74de668a) - can't expand group by for nulls and bools *(commit by [@tobymao](https://github.com/tobymao))*
- [`d859fc0`](https://github.com/tobymao/sqlglot/commit/d859fc0f6eeb0971dab5b22748d1e84425829444) - unnest annotation with generate_date_array *(PR [#3169](https://github.com/tobymao/sqlglot/pull/3169) by [@tobymao](https://github.com/tobymao))*


## [v23.0.1] - 2024-03-19
### :sparkles: New Features
- [`931774d`](https://github.com/tobymao/sqlglot/commit/931774dde50aa04efecd1ae9cdd6965655670d71) - iterative connector sql *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`5e18d49`](https://github.com/tobymao/sqlglot/commit/5e18d490be3990116bbacd1b09dd52542f51c151) - fill in missing implementation details for replace(None) *(PR [#3166](https://github.com/tobymao/sqlglot/pull/3166) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3165](https://github.com/tobymao/sqlglot/issues/3165) opened by [@streamnsight](https://github.com/streamnsight)*
- [`a0df28f`](https://github.com/tobymao/sqlglot/commit/a0df28f4092ca84d07111cead550b9d6772993ad) - can't simplify null parens *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`eb0a6c3`](https://github.com/tobymao/sqlglot/commit/eb0a6c31c92d16abe785087271b14f7611ff24bc) - actually pop the where statement *(commit by [@tobymao](https://github.com/tobymao))*
- [`f6778ef`](https://github.com/tobymao/sqlglot/commit/f6778ef039a646fb5641f0e91b28f6cbc2f52e78) - add recursion test *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.0.0] - 2024-03-18
### :sparkles: New Features
- [`e838713`](https://github.com/tobymao/sqlglot/commit/e838713bdb3da8a5d04eed43b2015a9d3a71addd) - **mysql**: Support for multi arg GROUP_CONCAT *(PR [#3150](https://github.com/tobymao/sqlglot/pull/3150) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3142](https://github.com/tobymao/sqlglot/issues/3142) opened by [@optionals](https://github.com/optionals)*
- [`7e8f134`](https://github.com/tobymao/sqlglot/commit/7e8f134fb2d78940b27f81be7a347caee371601c) - **test**: Add standard alias to some TPC-DS query *(PR [#3151](https://github.com/tobymao/sqlglot/pull/3151) by [@fool1280](https://github.com/fool1280))*
- [`6d0e965`](https://github.com/tobymao/sqlglot/commit/6d0e9658733c672154ec69fd2a4140332954b466) - add skip limit token kwarg *(PR [#3149](https://github.com/tobymao/sqlglot/pull/3149) by [@z3z1ma](https://github.com/z3z1ma))*
- [`3ed5845`](https://github.com/tobymao/sqlglot/commit/3ed58458f9c89a1241a6fa6bb787e236289af58d) - include table alias in bigquery unnest *(PR [#3156](https://github.com/tobymao/sqlglot/pull/3156) by [@eakmanrq](https://github.com/eakmanrq))*
- [`706fac3`](https://github.com/tobymao/sqlglot/commit/706fac382fbde6c1c6af8acd277291a3f18f94ee) - add bigquery mod op *(PR [#3157](https://github.com/tobymao/sqlglot/pull/3157) by [@eakmanrq](https://github.com/eakmanrq))*
- [`6ffdc25`](https://github.com/tobymao/sqlglot/commit/6ffdc25c673db33c3e9ac5a2c6970c4331a3f978) - **clickhouse**: Support for INSERT INTO TABLE FUNCTION *(PR [#3162](https://github.com/tobymao/sqlglot/pull/3162) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3161](https://github.com/tobymao/sqlglot/issues/3161) opened by [@mlipiev](https://github.com/mlipiev)*
- [`021af42`](https://github.com/tobymao/sqlglot/commit/021af4206f4ff2ad4bd57d30cf1f2f78f24fc844) - **snowflake**: Adding support for DATE, TO_DATE, TRY_TO_DATE functions *(PR [#3160](https://github.com/tobymao/sqlglot/pull/3160) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3152](https://github.com/tobymao/sqlglot/issues/3152) opened by [@lei0zhou](https://github.com/lei0zhou)*

### :bug: Bug Fixes
- [`cfde552`](https://github.com/tobymao/sqlglot/commit/cfde552005e1682d6dd1b71850e163021bf4532f) - asof identifier closes [#3153](https://github.com/tobymao/sqlglot/pull/3153) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b1e6eef`](https://github.com/tobymao/sqlglot/commit/b1e6eefcd4dd60f541047a10ed35c1ac733a636c) - bigquery values transpilation with no column alias *(commit by [@tobymao](https://github.com/tobymao))*
- [`c0760b3`](https://github.com/tobymao/sqlglot/commit/c0760b3be11af701273e55c2c976d67d9a575cc4) - parse over any closes [#3155](https://github.com/tobymao/sqlglot/pull/3155) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`38b931e`](https://github.com/tobymao/sqlglot/commit/38b931ebed9255ce5d0d6185414b6f01ca02b0fd) - pin ruff *(commit by [@tobymao](https://github.com/tobymao))*
- [`66a6284`](https://github.com/tobymao/sqlglot/commit/66a62847342f15b8d412fb91814342951fe23247) - improve type hints of Query methods *(PR [#3148](https://github.com/tobymao/sqlglot/pull/3148) by [@georgesittas](https://github.com/georgesittas))*


## [v22.5.0] - 2024-03-14
### :boom: BREAKING CHANGES
- due to [`2b4952e`](https://github.com/tobymao/sqlglot/commit/2b4952eb151b3f20739803e7bf443b56da457b1f) - desugar LOG2 and LOG10 by converting them into LOG *(PR [#3139](https://github.com/tobymao/sqlglot/pull/3139) by [@georgesittas](https://github.com/georgesittas))*:

  desugar LOG2 and LOG10 by converting them into LOG (#3139)


### :sparkles: New Features
- [`c01ff44`](https://github.com/tobymao/sqlglot/commit/c01ff44b036526807624ba2d1f4b247081e8c56f) - **snowflake**: Add TO_TIMESTAMP test and update env.py *(PR [#3130](https://github.com/tobymao/sqlglot/pull/3130) by [@fool1280](https://github.com/fool1280))*
- [`8526c8e`](https://github.com/tobymao/sqlglot/commit/8526c8e30376c0826ab31a0a342656d5ebced662) - **tsql**: transpile LIMIT with OFFSET properly *(PR [#3145](https://github.com/tobymao/sqlglot/pull/3145) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3144](https://github.com/tobymao/sqlglot/issues/3144) opened by [@iMayK](https://github.com/iMayK)*

### :bug: Bug Fixes
- [`a9db8ff`](https://github.com/tobymao/sqlglot/commit/a9db8ff6ac528da8c3a7a66f0b80a3f0d1a0ed7e) - don't mutate parent nested classes if undefined in a dialect *(PR [#3134](https://github.com/tobymao/sqlglot/pull/3134) by [@georgesittas](https://github.com/georgesittas))*
- [`d6bac3e`](https://github.com/tobymao/sqlglot/commit/d6bac3e54c6445c52daa04015b1b2e4a6933e682) - **duckdb**: Slice + Array bug *(PR [#3137](https://github.com/tobymao/sqlglot/pull/3137) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3136](https://github.com/tobymao/sqlglot/issues/3136) opened by [@cpcloud](https://github.com/cpcloud)*
- [`230a845`](https://github.com/tobymao/sqlglot/commit/230a845d82576b24ef8a3bbcc83677ed637e8247) - optimizer bugs *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`2b4952e`](https://github.com/tobymao/sqlglot/commit/2b4952eb151b3f20739803e7bf443b56da457b1f) - desugar LOG2 and LOG10 by converting them into LOG *(PR [#3139](https://github.com/tobymao/sqlglot/pull/3139) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3138](https://github.com/tobymao/sqlglot/issues/3138) opened by [@baruchoxman](https://github.com/baruchoxman)*

### :wrench: Chores
- [`ebbf5a1`](https://github.com/tobymao/sqlglot/commit/ebbf5a14da12b442bff84d93f8542d4322e0811d) - copy sqlglot.svg in docs/ to also display logo in website *(PR [#3147](https://github.com/tobymao/sqlglot/pull/3147) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3146](https://github.com/tobymao/sqlglot/issues/3146) opened by [@lostmygithubaccount](https://github.com/lostmygithubaccount)*


## [v22.4.0] - 2024-03-12
### :boom: BREAKING CHANGES
- due to [`b1c8cac`](https://github.com/tobymao/sqlglot/commit/b1c8cace6ed3e58657726fa5617a6df63d91f737) - traverse union scopes iteratively *(PR [#3112](https://github.com/tobymao/sqlglot/pull/3112) by [@georgesittas](https://github.com/georgesittas))*:

  traverse union scopes iteratively (#3112)


### :sparkles: New Features
- [`88033da`](https://github.com/tobymao/sqlglot/commit/88033dad05550cde05dcb86cce61a621c071382c) - **test**: add more passing tpcds tests *(PR [#3110](https://github.com/tobymao/sqlglot/pull/3110) by [@fool1280](https://github.com/fool1280))*
- [`804af34`](https://github.com/tobymao/sqlglot/commit/804af347a7cefac251b78fdcb8ff35b63c249d82) - **duckdb**: add support for positional joins *(PR [#3111](https://github.com/tobymao/sqlglot/pull/3111) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3109](https://github.com/tobymao/sqlglot/issues/3109) opened by [@dylanscott](https://github.com/dylanscott)*
- [`c4e7bbf`](https://github.com/tobymao/sqlglot/commit/c4e7bbfd3d88f3efb1fea806f85091dbe32379cf) - improve transpilation of TO_NUMBER *(commit by [@codeDing18](https://github.com/codeDing18))*
- [`80d484c`](https://github.com/tobymao/sqlglot/commit/80d484c428329fb53c905fff9f86ea0ee7bcef3b) - **postgres**: generate StrToDate *(PR [#3124](https://github.com/tobymao/sqlglot/pull/3124) by [@georgesittas](https://github.com/georgesittas))*
- [`09708f5`](https://github.com/tobymao/sqlglot/commit/09708f571bb7b62e96bbfba363b00714243d1a17) - Adding EXCLUDE constraint support *(PR [#3116](https://github.com/tobymao/sqlglot/pull/3116) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3097](https://github.com/tobymao/sqlglot/issues/3097) opened by [@dezhin](https://github.com/dezhin)*
- [`9b25a8e`](https://github.com/tobymao/sqlglot/commit/9b25a8e3788c4cc7a299c703fe5b4086fe86015d) - Adding BACKUP property *(PR [#3127](https://github.com/tobymao/sqlglot/pull/3127) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3125](https://github.com/tobymao/sqlglot/issues/3125) opened by [@hsheth2](https://github.com/hsheth2)*
- [`0ea849b`](https://github.com/tobymao/sqlglot/commit/0ea849b35bd3dd980c4f851d3ea7b5bc628e6fb5) - Adding NAME data type in Postgres/Redshift *(PR [#3128](https://github.com/tobymao/sqlglot/pull/3128) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3123](https://github.com/tobymao/sqlglot/issues/3123) opened by [@hsheth2](https://github.com/hsheth2)*

### :bug: Bug Fixes
- [`c333017`](https://github.com/tobymao/sqlglot/commit/c333017fe49c0645cdaa3a75d0a7cc6a5b46dddc) - correctly generate ArrayJoin in various dialects *(PR [#3120](https://github.com/tobymao/sqlglot/pull/3120) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3119](https://github.com/tobymao/sqlglot/issues/3119) opened by [@cpcloud](https://github.com/cpcloud)*
- [`12d72a6`](https://github.com/tobymao/sqlglot/commit/12d72a6ff6534919979f77a5f045aa9d947d9a09) - make the lineage sources dict type covariant *(PR [#3122](https://github.com/tobymao/sqlglot/pull/3122) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3121](https://github.com/tobymao/sqlglot/issues/3121) opened by [@rexledesma](https://github.com/rexledesma)*
- [`b1c8cac`](https://github.com/tobymao/sqlglot/commit/b1c8cace6ed3e58657726fa5617a6df63d91f737) - traverse union scopes iteratively *(PR [#3112](https://github.com/tobymao/sqlglot/pull/3112) by [@georgesittas](https://github.com/georgesittas))*
- [`94b5a2f`](https://github.com/tobymao/sqlglot/commit/94b5a2fcba3c41d38734f045b7f1d5d4735e4828) - **athena**: Fix CREATE TABLE properties, STRING data type *(PR [#3129](https://github.com/tobymao/sqlglot/pull/3129) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3126](https://github.com/tobymao/sqlglot/issues/3126) opened by [@matthias-Q](https://github.com/matthias-Q)*

### :recycle: Refactors
- [`0ce9ef1`](https://github.com/tobymao/sqlglot/commit/0ce9ef12d9c030b145d7a7a7432bfc188d6c179a) - improve parsing of storage provider setting in index params *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v22.3.1] - 2024-03-09
### :sparkles: New Features
- [`80b2320`](https://github.com/tobymao/sqlglot/commit/80b23201f9668a5845002c1c21b0a394003847f9) - no recursion dfs *(PR [#3105](https://github.com/tobymao/sqlglot/pull/3105) by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`fa84e2c`](https://github.com/tobymao/sqlglot/commit/fa84e2c2d9ae349033039ec649decc371561e421) - copy all arg keys, including those set to None *(PR [#3108](https://github.com/tobymao/sqlglot/pull/3108) by [@georgesittas](https://github.com/georgesittas))*


## [v22.3.0] - 2024-03-08
### :sparkles: New Features
- [`46c9c2c`](https://github.com/tobymao/sqlglot/commit/46c9c2c35ea5132995cb07a99b94d18d959e6172) - **snowflake**: parse CREATE SEQUENCE *(PR [#3072](https://github.com/tobymao/sqlglot/pull/3072) by [@tekumara](https://github.com/tekumara))*
  - :arrow_lower_right: *addresses issue [#2954](https://github.com/tobymao/sqlglot/issues/2954) opened by [@tharwan](https://github.com/tharwan)*
- [`9f1e1ad`](https://github.com/tobymao/sqlglot/commit/9f1e1ad4350fb412319511825ca3da9b9af14084) - add Athena dialect *(PR [#3089](https://github.com/tobymao/sqlglot/pull/3089) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3087](https://github.com/tobymao/sqlglot/issues/3087) opened by [@sbrandtb](https://github.com/sbrandtb)*
- [`efee388`](https://github.com/tobymao/sqlglot/commit/efee38858c4501ccace4b3eb3f066cb352f3ac60) - no more recursion for union generation *(PR [#3101](https://github.com/tobymao/sqlglot/pull/3101) by [@tobymao](https://github.com/tobymao))*
- [`ddab9df`](https://github.com/tobymao/sqlglot/commit/ddab9dff663985d9473ce4b2dbe4fe266ae1bdf7) - **duckdb**: add support for exp.ArrayJoin *(PR [#3102](https://github.com/tobymao/sqlglot/pull/3102) by [@seruman](https://github.com/seruman))*
- [`8d5be0c`](https://github.com/tobymao/sqlglot/commit/8d5be0cf54e77000b220cfcca0edfdeb1759b70b) - **duckdb**: make ARRAY_TO_STRING transpilable to other dialects *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a38db01`](https://github.com/tobymao/sqlglot/commit/a38db014cce8ada9554c205c879ae0c0dfda1b14) - Generalizing CREATE SEQUENCE *(PR [#3090](https://github.com/tobymao/sqlglot/pull/3090) by [@VaggelisD](https://github.com/VaggelisD))*
- [`18fd079`](https://github.com/tobymao/sqlglot/commit/18fd0794302a1ecaa91be9dfbc7feddd0b8a3b05) - no recursion copy *(PR [#3103](https://github.com/tobymao/sqlglot/pull/3103) by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`b101013`](https://github.com/tobymao/sqlglot/commit/b101013336d0aef6dc99b5ebef85afc12591e212) - subquery edge cases *(PR [#3076](https://github.com/tobymao/sqlglot/pull/3076) by [@tobymao](https://github.com/tobymao))*
- [`8c4400b`](https://github.com/tobymao/sqlglot/commit/8c4400ba194661d1e1ee4aa4ea2649b2356a5f02) - **bigquery**: more table qualification edge cases closes [#3083](https://github.com/tobymao/sqlglot/pull/3083) *(commit by [@tobymao](https://github.com/tobymao))*
- [`d898f55`](https://github.com/tobymao/sqlglot/commit/d898f559fac44789da08689e835619f978c05a3e) - **bigquery**: even more edge cases *(commit by [@tobymao](https://github.com/tobymao))*
- [`4fb74ff`](https://github.com/tobymao/sqlglot/commit/4fb74ff61effd9e5fa8593cdf1c9229d5106ab7e) - dataframe optimize user input *(PR [#3092](https://github.com/tobymao/sqlglot/pull/3092) by [@eakmanrq](https://github.com/eakmanrq))*
  - :arrow_lower_right: *fixes issue [#3091](https://github.com/tobymao/sqlglot/issues/3091) opened by [@alexdemeo](https://github.com/alexdemeo)*

### :recycle: Refactors
- [`cea7508`](https://github.com/tobymao/sqlglot/commit/cea7508c5f2b5838e889486d28df47ad9b263345) - **lineage**: simplify `Node.walk()` *(PR [#3098](https://github.com/tobymao/sqlglot/pull/3098) by [@rexledesma](https://github.com/rexledesma))*
- [`ebe5a46`](https://github.com/tobymao/sqlglot/commit/ebe5a462ed50711d6ded18b454c5294e487e323f) - **executor**: simplify column type inference *(PR [#3104](https://github.com/tobymao/sqlglot/pull/3104) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`6c67a2b`](https://github.com/tobymao/sqlglot/commit/6c67a2b0dbe2a66ea4ce2e008101f4cf41b1c517) - reduce size of tpcds *(commit by [@tobymao](https://github.com/tobymao))*
- [`21e4fca`](https://github.com/tobymao/sqlglot/commit/21e4fca2b744a22981d8ff1696986061d3344d40) - update dialect count in README to include Athena *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v22.2.1] - 2024-03-04
### :sparkles: New Features
- [`19e07f3`](https://github.com/tobymao/sqlglot/commit/19e07f39688f53fafac25655616883363f20b1cf) - initial commit prql *(commit by [@tobymao](https://github.com/tobymao))*
- [`13b64fd`](https://github.com/tobymao/sqlglot/commit/13b64fdcdde35e8fe022f76f4f2a5d55d53b982f) - more prql *(commit by [@tobymao](https://github.com/tobymao))*
- [`3d263aa`](https://github.com/tobymao/sqlglot/commit/3d263aafbfb8d45bc678914e1eb925592c30eaf8) - **oracle**: Support for INSERT hint *(PR [#3077](https://github.com/tobymao/sqlglot/pull/3077) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3074](https://github.com/tobymao/sqlglot/issues/3074) opened by [@sunrutcon](https://github.com/sunrutcon)*

### :bug: Bug Fixes
- [`c51b64f`](https://github.com/tobymao/sqlglot/commit/c51b64fa6a437698fd8b347d98ffaf9fb543d2d5) - json extract precedence closes [#3068](https://github.com/tobymao/sqlglot/pull/3068) *(commit by [@tobymao](https://github.com/tobymao))*
- [`223a475`](https://github.com/tobymao/sqlglot/commit/223a4751f88809710872fa7d757d22d9eeeb4f40) - **planner**: don't overwrite JOIN step name *(PR [#3071](https://github.com/tobymao/sqlglot/pull/3071) by [@georgesittas](https://github.com/georgesittas))*
- [`2770ddc`](https://github.com/tobymao/sqlglot/commit/2770ddcc34148f85caeabf2b6f4f799b3e825a6c) - drop CLUSTER/DISTRIBUTED/SORT BY modifiers when unsupported *(PR [#3069](https://github.com/tobymao/sqlglot/pull/3069) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3066](https://github.com/tobymao/sqlglot/issues/3066) opened by [@cploonker](https://github.com/cploonker)*
- [`4173ea2`](https://github.com/tobymao/sqlglot/commit/4173ea29bbd8944896c259fe45209de69fcbdc46) - handle lineage of subqueries *(PR [#3075](https://github.com/tobymao/sqlglot/pull/3075) by [@tobymao](https://github.com/tobymao))*
- [`0ebce40`](https://github.com/tobymao/sqlglot/commit/0ebce40f5e524c61ece022bbf8640556e880a4bf) - **redshift**: don't transform multi-arg DISTINCT clause *(PR [#3079](https://github.com/tobymao/sqlglot/pull/3079) by [@georgesittas](https://github.com/georgesittas))*


## [v22.2.0] - 2024-03-01
### :boom: BREAKING CHANGES
- due to [`08bafbd`](https://github.com/tobymao/sqlglot/commit/08bafbd597b6a81e222832fac9f068f1290e41fa) - handle unnesting groups closes [#3056](https://github.com/tobymao/sqlglot/pull/3056) *(PR [#3058](https://github.com/tobymao/sqlglot/pull/3058) by [@tobymao](https://github.com/tobymao))*:

  handle unnesting groups closes #3056 (#3058)

- due to [`4029fab`](https://github.com/tobymao/sqlglot/commit/4029fab81e9abcedd6321baaf5baf9aa192f643d) - expand alias refs of double aggs if it is a window func *(PR [#3059](https://github.com/tobymao/sqlglot/pull/3059) by [@tobymao](https://github.com/tobymao))*:

  expand alias refs of double aggs if it is a window func (#3059)


### :sparkles: New Features
- [`8662e31`](https://github.com/tobymao/sqlglot/commit/8662e31bd115eb668c4b74377ed2985937e81510) - **postgres**: improve transpilation of JSON array unnesting *(PR [#3063](https://github.com/tobymao/sqlglot/pull/3063) by [@georgesittas](https://github.com/georgesittas))*
- [`c9bde44`](https://github.com/tobymao/sqlglot/commit/c9bde44bac5f7026388ec6357a6c1e00ee760edc) - Making parse_number & parse_string more lenient *(PR [#3064](https://github.com/tobymao/sqlglot/pull/3064) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`08bafbd`](https://github.com/tobymao/sqlglot/commit/08bafbd597b6a81e222832fac9f068f1290e41fa) - handle unnesting groups closes [#3056](https://github.com/tobymao/sqlglot/pull/3056) *(PR [#3058](https://github.com/tobymao/sqlglot/pull/3058) by [@tobymao](https://github.com/tobymao))*
- [`4029fab`](https://github.com/tobymao/sqlglot/commit/4029fab81e9abcedd6321baaf5baf9aa192f643d) - expand alias refs of double aggs if it is a window func *(PR [#3059](https://github.com/tobymao/sqlglot/pull/3059) by [@tobymao](https://github.com/tobymao))*
- [`4e6e82c`](https://github.com/tobymao/sqlglot/commit/4e6e82c9d4d9ef33635446e19b4b44f3ae27160c) - **snowflake**: allow any identifier after : closes [#3061](https://github.com/tobymao/sqlglot/pull/3061) *(PR [#3062](https://github.com/tobymao/sqlglot/pull/3062) by [@georgesittas](https://github.com/georgesittas))*
- [`e2becea`](https://github.com/tobymao/sqlglot/commit/e2becead1e6be12ddf8bde703d2c403220506784) - is distinct from parsing *(commit by [@tobymao](https://github.com/tobymao))*
- [`c8a753b`](https://github.com/tobymao/sqlglot/commit/c8a753b488d99172db9df10616e8bd3431452ff8) - Ignore Identifier nodes in the diffing algorithm *(PR [#3065](https://github.com/tobymao/sqlglot/pull/3065) by [@izeigerman](https://github.com/izeigerman))*


## [v22.1.1] - 2024-02-29
### :sparkles: New Features
- [`1e25ec9`](https://github.com/tobymao/sqlglot/commit/1e25ec984510a1ffee76956b0dcb15bcd84f5d44) - **test**: handle NULL value in TPC-DS  *(PR [#3052](https://github.com/tobymao/sqlglot/pull/3052) by [@fool1280](https://github.com/fool1280))*
- [`ad21b6b`](https://github.com/tobymao/sqlglot/commit/ad21b6b47716d394ca6b8fb3b82d58b887d5adb3) - **test**: add more passing tpc-ds test *(PR [#3053](https://github.com/tobymao/sqlglot/pull/3053) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`08249af`](https://github.com/tobymao/sqlglot/commit/08249af50351a24277e1f3f1574629eb5c68d3a5) - Hive UnixToTime regression, README stale results *(PR [#3055](https://github.com/tobymao/sqlglot/pull/3055) by [@VaggelisD](https://github.com/VaggelisD))*
- [`39b3813`](https://github.com/tobymao/sqlglot/commit/39b381341fe697ae54f5d3a438b4035447fe552a) - **redshift**: don't pop recursive cte table columns *(commit by [@tobymao](https://github.com/tobymao))*
- [`6a9501f`](https://github.com/tobymao/sqlglot/commit/6a9501f7407be3682ce3b9cc73b7340ad9a0c2e8) - ensure UDF identifier quotes are preserved *(PR [#3057](https://github.com/tobymao/sqlglot/pull/3057) by [@georgesittas](https://github.com/georgesittas))*


## [v22.1.0] - 2024-02-29
### :sparkles: New Features
- [`6393979`](https://github.com/tobymao/sqlglot/commit/63939796b39c69b25adfc6f224ccd4761f23cb66) - **oracle**: connect_by_root closes [#3050](https://github.com/tobymao/sqlglot/pull/3050) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`bd0a40d`](https://github.com/tobymao/sqlglot/commit/bd0a40dde2ab2ad168ada0d5bae0c99fba9d762f) - normalize column for lineage and raise if cannot find closes [#3049](https://github.com/tobymao/sqlglot/pull/3049) *(commit by [@tobymao](https://github.com/tobymao))*


## [v22.0.2] - 2024-02-28
### :sparkles: New Features
- [`51f8d58`](https://github.com/tobymao/sqlglot/commit/51f8d5897b18e6f7c0bc66881a3e36c8842ff2ff) - **tsql**: add support for OPTION clause, select only *(PR [#3025](https://github.com/tobymao/sqlglot/pull/3025) by [@nadav-botanica](https://github.com/nadav-botanica))*
- [`c9eef99`](https://github.com/tobymao/sqlglot/commit/c9eef99b8fe3367c22a8186fb397ad550ac11386) - Support for TRUNCATE TABLE/DATABASE DDL *(PR [#3026](https://github.com/tobymao/sqlglot/pull/3026) by [@VaggelisD](https://github.com/VaggelisD))*
- [`703b878`](https://github.com/tobymao/sqlglot/commit/703b87816c3e5f7b50407d2f2a14f3a9cba4e3f8) - **mysql**: add LOCK property, allow properties after ALTER TABLE *(PR [#3027](https://github.com/tobymao/sqlglot/pull/3027) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3020](https://github.com/tobymao/sqlglot/issues/3020) opened by [@samotarnik](https://github.com/samotarnik)*

### :bug: Bug Fixes
- [`bc4acb9`](https://github.com/tobymao/sqlglot/commit/bc4acb9582a80a6c3d4b491b48a68f110e399e3a) - allow trailing comma in ORDER BY list *(PR [#3031](https://github.com/tobymao/sqlglot/pull/3031) by [@georgesittas](https://github.com/georgesittas))*
- [`4105639`](https://github.com/tobymao/sqlglot/commit/4105639ddbbc504d4bd4607511ac35e8ca30c774) - **bigquery**: unquoted project-0.x closes [#3029](https://github.com/tobymao/sqlglot/pull/3029) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f1f2aec`](https://github.com/tobymao/sqlglot/commit/f1f2aecb09c6c0d9a965d87669368945abd112cc) - bigquery edgecase *(commit by [@tobymao](https://github.com/tobymao))*
- [`5c01c01`](https://github.com/tobymao/sqlglot/commit/5c01c010348271e8cfddea3ed0ac51293c3819b3) - handle falsey values for replace_placeholders kwargs *(PR [#3036](https://github.com/tobymao/sqlglot/pull/3036) by [@sarchila](https://github.com/sarchila))*
- [`ccfbb22`](https://github.com/tobymao/sqlglot/commit/ccfbb2238131bda8fc7a3ad8a9c50a0f009dac52) - **clickhouse**: make CTE expression parser more flexible fixes [#3038](https://github.com/tobymao/sqlglot/pull/3038) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`30e0bb1`](https://github.com/tobymao/sqlglot/commit/30e0bb13162e75c53b031bbb69c66093f8ad4a96) - another edge case *(commit by [@tobymao](https://github.com/tobymao))*
- [`0d93852`](https://github.com/tobymao/sqlglot/commit/0d938524a618b4bd7c057623a2c8755ca3afec6d) - **oracle**: handle GLOBAL/PRIVATE keyword in temp table DDL *(PR [#3045](https://github.com/tobymao/sqlglot/pull/3045) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3037](https://github.com/tobymao/sqlglot/issues/3037) opened by [@gforsyth](https://github.com/gforsyth)*
- [`e89d38d`](https://github.com/tobymao/sqlglot/commit/e89d38ddd5f699f2ac09baf77238ad5fab00acb8) - **duckdb**: recognize ENUM as a type *(PR [#3044](https://github.com/tobymao/sqlglot/pull/3044) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3043](https://github.com/tobymao/sqlglot/issues/3043) opened by [@joouha](https://github.com/joouha)*
- [`4db7781`](https://github.com/tobymao/sqlglot/commit/4db77816a44652b3edc8aae5aab24242854f9a14) - avoid raising a KeyError in the lineage module, log a warning *(PR [#3048](https://github.com/tobymao/sqlglot/pull/3048) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`5337980`](https://github.com/tobymao/sqlglot/commit/53379805454f0e6f325581b839d2fcb37c10de1b) - simplify parsing of keyword sequences as Vars *(PR [#3034](https://github.com/tobymao/sqlglot/pull/3034) by [@georgesittas](https://github.com/georgesittas))*
- [`bc35c59`](https://github.com/tobymao/sqlglot/commit/bc35c59004cb3fb9849f0ee8e5f06b356396c0b0) - use _parse_var_from_options for USE statement parser *(PR [#3035](https://github.com/tobymao/sqlglot/pull/3035) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c0d355a`](https://github.com/tobymao/sqlglot/commit/c0d355a27d86539dfd95a87fea7e1bd75c4fabe4) - bump sqlglotrs to 0.1.2 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v22.0.1] - 2024-02-26
### :bug: Bug Fixes
- [`e2fc6e8`](https://github.com/tobymao/sqlglot/commit/e2fc6e88dc7ae52d956dd84721de197c6c698d90) - **optimizer**: fix parent mutation of new_projections in column qualifier *(PR [#3030](https://github.com/tobymao/sqlglot/pull/3030) by [@georgesittas](https://github.com/georgesittas))*


## [v22.0.0] - 2024-02-26
### :boom: BREAKING CHANGES
- due to [`2507aa2`](https://github.com/tobymao/sqlglot/commit/2507aa2dbad3304304558565f266a7f94acd9e98) - consolidate Subqueryable and Unionable into Query expression *(PR [#2992](https://github.com/tobymao/sqlglot/pull/2992) by [@georgesittas](https://github.com/georgesittas))*:

  consolidate Subqueryable and Unionable into Query expression (#2992)

- due to [`d5eb2b1`](https://github.com/tobymao/sqlglot/commit/d5eb2b1e0907026e6e981a8f453f747cb16f44d6) - make implicit unnest syntax explicit by using UNNEST calls *(PR [#3005](https://github.com/tobymao/sqlglot/pull/3005) by [@georgesittas](https://github.com/georgesittas))*:

  make implicit unnest syntax explicit by using UNNEST calls (#3005)

- due to [`238f9aa`](https://github.com/tobymao/sqlglot/commit/238f9aa7c32037d0c280cfe6ece77eed9c311cc5) - refactor structs to always be aliases *(PR [#3017](https://github.com/tobymao/sqlglot/pull/3017) by [@tobymao](https://github.com/tobymao))*:

  refactor structs to always be aliases (#3017)

- due to [`06bcfcd`](https://github.com/tobymao/sqlglot/commit/06bcfcdf69f850693d941675bbcfce1aa80482f6) - select expressions not statements closes [#3022](https://github.com/tobymao/sqlglot/pull/3022), statements can be parsed without into *(commit by [@tobymao](https://github.com/tobymao))*:

  select expressions not statements closes #3022, statements can be parsed without into

- due to [`1612e62`](https://github.com/tobymao/sqlglot/commit/1612e622bd3514d9ca366837f47452969e5267d8) - Add reference to lineage node *(PR [#3018](https://github.com/tobymao/sqlglot/pull/3018) by [@vchan](https://github.com/vchan))*:

  Add reference to lineage node (#3018)


### :sparkles: New Features
- [`e50609b`](https://github.com/tobymao/sqlglot/commit/e50609b119c65407f4f7fe27f06510187dc750a0) - Supporting RANGE <-> GENERATE_SERIES between DuckDB & SQLite *(PR [#3010](https://github.com/tobymao/sqlglot/pull/3010) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1709ec2`](https://github.com/tobymao/sqlglot/commit/1709ec2519edc4b1a91f435d76f1b962355be326) - bigquery e6s format *(commit by [@tobymao](https://github.com/tobymao))*
- [`17e34e7`](https://github.com/tobymao/sqlglot/commit/17e34e79d22e3c8211f1bf42047d4ed3557628b6) - add unnest type annotations *(PR [#3019](https://github.com/tobymao/sqlglot/pull/3019) by [@tobymao](https://github.com/tobymao))*
- [`efdbc12`](https://github.com/tobymao/sqlglot/commit/efdbc127a06b1c6204327caa0d6b0cb01590da13) - clickhouse prewhere closes [#3024](https://github.com/tobymao/sqlglot/pull/3024) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1612e62`](https://github.com/tobymao/sqlglot/commit/1612e622bd3514d9ca366837f47452969e5267d8) - Add reference to lineage node *(PR [#3018](https://github.com/tobymao/sqlglot/pull/3018) by [@vchan](https://github.com/vchan))*
- [`5c3bd10`](https://github.com/tobymao/sqlglot/commit/5c3bd1074960874b4557b13df6d30782fe7b0757) - **test**: add more passing tests of tpc-ds *(PR [#3016](https://github.com/tobymao/sqlglot/pull/3016) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`7f547e6`](https://github.com/tobymao/sqlglot/commit/7f547e641f7a0ecaa804d5bea14bd24abce1d346) - it's actually seconds + fraction *(commit by [@tobymao](https://github.com/tobymao))*
- [`238f9aa`](https://github.com/tobymao/sqlglot/commit/238f9aa7c32037d0c280cfe6ece77eed9c311cc5) - refactor structs to always be aliases *(PR [#3017](https://github.com/tobymao/sqlglot/pull/3017) by [@tobymao](https://github.com/tobymao))*
  - :arrow_lower_right: *fixes issue [#3015](https://github.com/tobymao/sqlglot/issues/3015) opened by [@wizardxz](https://github.com/wizardxz)*
- [`06bcfcd`](https://github.com/tobymao/sqlglot/commit/06bcfcdf69f850693d941675bbcfce1aa80482f6) - select expressions not statements closes [#3022](https://github.com/tobymao/sqlglot/pull/3022), statements can be parsed without into *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`2507aa2`](https://github.com/tobymao/sqlglot/commit/2507aa2dbad3304304558565f266a7f94acd9e98) - consolidate Subqueryable and Unionable into Query expression *(PR [#2992](https://github.com/tobymao/sqlglot/pull/2992) by [@georgesittas](https://github.com/georgesittas))*
- [`d5eb2b1`](https://github.com/tobymao/sqlglot/commit/d5eb2b1e0907026e6e981a8f453f747cb16f44d6) - make implicit unnest syntax explicit by using UNNEST calls *(PR [#3005](https://github.com/tobymao/sqlglot/pull/3005) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2996](https://github.com/tobymao/sqlglot/issues/2996) opened by [@wizardxz](https://github.com/wizardxz)*
- [`8943179`](https://github.com/tobymao/sqlglot/commit/8943179dfadba4ed36740322e1e5d3611032b51e) - move limit method to Query, get rid of Subquery.subquery override *(PR [#3013](https://github.com/tobymao/sqlglot/pull/3013) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`9595240`](https://github.com/tobymao/sqlglot/commit/9595240a1c0f0e5ace9f67f31564e5d5edb9a9d2) - make prewhere clickhouse only *(commit by [@tobymao](https://github.com/tobymao))*


## [v21.2.1] - 2024-02-22
### :sparkles: New Features
- [`2a88e40`](https://github.com/tobymao/sqlglot/commit/2a88e40da89fa083bbd8fd0174082fa8e677780a) - **bigquery**: support ELSE and ELSEIF procedural statements *(PR [#3011](https://github.com/tobymao/sqlglot/pull/3011) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3009](https://github.com/tobymao/sqlglot/issues/3009) opened by [@razvan-am](https://github.com/razvan-am)*
- [`d2e15ed`](https://github.com/tobymao/sqlglot/commit/d2e15ed9b2ab2699f7105f73170b9d780293d432) - improve transpilation of Doris' MONTHS_ADD *(PR [#3012](https://github.com/tobymao/sqlglot/pull/3012) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`c23ac05`](https://github.com/tobymao/sqlglot/commit/c23ac05379e2aa5cb5681e26e2c0b8137300baa3) - bigquery group by order by rewriting with indices *(commit by [@tobymao](https://github.com/tobymao))*


## [v21.2.0] - 2024-02-22
### :boom: BREAKING CHANGES
- due to [`2940417`](https://github.com/tobymao/sqlglot/commit/2940417116761f821c913bf093759243db33c343) - simplify ADD CONSTRAINT handling *(PR [#2990](https://github.com/tobymao/sqlglot/pull/2990) by [@georgesittas](https://github.com/georgesittas))*:

  simplify ADD CONSTRAINT handling (#2990)


### :sparkles: New Features
- [`7c48079`](https://github.com/tobymao/sqlglot/commit/7c4807918de53d18fbfe0295b2644f0ad46003a8) - support parameters in BigQuery / DuckDB *(PR [#2991](https://github.com/tobymao/sqlglot/pull/2991) by [@r1b](https://github.com/r1b))*
- [`b7c2744`](https://github.com/tobymao/sqlglot/commit/b7c2744eba3df631b575e8ab35f29f46419f83ba) - **tests**: update test_executor with tpc-ds  *(PR [#2983](https://github.com/tobymao/sqlglot/pull/2983) by [@fool1280](https://github.com/fool1280))*
- [`c433cad`](https://github.com/tobymao/sqlglot/commit/c433cad7df383e97308ceb946d7f1dc171a5d60b) - allow more leniant bigquery wildcard parsing *(PR [#2998](https://github.com/tobymao/sqlglot/pull/2998) by [@tobymao](https://github.com/tobymao))*
- [`8607247`](https://github.com/tobymao/sqlglot/commit/860724732b70b5557221998a45c3c950b39d664a) - support LEFT JOIN UNNEST in duckdb *(PR [#2999](https://github.com/tobymao/sqlglot/pull/2999) by [@r1b](https://github.com/r1b))*
- [`64e38ed`](https://github.com/tobymao/sqlglot/commit/64e38edb32f9a66a9503e75424d0545da3dbe5df) - add support for more Snowflake SHOW commands *(PR [#3002](https://github.com/tobymao/sqlglot/pull/3002) by [@DanCardin](https://github.com/DanCardin))*

### :bug: Bug Fixes
- [`bc18f56`](https://github.com/tobymao/sqlglot/commit/bc18f56a39e0034e2b285efd7a882a417c517a99) - **optimizer**: don't coerce nested arg types in annotate_by_args *(PR [#2997](https://github.com/tobymao/sqlglot/pull/2997) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2152](https://github.com/TobikoData/sqlmesh/issues/2152) opened by [@plaflamme](https://github.com/plaflamme)*
- [`ccd8cc0`](https://github.com/tobymao/sqlglot/commit/ccd8cc01429d21653198edce079679e17dbb22f6) - doris to_char closes [#3001](https://github.com/tobymao/sqlglot/pull/3001) *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`2940417`](https://github.com/tobymao/sqlglot/commit/2940417116761f821c913bf093759243db33c343) - simplify ADD CONSTRAINT handling *(PR [#2990](https://github.com/tobymao/sqlglot/pull/2990) by [@georgesittas](https://github.com/georgesittas))*
- [`d2711f7`](https://github.com/tobymao/sqlglot/commit/d2711f717aac4a7b624225d31c7fa827f8287476) - clean up duplicative placeholder_sql implementations *(PR [#2993](https://github.com/tobymao/sqlglot/pull/2993) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`53efb58`](https://github.com/tobymao/sqlglot/commit/53efb587a642a171bdb4fb6ad4c33a83c4391908) - cleanup tests *(commit by [@tobymao](https://github.com/tobymao))*


## [v21.1.2] - 2024-02-19
### :sparkles: New Features
- [`b8cbf66`](https://github.com/tobymao/sqlglot/commit/b8cbf66471158371a27d9145b3b553b7a1384c9d) - **bigquery**: parse procedural EXCEPTION WHEN statement into a Command closes [#2981](https://github.com/tobymao/sqlglot/pull/2981) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`906ceee`](https://github.com/tobymao/sqlglot/commit/906ceee0946c547f83177916c3f8d6aeb23023a8) - **duckdb**: implement generation logic for exp.ArrayAny *(PR [#2984](https://github.com/tobymao/sqlglot/pull/2984) by [@georgesittas](https://github.com/georgesittas))*
- [`92455e4`](https://github.com/tobymao/sqlglot/commit/92455e4d4e2c8d5a874a5050d9a38f943479cdca) - **snowflake**: create storage integration  *(PR [#2985](https://github.com/tobymao/sqlglot/pull/2985) by [@tekumara](https://github.com/tekumara))*
- [`bedf6e9`](https://github.com/tobymao/sqlglot/commit/bedf6e9dabf9da25e1fff2f3c8ae22fbf7face0b) - improve transpilation support for ArrayAny *(PR [#2986](https://github.com/tobymao/sqlglot/pull/2986) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2707](https://github.com/tobymao/sqlglot/issues/2707) opened by [@HuashiSCNU0303](https://github.com/HuashiSCNU0303)*

### :bug: Bug Fixes
- [`cc67ab2`](https://github.com/tobymao/sqlglot/commit/cc67ab2513c71a6b9574f8c3cf4c8ba2927d798f) - **tsql**: map StrPosition back to CHARINDEX fixes [#2968](https://github.com/tobymao/sqlglot/pull/2968) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`46f15ce`](https://github.com/tobymao/sqlglot/commit/46f15cef87de3159bc1d422b2620278e9e27ec16) - **postgres**: ensure json extraction can roundtrip unaltered *(PR [#2974](https://github.com/tobymao/sqlglot/pull/2974) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2971](https://github.com/tobymao/sqlglot/issues/2971) opened by [@l-vincent-l](https://github.com/l-vincent-l)*
- [`7ee4fe7`](https://github.com/tobymao/sqlglot/commit/7ee4fe73b29234f2837a212b6c872efd7f5c30ea) - expand using with star except *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`5a34f3d`](https://github.com/tobymao/sqlglot/commit/5a34f3d5f652ac209fd122aa25e46d99d8e5cba6) - clean up tech debt in dialect implementations *(PR [#2977](https://github.com/tobymao/sqlglot/pull/2977) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`ae92789`](https://github.com/tobymao/sqlglot/commit/ae92789cdac4c4f0bb3d5f542bd9fe93aee4ea70) - rephrase some sentences in the FAQ section *(PR [#2980](https://github.com/tobymao/sqlglot/pull/2980) by [@georgesittas](https://github.com/georgesittas))*
- [`22ed4d0`](https://github.com/tobymao/sqlglot/commit/22ed4d0a976dbba15962670873422e86874680b0) - cleanup kv defs from brackets *(PR [#2987](https://github.com/tobymao/sqlglot/pull/2987) by [@tobymao](https://github.com/tobymao))*


## [v21.1.1] - 2024-02-14
### :sparkles: New Features
- [`1d0b3d3`](https://github.com/tobymao/sqlglot/commit/1d0b3d3a22ba5a8128505d636a2ff71d0ea03d03) - add support for multi-part interval addition syntax *(PR [#2970](https://github.com/tobymao/sqlglot/pull/2970) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2969](https://github.com/tobymao/sqlglot/issues/2969) opened by [@aersam](https://github.com/aersam)*

### :bug: Bug Fixes
- [`1c67f03`](https://github.com/tobymao/sqlglot/commit/1c67f030cd9df530e26c620079b2298b1db97d50) - **parser**: enable parsing of values into Identifier for some dialects *(PR [#2962](https://github.com/tobymao/sqlglot/pull/2962) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2957](https://github.com/tobymao/sqlglot/issues/2957) opened by [@hsheth2](https://github.com/hsheth2)*
- [`d8b0d4f`](https://github.com/tobymao/sqlglot/commit/d8b0d4fcc82662004056a68b05ca20f30996661f) - don't treat VALUES as a keyword in BigQuery, Redshift *(PR [#2965](https://github.com/tobymao/sqlglot/pull/2965) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2963](https://github.com/tobymao/sqlglot/issues/2963) opened by [@sean-rose](https://github.com/sean-rose)*
- [`5b7fd10`](https://github.com/tobymao/sqlglot/commit/5b7fd107f279c2f83c9d66d4353032c6d830202c) - **optimizer**: more optimizations for qualifying wide tables *(PR [#2972](https://github.com/tobymao/sqlglot/pull/2972) by [@barakalon](https://github.com/barakalon))*
- [`6cb985a`](https://github.com/tobymao/sqlglot/commit/6cb985ae1346c1a912ed6f81be30310ee1c91dfa) - pass dialect in to_table call inside replace_tables *(PR [#2973](https://github.com/tobymao/sqlglot/pull/2973) by [@georgesittas](https://github.com/georgesittas))*


## [v21.1.0] - 2024-02-12
### :sparkles: New Features
- [`e71d489`](https://github.com/tobymao/sqlglot/commit/e71d4899e6744812fdefc2704c66bbd6043b5bc9) - add array and tuple helpers *(commit by [@tobymao](https://github.com/tobymao))*
- [`876e075`](https://github.com/tobymao/sqlglot/commit/876e07580bb2de06b587fc8ad40eb67604ae8507) - **postgres**: root operator closes [#2940](https://github.com/tobymao/sqlglot/pull/2940) *(commit by [@tobymao](https://github.com/tobymao))*
- [`e731276`](https://github.com/tobymao/sqlglot/commit/e731276dd5490a7d294430e0887eebf19e16d28f) - **snowflake**: add support for SHOW USERS *(PR [#2948](https://github.com/tobymao/sqlglot/pull/2948) by [@DanCardin](https://github.com/DanCardin))*
- [`b9d4468`](https://github.com/tobymao/sqlglot/commit/b9d44688c2b785212db635f121b686df02e2dec9) - **tableau**: identifier and quotes closes [#2950](https://github.com/tobymao/sqlglot/pull/2950) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f8d9dbf`](https://github.com/tobymao/sqlglot/commit/f8d9dbf6744f95bf4b7517e8bcc35dd3a6f70c5d) - **sqlite**: add support for IIF *(PR [#2951](https://github.com/tobymao/sqlglot/pull/2951) by [@georgesittas](https://github.com/georgesittas))*
- [`b755551`](https://github.com/tobymao/sqlglot/commit/b7555516c6bf038dc39c4bba2b243839ceb6e3b5) - **clickhouse**: add basic support for system statement *(PR [#2953](https://github.com/tobymao/sqlglot/pull/2953) by [@GaliFFun](https://github.com/GaliFFun))*

### :bug: Bug Fixes
- [`844018b`](https://github.com/tobymao/sqlglot/commit/844018b8d3a3398d746fdc04c966c7e19d311998) - explode_outer to unnest closes [#2941](https://github.com/tobymao/sqlglot/pull/2941) *(commit by [@tobymao](https://github.com/tobymao))*
- [`159da45`](https://github.com/tobymao/sqlglot/commit/159da4523d6eb3ca6853d631bb98dc8f13c7b0fb) - posexplode_outer to unnest *(PR [#2942](https://github.com/tobymao/sqlglot/pull/2942) by [@chelsea-lin](https://github.com/chelsea-lin))*
- [`76d6634`](https://github.com/tobymao/sqlglot/commit/76d66340e566bd9fa8c783f5d311101eb2e80480) - **spark**: CREATE TABLE ... PARTITIONED BY fixes *(PR [#2937](https://github.com/tobymao/sqlglot/pull/2937) by [@barakalon](https://github.com/barakalon))*
- [`d07ddf9`](https://github.com/tobymao/sqlglot/commit/d07ddf9b460c1b6f672fda4f34dc9231419e6c9d) - **optimizer**: remove redundant casts *(PR [#2945](https://github.com/tobymao/sqlglot/pull/2945) by [@barakalon](https://github.com/barakalon))*
- [`b70a394`](https://github.com/tobymao/sqlglot/commit/b70a394222bf209298026fd100f6b9498acf9fff) - if doesn't support different types *(commit by [@tobymao](https://github.com/tobymao))*
- [`6a988e0`](https://github.com/tobymao/sqlglot/commit/6a988e0160022d33623cd036bf84bb0b222c9062) - **bigquery**: fix annotation of timestamp(x) *(PR [#2946](https://github.com/tobymao/sqlglot/pull/2946) by [@georgesittas](https://github.com/georgesittas))*
- [`78e6d0d`](https://github.com/tobymao/sqlglot/commit/78e6d0de83efbff1d3b61c8550db56c1819f7c22) - **optimizer**: qualify_columns optimizations for wide tables *(PR [#2955](https://github.com/tobymao/sqlglot/pull/2955) by [@barakalon](https://github.com/barakalon))*
- [`c20cc70`](https://github.com/tobymao/sqlglot/commit/c20cc70dfc7f6395af157521c7e99074d697beb4) - **redshift**: don't assume Table is an unnested Column if Join has a predicate *(PR [#2956](https://github.com/tobymao/sqlglot/pull/2956) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2952](https://github.com/tobymao/sqlglot/issues/2952) opened by [@vidit-wisdom](https://github.com/vidit-wisdom)*

### :wrench: Chores
- [`c4524ce`](https://github.com/tobymao/sqlglot/commit/c4524ce1e6a85e16db7ea0289116d0160732dc51) - fix unit test *(commit by [@tobymao](https://github.com/tobymao))*


## [v21.0.2] - 2024-02-08
### :sparkles: New Features
- [`1842c96`](https://github.com/tobymao/sqlglot/commit/1842c96611cadb0227dd3ce8f42457679ab0e08b) - **clickhouse**: add support for LIMIT BY clause *(PR [#2926](https://github.com/tobymao/sqlglot/pull/2926) by [@georgesittas](https://github.com/georgesittas))*
- [`9241858`](https://github.com/tobymao/sqlglot/commit/9241858e559f089b166d9b794e3ebb395624d84a) - add typing for explode closes [#2927](https://github.com/tobymao/sqlglot/pull/2927) *(commit by [@tobymao](https://github.com/tobymao))*
- [`85073d1`](https://github.com/tobymao/sqlglot/commit/85073d1538de8ceef3e5c622a901efd9e6bd38e3) - transpile multi-arg DISTINCT expression *(PR [#2936](https://github.com/tobymao/sqlglot/pull/2936) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2930](https://github.com/tobymao/sqlglot/issues/2930) opened by [@Jake-00](https://github.com/Jake-00)*

### :bug: Bug Fixes
- [`b827626`](https://github.com/tobymao/sqlglot/commit/b8276262bdca57e358284fadfdd468d2bc957e84) - remove find method from Schema *(PR [#2934](https://github.com/tobymao/sqlglot/pull/2934) by [@georgesittas](https://github.com/georgesittas))*
- [`08cd117`](https://github.com/tobymao/sqlglot/commit/08cd117322302f08c95889ebf8699f4171c1d504) - **postgres**: fallback to parameter parser if heredoc is untokenizable *(PR [#2935](https://github.com/tobymao/sqlglot/pull/2935) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2931](https://github.com/tobymao/sqlglot/issues/2931) opened by [@eric-zhu](https://github.com/eric-zhu)*

### :wrench: Chores
- [`e4b5edb`](https://github.com/tobymao/sqlglot/commit/e4b5edbef42944b44d11c35aea31411ce3d79826) - bump sqlglotrs to 0.1.1 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v21.0.1] - 2024-02-07
### :sparkles: New Features
- [`3a20eac`](https://github.com/tobymao/sqlglot/commit/3a20eaccbf5d5a80bd24b95c837cca8103dfe70a) - **clickhouse**: add support for JSONExtractString, clean up some helpers *(PR [#2925](https://github.com/tobymao/sqlglot/pull/2925) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2051](https://github.com/tobymao/sqlglot/issues/2051) opened by [@BTheunissen](https://github.com/BTheunissen)*


## [v21.0.0] - 2024-02-07
### :boom: BREAKING CHANGES
- due to [`b4e8868`](https://github.com/tobymao/sqlglot/commit/b4e886877ecfbafdd64c515c765c3c54764bd987) - improve transpilation of JSON paths across dialects *(PR [#2883](https://github.com/tobymao/sqlglot/pull/2883) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilation of JSON paths across dialects (#2883)

- due to [`aa388ea`](https://github.com/tobymao/sqlglot/commit/aa388ea64404a26550dbb0734f4d3e35111f9e2c) - ignore nulls closes [#2896](https://github.com/tobymao/sqlglot/pull/2896) *(PR [#2898](https://github.com/tobymao/sqlglot/pull/2898) by [@tobymao](https://github.com/tobymao))*:

  ignore nulls closes #2896 (#2898)

- due to [`617a8c0`](https://github.com/tobymao/sqlglot/commit/617a8c0dfc5e9f2716f7827381af0db2e135059e) - timestamp diff for mysql and databricks *(commit by [@tobymao](https://github.com/tobymao))*:

  timestamp diff for mysql and databricks

- due to [`b00b393`](https://github.com/tobymao/sqlglot/commit/b00b393d853ae05a3fce4ef78d7673edbcabf67d) - use raise instead of assert for assert_is *(commit by [@tobymao](https://github.com/tobymao))*:

  use raise instead of assert for assert_is

- due to [`326aa31`](https://github.com/tobymao/sqlglot/commit/326aa31e32e511f4e40d3a5a7b1d599b5e2c1307) - deprecate case where transforms can be plain strs *(PR [#2919](https://github.com/tobymao/sqlglot/pull/2919) by [@georgesittas](https://github.com/georgesittas))*:

  deprecate case where transforms can be plain strs (#2919)


### :sparkles: New Features
- [`fb450f0`](https://github.com/tobymao/sqlglot/commit/fb450f0263ecd6b7c9d0f49d84441327d50b9d83) - add tsql right left auto casting closes [#2899](https://github.com/tobymao/sqlglot/pull/2899) *(commit by [@tobymao](https://github.com/tobymao))*
- [`617a8c0`](https://github.com/tobymao/sqlglot/commit/617a8c0dfc5e9f2716f7827381af0db2e135059e) - timestamp diff for mysql and databricks *(commit by [@tobymao](https://github.com/tobymao))*
- [`3fa92ca`](https://github.com/tobymao/sqlglot/commit/3fa92cac285cbb2bd9d8b5724dadb77be7e12731) - **redshift**: parse GETDATE *(PR [#2904](https://github.com/tobymao/sqlglot/pull/2904) by [@erickpeirson](https://github.com/erickpeirson))*
- [`d262139`](https://github.com/tobymao/sqlglot/commit/d26213998b27fa9b6a66b6d21ab5a3a15f65635e) - **snowflake**: implement parsing logic for SHOW TABLES *(PR [#2913](https://github.com/tobymao/sqlglot/pull/2913) by [@tekumara](https://github.com/tekumara))*
- [`838e780`](https://github.com/tobymao/sqlglot/commit/838e7800c32ad16074efef6a188ebd89083a9717) - improve transpilation of CREATE TABLE LIKE statement *(PR [#2923](https://github.com/tobymao/sqlglot/pull/2923) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2922](https://github.com/tobymao/sqlglot/issues/2922) opened by [@tharwan](https://github.com/tharwan)*
- [`cbbad1f`](https://github.com/tobymao/sqlglot/commit/cbbad1fc40b6b2ca837ddb0f798b1802ad4063da) - improve transpilation of JSON path wildcards *(PR [#2924](https://github.com/tobymao/sqlglot/pull/2924) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`aa388ea`](https://github.com/tobymao/sqlglot/commit/aa388ea64404a26550dbb0734f4d3e35111f9e2c) - ignore nulls closes [#2896](https://github.com/tobymao/sqlglot/pull/2896) *(PR [#2898](https://github.com/tobymao/sqlglot/pull/2898) by [@tobymao](https://github.com/tobymao))*
- [`b00b393`](https://github.com/tobymao/sqlglot/commit/b00b393d853ae05a3fce4ef78d7673edbcabf67d) - use raise instead of assert for assert_is *(commit by [@tobymao](https://github.com/tobymao))*
- [`ab97246`](https://github.com/tobymao/sqlglot/commit/ab972462b1c545b4a60bb88cb40cdb98cb64e360) - array overlaps closes [#2903](https://github.com/tobymao/sqlglot/pull/2903) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f3bdcb0`](https://github.com/tobymao/sqlglot/commit/f3bdcb087bb993289c4a1a5d2de54155ae2d2681) - **duckdb**: fix JSON pointer path parsing, reduce warning noise *(PR [#2911](https://github.com/tobymao/sqlglot/pull/2911) by [@georgesittas](https://github.com/georgesittas))*
- [`072264f`](https://github.com/tobymao/sqlglot/commit/072264f8af25737050f7becd27af5a9331bde896) - **mysql**: SHOW SCHEMAS *(PR [#2916](https://github.com/tobymao/sqlglot/pull/2916) by [@barakalon](https://github.com/barakalon))*
- [`15fdff2`](https://github.com/tobymao/sqlglot/commit/15fdff2df3363ab8d3595e7eeb8baee65e525733) - **optimizer**: don't remove NOT parenthesis *(PR [#2917](https://github.com/tobymao/sqlglot/pull/2917) by [@barakalon](https://github.com/barakalon))*
- [`d20d826`](https://github.com/tobymao/sqlglot/commit/d20d826e9cc4a9b0d636a9b56b5547cd906a5903) - have table exclude this if schema target *(PR [#2921](https://github.com/tobymao/sqlglot/pull/2921) by [@eakmanrq](https://github.com/eakmanrq))*

### :recycle: Refactors
- [`b4e8868`](https://github.com/tobymao/sqlglot/commit/b4e886877ecfbafdd64c515c765c3c54764bd987) - improve transpilation of JSON paths across dialects *(PR [#2883](https://github.com/tobymao/sqlglot/pull/2883) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2839](https://github.com/tobymao/sqlglot/issues/2839) opened by [@FlaviovLeal](https://github.com/FlaviovLeal)*
- [`9481f94`](https://github.com/tobymao/sqlglot/commit/9481f946b068e43d99c9aaae6e1c59abf384eeac) - several JSON path improvements *(PR [#2914](https://github.com/tobymao/sqlglot/pull/2914) by [@georgesittas](https://github.com/georgesittas))*
- [`326aa31`](https://github.com/tobymao/sqlglot/commit/326aa31e32e511f4e40d3a5a7b1d599b5e2c1307) - deprecate case where transforms can be plain strs *(PR [#2919](https://github.com/tobymao/sqlglot/pull/2919) by [@georgesittas](https://github.com/georgesittas))*
- [`15582f4`](https://github.com/tobymao/sqlglot/commit/15582f40bd18da3fa7adbe454b401ef8d31a131e) - move JSON path generation logic in Generator *(PR [#2920](https://github.com/tobymao/sqlglot/pull/2920) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`afb4f9b`](https://github.com/tobymao/sqlglot/commit/afb4f9bfe074200e60b5a870267fe21aa04a87c5) - switch to ruff *(commit by [@tobymao](https://github.com/tobymao))*
- [`f9fdf7b`](https://github.com/tobymao/sqlglot/commit/f9fdf7b3bb25aa7e830b70600728bb35ee1e4ff7) - switch to ruff *(PR [#2912](https://github.com/tobymao/sqlglot/pull/2912) by [@tobymao](https://github.com/tobymao))*
- [`71c33fa`](https://github.com/tobymao/sqlglot/commit/71c33fa13b9c416ae50acb10a9b08dcfcfd35f92) - pandas warning *(commit by [@tobymao](https://github.com/tobymao))*


## [v20.11.0] - 2024-01-29
### :boom: BREAKING CHANGES
- due to [`eb8b40a`](https://github.com/tobymao/sqlglot/commit/eb8b40aade54eec8b34a808dda95420dcf7a7e13) - deprecate NULL, TRUE, FALSE constant expressions *(PR [#2884](https://github.com/tobymao/sqlglot/pull/2884) by [@georgesittas](https://github.com/georgesittas))*:

  deprecate NULL, TRUE, FALSE constant expressions (#2884)


### :sparkles: New Features
- [`3a8ed85`](https://github.com/tobymao/sqlglot/commit/3a8ed8573d5562110b312586ae6fca22038e5d05) - add alter table alter comment closes [#2889](https://github.com/tobymao/sqlglot/pull/2889) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`dc2d7d7`](https://github.com/tobymao/sqlglot/commit/dc2d7d7dd4253fe6b247d534bd92327f186e9aa8) - **tsql**: len text transpilation closes [#2885](https://github.com/tobymao/sqlglot/pull/2885) *(commit by [@tobymao](https://github.com/tobymao))*
- [`ad50f47`](https://github.com/tobymao/sqlglot/commit/ad50f479c47d5b4990f1b41272c69079a453cf21) - type imports *(PR [#2886](https://github.com/tobymao/sqlglot/pull/2886) by [@tobymao](https://github.com/tobymao))*
- [`e4fb7f6`](https://github.com/tobymao/sqlglot/commit/e4fb7f6e1b8ab15ceb5acc6a93256c849c738740) - union should return union *(commit by [@tobymao](https://github.com/tobymao))*
- [`8f795ea`](https://github.com/tobymao/sqlglot/commit/8f795ea00164b69acba093c3684ab54b62138e8e) - don't expand star except/replace refs *(commit by [@tobymao](https://github.com/tobymao))*
- [`218121c`](https://github.com/tobymao/sqlglot/commit/218121c274656a1b252143a7d0fc2d73407115ca) - alter table cluster by closes [#2887](https://github.com/tobymao/sqlglot/pull/2887) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5cec283`](https://github.com/tobymao/sqlglot/commit/5cec2839f8ed8477821bf766025f4b5de0621fe2) - bigquery script if statement closes [#2888](https://github.com/tobymao/sqlglot/pull/2888) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5fc7791`](https://github.com/tobymao/sqlglot/commit/5fc7791a4d19d704c0d4fafe8924cf8f76fcb867) - all view column options without types closes [#2891](https://github.com/tobymao/sqlglot/pull/2891) *(commit by [@tobymao](https://github.com/tobymao))*
- [`102304e`](https://github.com/tobymao/sqlglot/commit/102304e28f2ed7126840789837ed797a75bae44e) - **postgres**: generate CurrentUser without parentheses closes [#2893](https://github.com/tobymao/sqlglot/pull/2893) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`43902db`](https://github.com/tobymao/sqlglot/commit/43902db25706a2434fe7e9ba39addd1c31c2aa64) - error level ignore comments closes [#2895](https://github.com/tobymao/sqlglot/pull/2895) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`eb8b40a`](https://github.com/tobymao/sqlglot/commit/eb8b40aade54eec8b34a808dda95420dcf7a7e13) - deprecate NULL, TRUE, FALSE constant expressions *(PR [#2884](https://github.com/tobymao/sqlglot/pull/2884) by [@georgesittas](https://github.com/georgesittas))*
- [`29cddd5`](https://github.com/tobymao/sqlglot/commit/29cddd5c3f5401033197d47e7544cedd91b8046c) - change warning message *(commit by [@tobymao](https://github.com/tobymao))*
- [`9eac93e`](https://github.com/tobymao/sqlglot/commit/9eac93e0acd5ae8b034045759fc48937586cbc2e) - upgrade black *(commit by [@tobymao](https://github.com/tobymao))*
- [`4f3fac7`](https://github.com/tobymao/sqlglot/commit/4f3fac7815e0d8206c80f1f255336ab630503d4d) - cleanup command parsing and warnings *(commit by [@tobymao](https://github.com/tobymao))*


## [v20.10.0] - 2024-01-24
### :boom: BREAKING CHANGES
- due to [`1f5fc39`](https://github.com/tobymao/sqlglot/commit/1f5fc39c10b92b94bd94afa5fd038fdb9afeb4b4) - jsonpath parsing *(PR [#2867](https://github.com/tobymao/sqlglot/pull/2867) by [@tobymao](https://github.com/tobymao))*:

  jsonpath parsing (#2867)


### :sparkles: New Features
- [`89b439e`](https://github.com/tobymao/sqlglot/commit/89b439e2f93b6c3bedb4e58fe4b5014d42dd5080) - **postgres**: support the INCLUDE clause in INDEX creation *(PR [#2857](https://github.com/tobymao/sqlglot/pull/2857) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2855](undefined) opened by [@dezhin](https://github.com/dezhin)*
- [`90ffff8`](https://github.com/tobymao/sqlglot/commit/90ffff83266b5714b1371a576d9484dfbe4be155) - **clickhouse**: AggregateFunction data type *(PR [#2832](https://github.com/tobymao/sqlglot/pull/2832) by [@pkit](https://github.com/pkit))*
- [`326d3ae`](https://github.com/tobymao/sqlglot/commit/326d3ae7113cca4a67cca3ab3335f7b8dde91f71) - improve transpilation of Spark's TO_UTC_TIMESTAMP *(PR [#2861](https://github.com/tobymao/sqlglot/pull/2861) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2846](undefined) opened by [@hdemers](https://github.com/hdemers)*
- [`6d03587`](https://github.com/tobymao/sqlglot/commit/6d035871feee2b206d73e37b5f9b8c99fc40708f) - **snowflake**: SHOW SCHEMAS/OBJECTS *(PR [#2845](https://github.com/tobymao/sqlglot/pull/2845) by [@tekumara](https://github.com/tekumara))*
  - :arrow_lower_right: *addresses issue [#2784](undefined) opened by [@tekumara](https://github.com/tekumara)*
- [`d5a08b8`](https://github.com/tobymao/sqlglot/commit/d5a08b8f3a1cdd87e9560c0fe4f11b0f1586978b) - **optimizer**: improve struct type annotation support for EQ-delimited kv pairs *(PR [#2863](https://github.com/tobymao/sqlglot/pull/2863) by [@fool1280](https://github.com/fool1280))*
- [`1f5fc39`](https://github.com/tobymao/sqlglot/commit/1f5fc39c10b92b94bd94afa5fd038fdb9afeb4b4) - jsonpath parsing *(PR [#2867](https://github.com/tobymao/sqlglot/pull/2867) by [@tobymao](https://github.com/tobymao))*
- [`7fd9045`](https://github.com/tobymao/sqlglot/commit/7fd9045488beb88b2726ae906b8769b7963d1b37) - add support for rename column *(PR [#2866](https://github.com/tobymao/sqlglot/pull/2866) by [@gableh](https://github.com/gableh))*
- [`89b781b`](https://github.com/tobymao/sqlglot/commit/89b781b991ce264cd7f8c44fa67860eb9a587b07) - **postgres**: add support for the INHERITS property closes [#2871](https://github.com/tobymao/sqlglot/pull/2871) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`970c202`](https://github.com/tobymao/sqlglot/commit/970c2022a27d4fc355fedb7af830367a5dd96009) - **postgres**: add support for the SET property *(PR [#2873](https://github.com/tobymao/sqlglot/pull/2873) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2872](undefined) opened by [@edongashi](https://github.com/edongashi)*
- [`6845c37`](https://github.com/tobymao/sqlglot/commit/6845c37e749448972a231926236c08affb71a64f) - make the CREATE parser more lenient *(PR [#2875](https://github.com/tobymao/sqlglot/pull/2875) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`bf03a45`](https://github.com/tobymao/sqlglot/commit/bf03a45d8df9abd63b8102e431c13ca0eb0b0fb0) - **snowflake**: extend _parse_range to gracefully handle colon operator *(PR [#2856](https://github.com/tobymao/sqlglot/pull/2856) by [@georgesittas](https://github.com/georgesittas))*
- [`ad14f4e`](https://github.com/tobymao/sqlglot/commit/ad14f4ed1ed3870ae0d7370643c67c235fc89b4b) - qualify alter table table refs in optimizer qualify *(PR [#2862](https://github.com/tobymao/sqlglot/pull/2862) by [@z3z1ma](https://github.com/z3z1ma))*
- [`8599903`](https://github.com/tobymao/sqlglot/commit/859990356210f3091b1b66647c1a674fdb0f2ad9) - **optimizer**: compute external columns for union sopes correctly *(PR [#2864](https://github.com/tobymao/sqlglot/pull/2864) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2860](undefined) opened by [@derekpaulsen](https://github.com/derekpaulsen)*
- [`3e065f9`](https://github.com/tobymao/sqlglot/commit/3e065f9503607e6c620fc187e2bdc2d45f7fa1dd) - **optimizer**: don't copy projection in qualify_outputs when attaching alias *(PR [#2868](https://github.com/tobymao/sqlglot/pull/2868) by [@georgesittas](https://github.com/georgesittas))*
- [`a642758`](https://github.com/tobymao/sqlglot/commit/a6427585e16fa4b5adc9e01cc22baeb09b2f69bb) - avoid dag cycle with unnesting subqueries closes [#2876](https://github.com/tobymao/sqlglot/pull/2876) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0648453`](https://github.com/tobymao/sqlglot/commit/06484532f0e222004e844501a192b8d4aec654c7) - set div type on multiplication closes [#2878](https://github.com/tobymao/sqlglot/pull/2878) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b7fb800`](https://github.com/tobymao/sqlglot/commit/b7fb8006b825ec36d62135877b4e32fccad8d044) - **oracle**: generate with time zone for timestamptz fixes [#2879](https://github.com/tobymao/sqlglot/pull/2879) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`8770e3b`](https://github.com/tobymao/sqlglot/commit/8770e3b7855110a82cb3bc05f3cb6c36a88cfdb2) - **optimizer**: don't qualify CTEs for DDL/DML statements *(PR [#2880](https://github.com/tobymao/sqlglot/pull/2880) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2877](undefined) opened by [@hsheth2](https://github.com/hsheth2)*

### :wrench: Chores
- [`d55cfba`](https://github.com/tobymao/sqlglot/commit/d55cfba7226a61cc8419b3a83eaa0a2ead23be10) - move postgres tests *(commit by [@tobymao](https://github.com/tobymao))*
- [`0e43c58`](https://github.com/tobymao/sqlglot/commit/0e43c58a38bb6af63f653062e79626b85f605b63) - **parser**: warn when parsing (>1 tokens) SQL into exp.Command *(PR [#2874](https://github.com/tobymao/sqlglot/pull/2874) by [@georgesittas](https://github.com/georgesittas))*


## [v20.9.0] - 2024-01-18
### :boom: BREAKING CHANGES
- due to [`1be93e4`](https://github.com/tobymao/sqlglot/commit/1be93e45d8347e5fa8a4e39dad625c6dd66ea461) - properly support all unix time scales *(commit by [@tobymao](https://github.com/tobymao))*:

  properly support all unix time scales


### :sparkles: New Features
- [`816976f`](https://github.com/tobymao/sqlglot/commit/816976f52865fb8ade580c727a890a90378c8e50) - extend submodule annotate_types to handle STRUCT *(PR [#2783](https://github.com/tobymao/sqlglot/pull/2783) by [@fool1280](https://github.com/fool1280))*
- [`7bce2f6`](https://github.com/tobymao/sqlglot/commit/7bce2f6abe79dfd8064c625294d94364042207c5) - **oracle**: add support for ORDER SIBLINGS BY clause *(PR [#2821](https://github.com/tobymao/sqlglot/pull/2821) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2819](undefined) opened by [@Shweta-BI-Lead](https://github.com/Shweta-BI-Lead)*
- [`ce8d254`](https://github.com/tobymao/sqlglot/commit/ce8d254305f56724982eed8e099ab1abeb8750a1) - **snowflake**: parse RM/REMOVE as commands *(PR [#2825](https://github.com/tobymao/sqlglot/pull/2825) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2824](undefined) opened by [@sfc-gh-jlambert](https://github.com/sfc-gh-jlambert)*
- [`1902778`](https://github.com/tobymao/sqlglot/commit/19027786facf8ff730af49c1693149e244502cb0) - add support for multi-unit intervals *(PR [#2822](https://github.com/tobymao/sqlglot/pull/2822) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2793](undefined) opened by [@nikmalviya](https://github.com/nikmalviya)*
- [`a537898`](https://github.com/tobymao/sqlglot/commit/a53789840b56be747fa5d670a9d5ea120aee371a) - add support for DESCRIBE EXTENDED *(PR [#2828](https://github.com/tobymao/sqlglot/pull/2828) by [@georgesittas](https://github.com/georgesittas))*
- [`6e50759`](https://github.com/tobymao/sqlglot/commit/6e50759fb19c8d00825f626fb8c1ab6792fabd56) - **clickhouse**: support Date32 type *(PR [#2830](https://github.com/tobymao/sqlglot/pull/2830) by [@pkit](https://github.com/pkit))*
- [`9560e8f`](https://github.com/tobymao/sqlglot/commit/9560e8fa93d6ac7f4f015bd55091d2fe75e85508) - add support for Heredocs in Databricks Python UDFs *(PR [#2801](https://github.com/tobymao/sqlglot/pull/2801) by [@viethungle-vt1401](https://github.com/viethungle-vt1401))*
- [`52ed590`](https://github.com/tobymao/sqlglot/commit/52ed590b0fa75ef8a9f6e4cb2fb48b4fff65996f) - transpile SELECT .. INTO to dialects that do not support it *(PR [#2820](https://github.com/tobymao/sqlglot/pull/2820) by [@giorgosnikolaou](https://github.com/giorgosnikolaou))*
- [`ea536c4`](https://github.com/tobymao/sqlglot/commit/ea536c4bd7bae0b2916d4bdf9a0ae6a7c5106135) - remove target alias in trino merge *(PR [#2852](https://github.com/tobymao/sqlglot/pull/2852) by [@eakmanrq](https://github.com/eakmanrq))*

### :bug: Bug Fixes
- [`6ddbefc`](https://github.com/tobymao/sqlglot/commit/6ddbefcb08ef933454ff8501ac4a3ea4cba2fe60) - **snowflake**: apply range parser after colon, if any *(PR [#2800](https://github.com/tobymao/sqlglot/pull/2800) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2798](undefined) opened by [@mbarugelCA](https://github.com/mbarugelCA)*
- [`0c4f44e`](https://github.com/tobymao/sqlglot/commit/0c4f44e8027b28613c72285313493c4683c65275) - **oracle**: regexp_replace replacement is optional closes [#2803](https://github.com/tobymao/sqlglot/pull/2803) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5072d5a`](https://github.com/tobymao/sqlglot/commit/5072d5af9a9f629e857071a66228317afd89b1a6) - **oracle**: improve parsing of JSON_OBJECT[AGG] functions *(PR [#2807](https://github.com/tobymao/sqlglot/pull/2807) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2806](undefined) opened by [@Shweta-BI-Lead](https://github.com/Shweta-BI-Lead)*
- [`ea39f10`](https://github.com/tobymao/sqlglot/commit/ea39f10150916f2624cb6efcefb6752154c2f88c) - **optimizer**: pushdown predicates more conservatively to avoid DAG cycles *(PR [#2808](https://github.com/tobymao/sqlglot/pull/2808) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2797](undefined) opened by [@Konntroll](https://github.com/Konntroll)*
- [`ea58003`](https://github.com/tobymao/sqlglot/commit/ea58003caeed17861085825c19a1a5823e065691) - **snowflake**: insert overwrite into closes [#2815](https://github.com/tobymao/sqlglot/pull/2815) *(commit by [@tobymao](https://github.com/tobymao))*
- [`d5fa5be`](https://github.com/tobymao/sqlglot/commit/d5fa5be010a2656ead5524a0d756da6e25ab31dc) - **optimizer**: handle table alias columns for (UN)PIVOTs *(PR [#2816](https://github.com/tobymao/sqlglot/pull/2816) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2791](undefined) opened by [@billstark](https://github.com/billstark)*
- [`24177f3`](https://github.com/tobymao/sqlglot/commit/24177f39b448490618d3b04f8b8ad75ec2069fd3) - unnest star expansion closes [#2811](https://github.com/tobymao/sqlglot/pull/2811) *(PR [#2818](https://github.com/tobymao/sqlglot/pull/2818) by [@tobymao](https://github.com/tobymao))*
- [`3dba6c1`](https://github.com/tobymao/sqlglot/commit/3dba6c194e2fff90f93f0370255a94a1b2a2365a) - add parentheses to no_safe_divide args *(PR [#2826](https://github.com/tobymao/sqlglot/pull/2826) by [@j1ah0ng](https://github.com/j1ah0ng))*
- [`ed68d6b`](https://github.com/tobymao/sqlglot/commit/ed68d6be51e8054eaf8b7bf64048f20c999c6cd2) - **clickhouse**: add ipv4/6 data type parser *(PR [#2829](https://github.com/tobymao/sqlglot/pull/2829) by [@pkit](https://github.com/pkit))*
- [`70b280f`](https://github.com/tobymao/sqlglot/commit/70b280f24df0f4a0d4cc8262d72ce46412f76be3) - allow insert columns to have spaces *(commit by [@tobymao](https://github.com/tobymao))*
- [`57917b8`](https://github.com/tobymao/sqlglot/commit/57917b88ddd9d7d85bc76fc8cb46ffcbf228453d) - **oracle**: TO_TIMESTAMP not parsed as StrToTime *(PR [#2833](https://github.com/tobymao/sqlglot/pull/2833) by [@pkit](https://github.com/pkit))*
  - :arrow_lower_right: *fixes issue [#2831](undefined) opened by [@Rhiyo](https://github.com/Rhiyo)*
- [`9960e11`](https://github.com/tobymao/sqlglot/commit/9960e114818640f26aaa6d911ad3e7ee53df1842) - **optimizer**: annotate struct value without alias correctly *(PR [#2812](https://github.com/tobymao/sqlglot/pull/2812) by [@fool1280](https://github.com/fool1280))*
- [`a6d396b`](https://github.com/tobymao/sqlglot/commit/a6d396b79cfa199a66b04af9ed62bcd7cd619096) - **doris**: add transformation of aggregation function and last_day function *(PR [#2835](https://github.com/tobymao/sqlglot/pull/2835) by [@echo-hhj](https://github.com/echo-hhj))*
- [`8cc252b`](https://github.com/tobymao/sqlglot/commit/8cc252b61b418004bbd6380a8447cb383cf51282) - interval without unit alias closes [#2838](https://github.com/tobymao/sqlglot/pull/2838) *(commit by [@tobymao](https://github.com/tobymao))*
- [`607817f`](https://github.com/tobymao/sqlglot/commit/607817f7e43edefe0a077bfeb81a77dd78e170e5) - schema with period name closes [#2842](https://github.com/tobymao/sqlglot/pull/2842) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1be93e4`](https://github.com/tobymao/sqlglot/commit/1be93e45d8347e5fa8a4e39dad625c6dd66ea461) - properly support all unix time scales *(commit by [@tobymao](https://github.com/tobymao))*
- [`a657fc0`](https://github.com/tobymao/sqlglot/commit/a657fc0ea21aff7452f292fecfcb4bc08ca2e4e9) - **clickhouse,doris**: fix the transformation of ArraySum *(PR [#2843](https://github.com/tobymao/sqlglot/pull/2843) by [@echo-hhj](https://github.com/echo-hhj))*
- [`c92888c`](https://github.com/tobymao/sqlglot/commit/c92888c6b49d2ba60ce789281535679fd93cd235) - **parser**: fix order of query modifier parsing for nested subqueries *(PR [#2851](https://github.com/tobymao/sqlglot/pull/2851) by [@georgesittas](https://github.com/georgesittas))*
- [`7949a4f`](https://github.com/tobymao/sqlglot/commit/7949a4f295fc0a9f0becca9b6460f8517ec733f1) - **clickhouse**: ensure arraySum generation is preserved, add tests *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`17a6ac6`](https://github.com/tobymao/sqlglot/commit/17a6ac6f5fb96a42668842b093a823662b5850b8) - move comment in expr as alias next to the alias *(PR [#2853](https://github.com/tobymao/sqlglot/pull/2853) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`9e5ae50`](https://github.com/tobymao/sqlglot/commit/9e5ae50e02879cfb4915584df90f8dcfadbca321) - use flag instead of regex *(commit by [@tobymao](https://github.com/tobymao))*
- [`5c13a1e`](https://github.com/tobymao/sqlglot/commit/5c13a1e8e2ede284d12920734cea8ff82ebaf054) - simplify merge without target transformation *(PR [#2854](https://github.com/tobymao/sqlglot/pull/2854) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`5996a69`](https://github.com/tobymao/sqlglot/commit/5996a6949979dcfceee133f943a010ec4820e808) - **presto**: get rid of assert in ELEMENT_AT parser *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`f9a02ec`](https://github.com/tobymao/sqlglot/commit/f9a02ecc44f4d0139aae5edda922cc95d4a3e892) - fix table, column instantiation in schema normalization tests *(PR [#2817](https://github.com/tobymao/sqlglot/pull/2817) by [@georgesittas](https://github.com/georgesittas))*


## [v20.8.0] - 2024-01-08
### :boom: BREAKING CHANGES
- due to [`68e1214`](https://github.com/tobymao/sqlglot/commit/68e121462b2c3dc388f3ae0d1d392ee8afc63133) - column field typing *(commit by [@tobymao](https://github.com/tobymao))*:

  column field typing


### :sparkles: New Features
- [`2d822f3`](https://github.com/tobymao/sqlglot/commit/2d822f3972bf0f77baaadb135a5e19c1bc0c4040) - improve support for Doris' TO_DATE, Oracle's SYSDATE *(PR [#2775](https://github.com/tobymao/sqlglot/pull/2775) by [@georgesittas](https://github.com/georgesittas))*
- [`7187215`](https://github.com/tobymao/sqlglot/commit/71872159114b85324d08191b854ab8462a298742) - desc builder *(commit by [@tobymao](https://github.com/tobymao))*
- [`ba62639`](https://github.com/tobymao/sqlglot/commit/ba62639aa6d81a062c867ebe20af64446b931b7d) - add support for CREATE FUNCTION (SQL) characteristics for MySQL and Databricks *(PR [#2777](https://github.com/tobymao/sqlglot/pull/2777) by [@viethungle-vt1401](https://github.com/viethungle-vt1401))*
  - :arrow_lower_right: *addresses issue [#1980](undefined) opened by [@xinglin-zhao](https://github.com/xinglin-zhao)*
- [`963e2dc`](https://github.com/tobymao/sqlglot/commit/963e2dc9a4b699938d0477bc379e9d2da01818af) - **snowflake**: add support for SHOW COLUMNS  *(PR [#2778](https://github.com/tobymao/sqlglot/pull/2778) by [@andrew-sha](https://github.com/andrew-sha))*
- [`46c9733`](https://github.com/tobymao/sqlglot/commit/46c973309850d4e32b1a0f0594d7b143eb14d059) - **tsql**: round func closes [#2790](https://github.com/tobymao/sqlglot/pull/2790) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2dfb7e8`](https://github.com/tobymao/sqlglot/commit/2dfb7e806c52715d9e83d2201ed63974ff238ad3) - **optimizer**: add support for the UNPIVOT operator *(PR [#2771](https://github.com/tobymao/sqlglot/pull/2771) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`68e1214`](https://github.com/tobymao/sqlglot/commit/68e121462b2c3dc388f3ae0d1d392ee8afc63133) - column field typing *(commit by [@tobymao](https://github.com/tobymao))*
- [`3f31706`](https://github.com/tobymao/sqlglot/commit/3f31706b913e53d13e45fe94b41ee115cc7bd5c5) - tsql exec command [#2772](https://github.com/tobymao/sqlglot/pull/2772) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f6cbadb`](https://github.com/tobymao/sqlglot/commit/f6cbadb7a293035720460f869dd1a6d48a707d04) - **snowflake**: add a couple of special fn types *(PR [#2774](https://github.com/tobymao/sqlglot/pull/2774) by [@georgesittas](https://github.com/georgesittas))*
- [`0634f73`](https://github.com/tobymao/sqlglot/commit/0634f738d3a935cde3e7df1671c65e666c7a52b4) - **optimizer**: replace star with outer column list *(PR [#2776](https://github.com/tobymao/sqlglot/pull/2776) by [@georgesittas](https://github.com/georgesittas))*
- [`d31ae0d`](https://github.com/tobymao/sqlglot/commit/d31ae0decb46678851744356c7b113f8c1c3e8c9) - allow string aliases closes [#2788](https://github.com/tobymao/sqlglot/pull/2788) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8f8f00e`](https://github.com/tobymao/sqlglot/commit/8f8f00ec66beb6dc3d90898ead29828eee8f5e32) - don't transform null ordering with positional orders closes [#2779](https://github.com/tobymao/sqlglot/pull/2779) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f85ce3b`](https://github.com/tobymao/sqlglot/commit/f85ce3b354366b2e206e6d2815f34a8e345d10ba) - **tsql**: gracefully handle complex formats in FORMAT *(PR [#2794](https://github.com/tobymao/sqlglot/pull/2794) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2787](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`a2499f5`](https://github.com/tobymao/sqlglot/commit/a2499f591eeb7538db86abd8cc9341c8d91e325d) - **tsql**: generate correct TRIM syntax closes [#2786](https://github.com/tobymao/sqlglot/pull/2786) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`59ecd2f`](https://github.com/tobymao/sqlglot/commit/59ecd2f17cac61b1ed7d206437d2fab4497e58fa) - **clickhouse**: allow transpilation of countIf, fix 2 arg variant parsing *(PR [#2795](https://github.com/tobymao/sqlglot/pull/2795) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2792](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`3f7748b`](https://github.com/tobymao/sqlglot/commit/3f7748b08d3f6616d8ea7eac6ded4980b6507ddf) - don't transpile nulls last in window specs *(commit by [@tobymao](https://github.com/tobymao))*


## [v20.7.1] - 2024-01-04
### :bug: Bug Fixes
- [`9d94b9c`](https://github.com/tobymao/sqlglot/commit/9d94b9c77f5fbd22ad147789f23e971ec6bb3c72) - don't normalize schema if normalize is set to false *(PR [#2767](https://github.com/tobymao/sqlglot/pull/2767) by [@tobymao](https://github.com/tobymao))*


## [v20.6.0] - 2024-01-04
### :boom: BREAKING CHANGES
- due to [`4648c6a`](https://github.com/tobymao/sqlglot/commit/4648c6acbeab8d5155be3f8e53d11d8f00c33a2e) - set sample clause keyword(s) as class constant to enable transpilation *(PR [#2750](https://github.com/tobymao/sqlglot/pull/2750) by [@georgesittas](https://github.com/georgesittas))*:

  set sample clause keyword(s) as class constant to enable transpilation (#2750)

- due to [`0b6bdc4`](https://github.com/tobymao/sqlglot/commit/0b6bdc4513aa417dae0a00565b56e78b544306e7) - improve transpilation of JSON value extraction *(PR [#2744](https://github.com/tobymao/sqlglot/pull/2744) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilation of JSON value extraction (#2744)

- due to [`862b305`](https://github.com/tobymao/sqlglot/commit/862b305db47c0cea4a66b61211a3ed088e935ea5) - improve table sampling transpilation *(PR [#2761](https://github.com/tobymao/sqlglot/pull/2761) by [@georgesittas](https://github.com/georgesittas))*:

  improve table sampling transpilation (#2761)


### :sparkles: New Features
- [`202f035`](https://github.com/tobymao/sqlglot/commit/202f035e88e78c15b34c9cc85c4396eab8da1d29) - clickhouse: add aggregate parsing *(PR [#2734](https://github.com/tobymao/sqlglot/pull/2734) by [@pkit](https://github.com/pkit))*
- [`e772e26`](https://github.com/tobymao/sqlglot/commit/e772e262c551f6aa5bb726579253a55db5686da3) - guess the correct dialect in case we get an unknown one *(PR [#2753](https://github.com/tobymao/sqlglot/pull/2753) by [@georgesittas](https://github.com/georgesittas))*
- [`7a07862`](https://github.com/tobymao/sqlglot/commit/7a07862dba744a969c98b2c1069531423673461d) - implement to_s method in Expression for verbose repr mode *(PR [#2756](https://github.com/tobymao/sqlglot/pull/2756) by [@georgesittas](https://github.com/georgesittas))*
- [`4072184`](https://github.com/tobymao/sqlglot/commit/4072184cc498a509bceba2a3dfe12f43794273df) - improve transpilation of TIME/TIMESTAMP_FROM_PARTS *(PR [#2755](https://github.com/tobymao/sqlglot/pull/2755) by [@georgesittas](https://github.com/georgesittas))*
- [`3bd811d`](https://github.com/tobymao/sqlglot/commit/3bd811d67e7001046812f2a8590bd07e23b81c88) - **optimizer**: allow star expansion to be turned off *(PR [#2762](https://github.com/tobymao/sqlglot/pull/2762) by [@georgesittas](https://github.com/georgesittas))*
- [`c246285`](https://github.com/tobymao/sqlglot/commit/c24628531bbd587473e0a43ded7ba8b5e4f35cd8) - bigquery unix_date closes [#2758](https://github.com/tobymao/sqlglot/pull/2758) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a2abbc7`](https://github.com/tobymao/sqlglot/commit/a2abbc773fb330e669c81abc115a81e1055a060f) - improve transpilation of LAST_DAY *(PR [#2766](https://github.com/tobymao/sqlglot/pull/2766) by [@georgesittas](https://github.com/georgesittas))*
- [`7e7ac65`](https://github.com/tobymao/sqlglot/commit/7e7ac65bb67ef5a45ef487859a9cff4f2d0fc07a) - **snowflake**: add support for OBJECT_CONSTRUCT_KEEP_NULL *(PR [#2769](https://github.com/tobymao/sqlglot/pull/2769) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2768](undefined) opened by [@tekumara](https://github.com/tekumara)*

### :bug: Bug Fixes
- [`ed972f9`](https://github.com/tobymao/sqlglot/commit/ed972f9f50fcdc612b7770739a249a918c4d4011) - alter table rename should not qualify with db in postgres *(PR [#2736](https://github.com/tobymao/sqlglot/pull/2736) by [@z3z1ma](https://github.com/z3z1ma))*
- [`1ebfb36`](https://github.com/tobymao/sqlglot/commit/1ebfb3688975e420a70bac10c49ad127446c4c65) - else interval *(commit by [@tobymao](https://github.com/tobymao))*
- [`a43174f`](https://github.com/tobymao/sqlglot/commit/a43174f8ebdb4a51ad12d7dc9c332372a5a0bd84) - generate `CROSS JOIN` instead of comma in `explode_to_unnest` transformation *(PR [#2739](https://github.com/tobymao/sqlglot/pull/2739) by [@cpcloud](https://github.com/cpcloud))*
  - :arrow_lower_right: *fixes issue [#2735](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`e543c55`](https://github.com/tobymao/sqlglot/commit/e543c558a3efea960028bf4c9864cad48e616e82) - interval is null *(commit by [@tobymao](https://github.com/tobymao))*
- [`fb3188f`](https://github.com/tobymao/sqlglot/commit/fb3188f43bfdef2fb315b8b1280aaa207bd7888a) - lineage closes [#2742](https://github.com/tobymao/sqlglot/pull/2742) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b608b2d`](https://github.com/tobymao/sqlglot/commit/b608b2d944934d9da4a2cb373bdf69322f25041a) - **duckdb**: percentile_cont closes [#2741](https://github.com/tobymao/sqlglot/pull/2741) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0b6bdc4`](https://github.com/tobymao/sqlglot/commit/0b6bdc4513aa417dae0a00565b56e78b544306e7) - improve transpilation of JSON value extraction *(PR [#2744](https://github.com/tobymao/sqlglot/pull/2744) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2738](undefined) opened by [@tekumara](https://github.com/tekumara)*
- [`33d6e5f`](https://github.com/tobymao/sqlglot/commit/33d6e5f2636451ffcdd914c100a446246d8031df) - **bigquery**: enable transpilation of single-argument TIME func *(PR [#2752](https://github.com/tobymao/sqlglot/pull/2752) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2749](undefined) opened by [@jherrmannNetfonds](https://github.com/jherrmannNetfonds)*
- [`72f8cfa`](https://github.com/tobymao/sqlglot/commit/72f8cfa2c156efb586bc91d20efd8d0cf9c18735) - **snowflake**: parse two argument version of TIMESTAMP_FROM_PARTS *(PR [#2754](https://github.com/tobymao/sqlglot/pull/2754) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2751](undefined) opened by [@notgerry](https://github.com/notgerry)*
- [`2a94f2b`](https://github.com/tobymao/sqlglot/commit/2a94f2ba0d93ee1454a2d19cc8577e211fd5cbe0) - **bigquery**: fix parsing of COUNTIF *(PR [#2765](https://github.com/tobymao/sqlglot/pull/2765) by [@giovannipcarvalho](https://github.com/giovannipcarvalho))*
  - :arrow_lower_right: *fixes issue [#2764](undefined) opened by [@giovannipcarvalho](https://github.com/giovannipcarvalho)*
- [`862b305`](https://github.com/tobymao/sqlglot/commit/862b305db47c0cea4a66b61211a3ed088e935ea5) - improve table sampling transpilation *(PR [#2761](https://github.com/tobymao/sqlglot/pull/2761) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2757](undefined) opened by [@tekumara](https://github.com/tekumara)*

### :recycle: Refactors
- [`4648c6a`](https://github.com/tobymao/sqlglot/commit/4648c6acbeab8d5155be3f8e53d11d8f00c33a2e) - set sample clause keyword(s) as class constant to enable transpilation *(PR [#2750](https://github.com/tobymao/sqlglot/pull/2750) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2747](undefined) opened by [@tekumara](https://github.com/tekumara)*

[v11.6.3]: https://github.com/tobymao/sqlglot/compare/v11.6.2...v11.6.3

[v11.7.0]: https://github.com/tobymao/sqlglot/compare/v11.6.3...v11.7.0
[v12.0.0]: https://github.com/tobymao/sqlglot/compare/v11.7.1...v12.0.0
[v12.1.0]: https://github.com/tobymao/sqlglot/compare/v12.0.0...v12.1.0
[v12.2.0]: https://github.com/tobymao/sqlglot/compare/v12.1.0...v12.2.0
[v12.4.0]: https://github.com/tobymao/sqlglot/compare/v12.3.0...v12.4.0
[v13.0.0]: https://github.com/tobymao/sqlglot/compare/v12.4.0...v13.0.0
[v13.0.1]: https://github.com/tobymao/sqlglot/compare/v13.0.0...v13.0.1
[v13.0.2]: https://github.com/tobymao/sqlglot/compare/v13.0.1...v13.0.2
[v13.1.0]: https://github.com/tobymao/sqlglot/compare/v13.0.2...v13.1.0
[v13.2.0]: https://github.com/tobymao/sqlglot/compare/v13.1.0...v13.2.0
[v13.2.1]: https://github.com/tobymao/sqlglot/compare/v13.2.0...v13.2.1
[v13.2.2]: https://github.com/tobymao/sqlglot/compare/v13.2.1...v13.2.2
[v13.3.0]: https://github.com/tobymao/sqlglot/compare/v13.2.2...v13.3.0
[v13.3.1]: https://github.com/tobymao/sqlglot/compare/v13.3.0...v13.3.1
[v14.0.0]: https://github.com/tobymao/sqlglot/compare/v13.3.1...v14.0.0
[v14.1.0]: https://github.com/tobymao/sqlglot/compare/show...v14.1.0
[v14.1.1]: https://github.com/tobymao/sqlglot/compare/v14.1.0...v14.1.1
[v15.0.0]: https://github.com/tobymao/sqlglot/compare/v14.1.1...v15.0.0
[v15.1.0]: https://github.com/tobymao/sqlglot/compare/v15.0.0...v15.1.0
[v15.2.0]: https://github.com/tobymao/sqlglot/compare/v15.1.0...v15.2.0
[v15.3.0]: https://github.com/tobymao/sqlglot/compare/v15.2.0...v15.3.0
[v16.0.0]: https://github.com/tobymao/sqlglot/compare/v15.2.0...v16.0.0
[v16.1.0]: https://github.com/tobymao/sqlglot/compare/v16.0.0...v16.1.0
[v16.1.1]: https://github.com/tobymao/sqlglot/compare/v16.1.0...v16.1.1
[v16.1.3]: https://github.com/tobymao/sqlglot/compare/v16.1.2...v16.1.3
[v16.1.4]: https://github.com/tobymao/sqlglot/compare/v16.1.3...v16.1.4
[v16.2.0]: https://github.com/tobymao/sqlglot/compare/v16.1.4...v16.2.0
[v16.2.1]: https://github.com/tobymao/sqlglot/compare/v16.2.0...v16.2.1
[v16.3.0]: https://github.com/tobymao/sqlglot/compare/v16.2.1...v16.3.0
[v16.3.1]: https://github.com/tobymao/sqlglot/compare/v16.3.0...v16.3.1
[v16.4.0]: https://github.com/tobymao/sqlglot/compare/v16.3.1...v16.4.0
[v16.4.1]: https://github.com/tobymao/sqlglot/compare/v16.4.0...v16.4.1
[v16.4.2]: https://github.com/tobymao/sqlglot/compare/v16.4.1...v16.4.2
[v16.5.0]: https://github.com/tobymao/sqlglot/compare/v16.4.2...v16.5.0
[v16.6.0]: https://github.com/tobymao/sqlglot/compare/v16.5.0...v16.6.0
[v16.7.0]: https://github.com/tobymao/sqlglot/compare/v16.6.0...v16.7.0
[v16.7.1]: https://github.com/tobymao/sqlglot/compare/v16.7.0...v16.7.1
[v16.7.2]: https://github.com/tobymao/sqlglot/compare/v16.7.1...v16.7.2
[v16.7.4]: https://github.com/tobymao/sqlglot/compare/v16.7.3...v16.7.4
[v16.7.6]: https://github.com/tobymao/sqlglot/compare/v16.7.5...v16.7.6
[v16.7.7]: https://github.com/tobymao/sqlglot/compare/v16.7.6...v16.7.7
[v16.8.0]: https://github.com/tobymao/sqlglot/compare/v16.7.7...v16.8.0
[v16.8.1]: https://github.com/tobymao/sqlglot/compare/v16.8.0...v16.8.1
[v17.0.0]: https://github.com/tobymao/sqlglot/compare/v16.8.1...v17.0.0
[v17.1.0]: https://github.com/tobymao/sqlglot/compare/v17.0.0...v17.1.0
[v17.2.0]: https://github.com/tobymao/sqlglot/compare/v17.1.0...v17.2.0
[v17.3.0]: https://github.com/tobymao/sqlglot/compare/v17.2.0...v17.3.0
[v17.4.0]: https://github.com/tobymao/sqlglot/compare/v17.3.0...v17.4.0
[v17.4.1]: https://github.com/tobymao/sqlglot/compare/v17.4.0...v17.4.1
[v17.5.0]: https://github.com/tobymao/sqlglot/compare/v17.4.1...v17.5.0
[v17.6.0]: https://github.com/tobymao/sqlglot/compare/v17.5.0...v17.6.0
[v17.6.1]: https://github.com/tobymao/sqlglot/compare/v17.6.0...v17.6.1
[v17.7.0]: https://github.com/tobymao/sqlglot/compare/v17.6.1...v17.7.0
[v17.8.0]: https://github.com/tobymao/sqlglot/compare/v17.7.0...v17.8.0
[v17.8.1]: https://github.com/tobymao/sqlglot/compare/v17.8.0...v17.8.1
[v17.8.2]: https://github.com/tobymao/sqlglot/compare/v17.8.1...v17.8.2
[v17.8.3]: https://github.com/tobymao/sqlglot/compare/v17.8.2...v17.8.3
[v17.8.4]: https://github.com/tobymao/sqlglot/compare/v17.8.3...v17.8.4
[v17.8.5]: https://github.com/tobymao/sqlglot/compare/v17.8.4...v17.8.5
[v17.9.0]: https://github.com/tobymao/sqlglot/compare/v17.8.6...v17.9.0
[v17.9.1]: https://github.com/tobymao/sqlglot/compare/v17.9.0...v17.9.1
[v17.10.0]: https://github.com/tobymao/sqlglot/compare/v17.9.1...v17.10.0
[v17.10.1]: https://github.com/tobymao/sqlglot/compare/v17.10.0...v17.10.1
[v17.10.2]: https://github.com/tobymao/sqlglot/compare/v17.10.1...v17.10.2
[v17.12.0]: https://github.com/tobymao/sqlglot/compare/v17.11.0...v17.12.0
[v17.13.0]: https://github.com/tobymao/sqlglot/compare/v17.12.0...v17.13.0
[v17.14.0]: https://github.com/tobymao/sqlglot/compare/v17.13.0...v17.14.0
[v17.14.1]: https://github.com/tobymao/sqlglot/compare/v17.14.0...v17.14.1
[v17.14.2]: https://github.com/tobymao/sqlglot/compare/v17.14.1...v17.14.2
[v17.15.0]: https://github.com/tobymao/sqlglot/compare/v17.14.2...v17.15.0
[v17.15.1]: https://github.com/tobymao/sqlglot/compare/v17.15.0...v17.15.1
[v17.16.0]: https://github.com/tobymao/sqlglot/compare/v17.15.1...v17.16.0
[v17.16.1]: https://github.com/tobymao/sqlglot/compare/v17.16.0...v17.16.1
[v17.16.2]: https://github.com/tobymao/sqlglot/compare/v17.16.1...v17.16.2
[v18.0.0]: https://github.com/tobymao/sqlglot/compare/v17.16.2...v18.0.0
[v18.0.1]: https://github.com/tobymao/sqlglot/compare/v18.0.0...v18.0.1
[v18.1.0]: https://github.com/tobymao/sqlglot/compare/v18.0.1...v18.1.0
[v18.2.0]: https://github.com/tobymao/sqlglot/compare/v18.1.0...v18.2.0
[v18.3.0]: https://github.com/tobymao/sqlglot/compare/v18.2.0...v18.3.0
[v18.4.0]: https://github.com/tobymao/sqlglot/compare/v18.3.0...v18.4.0
[v18.4.1]: https://github.com/tobymao/sqlglot/compare/v18.4.0...v18.4.1
[v18.5.0]: https://github.com/tobymao/sqlglot/compare/v18.4.1...v18.5.0
[v18.5.1]: https://github.com/tobymao/sqlglot/compare/v18.5.0...v18.5.1
[v18.6.0]: https://github.com/tobymao/sqlglot/compare/v18.5.1...v18.6.0
[v18.7.0]: https://github.com/tobymao/sqlglot/compare/v18.6.0...v18.7.0
[v18.8.0]: https://github.com/tobymao/sqlglot/compare/v18.7.0...v18.8.0
[v18.9.0]: https://github.com/tobymao/sqlglot/compare/v18.8.0...v18.9.0
[v18.10.0]: https://github.com/tobymao/sqlglot/compare/v18.9.0...v18.10.0
[v18.10.1]: https://github.com/tobymao/sqlglot/compare/v18.10.0...v18.10.1
[v18.11.0]: https://github.com/tobymao/sqlglot/compare/v18.10.1...v18.11.0
[v18.11.1]: https://github.com/tobymao/sqlglot/compare/v18.11.0...v18.11.1
[v18.11.2]: https://github.com/tobymao/sqlglot/compare/v18.11.1...v18.11.2
[v18.11.3]: https://github.com/tobymao/sqlglot/compare/v18.11.2...v18.11.3
[v18.11.4]: https://github.com/tobymao/sqlglot/compare/v18.11.3...v18.11.4
[v18.11.5]: https://github.com/tobymao/sqlglot/compare/v18.11.4...v18.11.5
[v18.11.6]: https://github.com/tobymao/sqlglot/compare/v18.11.5...v18.11.6
[v18.12.0]: https://github.com/tobymao/sqlglot/compare/v18.11.6...v18.12.0
[v18.13.0]: https://github.com/tobymao/sqlglot/compare/v18.12.0...v18.13.0
[v18.14.0]: https://github.com/tobymao/sqlglot/compare/v18.13.0...v18.14.0
[v18.15.0]: https://github.com/tobymao/sqlglot/compare/v18.14.0...v18.15.0
[v18.15.1]: https://github.com/tobymao/sqlglot/compare/v18.15.0...v18.15.1
[v18.16.0]: https://github.com/tobymao/sqlglot/compare/v18.15.1...v18.16.0
[v18.16.1]: https://github.com/tobymao/sqlglot/compare/v18.16.0...v18.16.1
[v18.17.0]: https://github.com/tobymao/sqlglot/compare/v18.16.1...v18.17.0
[v19.0.0]: https://github.com/tobymao/sqlglot/compare/v18.17.0...v19.0.0
[v19.0.1]: https://github.com/tobymao/sqlglot/compare/v19.0.0...v19.0.1
[v19.0.2]: https://github.com/tobymao/sqlglot/compare/v19.0.1...v19.0.2
[v19.0.3]: https://github.com/tobymao/sqlglot/compare/v19.0.2...v19.0.3
[v19.1.0]: https://github.com/tobymao/sqlglot/compare/v19.0.3...v19.1.0
[v19.1.1]: https://github.com/tobymao/sqlglot/compare/v19.1.0...v19.1.1
[v19.1.2]: https://github.com/tobymao/sqlglot/compare/v19.1.1...v19.1.2
[v19.1.3]: https://github.com/tobymao/sqlglot/compare/v19.1.2...v19.1.3
[v19.2.0]: https://github.com/tobymao/sqlglot/compare/v19.1.3...v19.2.0
[v19.3.0]: https://github.com/tobymao/sqlglot/compare/v19.2.0...v19.3.0
[v19.3.1]: https://github.com/tobymao/sqlglot/compare/v19.3.0...v19.3.1
[v19.4.0]: https://github.com/tobymao/sqlglot/compare/v19.3.1...v19.4.0
[v19.5.0]: https://github.com/tobymao/sqlglot/compare/v19.4.0...v19.5.0
[v19.5.1]: https://github.com/tobymao/sqlglot/compare/v19.5.0...v19.5.1
[v19.6.0]: https://github.com/tobymao/sqlglot/compare/v19.5.1...v19.6.0
[v19.7.0]: https://github.com/tobymao/sqlglot/compare/v19.6.0...v19.7.0
[v19.8.0]: https://github.com/tobymao/sqlglot/compare/v19.7.0...v19.8.0
[v19.8.1]: https://github.com/tobymao/sqlglot/compare/v19.8.0...v19.8.1
[v19.8.2]: https://github.com/tobymao/sqlglot/compare/v19.8.1...v19.8.2
[v19.8.3]: https://github.com/tobymao/sqlglot/compare/v19.8.2...v19.8.3
[v19.9.0]: https://github.com/tobymao/sqlglot/compare/v19.8.3...v19.9.0
[v20.0.0]: https://github.com/tobymao/sqlglot/compare/v19.9.0...v20.0.0
[v20.1.0]: https://github.com/tobymao/sqlglot/compare/v20.0.0...v20.1.0
[v20.2.0]: https://github.com/tobymao/sqlglot/compare/v20.1.0...v20.2.0
[v20.5.0]: https://github.com/tobymao/sqlglot/compare/v20.4.0...v20.5.0
[v20.6.0]: https://github.com/tobymao/sqlglot/compare/v20.5.0...v20.6.0
[v20.7.1]: https://github.com/tobymao/sqlglot/compare/v20.6.0...v20.7.1
[v20.8.0]: https://github.com/tobymao/sqlglot/compare/v20.7.1...v20.8.0
[v20.9.0]: https://github.com/tobymao/sqlglot/compare/v20.8.0...v20.9.0
[v20.10.0]: https://github.com/tobymao/sqlglot/compare/v20.9.0...v20.10.0
[v20.11.0]: https://github.com/tobymao/sqlglot/compare/v20.10.0...v20.11.0
[v21.0.0]: https://github.com/tobymao/sqlglot/compare/v20.11.0...v21.0.0
[v21.0.1]: https://github.com/tobymao/sqlglot/compare/v21.0.0...v21.0.1
[v21.0.2]: https://github.com/tobymao/sqlglot/compare/v21.0.1...v21.0.2
[v21.1.0]: https://github.com/tobymao/sqlglot/compare/v21.0.2...v21.1.0
[v21.1.1]: https://github.com/tobymao/sqlglot/compare/v21.1.0...v21.1.1
[v21.1.2]: https://github.com/tobymao/sqlglot/compare/v21.1.1...v21.1.2
[v21.2.0]: https://github.com/tobymao/sqlglot/compare/v21.1.2...v21.2.0
[v21.2.1]: https://github.com/tobymao/sqlglot/compare/v21.2.0...v21.2.1
[v22.0.0]: https://github.com/tobymao/sqlglot/compare/v21.2.1...v22.0.0
[v22.0.1]: https://github.com/tobymao/sqlglot/compare/v22.0.0...v22.0.1
[v22.0.2]: https://github.com/tobymao/sqlglot/compare/v22.0.1...v22.0.2
[v22.1.0]: https://github.com/tobymao/sqlglot/compare/v22.0.2...v22.1.0
[v22.1.1]: https://github.com/tobymao/sqlglot/compare/v22.1.0...v22.1.1
[v22.2.0]: https://github.com/tobymao/sqlglot/compare/v22.1.1...v22.2.0
[v22.2.1]: https://github.com/tobymao/sqlglot/compare/v22.2.0...v22.2.1
[v22.3.0]: https://github.com/tobymao/sqlglot/compare/v22.2.1...v22.3.0
[v22.3.1]: https://github.com/tobymao/sqlglot/compare/v22.3.0...v22.3.1
[v22.4.0]: https://github.com/tobymao/sqlglot/compare/v22.3.1...v22.4.0
[v22.5.0]: https://github.com/tobymao/sqlglot/compare/v22.4.0...v22.5.0
[v23.0.0]: https://github.com/tobymao/sqlglot/compare/v22.5.0...v23.0.0
[v23.0.1]: https://github.com/tobymao/sqlglot/compare/v23.0.0...v23.0.1
[v23.0.2]: https://github.com/tobymao/sqlglot/compare/v23.0.1...v23.0.2
[v23.0.3]: https://github.com/tobymao/sqlglot/compare/v23.0.2...v23.0.3
[v23.0.4]: https://github.com/tobymao/sqlglot/compare/v23.0.3...v23.0.4
[v23.0.5]: https://github.com/tobymao/sqlglot/compare/v23.0.4...v23.0.5
[v23.1.0]: https://github.com/tobymao/sqlglot/compare/v23.0.5...v23.1.0
[v23.3.0]: https://github.com/tobymao/sqlglot/compare/v23.2.0...v23.3.0
[v23.4.0]: https://github.com/tobymao/sqlglot/compare/v23.3.0...v23.4.0
[v23.6.0]: https://github.com/tobymao/sqlglot/compare/v23.5.0...v23.6.0
[v23.6.4]: https://github.com/tobymao/sqlglot/compare/v23.6.3...v23.6.4
[v23.7.0]: https://github.com/tobymao/sqlglot/compare/v23.6.4...v23.7.0
[v23.8.0]: https://github.com/tobymao/sqlglot/compare/v23.7.0...v23.8.0
[v23.8.1]: https://github.com/tobymao/sqlglot/compare/v23.8.0...v23.8.1
[v23.8.2]: https://github.com/tobymao/sqlglot/compare/v23.8.1...v23.8.2
[v23.9.0]: https://github.com/tobymao/sqlglot/compare/v23.8.2...v23.9.0
[v23.10.0]: https://github.com/tobymao/sqlglot/compare/v23.9.0...v23.10.0
[v23.11.0]: https://github.com/tobymao/sqlglot/compare/v23.10.0...v23.11.0
[v23.11.1]: https://github.com/tobymao/sqlglot/compare/v23.11.0...v23.11.1
[v23.11.2]: https://github.com/tobymao/sqlglot/compare/v23.11.1...v23.11.2
[v23.12.0]: https://github.com/tobymao/sqlglot/compare/v23.11.2...v23.12.0
[v23.12.1]: https://github.com/tobymao/sqlglot/compare/v23.12.0...v23.12.1
[v23.12.2]: https://github.com/tobymao/sqlglot/compare/v23.12.1...v23.12.2
[v23.13.0]: https://github.com/tobymao/sqlglot/compare/v23.12.2...v23.13.0
[v23.13.1]: https://github.com/tobymao/sqlglot/compare/v23.13.0...v23.13.1
[v23.13.2]: https://github.com/tobymao/sqlglot/compare/v23.13.1...v23.13.2
[v23.13.3]: https://github.com/tobymao/sqlglot/compare/v23.13.2...v23.13.3
[v23.13.4]: https://github.com/tobymao/sqlglot/compare/v23.13.3...v23.13.4
[v23.13.5]: https://github.com/tobymao/sqlglot/compare/v23.13.4...v23.13.5
[v23.13.6]: https://github.com/tobymao/sqlglot/compare/v23.13.5...v23.13.6
[v23.13.7]: https://github.com/tobymao/sqlglot/compare/v23.13.6...v23.13.7
[v23.14.0]: https://github.com/tobymao/sqlglot/compare/v23.13.7...v23.14.0
[v23.15.0]: https://github.com/tobymao/sqlglot/compare/v23.14.0...v23.15.0
[v23.15.1]: https://github.com/tobymao/sqlglot/compare/v23.15.0...v23.15.1
[v23.15.2]: https://github.com/tobymao/sqlglot/compare/v23.15.1...v23.15.2
[v23.15.3]: https://github.com/tobymao/sqlglot/compare/v23.15.2...v23.15.3
[v23.15.6]: https://github.com/tobymao/sqlglot/compare/v23.15.5...v23.15.6
[v23.15.7]: https://github.com/tobymao/sqlglot/compare/v23.15.6...v23.15.7
[v23.15.8]: https://github.com/tobymao/sqlglot/compare/v23.15.7...v23.15.8
[v23.15.9]: https://github.com/tobymao/sqlglot/compare/v23.15.8...v23.15.9
[v23.15.10]: https://github.com/tobymao/sqlglot/compare/v23.15.9...v23.15.10
[v23.16.0]: https://github.com/tobymao/sqlglot/compare/v23.15.10...v23.16.0
[v23.17.0]: https://github.com/tobymao/sqlglot/compare/v23.16.0...v23.17.0
[v24.0.0]: https://github.com/tobymao/sqlglot/compare/v23.17.0...v24.0.0
[v24.0.1]: https://github.com/tobymao/sqlglot/compare/v24.0.0...v24.0.1
[v24.0.2]: https://github.com/tobymao/sqlglot/compare/v24.0.1...v24.0.2
[v24.0.3]: https://github.com/tobymao/sqlglot/compare/v24.0.2...v24.0.3
[v24.1.0]: https://github.com/tobymao/sqlglot/compare/v24.0.3...v24.1.0
[v24.1.1]: https://github.com/tobymao/sqlglot/compare/v24.1.0...v24.1.1
[v24.1.2]: https://github.com/tobymao/sqlglot/compare/v24.1.1...v24.1.2
[v25.0.0]: https://github.com/tobymao/sqlglot/compare/v24.1.2...v25.0.0
[v25.0.2]: https://github.com/tobymao/sqlglot/compare/v25.0.1...v25.0.2
[v25.0.3]: https://github.com/tobymao/sqlglot/compare/v25.0.2...v25.0.3
[v25.1.0]: https://github.com/tobymao/sqlglot/compare/v25.0.3...v25.1.0
[v25.2.0]: https://github.com/tobymao/sqlglot/compare/v25.1.0...v25.2.0
[v25.3.0]: https://github.com/tobymao/sqlglot/compare/v25.2.0...v25.3.0
[v25.3.1]: https://github.com/tobymao/sqlglot/compare/v25.3.0...v25.3.1
[v25.3.2]: https://github.com/tobymao/sqlglot/compare/v25.3.1...v25.3.2
[v25.3.3]: https://github.com/tobymao/sqlglot/compare/v25.3.2...v25.3.3
[v25.4.0]: https://github.com/tobymao/sqlglot/compare/v25.3.3...v25.4.0
[v25.4.1]: https://github.com/tobymao/sqlglot/compare/v25.4.0...v25.4.1
[v25.5.0]: https://github.com/tobymao/sqlglot/compare/v25.4.1...v25.5.0
[v25.5.1]: https://github.com/tobymao/sqlglot/compare/v25.5.0...v25.5.1
[v25.6.0]: https://github.com/tobymao/sqlglot/compare/v25.5.1...v25.6.0
[v25.6.1]: https://github.com/tobymao/sqlglot/compare/v25.6.0...v25.6.1
[v25.7.0]: https://github.com/tobymao/sqlglot/compare/v25.6.1...v25.7.0
[v25.7.1]: https://github.com/tobymao/sqlglot/compare/v25.7.0...v25.7.1
[v25.8.0]: https://github.com/tobymao/sqlglot/compare/v25.7.1...v25.8.0
[v25.8.1]: https://github.com/tobymao/sqlglot/compare/v25.8.0...v25.8.1
[v25.9.0]: https://github.com/tobymao/sqlglot/compare/v25.8.1...v25.9.0
[v25.10.0]: https://github.com/tobymao/sqlglot/compare/v25.9.0...v25.10.0
[v25.11.0]: https://github.com/tobymao/sqlglot/compare/v25.10.0...v25.11.0
[v25.11.1]: https://github.com/tobymao/sqlglot/compare/v25.11.0...v25.11.1
[v25.11.2]: https://github.com/tobymao/sqlglot/compare/v25.11.1...v25.11.2
[v25.11.3]: https://github.com/tobymao/sqlglot/compare/v25.11.2...v25.11.3
[v25.12.0]: https://github.com/tobymao/sqlglot/compare/v25.11.3...v25.12.0
[v25.13.0]: https://github.com/tobymao/sqlglot/compare/v25.12.0...v25.13.0
[v25.14.0]: https://github.com/tobymao/sqlglot/compare/v25.13.0...v25.14.0
[v25.15.0]: https://github.com/tobymao/sqlglot/compare/v25.14.0...v25.15.0
[v25.16.0]: https://github.com/tobymao/sqlglot/compare/v25.15.0...v25.16.0
[v25.16.1]: https://github.com/tobymao/sqlglot/compare/v25.16.0...v25.16.1
[v25.17.0]: https://github.com/tobymao/sqlglot/compare/v25.16.1...v25.17.0
[v25.18.0]: https://github.com/tobymao/sqlglot/compare/v25.17.0...v25.18.0
[v25.19.0]: https://github.com/tobymao/sqlglot/compare/v25.18.0...v25.19.0
[v25.20.0]: https://github.com/tobymao/sqlglot/compare/v25.19.0...v25.20.0
[v25.20.1]: https://github.com/tobymao/sqlglot/compare/v25.20.0...v25.20.1
[v25.21.0]: https://github.com/tobymao/sqlglot/compare/v25.20.1...v25.21.0
[v25.21.1]: https://github.com/tobymao/sqlglot/compare/v25.21.0...v25.21.1
[v25.21.2]: https://github.com/tobymao/sqlglot/compare/v25.21.1...v25.21.2
[v25.21.3]: https://github.com/tobymao/sqlglot/compare/v25.21.2...v25.21.3
[v25.22.0]: https://github.com/tobymao/sqlglot/compare/v25.21.3...v25.22.0
[v25.23.0]: https://github.com/tobymao/sqlglot/compare/v25.20.2...v25.23.0
[v25.23.1]: https://github.com/tobymao/sqlglot/compare/v25.23.0...v25.23.1
[v25.23.2]: https://github.com/tobymao/sqlglot/compare/v25.23.1...v25.23.2
[v25.24.0]: https://github.com/tobymao/sqlglot/compare/v25.23.2...v25.24.0
[v25.24.1]: https://github.com/tobymao/sqlglot/compare/v25.24.0...v25.24.1
[v25.24.2]: https://github.com/tobymao/sqlglot/compare/v25.24.1...v25.24.2
[v25.24.3]: https://github.com/tobymao/sqlglot/compare/v25.24.2...v25.24.3
[v25.24.4]: https://github.com/tobymao/sqlglot/compare/v25.24.3...v25.24.4
[v25.24.5]: https://github.com/tobymao/sqlglot/compare/v25.24.4...v25.24.5
[v25.25.0]: https://github.com/tobymao/sqlglot/compare/v25.24.5...v25.25.0
[v25.25.1]: https://github.com/tobymao/sqlglot/compare/v25.25.0...v25.25.1
[v25.26.0]: https://github.com/tobymao/sqlglot/compare/v25.25.1...v25.26.0
[v25.27.0]: https://github.com/tobymao/sqlglot/compare/v25.26.0...v25.27.0
[v25.28.0]: https://github.com/tobymao/sqlglot/compare/v25.27.0...v25.28.0
[v25.29.0]: https://github.com/tobymao/sqlglot/compare/v25.28.0...v25.29.0
[v25.30.0]: https://github.com/tobymao/sqlglot/compare/v25.29.0...v25.30.0
[v25.31.0]: https://github.com/tobymao/sqlglot/compare/v25.30.0...v25.31.0
[v25.31.1]: https://github.com/tobymao/sqlglot/compare/v25.31.0...v25.31.1
[v25.31.2]: https://github.com/tobymao/sqlglot/compare/v25.31.1...v25.31.2
[v25.31.3]: https://github.com/tobymao/sqlglot/compare/v25.31.2...v25.31.3
[v25.31.4]: https://github.com/tobymao/sqlglot/compare/v25.31.3...v25.31.4
[v25.32.0]: https://github.com/tobymao/sqlglot/compare/v25.31.4...v25.32.0
[v25.32.1]: https://github.com/tobymao/sqlglot/compare/v25.32.0...v25.32.1
[v25.33.0]: https://github.com/tobymao/sqlglot/compare/v25.32.1...v25.33.0
[v25.34.0]: https://github.com/tobymao/sqlglot/compare/v25.33.0...v25.34.0
[v25.34.1]: https://github.com/tobymao/sqlglot/compare/v25.34.0...v25.34.1
[v26.0.0]: https://github.com/tobymao/sqlglot/compare/v25.34.1...v26.0.0
[v26.0.1]: https://github.com/tobymao/sqlglot/compare/v26.0.0...v26.0.1
[v26.1.0]: https://github.com/tobymao/sqlglot/compare/v26.0.1...v26.1.0
[v26.1.1]: https://github.com/tobymao/sqlglot/compare/v26.1.0...v26.1.1
[v26.1.2]: https://github.com/tobymao/sqlglot/compare/v26.1.1...v26.1.2
[v26.1.3]: https://github.com/tobymao/sqlglot/compare/v26.1.2...v26.1.3
[v26.2.0]: https://github.com/tobymao/sqlglot/compare/v26.1.3...v26.2.0
[v26.2.1]: https://github.com/tobymao/sqlglot/compare/v26.2.0...v26.2.1
[v26.3.0]: https://github.com/tobymao/sqlglot/compare/v26.2.1...v26.3.0
[v26.3.1]: https://github.com/tobymao/sqlglot/compare/v26.3.0...v26.3.1
[v26.3.2]: https://github.com/tobymao/sqlglot/compare/v26.3.1...v26.3.2
[v26.3.3]: https://github.com/tobymao/sqlglot/compare/v26.3.2...v26.3.3
[v26.3.4]: https://github.com/tobymao/sqlglot/compare/v26.3.3...v26.3.4
[v26.3.5]: https://github.com/tobymao/sqlglot/compare/v26.3.4...v26.3.5
[v26.3.6]: https://github.com/tobymao/sqlglot/compare/v26.3.5...v26.3.6
[v26.3.7]: https://github.com/tobymao/sqlglot/compare/v26.3.6...v26.3.7
[v26.3.8]: https://github.com/tobymao/sqlglot/compare/v26.3.7...v26.3.8
[v26.3.9]: https://github.com/tobymao/sqlglot/compare/v26.3.8...v26.3.9
[v26.4.0]: https://github.com/tobymao/sqlglot/compare/v26.3.9...v26.4.0
[v26.4.1]: https://github.com/tobymao/sqlglot/compare/v26.4.0...v26.4.1
[v26.5.0]: https://github.com/tobymao/sqlglot/compare/v26.4.1...v26.5.0
[v26.6.0]: https://github.com/tobymao/sqlglot/compare/v26.5.0...v26.6.0
[v26.7.0]: https://github.com/tobymao/sqlglot/compare/v26.6.0...v26.7.0
[v26.8.0]: https://github.com/tobymao/sqlglot/compare/v26.7.0...v26.8.0
[v26.9.0]: https://github.com/tobymao/sqlglot/compare/v26.8.0...v26.9.0
[v26.10.0]: https://github.com/tobymao/sqlglot/compare/v26.9.0...v26.10.0
[v26.10.1]: https://github.com/tobymao/sqlglot/compare/v26.10.0...v26.10.1
[v26.11.0]: https://github.com/tobymao/sqlglot/compare/v26.10.1...v26.11.0
[v26.11.1]: https://github.com/tobymao/sqlglot/compare/v26.11.0...v26.11.1
[v26.12.0]: https://github.com/tobymao/sqlglot/compare/v26.11.1...v26.12.0
[v26.13.0]: https://github.com/tobymao/sqlglot/compare/v26.12.1...v26.13.0
[v26.13.1]: https://github.com/tobymao/sqlglot/compare/v26.13.0...v26.13.1
[v26.13.2]: https://github.com/tobymao/sqlglot/compare/v26.13.1...v26.13.2
[v26.14.0]: https://github.com/tobymao/sqlglot/compare/v26.13.2...v26.14.0
[v26.15.0]: https://github.com/tobymao/sqlglot/compare/v26.14.0...v26.15.0
[v26.16.0]: https://github.com/tobymao/sqlglot/compare/v26.15.0...v26.16.0
[v26.16.1]: https://github.com/tobymao/sqlglot/compare/v26.16.0...v26.16.1
[v26.16.2]: https://github.com/tobymao/sqlglot/compare/v26.16.1...v26.16.2
[v26.16.3]: https://github.com/tobymao/sqlglot/compare/v26.16.2...v26.16.3
[v26.16.4]: https://github.com/tobymao/sqlglot/compare/v26.16.3...v26.16.4
[v26.18.0]: https://github.com/tobymao/sqlglot/compare/v26.12.3...v26.18.0
[v26.18.1]: https://github.com/tobymao/sqlglot/compare/v26.18.0...v26.18.1
[v26.19.0]: https://github.com/tobymao/sqlglot/compare/v26.18.1...v26.19.0
[v26.20.0]: https://github.com/tobymao/sqlglot/compare/v26.19.0...v26.20.0
[v26.21.0]: https://github.com/tobymao/sqlglot/compare/v26.20.0...v26.21.0
[v26.22.0]: https://github.com/tobymao/sqlglot/compare/v26.21.0...v26.22.0
[v26.22.1]: https://github.com/tobymao/sqlglot/compare/v26.22.0...v26.22.1
[v26.23.0]: https://github.com/tobymao/sqlglot/compare/v26.22.1...v26.23.0
[v26.24.0]: https://github.com/tobymao/sqlglot/compare/v26.23.0...v26.24.0
[v26.25.0]: https://github.com/tobymao/sqlglot/compare/v26.24.0...v26.25.0
[v26.25.1]: https://github.com/tobymao/sqlglot/compare/v26.25.0...v26.25.1
[v26.25.2]: https://github.com/tobymao/sqlglot/compare/v26.25.1...v26.25.2
[v26.25.3]: https://github.com/tobymao/sqlglot/compare/v26.25.2...v26.25.3
[v26.26.0]: https://github.com/tobymao/sqlglot/compare/v26.25.3...v26.26.0
[v26.27.0]: https://github.com/tobymao/sqlglot/compare/v26.26.0...v26.27.0
[v26.28.1]: https://github.com/tobymao/sqlglot/compare/v26.27.1...v26.28.1
[v26.29.0]: https://github.com/tobymao/sqlglot/compare/v26.28.1...v26.29.0
[v26.30.0]: https://github.com/tobymao/sqlglot/compare/v26.29.0...v26.30.0
[v26.31.0]: https://github.com/tobymao/sqlglot/compare/v26.21.2...v26.31.0
[v26.33.0]: https://github.com/tobymao/sqlglot/compare/v26.32.0...v26.33.0
[v27.0.0]: https://github.com/tobymao/sqlglot/compare/v26.21.3...v27.0.0
[v27.1.0]: https://github.com/tobymao/sqlglot/compare/v26.31.2...v27.1.0
[v27.2.0]: https://github.com/tobymao/sqlglot/compare/v27.1.0...v27.2.0
[v27.3.0]: https://github.com/tobymao/sqlglot/compare/v27.0.1...v27.3.0
[v27.3.1]: https://github.com/tobymao/sqlglot/compare/v27.3.0...v27.3.1
[v27.4.0]: https://github.com/tobymao/sqlglot/compare/v27.3.1...v27.4.0
[v27.4.1]: https://github.com/tobymao/sqlglot/compare/v27.4.0...v27.4.1
[v27.5.0]: https://github.com/tobymao/sqlglot/compare/v27.4.1...v27.5.0
[v27.5.1]: https://github.com/tobymao/sqlglot/compare/v27.5.0...v27.5.1
[v27.6.0]: https://github.com/tobymao/sqlglot/compare/v27.5.1...v27.6.0
[v27.7.0]: https://github.com/tobymao/sqlglot/compare/v27.6.0...v27.7.0
[v27.8.0]: https://github.com/tobymao/sqlglot/compare/v27.7.0...v27.8.0
[v27.9.0]: https://github.com/tobymao/sqlglot/compare/v27.8.0...v27.9.0
[v27.10.0]: https://github.com/tobymao/sqlglot/compare/v27.9.0...v27.10.0
[v27.11.0]: https://github.com/tobymao/sqlglot/compare/v27.10.0...v27.11.0
[v27.12.0]: https://github.com/tobymao/sqlglot/compare/v27.11.0...v27.12.0
[v27.13.0]: https://github.com/tobymao/sqlglot/compare/v27.12.0...v27.13.0
[v27.13.1]: https://github.com/tobymao/sqlglot/compare/v27.13.0...v27.13.1
[v27.13.2]: https://github.com/tobymao/sqlglot/compare/v27.13.1...v27.13.2
[v27.14.0]: https://github.com/tobymao/sqlglot/compare/v27.13.2...v27.14.0
[v27.15.0]: https://github.com/tobymao/sqlglot/compare/v27.14.0...v27.15.0
[v27.15.1]: https://github.com/tobymao/sqlglot/compare/v27.15.0...v27.15.1
[v27.15.2]: https://github.com/tobymao/sqlglot/compare/v27.15.1...v27.15.2
[v27.15.3]: https://github.com/tobymao/sqlglot/compare/v27.15.2...v27.15.3
[v27.16.0]: https://github.com/tobymao/sqlglot/compare/v27.15.3...v27.16.0
[v27.16.1]: https://github.com/tobymao/sqlglot/compare/v27.16.0...v27.16.1
[v27.16.2]: https://github.com/tobymao/sqlglot/compare/v27.16.1...v27.16.2
[v27.16.3]: https://github.com/tobymao/sqlglot/compare/v27.16.2...v27.16.3
[v27.17.0]: https://github.com/tobymao/sqlglot/compare/v27.16.3...v27.17.0
[v27.18.0]: https://github.com/tobymao/sqlglot/compare/v27.17.0...v27.18.0
[v27.19.0]: https://github.com/tobymao/sqlglot/compare/v27.18.0...v27.19.0
[v27.20.0]: https://github.com/tobymao/sqlglot/compare/v27.19.0...v27.20.0
[v27.21.0]: https://github.com/tobymao/sqlglot/compare/v27.20.0...v27.21.0
[v27.22.0]: https://github.com/tobymao/sqlglot/compare/v27.21.0...v27.22.0
[v27.22.1]: https://github.com/tobymao/sqlglot/compare/v27.22.0...v27.22.1
