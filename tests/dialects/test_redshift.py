from tests.dialects.test_dialect import Validator


class TestRedshift(Validator):
    dialect = "redshift"

    def test_redshift(self):
        self.validate_all(
            'create table "group" ("col" char(10))',
            write={
                "redshift": 'CREATE TABLE "group" ("col" CHAR(10))',
                "mysql": "CREATE TABLE `group` (`col` CHAR(10))",
            },
        )
        self.validate_all(
            'create table if not exists city_slash_id("city/id" integer not null, state char(2) not null)',
            write={
                "redshift": 'CREATE TABLE IF NOT EXISTS city_slash_id ("city/id" INTEGER NOT NULL, state CHAR(2) NOT NULL)',
                "presto": 'CREATE TABLE IF NOT EXISTS city_slash_id ("city/id" INTEGER NOT NULL, state CHAR(2) NOT NULL)',
            },
        )
        self.validate_all(
            "SELECT ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;POINT(10 20)')::geography)",
            write={
                "redshift": "SELECT ST_ASEWKT(CAST(ST_GEOMFROMEWKT('SRID=4326;POINT(10 20)') AS GEOGRAPHY))",
                "bigquery": "SELECT ST_ASEWKT(CAST(ST_GEOMFROMEWKT('SRID=4326;POINT(10 20)') AS GEOGRAPHY))",
            },
        )
        self.validate_all(
            "SELECT ST_AsEWKT(ST_GeogFromText('LINESTRING(110 40, 2 3, -10 80, -7 9)')::geometry)",
            write={
                "redshift": "SELECT ST_ASEWKT(CAST(ST_GEOGFROMTEXT('LINESTRING(110 40, 2 3, -10 80, -7 9)') AS GEOMETRY))",
            },
        )
        self.validate_all(
            "SELECT 'abc'::BINARY",
            write={
                "redshift": "SELECT CAST('abc' AS VARBYTE)",
            },
        )
        self.validate_all(
            "SELECT * FROM venue WHERE (venuecity, venuestate) IN (('Miami', 'FL'), ('Tampa', 'FL')) ORDER BY venueid",
            write={
                "redshift": "SELECT * FROM venue WHERE (venuecity, venuestate) IN (('Miami', 'FL'), ('Tampa', 'FL')) ORDER BY venueid",
            },
        )
        self.validate_all(
            'SELECT tablename, "column" FROM pg_table_def WHERE "column" LIKE \'%start\\_%\' LIMIT 5',
            write={
                "redshift": 'SELECT tablename, "column" FROM pg_table_def WHERE "column" LIKE \'%start\\\\_%\' LIMIT 5'
            },
        )

    def test_identity(self):
        self.validate_identity("CAST('bla' AS SUPER)")
        self.validate_identity("CREATE TABLE real1 (realcol REAL)")
        self.validate_identity("CAST('foo' AS HLLSKETCH)")
        self.validate_identity("SELECT DATEADD(day, 1, 'today')")
        self.validate_identity("'abc' SIMILAR TO '(b|c)%'")
        self.validate_identity(
            "SELECT caldate + INTERVAL '1 second' AS dateplus FROM date WHERE caldate = '12-31-2008'"
        )
        self.validate_identity(
            "CREATE TABLE datetable (start_date DATE, end_date DATE)"
        )
        self.validate_identity(
            "SELECT COUNT(*) FROM event WHERE eventname LIKE '%Ring%' OR eventname LIKE '%Die%'"
        )
