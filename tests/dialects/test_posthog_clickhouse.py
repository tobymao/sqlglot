from tests.dialects.test_dialect import Validator


class TestPosthogClickhouse(Validator):
    dialect = "posthogclickhouse"

    def test_posthog_clickhouse(self):
        self.validate_all(
            "PROPERTY('email') = 'test@test.com'",
            write={
                "posthogclickhouse": "JSONExtractRaw(properties, 'email') = 'test@test.com'"
            },
        )

        # self.validate_all(
        #     "SELECT * FROM events WHERE something = 'value'",
        #     write={
        #         "posthogclickhouse": "SELECT * FROM events WHERE something = 'value' AND team_id = 2"
        #     },
        # )

        # self.validate_all(
        #     "SELECT * FROM events INNER JOIN (SELECT * FROM events)",
        #     write={
        #         "posthogclickhouse": "SELECT * FROM events INNER JOIN (SELECT * FROM events WHERE team_id = 2) WHERE team_id = 2"
        #     },
        # )

        # self.validate_all(
        #     "SELECT * FROM events",
        #     write={"posthogclickhouse": "SELECT * FROM events WHERE team_id = 2"},
        # )
