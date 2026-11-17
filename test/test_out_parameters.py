"""Live-database test for Db2 stored-procedure OUT parameters.

This test only runs when a real Db2 backend is configured (``--dburi``),
because it creates and calls a stored procedure. It is skipped otherwise.

Modernized for the SQLAlchemy 2.0 connection API (the old connectionless
``config.db.execute(...)`` and ``outparam()`` helpers were removed in 2.0).
"""

from sqlalchemy import text
from sqlalchemy.testing import config, fixtures
from sqlalchemy.testing.assertions import eq_


class OutParamTest(fixtures.TestBase):
    __only_on__ = "db2"
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        with config.db.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE OR REPLACE PROCEDURE foo(
                    IN x_in INTEGER,
                    OUT x_out INTEGER,
                    OUT y_out INTEGER,
                    OUT z_out VARCHAR(20))
                BEGIN
                    SET x_out = 10;
                    SET y_out = x_in * 15;
                    SET z_out = NULL;
                END
                """
            )

    def test_out_params(self, connection):
        result = connection.execute(
            text("call foo(:x_in, :x_out, :y_out, :z_out)"),
            {"x_in": 5, "x_out": 0, "y_out": 0, "z_out": ""},
        )
        eq_(
            result.out_parameters,
            {"x_out": 10, "y_out": 75, "z_out": None},
        )
        assert isinstance(result.out_parameters["x_out"], int)

    @classmethod
    def teardown_test_class(cls):
        with config.db.begin() as conn:
            conn.exec_driver_sql("DROP PROCEDURE foo")
