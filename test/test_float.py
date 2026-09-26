"""Float result conversion with the ibm_db DBAPI."""

from decimal import Decimal

from sqlalchemy import Column, Float, Integer, REAL, Table, select
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from ibm_db_sa.ibm_db import DB2Dialect_ibm_db


VALUES = (1.25, -2.5, 0.0, None)


def _expected(value, asdecimal):
    if value is None or not asdecimal:
        return value
    return Decimal(str(value))


class TestFloatResults(fixtures.TestBase):
    def _check(self, type_, asdecimal):
        dialect = DB2Dialect_ibm_db()
        impl = type_.dialect_impl(dialect)
        processor = impl.result_processor(dialect, None)
        for value in VALUES:
            result = processor(value) if processor else value
            expected = _expected(value, asdecimal)
            eq_(result, expected)
            eq_(type(result), type(expected))

    def test_float_asdecimal(self):
        self._check(Float(asdecimal=True), True)

    def test_real_asdecimal(self):
        self._check(REAL(asdecimal=True), True)

    def test_float_default(self):
        self._check(Float(), False)


class TestFloatRoundTrip(fixtures.TestBase):
    __only_on__ = "ibm_db_sa+ibm_db_sa"
    __backend__ = True

    def _round_trip(self, metadata, connection, asdecimal):
        table = Table(
            "float_results",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("amount", Float(asdecimal=asdecimal)),
        )
        table.create(connection)
        connection.execute(
            table.insert(),
            [{"id": i, "amount": value} for i, value in enumerate(VALUES)],
        )
        actual = (
            connection.execute(select(table.c.amount).order_by(table.c.id))
            .scalars()
            .all()
        )
        expected = [_expected(value, asdecimal) for value in VALUES]
        eq_(actual, expected)
        for value, result in zip(expected, actual):
            eq_(type(result), type(value))

    def test_float_asdecimal_round_trip(self, metadata, connection):
        self._round_trip(metadata, connection, True)

    def test_float_round_trip(self, metadata, connection):
        self._round_trip(metadata, connection, False)
