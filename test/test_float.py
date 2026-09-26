"""Float result conversion with the ibm_db DBAPI."""

from decimal import Decimal

from sqlalchemy import Column, Float, Integer, MetaData, REAL, Table, inspect, select
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from ibm_db_sa.base import DOUBLE
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

    def test_double_is_float(self):
        type_ = DOUBLE()
        eq_(isinstance(type_, Float), True)
        eq_(type_.asdecimal, False)
        eq_(type_.python_type, float)
        eq_(str(type_.compile(dialect=DB2Dialect_ibm_db())), "DOUBLE")
        self._check(type_, False)


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

    def test_reflected_double(self, metadata, connection):
        table = Table(
            "float_reflect",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("amount", DOUBLE()),
        )
        table.create(connection)
        connection.execute(
            table.insert(),
            [{"id": i, "amount": value} for i, value in enumerate(VALUES)],
        )
        inspector = inspect(connection)
        schema = connection.dialect.normalize_name(
            connection.exec_driver_sql("VALUES CURRENT SCHEMA").scalar().strip()
        )
        columns = inspector.get_columns("float_reflect", schema=schema)
        type_ = next(c["type"] for c in columns if c["name"] == "amount")
        eq_(isinstance(type_, DOUBLE), True)
        eq_(type_.asdecimal, False)
        eq_(type_.python_type, float)
        reflected = Table(
            "float_reflect", MetaData(), schema=schema, autoload_with=connection
        )
        actual = (
            connection.execute(select(reflected.c.amount).order_by(reflected.c.id))
            .scalars()
            .all()
        )
        eq_(actual, list(VALUES))
        for value, result in zip(VALUES, actual):
            eq_(type(result), type(value))
