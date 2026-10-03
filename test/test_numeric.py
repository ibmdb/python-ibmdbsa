"""Exact numeric binding for the ibm_db DBAPI."""

from decimal import Decimal

from sqlalchemy import Column, DECIMAL, Float, Integer, Numeric, Table, select
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from ibm_db_sa.ibm_db import DB2Dialect_ibm_db


VALUES = (
    Decimal("123456789012345.67"),
    Decimal("123456789012345678901.1234567890"),
    Decimal("-123456789012345678901.1234567890"),
    Decimal("1E-10"),
    Decimal("0"),
    None,
)


class TestNumericBind(fixtures.TestBase):
    def _check_bind(self, type_):
        dialect = DB2Dialect_ibm_db()
        processor = type_.dialect_impl(dialect).bind_processor(dialect)
        for value in VALUES + (7, 1.25):
            bound = processor(value) if processor else value
            eq_(bound, value)
            eq_(type(bound), type(value))

    def test_numeric_bind(self):
        self._check_bind(Numeric(31, 10))

    def test_decimal_bind(self):
        self._check_bind(DECIMAL(31, 10))

    def test_numeric_as_float_bind(self):
        # asdecimal controls results, not the precision of input parameters.
        self._check_bind(Numeric(31, 10, asdecimal=False))

    def test_numeric_results(self):
        dialect = DB2Dialect_ibm_db()
        for asdecimal in (True, False):
            type_ = Numeric(31, 10, asdecimal=asdecimal).dialect_impl(dialect)
            processor = type_.result_processor(dialect, None)
            for value in VALUES:
                result = processor(value) if processor else value
                expected = value if asdecimal or value is None else float(value)
                eq_(result, expected)
                eq_(type(result), type(expected))

    def test_float_results(self):
        dialect = DB2Dialect_ibm_db()
        type_ = Float().dialect_impl(dialect)
        processor = type_.result_processor(dialect, None)
        for value in (1.25, None):
            result = processor(value) if processor else value
            eq_(result, value)
            eq_(type(result), type(value))


class TestNumericRoundTrip(fixtures.TestBase):
    __only_on__ = "ibm_db_sa+ibm_db_sa"
    __backend__ = True

    def _table(self, metadata, connection, type_):
        table = Table(
            "numeric_bind_exact",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("amount", type_),
        )
        table.create(connection)
        return table

    def _round_trip(self, metadata, connection, type_):
        table = self._table(metadata, connection, type_)
        # Exercise both execute and executemany, without casts or tolerances.
        connection.execute(table.insert(), {"id": 0, "amount": VALUES[0]})
        connection.execute(
            table.insert(),
            [{"id": i, "amount": value} for i, value in enumerate(VALUES[1:], 1)],
        )
        actual = (
            connection.execute(select(table.c.amount).order_by(table.c.id))
            .scalars()
            .all()
        )
        eq_(actual, list(VALUES))
        for value, result in zip(VALUES, actual):
            eq_(type(result), type(value))
        # Decimal-valued predicates must not round a filter parameter either.
        for i, value in enumerate(VALUES[:-1]):
            eq_(connection.scalar(select(table.c.id).where(table.c.amount == value)), i)

    def test_decimal_round_trip(self, metadata, connection):
        self._round_trip(metadata, connection, DECIMAL(31, 10))

    def test_numeric_round_trip(self, metadata, connection):
        self._round_trip(metadata, connection, Numeric(31, 10))

    def test_dbapi_accepts_decimal(self, metadata, connection):
        self._table(metadata, connection, DECIMAL(31, 10))
        # Bypass SQLAlchemy type processors to verify DBAPI native support.
        connection.exec_driver_sql(
            "INSERT INTO numeric_bind_exact (id, amount) VALUES (?, ?)",
            (0, VALUES[0]),
        )
        connection.exec_driver_sql(
            "INSERT INTO numeric_bind_exact (id, amount) VALUES (?, ?)",
            list(enumerate(VALUES[1:], 1)),
        )
        actual = (
            connection.exec_driver_sql(
                "SELECT amount FROM numeric_bind_exact ORDER BY id"
            )
            .scalars()
            .all()
        )
        eq_(actual, list(VALUES))
        for value, result in zip(VALUES, actual):
            eq_(type(result), type(value))
