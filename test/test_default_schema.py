"""Default schema detection for the ibm_db dialect."""

from sqlalchemy import Column, Integer, Table, inspect
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from ibm_db_sa.ibm_db import DB2Dialect_ibm_db


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _DBAPIConnection:
    # ibm_db_dbi returns the user passed to connect(), which is '' when the
    # credentials are part of the DSN.
    def get_current_schema(self):
        return ""


class _Connection:
    connection = _DBAPIConnection()

    def __init__(self):
        self.statements = []

    def exec_driver_sql(self, statement):
        self.statements.append(statement)
        return _Result("DB2INST1 ")


class TestDefaultSchemaName(fixtures.TestBase):
    def test_uses_server_current_schema(self):
        connection = _Connection()
        eq_(DB2Dialect_ibm_db()._get_default_schema_name(connection), "db2inst1")
        eq_(connection.statements, ["VALUES CURRENT SCHEMA"])


class TestDefaultSchemaReflection(fixtures.TestBase):
    __only_on__ = "ibm_db_sa+ibm_db_sa"
    __backend__ = True

    def test_default_schema_matches_server(self, connection):
        current = connection.exec_driver_sql("VALUES CURRENT SCHEMA").scalar()
        expected = connection.dialect.normalize_name(current.strip())
        eq_(connection.dialect.default_schema_name, expected)
        eq_(inspect(connection).default_schema_name, expected)

    def test_reflection_without_schema(self, metadata, connection):
        Table(
            "default_schema_reflect",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("amount", Integer),
        ).create(connection)
        inspector = inspect(connection)
        eq_("default_schema_reflect" in inspector.get_table_names(), True)
        columns = inspector.get_columns("default_schema_reflect")
        eq_([c["name"] for c in columns], ["id", "amount"])
