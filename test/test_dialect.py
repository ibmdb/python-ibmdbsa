"""Offline unit tests for the ibm_db_sa dialect.

These tests do NOT require a live Db2 connection. They exercise the SQL
compiler, DDL generation and type rendering using SQLAlchemy's
``AssertsCompiledSQL`` mixin. Run them with::

    pytest test/test_dialect.py

Because they need no database, they are the fast feedback loop used when
validating dialect changes (for example the SQLAlchemy 2.1 migration).
"""

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    func,
    select,
)
from sqlalchemy.dialects import registry
from sqlalchemy.testing import AssertsCompiledSQL, fixtures

registry.register("db2", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")

from ibm_db_sa.ibm_db import DB2Dialect_ibm_db  # noqa: E402


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = DB2Dialect_ibm_db()

    def setup_method(self):
        self.metadata = MetaData()
        self.table = Table(
            "some_table",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

    def test_select_all(self):
        stmt = select(self.table)
        self.assert_compile(
            stmt,
            'SELECT some_table.id, some_table.name FROM some_table',
        )

    def test_limit_renders(self):
        stmt = select(self.table).limit(5)
        sql = str(stmt.compile(dialect=self.__dialect__)).upper()
        assert "LIMIT" in sql or "FETCH FIRST" in sql or "ROW_NUMBER" in sql

    def test_offset_uses_rownumber(self):
        stmt = select(self.table).offset(10)
        sql = str(stmt.compile(dialect=self.__dialect__)).upper()
        assert "ROW_NUMBER()" in sql

    def test_avg_wraps_double(self):
        stmt = select(func.avg(self.table.c.id))
        sql = str(stmt.compile(dialect=self.__dialect__)).upper()
        assert "AVG(DOUBLE(" in sql

    def test_mod_operator(self):
        stmt = select(self.table.c.id % 3)
        sql = str(stmt.compile(dialect=self.__dialect__))
        assert "mod(" in sql.lower()


class TypeCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = DB2Dialect_ibm_db()

    def test_string_type(self):
        from sqlalchemy import String

        self.assert_compile(String(40), "VARCHAR(40)")

    def test_integer_type(self):
        from sqlalchemy import Integer

        # Db2 renders Integer as INT
        self.assert_compile(Integer(), "INT")


