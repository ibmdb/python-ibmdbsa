"""Offline unit tests for the ibm_db_sa dialect.

These tests do not need a Db2 connection; they only compile SQL, DDL and types.
Run them with ``pytest test/test_dialect.py``.
"""

import decimal

import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Enum,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UnicodeText,
    exc,
    func,
    literal,
    literal_column,
    select,
)
from sqlalchemy.sql import quoted_name
from sqlalchemy.testing import AssertsCompiledSQL, fixtures

from ibm_db_sa.ibm_db import SA_VERSION_MM, DB2Dialect_ibm_db


def _dialect(dbms_name=None):
    dialect = DB2Dialect_ibm_db()
    dialect.dbms_name = dbms_name
    return dialect


class _TableFixture:
    def setup_method(self):
        self.metadata = MetaData()
        self.table = Table(
            "some_table",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("big", BigInteger),
        )


class CompileTest(_TableFixture, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = _dialect()

    def test_select_all(self):
        self.assert_compile(
            select(self.table.c.id, self.table.c.name),
            "SELECT some_table.id, some_table.name FROM some_table",
        )

    def test_limit_renders(self):
        self.assert_compile(
            select(self.table.c.id).limit(5),
            "SELECT some_table.id FROM some_table LIMIT __[POSTCOMPILE_param_1]",
        )

    def test_offset_uses_rownumber(self):
        self.assert_compile(
            select(self.table.c.id).offset(10),
            '( SELECT  "Z.__db2_1" FROM ( SELECT  "Z.__db2_1", '
            '( ROW_NUMBER() OVER() ) AS "Z.__ROWNUM" FROM (  '
            'SELECT some_table.id  AS "Z.__db2_1" FROM some_table  ) AS M ) Z '
            'WHERE "Z.__ROWNUM" > __[POSTCOMPILE_param_1] )',
        )

    def test_offset_in_subquery_is_not_truncated(self):
        # Only the trailing OFFSET of each SELECT is rewritten; the outer
        # WHERE clause after the subquery must be kept intact.
        sub = select(self.table.c.id).offset(3).subquery()
        sql = str(
            select(sub.c.id).where(sub.c.id > 1).compile(dialect=self.__dialect__)
        )
        assert "OFFSET" not in sql.upper()
        assert sql.rstrip().endswith("WHERE anon_1.id > ?")

    def test_expr_offset_is_fully_stripped(self):
        stmt = select(self.table.c.id).offset(literal_column("1") + literal_column("2"))
        sql = str(stmt.compile(dialect=self.__dialect__))
        assert "OFFSET" not in sql.upper()
        assert '"Z.__ROWNUM" > 1 + 2' in sql

    def test_outer_expr_offset_with_subquery_offset(self):
        sub = select(self.table.c.id).offset(3).subquery()
        stmt = select(sub.c.id).offset(literal_column("1") + literal_column("2"))
        sql = str(stmt.compile(dialect=self.__dialect__))
        assert "OFFSET" not in sql.upper()
        assert sql.count("ROW_NUMBER()") == 2
        assert '"Z.__ROWNUM" > 1 + 2' in sql

    def test_avg_wraps_double(self):
        self.assert_compile(
            select(func.avg(self.table.c.id)),
            "SELECT AVG(DOUBLE((some_table.id))) AS avg_1 FROM some_table",
        )

    def test_mod_operator(self):
        self.assert_compile(
            select(self.table.c.id % 3),
            "SELECT mod(some_table.id, ?) AS anon_1 FROM some_table",
        )

    def test_truediv_casts_divisor(self):
        if SA_VERSION_MM < (2, 0):
            expected = "SELECT some_table.id / CAST(? AS INT) AS anon_1 FROM some_table"
        else:
            expected = (
                "SELECT some_table.id / CAST(CAST(? AS INT) AS DECIMAL(19, 6)) "
                "AS anon_1 FROM some_table"
            )
        self.assert_compile(select(self.table.c.id / 2), expected)

    @pytest.mark.skipif(SA_VERSION_MM < (2, 0), reason="'//' requires SQLAlchemy 2.0+")
    def test_floordiv_integers(self):
        self.assert_compile(
            select(self.table.c.id // self.table.c.id),
            "SELECT CAST(some_table.id AS INT) / CAST(some_table.id AS INT) "
            "AS anon_1 FROM some_table",
        )

    def test_bigint_bind_is_cast(self):
        self.assert_compile(
            select(self.table.c.id).where(self.table.c.big == 5),
            "SELECT some_table.id FROM some_table "
            "WHERE some_table.big = CAST(? AS BIGINT)",
        )

    def test_integer_bind_is_not_cast(self):
        self.assert_compile(
            select(self.table.c.name).where(self.table.c.id == 5),
            "SELECT some_table.name FROM some_table WHERE some_table.id = ?",
        )

    def test_non_ascii_literal_renders_ux_constant(self):
        self.assert_compile(
            select(literal("\u00e9")),
            "SELECT UX'00E9' AS anon_1 FROM SYSIBM.SYSDUMMY1",
            literal_binds=True,
        )

    def test_ascii_literal_unchanged(self):
        self.assert_compile(
            select(literal("abc")),
            "SELECT 'abc' AS anon_1 FROM SYSIBM.SYSDUMMY1",
            literal_binds=True,
        )


class AS400CompileTest(_TableFixture, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = _dialect("AS")

    def test_empty_in_uses_typed_null(self):
        self.assert_compile(
            select(self.table.c.id).where(self.table.c.id.in_([])),
            "SELECT some_table.id FROM some_table "
            "WHERE some_table.id IN (CAST(NULL AS INTEGER)) AND (1 != 1)",
            render_postcompile=True,
        )

    def test_empty_not_in_uses_typed_null(self):
        self.assert_compile(
            select(self.table.c.id).where(self.table.c.id.not_in([])),
            "SELECT some_table.id FROM some_table "
            "WHERE (some_table.id NOT IN (CAST(NULL AS INTEGER)) OR (1 = 1))",
            render_postcompile=True,
        )


class TypeCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = _dialect()

    @pytest.mark.parametrize(
        "type_, expected",
        [
            (String(40), "VARCHAR(40)"),
            (Integer(), "INT"),
            (BigInteger(), "BIGINT"),
            (Boolean(), "BOOLEAN"),
            (UnicodeText(), "DBCLOB(1M)"),
            (Enum("a", "bb", name="e"), "VARCHAR(2 CODEUNITS32)"),
        ],
    )
    def test_luw_types(self, type_, expected):
        self.assert_compile(type_, expected)

    @pytest.mark.parametrize(
        "type_, expected",
        [
            (Boolean(), "SMALLINT"),
            (UnicodeText(), "DBCLOB(1M) CCSID 1200"),
            (Enum("a", "bb", name="e"), "VARCHAR(2) CCSID 1208"),
            (String(40), "VARCHAR(40) CCSID 1208"),
        ],
    )
    def test_as400_types(self, type_, expected):
        self.assert_compile(type_, expected, dialect=_dialect("AS"))

    def test_zos_enum_has_no_codeunits(self):
        self.assert_compile(
            Enum("a", "bb", name="e"), "VARCHAR(2)", dialect=_dialect("DB2")
        )

    def test_server_type_is_per_dialect(self):
        # Two dialects in one process must not share the server type.
        as400, luw = _dialect("AS"), _dialect("DB2/LINUXX8664")
        assert as400.type_compiler.process(Boolean()) == "SMALLINT"
        assert luw.type_compiler.process(Boolean()) == "BOOLEAN"


class CreateTableAsTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = _dialect()

    def _ctas(self, **kw):
        schema = pytest.importorskip("sqlalchemy.schema")
        if not hasattr(schema, "CreateTableAs"):
            pytest.skip("CreateTableAs requires SQLAlchemy 2.1+")
        src = Table("src", MetaData(), Column("id", Integer))
        return schema.CreateTableAs(select(src.c.id), "dst", **kw)

    def test_create_table_as(self):
        self.assert_compile(
            self._ctas(),
            "CREATE TABLE dst AS (SELECT src.id FROM src) WITH DATA",
        )

    @pytest.mark.parametrize("option", ["temporary", "if_not_exists"])
    def test_unsupported_options_raise(self, option):
        stmt = self._ctas(**{option: True})
        with pytest.raises(exc.CompileError):
            stmt.compile(dialect=self.__dialect__)


class NameNormalizationTest(fixtures.TestBase):
    def setup_method(self):
        self.reflector = _dialect()._reflector

    @pytest.mark.parametrize(
        "name, expected, quote",
        [
            ("SOME_TABLE", "some_table", None),
            ("REF", "ref", None),
            ("MixedCase", "MixedCase", True),
            ("lower", "lower", True),
        ],
    )
    def test_normalize_name(self, name, expected, quote):
        result = self.reflector.normalize_name(name)
        assert result == expected
        assert getattr(result, "quote", None) is quote

    def test_normalize_none(self):
        assert self.reflector.normalize_name(None) is None

    @pytest.mark.parametrize(
        "name, expected",
        [
            ("some_table", "SOME_TABLE"),
            ("MixedCase", "MixedCase"),
            (quoted_name("lower", quote=True), "lower"),
        ],
    )
    def test_denormalize_name(self, name, expected):
        assert self.reflector.denormalize_name(name) == expected

    @pytest.mark.parametrize("name", ["SOME_TABLE", "MixedCase", "lower"])
    def test_round_trip(self, name):
        r = self.reflector
        assert r.denormalize_name(r.normalize_name(name)) == name


class HomogenizeNumericTest(fixtures.TestBase):
    homogenize = staticmethod(DB2Dialect_ibm_db._homogenize_numeric_columns)

    def test_decimal_column_uses_decimal(self):
        result = self.homogenize([(1,), (1.5,)], [Numeric(10, 2)])
        assert result == [(decimal.Decimal("1"),), (decimal.Decimal("1.5"),)]

    def test_float_column_uses_float(self):
        result = self.homogenize([(1,), (1.5,)], [Float()])
        assert result == [(1.0,), (1.5,)]
        assert all(isinstance(row[0], float) for row in result)

    def test_non_numeric_column_is_untouched(self):
        params = [(1,), (1.5,)]
        assert self.homogenize(params, [Integer()]) is params

    def test_uniform_column_is_untouched(self):
        params = [(1, "a"), (2, "b")]
        assert self.homogenize(params, [Numeric(), String()]) is params

    def test_unknown_types_fall_back_to_all_columns(self):
        assert self.homogenize([(1,), (1.5,)]) == [(1.0,), (1.5,)]
