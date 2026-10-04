"""DB2 LUW reflection, binary binds, XML and DECFLOAT results with ibm_db."""

from decimal import Decimal

from sqlalchemy import (
    BINARY,
    LargeBinary,
    MetaData,
    Table,
    VARBINARY,
    inspect,
    select,
)
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from ibm_db_sa import base
from ibm_db_sa.base import XML
from ibm_db_sa.ibm_db import DB2Dialect_ibm_db
from ibm_db_sa.reflection import DB2Reflector


class TestProcessors(fixtures.TestBase):
    def test_binary_binds_bytes(self):
        dialect = DB2Dialect_ibm_db()
        for type_ in (LargeBinary(), BINARY(4), VARBINARY(8)):
            processor = type_.dialect_impl(dialect).bind_processor(dialect)
            bound = processor(memoryview(b"\x00\xff"))
            eq_(bound, b"\x00\xff")
            eq_(type(bound), bytes)
            eq_(processor(None), None)

    def test_xml_drops_cli_serialization_prefix(self):
        dialect = DB2Dialect_ibm_db()
        processor = XML().dialect_impl(dialect).result_processor(dialect, None)
        prefix = '\ufeff<?xml version="1.0" encoding="UTF-16" ?>'
        eq_(processor(prefix + "<a>1</a>"), "<a>1</a>")
        other = '<?xml version="1.0" encoding="UTF-8"?><a/>'
        eq_(processor(other), other)
        eq_(processor(None), None)

    def test_decfloat_processors(self):
        dialect = DB2Dialect_ibm_db()
        impl = base.DECFLOAT(34).dialect_impl(dialect)
        eq_(type(impl), base.DECFLOAT)
        value = "3.141592653589793238462643383279502"
        result = impl.result_processor(dialect, None)(value)
        eq_(result, Decimal(value))
        eq_(type(result), Decimal)
        eq_(impl.bind_processor(dialect)(Decimal(value)), value)
        eq_(str(base.DECFLOAT(16).compile(dialect=dialect)), "DECFLOAT(16)")

    def test_is_disconnect_covers_base_error(self):
        dialect = DB2Dialect_ibm_db()
        dbapi = dialect.dbapi = DB2Dialect_ibm_db.import_dbapi()
        lost = "[IBM][CLI Driver] SQL30081N  A communication error has been detected."
        eq_(dialect.is_disconnect(dbapi.Error(lost), None, None), True)
        eq_(dialect.is_disconnect(dbapi.Error("SQL0204N"), None, None), False)

    def test_has_sequence_accepts_inspector_keywords(self):
        dialect = DB2Dialect_ibm_db()
        reflector = DB2Reflector(dialect)

        class Result:
            def first(self):
                return ("SEQ1",)

        class Connection:
            def execute(self, statement):
                return Result()

        eq_(
            reflector.has_sequence(Connection(), "seq1", schema="s", info_cache={}),
            True,
        )


class TestLUWReflection(fixtures.TestBase):
    __only_on__ = "ibm_db_sa+ibm_db_sa"
    __backend__ = True

    DDL = [
        "CREATE SCHEMA IBMSA_A",
        "CREATE SCHEMA IBMSA_B",
        "CREATE TABLE IBMSA_A.PARENT (A INT NOT NULL, B INT NOT NULL, "
        "CONSTRAINT PK_PARENT PRIMARY KEY (B, A))",
        'CREATE TABLE IBMSA_A.CHILD (ID INT NOT NULL, PA INT, PB INT, "AMT$X" INT '
        "NOT NULL, U1 INT NOT NULL, U2 INT NOT NULL, X XML, "
        'CONSTRAINT PK_CHILD PRIMARY KEY (ID, "AMT$X"), '
        "CONSTRAINT FK_PARENT FOREIGN KEY (PB, PA) REFERENCES IBMSA_A.PARENT (B, A) "
        "ON DELETE CASCADE, CONSTRAINT UQ_C UNIQUE (U2, U1), "
        "CONSTRAINT CK_U CHECK (U1 >= 0))",
        "CREATE TABLE IBMSA_B.PARENT (A INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE IBMSA_B.CHILD (ID INT NOT NULL PRIMARY KEY, PA INT NOT NULL, "
        "CONSTRAINT FK_OTHER FOREIGN KEY (PA) REFERENCES IBMSA_B.PARENT (A), "
        "CONSTRAINT UQ_C UNIQUE (PA))",
        'CREATE INDEX IBMSA_A.IX_CHILD ON IBMSA_A.CHILD (U1 DESC, "AMT$X" ASC)',
        "CREATE SEQUENCE IBMSA_A.SEQ1",
        "CREATE TABLE IBMSA_A.TYPES (ID INT NOT NULL, DF DECFLOAT(16), "
        "BI BINARY(4), VB VARBINARY(8), CB CHAR(4) FOR BIT DATA, BL BLOB(1K))",
    ]
    DROP = [
        "DROP TABLE IBMSA_A.CHILD",
        "DROP TABLE IBMSA_A.PARENT",
        "DROP TABLE IBMSA_A.TYPES",
        "DROP TABLE IBMSA_B.CHILD",
        "DROP TABLE IBMSA_B.PARENT",
        "DROP SEQUENCE IBMSA_A.SEQ1",
        "DROP SCHEMA IBMSA_A RESTRICT",
        "DROP SCHEMA IBMSA_B RESTRICT",
    ]

    @classmethod
    def setup_test_class(cls):
        from sqlalchemy.testing import config

        with config.db.begin() as conn:
            for statement in cls.DDL:
                conn.exec_driver_sql(statement)

    @classmethod
    def teardown_test_class(cls):
        from sqlalchemy.testing import config

        for statement in cls.DROP:
            with config.db.begin() as conn:
                conn.exec_driver_sql(statement)

    def test_foreign_keys_are_schema_scoped_and_ordered(self, connection):
        inspector = inspect(connection)
        eq_(
            inspector.get_foreign_keys("child", schema="ibmsa_a"),
            [
                {
                    "name": "fk_parent",
                    "constrained_columns": ["pb", "pa"],
                    "referred_schema": "ibmsa_a",
                    "referred_table": "parent",
                    "referred_columns": ["b", "a"],
                    "options": {"ondelete": "CASCADE"},
                }
            ],
        )
        eq_(
            inspector.get_foreign_keys("child", schema="ibmsa_b"),
            [
                {
                    "name": "fk_other",
                    "constrained_columns": ["pa"],
                    "referred_schema": "ibmsa_b",
                    "referred_table": "parent",
                    "referred_columns": ["a"],
                    "options": {},
                }
            ],
        )

    def test_unique_constraints_are_table_scoped_and_ordered(self, connection):
        inspector = inspect(connection)
        eq_(
            inspector.get_unique_constraints("child", schema="ibmsa_a"),
            [{"name": "uq_c", "column_names": ["u2", "u1"]}],
        )
        eq_(
            inspector.get_unique_constraints("child", schema="ibmsa_b"),
            [{"name": "uq_c", "column_names": ["pa"]}],
        )

    def test_primary_key_constraint_name_and_columns(self, connection):
        eq_(
            inspect(connection).get_pk_constraint("child", schema="ibmsa_a"),
            {"constrained_columns": ["id", "amt$x"], "name": "pk_child"},
        )

    def test_indexes(self, connection):
        eq_(
            inspect(connection).get_indexes("child", schema="ibmsa_a"),
            [
                {
                    "name": "ix_child",
                    "column_names": ["u1", "amt$x"],
                    "unique": False,
                    "column_sorting": {"u1": ("desc",)},
                }
            ],
        )

    def test_check_constraints(self, connection):
        eq_(
            inspect(connection).get_check_constraints("child", schema="ibmsa_a"),
            [{"name": "ck_u", "sqltext": "U1 >= 0"}],
        )

    def test_has_sequence(self, connection):
        inspector = inspect(connection)
        eq_(inspector.has_sequence("seq1", schema="ibmsa_a"), True)
        eq_(inspector.has_sequence("nope", schema="ibmsa_a"), False)

    def test_column_types_match_values(self, connection):
        columns = {
            c["name"]: c["type"]
            for c in inspect(connection).get_columns("types", schema="ibmsa_a")
        }
        eq_(repr(columns["df"]), "DECFLOAT(precision=16)")
        eq_(repr(columns["bi"]), "BINARY(length=4)")
        eq_(repr(columns["vb"]), "VARBINARY(length=8)")
        eq_(repr(columns["cb"]), "BINARY(length=4)")
        table = Table("types", MetaData(), schema="ibmsa_a", autoload_with=connection)
        rows = [
            dict(
                id=1,
                df=Decimal("1.234567890123456"),
                bi=b"abcd",
                vb=b"\x01",
                cb=b"\xde\xad\xbe\xef",
                bl=b"\x00\xff",
            ),
            dict(
                id=2, df=Decimal("-0.5"), bi=b"wxyz", vb=b"", cb=b"\x00" * 4, bl=b"\x10"
            ),
            dict(id=3, df=None, bi=None, vb=None, cb=None, bl=None),
        ]
        connection.execute(table.insert(), rows)
        actual = [
            dict(r._mapping)
            for r in connection.execute(select(table).order_by(table.c.id))
        ]
        eq_(actual, rows)
        eq_([type(r["df"]) for r in actual], [Decimal, Decimal, type(None)])

    def test_xml_value(self, connection):
        table = Table("child", MetaData(), schema="ibmsa_a", autoload_with=connection)
        connection.execute(
            table.insert(), [{"id": 1, "amt$x": 0, "u1": 1, "u2": 1, "x": "<a>1</a>"}]
        )
        eq_(connection.execute(select(table.c.x)).scalar(), "<a>1</a>")
