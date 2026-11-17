"""pytest bootstrap for the ibm_db_sa dialect test suite.

This is the entry point used by SQLAlchemy's testing framework for
third-party dialects (see SQLAlchemy's README.unittests.rst).

It performs two jobs:

1. Registers the Db2 dialect entry points so they resolve even when
   running directly from a source checkout (before ``pip install``).
2. Loads SQLAlchemy's pytest plugin, which provides the ``--dburi``
   option, the ``config`` object, database fixtures and the machinery
   that runs ``sqlalchemy.testing.suite`` against this dialect.

Run the tests with, for example::

    pytest --dburi "db2+ibm_db://user:pass@host:50000/DBNAME"

or configure the URL under ``[db] default`` in ``setup.cfg`` and simply
run ``pytest``.
"""

from sqlalchemy.dialects import registry

# Make the dialect importable by name from a source checkout.
#
# NOTE on the extra "doubled" keys below: SQLAlchemy's test provisioning
# (provision.generate_driver_url) rebuilds the URL as
# "<backend>+<get_driver_name()>" and then looks up that entry point.
# For a bare "db2://" URL that becomes "db2+db2" -> "db2.db2", and for the
# legacy "ibm_db_sa://" URL it becomes "ibm_db_sa+ibm_db_sa" ->
# "ibm_db_sa.ibm_db_sa". We register those aliases so ALL of these forms
# work as --dburi values:
#     db2+ibm_db://...   (canonical / recommended)
#     db2://...          (bare)
#     ibm_db_sa://...    (legacy)
registry.register("db2", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("db2.db2", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("db2.ibm_db", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("db2.ibm_db_sa", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("db2.pyodbc", "ibm_db_sa.pyodbc", "DB2Dialect_pyodbc")
registry.register("db2.pyodbc400", "ibm_db_sa.pyodbc", "AS400Dialect_pyodbc")

# Backwards-compatible "ibm_db_sa://" style names.
registry.register("ibm_db_sa", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("ibm_db_sa.ibm_db", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("ibm_db_sa.ibm_db_sa", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("ibm_db_sa.pyodbc", "ibm_db_sa.pyodbc", "DB2Dialect_pyodbc")

# Load the SQLAlchemy pytest plugin. This MUST happen after the registry
# calls above. It wires up --dburi, the `config` fixture, and the suite.
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402,F401,F403

# Register the Db2 test-provisioning hooks (e.g. temp_table_keyword_args).
# Must happen after the plugin import above so the provisioning registry
# exists. Import for its registration side effects only.
import ibm_db_sa.provision  # noqa: E402,F401

import pytest  # noqa: E402


# --- Db2 hard-limit skips that the upstream suite does not gate ------------
#
# Db2's maximum DECIMAL precision is 31 digits. A few SQLAlchemy compliance
# cases deliberately exercise 38-digit DECIMAL arithmetic to pin down
# true-division behaviour on other backends:
#
#   TrueDivTest::test_truediv_divisor[numeric_38_digit_divisor-*]
#       uses NUMERIC(38, 0) with a 38-digit literal; Db2 rejects it with
#       SQL0405N ("numeric literal ... is out of range").
#   TrueDivTest::test_truediv_divisor[numeric_tiny_divisor-*]
#       uses NUMERIC(38, 20); Db2 rejects the type itself with SQL0604N
#       ("DECIMAL(38, 20) is not valid").
#
# Unlike the sibling ``numeric_many_decimals`` case (which the test marks
# ``.as_skips()`` via precision_numerics_many_significant_digits), these two
# combinations carry no requirement, so they cannot be turned off from
# requirements.py. They are a genuine Db2 limitation, not a dialect bug, so we
# skip them here by node id. The 31-digit-and-under variants of the same test
# (float, numeric_whole_number) still run and must keep passing.
_DB2_DECIMAL_LIMIT_SKIP_IDS = (
    "test_truediv_divisor[numeric_38_digit_divisor",
    "test_truediv_divisor[numeric_tiny_divisor",
)


def pytest_collection_modifyitems(config, items):  # noqa: D401
    """Skip the Db2-unsupported 38-digit DECIMAL true-division cases."""
    skip_db2_decimal = pytest.mark.skip(
        reason=(
            "Db2 DECIMAL precision maximum is 31 digits; this case requires "
            "NUMERIC(38, ...) which Db2 rejects (SQL0405N / SQL0604N)."
        )
    )
    for item in items:
        if any(tag in item.nodeid for tag in _DB2_DECIMAL_LIMIT_SKIP_IDS):
            item.add_marker(skip_db2_decimal)

