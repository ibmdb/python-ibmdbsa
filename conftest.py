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

or select the named URL from ``setup.cfg`` with ``pytest --db db2``.
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

import os  # noqa: E402

import pytest  # noqa: E402
from sqlalchemy.testing import config as _sa_config  # noqa: E402
from sqlalchemy.testing.plugin import pytestplugin as _sa_plugin  # noqa: E402


# Db2 DECIMAL precision is capped at 31 digits; these upstream cases need
# NUMERIC(38, ...) (SQL0405N / SQL0604N) and have no requirement to gate them.
_DB2_DECIMAL_LIMIT_SKIP_IDS = (
    "test_truediv_divisor[numeric_38_digit_divisor",
    "test_truediv_divisor[numeric_tiny_divisor",
)

# The compliance suite is only meaningful against Db2, not the sqlite default.
_DB2_ONLY_FILES = ("test_suite.py",)


def _is_db2_backend():
    db = getattr(_sa_config, "db", None)
    return db is not None and db.dialect.name == "ibm_db_sa"


def pytest_collection_modifyitems(session, config, items):
    # Run SQLAlchemy's hook first: it expands __backend__ classes and applies
    # __only_on__ / __requires__ gating.
    _sa_plugin.pytest_collection_modifyitems(session, config, items)

    skip_db2_decimal = pytest.mark.skip(
        reason="Db2 DECIMAL precision maximum is 31 digits (SQL0405N / SQL0604N)."
    )
    skip_not_db2 = pytest.mark.skip(
        reason="Requires a Db2 database; run with --db db2 or --dburi <db2-url>."
    )
    is_db2 = _is_db2_backend()
    for item in items:
        if any(tag in item.nodeid for tag in _DB2_DECIMAL_LIMIT_SKIP_IDS):
            item.add_marker(skip_db2_decimal)
        if not is_db2 and os.path.basename(str(item.fspath)) in _DB2_ONLY_FILES:
            item.add_marker(skip_not_db2)

