#!/usr/bin/env python
"""Convenience wrapper to run the ibm_db_sa test suite via pytest.

This replaces the old nose/`sqlalchemy.testing.runner` entry point.
It simply forwards all command-line arguments to pytest, so you can do::

    python run_tests.py test/test_dialect.py -v
    python run_tests.py --dburi "db2+ibm_db://user:pass@host:50000/DB"

Dialect registration and the SQLAlchemy pytest plugin are wired up by
``conftest.py`` at the repository root.
"""

import sys

import pytest

if __name__ == "__main__":
    args = sys.argv[1:] or ["test/test_dialect.py"]
    raise SystemExit(pytest.main(args))

