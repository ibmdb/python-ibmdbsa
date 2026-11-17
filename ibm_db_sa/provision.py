# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                     |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008, 2016.                                |
# +--------------------------------------------------------------------------+
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# +--------------------------------------------------------------------------+
"""Test-only provisioning hooks for the ibm_db_sa (Db2) dialect.

Imported only by SQLAlchemy's testing framework, so it has no effect at
runtime. It registers ``temp_table_keyword_args``, which has no default
implementation and otherwise raises ``NotImplementedError`` during
temp-table reflection fixtures. The import is guarded to stay a no-op on
SQLAlchemy versions where the symbol is missing.
"""

try:
    from sqlalchemy.testing.provision import temp_table_keyword_args
except Exception:  # pragma: no cover - SQLAlchemy without the provisioning hook
    temp_table_keyword_args = None


if temp_table_keyword_args is not None:

    # Register under both "ibm_db_sa" (dialect name) and "db2" (conftest alias)
    # so provisioning resolves regardless of the active backend name.
    @temp_table_keyword_args.for_db("ibm_db_sa", "db2")
    def _db2_temp_table_keyword_args(cfg, eng):
        """Return the ``GLOBAL TEMPORARY`` prefix so SQLAlchemy emits valid
        Db2 ``CREATE GLOBAL TEMPORARY TABLE`` DDL for temp-table fixtures.
        """
        return {"prefixes": ["GLOBAL TEMPORARY"]}

