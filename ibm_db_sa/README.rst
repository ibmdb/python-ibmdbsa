IBM_DB_SA
=========

The IBM_DB_SA adapter provides the Python/SQLAlchemy interface to IBM
Data Servers.

Version
--------

0.3.0 (2013/01/26)

This version is all new for version 0.8 of SQLAlchemy and will also work
with version 0.7.

Supported Environments
----------------------

- Python 2.5 or greater
- SQLAlchemy 0.7.3 and above
- the ibm_db_dbi DBAPI library.

Not Supported / Not Tested
---------------------------

- Python 3 has not yet been tested.

- pyodbc support has not been tested.

- zxjdbc/Jython support is not fully implemented.

Installation
------------

Standard Python setup should be used::

  python setup.py install

Connecting
----------

A TCP/IP connection can be specified as the following::

	from sqlalchemy import create_engine

	e = create_engine("db2+ibm_db://user:pass@host[:port]/database")

For a local socket connection, exclude the "host" and "port" portions::

	from sqlalchemy import create_engine

	e = create_engine("db2+ibm_db://user:pass@/database")

Supported Databases
-------------------

- IBM DB2 Universal Database for Linux/Unix/Windows versions 9.7 onwards
- Remote connections to i5/OS (iSeries)
- Remote connections to z/OS (DB2 UDB for zOS), only by default ibm_db drivers

Credits
-------

ibm_db_sa for SQLAlchemy was first produced by IBM Inc., targeting
version 0.4.   The library was ported for version 0.6 and 0.7 by Jaimy Azle.
Port for version 0.8 and modernization of test suite by Mike Bayer.

