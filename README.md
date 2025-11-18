IBM_DB_SA
=========

The IBM_DB_SA adapter provides the Python/SQLAlchemy interface to IBM Data Servers.

Version
--------
0.4.3 (2025/11/18)

Prerequisites
--------------
1. Install Python 3.8 or newer versions or Jython 2.7.x .
2. SQLAlchemy version between 0.7.3 - 2.0.x.
3. IBM_DB driver and IBM_DB_DBI wrapper 1.0.1 or higher.
```
   Install ibm_db driver with below commands:
	    Linux and Windows: 
	   	   pip install ibm_db
	    Mac:
		   pip install --no-cache-dir ibm_db
```

Install and Configuration
=========================
The IBM_DB_SA Python Egg component (.egg) can be installed using the standard setuptools provided by the Python Easy Install through Python Entreprise 
Application Kit community portal:
  http://peak.telecommunity.com/DevCenter/EasyInstall

Please follow the steps provided to Install "Easy Install" in the link above and follow up with these additional steps to install IBM_DB_SA:

  1. To install IBM_DB_SA from pypi repository(pypi.python.org):
    Windows:
      > pip install ibm_db_sa
    Linux/Unix:
      $ sudo pip install ibm_db_sa
  
  2. To install IBM_DB_SA egg component from the downloaded .egg file
    Windows:
      > easy_install ibm_db_sa-x.x.x-pyx.x.egg
    Linux/Unix:
      $ sudo easy_install ibm_db_sa-x.x.x-pyx.x.egg
  
  3. To install IBM_DB_SA from source
    Standard Python setup should be used::
        python setup.py install

  4. In case you want to control the sqlalchemy version use the following commands
  ```
    Install SQLAlchemy supported version:
       pip install sqlalchemy==1.3.23
       pip install ibm_db_sa
  ```

  5. IBM_DB_SA is now available as part of conda-forge channel.
    Install Instruction from conda-forge channel
  ```
    conda install -c conda-forge ibm_db
    conda install -c conda-forge ibm_db_sa
  ```
  #### PLATFORMS SUPPORTED THROUGH CONDA FORGE ####
    1. WINDOWS
    2. MAC
    4. LINUX

Connecting
----------
A TCP/IP connection can be specified as the following::
```
	from sqlalchemy import create_engine

	e = create_engine("db2+ibm_db://user:pass@host[:port]/database")
```

For a local socket connection, exclude the "host" and "port" portions::

```
	from sqlalchemy import create_engine

	e = create_engine("db2+ibm_db://user:pass@/database")
```

Supported Databases
-------------------
- IBM DB2 Universal Database for Linux/Unix/Windows versions 9.7 onwards 
- IBM Db2 on Cloud
- IBM Db2 on ZOS
- IBM Db2 on Iseries

Note
-------------------------------------------------------------
By default, all tables and schemas stored in a Db2 database are created
using capital letters only.
However, if you have a table name in lowercase letters, you can still reference
it by enclosing the name in single quotes inside double quotes.
For example
```
	if users table is in small letter inside database
	So, you can use single quotes "'users'".
	If you will not use single quotes such as "users", it will
	be refered as "USERS".

	metadata = sqlalchemy.MetaData(schema="schema1")
	table = sqlalchemy.Table("'users'", metadata, autoload_with=engine)

```

Known Limitations in ibm_db_sa adapter for DB2 databases
-------------------------------------------------------------
1) Non-standard SQL queries are not supported. e.g. "SELECT ? FROM TAB1"
2) For updations involving primary/foreign key references, the entries should be made in correct order. Integrity check is always on and thus the primary keys referenced by the foreign keys in the referencing tables should always exist in the parent table.
3) Unique key which contains nullable column not supported
4) UPDATE CASCADE for foreign keys not supported
5) DEFERRABLE INITIALLY deferred not supported
6) Subquery in ON clause of LEFT OUTER JOIN not supported
7) PyODBC and Jython/zxjdbc support is experimental


Credits
-------
ibm_db_sa for SQLAlchemy was first produced by IBM Inc., targeting version 0.4.
The library was ported for version 0.6 and 0.7 by Jaimy Azle.
Port for version 0.8 and modernization of test suite by Mike Bayer.

Contributing to IBM_DB_SA python project
----------------------------------------
See `CONTRIBUTING
<https://github.com/ibmdb/python-ibmdbsa/tree/master/ibm_db_sa/contributing/CONTRIBUTING.md>`_.

```
The developer sign-off should include the reference to the DCO in remarks(example below):
DCO 1.1 Signed-off-by: Random J Developer <random@developer.org>
```

