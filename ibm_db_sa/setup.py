#!/usr/bin/env python

from sys import platform
from setuptools import setup, find_packages
from distutils.core import setup

PACKAGE = 'ibm_db_sa'
VERSION = '0.2.1'
LICENSE = 'Apache License 2.0'

IS_JYTHON = platform.startswith("java")
setup( name    = PACKAGE,
       version = VERSION,
       license = LICENSE,
       description  = 'SQLAlchemy support for IBM Data Servers',
       author       = 'IBM Application Development Team',
       author_email = 'opendev@us.ibm.com',
       url          = 'http://pypi.python.org/pypi/ibm_db/',
       download_url = 'http://code.google.com/p/ibm-db/downloads/list',
       keywords     = 'sqlalchemy database interface IBM Data Servers DB2 Informix IDS',
       classifiers  = ['Development Status :: 4 - Beta',
                      'Intended Audience :: Developers',
                      'License :: OSI Approved :: Apache License 2.0',
                      'Operating System :: OS Independent',
                      'Topic :: Databases :: Front-end, middle-tier'],
       long_description = '''
                      IBM_DB_SA implementats the SQLAlchemy version 0.7.0 specification
                      in support of IBM Data Servers: DB2 8 and 9, Informix IDS 11''',
       platforms        = 'All',
       install_requires = ['sqlalchemy>=0.6.0'] if IS_JYTHON else ['ibm_db>=1.0.5', 'sqlalchemy>=0.6.0'],
       dependency_links = ['http://pypi.python.org/pypi/SQLAlchemy/'] if IS_JYTHON else ['http://pypi.python.org/pypi/ibm_db/', 'http://pypi.python.org/pypi/SQLAlchemy/'],
       packages     = find_packages(),
       data_files   = [ ('', ['./README']),
                        ('', ['./CHANGES']),
                        ('', ['./LICENSE']) ],
       entry_points = {
         'sqlalchemy.dialects': ['ibm_db_sa = ibm_db_sa.base:dialect',
                                 'ibm_db_sa.zxjdbc = ibm_db_sa.zxjdbc:dialect',
                                 'ibm_db_sa.pyodbc = ibm_db_sa.pyodbc:dialect',
                                 'ibm_db_sa.zxjdbc400 = ibm_db_sa.zxjdbc400:dialect',
                                 'ibm_db_sa.pyodbc400 = ibm_db_sa.pyodbc400:dialect',
                                 'ibm_db_sa_zxjdbc = ibm_db_sa.zxjdbc:dialect',
                                 'ibm_db_sa_pyodbc = ibm_db_sa.pyodbc:dialect',
                                 'ibm_db_sa_zxjdbc400 = ibm_db_sa.zxjdbc400:dialect',
                                 'ibm_db_sa_pyodbc400 = ibm_db_sa.pyodbc400:dialect',]
       },
       include_package_data = True,
       zip_safe             = False,
       tests_require=['nose >= 0.11'],
       test_suite="sqla_nose.py",
     )
