#!/usr/bin/env python

from setuptools import setup
import os
import re


v = open(os.path.join(os.path.dirname(__file__), 'ibm_db_sa', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.md')
if 'USE_PYODBC' in os.environ and os.environ['USE_PYODBC'] == '1':
    require = ['sqlalchemy>=0.7.3']
else:
    require = ['sqlalchemy>=0.7.3','ibm_db>=2.0.0']
    

setup(
         name='ibm_db_sa',
         version=VERSION,
         license = "Apache-2.0",
         license_files = ("LICENSE",),
         description='SQLAlchemy support for IBM Data Servers',
         author='IBM Application Development Team',
         author_email='balram.choudhary@ibm.com',
         url='http://pypi.python.org/pypi/ibm_db_sa/',
         download_url='https://github.com/ibmdb/python-ibmdbsa',
         keywords='sqlalchemy database interface IBM Data Servers Db2',
         long_description_content_type='text/markdown',
         classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Operating System :: OS Independent',
            'Topic :: Database :: Front-Ends'
        ],
         long_description=open(readme).read(),
         platforms='All',
         install_requires= require,
         packages=['ibm_db_sa'],
        entry_points={
         'sqlalchemy.dialects': [
                     'db2as400=ibm_db_sa.ibm_db_as400:AS400Dialect',
                     'db2=ibm_db_sa.ibm_db:DB2Dialect_ibm_db',
                     'db2.ibm_db=ibm_db_sa.ibm_db:DB2Dialect_ibm_db',
                     'db2.zxjdbc=ibm_db_sa.zxjdbc:DB2Dialect_zxjdbc',
                     'db2.pyodbc=ibm_db_sa.pyodbc:DB2Dialect_pyodbc',
                     'db2.zxjdbc400=ibm_db_sa.zxjdbc:AS400Dialect_zxjdbc',
                     'db2.pyodbc400=ibm_db_sa.pyodbc:AS400Dialect_pyodbc',

                     # older "ibm_db_sa://" style for backwards
                     # compatibility
                     'ibm_db_sa=ibm_db_sa.ibm_db:DB2Dialect_ibm_db',
                     'ibm_db_sa.zxjdbc=ibm_db_sa.zxjdbc:DB2Dialect_zxjdbc',
                     'ibm_db_sa.pyodbc=ibm_db_sa.pyodbc:DB2Dialect_pyodbc',
                     'ibm_db_sa.zxjdbc400=ibm_db_sa.zxjdbc:AS400Dialect_zxjdbc',
                     'ibm_db_sa.pyodbc400=ibm_db_sa.pyodbc:AS400Dialect_pyodbc',
                    ]
       },
       zip_safe=False,
       extras_require={
           "dev": ["nose>=0.11"]
       }
     )
