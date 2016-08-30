# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008, 2016.                                |
# +--------------------------------------------------------------------------+
# | This module complies with SQLAlchemy 0.8 and is                          |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Alex Pitigoi, Abhigyan Agrawal, Rahul Priyadarshi               |
# | Contributors: Jaimy Azle, Mike Bayer                                     |
# +--------------------------------------------------------------------------+

__version__ = '0.3.3'

from . import ibm_db, pyodbc, base, zxjdbc


# default dialect
base.dialect = ibm_db.dialect

from .base import \
    BIGINT, BLOB, CHAR, CLOB, DATE, DATETIME, \
    DECIMAL, DOUBLE, DECIMAL,\
    GRAPHIC, INTEGER, INTEGER, LONGVARCHAR, \
    NUMERIC, SMALLINT, REAL, TIME, TIMESTAMP, \
    VARCHAR, VARGRAPHIC, dialect

#__all__ = (
    # TODO: (put types here)
#    'dialect'
#)
