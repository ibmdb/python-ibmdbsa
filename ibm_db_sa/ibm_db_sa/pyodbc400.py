# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008.                                      |
# +--------------------------------------------------------------------------+
# | This module complies with SQLAlchemy 0.4 and is                          |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Jaimy Azle                                                      |
# | Version: 0.2.x                                                           |
# +--------------------------------------------------------------------------+
from decimal import Decimal as _python_Decimal
from sqlalchemy import sql, util
from sqlalchemy import types as sa_types
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from ibm_db_sa import base as ibm_base
from ibm_db_sa import base400 as ibm_base400

__all__ = (
'IBM_DBPyODBCDialect', 'dialect'
)

class IBM_DB400PyODBCDate(sa_types.Date):

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            else:
                return value.date()
        return process


class IBM_DB400PyODBCNumeric(sa_types.Numeric):

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            def process(value):
                if isinstance(value, _python_Decimal):
                    return value
                elif (value == None):
                    return None
                else:
                    return _python_Decimal(str(value))
        else:
            def process(value):
                if isinstance(value, _python_Decimal):
                    return float(value)
                else:
                    return value
        return process


class IBM_DB400PyODBCDialect(PyODBCConnector, ibm_base400.IBM_DB400Dialect):

  supports_unicode_statements   = False
  supports_sane_rowcount        = False
  supports_sane_multi_rowcount  = False
  supports_native_decimal       = True
  supports_char_length          = True
  supports_native_decimal       = False

  pyodbc_driver_name = "IBM DB2 ODBC DRIVER"
  colspecs = util.update_copy(
      ibm_base.IBM_DBDialect.colspecs,
      {
            sa_types.Date : IBM_DB400PyODBCDate,
            sa_types.Numeric: IBM_DB400PyODBCNumeric
      }
  )

  def __init__(self, use_ansiquotes=None, **kwargs):
    kwargs.setdefault('convert_unicode', True)
    super(IBM_DB400PyODBCDialect, self).__init__(**kwargs)
    self.paramstyle = IBM_DB400PyODBCDialect.dbapi().paramstyle

dialect = IBM_DB400PyODBCDialect