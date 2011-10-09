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

__all__ = (
'IBM_DBPyODBCDialect', 'dialect'
)

class IBM_DBPyODBCDate(sa_types.Date):

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            else:
                return value.date()
        return process


class IBM_DBPyODBCNumeric(sa_types.Numeric):

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

class IBM_DBPyODBCDialect(PyODBCConnector, ibm_base.IBM_DBDialect):

  supports_unicode_statements   = False
  supports_native_decimal       = True
  supports_char_length          = True
  supports_native_decimal       = False

  pyodbc_driver_name = "IBM DB2 ODBC DRIVER"
  colspecs = util.update_copy(
      ibm_base.IBM_DBDialect.colspecs,
      {
          sa_types.Date : IBM_DBPyODBCDate,
          sa_types.Numeric: IBM_DBPyODBCNumeric
      }
  )

  def __init__(self, use_ansiquotes=None, **kwargs):
    kwargs.setdefault('convert_unicode', True)
    super(IBM_DBPyODBCDialect, self).__init__(**kwargs)
    self.paramstyle = IBM_DBPyODBCDialect.dbapi().paramstyle

  def create_connect_args(self, url):
    opts = url.translate_connect_args(username='user')
    opts.update(url.query)

    keys = opts
    query = url.query

    connect_args = {}
    for param in ('ansi', 'unicode_results', 'autocommit'):
      if param in keys:
        connect_args[param] = asbool(keys.pop(param))

    if 'odbc_connect' in keys:
      connectors = [urllib.unquote_plus(keys.pop('odbc_connect'))]
    else:
      dsn_connection = 'dsn' in keys or \
                      ('host' in keys and 'database' not in keys)
      if dsn_connection:
        connectors= ['dsn=%s' % (keys.pop('host', '') or \
                      keys.pop('dsn', ''))]
      else:
        port = ''
        if 'port' in keys and not 'port' in query:
          port = ',%d' % int(keys.pop('port'))

        database = keys.pop('database', '')
        db_alias = database
        if 'alias' in keys and not 'alias' in query:
          db_alias = keys.pop('alias')

        connectors = ["driver={%s}" %
                        keys.pop('driver', self.pyodbc_driver_name),
                      'server=%s%s' % (keys.pop('host', ''), port),
                      'database=%s' % database,
                      'dbalias=%s' % db_alias]

        user = keys.pop("user", None)
        if user:
          connectors.append("uid=%s" % user)
          connectors.append("pwd=%s" % keys.pop('password', ''))
        else:
          connectors.append("trusted_connection=yes")

        # if set to 'yes', the odbc layer will try to automagically
        # convert textual data from your database encoding to your
        # client encoding.  this should obviously be set to 'no' if
        # you query a cp1253 encoded database from a latin1 client...
        if 'odbc_autotranslate' in keys:
          connectors.append("autotranslate=%s" %
                            keys.pop("odbc_autotranslate"))

        connectors.extend(['%s=%s' % (k,v) for k,v in keys.iteritems()])
    return [[";".join (connectors)], connect_args]

dialect = IBM_DBPyODBCDialect