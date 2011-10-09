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
from sqlalchemy.engine.base import FullyBufferedResultProxy, ResultProxy
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from ibm_db_sa import base as ibm_base
from ibm_db_sa import base400 as ibm_base400

__all__ = (
'IBM_DB400ZxJDBCDialect', 'dialect'
)

SQLException = zxJDBC = None

class IBM_DB400ZxJDBCDate(sa_types.Date):

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            else:
                return value.date()
        return process


class IBM_DB400ZxJDBCNumeric(sa_types.Numeric):

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

class ReturningResultProxy(FullyBufferedResultProxy):

    """ResultProxy backed by the RETURNING ResultSet results."""

    def __init__(self, context, returning_row):
        self._returning_row = returning_row
        super(ReturningResultProxy, self).__init__(context)

    def _cursor_description(self):
        ret = []
        for c in self.context.compiled.returning_cols:
            if hasattr(c, 'name'):
                ret.append((c.name, c.type))
            else:
                ret.append((c.anon_label, c.type))
        return ret

    def _buffer_rows(self):
        return [self._returning_row]

class ReturningParam(object):

    """A bindparam value representing a RETURNING parameter.

    Specially handled by IBM_DB2DataHandler.
    """

    def __init__(self, type):
        self.type = type

    def __eq__(self, other):
        if isinstance(other, ReturningParam):
            return self.type == other.type
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, ReturningParam):
            return self.type != other.type
        return NotImplemented

    def __repr__(self):
        kls = self.__class__
        return '<%s.%s object at 0x%x type=%s>' % (kls.__module__, kls.__name__, id(self),
                                                   self.type)

class IBM_DB400ZxJDBCExecutionContext(ibm_base.IBM_DBExecutionContext):

    def pre_exec(self):
        if hasattr(self.compiled, 'returning_parameters'):
            self.statement = self.cursor.prepare(self.statement)

    def get_result_proxy(self):
        if hasattr(self.compiled, 'returning_parameters'):
            rrs = None
            try:
                try:
                    rrs = self.statement.__statement__.getReturnResultSet()
                    rrs.next()
                except SQLException, sqle:
                    msg = '%s [SQLCode: %d]' % (sqle.getMessage(), sqle.getErrorCode())
                    if sqle.getSQLState() is not None:
                        msg += ' [SQLState: %s]' % sqle.getSQLState()
                    raise zxJDBC.Error(msg)
                else:
                    row = tuple(self.cursor.datahandler.getPyObject(rrs, index, dbtype)
                                for index, dbtype in self.compiled.returning_parameters)
                    return ReturningResultProxy(self, row)
            finally:
                if rrs is not None:
                    try:
                        rrs.close()
                    except SQLException:
                        pass
                self.statement.close()

        return ResultProxy(self)

    def create_cursor(self):
        cursor = self._dbapi_connection.cursor()
        cursor.datahandler = self.dialect.DataHandler(cursor.datahandler)
        return cursor

class IBM_DB400ZxJDBCCompiler(ibm_base.IBM_DBCompiler):

    def returning_clause(self, stmt, returning_cols):
        self.returning_cols = list(expression._select_iterables(returning_cols))

        # within_columns_clause=False so that labels (foo AS bar) don't render
        columns = [self.process(c, within_columns_clause=False, result_map=self.result_map)
                   for c in self.returning_cols]

        if not hasattr(self, 'returning_parameters'):
            self.returning_parameters = []

        binds = []
        for i, col in enumerate(self.returning_cols):
            dbtype = col.type.dialect_impl(self.dialect).get_dbapi_type(self.dialect.dbapi)
            self.returning_parameters.append((i + 1, dbtype))

            bindparam = sql.bindparam("ret_%d" % i, value=ReturningParam(dbtype))
            self.binds[bindparam.key] = bindparam
            binds.append(self.bindparam_string(self._truncate_bindparam(bindparam)))

        return 'RETURNING ' + ', '.join(columns) +  " INTO " + ", ".join(binds)

class IBM_DB400ZxJDBCDialect(ZxJDBCConnector, ibm_base400.IBM_DB400Dialect):

  supports_unicode_statements = supports_unicode_binds = \
    returns_unicode_strings = supports_unicode = False
  supports_sane_rowcount        = False
  supports_sane_multi_rowcount  = False

  jdbc_db_name = 'as400'
  jdbc_driver_name = 'com.ibm.as400.access.AS400JDBCDriver'

  statement_compiler = IBM_DB400ZxJDBCCompiler
  execution_ctx_cls = IBM_DB400ZxJDBCExecutionContext

  colspecs = util.update_copy(
      ibm_base.IBM_DBDialect.colspecs,
      {
            sa_types.Date : IBM_DB400ZxJDBCDate,
            sa_types.Numeric: IBM_DB400ZxJDBCNumeric
      }
  )

  def __init__(self, use_ansiquotes=None, **kwargs):
    super(IBM_DB400ZxJDBCDialect, self).__init__(**kwargs)
    self.paramstyle = IBM_DB400ZxJDBCDialect.dbapi().paramstyle

    global SQLException, zxJDBC
    from java.sql import SQLException, Types as java_Types
    from com.ziclix.python.sql import zxJDBC
    from com.ziclix.python.sql import FilterDataHandler

    class IBM_DB2DataHandler(FilterDataHandler):

      def setJDBCObject(self, statement, index, object, dbtype=None):
        if type(object) is ReturningParam:
          statement.registerReturnParameter(index, object.type)
        elif dbtype is None:
          if (isinstance(object, int)):
            statement.setObject(index, str(object), java_Types.BIGINT)
          elif (isinstance(object, _python_Decimal)):
            statement.setObject(index, str(object), java_Types.DECIMAL)
          else:
            statement.setObject(index, object)
        else:
          FilterDataHandler.setJDBCObject(self, statement, index, object, dbtype)

    self.DataHandler = IBM_DB2DataHandler

  def _driver_kwargs(self):
    """return kw arg dict to be sent to connect()."""
    return dict(characterEncoding='UTF-8', yearIsDateType='false')

dialect = IBM_DB400ZxJDBCDialect

