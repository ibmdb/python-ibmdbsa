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
# | Author: Jaimy Azle                                                       |
# | Contributor: Mike Bayer                                                  |
# +--------------------------------------------------------------------------+

# raise NotImplementedError(
#         "The zxjdbc dialect is not implemented at this time.")


# NOTE: it appears that to use zxjdbc, the "RETURNING" syntax
# must be installed in DB2, which appears to be optional.  It would
# be best if the RETURNING support were built into the base dialect
# and not be local to zxjdbc here.

from decimal import Decimal as _python_Decimal
from sqlalchemy import sql, util
from sqlalchemy import types as sa_types
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from .base import _SelectLastRowIDMixin, DB2Dialect, DB2ExecutionContext, DB2Compiler
from . import reflection as ibm_reflection

class DB2ExecutionContext_zxjdbc(DB2ExecutionContext):

    def create_cursor(self):
        cursor = self._dbapi_connection.cursor()
        cursor.datahandler = self.dialect.DataHandler(cursor.datahandler)
        return cursor

class DB2Dialect_zxjdbc(ZxJDBCConnector, DB2Dialect):

    supports_unicode_statements = supports_unicode_binds = \
    returns_unicode_strings = supports_unicode = False
    supports_sane_multi_rowcount = False

    supports_unicode_statements = False
    supports_sane_rowcount = True
    supports_char_length = True

    jdbc_db_name = 'db2'
    jdbc_driver_name = 'com.ibm.db2.jcc.DB2Driver'

    statement_compiler = DB2Compiler
    execution_ctx_cls = DB2ExecutionContext_zxjdbc

    @classmethod
    def dbapi(cls):

        global SQLException, zxJDBC
        from java.sql import SQLException, Types as java_Types
        from com.ziclix.python.sql import zxJDBC
        from com.ziclix.python.sql import FilterDataHandler

        # TODO: this should be somewhere else
        class IBM_DB2DataHandler(FilterDataHandler):

            def setJDBCObject(self, statement, index, object, dbtype=None):
                if dbtype is None:
                    if (isinstance(object, int)):
                        statement.setObject(index, str(object), java_Types.INTEGER)
                    elif (isinstance(object, long)):
                        statement.setObject(index, str(object), java_Types.BIGINT)
                    elif (isinstance(object, _python_Decimal)):
                        statement.setObject(index, str(object), java_Types.DECIMAL)
                    else:
                        statement.setObject(index, object)
                else:
                    FilterDataHandler.setJDBCObject(self, statement, index, object, dbtype)

        cls.DataHandler = IBM_DB2DataHandler
        return zxJDBC


class AS400Dialect_zxjdbc(DB2Dialect_zxjdbc):
    jdbc_db_name = 'as400'
    jdbc_driver_name = 'com.ibm.as400.access.AS400JDBCDriver'

    _reflector_cls = ibm_reflection.AS400Reflector




