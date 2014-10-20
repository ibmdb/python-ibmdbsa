# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008, 2014.                                |
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

from .base import DB2ExecutionContext, DB2Dialect
from sqlalchemy import processors, types as sa_types, util
from sqlalchemy import __version__ as SA_Version
SA_Version = [long(ver_token) for ver_token in SA_Version.split('.')[0:2]]

if SA_Version < [0, 8]:
    from sqlalchemy.engine import base
else:
    from sqlalchemy.engine import result as _result

class _IBM_Numeric_ibm_db(sa_types.Numeric):
    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            return None
        else:
            return processors.to_float


class DB2ExecutionContext_ibm_db(DB2ExecutionContext):
    _callproc_result = None
    _out_parameters = None

    def get_lastrowid(self):
        return self.cursor.last_identity_val


    def pre_exec(self):
        # if a single execute, check for outparams
        if len(self.compiled_parameters) == 1:
            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    self._out_parameters = True
                    break
                
    def get_result_proxy(self):
        if self._callproc_result and self._out_parameters:
            if SA_Version < [0, 8]:
                result = base.ResultProxy(self)
            else:
                result = _result.ResultProxy(self)
            result.out_parameters = {}
            
            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    name = self.compiled.bind_names[bindparam]
                    result.out_parameters[name] = self._callproc_result[self.compiled.positiontup.index(name)]
            
            return result
        else:
            if SA_Version < [0, 8]:
                result = base.ResultProxy(self)
            else:
                result = _result.ResultProxy(self)
            return result
         
class DB2Dialect_ibm_db(DB2Dialect):

    driver = 'ibm_db_sa'
    supports_unicode_statements = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_native_decimal = False
    supports_char_length = True
    supports_default_values = False
    supports_multivalues_insert = True
    execution_ctx_cls = DB2ExecutionContext_ibm_db

    colspecs = util.update_copy(
        DB2Dialect.colspecs,
        {
            sa_types.Numeric: _IBM_Numeric_ibm_db
        }
    )

    @classmethod
    def dbapi(cls):
        """ Returns: the underlying DBAPI driver module
        """
        import ibm_db_dbi as module
        return module

    def do_execute(self, cursor, statement, parameters, context=None):
        if context and context._out_parameters:
            statement = statement.split('(', 1)[0].split()[1]
            context._callproc_result = cursor.callproc(statement, parameters)
        else:
            cursor.execute(statement, parameters)

    def _get_server_version_info(self, connection):
        return connection.connection.server_info()

    def create_connect_args(self, url):
        # DSN support through CLI configuration (../cfg/db2cli.ini),
        # while 2 connection attributes are mandatory: database alias
        # and UID (in support to current schema), all the other
        # connection attributes (protocol, hostname, servicename) are
        # provided through db2cli.ini database catalog entry. Example
        # 1: ibm_db_sa:///<database_alias>?UID=db2inst1 or Example 2:
        # ibm_db_sa:///?DSN=<database_alias>;UID=db2inst1
        if not url.host:
            dsn = url.database
            uid = url.username
            pwd = url.password
            return ((dsn, uid, pwd, '', ''), {})
        else:
            # Full URL string support for connection to remote data servers
            dsn_param = ['DRIVER={IBM DB2 ODBC DRIVER}']
            dsn_param.append('DATABASE=%s' % url.database)
            dsn_param.append('HOSTNAME=%s' % url.host)
            dsn_param.append('PROTOCOL=TCPIP')
            if url.port:
                dsn_param.append('PORT=%s' % url.port)
            if url.username:
                dsn_param.append('UID=%s' % url.username)
            if url.password:
                dsn_param.append('PWD=%s' % url.password)
            
            #check for SSL arguments
            ssl_keys = ['Security', 'SSLClientKeystoredb', 'SSLClientKeystash']
            query_keys = url.query.keys()
            for key in ssl_keys:
                for query_key in query_keys:
                    if query_key.lower() == key.lower():
                        dsn_param.append('%(ssl_key)s=%(value)s' % {'ssl_key': key, 'value': url.query[query_key]})
                        del url.query[query_key]
                        break
                           
            dsn = ';'.join(dsn_param)      
            dsn += ';'
            return ((dsn, url.username, '', '', ''), {})

    # Retrieves current schema for the specified connection object
    def _get_default_schema_name(self, connection):
        return self.normalize_name(connection.connection.get_current_schema())


    # Checks if the DB_API driver error indicates an invalid connection
    def is_disconnect(self, ex, connection, cursor):
        if isinstance(ex, (self.dbapi.ProgrammingError,
                                             self.dbapi.OperationalError)):
            return 'Connection is not active' in str(ex) or \
                        'connection is no longer active' in str(ex) or \
                        'Connection Resource cannot be found' in str(ex) or \
                        'SQL30081N' in str(ex)
        else:
            return False

dialect = DB2Dialect_ibm_db
