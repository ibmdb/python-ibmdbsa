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
from sqlalchemy.exc import ArgumentError

if util.py3k:
    long = int

SA_Version = [long(ver_token) for ver_token in SA_Version.split('.')[0:2]]

if SA_Version < [0, 8]:
    from sqlalchemy.engine.base import ResultProxy
else:
    from sqlalchemy.engine import ResultProxy


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
        result = ResultProxy(self)
        if self._callproc_result and self._out_parameters:
            result.out_parameters = {}
            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    name = self.compiled.bind_names[bindparam]
                    result.out_parameters[name] = self._callproc_result[self.compiled.positiontup.index(name)]
        return result

    def get_rowcount(self):
        return self.cursor.rowcount

    def result(self):
        return ResultProxy(self)


class DB2Dialect_ibm_db(DB2Dialect):

    driver = 'ibm_db_sa'
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
        """
        Returns: the underlying DBAPI driver module
        """
        import ibm_db_dbi as module
        return module


    def do_execute(self, cursor, statement, parameters, context=None):
        if isinstance(statement, bytes):
            statement = statement.decode(self.encoding)
        # TODO is bytes parameter conversion really needed?  seems to work without it in py3
        if parameters:
            parameters = tuple(p.decode(self.encoding) if isinstance(p, bytes) else p for p in parameters)
        if context and context._out_parameters:
            statement = statement.split('(', 1)[0].split()[1]
            context._callproc_result = cursor.callproc(statement, parameters)
        else:
            cursor.execute(statement, parameters)


    def _get_server_version_info(self, connection):
        return connection.connection.server_info()


    _isolation_lookup = set(['READ STABILITY', 'RS', 'UNCOMMITTED READ', 'UR',
                             'CURSOR STABILITY', 'CS', 'REPEATABLE READ', 'RR'])

    def set_isolation_level(self, connection, level):
        if level is None or len(level.strip()) < 1:
            level = 'CS'
        level = level.upper().replace("-", " ")
        if level not in self._isolation_lookup:
            raise ArgumentError("Invalid value '%s' for isolation_level. "
                                "Valid isolation levels for %s are %s" %
                                (level, self.name, ", ".join(self._isolation_lookup)))
        cursor = connection.cursor()
        cursor.execute("SET CURRENT ISOLATION %s" % level)
        cursor.execute("COMMIT")
        cursor.close()


    def get_isolation_level(self, connection):
        cursor = connection.cursor()
        cursor.execute('SELECT CURRENT ISOLATION FROM sysibm.sysdummy1')
        val = cursor.fetchone()[0]
        cursor.close()
        if util.py3k and isinstance(val, bytes):
            val = val.decode()
        return val


    def reset_isolation_level(self, connection):
        self.set_isolation_level(connection, 'CS')


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
            return (dsn, uid, pwd, '', ''), {}
        else:
            # Full URL string support for connection to remote data servers
            dsn_param = ['DRIVER={IBM DB2 ODBC DRIVER}',
                         'DATABASE=%s' % url.database,
                         'HOSTNAME=%s' % url.host,
                         'PROTOCOL=TCPIP']
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
            return (dsn, url.username, '', '', ''), {}


    # Retrieves current schema for the specified connection object
    def _get_default_schema_name(self, connection):
        return self.normalize_name(connection.connection.get_current_schema())


    # Checks if the DB_API driver error indicates an invalid connection
    def is_disconnect(self, ex, connection, cursor):
        if isinstance(ex, (self.dbapi.ProgrammingError, self.dbapi.OperationalError)):
            connection_errors = ('Connection is not active', 'connection is no longer active',
                                 'Connection Resource cannot be found', 'SQL30081N'
                                 'CLI0108E', 'CLI0106E', 'SQL1224N')
            for err_msg in connection_errors:
                if err_msg in str(ex):
                    return True
        return False


dialect = DB2Dialect_ibm_db


