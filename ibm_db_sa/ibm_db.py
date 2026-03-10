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
# | Authors: Alex Pitigoi, Abhigyan Agrawal, Rahul Priyadarshi,Abhinav Radke |
# | Contributors: Jaimy Azle, Mike Bayer,Hemlata Bhatt                       |
# +--------------------------------------------------------------------------+

import re
from sqlalchemy import __version__ as SA_VERSION_STR
from .logger import init_ibmdbsa_logging, logger, log_entry_exit
m = re.match(r"^\s*(\d+)\.(\d+)", SA_VERSION_STR)
SA_VERSION_MM = (int(m.group(1)), int(m.group(2))) if m else (0, 0)

from .base import DB2ExecutionContext, DB2Dialect

if SA_VERSION_MM < (2, 0):
    from sqlalchemy import processors, types as sa_types, util
else:
    from sqlalchemy import types as sa_types, util
    from sqlalchemy.engine import processors

from sqlalchemy.exc import ArgumentError

SQL_TXN_READ_UNCOMMITTED = 1
SQL_TXN_READ_COMMITTED = 2
SQL_TXN_REPEATABLE_READ = 4
SQL_TXN_SERIALIZABLE = 8
SQL_ATTR_TXN_ISOLATION = 108

if SA_VERSION_MM < (0, 8):
    from sqlalchemy.engine import base
else:
    from sqlalchemy.engine import result as _result


class _IBM_Numeric_ibm_db(sa_types.Numeric):
   @log_entry_exit
   def result_processor(self, dialect, coltype):
       logger.debug("Creating result processor for _IBM_Numeric_ibm_db")
       def to_float(value):
           logger.debug("Processing numeric result value: %s", value)
           if value is None:
               return None
           else:
               return float(value)
       if self.asdecimal:
           logger.debug("Returning None processor since asdecimal=True")
           return None
       else:
           logger.debug("Returning float conversion processor")
           return to_float


class DB2ExecutionContext_ibm_db(DB2ExecutionContext):
    _callproc_result = None
    _out_parameters = None

    @log_entry_exit
    def get_lastrowid(self):
        logger.debug("Fetching last inserted row id")
        return self.cursor.last_identity_val

    @log_entry_exit
    def pre_exec(self):
        # check for the compiled_parameters attribute in self
        logger.debug("Executing pre_exec checks")
        if hasattr(self, "compiled_parameters"):
            # if a single execute, check for outparams
            logger.debug("Compiled parameters detected")
            if len(self.compiled_parameters) == 1:
                for bindparam in self.compiled.binds.values():
                    if bindparam.isoutparam:
                        logger.debug("OUT parameter detected")
                        self._out_parameters = True
                        break
        else:
            logger.debug("No compiled_parameters attribute found")

    @log_entry_exit
    def get_result_proxy(self):
        logger.debug("Creating result proxy")
        if self._callproc_result and self._out_parameters:
            if SA_VERSION_MM < (0, 8):
                result = base.ResultProxy(self)
            else:
                result = _result.ResultProxy(self)
            result.out_parameters = {}
            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    name = self.compiled.bind_names[bindparam]
                    logger.debug("Processing OUT parameter: %s", name)
                    result.out_parameters[name] = \
                        self._callproc_result[self.compiled.positiontup.index(name)]
            return result
        else:
            if SA_VERSION_MM < (0, 8):
                result = base.ResultProxy(self)
            else:
                result = _result.ResultProxy(self)
            return result


class DB2Dialect_ibm_db(DB2Dialect):
    driver = 'ibm_db_sa'
    supports_unicode_statements = True
    supports_statement_cache = True
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

    if SA_VERSION_MM < (2, 0):
        @classmethod
        @log_entry_exit
        def dbapi(cls):
            """ Returns: the underlying DBAPI driver module
            """
            logger.debug("Importing ibm_db_dbi DBAPI module")
            import ibm_db_dbi as module
            return module
    else:
        @classmethod
        @log_entry_exit
        def import_dbapi(cls):
            """ Returns: the underlying DBAPI driver module
            """
            logger.debug("Importing ibm_db_dbi DBAPI module")
            import ibm_db_dbi as module
            return module

    @log_entry_exit
    def do_execute(self, cursor, statement, parameters, context=None):
        logger.debug("Executing SQL statement")
        logger.debug("Statement: %s", statement)
        logger.debug("Parameters: %s", parameters)
        if context and context._out_parameters:
            logger.debug("Detected stored procedure execution")
            statement = statement.split('(', 1)[0].split()[1]
            context._callproc_result = cursor.callproc(statement, parameters)
        else:
            check_server = getattr(DB2Dialect, 'serverType')
            if ("round(" in statement.casefold()) and check_server == "DB2":
                logger.debug("Applying round() workaround for DB2")
                value_index = 0
                while '?' in statement and value_index < len(parameters):
                    statement = statement.replace('?', str(parameters[value_index]), 1)
                    value_index += 1
                cursor.execute(statement)
            else:
                cursor.execute(statement, parameters)

    @log_entry_exit
    def _get_server_version_info(self, connection):
        logger.debug("Fetching DB2 server version")
        version = connection.connection.server_info()
        logger.debug("Server version info: %s", version)
        return version

    _isolation_lookup = set(['READ STABILITY', 'RS', 'UNCOMMITTED READ', 'UR',
                             'CURSOR STABILITY', 'CS', 'REPEATABLE READ', 'RR'])

    _isolation_levels_cli = {'RR': SQL_TXN_SERIALIZABLE, 'REPEATABLE READ': SQL_TXN_SERIALIZABLE,
                             'UR': SQL_TXN_READ_UNCOMMITTED, 'UNCOMMITTED READ': SQL_TXN_READ_UNCOMMITTED,
                             'RS': SQL_TXN_REPEATABLE_READ, 'READ STABILITY': SQL_TXN_REPEATABLE_READ,
                             'CS': SQL_TXN_READ_COMMITTED, 'CURSOR STABILITY': SQL_TXN_READ_COMMITTED}

    _isolation_levels_returned = {value: key for key, value in _isolation_levels_cli.items()}

    @log_entry_exit
    def _get_cli_isolation_levels(self, level):
        logger.debug("Fetching CLI isolation level mapping for: %s", level)
        value = self._isolation_levels_cli[level]
        logger.debug("CLI isolation level value: %s", value)
        return value

    @log_entry_exit
    def set_isolation_level(self, connection, level):
        logger.debug("Requested isolation level: %s", level)
        if level is None:
            logger.debug("Isolation level is None, defaulting to CS")
            level = 'CS'
        else:
            if len(level.strip()) < 1:
                logger.debug("Isolation level empty after strip, defaulting to CS")
                level = 'CS'
        level = level.upper().replace("-", " ").replace("_", " ")
        logger.debug("Normalized isolation level: %s", level)
        if level not in self._isolation_lookup:
            logger.error("Invalid isolation level requested: %s", level)
            raise ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s" %
                (level, self.name, ", ".join(self._isolation_lookup))
            )
        attrib = {SQL_ATTR_TXN_ISOLATION: self._get_cli_isolation_levels(level)}
        logger.debug("Setting isolation level with attributes: %s", attrib)
        res = connection.set_option(attrib)
        logger.debug("Isolation level set result: %s", res)

    @log_entry_exit
    def get_isolation_level(self, connection):
        logger.debug("Retrieving current isolation level")
        attrib = SQL_ATTR_TXN_ISOLATION
        res = connection.get_option(attrib)
        logger.debug("Raw isolation level value from connection: %s", res)
        val = self._isolation_levels_returned[res]
        logger.debug("Mapped isolation level: %s", val)
        return val

    @log_entry_exit
    def reset_isolation_level(self, connection):
        logger.debug("Resetting isolation level to default (CS)")
        self.set_isolation_level(connection, 'CS')

    def create_connect_args(self, url):
        url, ibmdbsa_log_value = init_ibmdbsa_logging(url)
        # DSN support through CLI configuration (../cfg/db2cli.ini),
        # while 2 connection attributes are mandatory: database alias
        # and UID (in support to current schema), all the other
        # connection attributes (protocol, hostname, servicename) are
        # provided through db2cli.ini database catalog entry. Example
        # 1: ibm_db_sa:///<database_alias>?UID=db2inst1 or Example 2:
        # ibm_db_sa:///?DSN=<database_alias>;UID=db2inst1
        logger.info("entry create_connect_args()")
        if not url.host:
            logger.debug("Using DSN based connection")
            dsn = url.database
            uid = url.username
            pwd = url.password
            logger.debug("DSN connection parameters -> database=%s user=%s", dsn, uid)
            logger.info("exit create_connect_args()")
            return (dsn, uid, pwd, '', ''), {}
        else:
            # Full URL string support for connection to remote data servers
            logger.debug("Using full connection URL for remote DB2 server")
            dsn_param = ['DATABASE=%s' % url.database,
                         'HOSTNAME=%s' % url.host,
                         'PROTOCOL=TCPIP']
            logger.debug("Host: %s", url.host)
            logger.debug("Database: %s", url.database)
            if url.port:
                logger.debug("Port: %s", url.port)
                dsn_param.append('PORT=%s' % url.port)
            if url.username:
                logger.debug("User: %s", url.username)
                dsn_param.append('UID=%s' % url.username)
            if url.password:
                # if password contains ';' truncate at first ';' (existing logic)
                if ';' in url.password:
                    logger.debug("Password contains ';', truncating")
                    url = url._replace(password=(url.password).partition(";")[0])
                dsn_param.append('PWD=%s' % url.password)
            # check for connection arguments
            connection_keys = ['Security', 'SSLClientKeystoredb', 'SSLClientKeystash', 'SSLServerCertificate',
                               'CurrentSchema']
            # rebuild query_keys in case url changed
            query_keys = list(url.query.keys()) if url.query else []
            for key in connection_keys:
                for query_key in query_keys:
                    if query_key.lower() == key.lower():
                        logger.debug("Applying connection option: %s=%s", key, url.query[query_key])
                        dsn_param.append(
                            '%(connection_key)s=%(value)s' % {'connection_key': key, 'value': url.query[query_key]})
                        url = url.difference_update_query([query_key])
                        break
            dsn = ';'.join(dsn_param)
            dsn += ';'
            safe_dsn = dsn
            if 'PWD=' in safe_dsn:
                safe_dsn = re.sub(r'PWD=[^;]*', 'PWD=****', safe_dsn)
            logger.debug("Constructed DB2 DSN: %s", safe_dsn)
            logger.info("exit create_connect_args()")
            return (dsn, url.username, '', '', ''), {}

    # Retrieves current schema for the specified connection object
    @log_entry_exit
    def _get_default_schema_name(self, connection):
        logger.debug("Fetching current schema from DB2")
        schema = connection.connection.get_current_schema()
        logger.debug("Current schema returned: %s", schema)
        normalized_schema_name = self.normalize_name(schema)
        logger.debug("Normalized schema: %s", normalized_schema_name)
        return normalized_schema_name

    # Checks if the DB_API driver error indicates an invalid connection
    @log_entry_exit
    def is_disconnect(self, ex, connection, cursor):
        logger.debug("Checking if exception indicates disconnect")
        logger.debug("Exception received: %s", ex)
        if isinstance(ex, (self.dbapi.ProgrammingError,
                           self.dbapi.OperationalError)):
            connection_errors = ('Connection is not active',
                                 'connection is no longer active',
                                 'Connection Resource cannot be found',
                                 'SQL30081N',
                                 'CLI0108E',
                                 'CLI0106E',
                                 'SQL1224N')
            for err_msg in connection_errors:
                if err_msg in str(ex):
                    logger.debug("Disconnect detected due to error: %s", err_msg)
                    return True
        else:
            logger.debug("Exception type does not indicate disconnect")
        return False


dialect = DB2Dialect_ibm_db
