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
# | Authors: Alex Pitigoi, Abhigyan Agrawal                                  |
# | Updater: Jaimy Azle                                                      |
# | Version:                                                                 |
# +--------------------------------------------------------------------------+

from sqlalchemy import schema, sql, util
from sqlalchemy import types as sa_types
from sqlalchemy.engine import reflection
from ibm_db_sa import base as ibm_base

class IBM_DB_SAExecutionContext(ibm_base.IBM_DBExecutionContext):
  
  def get_lastrowid(self):
    return self.cursor.last_identity_val
    
class IBM_DB_SADialect(ibm_base.IBM_DBDialect):

  driver                        = 'ibm_db_sa'
  supports_unicode_statements   = False
  supports_sane_rowcount        = True
  supports_sane_multi_rowcount  = True
  supports_native_decimal       = True
  supports_char_length          = True
  supports_native_decimal       = False
  execution_ctx_cls             = IBM_DB_SAExecutionContext

  colspecs = util.update_copy(
        ibm_base.IBM_DBDialect.colspecs,
        {
        }
    )       

  def __init__(self, use_ansiquotes=None, **kwargs):
    super(IBM_DB_SADialect, self).__init__(**kwargs)
    self.paramstyle = IBM_DB_SADialect.dbapi().paramstyle

  @classmethod
  def dbapi(cls):
    """ Returns: the underlying DBAPI driver module
    """
    import ibm_db_dbi as module
    return module

  # Retrieves the IBM Data Server version for a given connection object
  def _get_server_version_info(self, connection):
    """ Inputs: - sqlalchemy.engine.base.Connection object has a <connection> reference
                  to sqlalchemy.pool._ConnectionFairy which has a <connection> reference
                  to sqlalchemy.databases.ibm_db_dbi.Connection, the actual DBAPI
                  driver connection handler:
                      sa_connection   = connection
                      sa_conn_fairy   = sa_connection.connection
                      ibm_db_dbi_conn = sa_conn_fairy.connection
        Returns: Tuple, representing the data server version.
    """
    version_info = connection.connection.server_info()
    return version_info

  # Build DB-API compatible connection arguments.
  def create_connect_args(self, url):
    """ Inputs:  sqlalchemy.engine.url object (attributes parsed from a RFC-1738-style string using
                 module-level make_url() function - driver://username:password@host:port/database or
                 driver:///?<attrib_1_name>=<attrib_1_value>;<attrib_2_name>=<attrib_2_value>)
        Returns: tuple consisting of a *args/**kwargs suitable to send directly to the dbapi connect function.
                 DBAPI.connect(dsn, user='', password='', host='', database='', conn_options=None)
                 DSN: 'DRIVER={IBM DB2 ODBC DRIVER};DATABASE=db_name;HOSTNAME=host_addr;
                       PORT=50000;PROTOCOL=TCPIP;UID=user_id;PWD=secret'
    """
    conn_args = url.translate_connect_args()

    # DSN support through CLI configuration (../cfg/db2cli.ini), while 2 connection
    # attributes are mandatory: database alias and UID (in support to current schema),
    # all the other connection attributes (protocol, hostname, servicename) are provided
    # through db2cli.ini database catalog entry.
    # Example 1: ibm_db_sa:///<database_alias>?UID=db2inst1 or
    # Example 2: ibm_db_sa:///?DSN=<database_alias>;UID=db2inst1
    if str(url).find('///') != -1:
      dsn, uid, pwd = '', '', ''
      if 'database' in conn_args and conn_args['database'] is not None:
        dsn = conn_args['database']
      else:
        if 'DSN' in url.query and url.query['DSN'] is not None:
          dsn = url.query['DSN']
      if 'UID' in url.query and url.query['UID'] is not None:
        uid = url.query['UID']
      if 'PWD' in url.query and url.query['PWD'] is not None:
        pwd = url.query['PWD']
      return ((dsn, uid, pwd,'',''), {})
    else:
      # Full URL string support for connection to remote data servers
      dsn_param = ['DRIVER={IBM DB2 ODBC DRIVER}']
      dsn_param.append( 'DATABASE=%s' % conn_args['database'] )
      dsn_param.append( 'HOSTNAME=%s' % conn_args['host'] )
      dsn_param.append( 'PORT=%s' % conn_args['port'] )
      dsn_param.append( 'PROTOCOL=TCPIP' )
      dsn_param.append( 'UID=%s' % conn_args['username'] )
      dsn_param.append( 'PWD=%s' % conn_args['password'] )
      dsn = ';'.join(dsn_param)
      dsn += ';'
      return ((dsn, conn_args['username'],'','',''), {})

  # Retrieves current schema for the specified connection object
  def _get_default_schema_name(self, connection):
    """ Inputs: - sqlalchemy.engine.base.Connection object has a <connection> reference
                  to sqlalchemy.pool._ConnectionFairy which has a <connection> reference
                  to sqlalchemy.databases.ibm_db_dbi.Connection, the actual DBAPI
                  driver connection handler
        Returns: representing the current schema.
    """
    schema_name = connection.connection.get_current_schema()
    return schema_name

  # Verifies if a specific table exists for a given schema
  def has_table(self, connection, table_name, schema=None):
    """ Inputs: - sqlalchemy.engine.base.Connection object has a <connection> reference
                  to sqlalchemy.pool._ConnectionFairy which has a <connection> reference
                  to sqlalchemy.databases.ibm_db_dbi.Connection, the actual DBAPI
                  driver connection handler
                - table_name string
                - schema string, if not using the default schema
        Returns: True, if table exsits, False otherwise.
    """
    if schema is None:
        schema = self._get_default_schema_name(connection)
    table = connection.connection.tables(schema, table_name)
    has_it = table is not None and \
             len(table) is not 0 \
             and table[0]['TABLE_NAME'] == table_name.upper()
    return has_it

  # Retrieves a list of table names for a given schema
  @reflection.cache
  def get_table_names(self, connection, schema = None, **kw):
    """ Inputs: - sqlalchemy.engine.base.Connection object has a <connection> reference
                  to sqlalchemy.pool._ConnectionFairy which has a <connection> reference
                  to sqlalchemy.databases.ibm_db_dbi.Connection, the actual DBAPI
                  driver connection handler
                - schema string, if not using the default schema
        Returns: List of strings representing table names
    """
    if schema is None:
        schema = self._get_default_schema_name(connection)
    tables = connection.connection.tables(schema)        
    return [table['TABLE_NAME'].lower() for table in tables]
    
  @reflection.cache
  def get_columns(self, connection, table_name, schema=None, **kw):   
    if schema is None:
        schema = self._get_default_schema_name(connection)           
      
    columns = connection.connection.columns(schema, table_name)
    sa_columns = []
    for column in columns:
      coltype = column['TYPE_NAME'].upper()      
      if coltype == 'DECIMAL':
        coltype = self.ischema_names.get(coltype)(column['COLUMN_SIZE'], column['DECIMAL_DIGITS'])
      elif coltype in ['CHAR', 'VARCHAR']:
        coltype = self.ischema_names.get(coltype)(column['COLUMN_SIZE'])        
      else:
        try:
          coltype = self.ischema_names[coltype]
        except KeyError:
          util.warn("Did not recognize type '%s' of column '%s'" % (coltype, column['COLUMN_NAME']))
          coltype = sa_types.NULLTYPE                          
      sa_columns.append({
          'name' : column['COLUMN_NAME'],
          'type' : coltype,
          'nullable' : column['NULLABLE'] == 1,
          'default' : column['COLUMN_DEF'],
          'autoincrement':column['COLUMN_DEF'] is None
        })    
    return sa_columns
    
  @reflection.cache
  def get_primary_keys(self, connection, table_name, schema=None, **kw):
    if schema is None:
        schema = self._get_default_schema_name(connection)
        
    pkeys = connection.connection.primary_keys(True, schema, table_name)
    return [key['COLUMN_NAME'] for key in pkeys]      
    
  @reflection.cache
  def get_foreign_keys(self, connection, table_name, schema=None, **kw):
    if schema is None:
        schema = self._get_default_schema_name(connection)
    
    fkeys = []
    fschema = {}
    fkeys_info = connection.connection.foreign_keys(True, schema, table_name)
    for key in fkeys_info:
      if not fschema.has_key(key['FK_NAME']):
        fschema[key['FK_NAME']] = {'name' : key['FK_NAME'],
              'constrained_columns' : [key['FKCOLUMN_NAME']],
              'referred_schema' : key['PKTABLE_SCHEM'],
              'referred_table' : key['PKTABLE_NAME'],
              'referred_columns' : [key['PKCOLUMN_NAME']]}
      else:
        fschema[key['FK_NAME']]['constrained_columns'].append(key['FKCOLUMN_NAME'])
        fschema[key['FK_NAME']]['referred_columns'].append(key['PKCOLUMN_NAME'])                                 
    return [value for key, value in fschema.iteritems() ]           

  # Retrieves a list of index names for a given schema
  @reflection.cache
  def get_indexes(self, connection, table_name, schema=None, **kw):
    if schema is None:
        schema = self._get_default_schema_name(connection)
    
    ndxnames = {}
    ndxlst = connection.connection.indexes(False, schema, table_name)    
    for ndx in ndxlst:
      if not ndxnames.has_key(ndx['INDEX_NAME']):
        ndxnames[ndx['INDEX_NAME']] = {'name' : ndx['INDEX_NAME'],
              'column_names' : [ndx['COLUMN_NAME']],
              'unique' : ndx['NON_UNIQUE'] == 0}               
      else:             
        ndxnames[ndx['INDEX_NAME']]['column_names'].append(ndx['COLUMN_NAME'])
    return [value for key, value in ndxnames.iteritems() ]

  # Checks if the DB_API driver error indicates an invalid connection
  def is_disconnect(self, ex, connection, cursor):
    """ Inputs: DB_API driver exception to be checked for invalid connection
        Returns: True, if the exception indicates invalid connection, False otherwise.
    """
    if isinstance(ex, (self.dbapi.ProgrammingError,
                       self.dbapi.OperationalError)):
        is_closed = 'Connection is not active' in str(ex) or \
                    'connection is no longer active' in str(ex) or \
                    'Connection Resource cannot be found' in str(ex)
        return is_closed
    else:
        return False

dialect = IBM_DB_SADialect
