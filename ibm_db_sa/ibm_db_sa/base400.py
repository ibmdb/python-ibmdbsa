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
from sqlalchemy import types as sa_types
from sqlalchemy import sql, util
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
from sqlalchemy.engine.base import FullyBufferedResultProxy, ResultProxy
from ibm_db_sa import base as ibm_base

ischema = MetaData()

sys_schemas = Table("SQLSCHEMAS", ischema,
  Column("TABLE_SCHEM", ibm_base.CoerceUnicode, key="schemaname"),
  schema="SYSIBM")

sys_tables = Table("SYSTABLES", ischema,
  Column("TABLE_SCHEMA", ibm_base.CoerceUnicode, key="tabschema"),
  Column("TABLE_NAME", ibm_base.CoerceUnicode, key="tabname"),
  Column("TABLE_TYPE", ibm_base.CoerceUnicode, key="tabtype"),
  schema="QSYS2")

sys_table_constraints = Table("SYSCST", ischema,
  Column("CONSTRAINT_SCHEMA", ibm_base.CoerceUnicode, key="conschema"),
  Column("CONSTRAINT_NAME", ibm_base.CoerceUnicode, key="conname"),
  Column("CONSTRAINT_TYPE", ibm_base.CoerceUnicode, key="contype"),
  Column("TABLE_SCHEMA", ibm_base.CoerceUnicode, key="tabschema"),
  Column("TABLE_NAME", ibm_base.CoerceUnicode, key="tabname"),
  Column("TABLE_TYPE", ibm_base.CoerceUnicode, key="tabtype"),
  schema="QSYS2")

sys_key_constraints = Table("SYSKEYCST", ischema,
  Column("CONSTRAINT_SCHEMA", ibm_base.CoerceUnicode, key="conschema"),
  Column("CONSTRAINT_NAME", ibm_base.CoerceUnicode, key="conname"),
  Column("TABLE_SCHEMA", ibm_base.CoerceUnicode, key="tabschema"),
  Column("TABLE_NAME", ibm_base.CoerceUnicode, key="tabname"),
  Column("COLUMN_NAME", ibm_base.CoerceUnicode, key="colname"),
  Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
  schema="QSYS2")

sys_columns = Table("SYSCOLUMNS", ischema,
  Column("TABLE_SCHEMA", ibm_base.CoerceUnicode, key="tabschema"),
  Column("TABLE_NAME", ibm_base.CoerceUnicode, key="tabname"),
  Column("COLUMN_NAME", ibm_base.CoerceUnicode, key="colname"),
  Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
  Column("DATA_TYPE", ibm_base.CoerceUnicode, key="typename"),
  Column("LENGTH", sa_types.Integer, key="length"),
  Column("NUMERIC_SCALE", sa_types.Integer, key="scale"),
  Column("IS_NULLABLE", sa_types.Integer, key="nullable"),
  Column("COLUMN_DEFAULT", ibm_base.CoerceUnicode, key="defaultval"),
  Column("HAS_DEFAULT", ibm_base.CoerceUnicode, key="hasdef"),
  schema="QSYS2")

sys_indexes = Table("SYSINDEXES", ischema,
  Column("TABLE_SCHEMA", ibm_base.CoerceUnicode, key="tabschema"),
  Column("TABLE_NAME", ibm_base.CoerceUnicode, key="tabname"),
  Column("INDEX_SCHEMA", ibm_base.CoerceUnicode, key="indschema"),
  Column("INDEX_NAME", ibm_base.CoerceUnicode, key="indname"),
  Column("IS_UNIQUE", ibm_base.CoerceUnicode, key="uniquerule"),
  schema="QSYS2")

sys_keys = Table("SYSKEYS", ischema,
  Column("INDEX_SCHEMA", ibm_base.CoerceUnicode, key="indschema"),
  Column("INDEX_NAME", ibm_base.CoerceUnicode, key="indname"),
  Column("COLUMN_NAME", ibm_base.CoerceUnicode, key="colname"),
  Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
  Column("ORDERING", ibm_base.CoerceUnicode, key="ordering"),
  schema="QSYS2")

sys_foreignkeys = Table("SQLFOREIGNKEYS", ischema,
  Column("FK_NAME", ibm_base.CoerceUnicode, key="fkname"),
  Column("FKTABLE_SCHEM", ibm_base.CoerceUnicode, key="fktabschema"),
  Column("FKTABLE_NAME", ibm_base.CoerceUnicode, key="fktabname"),
  Column("FKCOLUMN_NAME", ibm_base.CoerceUnicode, key="fkcolname"),
  Column("PK_NAME", ibm_base.CoerceUnicode, key="pkname"),
  Column("PKTABLE_SCHEM", ibm_base.CoerceUnicode, key="pktabschema"),
  Column("PKTABLE_NAME", ibm_base.CoerceUnicode, key="pktabname"),
  Column("PKCOLUMN_NAME", ibm_base.CoerceUnicode, key="pkcolname"),
  Column("KEY_SEQ", sa_types.Integer, key="colno"),
  schema="SYSIBM")

sys_views = Table("SYSVIEWS", ischema,
  Column("TABLE_SCHEMA", ibm_base.CoerceUnicode, key="viewschema"),
  Column("TABLE_NAME", ibm_base.CoerceUnicode, key="viewname"),
  Column("VIEW_DEFINITION", ibm_base.CoerceUnicode, key="text"),
  schema="QSYS2")

sys_sequences = Table("SYSSEQUENCES", ischema,
  Column("SEQUENCE_SCHEMA", ibm_base.CoerceUnicode, key="seqschema"),
  Column("SEQUENCE_NAME", ibm_base.CoerceUnicode, key="seqname"),
  schema="QSYS2")

class IBM_DB400Dialect(ibm_base.IBM_DBDialect):

  def __init__(self, use_ansiquotes=None, **kwargs):
    super(IBM_DB400Dialect, self).__init__(**kwargs)

  def has_table(self, connection, table_name, schema=None):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    table_name = self.denormalize_name(table_name)
    if current_schema:
        whereclause = sql.and_(sys_tables.c.tabschema==current_schema,
                               sys_tables.c.tabname==table_name)
    else:
        whereclause = sys_tables.c.tabname==table_name
    s = sql.select([sys_tables], whereclause)
    c = connection.execute(s)
    return c.first() is not None

  def has_sequence(self, connection, sequence_name, schema=None):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    sequence_name = self.denormalize_name(sequence_name)
    if current_schema:
        whereclause = sql.and_(sys_sequences.c.seqschema==current_schema,
                               sys_sequences.c.seqname==sequence_name)
    else:
        whereclause = sys_sequences.c.seqname==sequence_name
    s = sql.select([sys_sequences.c.seqname], whereclause)
    c = connection.execute(s)
    return c.first() is not None

  @reflection.cache
  def get_schema_names(self, connection, **kw):
    sysschema = sys_schemas
    query = sql.select([sysschema.c.schemaname],
        sql.not_(sysschema.c.schemaname.like('SYS%')),
        sql.not_(sysschema.c.schemaname.like('Q%')),
        order_by=[sysschema.c.schemaname]
    )
    return [self.normalize_name(r[0]) for r in connection.execute(query)]

  # Retrieves a list of table names for a given schema
  @reflection.cache
  def get_table_names(self, connection, schema = None, **kw):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    systbl = sys_tables
    query = sql.select([systbl.c.tabname],
        systbl.c.tabschema == current_schema,
        order_by=[systbl.c.tabname]
      )
    return [self.normalize_name(r[0]) for r in connection.execute(query)]

  @reflection.cache
  def get_view_names(self, connection, schema=None, **kw):
    current_schema = self.denormalize_name(schema or self.default_schema_name)

    query = sql.select([sys_views.c.viewname],
        sys_views.c.viewschema == current_schema,
        order_by=[sys_views.c.viewname]
      )
    return [self.normalize_name(r[0]) for r in connection.execute(query)]

  @reflection.cache
  def get_view_definition(self, connection, viewname, schema=None, **kw):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    viewname = self.denormalize_name(viewname)

    query = sql.select([sys_views.c.text],
        sys_views.c.viewschema == current_schema,
        sys_views.c.viewname == viewname
      )
    return connection.execute(query).scalar()

  @reflection.cache
  def get_columns(self, connection, table_name, schema=None, **kw):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    table_name = self.denormalize_name(table_name)
    syscols = sys_columns

    query = sql.select([syscols.c.colname, syscols.c.typename,
                        syscols.c.defaultval, syscols.c.nullable,
                        syscols.c.length, syscols.c.scale],
          sql.and_(
              syscols.c.tabschema == current_schema,
              syscols.c.tabname == table_name
            ),
          order_by=[syscols.c.tabschema, syscols.c.tabname, syscols.c.colname, syscols.c.colno]
        )
    sa_columns = []
    for r in connection.execute(query):
      coltype = r[1].upper()
      if coltype in ['DECIMAL', 'NUMERIC']:
        coltype = self.ischema_names.get(coltype)(int(r[4]), int(r[5]))
      elif coltype in ['CHARACTER', 'CHAR', 'VARCHAR', 'GRAPHIC', 'VARGRAPHIC']:
        coltype = self.ischema_names.get(coltype)(int(r[4]))
      else:
        try:
          coltype = self.ischema_names[coltype]
        except KeyError:
          util.warn("Did not recognize type '%s' of column '%s'" % (coltype, r[0]))
          coltype = coltype = sa_types.NULLTYPE

      sa_columns.append({
          'name' : self.normalize_name(r[0]),
          'type' : coltype,
          'nullable' : r[3] == 'Y',
          'default' : r[2],
          'autoincrement':r[2] is None,
        })
    return sa_columns

  @reflection.cache
  def get_primary_keys(self, connection, table_name, schema=None, **kw):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    table_name = self.denormalize_name(table_name)
    syscols = sys_columns
    sysconst = sys_table_constraints
    syskeyconst = sys_key_constraints
    sysindexes = sys_indexes

    query = sql.select([syskeyconst.c.colname, sysconst.c.tabname], sql.and_(
          syskeyconst.c.conschema == sysconst.c.conschema,
          syskeyconst.c.conname == sysconst.c.conname,
          sysconst.c.tabschema == current_schema,
          sysconst.c.tabname == table_name,
          sysconst.c.contype == 'PRIMARY KEY'
      ), order_by = [syskeyconst.c.colno])

    return [self.normalize_name(key[0]) for key in connection.execute(query)]

  @reflection.cache
  def get_foreign_keys(self, connection, table_name, schema=None, **kw):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    table_name = self.denormalize_name(table_name)
    sysfkeys = sys_foreignkeys
    query = sql.select([sysfkeys.c.fkname, sysfkeys.c.fktabschema, \
                        sysfkeys.c.fktabname, sysfkeys.c.fkcolname, \
                        sysfkeys.c.pkname, sysfkeys.c.pktabschema, \
                        sysfkeys.c.pktabname, sysfkeys.c.pkcolname],
        sql.and_(
          sysfkeys.c.fktabschema == current_schema,
          sysfkeys.c.fktabname == table_name
        ),
        order_by=[sysfkeys.c.colno]
      )
    fkeys = []
    fschema = {}
    for r in connection.execute(query):
      if not fschema.has_key(r[0]):
        fschema[r[0]] = {'name' : self.normalize_name(r[0]),
              'constrained_columns' : [self.normalize_name(r[3])],
              'referred_schema' : self.normalize_name(r[5]),
              'referred_table' : self.normalize_name(r[6]),
              'referred_columns' : [self.normalize_name(r[7])]}
      else:
        fschema[r[0]]['constrained_columns'].append(self.normalize_name(r[3]))
        fschema[r[0]]['referred_columns'].append(self.normalize_name(r[7]))
    return [value for key, value in fschema.iteritems() ]

  # Retrieves a list of index names for a given schema
  @reflection.cache
  def get_indexes(self, connection, table_name, schema=None, **kw):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    table_name = self.denormalize_name(table_name)
    sysidx = sys_indexes
    syskey = sys_keys

    query = sql.select([sysidx.c.indname, sysidx.c.uniquerule, syskey.c.colname], sql.and_(
          syskey.c.indschema == sysidx.c.indschema,
          syskey.c.indname == sysidx.c.indname,
          sysidx.c.tabschema == current_schema,
          sysidx.c.tabname == table_name
        ), order_by = [syskey.c.indname, syskey.c.colno]
      )
    indexes = {}
    for r in connection.execute(query):
      key = r[0].upper()
      if indexes.has_key(key):
        indexes[key]['column_names'].append(self.normalize_name(r[2]))
      else:
        indexes[key] = {
                          'name' : self.normalize_name(r[0]),
                          'column_names' : [self.normalize_name(r[2])],
                          'unique': r[1] == 'Y'
                      }
    return [value for key, value in indexes.iteritems()]
