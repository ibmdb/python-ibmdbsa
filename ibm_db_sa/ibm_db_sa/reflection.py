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
# | Contributors: Jaimy Azle, Mike Bayer,Hemlata Bhatt                                    |
# +--------------------------------------------------------------------------+

from sqlalchemy import types as sa_types
from sqlalchemy import sql, util
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
import re
import codecs


class CoerceUnicode(sa_types.TypeDecorator):
    impl = sa_types.Unicode

    def process_bind_param(self, value, dialect):
        if isinstance(value, str):
            value = value
        return value

class BaseReflector(object):
    def __init__(self, dialect):
        self.dialect = dialect
        self.ischema_names = dialect.ischema_names
        self.identifier_preparer = dialect.identifier_preparer

    def normalize_name(self, name):
        if isinstance(name, str):
            name = name
        if name != None:
            return name.lower() if name.upper() == name and \
               not self.identifier_preparer._requires_quotes(name.lower()) \
               else name
        return name

    def denormalize_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and \
                not self.identifier_preparer._requires_quotes(name.lower()):
            name = name.upper()
        if not self.dialect.supports_unicode_binds:
            if(isinstance(name, str)):
                name = name
            else:
                name = codecs.decode(name)
        else:
            name = unicode(name)
        return name

    def _get_default_schema_name(self, connection):
        """Return: current setting of the schema attribute"""
        default_schema_name = connection.execute(
                    u'SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1').scalar()
        if isinstance(default_schema_name, str):
            default_schema_name = default_schema_name.strip()
        elif isinstance(default_schema_name, unicode):
            default_schema_name = default_schema_name.strip().__str__()
        return self.normalize_name(default_schema_name)

    @property
    def default_schema_name(self):
        return self.dialect.default_schema_name

class DB2Reflector(BaseReflector):
    ischema = MetaData()

    sys_schemas = Table("SCHEMATA", ischema,
      Column("SCHEMANAME", CoerceUnicode, key="schemaname"),
      Column("OWNER", CoerceUnicode, key="owner"),
      Column("OWNERTYPE", CoerceUnicode, key="ownertype"),
      Column("DEFINER", CoerceUnicode, key="definer"),
      Column("DEFINERTYPE", CoerceUnicode, key="definertype"),
      Column("REMARK", CoerceUnicode, key="remark"),
      schema="SYSCAT")

    sys_tables = Table("TABLES", ischema,
      Column("TABSCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABNAME", CoerceUnicode, key="tabname"),
      Column("OWNER", CoerceUnicode, key="owner"),
      Column("OWNERTYPE", CoerceUnicode, key="ownertype"),
      Column("TYPE", CoerceUnicode, key="type"),
      Column("STATUS", CoerceUnicode, key="status"),
      schema="SYSCAT")

    sys_indexes = Table("INDEXES", ischema,
      Column("TABSCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABNAME", CoerceUnicode, key="tabname"),
      Column("INDNAME", CoerceUnicode, key="indname"),
      Column("COLNAMES", CoerceUnicode, key="colnames"),
      Column("UNIQUERULE", CoerceUnicode, key="uniquerule"),
      Column("SYSTEM_REQUIRED", CoerceUnicode, key="system_required"),
      schema="SYSCAT")
    
    sys_tabconst = Table("TABCONST", ischema,
      Column("TABSCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABNAME", CoerceUnicode, key="tabname"),
      Column("CONSTNAME", CoerceUnicode, key="constname"),
      Column("TYPE", CoerceUnicode, key="type"),
      schema="SYSCAT")
    
    sys_keycoluse = Table("KEYCOLUSE", ischema,
      Column("TABSCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABNAME", CoerceUnicode, key="tabname"),
      Column("CONSTNAME", CoerceUnicode, key="constname"),
      Column("COLNAME", CoerceUnicode, key="colname"),
      schema="SYSCAT")

    sys_foreignkeys = Table("SQLFOREIGNKEYS", ischema,
      Column("FK_NAME", CoerceUnicode, key="fkname"),
      Column("FKTABLE_SCHEM", CoerceUnicode, key="fktabschema"),
      Column("FKTABLE_NAME", CoerceUnicode, key="fktabname"),
      Column("FKCOLUMN_NAME", CoerceUnicode, key="fkcolname"),
      Column("PK_NAME", CoerceUnicode, key="pkname"),
      Column("PKTABLE_SCHEM", CoerceUnicode, key="pktabschema"),
      Column("PKTABLE_NAME", CoerceUnicode, key="pktabname"),
      Column("PKCOLUMN_NAME", CoerceUnicode, key="pkcolname"),
      Column("KEY_SEQ", sa_types.Integer, key="colno"),
      schema="SYSIBM")

    sys_columns = Table("COLUMNS", ischema,
      Column("TABSCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABNAME", CoerceUnicode, key="tabname"),
      Column("COLNAME", CoerceUnicode, key="colname"),
      Column("COLNO", sa_types.Integer, key="colno"),
      Column("TYPENAME", CoerceUnicode, key="typename"),
      Column("LENGTH", sa_types.Integer, key="length"),
      Column("SCALE", sa_types.Integer, key="scale"),
      Column("DEFAULT", CoerceUnicode, key="defaultval"),
      Column("NULLS", CoerceUnicode, key="nullable"),
      Column("IDENTITY", CoerceUnicode, key="identity"),
      Column("GENERATED", CoerceUnicode, key="generated"),
      schema="SYSCAT")

    sys_views = Table("VIEWS", ischema,
      Column("VIEWSCHEMA", CoerceUnicode, key="viewschema"),
      Column("VIEWNAME", CoerceUnicode, key="viewname"),
      Column("TEXT", CoerceUnicode, key="text"),
      schema="SYSCAT")

    sys_sequences = Table("SEQUENCES", ischema,
      Column("SEQSCHEMA", CoerceUnicode, key="seqschema"),
      Column("SEQNAME", CoerceUnicode, key="seqname"),
      schema="SYSCAT")

    def has_table(self, connection, table_name, schema=None):
        current_schema = self.denormalize_name(
                            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
            whereclause = sql.and_(self.sys_tables.c.tabschema == current_schema,
                                   self.sys_tables.c.tabname == table_name)
        else:
            whereclause = self.sys_tables.c.tabname == table_name
        s = sql.select([self.sys_tables.c.tabname], whereclause)
        c = connection.execute(s)
        return c.first() is not None

    def has_sequence(self, connection, sequence_name, schema=None):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        sequence_name = self.denormalize_name(sequence_name)
        if current_schema:
            whereclause = sql.and_(self.sys_sequences.c.seqschema == current_schema,
                                   self.sys_sequences.c.seqname == sequence_name)
        else:
            whereclause = self.sys_sequences.c.seqname == sequence_name
        s = sql.select([self.sys_sequences.c.seqname], whereclause)
        c = connection.execute(s)
        return c.first() is not None

    def get_schema_names(self, connection, **kw):
        sysschema = self.sys_schemas
        query = sql.select([sysschema.c.schemaname],
            sql.not_(sysschema.c.schemaname.like('SYS%')),
            order_by=[sysschema.c.schemaname]
        )
        return [self.normalize_name(r[0]) for r in connection.execute(query)]


    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        systbl = self.sys_tables
        query = sql.select([systbl.c.tabname]).\
                    where(systbl.c.type == 'T').\
                    where(systbl.c.tabschema == current_schema).\
                    order_by(systbl.c.tabname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)

        query = sql.select([self.sys_views.c.viewname]).\
            where(self.sys_views.c.viewschema == current_schema).\
            order_by(self.sys_views.c.viewname)

        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)

        query = sql.select([self.sys_views.c.text]).\
            where(self.sys_views.c.viewschema == current_schema).\
            where(self.sys_views.c.viewname == viewname)

        return connection.execute(query).scalar()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syscols = self.sys_columns

        query = sql.select([syscols.c.colname, syscols.c.typename,
                            syscols.c.defaultval, syscols.c.nullable,
                            syscols.c.length, syscols.c.scale,
                            syscols.c.identity, syscols.c.generated],
              sql.and_(
                  syscols.c.tabschema == current_schema,
                  syscols.c.tabname == table_name
                ),
              order_by=[syscols.c.colno]
            )
        sa_columns = []
        for r in connection.execute(query):
            coltype = r[1].upper()
            if coltype in ['DECIMAL', 'NUMERIC']:
                coltype = self.ischema_names.get(coltype)(int(r[4]), int(r[5]))
            elif coltype in ['CHARACTER', 'CHAR', 'VARCHAR',
                            'GRAPHIC', 'VARGRAPHIC']:
                coltype = self.ischema_names.get(coltype)(int(r[4]))
            else:
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                            (coltype, r[0]))
                    coltype = coltype = sa_types.NULLTYPE

            sa_columns.append({
                    'name': self.normalize_name(r[0]),
                    'type': coltype,
                    'nullable': r[3] == 'Y',
                    'default': r[2] or None,
                    'autoincrement': (r[6] == 'Y') and (r[7] != ' '),
                })
        return sa_columns

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysindexes = self.sys_indexes
        col_finder = re.compile("(\w+)")
        query = sql.select([sysindexes.c.colnames],
              sql.and_(
                  sysindexes.c.tabschema == current_schema,
                  sysindexes.c.tabname == table_name,
                  sysindexes.c.uniquerule == 'P'
                ),
              order_by=[sysindexes.c.tabschema, sysindexes.c.tabname]
            )
        pk_columns = []
        for r in connection.execute(query):
            cols = col_finder.findall(r[0])
            pk_columns.extend(cols)
        return [self.normalize_name(col) for col in pk_columns]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        sysfkeys = self.sys_foreignkeys
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

        fschema = {}
        for r in connection.execute(query):
            if not (r[0]) in fschema:
                referred_schema = self.normalize_name(r[5])

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                    referred_schema == default_schema:
                    referred_schema = None

                fschema[r[0]] = {
                    'name': self.normalize_name(r[0]),
                    'constrained_columns': [self.normalize_name(r[3])],
                    'referred_schema': referred_schema,
                    'referred_table': self.normalize_name(r[6]),
                    'referred_columns': [self.normalize_name(r[7])]}
            else:
                fschema[r[0]]['constrained_columns'].append(self.normalize_name(r[3]))
                fschema[r[0]]['referred_columns'].append(self.normalize_name(r[7]))
        return [value for key, value in fschema.items()]
    
    @reflection.cache
    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        sysfkeys = self.sys_foreignkeys
        query = sql.select([sysfkeys.c.fkname, sysfkeys.c.fktabschema, \
                            sysfkeys.c.fktabname, sysfkeys.c.fkcolname, \
                            sysfkeys.c.pkname, sysfkeys.c.pktabschema, \
                            sysfkeys.c.pktabname, sysfkeys.c.pkcolname],
            sql.and_(
              sysfkeys.c.pktabschema == current_schema,
              sysfkeys.c.pktabname == table_name
            ),
            order_by=[sysfkeys.c.colno]
          )

        fschema = {}
        for r in connection.execute(query):
            if not fschema.has_key(r[0]):
                constrained_schema = self.normalize_name(r[1])

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                    constrained_schema == default_schema:
                    constrained_schema = None

                fschema[r[0]] = {
                    'name': self.normalize_name(r[0]),
                    'constrained_schema': constrained_schema,
                    'constrained_table': self.normalize_name(r[2]),
                    'constrained_columns': [self.normalize_name(r[3])],
                    'referred_schema': schema,
                    'referred_table': self.normalize_name(r[6]),
                    'referred_columns': [self.normalize_name(r[7])]}
            else:
                fschema[r[0]]['constrained_columns'].append(self.normalize_name(r[3]))
                fschema[r[0]]['referred_columns'].append(self.normalize_name(r[7]))
        return [value for key, value in fschema.iteritems()]


    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysidx = self.sys_indexes
        query = sql.select([sysidx.c.indname, sysidx.c.colnames, sysidx.c.uniquerule, sysidx.c.system_required],
            sql.and_(
              sysidx.c.tabschema == current_schema,
              sysidx.c.tabname == table_name
            ),
            order_by=[sysidx.c.tabname]
          )
        indexes = []
        col_finder = re.compile("(\w+)")
        for r in connection.execute(query):
            if r[2] != 'P':
                if r[2] == 'U' and r[3] != 0:
                    continue
                indexes.append({
                        'name': self.normalize_name(r[0]),
                        'column_names': [self.normalize_name(col)
                                        for col in col_finder.findall(r[1])],
                        'unique': r[2] == 'U'
                    })
        return indexes
    
    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syskeycol = self.sys_keycoluse
        sysconst = self.sys_tabconst
        query = sql.select([syskeycol.c.constname, syskeycol.c.colname],
            sql.and_(
              syskeycol.c.constname == sysconst.c.constname,
              sysconst.c.tabname == table_name,
              sysconst.c.tabschema == current_schema,
              sysconst.c.type == 'U'
            ),
            order_by=[syskeycol.c.constname]
          )
        uniqueConsts = []
        currConst = None
        for r in connection.execute(query):
            if currConst == r[0]:
                uniqueConsts[-1]['column_names'].append(self.normalize_name(r[1]))
            else:
                currConst = r[0]
                uniqueConsts.append({
                        'name': self.normalize_name(currConst),
                        'column_names': [self.normalize_name(r[1])],
                    })
        return uniqueConsts

class AS400Reflector(BaseReflector):

    ischema = MetaData()

    sys_schemas = Table("SQLSCHEMAS", ischema,
      Column("TABLE_SCHEM", CoerceUnicode, key="schemaname"),
      schema="SYSIBM")

    sys_tables = Table("SYSTABLES", ischema,
      Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABLE_NAME", CoerceUnicode, key="tabname"),
      Column("TABLE_TYPE", CoerceUnicode, key="tabtype"),
      schema="QSYS2")

    sys_table_constraints = Table("SYSCST", ischema,
      Column("CONSTRAINT_SCHEMA", CoerceUnicode, key="conschema"),
      Column("CONSTRAINT_NAME", CoerceUnicode, key="conname"),
      Column("CONSTRAINT_TYPE", CoerceUnicode, key="contype"),
      Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABLE_NAME", CoerceUnicode, key="tabname"),
      Column("TABLE_TYPE", CoerceUnicode, key="tabtype"),
      schema="QSYS2")

    sys_key_constraints = Table("SYSKEYCST", ischema,
      Column("CONSTRAINT_SCHEMA", CoerceUnicode, key="conschema"),
      Column("CONSTRAINT_NAME", CoerceUnicode, key="conname"),
      Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABLE_NAME", CoerceUnicode, key="tabname"),
      Column("COLUMN_NAME", CoerceUnicode, key="colname"),
      Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
      schema="QSYS2")

    sys_columns = Table("SYSCOLUMNS", ischema,
      Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABLE_NAME", CoerceUnicode, key="tabname"),
      Column("COLUMN_NAME", CoerceUnicode, key="colname"),
      Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
      Column("DATA_TYPE", CoerceUnicode, key="typename"),
      Column("LENGTH", sa_types.Integer, key="length"),
      Column("NUMERIC_SCALE", sa_types.Integer, key="scale"),
      Column("IS_NULLABLE", sa_types.Integer, key="nullable"),
      Column("COLUMN_DEFAULT", CoerceUnicode, key="defaultval"),
      Column("HAS_DEFAULT", CoerceUnicode, key="hasdef"),
      Column("IS_IDENTITY", CoerceUnicode, key="isid"),
      Column("IDENTITY_GENERATION", CoerceUnicode, key="idgenerate"),
      schema="QSYS2")

    sys_indexes = Table("SYSINDEXES", ischema,
      Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABLE_NAME", CoerceUnicode, key="tabname"),
      Column("INDEX_SCHEMA", CoerceUnicode, key="indschema"),
      Column("INDEX_NAME", CoerceUnicode, key="indname"),
      Column("IS_UNIQUE", CoerceUnicode, key="uniquerule"),
      schema="QSYS2")

    sys_keys = Table("SYSKEYS", ischema,
      Column("INDEX_SCHEMA", CoerceUnicode, key="indschema"),
      Column("INDEX_NAME", CoerceUnicode, key="indname"),
      Column("COLUMN_NAME", CoerceUnicode, key="colname"),
      Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
      Column("ORDERING", CoerceUnicode, key="ordering"),
      schema="QSYS2")

    sys_foreignkeys = Table("SQLFOREIGNKEYS", ischema,
      Column("FK_NAME", CoerceUnicode, key="fkname"),
      Column("FKTABLE_SCHEM", CoerceUnicode, key="fktabschema"),
      Column("FKTABLE_NAME", CoerceUnicode, key="fktabname"),
      Column("FKCOLUMN_NAME", CoerceUnicode, key="fkcolname"),
      Column("PK_NAME", CoerceUnicode, key="pkname"),
      Column("PKTABLE_SCHEM", CoerceUnicode, key="pktabschema"),
      Column("PKTABLE_NAME", CoerceUnicode, key="pktabname"),
      Column("PKCOLUMN_NAME", CoerceUnicode, key="pkcolname"),
      Column("KEY_SEQ", sa_types.Integer, key="colno"),
      schema="SYSIBM")

    sys_views = Table("SYSVIEWS", ischema,
      Column("TABLE_SCHEMA", CoerceUnicode, key="viewschema"),
      Column("TABLE_NAME", CoerceUnicode, key="viewname"),
      Column("VIEW_DEFINITION", CoerceUnicode, key="text"),
      schema="QSYS2")

    sys_sequences = Table("SYSSEQUENCES", ischema,
      Column("SEQUENCE_SCHEMA", CoerceUnicode, key="seqschema"),
      Column("SEQUENCE_NAME", CoerceUnicode, key="seqname"),
      schema="QSYS2")

    def has_table(self, connection, table_name, schema=None):
        current_schema = self.denormalize_name(
                                schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
                whereclause = sql.and_(
                            self.sys_tables.c.tabschema == current_schema,
                            self.sys_tables.c.tabname == table_name)
        else:
                whereclause = self.sys_tables.c.tabname == table_name
        s = sql.select([self.sys_tables], whereclause)
        c = connection.execute(s)
        return c.first() is not None

    def has_sequence(self, connection, sequence_name, schema=None):
        current_schema = self.denormalize_name(
                                schema or self.default_schema_name)
        sequence_name = self.denormalize_name(sequence_name)
        if current_schema:
                whereclause = sql.and_(
                            self.sys_sequences.c.seqschema == current_schema,
                            self.sys_sequences.c.seqname == sequence_name)
        else:
                whereclause = self.sys_sequences.c.seqname == sequence_name
        s = sql.select([self.sys_sequences.c.seqname], whereclause)
        c = connection.execute(s)
        return c.first() is not None

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sysschema = self.sys_schemas
        query = sql.select([sysschema.c.schemaname],
                sql.not_(sysschema.c.schemaname.like('SYS%')),
                sql.not_(sysschema.c.schemaname.like('Q%')),
                order_by=[sysschema.c.schemaname]
        )
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    # Retrieves a list of table names for a given schema
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        systbl = self.sys_tables
        query = sql.select([systbl.c.tabname]).\
                where(systbl.c.tabtype == 'T').\
                where(systbl.c.tabschema == current_schema).\
                order_by(systbl.c.tabname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(
                                schema or self.default_schema_name)

        query = sql.select([self.sys_views.c.viewname],
                self.sys_views.c.viewschema == current_schema,
                order_by=[self.sys_views.c.viewname]
            )
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        current_schema = self.denormalize_name(
                                schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)

        query = sql.select([self.sys_views.c.text],
                self.sys_views.c.viewschema == current_schema,
                self.sys_views.c.viewname == viewname
            )
        return connection.execute(query).scalar()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syscols = self.sys_columns

        query = sql.select([syscols.c.colname,
                                syscols.c.typename,
                                syscols.c.defaultval, syscols.c.nullable,
                                syscols.c.length, syscols.c.scale,
                                syscols.c.isid, syscols.c.idgenerate],
                    sql.and_(
                            syscols.c.tabschema == current_schema,
                            syscols.c.tabname == table_name
                        ),
                    order_by=[syscols.c.colno]
                )
        sa_columns = []
        for r in connection.execute(query):
            coltype = r[1].upper()
            if coltype in ['DECIMAL', 'NUMERIC']:
                coltype = self.ischema_names.get(coltype)(int(r[4]), int(r[5]))
            elif coltype in ['CHARACTER', 'CHAR', 'VARCHAR',
                                'GRAPHIC', 'VARGRAPHIC']:
                coltype = self.ischema_names.get(coltype)(int(r[4]))
            else:
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                                    (coltype, r[0]))
                    coltype = coltype = sa_types.NULLTYPE

            sa_columns.append({
                    'name': self.normalize_name(r[0]),
                    'type': coltype,
                    'nullable': r[3] == 'Y',
                    'default': r[2],
                    'autoincrement': (r[6] == 'YES') and (r[7] != None),
                })
        return sa_columns

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
                                    schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        syskeyconst = self.sys_key_constraints

        query = sql.select([syskeyconst.c.colname, sysconst.c.tabname],
                sql.and_(
                    syskeyconst.c.conschema == sysconst.c.conschema,
                    syskeyconst.c.conname == sysconst.c.conname,
                    sysconst.c.tabschema == current_schema,
                    sysconst.c.tabname == table_name,
                    sysconst.c.contype == 'PRIMARY KEY'
            ), order_by=[syskeyconst.c.colno])

        return [self.normalize_name(key[0])
                    for key in connection.execute(query)]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        sysfkeys = self.sys_foreignkeys
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
        fschema = {}
        for r in connection.execute(query):
            if not fschema.has_key(r[0]):
                referred_schema = self.normalize_name(r[5])

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                    referred_schema == default_schema:
                    referred_schema = None

                fschema[r[0]] = {'name': self.normalize_name(r[0]),
                            'constrained_columns': [self.normalize_name(r[3])],
                            'referred_schema': referred_schema,
                            'referred_table': self.normalize_name(r[6]),
                            'referred_columns': [self.normalize_name(r[7])]}
            else:
                fschema[r[0]]['constrained_columns'].append(self.normalize_name(r[3]))
                fschema[r[0]]['referred_columns'].append(self.normalize_name(r[7]))
        return [value for key, value in fschema.iteritems()]

    # Retrieves a list of index names for a given schema
    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
                                    schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysidx = self.sys_indexes
        syskey = self.sys_keys

        query = sql.select([sysidx.c.indname,
                            sysidx.c.uniquerule, syskey.c.colname], sql.and_(
                    syskey.c.indschema == sysidx.c.indschema,
                    syskey.c.indname == sysidx.c.indname,
                    sysidx.c.tabschema == current_schema,
                    sysidx.c.tabname == table_name
                ), order_by=[syskey.c.indname, syskey.c.colno]
            )
        indexes = {}
        for r in connection.execute(query):
            key = r[0].upper()
            if key in indexes:
                indexes[key]['column_names'].append(self.normalize_name(r[2]))
            else:
                indexes[key] = {
                                'name': self.normalize_name(r[0]),
                                'column_names': [self.normalize_name(r[2])],
                                'unique': r[1] == 'Y'
                        }
        return [value for key, value in indexes.iteritems()]
