# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008, 2019.                                |
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
import sys
from sqlalchemy import types as sa_types
from sqlalchemy import sql, util, join
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
from sqlalchemy import *
import re
import codecs
from sys import version_info


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
        if name is not None:
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
            if isinstance(name, str):
                name = name
            else:
                name = codecs.decode(name)
        else:
            if version_info[0] < 3:
                name = unicode(name)
            else:
                name = str(name)
        return name

    def _get_default_schema_name(self, connection):
        """Return: current setting of the schema attribute"""
        default_schema_name = connection.execute(
                    u'SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1').scalar()
        if isinstance(default_schema_name, str):
            default_schema_name = default_schema_name.strip()
        elif version_info[0] < 3:
            if isinstance(default_schema_name, unicode):
                default_schema_name = default_schema_name.strip().__str__()
            else:
                if isinstance(default_schema_name, str):
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
      Column("REMARKS", CoerceUnicode, key="remarks"),
      schema="SYSCAT")

    sys_indexes = Table("INDEXES", ischema,
      Column("TABSCHEMA", CoerceUnicode, key="tabschema"),
      Column("TABNAME", CoerceUnicode, key="tabname"),
      Column("INDNAME", CoerceUnicode, key="indname"),
      Column("COLNAMES", CoerceUnicode, key="colnames"),
      Column("UNIQUERULE", CoerceUnicode, key="uniquerule"),
      Column("SYSTEM_REQUIRED", sa_types.SMALLINT, key="system_required"),
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
      Column("KEYSEQ", CoerceUnicode, key="keyseq"),
      Column("PARTKEYSEQ", CoerceUnicode, key="partkeyseq"),
      Column("IDENTITY", CoerceUnicode, key="identity"),
      Column("GENERATED", CoerceUnicode, key="generated"),
      Column("REMARKS", CoerceUnicode, key="remarks"),
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

    def has_table(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
                            schema or self.default_schema_name)
        if table_name.startswith("'") and table_name.endswith("'"):
            table_name = table_name.replace("'", "")
            table_name = self.normalize_name(table_name)
        else:
            table_name = self.denormalize_name(table_name)
        if current_schema:
            whereclause = sql.and_(self.sys_tables.c.tabschema == current_schema,
                                   self.sys_tables.c.tabname == table_name)
        else:
            whereclause = self.sys_tables.c.tabname == table_name
        s = sql.select(self.sys_tables.c.tabname).where(whereclause)
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
        s = sql.select(self.sys_sequences.c.seqname).where(whereclause)
        c = connection.execute(s)
        return c.first() is not None

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        sys_sequence = self.sys_sequences
        query = sql.select(sys_sequence.c.seqname).\
            where(sys_sequence.c.seqschema == current_schema).\
            order_by(sys_sequence.c.seqschema, sys_sequence.c.seqname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sysschema = self.sys_schemas
        query = sql.select(sysschema.c.schemaname).\
            where(not_(sysschema.c.schemaname.like('SYS%'))).\
            order_by(sysschema.c.schemaname)
        return [self.normalize_name(r[0].rstrip()) for r in connection.execute(query)]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        systbl = self.sys_tables
        query = sql.select(systbl.c.tabname).\
            where(systbl.c.type == 'T').\
            where(systbl.c.tabschema == current_schema).\
            order_by(systbl.c.tabname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        systbl = self.sys_tables
        query = sql.select(systbl.c.remarks).\
            where(systbl.c.tabschema == current_schema).\
            where(systbl.c.tabname == table_name)
        return {'text': connection.execute(query).scalar()}

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)

        query = sql.select(self.sys_views.c.viewname).\
            where(self.sys_views.c.viewschema == current_schema).\
            order_by(self.sys_views.c.viewname)

        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)

        query = sql.select(self.sys_views.c.text).\
            where(self.sys_views.c.viewschema == current_schema).\
            where(self.sys_views.c.viewname == viewname)

        return connection.execute(query).scalar()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syscols = self.sys_columns

        query = sql.select(syscols.c.colname, syscols.c.typename,
                            syscols.c.defaultval, syscols.c.nullable,
                            syscols.c.length, syscols.c.scale,
                            syscols.c.identity, syscols.c.generated,
                            syscols.c.remarks).\
            where(and_(
                  syscols.c.tabschema == current_schema,
                  syscols.c.tabname == table_name)).\
            order_by(syscols.c.colno)
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
                    'comment': r[8] or None,
                })
        return sa_columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysindexes = self.sys_indexes
        col_finder = re.compile(r"(\w+)")
        query = sql.select(sysindexes.c.colnames, sysindexes.c.indname).\
            where(and_(sysindexes.c.tabschema == current_schema,
                       sysindexes.c.tabname == table_name,
                       sysindexes.c.uniquerule == 'P')).\
            order_by(sysindexes.c.tabschema, sysindexes.c.tabname)
        pk_columns = []
        pk_name = None
        for r in connection.execute(query):
            cols = col_finder.findall(r[0])
            pk_columns.extend(cols)
            if not pk_name:
                pk_name = self.normalize_name(r[1])

        return {"constrained_columns": [self.normalize_name(col) for col in pk_columns],
                "name": pk_name}

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syscols = self.sys_columns
        col_finder = re.compile(r"(\w+)")
        query = sql.select(syscols.c.colname).\
            where(and_(
                  syscols.c.tabschema == current_schema,
                  syscols.c.tabname == table_name,
                  syscols.c.keyseq > 0
                )).\
            order_by(syscols.c.tabschema, syscols.c.tabname)
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
        systbl = self.sys_tables
        query = sql.select(sysfkeys.c.fkname, sysfkeys.c.fktabschema,
                            sysfkeys.c.fktabname, sysfkeys.c.fkcolname,
                            sysfkeys.c.pkname, sysfkeys.c.pktabschema,
                            sysfkeys.c.pktabname, sysfkeys.c.pkcolname).\
            select_from(
                join(systbl,
                     sysfkeys,
                     sql.and_(
                         systbl.c.tabname == sysfkeys.c.pktabname,
                         systbl.c.tabschema == sysfkeys.c.pktabschema
                     )
                 )
            ).where(systbl.c.type == 'T').\
            where(systbl.c.tabschema == current_schema).\
            where(sysfkeys.c.fktabname == table_name).\
            order_by(systbl.c.tabname)

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
        query = sql.select(sysfkeys.c.fkname, sysfkeys.c.fktabschema,
                            sysfkeys.c.fktabname, sysfkeys.c.fkcolname,
                            sysfkeys.c.pkname, sysfkeys.c.pktabschema,
                            sysfkeys.c.pktabname, sysfkeys.c.pkcolname).\
            where(and_(
              sysfkeys.c.pktabschema == current_schema,
              sysfkeys.c.pktabname == table_name
            )).\
            order_by(sysfkeys.c.colno)

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
        return [value for key, value in fschema.items()]

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysidx = self.sys_indexes
        query = sql.select(sysidx.c.indname, sysidx.c.colnames,
                            sysidx.c.uniquerule, sysidx.c.system_required).\
            where(and_(sysidx.c.tabschema == current_schema,sysidx.c.tabname == table_name)).\
            order_by(sysidx.c.tabname)
        indexes = []
        col_finder = re.compile(r"(\w+)")
        for r in connection.execute(query):
            if r[2] != 'P':
                if r[2] == 'U' and r[3] != 0:
                    continue
                if 'sqlnotapplicable' in r[1].lower():
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
        query = (
            sql.select(syskeycol.c.constname, syskeycol.c.colname)
            .select_from(
                join(
                    syskeycol,
                    sysconst,
                    and_(
                        syskeycol.c.constname == sysconst.c.constname,
                        syskeycol.c.tabschema == sysconst.c.tabschema,
                        syskeycol.c.tabname == sysconst.c.tabname,
                    ),
                )
            )
            .where(
                and_(
                    sysconst.c.tabname == table_name,
                    sysconst.c.tabschema == current_schema,
                    sysconst.c.type == "U",
                )
            )
            .order_by(syskeycol.c.constname)
        )
        uniqueConsts = []
        currConst = None
        for r in connection.execute(query):
            if currConst == r[0]:
                uniqueConsts[-1]["column_names"].append(self.normalize_name(r[1]))
            else:
                currConst = r[0]
                uniqueConsts.append(
                    {
                        "name": self.normalize_name(currConst),
                        "column_names": [self.normalize_name(r[1])],
                    }
                )
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
      Column("LONG_COMMENT", CoerceUnicode, key="remarks"),
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
      Column("LONG_COMMENT", CoerceUnicode, key="remark"),
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

    def has_table(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
                                schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
                whereclause = sql.and_(
                            self.sys_tables.c.tabschema == current_schema,
                            self.sys_tables.c.tabname == table_name)
        else:
                whereclause = self.sys_tables.c.tabname == table_name
        s = sql.select(self.sys_tables).where(whereclause)
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
        s = sql.select(self.sys_sequences.c.seqname).where(whereclause)
        c = connection.execute(s)
        return c.first() is not None

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        systbl = self.sys_tables
        query = sql.select(systbl.c.remarks).\
            where(systbl.c.tabschema == current_schema).\
            where(systbl.c.tabname == table_name)
        return {'text': connection.execute(query).scalar()}

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        sys_sequence = self.sys_sequences
        query = sql.select(sys_sequence.c.seqname).\
            where(sys_sequence.c.seqschema == current_schema).\
            order_by(sys_sequence.c.seqschema, sys_sequence.c.seqname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sysschema = self.sys_schemas
        if version_info[0] < 3:
            query = sql.select(sysschema.c.schemaname). \
                where(~sysschema.c.schemaname.like(unicode('Q%'))). \
                where(~sysschema.c.schemaname.like(unicode('SYS%'))). \
                order_by(sysschema.c.schemaname)
        else:
            query = sql.select(sysschema.c.schemaname). \
                where(~sysschema.c.schemaname.like(str('Q%'))). \
                where(~sysschema.c.schemaname.like(str('SYS%'))). \
                order_by(sysschema.c.schemaname)
        return [self.normalize_name(r[0].rstrip()) for r in connection.execute(query)]

    # Retrieves a list of table names for a given schema
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        systbl = self.sys_tables
        if version_info[0] < 3:
            query = sql.select(systbl.c.tabname). \
                where(systbl.c.tabtype == unicode('T')). \
                where(systbl.c.tabschema == current_schema). \
                order_by(systbl.c.tabname)
        else:
            query = sql.select(systbl.c.tabname). \
                where(systbl.c.tabtype == str('T')). \
                where(systbl.c.tabschema == current_schema). \
                order_by(systbl.c.tabname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(
                                schema or self.default_schema_name)

        query = sql.select(self.sys_views.c.viewname).\
            where(self.sys_views.c.viewschema == current_schema).\
            order_by(self.sys_views.c.viewname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        current_schema = self.denormalize_name(
                                schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)

        query = sql.select(self.sys_views.c.text).\
            where(self.sys_views.c.viewschema == current_schema).\
            where(self.sys_views.c.viewname == viewname)
        return connection.execute(query).scalar()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syscols = self.sys_columns

        query = sql.select(syscols.c.colname,syscols.c.typename,
                           syscols.c.defaultval, syscols.c.nullable,
                           syscols.c.length, syscols.c.scale,
                           syscols.c.isid, syscols.c.idgenerate,
                           syscols.c.remark).\
            where(and_(
                syscols.c.tabschema == current_schema,
                syscols.c.tabname == table_name)).\
            order_by(syscols.c.colno)
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

            if version_info[0] < 3:
                sa_columns.append({
                    'name': self.normalize_name(r[0]),
                    'type': coltype,
                    'nullable': r[3] == unicode('Y'),
                    'default': r[2],
                    'autoincrement': (r[6] == unicode('YES')) and (r[7] != None),
                    'comment': r[8] or None,
                })
            else:
                sa_columns.append({
                    'name': self.normalize_name(r[0]),
                    'type': coltype,
                    'nullable': r[3] == str('Y'),
                    'default': r[2],
                    'autoincrement': (r[6] == str('YES')) and (r[7] != None),
                    'comment': r[8] or None,
                })
        return sa_columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        syskeyconst = self.sys_key_constraints

        query = sql.select(syskeyconst.c.colname, sysconst.c.tabname, sysconst.c.conname).\
            where(and_(
                syskeyconst.c.conschema == sysconst.c.conschema,
                syskeyconst.c.conname == sysconst.c.conname,
                sysconst.c.tabschema == current_schema,
                sysconst.c.tabname == table_name,
                sysconst.c.contype == 'PRIMARY KEY')).\
            order_by(syskeyconst.c.colno)

        pk_columns = []
        pk_name = None
        for key in connection.execute(query):
            pk_columns.append(self.normalize_name(key[0]))
            if not pk_name:
                pk_name = self.normalize_name(key[2])
        return {"constrained_columns": pk_columns, "name": pk_name}

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
                                    schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        syskeyconst = self.sys_key_constraints

        if version_info[0] < 3:
            query = sql.select(syskeyconst.c.colname, sysconst.c.tabname). \
                where(and_(
                syskeyconst.c.conschema == sysconst.c.conschema,
                syskeyconst.c.conname == sysconst.c.conname,
                sysconst.c.tabschema == current_schema,
                sysconst.c.tabname == table_name,
                sysconst.c.contype == unicode('PRIMARY KEY'))). \
                order_by(syskeyconst.c.colno)
        else:
            query = sql.select(syskeyconst.c.colname, sysconst.c.tabname). \
                where(and_(
                syskeyconst.c.conschema == sysconst.c.conschema,
                syskeyconst.c.conname == sysconst.c.conname,
                sysconst.c.tabschema == current_schema,
                sysconst.c.tabname == table_name,
                sysconst.c.contype == str('PRIMARY KEY'))). \
                order_by(syskeyconst.c.colno)

        return [self.normalize_name(key[0])
                    for key in connection.execute(query)]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        sysfkeys = self.sys_foreignkeys
        query = sql.select(sysfkeys.c.fkname, sysfkeys.c.fktabschema,
                           sysfkeys.c.fktabname, sysfkeys.c.fkcolname,
                           sysfkeys.c.pkname, sysfkeys.c.pktabschema,
                           sysfkeys.c.pktabname, sysfkeys.c.pkcolname).\
            where(and_(
                sysfkeys.c.fktabschema == current_schema,
                sysfkeys.c.fktabname == table_name)).\
            order_by(sysfkeys.c.colno)
        fschema = {}
        for r in connection.execute(query):
            if r[0] not in fschema:
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
        return [value for key, value in fschema.items()]

    # Retrieves a list of index names for a given schema
    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
                                    schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)

        sysidx = self.sys_indexes
        syskey = self.sys_keys

        query = sql.select(sysidx.c.indname,sysidx.c.uniquerule,
                           syskey.c.colname).\
            where(and_(
                syskey.c.indschema == sysidx.c.indschema,
                syskey.c.indname == sysidx.c.indname,
                sysidx.c.tabschema == current_schema,
                sysidx.c.tabname == table_name)).\
            order_by(syskey.c.indname, syskey.c.colno)
        indexes = {}
        for r in connection.execute(query):
            key = r[0].upper()
            if key in indexes:
                indexes[key]['column_names'].append(self.normalize_name(r[2]))
            else:
                if version_info[0] < 3:
                    indexes[key] = {
                        'name': self.normalize_name(r[0]),
                        'column_names': [self.normalize_name(r[2])],
                        'unique': r[1] == unicode('Y')
                    }
                else:
                    indexes[key] = {
                        'name': self.normalize_name(r[0]),
                        'column_names': [self.normalize_name(r[2])],
                        'unique': r[1] == str('Y')
                    }
        return [value for key, value in indexes.items()]

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        uniqueConsts = []
        return uniqueConsts


class OS390Reflector(BaseReflector):
    ischema = MetaData()

    sys_schemas = Table("SYSSCHEMAAUTH", ischema,
        Column("SCHEMANAME", CoerceUnicode, key="schemaname"),
        Column("GRANTEE", CoerceUnicode, key="owner"),
        Column("GRANTEETYPE", CoerceUnicode, key="ownertype"),
        Column("GRANTOR", CoerceUnicode, key="definer"),
        Column("GRANTORTYPE", CoerceUnicode, key="definertype"),
        schema="SYSIBM")

    sys_tables = Table("SYSTABLES", ischema,
        Column("CREATOR", CoerceUnicode, key="tabschema"),
        Column("NAME", CoerceUnicode, key="tabname"),
        Column("OWNER", CoerceUnicode, key="owner"),
        Column("OWNERTYPE", CoerceUnicode, key="ownertype"),
        Column("TYPE", CoerceUnicode, key="type"),
        Column("STATUS", CoerceUnicode, key="status"),
        Column("REMARKS", CoerceUnicode, key="remarks"),
        schema="SYSIBM")

    sys_indexes = Table("SYSINDEXES", ischema,
        Column("CREATOR", CoerceUnicode, key="tabschema"),
        Column("TBNAME", CoerceUnicode, key="tabname"),
        Column("NAME", CoerceUnicode, key="indname"),
        Column("UNIQUERULE", CoerceUnicode, key="uniquerule"),
        Column("IBMREQD", sa_types.SMALLINT, key="system_required"),
        schema="SYSIBM")

    sys_tabconst = Table("SYSTABCONST", ischema,
        Column("TBCREATOR", CoerceUnicode, key="tabschema"),
        Column("TBNAME", CoerceUnicode, key="tabname"),
        Column("CONSTNAME", CoerceUnicode, key="constname"),
        Column("TYPE", CoerceUnicode, key="type"),
        schema="SYSIBM")

    sys_keycoluse = Table("SYSKEYCOLUSE", ischema,
        Column("TBCREATOR", CoerceUnicode, key="tabschema"),
        Column("TBNAME", CoerceUnicode, key="tabname"),
        Column("CONSTNAME", CoerceUnicode, key="constname"),
        Column("COLNAME", CoerceUnicode, key="colname"),
        schema="SYSIBM")

    sys_rels = Table("SYSRELS", ischema,
        Column("CREATOR", CoerceUnicode, key="fktabschema"),
        Column("TBNAME", CoerceUnicode, key="fktabname"),
        Column("RELNAME", CoerceUnicode, key="fkname"),
        Column("REFTBNAME", CoerceUnicode, key="pktabname"),
        Column("REFTBCREATOR", CoerceUnicode, key="pktabschema"),
        schema="SYSIBM")

    sys_foreignkeys = Table("SYSFOREIGNKEYS", ischema,
        Column("CREATOR", CoerceUnicode, key="fktabschema"),
        Column("TBNAME", CoerceUnicode, key="fktabname"),
        Column("RELNAME", CoerceUnicode, key="fkname"),
        Column("COLNAME", CoerceUnicode, key="fkcolname"),
        Column("COLSEQ", sa_types.Integer, key="colno"),
        schema="SYSIBM")

    sys_columns = Table("SYSCOLUMNS", ischema,
        Column("TBCREATOR", CoerceUnicode, key="tabschema"),
        Column("TBNAME", CoerceUnicode, key="tabname"),
        Column("NAME", CoerceUnicode, key="colname"),
        Column("COLNO", sa_types.Integer, key="colno"),
        Column("TYPENAME", CoerceUnicode, key="typename"),
        Column("LENGTH", sa_types.Integer, key="length"),
        Column("SCALE", sa_types.Integer, key="scale"),
        Column("DEFAULT", CoerceUnicode, key="defaultval"),
        Column("NULLS", CoerceUnicode, key="nullable"),
        Column("GENERATED_ATTR", CoerceUnicode, key="generated"),
        Column("KEYSEQ", sa_types.Integer, key="keyseq"),
        Column("REMARKS", sa_types.Integer, key="remark"),
        schema="SYSIBM")

    sys_views = Table("SYSVIEWS", ischema,
        Column("CREATOR", CoerceUnicode, key="viewschema"),
        Column("NAME", CoerceUnicode, key="viewname"),
        Column("STATEMENT", CoerceUnicode, key="text"),
        schema="SYSIBM")

    sys_sequences = Table("SYSSEQUENCES", ischema,
        Column("SCHEMA", CoerceUnicode, key="seqschema"),
        Column("NAME", CoerceUnicode, key="seqname"),
        schema="SYSIBM")

    def has_table(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
                            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
            whereclause = sql.and_(self.sys_tables.c.tabschema == current_schema,
                                   self.sys_tables.c.tabname == table_name)
        else:
            whereclause = self.sys_tables.c.tabname == table_name
        s = sql.select(self.sys_tables.c.tabname).where(whereclause)
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
        s = sql.select(self.sys_sequences.c.seqname).where(whereclause)
        c = connection.execute(s)
        return c.first() is not None

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        sys_sequence = self.sys_sequences
        query = sql.select(sys_sequence.c.seqname).\
            where(sys_sequence.c.seqschema == current_schema).\
            order_by(sys_sequence.c.seqschema, sys_sequence.c.seqname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sysschema = self.sys_tables
        query = sql.select(sysschema.c.tabschema).\
            where(not_(sysschema.c.tabschema.like('SYS%'))).\
            distinct(sysschema.c.tabschema)
        return [self.normalize_name(r[0].rstrip()) for r in connection.execute(query)]

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        systbl = self.sys_tables
        query = sql.select(systbl.c.remarks).\
            where(systbl.c.tabschema == current_schema).\
            where(systbl.c.tabname == table_name)
        return {'text': connection.execute(query).scalar()}

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        systbl = self.sys_tables
        query = sql.select(systbl.c.tabname).\
            where(systbl.c.type == 'T').\
            where(systbl.c.tabschema == current_schema).\
            order_by(systbl.c.tabname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)

        query = sql.select(self.sys_views.c.viewname).\
            where(self.sys_views.c.viewschema == current_schema).\
            order_by(self.sys_views.c.viewname)

        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)

        query = sql.select(self.sys_views.c.text).\
            where(self.sys_views.c.viewschema == current_schema).\
            where(self.sys_views.c.viewname == viewname)

        return connection.execute(query).scalar()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syscols = self.sys_columns

        query = sql.select(syscols.c.colname, syscols.c.typename,
                           syscols.c.defaultval, syscols.c.nullable,
                           syscols.c.length, syscols.c.scale,
                           syscols.c.generated, syscols.c.remark).\
            where(and_(
                syscols.c.tabschema == current_schema,
                syscols.c.tabname == table_name)).\
            order_by(syscols.c.colno)
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
                    'autoincrement': (r[2] == 'J') and (r[2] != ' ') ,
                    'comment': r[7] or None,
                })
        return sa_columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysindexes = self.sys_columns
        col_finder = re.compile(r"(\w+)")
        query = sql.select(sysindexes.c.colname).\
            where(and_(
                sysindexes.c.tabschema == current_schema,
                sysindexes.c.tabname == table_name,
                sysindexes.c.keyseq > 0)).\
            order_by(sysindexes.c.tabschema, sysindexes.c.tabname)
        pk_columns = []
        for r in connection.execute(query):
            cols = col_finder.findall(r[0])
            pk_columns.extend(cols)
        return {"constrained_columns": [self.normalize_name(col) for col in pk_columns], "name": None}

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysindexes = self.sys_columns
        col_finder = re.compile(r"(\w+)")
        query = sql.select(sysindexes.c.colname).\
            where(and_(
                sysindexes.c.tabschema == current_schema,
                sysindexes.c.tabname == table_name,
                sysindexes.c.keyseq > 0)).\
            order_by(sysindexes.c.tabschema, sysindexes.c.tabname)
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
        sysrels = self.sys_rels
        syscolspk = self.sys_columns
        sysindex = self.sys_indexes
        query = sql.select(sysrels.c.fkname, sysrels.c.fktabschema,
                           sysrels.c.fktabname, sysfkeys.c.fkcolname,
                           sysindex.c.indname, sysrels.c.pktabschema,
                           sysrels.c.pktabname, syscolspk.c.colname).\
            where(and_(
                sysrels.c.fktabschema == current_schema,
                sysrels.c.fktabname == table_name,
                sysrels.c.fktabname == sysfkeys.c.fktabname,
                sysrels.c.pktabname == syscolspk.c.tabname,
                syscolspk.c.tabname == sysindex.c.tabname,syscolspk.c.keyseq > 0)).\
            order_by(sysfkeys.c.colno)

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
        sysrels = self.sys_rels
        syscolspk = self.sys_columns
        sysindex = self.sys_indexes
        query = sql.select(sysrels.c.fkname, sysrels.c.fktabschema,
                           sysrels.c.fktabname, sysfkeys.c.fkcolname,
                           sysindex.c.indname, sysrels.c.pktabschema,
                           sysrels.c.pktabname, syscolspk.c.colname).\
            where(and_(
                syscolspk.c.tabschema == current_schema,
                syscolspk.c.tabname == table_name,
                sysrels.c.fktabname == sysfkeys.c.fktabname,
                sysrels.c.pktabname == syscolspk.c.tabname,
                syscolspk.c.tabname == sysindex.c.tabname,
                syscolspk.c.keyseq > 0)).\
            order_by(sysfkeys.c.colno)

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
        return [value for key, value in fschema.items()]

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysidx = self.sys_indexes
        syscolpk = self.sys_columns
        query = sql.select(sysidx.c.indname, syscolpk.c.colname, sysidx.c.uniquerule, sysidx.c.system_required).\
            where(and_(
                sysidx.c.tabschema == current_schema,
                sysidx.c.tabname == table_name,
                syscolpk.c.colname == sysidx.c.tabname,
                syscolpk.c.keyseq > 0)).\
            order_by(sysidx.c.tabname)
        indexes = []
        col_finder = re.compile(r"(\w+)")
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
        query = (
            sql.select(syskeycol.c.constname, syskeycol.c.colname)
            .select_from(
                join(
                    syskeycol,
                    sysconst,
                    and_(
                        syskeycol.c.constname == sysconst.c.constname,
                        syskeycol.c.tabschema == sysconst.c.tabschema,
                        syskeycol.c.tabname == sysconst.c.tabname,
                    ),
                )
            )
            .where(
                and_(
                    sysconst.c.tabname == table_name,
                    sysconst.c.tabschema == current_schema,
                    sysconst.c.type == "U",
                )
            )
            .order_by(syskeycol.c.constname)
        )
        uniqueConsts = []
        currConst = None
        for r in connection.execute(query):
            if currConst == r[0]:
                uniqueConsts[-1]["column_names"].append(self.normalize_name(r[1]))
            else:
                currConst = r[0]
                uniqueConsts.append(
                    {
                        "name": self.normalize_name(currConst),
                        "column_names": [self.normalize_name(r[1])],
                    }
                )
        return uniqueConsts
