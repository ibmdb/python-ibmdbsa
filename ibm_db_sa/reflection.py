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
from .logger import logger, log_entry_exit
import re
import codecs
from sys import version_info


class CoerceUnicode(sa_types.TypeDecorator):
    impl = sa_types.Unicode
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if isinstance(value, str):
            value = value
        return value


class BaseReflector(object):
    @log_entry_exit
    def __init__(self, dialect):
        self.dialect = dialect
        self.ischema_names = dialect.ischema_names
        self.identifier_preparer = dialect.identifier_preparer
        logger.debug(
            f"BaseReflector initialized -> "
            f"dialect={dialect}, "
        )

    @log_entry_exit
    def normalize_name(self, name):
        try:
            original_name = name
            if isinstance(name, str):
                name = name
            if name is not None:
                requires_quotes = self.identifier_preparer._requires_quotes(
                    name.lower()
                )
                result = (
                    name.lower()
                    if name.upper() == name and not requires_quotes
                    else name
                )
                logger.debug(
                    f"normalize_name -> original={original_name}, "
                    f"requires_quotes={requires_quotes}, "
                    f"result={result}"
                )
                return result
            logger.debug("normalize_name -> input is None")
            return name
        except Exception as e:
            logger.error(f"Error in normalize_name: {e}")
            logger.exception("Stack trace in normalize_name")
            raise

    @log_entry_exit
    def denormalize_name(self, name):
        try:
            original_name = name
            if name is None:
                logger.debug("denormalize_name -> input is None")
                return None
            lower_name = name.lower()
            requires_quotes = self.identifier_preparer._requires_quotes(
                lower_name
            )
            if lower_name == name and not requires_quotes:
                name = name.upper()
            supports_unicode = self.dialect.supports_unicode_binds
            if not supports_unicode:
                if isinstance(name, str):
                    name = name
                else:
                    name = codecs.decode(name)
            else:
                if version_info[0] < 3:
                    name = unicode(name)
                else:
                    name = str(name)
            logger.debug(
                f"denormalize_name -> original={original_name}, "
                f"requires_quotes={requires_quotes}, "
                f"supports_unicode_binds={supports_unicode}, "
                f"result={name}"
            )
            return name
        except Exception as e:
            logger.error(f"Error in denormalize_name: {e}")
            logger.exception("Stack trace in denormalize_name")
            raise

    @log_entry_exit
    def _get_default_schema_name(self, connection):
        """Return: current setting of the schema attribute"""
        try:
            logger.debug("Fetching default schema name from database.")
            default_schema_name = connection.execute(
                u"SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1"
            ).scalar()
            logger.debug(
                f"Raw default schema fetched -> {default_schema_name}"
            )
            if isinstance(default_schema_name, str):
                default_schema_name = default_schema_name.strip()
            elif version_info[0] < 3:
                if isinstance(default_schema_name, unicode):
                    default_schema_name = (
                        default_schema_name.strip().__str__()
                    )
                else:
                    if isinstance(default_schema_name, str):
                        default_schema_name = (
                            default_schema_name.strip().__str__()
                        )
            normalized = self.normalize_name(default_schema_name)
            logger.debug(
                f"Normalized default schema -> {normalized}"
            )
            return normalized
        except Exception as e:
            logger.error(f"Error fetching default schema name: {e}")
            logger.exception("Stack trace in _get_default_schema_name")
            raise

    @property
    def default_schema_name(self):
        schema_name = self.dialect.default_schema_name
        logger.debug(
            f"Accessing default_schema_name property -> {schema_name}"
        )
        return schema_name


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

    @log_entry_exit
    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            logger.debug(f"Checking table existence -> schema={schema}, table={table_name}")
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            original_table_name = table_name
            if table_name.startswith("'") and table_name.endswith("'"):
                table_name = table_name.replace("'", "")
                table_name = self.normalize_name(table_name)
            else:
                table_name = self.denormalize_name(table_name)
            logger.debug(
                f"Resolved identifiers -> "
                f"schema={current_schema}, "
                f"table={table_name}"
            )
            if current_schema:
                whereclause = sql.and_(
                    self.sys_tables.c.tabschema == current_schema,
                    self.sys_tables.c.tabname == table_name
                )
            else:
                whereclause = self.sys_tables.c.tabname == table_name
            s = sql.select(self.sys_tables.c.tabname).where(whereclause)
            logger.debug(f"Generated has_table SQL -> {s}")
            result = connection.execute(s).first() is not None
            logger.debug(f"has_table result -> table={original_table_name}, exists={result}")
            return result
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            logger.exception("Stack trace in has_table")
            raise

    @log_entry_exit
    def has_sequence(self, connection, sequence_name, schema=None):
        try:
            logger.debug(f"Checking sequence existence -> schema={schema}, sequence={sequence_name}")
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            sequence_name = self.denormalize_name(sequence_name)
            logger.debug(
                f"Resolved identifiers -> "
                f"schema={current_schema}, "
                f"sequence={sequence_name}"
            )
            if current_schema:
                whereclause = sql.and_(
                    self.sys_sequences.c.seqschema == current_schema,
                    self.sys_sequences.c.seqname == sequence_name
                )
            else:
                whereclause = self.sys_sequences.c.seqname == sequence_name
            s = sql.select(self.sys_sequences.c.seqname).where(whereclause)
            logger.debug(f"Generated has_sequence SQL -> {s}")
            result = connection.execute(s).first() is not None
            logger.debug(f"has_sequence result -> sequence={sequence_name}, exists={result}")
            return result
        except Exception as e:
            logger.error(f"Error checking sequence existence: {e}")
            logger.exception("Stack trace in has_sequence")
            raise

    @reflection.cache
    @log_entry_exit
    def get_sequence_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(f"Fetching sequence names -> schema={current_schema}")
            sys_sequence = self.sys_sequences
            query = (
                sql.select(sys_sequence.c.seqname)
                .where(sys_sequence.c.seqschema == current_schema)
                .order_by(
                    sys_sequence.c.seqschema,
                    sys_sequence.c.seqname
                )
            )
            logger.debug(f"Generated get_sequence_names SQL -> {query}")
            result = [self.normalize_name(r[0]) for r in connection.execute(query)]
            logger.debug(f"Reflected sequences -> count={len(result)}, sequences={result}")
            return result
        except Exception as e:
            logger.error(f"Error fetching sequence names: {e}")
            logger.exception("Stack trace in get_sequence_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_schema_names(self, connection, **kw):
        try:
            logger.debug("Fetching schema names.")
            sysschema = self.sys_schemas
            query = (
                sql.select(sysschema.c.schemaname)
                .where(not_(sysschema.c.schemaname.like('SYS%')))
                .order_by(sysschema.c.schemaname)
            )
            logger.debug(f"Generated get_schema_names SQL -> {query}")
            result = [self.normalize_name(r[0].rstrip()) for r in connection.execute(query)]
            logger.debug(f"Reflected schemas -> count={len(result)}, schemas={result}")
            return result
        except Exception as e:
            logger.error(f"Error fetching schema names: {e}")
            logger.exception("Stack trace in get_schema_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_table_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(f"Fetching table names -> schema={current_schema}")
            systbl = self.sys_tables
            query = (
                sql.select(systbl.c.tabname)
                .where(systbl.c.type == 'T')
                .where(systbl.c.tabschema == current_schema)
                .order_by(systbl.c.tabname)
            )
            logger.debug(f"Generated get_table_names SQL -> {query}")
            result = [self.normalize_name(r[0]) for r in connection.execute(query)]
            logger.debug(f"Reflected tables -> count={len(result)}, tables={result}")
            return result
        except Exception as e:
            logger.error(f"Error fetching table names: {e}")
            logger.exception("Stack trace in get_table_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"Fetching table comment -> schema={current_schema}, table={table_name}")
            systbl = self.sys_tables
            query = (
                sql.select(systbl.c.remarks)
                .where(systbl.c.tabschema == current_schema)
                .where(systbl.c.tabname == table_name)
            )
            logger.debug(f"Generated get_table_comment SQL -> {query}")
            comment = connection.execute(query).scalar()
            logger.debug(f"Table comment result -> {comment}")
            return {'text': comment}
        except Exception as e:
            logger.error(f"Error fetching table comment: {e}")
            logger.exception("Stack trace in get_table_comment")
            raise

    @reflection.cache
    @log_entry_exit
    def get_view_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(f"Fetching view names -> schema={current_schema}")
            query = (
                sql.select(self.sys_views.c.viewname)
                .where(self.sys_views.c.viewschema == current_schema)
                .order_by(self.sys_views.c.viewname)
            )
            logger.debug(f"Generated get_view_names SQL -> {query}")
            result = [self.normalize_name(r[0]) for r in connection.execute(query)]
            logger.debug(f"Reflected views -> count={len(result)}, views={result}")
            return result
        except Exception as e:
            logger.error(f"Error fetching view names: {e}")
            logger.exception("Stack trace in get_view_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            viewname = self.denormalize_name(viewname)
            logger.debug(f"Fetching view definition -> schema={current_schema}, view={viewname}")
            query = (
                sql.select(self.sys_views.c.text)
                .where(self.sys_views.c.viewschema == current_schema)
                .where(self.sys_views.c.viewname == viewname)
            )
            logger.debug(f"Generated get_view_definition SQL -> {query}")
            definition = connection.execute(query).scalar()
            logger.debug(f"View definition length -> {len(definition) if definition else 0}")
            return definition
        except Exception as e:
            logger.error(f"Error fetching view definition: {e}")
            logger.exception("Stack trace in get_view_definition")
            raise

    @reflection.cache
    @log_entry_exit
    def get_columns(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"Fetching columns -> schema={current_schema}, table={table_name}")
            syscols = self.sys_columns
            query = (
                sql.select(
                    syscols.c.colname, syscols.c.typename,
                    syscols.c.defaultval, syscols.c.nullable,
                    syscols.c.length, syscols.c.scale,
                    syscols.c.identity, syscols.c.generated,
                    syscols.c.remarks
                )
                .where(and_(
                    syscols.c.tabschema == current_schema,
                    syscols.c.tabname == table_name
                ))
                .order_by(syscols.c.colno)
            )
            logger.debug(f"Generated get_columns SQL -> {query}")
            sa_columns = []
            for r in connection.execute(query):
                raw_type = r[1].upper()
                logger.debug(
                    f"Processing column -> "
                    f"name={r[0]}, type={raw_type}, "
                    f"length={r[4]}, scale={r[5]}"
                )
                if raw_type in ['DECIMAL', 'NUMERIC']:
                    coltype = self.ischema_names.get(raw_type)(int(r[4]), int(r[5]))
                elif raw_type in ['CHARACTER', 'CHAR', 'VARCHAR',
                                  'GRAPHIC', 'VARGRAPHIC']:
                    coltype = self.ischema_names.get(raw_type)(int(r[4]))
                else:
                    try:
                        coltype = self.ischema_names[raw_type]
                    except KeyError:
                        logger.warning(
                            f"Unrecognized column type '{raw_type}' "
                            f"for column '{r[0]}'"
                        )
                        coltype = sa_types.NULLTYPE
                column_info = {
                    'name': self.normalize_name(r[0]),
                    'type': coltype,
                    'nullable': r[3] == 'Y',
                    'default': r[2] or None,
                    'autoincrement': (r[6] == 'Y') and (r[7] != ' '),
                    'comment': r[8] or None,
                }
                logger.debug(f"Column reflected -> {column_info}")
                sa_columns.append(column_info)
            logger.debug(f"Total columns reflected -> count={len(sa_columns)}")
            return sa_columns
        except Exception as e:
            logger.error(f"Error reflecting columns: {e}")
            logger.exception("Stack trace in get_columns")
            raise

    @reflection.cache
    @log_entry_exit
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"Fetching primary key -> schema={current_schema}, table={table_name}")
            sysindexes = self.sys_indexes
            col_finder = re.compile(r"(\w+)")
            query = (
                sql.select(sysindexes.c.colnames, sysindexes.c.indname)
                .where(and_(
                    sysindexes.c.tabschema == current_schema,
                    sysindexes.c.tabname == table_name,
                    sysindexes.c.uniquerule == 'P'
                ))
                .order_by(
                    sysindexes.c.tabschema,
                    sysindexes.c.tabname
                ))
            logger.debug(f"Generated get_pk_constraint SQL -> {query}")
            pk_columns = []
            pk_name = None
            for r in connection.execute(query):
                cols = col_finder.findall(r[0])
                pk_columns.extend(cols)
                if not pk_name:
                    pk_name = self.normalize_name(r[1])
            normalized_columns = [self.normalize_name(col) for col in pk_columns]
            logger.debug(
                f"Primary key reflected -> "
                f"name={pk_name}, columns={normalized_columns}"
            )
            return {
                "constrained_columns": normalized_columns,
                "name": pk_name
            }
        except Exception as e:
            logger.error(f"Error reflecting primary key: {e}")
            logger.exception("Stack trace in get_pk_constraint")
            raise

    @reflection.cache
    @log_entry_exit
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"Fetching primary keys -> schema={current_schema}, table={table_name}")
            syscols = self.sys_columns
            col_finder = re.compile(r"(\w+)")
            query = (
                sql.select(syscols.c.colname)
                .where(and_(
                    syscols.c.tabschema == current_schema,
                    syscols.c.tabname == table_name,
                    syscols.c.keyseq > 0
                ))
                .order_by(syscols.c.tabschema, syscols.c.tabname)
            )
            logger.debug(f"Generated get_primary_keys SQL -> {query}")
            pk_columns = []
            for r in connection.execute(query):
                cols = col_finder.findall(r[0])
                pk_columns.extend(cols)
            normalized_columns = [self.normalize_name(col) for col in pk_columns]
            logger.debug(f"Primary keys reflected -> columns={normalized_columns}")
            return normalized_columns
        except Exception as e:
            logger.error(f"Error reflecting primary keys: {e}")
            logger.exception("Stack trace in get_primary_keys")
            raise

    @reflection.cache
    @log_entry_exit
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        try:
            default_schema = self.default_schema_name
            current_schema = self.denormalize_name(schema or default_schema)
            normalized_default_schema = self.normalize_name(default_schema)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"Fetching foreign keys -> "
                f"schema={current_schema}, table={table_name}"
            )
            sysfkeys = self.sys_foreignkeys
            systbl = self.sys_tables
            query = (
                sql.select(
                    sysfkeys.c.fkname, sysfkeys.c.fktabschema,
                    sysfkeys.c.fktabname, sysfkeys.c.fkcolname,
                    sysfkeys.c.pkname, sysfkeys.c.pktabschema,
                    sysfkeys.c.pktabname, sysfkeys.c.pkcolname
                )
                .select_from(
                    join(
                        systbl,
                        sysfkeys,
                        sql.and_(
                            systbl.c.tabname == sysfkeys.c.pktabname,
                            systbl.c.tabschema == sysfkeys.c.pktabschema
                        )
                    )
                )
                .where(systbl.c.type == 'T')
                .where(systbl.c.tabschema == current_schema)
                .where(sysfkeys.c.fktabname == table_name)
                .order_by(systbl.c.tabname)
            )
            logger.debug(f"Generated get_foreign_keys SQL -> {query}")
            fschema = {}
            for r in connection.execute(query):
                fk_name = r[0]
                if fk_name not in fschema:
                    referred_schema = self.normalize_name(r[5])
                    # if no schema specified and referred schema here is the
                    # default, then set to None
                    if schema is None and \
                            referred_schema == normalized_default_schema:
                        referred_schema = None
                    fschema[fk_name] = {
                        'name': self.normalize_name(fk_name),
                        'constrained_columns': [self.normalize_name(r[3])],
                        'referred_schema': referred_schema,
                        'referred_table': self.normalize_name(r[6]),
                        'referred_columns': [self.normalize_name(r[7])]
                    }
                    logger.debug(f"Foreign key discovered -> {fschema[fk_name]}")
                else:
                    fschema[fk_name]['constrained_columns'].append(self.normalize_name(r[3]))
                    fschema[fk_name]['referred_columns'].append(self.normalize_name(r[7]))
            result = [value for value in fschema.values()]
            logger.debug(f"Total foreign keys reflected -> count={len(result)}")
            return result
        except Exception as e:
            logger.error(f"Error reflecting foreign keys: {e}")
            logger.exception("Stack trace in get_foreign_keys")
            raise

    @reflection.cache
    @log_entry_exit
    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        try:
            default_schema = self.default_schema_name
            current_schema = self.denormalize_name(schema or default_schema)
            normalized_default_schema = self.normalize_name(default_schema)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"Fetching incoming foreign keys -> "
                f"schema={current_schema}, table={table_name}"
            )
            sysfkeys = self.sys_foreignkeys
            query = (
                sql.select(
                    sysfkeys.c.fkname, sysfkeys.c.fktabschema,
                    sysfkeys.c.fktabname, sysfkeys.c.fkcolname,
                    sysfkeys.c.pkname, sysfkeys.c.pktabschema,
                    sysfkeys.c.pktabname, sysfkeys.c.pkcolname
                )
                .where(and_(
                    sysfkeys.c.pktabschema == current_schema,
                    sysfkeys.c.pktabname == table_name
                ))
                .order_by(sysfkeys.c.colno)
            )
            logger.debug(f"Generated get_incoming_foreign_keys SQL -> {query}")
            fschema = {}
            for r in connection.execute(query):
                fk_name = r[0]
                if fk_name not in fschema:
                    constrained_schema = self.normalize_name(r[1])
                    # if no schema specified and referred schema here is the
                    # default, then set to None
                    if schema is None and \
                            constrained_schema == normalized_default_schema:
                        constrained_schema = None
                    fschema[fk_name] = {
                        'name': self.normalize_name(fk_name),
                        'constrained_schema': constrained_schema,
                        'constrained_table': self.normalize_name(r[2]),
                        'constrained_columns': [self.normalize_name(r[3])],
                        'referred_schema': schema,
                        'referred_table': self.normalize_name(r[6]),
                        'referred_columns': [self.normalize_name(r[7])]
                    }
                    logger.debug(f"Incoming foreign key discovered -> {fschema[fk_name]}")
                else:
                    fschema[fk_name]['constrained_columns'].append(self.normalize_name(r[3]))
                    fschema[fk_name]['referred_columns'].append(self.normalize_name(r[7]))
            result = [value for value in fschema.values()]
            logger.debug(f"Total incoming foreign keys reflected -> count={len(result)}")
            return result
        except Exception as e:
            logger.error(f"Error reflecting incoming foreign keys: {e}")
            logger.exception("Stack trace in get_incoming_foreign_keys")
            raise

    @reflection.cache
    @log_entry_exit
    def get_indexes(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"Fetching indexes -> schema={current_schema}, table={table_name}")
            sysidx = self.sys_indexes
            query = (
                sql.select(sysidx.c.indname, sysidx.c.colnames,
                    sysidx.c.uniquerule, sysidx.c.system_required
                )
                .where(and_(
                    sysidx.c.tabschema == current_schema,
                    sysidx.c.tabname == table_name
                ))
                .order_by(sysidx.c.tabname)
            )
            logger.debug(f"Generated get_indexes SQL -> {query}")
            indexes = []
            col_finder = re.compile(r"(\w+)")
            for r in connection.execute(query):
                index_name = r[0]
                column_text = r[1]
                unique_rule = r[2]
                system_required = r[3]
                logger.debug(
                    f"Processing index row -> "
                    f"name={index_name}, unique_rule={unique_rule}, "
                    f"system_required={system_required}"
                )
                if unique_rule == 'P':
                    logger.debug(f"Skipping primary key index -> {index_name}")
                    continue
                if unique_rule == 'U' and system_required != 0:
                    logger.debug(f"Skipping system-required unique index -> {index_name}")
                    continue
                if 'sqlnotapplicable' in column_text.lower():
                    logger.debug(f"Skipping internal index -> {index_name}")
                    continue
                normalized_columns = [self.normalize_name(col) for col in col_finder.findall(column_text)]
                index_info = {
                    'name': self.normalize_name(index_name),
                    'column_names': normalized_columns,
                    'unique': unique_rule == 'U'
                }
                logger.debug(f"Index reflected -> {index_info}")
                indexes.append(index_info)
            logger.debug(f"Total indexes reflected -> count={len(indexes)}")
            return indexes
        except Exception as e:
            logger.error(f"Error reflecting indexes: {e}")
            logger.exception("Stack trace in get_indexes")
            raise

    @reflection.cache
    @log_entry_exit
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"Fetching unique constraints -> "
                f"schema={current_schema}, table={table_name}"
            )
            syskeycol = self.sys_keycoluse
            sysconst = self.sys_tabconst
            query = (
                sql.select(
                    syskeycol.c.constname,
                    syskeycol.c.colname
                )
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
            logger.debug(f"Generated get_unique_constraints SQL -> {query}")
            uniqueConsts = []
            currConst = None
            for r in connection.execute(query):
                constraint_name = r[0]
                column_name = self.normalize_name(r[1])
                if currConst == constraint_name:
                    uniqueConsts[-1]["column_names"].append(column_name)
                    logger.debug(
                        f"Appending column to constraint -> "
                        f"name={constraint_name}, column={column_name}"
                    )
                else:
                    currConst = constraint_name
                    constraint_info = {
                        "name": self.normalize_name(currConst),
                        "column_names": [column_name],
                    }
                    logger.debug(f"New unique constraint discovered -> {constraint_info}")
                    uniqueConsts.append(constraint_info)
            logger.debug(
                f"Total unique constraints reflected -> "
                f"count={len(uniqueConsts)}"
            )
            return uniqueConsts
        except Exception as e:
            logger.error(f"Error reflecting unique constraints: {e}")
            logger.exception("Stack trace in get_unique_constraints")
            raise


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

    @log_entry_exit
    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"[AS400] Checking table existence -> "
                f"schema={current_schema}, table={table_name}"
            )
            if current_schema:
                whereclause = sql.and_(
                    self.sys_tables.c.tabschema == current_schema,
                    self.sys_tables.c.tabname == table_name
                )
            else:
                whereclause = self.sys_tables.c.tabname == table_name
            s = sql.select(self.sys_tables).where(whereclause)
            logger.debug(f"[AS400] Generated has_table SQL -> {s}")
            result = connection.execute(s).first() is not None
            logger.debug(f"[AS400] has_table result -> exists={result}")
            return result
        except Exception as e:
            logger.error(f"[AS400] Error in has_table: {e}")
            logger.exception("Stack trace in AS400 has_table")
            raise

    @log_entry_exit
    def has_sequence(self, connection, sequence_name, schema=None):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            sequence_name = self.denormalize_name(sequence_name)
            logger.debug(
                f"[AS400] Checking sequence existence -> "
                f"schema={current_schema}, sequence={sequence_name}"
            )
            if current_schema:
                whereclause = sql.and_(
                    self.sys_sequences.c.seqschema == current_schema,
                    self.sys_sequences.c.seqname == sequence_name
                )
            else:
                whereclause = self.sys_sequences.c.seqname == sequence_name
            s = sql.select(self.sys_sequences.c.seqname).where(whereclause)
            logger.debug(f"[AS400] Generated has_sequence SQL -> {s}")
            result = connection.execute(s).first() is not None
            logger.debug(f"[AS400] has_sequence result -> exists={result}")
            return result
        except Exception as e:
            logger.error(f"[AS400] Error in has_sequence: {e}")
            logger.exception("Stack trace in AS400 has_sequence")
            raise

    @log_entry_exit
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"[AS400] Fetching table comment -> "
                f"schema={current_schema}, table={table_name}"
            )
            systbl = self.sys_tables
            query = (
                sql.select(systbl.c.remarks)
                .where(systbl.c.tabschema == current_schema)
                .where(systbl.c.tabname == table_name)
            )
            logger.debug(f"[AS400] Generated get_table_comment SQL -> {query}")
            comment = connection.execute(query).scalar()
            logger.debug(f"[AS400] Table comment result -> {comment}")
            return {'text': comment}
        except Exception as e:
            logger.error(f"[AS400] Error in get_table_comment: {e}")
            logger.exception("Stack trace in AS400 get_table_comment")
            raise

    @reflection.cache
    @log_entry_exit
    def get_sequence_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(
                f"[AS400] Fetching sequence names -> "
                f"schema={current_schema}"
            )
            sys_sequence = self.sys_sequences
            query = (
                sql.select(sys_sequence.c.seqname)
                .where(sys_sequence.c.seqschema == current_schema)
                .order_by(sys_sequence.c.seqschema, sys_sequence.c.seqname)
            )
            logger.debug(f"[AS400] Generated get_sequence_names SQL -> {query}")
            result = [self.normalize_name(r[0]) for r in connection.execute(query)]
            logger.debug(
                f"[AS400] Reflected sequences -> count={len(result)}, "
                f"sequences={result}"
            )
            return result
        except Exception as e:
            logger.error(f"[AS400] Error in get_sequence_names: {e}")
            logger.exception("Stack trace in AS400 get_sequence_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_schema_names(self, connection, **kw):
        try:
            logger.debug("[AS400] Fetching schema names")
            sysschema = self.sys_schemas
            if version_info[0] < 3:
                logger.debug("[AS400] Using unicode branch for schema filtering")
                query = (
                    sql.select(sysschema.c.schemaname)
                    .where(~sysschema.c.schemaname.like(unicode('Q%')))
                    .where(~sysschema.c.schemaname.like(unicode('SYS%')))
                    .order_by(sysschema.c.schemaname)
                )
            else:
                logger.debug("[AS400] Using str branch for schema filtering")
                query = (
                    sql.select(sysschema.c.schemaname)
                    .where(~sysschema.c.schemaname.like(str('Q%')))
                    .where(~sysschema.c.schemaname.like(str('SYS%')))
                    .order_by(sysschema.c.schemaname)
                )
            logger.debug(f"[AS400] Generated get_schema_names SQL -> {query}")
            result = [
                self.normalize_name(r[0].rstrip())
                for r in connection.execute(query)
            ]
            logger.debug(f"[AS400] Reflected schemas -> count={len(result)}, schemas={result}")
            return result
        except Exception as e:
            logger.error(f"[AS400] Error in get_schema_names: {e}")
            logger.exception("Stack trace in AS400 get_schema_names")
            raise

    # Retrieves a list of table names for a given schema
    @reflection.cache
    @log_entry_exit
    def get_table_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(f"[AS400] Fetching table names -> schema={current_schema}")
            systbl = self.sys_tables
            if version_info[0] < 3:
                logger.debug("[AS400] Using unicode branch for table type filter")
                query = (
                    sql.select(systbl.c.tabname)
                    .where(systbl.c.tabtype == unicode('T'))
                    .where(systbl.c.tabschema == current_schema)
                    .order_by(systbl.c.tabname)
                )
            else:
                logger.debug("[AS400] Using str branch for table type filter")
                query = (
                    sql.select(systbl.c.tabname)
                    .where(systbl.c.tabtype == str('T'))
                    .where(systbl.c.tabschema == current_schema)
                    .order_by(systbl.c.tabname)
                )
            logger.debug(f"[AS400] Generated get_table_names SQL -> {query}")
            result = [
                self.normalize_name(r[0])
                for r in connection.execute(query)
            ]
            logger.debug(f"[AS400] Reflected tables -> count={len(result)}, tables={result}")
            return result
        except Exception as e:
            logger.error(f"[AS400] Error in get_table_names: {e}")
            logger.exception("Stack trace in AS400 get_table_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_view_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(f"[AS400] Fetching view names -> schema={current_schema}")
            query = (
                sql.select(self.sys_views.c.viewname)
                .where(self.sys_views.c.viewschema == current_schema)
                .order_by(self.sys_views.c.viewname)
            )
            logger.debug(f"[AS400] Generated get_view_names SQL -> {query}")
            result = [
                self.normalize_name(r[0])
                for r in connection.execute(query)
            ]
            logger.debug(f"[AS400] Reflected views -> count={len(result)}, views={result}")
            return result
        except Exception as e:
            logger.error(f"[AS400] Error in get_view_names: {e}")
            logger.exception("Stack trace in AS400 get_view_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            viewname = self.denormalize_name(viewname)
            logger.debug(
                f"[AS400] Fetching view definition -> "
                f"schema={current_schema}, view={viewname}"
            )
            query = (
                sql.select(self.sys_views.c.text)
                .where(self.sys_views.c.viewschema == current_schema)
                .where(self.sys_views.c.viewname == viewname)
            )
            logger.debug(f"[AS400] Generated get_view_definition SQL -> {query}")
            definition = connection.execute(query).scalar()
            logger.debug(
                f"[AS400] View definition length -> "
                f"{len(definition) if definition else 0}"
            )
            return definition
        except Exception as e:
            logger.error(f"[AS400] Error in get_view_definition: {e}")
            logger.exception("Stack trace in AS400 get_view_definition")
            raise

    @reflection.cache
    @log_entry_exit
    def get_columns(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"[AS400] Fetching columns -> "
                f"schema={current_schema}, table={table_name}"
            )
            syscols = self.sys_columns
            query = (
                sql.select(
                    syscols.c.colname, syscols.c.typename,
                    syscols.c.defaultval, syscols.c.nullable,
                    syscols.c.length, syscols.c.scale,
                    syscols.c.isid, syscols.c.idgenerate,
                    syscols.c.remark
                )
                .where(and_(
                    syscols.c.tabschema == current_schema,
                    syscols.c.tabname == table_name
                ))
                .order_by(syscols.c.colno)
            )
            logger.debug(f"[AS400] Generated get_columns SQL -> {query}")
            sa_columns = []
            for r in connection.execute(query):
                raw_type = r[1].upper()
                logger.debug(
                    f"[AS400] Processing column -> "
                    f"name={r[0]}, type={raw_type}, "
                    f"length={r[4]}, scale={r[5]}"
                )
                if raw_type in ['DECIMAL', 'NUMERIC']:
                    coltype = self.ischema_names.get(raw_type)(int(r[4]), int(r[5]))
                elif raw_type in ['CHARACTER', 'CHAR', 'VARCHAR',
                                  'GRAPHIC', 'VARGRAPHIC']:
                    coltype = self.ischema_names.get(raw_type)(int(r[4]))
                else:
                    try:
                        coltype = self.ischema_names[raw_type]
                    except KeyError:
                        logger.warning(
                            f"[AS400] Unrecognized type '{raw_type}' "
                            f"for column '{r[0]}'"
                        )
                        coltype = sa_types.NULLTYPE
                if version_info[0] < 3:
                    nullable_flag = r[3] == unicode('Y')
                    autoinc_flag = (r[6] == unicode('YES')) and (r[7] is not None)
                else:
                    nullable_flag = r[3] == str('Y')
                    autoinc_flag = (r[6] == str('YES')) and (r[7] is not None)
                column_info = {
                    'name': self.normalize_name(r[0]),
                    'type': coltype,
                    'nullable': nullable_flag,
                    'default': r[2],
                    'autoincrement': autoinc_flag,
                    'comment': r[8] or None,
                }
                logger.debug(f"[AS400] Column reflected -> {column_info}")
                sa_columns.append(column_info)
            logger.debug(f"[AS400] Total columns reflected -> count={len(sa_columns)}")
            return sa_columns
        except Exception as e:
            logger.error(f"[AS400] Error reflecting columns: {e}")
            logger.exception("Stack trace in AS400 get_columns")
            raise

    @reflection.cache
    @log_entry_exit
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"[AS400] Fetching PK constraint -> "
                f"schema={current_schema}, table={table_name}"
            )
            sysconst = self.sys_table_constraints
            syskeyconst = self.sys_key_constraints
            query = (
                sql.select(syskeyconst.c.colname, sysconst.c.tabname, sysconst.c.conname)
                .where(and_(
                    syskeyconst.c.conschema == sysconst.c.conschema,
                    syskeyconst.c.conname == sysconst.c.conname,
                    sysconst.c.tabschema == current_schema,
                    sysconst.c.tabname == table_name,
                    sysconst.c.contype == 'PRIMARY KEY'
                )).order_by(syskeyconst.c.colno)
            )
            logger.debug(f"[AS400] Generated get_pk_constraint SQL -> {query}")
            pk_columns = []
            pk_name = None
            for key in connection.execute(query):
                pk_columns.append(self.normalize_name(key[0]))
                if not pk_name:
                    pk_name = self.normalize_name(key[2])
            logger.debug(f"[AS400] PK reflected -> name={pk_name}, columns={pk_columns}")
            return {"constrained_columns": pk_columns, "name": pk_name}
        except Exception as e:
            logger.error(f"[AS400] Error reflecting PK constraint: {e}")
            logger.exception("Stack trace in AS400 get_pk_constraint")
            raise

    @reflection.cache
    @log_entry_exit
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"[AS400] Fetching primary keys -> "
                f"schema={current_schema}, table={table_name}"
            )
            sysconst = self.sys_table_constraints
            syskeyconst = self.sys_key_constraints
            if version_info[0] < 3:
                logger.debug("[AS400] Using unicode branch for PK lookup")
                query = (
                    sql.select(syskeyconst.c.colname, sysconst.c.tabname)
                    .where(and_(
                        syskeyconst.c.conschema == sysconst.c.conschema,
                        syskeyconst.c.conname == sysconst.c.conname,
                        sysconst.c.tabschema == current_schema,
                        sysconst.c.tabname == table_name,
                        sysconst.c.contype == unicode('PRIMARY KEY')
                    ))
                    .order_by(syskeyconst.c.colno)
                )
            else:
                logger.debug("[AS400] Using str branch for PK lookup")
                query = (
                    sql.select(syskeyconst.c.colname, sysconst.c.tabname)
                    .where(and_(
                        syskeyconst.c.conschema == sysconst.c.conschema,
                        syskeyconst.c.conname == sysconst.c.conname,
                        sysconst.c.tabschema == current_schema,
                        sysconst.c.tabname == table_name,
                        sysconst.c.contype == str('PRIMARY KEY')
                    ))
                    .order_by(syskeyconst.c.colno)
                )
            logger.debug(f"[AS400] Generated get_primary_keys SQL -> {query}")
            result = [
                self.normalize_name(key[0])
                for key in connection.execute(query)
            ]
            logger.debug(f"[AS400] Primary keys reflected -> {result}")
            return result
        except Exception as e:
            logger.error(f"[AS400] Error reflecting primary keys: {e}")
            logger.exception("Stack trace in AS400 get_primary_keys")
            raise

    @reflection.cache
    @log_entry_exit
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        try:
            default_schema = self.default_schema_name
            current_schema = self.denormalize_name(schema or default_schema)
            normalized_default_schema = self.normalize_name(default_schema)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"[AS400] Fetching foreign keys -> "
                f"schema={current_schema}, table={table_name}"
            )
            sysfkeys = self.sys_foreignkeys
            query = (
                sql.select(
                    sysfkeys.c.fkname, sysfkeys.c.fktabschema,
                    sysfkeys.c.fktabname, sysfkeys.c.fkcolname,
                    sysfkeys.c.pkname, sysfkeys.c.pktabschema,
                    sysfkeys.c.pktabname, sysfkeys.c.pkcolname
                )
                .where(and_(
                    sysfkeys.c.fktabschema == current_schema,
                    sysfkeys.c.fktabname == table_name
                ))
                .order_by(sysfkeys.c.colno)
            )
            logger.debug(f"[AS400] Generated get_foreign_keys SQL -> {query}")
            fschema = {}
            for r in connection.execute(query):
                fk_name = r[0]
                if fk_name not in fschema:
                    referred_schema = self.normalize_name(r[5])
                    # if no schema specified and referred schema here is the
                    # default, then set to None
                    if schema is None and \
                            referred_schema == normalized_default_schema:
                        referred_schema = None
                    fschema[fk_name] = {
                        'name': self.normalize_name(fk_name),
                        'constrained_columns': [self.normalize_name(r[3])],
                        'referred_schema': referred_schema,
                        'referred_table': self.normalize_name(r[6]),
                        'referred_columns': [self.normalize_name(r[7])]
                    }
                    logger.debug(f"[AS400] Foreign key discovered -> {fschema[fk_name]}")
                else:
                    fschema[fk_name]['constrained_columns'].append(self.normalize_name(r[3]))
                    fschema[fk_name]['referred_columns'].append(self.normalize_name(r[7]))
            result = list(fschema.values())
            logger.debug(f"[AS400] Total foreign keys reflected -> count={len(result)}")
            return result
        except Exception as e:
            logger.error(f"[AS400] Error reflecting foreign keys: {e}")
            logger.exception("Stack trace in AS400 get_foreign_keys")
            raise

    # Retrieves a list of index names for a given schema
    @reflection.cache
    @log_entry_exit
    def get_indexes(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"[AS400] Fetching indexes -> "
                f"schema={current_schema}, table={table_name}"
            )
            sysidx = self.sys_indexes
            syskey = self.sys_keys
            query = (
                sql.select(
                    sysidx.c.indname,
                    sysidx.c.uniquerule,
                    syskey.c.colname
                )
                .where(and_(
                    syskey.c.indschema == sysidx.c.indschema,
                    syskey.c.indname == sysidx.c.indname,
                    sysidx.c.tabschema == current_schema,
                    sysidx.c.tabname == table_name
                ))
                .order_by(syskey.c.indname, syskey.c.colno)
            )
            logger.debug(f"[AS400] Generated get_indexes SQL -> {query}")
            indexes = {}
            for r in connection.execute(query):
                index_name_raw = r[0]
                unique_flag_raw = r[1]
                column_raw = r[2]
                key = index_name_raw.upper()
                logger.debug(
                    f"[AS400] Processing index row -> "
                    f"name={index_name_raw}, "
                    f"unique_flag={unique_flag_raw}, "
                    f"column={column_raw}"
                )
                if key in indexes:
                    indexes[key]['column_names'].append(self.normalize_name(column_raw))
                else:
                    if version_info[0] < 3:
                        is_unique = unique_flag_raw == unicode('Y')
                    else:
                        is_unique = unique_flag_raw == str('Y')
                    indexes[key] = {
                        'name': self.normalize_name(index_name_raw),
                        'column_names': [self.normalize_name(column_raw)],
                        'unique': is_unique
                    }
                    logger.debug(
                        f"[AS400] New index discovered -> "
                        f"{indexes[key]}"
                    )
            result = list(indexes.values())
            logger.debug(f"[AS400] Total indexes reflected -> count={len(result)}")
            return result
        except Exception as e:
            logger.error(f"[AS400] Error reflecting indexes: {e}")
            logger.exception("Stack trace in AS400 get_indexes")
            raise

    @reflection.cache
    @log_entry_exit
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        logger.debug(
            f"[AS400] get_unique_constraints invoked -> "
            f"schema={schema}, table={table_name}"
        )
        uniqueConsts = []
        logger.debug(
            "[AS400] Unique constraints not implemented for AS400 "
            "(returning empty list)"
        )
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

    @log_entry_exit
    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(
                f"Checking table existence (OS390) -> "
                f"schema={current_schema}, table={table_name}"
            )
            if current_schema:
                whereclause = sql.and_(
                    self.sys_tables.c.tabschema == current_schema,
                    self.sys_tables.c.tabname == table_name
                )
            else:
                whereclause = self.sys_tables.c.tabname == table_name
            s = sql.select(self.sys_tables.c.tabname).where(whereclause)
            logger.debug(f"has_table SQL -> {s}")
            result = connection.execute(s).first() is not None
            logger.debug(f"has_table result -> {result}")
            return result
        except Exception:
            logger.exception("Error in has_table (OS390)")
            raise

    @log_entry_exit
    def has_sequence(self, connection, sequence_name, schema=None):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            sequence_name = self.denormalize_name(sequence_name)
            logger.debug(
                f"Checking sequence existence (OS390) -> "
                f"schema={current_schema}, sequence={sequence_name}"
            )
            if current_schema:
                whereclause = sql.and_(
                    self.sys_sequences.c.seqschema == current_schema,
                    self.sys_sequences.c.seqname == sequence_name
                )
            else:
                whereclause = self.sys_sequences.c.seqname == sequence_name
            s = sql.select(self.sys_sequences.c.seqname).where(whereclause)
            logger.debug(f"has_sequence SQL -> {s}")
            result = connection.execute(s).first() is not None
            logger.debug(f"has_sequence result -> {result}")
            return result
        except Exception:
            logger.exception("Error in has_sequence (OS390)")
            raise

    @reflection.cache
    @log_entry_exit
    def get_sequence_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(f"Fetching sequence names (OS390) -> schema={current_schema}")
            sys_sequence = self.sys_sequences
            query = (
                sql.select(sys_sequence.c.seqname)
                .where(sys_sequence.c.seqschema == current_schema)
                .order_by(sys_sequence.c.seqschema, sys_sequence.c.seqname)
            )
            logger.debug(f"get_sequence_names SQL -> {query}")
            result = [self.normalize_name(r[0]) for r in connection.execute(query)]
            logger.debug(f"Sequences found -> count={len(result)}")
            return result
        except Exception:
            logger.exception("Error in get_sequence_names (OS390)")
            raise

    @reflection.cache
    @log_entry_exit
    def get_schema_names(self, connection, **kw):
        try:
            logger.debug("[OS390] get_schema_names invoked")
            sysschema = self.sys_tables
            query = sql.select(sysschema.c.tabschema). \
                where(not_(sysschema.c.tabschema.like('SYS%'))). \
                distinct(sysschema.c.tabschema)
            logger.debug(f"[OS390] get_schema_names SQL -> {query}")
            result = [
                self.normalize_name(r[0].rstrip())
                for r in connection.execute(query)
            ]
            logger.debug(f"[OS390] schemas found -> count={len(result)}")
            return result
        except Exception:
            logger.exception("[OS390] Error in get_schema_names")
            raise

    @log_entry_exit
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"[OS390] get_table_comment -> schema={current_schema}, table={table_name}")
            systbl = self.sys_tables
            query = sql.select(systbl.c.remarks). \
                where(systbl.c.tabschema == current_schema). \
                where(systbl.c.tabname == table_name)
            logger.debug(f"[OS390] get_table_comment SQL -> {query}")
            comment = connection.execute(query).scalar()
            logger.debug(f"[OS390] table comment -> {comment}")
            return {'text': comment}
        except Exception:
            logger.exception("[OS390] Error in get_table_comment")
            raise

    @reflection.cache
    @log_entry_exit
    def get_table_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(f"[OS390] get_table_names -> schema={current_schema}")
            systbl = self.sys_tables
            query = sql.select(systbl.c.tabname). \
                where(systbl.c.type == 'T'). \
                where(systbl.c.tabschema == current_schema). \
                order_by(systbl.c.tabname)
            logger.debug(f"[OS390] get_table_names SQL -> {query}")
            result = [self.normalize_name(r[0]) for r in connection.execute(query)]
            logger.debug(f"[OS390] tables found -> count={len(result)}")
            return result
        except Exception:
            logger.exception("[OS390] Error in get_table_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_view_names(self, connection, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            logger.debug(f"[OS390] get_view_names -> schema={current_schema}")
            query = sql.select(self.sys_views.c.viewname). \
                where(self.sys_views.c.viewschema == current_schema). \
                order_by(self.sys_views.c.viewname)
            logger.debug(f"[OS390] get_view_names SQL -> {query}")
            result = [self.normalize_name(r[0]) for r in connection.execute(query)]
            logger.debug(f"[OS390] views found -> count={len(result)}")
            return result
        except Exception:
            logger.exception("[OS390] Error in get_view_names")
            raise

    @reflection.cache
    @log_entry_exit
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            viewname = self.denormalize_name(viewname)
            logger.debug(
                f"[OS390] get_view_definition -> "
                f"schema={current_schema}, view={viewname}"
            )
            query = sql.select(self.sys_views.c.text). \
                where(self.sys_views.c.viewschema == current_schema). \
                where(self.sys_views.c.viewname == viewname)
            logger.debug(f"[OS390] get_view_definition SQL -> {query}")
            result = connection.execute(query).scalar()
            logger.debug(
                f"[OS390] view definition length -> "
                f"{len(result) if result else 0}"
            )
            return result
        except Exception:
            logger.exception("[OS390] Error in get_view_definition")
            raise

    @reflection.cache
    @log_entry_exit
    def get_columns(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"[OS390] get_columns -> schema={current_schema}, table={table_name}")
            syscols = self.sys_columns
            query = sql.select(syscols.c.colname, syscols.c.typename,
                               syscols.c.defaultval, syscols.c.nullable,
                               syscols.c.length, syscols.c.scale,
                               syscols.c.generated, syscols.c.remark). \
                where(and_(
                syscols.c.tabschema == current_schema,
                syscols.c.tabname == table_name)). \
                order_by(syscols.c.colno)
            logger.debug(f"[OS390] get_columns SQL -> {query}")
            sa_columns = []
            for r in connection.execute(query):
                rowtype = r[1].upper()
                logger.debug(f"[OS390] Processing column -> name={r[0]}, raw_type={rowtype}")
                if rowtype in ['DECIMAL', 'NUMERIC']:
                    coltype = self.ischema_names.get(rowtype)(int(r[4]), int(r[5]))
                elif rowtype in ['CHARACTER', 'CHAR', 'VARCHAR',
                                 'GRAPHIC', 'VARGRAPHIC']:
                    coltype = self.ischema_names.get(rowtype)(int(r[4]))
                else:
                    try:
                        coltype = self.ischema_names[rowtype]
                    except KeyError:
                        logger.warning(f"[OS390] Unknown type '{rowtype}' for column '{r[0]}'")
                        util.warn(
                            "Did not recognize type '%s' of column '%s'" %
                            (rowtype, r[0])
                        )
                        coltype = sa_types.NULLTYPE
                sa_columns.append({
                    'name': self.normalize_name(r[0]),
                    'type': coltype,
                    'nullable': r[3] == 'Y',
                    'default': r[2] or None,
                    'autoincrement': (r[2] == 'J') and (r[2] != ' '),
                    'comment': r[7] or None,
                })
            logger.debug(f"[OS390] get_columns completed -> count={len(sa_columns)}")
            return sa_columns
        except Exception:
            logger.exception("[OS390] Error in get_columns")
            raise

    @reflection.cache
    @log_entry_exit
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"[OS390] get_pk_constraint -> schema={current_schema}, table={table_name}")
            sysindexes = self.sys_columns
            col_finder = re.compile(r"(\w+)")
            query = sql.select(sysindexes.c.colname). \
                where(and_(
                sysindexes.c.tabschema == current_schema,
                sysindexes.c.tabname == table_name,
                sysindexes.c.keyseq > 0)). \
                order_by(sysindexes.c.tabschema, sysindexes.c.tabname)
            logger.debug(f"[OS390] get_pk_constraint SQL -> {query}")
            pk_columns = []
            for r in connection.execute(query):
                cols = col_finder.findall(r[0])
                pk_columns.extend(cols)
            result = {
                "constrained_columns": [self.normalize_name(col) for col in pk_columns],
                "name": None
            }
            logger.debug(f"[OS390] get_pk_constraint result -> {result}")
            return result
        except Exception:
            logger.exception("[OS390] Error in get_pk_constraint")
            raise

    @reflection.cache
    @log_entry_exit
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"[OS390] get_primary_keys -> schema={current_schema}, table={table_name}")
            sysindexes = self.sys_columns
            col_finder = re.compile(r"(\w+)")
            query = sql.select(sysindexes.c.colname). \
                where(and_(
                sysindexes.c.tabschema == current_schema,
                sysindexes.c.tabname == table_name,
                sysindexes.c.keyseq > 0)). \
                order_by(sysindexes.c.tabschema, sysindexes.c.tabname)
            logger.debug(f"[OS390] get_primary_keys SQL -> {query}")
            pk_columns = []
            for r in connection.execute(query):
                cols = col_finder.findall(r[0])
                pk_columns.extend(cols)
            result = [self.normalize_name(col) for col in pk_columns]
            logger.debug(f"[OS390] get_primary_keys result -> {result}")
            return result
        except Exception:
            logger.exception("[OS390] Error in get_primary_keys")
            raise

    @reflection.cache
    @log_entry_exit
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        try:
            default_schema = self.default_schema_name
            current_schema = self.denormalize_name(schema or default_schema)
            default_schema = self.normalize_name(default_schema)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"[OS390] get_foreign_keys -> schema={current_schema}, table={table_name}")
            sysfkeys = self.sys_foreignkeys
            sysrels = self.sys_rels
            syscolspk = self.sys_columns
            sysindex = self.sys_indexes
            query = sql.select(
                sysrels.c.fkname, sysrels.c.fktabschema,
                sysrels.c.fktabname, sysfkeys.c.fkcolname,
                sysindex.c.indname, sysrels.c.pktabschema,
                sysrels.c.pktabname, syscolspk.c.colname). \
                where(and_(
                sysrels.c.fktabschema == current_schema,
                sysrels.c.fktabname == table_name,
                sysrels.c.fktabname == sysfkeys.c.fktabname,
                sysrels.c.pktabname == syscolspk.c.tabname,
                syscolspk.c.tabname == sysindex.c.tabname,
                syscolspk.c.keyseq > 0)). \
                order_by(sysfkeys.c.colno)
            logger.debug(f"[OS390] get_foreign_keys SQL -> {query}")
            fschema = {}
            for r in connection.execute(query):
                if r[0] not in fschema:
                    referred_schema = self.normalize_name(r[5])
                    # if no schema specified and referred schema here is the
                    # default, then set to None
                    if schema is None and referred_schema == default_schema:
                        referred_schema = None
                    fschema[r[0]] = {
                        'name': self.normalize_name(r[0]),
                        'constrained_columns': [self.normalize_name(r[3])],
                        'referred_schema': referred_schema,
                        'referred_table': self.normalize_name(r[6]),
                        'referred_columns': [self.normalize_name(r[7])]
                    }
                else:
                    fschema[r[0]]['constrained_columns'].append(self.normalize_name(r[3]))
                    fschema[r[0]]['referred_columns'].append(self.normalize_name(r[7]))
            result = [value for key, value in fschema.items()]
            logger.debug(f"[OS390] get_foreign_keys result count -> {len(result)}")
            return result
        except Exception:
            logger.exception("[OS390] Error in get_foreign_keys")
            raise

    @reflection.cache
    @log_entry_exit
    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        try:
            default_schema = self.default_schema_name
            current_schema = self.denormalize_name(schema or default_schema)
            default_schema = self.normalize_name(default_schema)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"[OS390] get_incoming_foreign_keys -> schema={current_schema}, table={table_name}")
            sysfkeys = self.sys_foreignkeys
            sysrels = self.sys_rels
            syscolspk = self.sys_columns
            sysindex = self.sys_indexes
            query = sql.select(
                sysrels.c.fkname, sysrels.c.fktabschema,
                sysrels.c.fktabname, sysfkeys.c.fkcolname,
                sysindex.c.indname, sysrels.c.pktabschema,
                sysrels.c.pktabname, syscolspk.c.colname). \
                where(and_(
                syscolspk.c.tabschema == current_schema,
                syscolspk.c.tabname == table_name,
                sysrels.c.fktabname == sysfkeys.c.fktabname,
                sysrels.c.pktabname == syscolspk.c.tabname,
                syscolspk.c.tabname == sysindex.c.tabname,
                syscolspk.c.keyseq > 0)). \
                order_by(sysfkeys.c.colno)
            logger.debug(f"[OS390] get_incoming_foreign_keys SQL -> {query}")
            fschema = {}
            for r in connection.execute(query):
                if r[0] not in fschema:
                    constrained_schema = self.normalize_name(r[1])
                    # if no schema specified and referred schema here is the
                    # default, then set to None
                    if schema is None and constrained_schema == default_schema:
                        constrained_schema = None
                    fschema[r[0]] = {
                        'name': self.normalize_name(r[0]),
                        'constrained_schema': constrained_schema,
                        'constrained_table': self.normalize_name(r[2]),
                        'constrained_columns': [self.normalize_name(r[3])],
                        'referred_schema': schema,
                        'referred_table': self.normalize_name(r[6]),
                        'referred_columns': [self.normalize_name(r[7])]
                    }
                else:
                    fschema[r[0]]['constrained_columns'].append(self.normalize_name(r[3]))
                    fschema[r[0]]['referred_columns'].append(self.normalize_name(r[7]))
            result = [value for key, value in fschema.items()]
            logger.debug(f"[OS390] get_incoming_foreign_keys result count -> {len(result)}")
            return result
        except Exception:
            logger.exception("[OS390] Error in get_incoming_foreign_keys")
            raise

    @reflection.cache
    @log_entry_exit
    def get_indexes(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"[OS390] get_indexes -> schema={current_schema}, table={table_name}")
            sysidx = self.sys_indexes
            syscolpk = self.sys_columns
            query = sql.select(
                sysidx.c.indname, syscolpk.c.colname,
                sysidx.c.uniquerule, sysidx.c.system_required). \
                where(and_(
                sysidx.c.tabschema == current_schema,
                sysidx.c.tabname == table_name,
                syscolpk.c.colname == sysidx.c.tabname,
                syscolpk.c.keyseq > 0)). \
                order_by(sysidx.c.tabname)
            logger.debug(f"[OS390] get_indexes SQL -> {query}")
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
            logger.debug(f"[OS390] get_indexes result count -> {len(indexes)}")
            return indexes
        except Exception:
            logger.exception("[OS390] Error in get_indexes")
            raise

    @reflection.cache
    @log_entry_exit
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        try:
            current_schema = self.denormalize_name(schema or self.default_schema_name)
            table_name = self.denormalize_name(table_name)
            logger.debug(f"[OS390] get_unique_constraints -> schema={current_schema}, table={table_name}")
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
            logger.debug(f"[OS390] get_unique_constraints SQL -> {query}")
            uniqueConsts = []
            currConst = None
            for r in connection.execute(query):
                if currConst == r[0]:
                    uniqueConsts[-1]["column_names"].append(self.normalize_name(r[1]))
                else:
                    currConst = r[0]
                    uniqueConsts.append({
                        "name": self.normalize_name(currConst),
                        "column_names": [self.normalize_name(r[1])],
                    })
            logger.debug(f"[OS390] get_unique_constraints result count -> {len(uniqueConsts)}")
            return uniqueConsts
        except Exception:
            logger.exception("[OS390] Error in get_unique_constraints")
            raise
