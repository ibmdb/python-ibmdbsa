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
# | Contributors: Jaimy Azle, Mike Bayer                                     |
# +--------------------------------------------------------------------------+
"""Support for IBM DB2 database

"""
import datetime, re
from sqlalchemy import types as sa_types
from sqlalchemy import schema as sa_schema
from sqlalchemy import util
from sqlalchemy.sql import compiler
from sqlalchemy.sql import operators
from sqlalchemy.engine import default
from sqlalchemy import __version__ as SA_Version
from . import reflection as ibm_reflection

from sqlalchemy.types import BLOB, CHAR, CLOB, DATE, DATETIME, INTEGER,\
    SMALLINT, BIGINT, DECIMAL, NUMERIC, REAL, TIME, TIMESTAMP,\
    VARCHAR, FLOAT

SA_Version = [int(ver_token) for ver_token in SA_Version.split('.')[0:2]]

# as documented from:
# http://publib.boulder.ibm.com/infocenter/db2luw/v9/index.jsp?topic=/com.ibm.db2.udb.doc/admin/r0001095.htm
RESERVED_WORDS = set(
   ['activate', 'disallow', 'locale', 'result', 'add', 'disconnect', 'localtime',
    'result_set_locator', 'after', 'distinct', 'localtimestamp', 'return', 'alias',
    'do', 'locator', 'returns', 'all', 'double', 'locators', 'revoke', 'allocate', 'drop',
    'lock', 'right', 'allow', 'dssize', 'lockmax', 'rollback', 'alter', 'dynamic',
    'locksize', 'routine', 'and', 'each', 'long', 'row', 'any', 'editproc', 'loop',
    'row_number', 'as', 'else', 'maintained', 'rownumber', 'asensitive', 'elseif',
    'materialized', 'rows', 'associate', 'enable', 'maxvalue', 'rowset', 'asutime',
    'encoding', 'microsecond', 'rrn', 'at', 'encryption', 'microseconds', 'run',
    'attributes', 'end', 'minute', 'savepoint', 'audit', 'end-exec', 'minutes', 'schema',
    'authorization', 'ending', 'minvalue', 'scratchpad', 'aux', 'erase', 'mode', 'scroll',
    'auxiliary', 'escape', 'modifies', 'search', 'before', 'every', 'month', 'second',
    'begin', 'except', 'months', 'seconds', 'between', 'exception', 'new', 'secqty',
    'binary', 'excluding', 'new_table', 'security', 'bufferpool', 'exclusive',
    'nextval', 'select', 'by', 'execute', 'no', 'sensitive', 'cache', 'exists', 'nocache',
    'sequence', 'call', 'exit', 'nocycle', 'session', 'called', 'explain', 'nodename',
    'session_user', 'capture', 'external', 'nodenumber', 'set', 'cardinality',
    'extract', 'nomaxvalue', 'signal', 'cascaded', 'fenced', 'nominvalue', 'simple',
    'case', 'fetch', 'none', 'some', 'cast', 'fieldproc', 'noorder', 'source', 'ccsid',
    'file', 'normalized', 'specific', 'char', 'final', 'not', 'sql', 'character', 'for',
    'null', 'sqlid', 'check', 'foreign', 'nulls', 'stacked', 'close', 'free', 'numparts',
    'standard', 'cluster', 'from', 'obid', 'start', 'collection', 'full', 'of', 'starting',
    'collid', 'function', 'old', 'statement', 'column', 'general', 'old_table', 'static',
    'comment', 'generated', 'on', 'stay', 'commit', 'get', 'open', 'stogroup', 'concat',
    'global', 'optimization', 'stores', 'condition', 'go', 'optimize', 'style', 'connect',
    'goto', 'option', 'substring', 'connection', 'grant', 'or', 'summary', 'constraint',
    'graphic', 'order', 'synonym', 'contains', 'group', 'out', 'sysfun', 'continue',
    'handler', 'outer', 'sysibm', 'count', 'hash', 'over', 'sysproc', 'count_big',
    'hashed_value', 'overriding', 'system', 'create', 'having', 'package',
    'system_user', 'cross', 'hint', 'padded', 'table', 'current', 'hold', 'pagesize',
    'tablespace', 'current_date', 'hour', 'parameter', 'then', 'current_lc_ctype',
    'hours', 'part', 'time', 'current_path', 'identity', 'partition', 'timestamp',
    'current_schema', 'if', 'partitioned', 'to', 'current_server', 'immediate',
    'partitioning', 'transaction', 'current_time', 'in', 'partitions', 'trigger',
    'current_timestamp', 'including', 'password', 'trim', 'current_timezone',
    'inclusive', 'path', 'type', 'current_user', 'increment', 'piecesize', 'undo',
    'cursor', 'index', 'plan', 'union', 'cycle', 'indicator', 'position', 'unique', 'data',
    'inherit', 'precision', 'until', 'database', 'inner', 'prepare', 'update',
    'datapartitionname', 'inout', 'prevval', 'usage', 'datapartitionnum',
    'insensitive', 'primary', 'user', 'date', 'insert', 'priqty', 'using', 'day',
    'integrity', 'privileges', 'validproc', 'days', 'intersect', 'procedure', 'value',
    'db2general', 'into', 'program', 'values', 'db2genrl', 'is', 'psid', 'variable',
    'db2sql', 'isobid', 'query', 'variant', 'dbinfo', 'isolation', 'queryno', 'vcat',
    'dbpartitionname', 'iterate', 'range', 'version', 'dbpartitionnum', 'jar', 'rank',
    'view', 'deallocate', 'java', 'read', 'volatile', 'declare', 'join', 'reads', 'volumes',
    'default', 'key', 'recovery', 'when', 'defaults', 'label', 'references', 'whenever',
    'definition', 'language', 'referencing', 'where', 'delete', 'lateral', 'refresh',
    'while', 'dense_rank', 'lc_ctype', 'release', 'with', 'denserank', 'leave', 'rename',
    'without', 'describe', 'left', 'repeat', 'wlm', 'descriptor', 'like', 'reset', 'write',
    'deterministic', 'linktype', 'resignal', 'xmlelement', 'diagnostics', 'local',
    'restart', 'year', 'disable', 'localdate', 'restrict', 'years', '', 'abs', 'grouping',
    'regr_intercept', 'are', 'int', 'regr_r2', 'array', 'integer', 'regr_slope',
    'asymmetric', 'intersection', 'regr_sxx', 'atomic', 'interval', 'regr_sxy', 'avg',
    'large', 'regr_syy', 'bigint', 'leading', 'rollup', 'blob', 'ln', 'scope', 'boolean',
    'lower', 'similar', 'both', 'match', 'smallint', 'ceil', 'max', 'specifictype',
    'ceiling', 'member', 'sqlexception', 'char_length', 'merge', 'sqlstate',
    'character_length', 'method', 'sqlwarning', 'clob', 'min', 'sqrt', 'coalesce', 'mod',
    'stddev_pop', 'collate', 'module', 'stddev_samp', 'collect', 'multiset',
    'submultiset', 'convert', 'national', 'sum', 'corr', 'natural', 'symmetric',
    'corresponding', 'nchar', 'tablesample', 'covar_pop', 'nclob', 'timezone_hour',
    'covar_samp', 'normalize', 'timezone_minute', 'cube', 'nullif', 'trailing',
    'cume_dist', 'numeric', 'translate', 'current_default_transform_group',
    'octet_length', 'translation', 'current_role', 'only', 'treat',
    'current_transform_group_for_type', 'overlaps', 'true', 'dec', 'overlay',
    'uescape', 'decimal', 'percent_rank', 'unknown', 'deref', 'percentile_cont',
    'unnest', 'element', 'percentile_disc', 'upper', 'exec', 'power', 'var_pop', 'exp',
    'real', 'var_samp', 'false', 'recursive', 'varchar', 'filter', 'ref', 'varying',
    'float', 'regr_avgx', 'width_bucket', 'floor', 'regr_avgy', 'window', 'fusion',
    'regr_count', 'within', 'asc'])


class _IBM_Boolean(sa_types.Boolean):

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            else:
                return bool(value)
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            elif bool(value):
                return '1'
            else:
                return '0'
        return process

class _IBM_Date(sa_types.Date):

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return value
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return str(value)
        return process

class DOUBLE(sa_types.Numeric):
    __visit_name__ = 'DOUBLE'

class LONGVARCHAR(sa_types.VARCHAR):
    __visit_name_ = 'LONGVARCHAR'

class DBCLOB(sa_types.CLOB):
    __visit_name__ = "DBCLOB"

class GRAPHIC(sa_types.CHAR):
    __visit_name__ = "GRAPHIC"

class VARGRAPHIC(sa_types.Unicode):
    __visit_name__ = "VARGRAPHIC"


class LONGVARGRAPHIC(sa_types.UnicodeText):
    __visit_name__ = "LONGVARGRAPHIC"

class XML(sa_types.Text):
    __visit_name__ = "XML"

colspecs = {
    sa_types.Boolean: _IBM_Boolean,
    sa_types.Date: _IBM_Date
# really ?
#    sa_types.Unicode: DB2VARGRAPHIC
}

ischema_names = {
    'BLOB': BLOB,
    'CHAR': CHAR,
    'CHARACTER': CHAR,
    'CLOB': CLOB,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'INTEGER': INTEGER,
    'SMALLINT': SMALLINT,
    'BIGINT': BIGINT,
    'DECIMAL': DECIMAL,
    'NUMERIC': NUMERIC,
    'REAL': REAL,
    'DOUBLE': DOUBLE,
    'FLOAT': FLOAT,
    'TIME': TIME,
    'TIMESTAMP': TIMESTAMP,
    'VARCHAR': VARCHAR,
    'LONGVARCHAR': LONGVARCHAR,
    'XML': XML,
    'GRAPHIC': GRAPHIC,
    'VARGRAPHIC': VARGRAPHIC,
    'LONGVARGRAPHIC': LONGVARGRAPHIC,
    'DBCLOB': DBCLOB
}


class DB2TypeCompiler(compiler.GenericTypeCompiler):


    def visit_TIMESTAMP(self, type_):
        return "TIMESTAMP"

    def visit_DATE(self, type_):
        return "DATE"

    def visit_TIME(self, type_):
        return "TIME"

    def visit_DATETIME(self, type_):
        return self.visit_TIMESTAMP(type_)

    def visit_SMALLINT(self, type_):
        return "SMALLINT"

    def visit_INT(self, type_):
        return "INT"

    def visit_BIGINT(self, type_):
        return "BIGINT"

    def visit_FLOAT(self, type_):
        return "FLOAT" if type_.precision is None else \
                "FLOAT(%(precision)s)" % {'precision': type_.precision}

    def visit_DOUBLE(self, type_):
        return "DOUBLE"

    def visit_XML(self, type_):
        return "XML"

    def visit_CLOB(self, type_):
        return "CLOB"

    def visit_BLOB(self, type_):
        return "BLOB(1M)" if type_.length in (None, 0) else \
                "BLOB(%(length)s)" % {'length': type_.length}

    def visit_DBCLOB(self, type_):
        return "DBCLOB(1M)" if type_.length in (None, 0) else \
                "DBCLOB(%(length)s)" % {'length': type_.length}

    def visit_VARCHAR(self, type_):
        return "VARCHAR(%(length)s)" % {'length': type_.length}

    def visit_LONGVARCHAR(self, type_):
        return "LONG VARCHAR"

    def visit_VARGRAPHIC(self, type_):
        return "VARGRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_LONGVARGRAPHIC(self, type_):
        return "LONG VARGRAPHIC"

    def visit_CHAR(self, type_):
        return "CHAR" if type_.length in (None, 0) else \
                "CHAR(%(length)s)" % {'length': type_.length}

    def visit_GRAPHIC(self, type_):
        return "GRAPHIC" if type_.length in (None, 0) else \
                "GRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_DECIMAL(self, type_):
        if not type_.precision:
            return "DECIMAL(31, 0)"
        elif not type_.scale:
            return "DECIMAL(%(precision)s, 0)" % {'precision': type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                            'precision': type_.precision, 'scale': type_.scale}


    def visit_numeric(self, type_):
        return self.visit_DECIMAL(type_)

    def visit_datetime(self, type_):
        return self.visit_TIMESTAMP(type_)

    def visit_date(self, type_):
        return self.visit_DATE(type_)

    def visit_time(self, type_):
        return self.visit_TIME(type_)

    def visit_integer(self, type_):
        return self.visit_INT(type_)

    def visit_boolean(self, type_):
        return self.visit_SMALLINT(type_)

    def visit_float(self, type_):
        return self.visit_FLOAT(type_)

    def visit_unicode(self, type_):
        return self.visit_VARGRAPHIC(type_)

    def visit_unicode_text(self, type_):
        return self.visit_LONGVARGRAPHIC(type_)

    def visit_string(self, type_):
        return self.visit_VARCHAR(type_)

    def visit_TEXT(self, type_):
        return self.visit_CLOB(type_)

    def visit_large_binary(self, type_):
        return self.visit_BLOB(type_)


class DB2Compiler(compiler.SQLCompiler):

    if SA_Version < [0, 9]:
        def visit_false(self, expr, **kw):
            return '0'
            
        def visit_true(self, expr, **kw):
            return '1'
            
    def get_cte_preamble(self, recursive):
        return "WITH"
        
    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def for_update_clause(self, select):
        if select.for_update == True:
            return ' WITH RS USE AND KEEP UPDATE LOCKS'
        elif select.for_update == 'read':
            return ' WITH RS USE AND KEEP SHARE LOCKS'
        else:
            return ''

    def visit_mod_binary(self, binary, operator, **kw):
        return "mod(%s, %s)" % (self.process(binary.left),
                                                self.process(binary.right))

    def limit_clause(self, select):
        if (select._limit is not None) and (select._offset is None):
            return " FETCH FIRST %s ROWS ONLY" % select._limit
        else:
            return ""

    def visit_select(self, select, **kwargs):
        limit, offset = select._limit, select._offset
        sql_ori = compiler.SQLCompiler.visit_select(self, select, **kwargs)
        if offset is not None:
            __rownum = 'Z.__ROWNUM'
            sql_split = re.split("[\s+]FROM ", sql_ori, 1)
            sql_sec = ""
            sql_sec = " \nFROM %s " % ( sql_split[1] )

            dummyVal = "Z.__db2_"
            sql_pri = ""

            sql_sel = "SELECT "
            if select._distinct:
                sql_sel = "SELECT DISTINCT "

            sql_select_token = sql_split[0].split( "," )
            i = 0
            while ( i < len( sql_select_token ) ):
                if sql_select_token[i].count( "TIMESTAMP(DATE(SUBSTR(CHAR(" ) == 1:
                    sql_sel = "%s \"%s%d\"," % ( sql_sel, dummyVal, i + 1 )
                    sql_pri = '%s %s,%s,%s,%s AS "%s%d",' % (
                                    sql_pri,
                                    sql_select_token[i],
                                    sql_select_token[i + 1],
                                    sql_select_token[i + 2],
                                    sql_select_token[i + 3],
                                    dummyVal, i + 1 )
                    i = i + 4
                    continue

                if sql_select_token[i].count( " AS " ) == 1:
                    temp_col_alias = sql_select_token[i].split( " AS " )
                    sql_pri = '%s %s,' % ( sql_pri, sql_select_token[i] )
                    sql_sel = "%s %s," % ( sql_sel, temp_col_alias[1] )
                    i = i + 1
                    continue

                sql_pri = '%s %s AS "%s%d",' % ( sql_pri, sql_select_token[i], dummyVal, i + 1 )
                sql_sel = "%s \"%s%d\"," % ( sql_sel, dummyVal, i + 1 )
                i = i + 1

            sql_pri = sql_pri[:len( sql_pri ) - 1]
            sql_pri = "%s%s" % ( sql_pri, sql_sec )
            sql_sel = sql_sel[:len( sql_sel ) - 1]
            sql = '%s, ( ROW_NUMBER() OVER() ) AS "%s" FROM ( %s ) AS M' % ( sql_sel, __rownum, sql_pri )
            sql = '%s FROM ( %s ) Z WHERE' % ( sql_sel, sql )

            if offset is not 0:
                sql = '%s "%s" > %d' % ( sql, __rownum, offset )
            if offset is not 0 and limit is not None:
                sql = '%s AND ' % ( sql )
            if limit is not None:
                sql = '%s "%s" <= %d' % ( sql, __rownum, offset + limit )
            return "( %s )" % ( sql, )
        else:
            return sql_ori

    def visit_sequence(self, sequence):
        return "NEXT VALUE FOR %s" % sequence.name

    def default_from(self):
        # DB2 uses SYSIBM.SYSDUMMY1 table for row count
        return  " FROM SYSIBM.SYSDUMMY1"

    def visit_function(self, func, result_map=None, **kwargs):
        if func.name.upper() == "AVG":
            return "AVG(DOUBLE(%s))" % (self.function_argspec(func, **kwargs))
        elif func.name.upper() == "CHAR_LENGTH":
            return "CHAR_LENGTH(%s, %s)" % (self.function_argspec(func, **kwargs), 'OCTETS')
        else:
            return compiler.SQLCompiler.visit_function(self, func, **kwargs)
    # TODO: this is wrong but need to know what DB2 is expecting here
    #    if func.name.upper() == "LENGTH":
    #        return "LENGTH('%s')" % func.compile().params[func.name + '_1']
    #    else:
    #        return compiler.SQLCompiler.visit_function(self, func, **kwargs)


    def visit_cast(self, cast, **kw):
        type_ = cast.typeclause.type

        # TODO: verify that CAST shouldn't be called with
        # other types, I was able to CAST against VARCHAR
        # for example
        if isinstance(type_, (
                    sa_types.DateTime, sa_types.Date, sa_types.Time,
                    sa_types.DECIMAL)):
            return super(DB2Compiler, self).visit_cast(cast, **kw)
        else:
            return self.process(cast.clause)

    def get_select_precolumns(self, select):
        if isinstance(select._distinct, str):
            return select._distinct.upper() + " "
        elif select._distinct:
            return "DISTINCT "
        else:
            return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        # NOTE: this is the same method as that used in mysql/base.py
        # to render INNER JOIN
        return ''.join(
            (self.process(join.left, asfrom=True, **kwargs),
             (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN "),
             self.process(join.right, asfrom=True, **kwargs),
             " ON ",
             self.process(join.onclause, **kwargs)))

    def visit_savepoint(self, savepoint_stmt):
        return "SAVEPOINT %(sid)s ON ROLLBACK RETAIN CURSORS" % {'sid':self.preparer.format_savepoint(savepoint_stmt)}

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        return 'ROLLBACK TO SAVEPOINT %(sid)s'% {'sid':self.preparer.format_savepoint(savepoint_stmt)}

    def visit_release_savepoint(self, savepoint_stmt):
        return 'RELEASE TO SAVEPOINT %(sid)s'% {'sid':self.preparer.format_savepoint(savepoint_stmt)}
    
    def visit_unary(self, unary, **kw):
        if (unary.operator == operators.exists)  and kw.get('within_columns_clause', False):
            usql = super(DB2Compiler, self).visit_unary(unary, **kw)
            usql = "CASE WHEN " + usql + " THEN 1 ELSE 0 END"
            return usql
        else:
            return super(DB2Compiler, self).visit_unary(unary, **kw)

class DB2DDLCompiler(compiler.DDLCompiler):
    
    def get_server_version_info(self, dialect):
        """Returns the DB2 server major and minor version as a list of ints."""
        if hasattr(dialect, 'dbms_ver'):
            return [int(ver_token) for ver_token in dialect.dbms_ver.split('.')[0:2]]
        else:
            return []
    
    def _is_nullable_unique_constraint_supported(self, dialect):
        """Checks to see if the DB2 version is at least 10.5.
        This is needed for checking if unique constraints with null columns are supported.
        """
        if hasattr(dialect, 'dbms_name') and (dialect.dbms_name.find('DB2/') != -1):
            return self.get_server_version_info(dialect) >= [10, 5]
        else:
            return False

    def get_column_specification(self, column, **kw):
        col_spec = [self.preparer.format_column(column)]
        col_spec.append(self.dialect.type_compiler.process(column.type))


        # column-options: "NOT NULL"
        if not column.nullable or column.primary_key:
            col_spec.append('NOT NULL')

        # default-clause:
        default = self.get_column_default_string(column)
        if default is not None:
            col_spec.append('WITH DEFAULT')
            col_spec.append(default)

        if column is column.table._autoincrement_column:
            col_spec.append('GENERATED BY DEFAULT')
            col_spec.append('AS IDENTITY')
            col_spec.append('(START WITH 1)')

        column_spec = ' '.join(col_spec)
        return column_spec

    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        if constraint.onupdate is not None:
            util.warn(
                "DB2 does not support UPDATE CASCADE for foreign keys.")

        return text

    def visit_drop_constraint(self, drop, **kw):
        constraint = drop.element
        if isinstance(constraint, sa_schema.ForeignKeyConstraint):
            qual = "FOREIGN KEY "
            const = self.preparer.format_constraint(constraint)
        elif isinstance(constraint, sa_schema.PrimaryKeyConstraint):
            qual = "PRIMARY KEY "
            const = ""
        elif isinstance(constraint, sa_schema.UniqueConstraint):
            qual = "UNIQUE "
            if self._is_nullable_unique_constraint_supported(self.dialect):
                for column in constraint:
                    if column.nullable:
                        constraint.uConstraint_as_index = True
                if getattr(constraint, 'uConstraint_as_index', None):
                    qual = "INDEX "
            const = self.preparer.format_constraint(constraint)
        else:
            qual = ""
            const = self.preparer.format_constraint(constraint)
            
        if hasattr(constraint, 'uConstraint_as_index') and constraint.uConstraint_as_index:
            return "DROP %s%s" % \
                                (qual, const)
        return "ALTER TABLE %s DROP %s%s" % \
                                (self.preparer.format_table(constraint.table),
                                qual, const)
                                
    def create_table_constraints(self, table, **kw):
        if self._is_nullable_unique_constraint_supported(self.dialect):
            for constraint in table._sorted_constraints:
                if isinstance(constraint, sa_schema.UniqueConstraint):
                    for column in constraint:
                        if column.nullable:
                            constraint.use_alter = True
                            constraint.uConstraint_as_index = True
                            break
                    if getattr(constraint, 'uConstraint_as_index', None):
                        if not constraint.name:
                            index_name = "%s_%s_%s" % ('ukey', self.preparer.format_table(constraint.table), '_'.join(column.name for column in constraint))
                        else:
                            index_name = constraint.name
                        index = sa_schema.Index(index_name, *(column for column in constraint))
                        index.unique = True
                        index.uConstraint_as_index = True
        result = super( DB2DDLCompiler, self ).create_table_constraints(table, **kw)
        return result
    
    def visit_create_index(self, create, include_schema=True, include_table_schema=True):
        if SA_Version < [0, 8]:
            sql = super( DB2DDLCompiler, self ).visit_create_index(create)
        else:
            sql = super( DB2DDLCompiler, self ).visit_create_index(create, include_schema, include_table_schema)
        if getattr(create.element, 'uConstraint_as_index', None):
            sql += ' EXCLUDE NULL KEYS'
        return sql

    def visit_add_constraint(self, create):
        if self._is_nullable_unique_constraint_supported(self.dialect):
            if isinstance(create.element, sa_schema.UniqueConstraint):
                for column in create.element:
                    if column.nullable:
                        create.element.uConstraint_as_index = True
                        break
                if getattr(create.element, 'uConstraint_as_index', None):
                    if not create.element.name:
                        index_name = "%s_%s_%s" % ('uk_index', self.preparer.format_table(create.element.table), '_'.join(column.name for column in create.element))
                    else:
                        index_name = create.element.name
                    index = sa_schema.Index(index_name, *(column for column in create.element))
                    index.unique = True
                    index.uConstraint_as_index = True
                    sql = self.visit_create_index(sa_schema.CreateIndex(index)) 
                    return sql
        sql = super( DB2DDLCompiler, self ).visit_add_constraint(create)
        return sql
    
class DB2IdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = RESERVED_WORDS
    illegal_initial_characters = set(range(0, 10)).union(["_", "$"])


class DB2ExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        return self._execute_scalar("SELECT NEXTVAL FOR " +
                    self.dialect.identifier_preparer.format_sequence(seq) +
                    " FROM SYSIBM.SYSDUMMY1", type_)

class _SelectLastRowIDMixin(object):
    _select_lastrowid = False
    _lastrowid = None


    def get_lastrowid(self):
        return self._lastrowid

    def pre_exec(self):
        if self.isinsert:
            tbl = self.compiled.statement.table
            seq_column = tbl._autoincrement_column
            insert_has_sequence = seq_column is not None

            self._select_lastrowid = insert_has_sequence and \
                                        not self.compiled.returning and \
                                        not self.compiled.inline

    def post_exec(self):
        conn = self.root_connection
        if self._select_lastrowid:
            conn._cursor_execute(self.cursor,
                    "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1",
                    (), self)
            row = self.cursor.fetchall()[0]
            if row[0] is not None:
                self._lastrowid = int(row[0])


class DB2Dialect(default.DefaultDialect):

    name = 'ibm_db_sa'
    max_identifier_length = 128
    encoding = 'utf-8'
    default_paramstyle = 'qmark'
    colspecs = colspecs
    ischema_names = ischema_names
    supports_char_length = False
    supports_unicode_statements = False
    supports_unicode_binds = False
    returns_unicode_strings = False
    postfetch_lastrowid = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_decimal = False
    supports_native_boolean = False
    preexecute_sequences = False
    supports_alter = True
    supports_sequences = True
    sequences_optional = True

    requires_name_normalize = True

    supports_default_values = False
    supports_empty_insert = False

    two_phase_transactions = False
    savepoints =  True

    statement_compiler = DB2Compiler
    ddl_compiler = DB2DDLCompiler
    type_compiler = DB2TypeCompiler
    preparer = DB2IdentifierPreparer
    execution_ctx_cls = DB2ExecutionContext

    _reflector_cls = ibm_reflection.DB2Reflector

    def __init__(self, **kw):
        super(DB2Dialect, self).__init__(**kw)

        self._reflector = self._reflector_cls(self)

    # reflection: these all defer to an BaseDB2Reflector
    # object which selects between DB2 and AS/400 schemas
    def initialize(self, connection):
        super(DB2Dialect, self).initialize(connection)
        self.dbms_ver = connection.connection.dbms_ver
        self.dbms_name = connection.connection.dbms_name
        
    def normalize_name(self, name):
        return self._reflector.normalize_name(name)

    def denormalize_name(self, name):
        return self._reflector.denormalize_name(name)

    def _get_default_schema_name(self, connection):
        return self._reflector._get_default_schema_name(connection)

    def has_table(self, connection, table_name, schema=None):
        return self._reflector.has_table(connection, table_name, schema=schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        return self._reflector.has_sequence(connection, sequence_name,
                        schema=schema)

    def get_schema_names(self, connection, **kw):
        return self._reflector.get_schema_names(connection, **kw)


    def get_table_names(self, connection, schema=None, **kw):
        return self._reflector.get_table_names(connection, schema=schema, **kw)

    def get_view_names(self, connection, schema=None, **kw):
        return self._reflector.get_view_names(connection, schema=schema, **kw)

    def get_view_definition(self, connection, viewname, schema=None, **kw):
        return self._reflector.get_view_definition(
                                connection, viewname, schema=schema, **kw)

    def get_columns(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_columns(
                                connection, table_name, schema=schema, **kw)

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_primary_keys(
                                connection, table_name, schema=schema, **kw)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_foreign_keys(
                                connection, table_name, schema=schema, **kw)
        
    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_incoming_foreign_keys(
                                connection, table_name, schema=schema, **kw)

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_indexes(
                                connection, table_name, schema=schema, **kw)
        
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_unique_constraints(
                                connection, table_name, schema=schema, **kw)


# legacy naming
IBM_DBCompiler = DB2Compiler
IBM_DBDDLCompiler = DB2DDLCompiler
IBM_DBIdentifierPreparer = DB2IdentifierPreparer
IBM_DBExecutionContext = DB2ExecutionContext
IBM_DBDialect = DB2Dialect

dialect = DB2Dialect
