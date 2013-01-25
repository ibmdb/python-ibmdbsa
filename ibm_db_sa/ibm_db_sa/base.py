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
# | Contributor: Jaimy Azle                                                  |
# | Version: 0.2.x                                                           |
# +--------------------------------------------------------------------------+
"""Support for IBM DB2 database

"""
import re
import datetime
from decimal import Decimal as _python_Decimal
from sqlalchemy import types as sa_types
from sqlalchemy import schema as sa_schema
from sqlalchemy import __version__ as __sa_version__
from sqlalchemy import log, processors
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default
from sqlalchemy.types import TypeDecorator, Unicode

from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
from sqlalchemy import sql, util

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
    'regr_count', 'within'])

class CoerceUnicode(TypeDecorator):
  impl = Unicode

  def process_bind_param(self, value, dialect):
      if isinstance(value, str):
          value = value.decode(dialect.encoding)
      return value

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

# Override module sqlalchemy.types
class IBM_DBBinary(sa_types.LargeBinary):
  def get_col_spec(self):
    if self.length is None:
      return "BLOB(1M)"
    else:
      return "BLOB(%s)" % self.length

class IBM_DBOldBinary(sa_types.Binary):
  def get_col_spec(self):
    if self.length is None:
      return "BLOB(1M)"
    else:
      return "BLOB(%s)" % self.length

class IBM_DBString(sa_types.String):
  def get_col_spec(self):
    if self.length is None:
      return "LONG VARCHAR"
    else:
      return "VARCHAR(%s)" % self.length

class IBM_DBBoolean(sa_types.Boolean):
  def get_col_spec(self):
    return "SMALLINT"

  def result_processor(self, dialect, coltype):
    def process(value):
      if value is None:
        return None
      if value == False:
        return 0
      elif value == True:
        return 1
    return process

  def bind_processor(self, dialect):
    def process(value):
      if value is None:
        return None
      if value == False:
        return '0'
      elif value == True:
        return '1'
    return process

class IBM_DBInteger(sa_types.Integer):
  def get_col_spec(self):
    return "INTEGER"

class IBM_DBNumeric(sa_types.Numeric):
  def get_col_spec(self):
    if not self.precision:
      return "DECIMAL(31,0)"
    else:
      if __sa_version__ > 0.5:
        return "DECIMAL(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.scale}
      else:
        return "DECIMAL(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

  def result_processor(self, dialect, coltype):
    if self.asdecimal:
      if self.scale is not None:
          return processors.to_decimal_processor_factory(_python_Decimal, self.scale)
      else:
          return processors.to_decimal_processor_factory(_python_Decimal)
    else:
      if dialect.supports_native_decimal:
          return processors.to_float
      else:
          return None

class IBM_DBDateTime(sa_types.DateTime):
  def get_col_spec(self):
    return "TIMESTAMP"

  def result_processor(self, dialect, coltype):
    def process(value):
      if value is None:
        return None
      if isinstance(value, datetime.datetime):
          value = datetime.datetime( value.year, value.month, value.day,
                                     value.hour, value.minute, value.second, value.microsecond)
      elif isinstance(value, datetime.time):
          value = datetime.datetime( value.year, value.month, value.day, 0, 0, 0, 0)
      return value
    return process

  def bind_processor(self, dialect):
    def process(value):
      if value is None:
        return None
      if isinstance(value, datetime.datetime):
          value = datetime.datetime( value.year, value.month, value.day,
                                     value.hour, value.minute, value.second, value.microsecond)
      elif isinstance(value, datetime.date):
          value = datetime.datetime( value.year, value.month, value.day, 0, 0, 0, 0)
      return str(value)
    return process

class IBM_DBDate(sa_types.Date):

  def get_col_spec(self):
    return "DATE"

  def result_processor(self, dialect, coltype):
    def process(value):
      if value is None:
        return None
      if isinstance(value, datetime.datetime):
          value = datetime.date( value.year, value.month, value.day)
      elif isinstance(value, datetime.date):
          value = datetime.date( value.year, value.month, value.day)
      return value
    return process

  def bind_processor(self, dialect):
    def process(value):
      if value is None:
        return None
      if isinstance(value, datetime.datetime):
          value = datetime.date( value.year, value.month, value.day)
      elif isinstance(value, datetime.time):
          value = datetime.date( value.year, value.month, value.day)
      return str(value)
    return process

class IBM_DBTime(sa_types.Time):
  def get_col_spec(self):
    return 'TIME'

class IBM_DBTimeStamp(sa_types.TIMESTAMP):
  def get_col_spec(self):
    return 'TIMESTAMP'

class IBM_DBDATETIME(sa_types.DATETIME):
  def get_col_spec(self):
    return 'TIMESTAMP'

class IBM_DBSmallInteger(sa_types.SmallInteger):
  def get_col_spec(self):
    return 'SMALLINT'

class IBM_DBFloat(sa_types.Float):
  def get_col_spec(self):
    return 'REAL'

class IBM_DBFLOAT(sa_types.FLOAT):
  def get_col_spec(self):
    return 'REAL'

class IBM_DBTEXT(sa_types.TEXT):
  def get_col_spec(self):
    if self.length is None:
      return 'LONG VARCHAR'
    else:
      return 'VARCHAR(%s)' % self.length

class IBM_DBDecimal(sa_types.DECIMAL):

  def get_col_spec(self):
    if not self.precision:
      return "DECIMAL(31,0)"
    else:
      if __sa_version__ > 0.5:
        return "DECIMAL(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.scale}
      else:
        return "DECIMAL(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

class IBM_DBINT(sa_types.INT):
  def get_col_spec(self):
    return 'INT'

class IBM_DBCLOB(sa_types.CLOB):
  def get_col_spec(self):
    return 'CLOB'

class IBM_DBDBCLOB(sa_types.CLOB):
  def get_col_spec(self):
    return 'DBCLOB'

class IBM_DBVARCHAR(sa_types.VARCHAR):
  def get_col_spec(self):
    if self.length is None:
      return 'LONG VARCHAR'
    else:
      return 'VARCHAR(%s)' % self.length

class IBM_DBChar(sa_types.CHAR):
  def get_col_spec(self):
    if self.length is None:
      return 'CHAR'
    else:
      return 'CHAR(%s)' % self.length

class IBM_DBGRAPHIC(sa_types.CHAR):
  def get_col_spec(self):
    if self.length is None:
      return 'GRAPHIC'
    else:
      return 'GRAPHIC(%s)' % self.length

class IBM_DBVARGRAPHIC(sa_types.UnicodeText):
  def get_col_spec(self):
    if self.length is None:
      return 'LONG VARGRAPHIC'
    else:
      return 'VARGRAPHIC(%s)' % self.length

class IBM_DBBLOB(sa_types.BLOB):
  def get_col_spec(self):
    if self.length is None:
      return 'BLOB(1M)'
    else:
      return 'BLOB(%s)' % self.length

class IBM_DBBOOLEAN(sa_types.BOOLEAN):
  def get_col_spec(self):
    return 'SMALLINT'

class IBM_DBDouble(sa_types.Float):
  def get_col_spec(self):
    if self.length is None:
      return 'DOUBLE(15)'
    else:
      return 'DOUBLE(%(precision)s)' % self.precision

class IBM_DBBigInteger(sa_types.BigInteger):
  def get_col_spec(self):
    return 'BIGINT'

class IBM_DBXML(sa_types.Text):
  def get_col_spec(self):
    return 'XML'

# Module level dictionary maps standard SQLAlchemy types to IBM_DB data types.
# The dictionary uses the SQLAlchemy data types as key, and maps an IBM_DB type as its value
colspecs = {
    sa_types.LargeBinary  : IBM_DBBinary,
    sa_types.Binary       : IBM_DBOldBinary,
    sa_types.String       : IBM_DBString,
    sa_types.Boolean      : IBM_DBBoolean,
    sa_types.Integer      : IBM_DBInteger,
    sa_types.BigInteger   : IBM_DBBigInteger,
    sa_types.Numeric      : IBM_DBNumeric,
    sa_types.DateTime     : IBM_DBDateTime,
    sa_types.Date         : IBM_DBDate,
    sa_types.Time         : IBM_DBTime,
    sa_types.SmallInteger : IBM_DBSmallInteger,
    sa_types.Float        : IBM_DBFloat,
    sa_types.FLOAT        : IBM_DBFloat,
    sa_types.TEXT         : IBM_DBTEXT,
    sa_types.DECIMAL      : IBM_DBDecimal,
    sa_types.INT          : IBM_DBINT,
    sa_types.TIMESTAMP    : IBM_DBTimeStamp,
    sa_types.DATETIME     : IBM_DBDATETIME,
    sa_types.CLOB         : IBM_DBCLOB,
    sa_types.VARCHAR      : IBM_DBVARCHAR,
    sa_types.CHAR         : IBM_DBChar,
    sa_types.BLOB         : IBM_DBBLOB,
    sa_types.BOOLEAN      : IBM_DBBOOLEAN,
    sa_types.Unicode      : IBM_DBVARGRAPHIC
}

# Module level dictionary which maps the data type name returned by a database
# to the IBM_DB type class allowing the correct type classes to be created
# based on the information_schema.  Any database type that is supported by the
# IBM_DB shall be mapped to an equivalent data type.
ischema_names = {
    'BLOB'         : IBM_DBBinary,
    'CHAR'         : IBM_DBChar,
    'CHARACTER'    : IBM_DBChar,
    'CLOB'         : IBM_DBCLOB,
    'DATE'         : IBM_DBDate,
    'DATETIME'     : IBM_DBDateTime,
    'INTEGER'      : IBM_DBInteger,
    'SMALLINT'     : IBM_DBSmallInteger,
    'BIGINT'       : IBM_DBBigInteger,
    'DECIMAL'      : IBM_DBDecimal,
    'NUMERIC'      : IBM_DBNumeric,
    'REAL'         : IBM_DBFloat,
    'DOUBLE'       : IBM_DBDouble,
    'TIME'         : IBM_DBTime,
    'TIMESTAMP'    : IBM_DBTimeStamp,
    'VARCHAR'      : IBM_DBString,
    'LONG VARCHAR' : IBM_DBTEXT,
    'XML'          : IBM_DBXML,
    'GRAPHIC'      : IBM_DBGRAPHIC,
    'VARGRAPHIC'   : IBM_DBVARGRAPHIC,
    'LONG VARGRAPHIC': IBM_DBVARGRAPHIC,
    'DBCLOB'       : IBM_DBDBCLOB
}

class IBM_DBTypeCompiler(compiler.GenericTypeCompiler):

  def visit_now_func(self, fn, **kw):
    return "CURRENT_TIMESTAMP"

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
    return "REAL"

  def visit_XML(self, type_):
    return "XML"

  def visit_CLOB(self, type_):
    return "CLOB"

  def visit_BLOB(self, type_):
    return "BLOB(1M)" if type_.length in (None, 0) else \
        "BLOB(%(length)s)" % {'length' : type_.length}

  def visit_DBCLOB(self, type_):
    return "DBCLOB(1M)" if type_.length in (None, 0) else \
        "DBCLOB(%(length)s)" % {'length' : type_.length}

  def visit_VARCHAR(self, type_):
    if self.dialect.supports_char_length:
      return "LONG VARCHAR" if type_.length in (None, 0) else \
        "VARCHAR(%(length)s)" % {'length' : type_.length}
    else:
      return "LONG VARCHAR"

  def visit_VARGRAPHIC(self, type_):
    if self.dialect.supports_char_length:
      return "LONG VARGRAPHIC" if type_.length in (None, 0) else \
        "VARGRAPHIC(%(length)s)" % {'length' : type_.length}
    else:
      return "LONG VARGRAPHIC"

  def visit_CHAR(self, type_):
    return "CHAR" if type_.length in (None, 0) else \
        "CHAR(%(length)s)" % {'length' : type_.length}

  def visit_GRAPHIC(self, type_):
    return "GRAPHIC" if type_.length in (None, 0) else \
        "GRAPHIC(%(length)s)" % {'length' : type_.length}

  def visit_DECIMAL(self, type_):
    if not type_.precision:
      return "DECIMAL(31, 0)"
    elif not type_.scale:
      return "DECIMAL(%(precision)s, 0)" % {'precision': type_.precision}
    else:
      return "DECIMAL(%(precision)s, %(scale)s)" % {'precision': type_.precision, 'scale': type_.scale}

  def visit_NUMERIC(self, type_):
    return self.visit_DECIMAL(type_)

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

  def visit_Float(self, type_):
    return self.visit_FLOAT(type_)

  def visit_unicode(self, type_):
    return self.visit_VARGRAPHIC(type_)

  def visit_unicode_text(self, type_):
    return self.visit_VARGRAPHIC(type_)

  def visit_string(self, type_):
    return self.visit_VARCHAR(type_)

  def visit_TEXT(self, type_):
    return self.visit_VARCHAR(type_)

  def visit_boolean(self, type_):
    return self.visit_SMALLINT(type_)

  def visit_large_binary(self, type_):
    return self.visit_BLOB(type_)


class IBM_DBCompiler(compiler.SQLCompiler):

  def __init__(self, *args, **kwargs):
    super(IBM_DBCompiler, self).__init__(*args, **kwargs)

  # Generates the limit/offset clause specific/expected by the database vendor
  def limit_clause(self, select):
    limit_str = ""
    if select._limit is not None:
      limit_str = " FETCH FIRST %s ROWS ONLY" % select._limit
    return limit_str

  # Implicit clause to be inserted when no FROM clause is provided
  def default_from(self):
    return " FROM SYSIBM.SYSDUMMY1"   # DB2 uses SYSIBM.SYSDUMMY1 table for row count

  def visit_function(self, func, result_map=None, **kwargs):
    if func.name.upper() == "LENGTH":
      return "LENGTH('%s')" % func.compile().params[func.name + '_1']
    else:
      return compiler.SQLCompiler.visit_function(self, func, **kwargs)

  def visit_typeclause(self, typeclause):
    type_ = typeclause.type.dialect_impl(self.dialect)
    if isinstance(type_, (sa_types.TIMESTAMP, sa_types.DECIMAL, \
        sa_types.DateTime, sa_types.Date, sa_types.Time)):
      return self.dialect.type_compiler.process(type_)
    else:
      return None

  def visit_cast(self, cast, **kwargs):
    type_ = self.process(cast.typeclause)
    if type_ is None:
        return self.process(cast.clause)
    return 'CAST(%s AS %s)' % (self.process(cast.clause), type_)

  def get_select_precolumns(self, select):
    if isinstance(select._distinct, basestring):
        return select._distinct.upper() + " "
    elif select._distinct:
        return "DISTINCT "
    else:
        return ""

  def visit_join(self, join, asfrom=False, **kwargs):
    return ''.join(
        (self.process(join.left, asfrom=True, **kwargs),
         (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN "),
         self.process(join.right, asfrom=True, **kwargs),
         " ON ",
         self.process(join.onclause, **kwargs)))

class IBM_DBDDLCompiler(compiler.DDLCompiler):

  def get_column_specification(self, column, **kw):
    """Inputs:  Column object to be specified as a string
                Boolean indicating whether this is the first column of the primary key
       Returns: String, representing the column type and attributes,
                including primary key, default values, and whether or not it is nullable.
    """
    # column-definition: column-name:
    col_spec = [self.preparer.format_column(column)]
    # data-type:
    col_spec.append(column.type.dialect_impl(self.dialect).get_col_spec())

    # column-options: "NOT NULL"
    if not column.nullable or column.primary_key:
      col_spec.append('NOT NULL')

    # default-clause:
    default = self.get_column_default_string(column)
    if default is not None:
      col_spec.append('WITH DEFAULT')
      #default = default.lstrip("'").rstrip("'")
      col_spec.append(default)

    # generated-column-spec:

    # identity-options:
    # example:  id INT GENERATED BY DEFAULT AS IDENTITY (START WITH 1),
    if column.primary_key    and \
       column.autoincrement  and \
       isinstance(column.type, sa_types.Integer) and \
       not getattr(self, 'has_IDENTITY', False): # allowed only for a single PK
      col_spec.append('GENERATED BY DEFAULT')
      col_spec.append('AS IDENTITY')
      col_spec.append('(START WITH 1)')
      self.has_IDENTITY = True                   # flag the existence of identity PK

    column_spec = ' '.join(col_spec)
    return column_spec

  # Defines SQL statement to be executed after table creation
  def post_create_table(self, table):
    if hasattr( self , 'has_IDENTITY' ):    # remove identity PK flag once table is created
      del self.has_IDENTITY
    return ""

  def visit_drop_index(self, drop):
    index = drop.element
    return "\nDROP INDEX %s" % (
            self.preparer.quote(
                        self._index_identifier(drop.element.name),
                        drop.element.quote)
            )

  def visit_drop_constraint(self, drop):
    constraint = drop.element
    if isinstance(constraint, sa_schema.ForeignKeyConstraint):
        qual = "FOREIGN KEY "
        const = self.preparer.format_constraint(constraint)
    elif isinstance(constraint, sa_schema.PrimaryKeyConstraint):
        qual = "PRIMARY KEY "
        const = ""
    elif isinstance(constraint, sa_schema.UniqueConstraint):
        qual = "INDEX "
        const = self.preparer.format_constraint(constraint)
    else:
        qual = ""
        const = self.preparer.format_constraint(constraint)
    return "ALTER TABLE %s DROP %s%s" % \
                (self.preparer.format_table(constraint.table),
                qual, const)

class IBM_DBIdentifierPreparer(compiler.IdentifierPreparer):

  reserved_words = RESERVED_WORDS
  illegal_initial_characters = set(xrange(0, 10)).union(["_", "$"])

  def __init__(self, dialect, **kw):
    super(IBM_DBIdentifierPreparer, self).__init__(dialect, initial_quote='"', \
      final_quote='"')

  def _bindparam_requires_quotes(self, value):
    return (value.lower() in self.reserved_words
            or value[0] in self.illegal_initial_characters
            or not self.legal_characters.match(unicode(value))
            )


class IBM_DBExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        return self._execute_scalar("SELECT NEXTVAL FOR " +
                    self.dialect.identifier_preparer.format_sequence(seq) +
                    " FROM SYSIBM.SYSDUMMY1", type_)

    def get_lastrowid(self):
      cursor = self.create_cursor()
      cursor.execute("SELECT IDENTITY_VAL_LOCAL() "+
          "FROM SYSIBM.SYSDUMMY1")
      lastrowid = cursor.fetchone()[0]
      cursor.close()
      if lastrowid is not None:
        lastrowid = int(lastrowid)
      return lastrowid

class IBM_DBDialect(default.DefaultDialect):
  """Details of the IBM_DB dialect.  Not used directly in application code."""

  name = 'ibm_db_sa'
  max_identifier_length = 128
  encoding = 'utf-8'
  default_paramstyle = 'named'
  colspecs = colspecs
  ischema_names = ischema_names
  supports_char_length = False
  supports_unicode_statements = False
  supports_unicode_binds = False
  returns_unicode_strings = False
  postfetch_lastrowid = True
  supports_sane_rowcount = False
  supports_sane_multi_rowcount = False
  supports_native_decimal = True
  preexecute_sequences = False
  supports_alter = True
  supports_sequences = True
  sequences_optional = True

  statement_compiler = IBM_DBCompiler
  ddl_compiler = IBM_DBDDLCompiler
  type_compiler = IBM_DBTypeCompiler
  preparer = IBM_DBIdentifierPreparer
  execution_ctx_cls = IBM_DBExecutionContext

  def __init__(self, use_ansiquotes=None, **kwargs):
    super(IBM_DBDialect, self).__init__(**kwargs)

  def normalize_name(self, name):
    if isinstance(name, str):
      name = name.decode(self.encoding)
    elif name != None:
      return name.lower() if name.upper() == name and \
        not self.identifier_preparer._requires_quotes(name.lower()) else name
    return name

  def denormalize_name(self, name):
    if name is None:
      return None
    elif name.lower() == name and not self.identifier_preparer._requires_quotes(name.lower()):
      name = name.upper()
    if not self.supports_unicode_binds:
      name = name.encode(self.encoding)
    else:
      name = unicode(name)
    return name

  # Retrieves connection attributes values
  def _get_default_schema_name(self, connection):
    """Return: current setting of the schema attribute
    """
    default_schema_name = connection.execute(u'SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1').scalar()
    if isinstance(default_schema_name, str):
      default_schema_name = default_schema_name.strip()
    return self.normalize_name(default_schema_name)

  def has_table(self, connection, table_name, schema=None):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    table_name = self.denormalize_name(table_name)
    if current_schema:
        whereclause = sql.and_(sys_tables.c.tabschema==current_schema,
                               sys_tables.c.tabname==table_name)
    else:
        whereclause = sys_tables.c.tabname==table_name
    s = sql.select([sys_tables.c.tabname], whereclause)
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
          'autoincrement':r[2] is None
        })
    return sa_columns

  @reflection.cache
  def get_primary_keys(self, connection, table_name, schema=None, **kw):
    current_schema = self.denormalize_name(schema or self.default_schema_name)
    table_name = self.denormalize_name(table_name)
    sysindexes = sys_indexes
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
    query = sql.select([sysidx.c.indname, sysidx.c.colnames, sysidx.c.uniquerule],
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
        indexes.append({
                    'name' : self.normalize_name(r[0]),
                    'column_names' : [self.normalize_name(col) for col in col_finder.findall(r[1])],
                    'unique': r[2] == 'U'
                })
    return indexes

  # Returns the converted SA adapter type for a given generic vendor type provided
  @classmethod
  def type_descriptor(self, typeobj):
    """ Inputs: generic type to be converted
        Returns: converted adapter type
    """
    return sa_types.adapt_type(typeobj, colspecs)

  def _compat_fetchall(self, rp, charset=None):
    return [_DecodingRowProxy(row, charset) for row in rp.fetchall()]

  def _compat_fetchone(self, rp, charset=None):
    return _DecodingRowProxy(rp.fetchone(), charset)

  def _compat_first(self, rp, charset=None):
    return _DecodingRowProxy(rp.first(), charset)

log.class_logger(IBM_DBDialect)

class _DecodingRowProxy(object):
  """Return unicode-decoded values based on type inspection.

  Smooth over data type issues (esp. with alpha driver versions) and
  normalize strings as Unicode regardless of user-configured driver
  encoding settings.

  """
  def __init__(self, rowproxy, charset):
    self.rowproxy = rowproxy
    self.charset = charset

  def __getitem__(self, index):
    item = self.rowproxy[index]
    if isinstance(item, _array):
        item = item.tostring()
    # Py2K
    if self.charset and isinstance(item, str):
    # end Py2K
    # Py3K
    #if self.charset and isinstance(item, bytes):
      return item.decode(self.charset)
    else:
      return item

  def __getattr__(self, attr):
    item = getattr(self.rowproxy, attr)
    if isinstance(item, _array):
      item = item.tostring()
    # Py2K
    if self.charset and isinstance(item, str):
    # end Py2K
    # Py3K
    #if self.charset and isinstance(item, bytes):
      return item.decode(self.charset)
    else:
      return item


