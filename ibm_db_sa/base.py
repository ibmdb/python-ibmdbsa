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
import sys
import sqlalchemy
import datetime, re
from sqlalchemy import types as sa_types
from sqlalchemy import schema as sa_schema
from sqlalchemy import util
from sqlalchemy import exc
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql import compiler
from sqlalchemy.sql import operators
from sqlalchemy.engine import default
from sqlalchemy import event
from sqlalchemy.engine import Engine
from .logger import logger, log_entry_exit
from sqlalchemy import __version__ as SA_VERSION_STR
from . import reflection as ibm_reflection

m = re.match(r"^\s*(\d+)\.(\d+)", SA_VERSION_STR)
SA_VERSION_MM = (int(m.group(1)), int(m.group(2))) if m else (0, 0)

# SQLAlchemy >= 2.0
if SA_VERSION_MM >= (2, 0):
   from sqlalchemy.sql.sqltypes import (
       Integer, SmallInteger, BigInteger, String, Text, Unicode, UnicodeText,
       Boolean, Date, Time, DateTime, Interval,
       Float, Numeric, DECIMAL,
       Enum, LargeBinary, JSON, PickleType, REAL,
       CHAR, VARCHAR, NCHAR, NVARCHAR, BINARY, VARBINARY,
       CLOB, BLOB, SchemaType, TupleType, UUID, Uuid,
   )
   from sqlalchemy.sql.type_api import (
       TypeEngine, TypeDecorator, UserDefinedType, Variant, ExternalType,
   )
   from sqlalchemy.sql.sqltypes import NullType
# SQLAlchemy >= 1.4 and < 2.0
elif SA_VERSION_MM >= (1, 4):
   from sqlalchemy.sql.sqltypes import (
       Integer, SmallInteger, BigInteger, String, Text, Unicode, UnicodeText,
       Boolean, Date, Time, DateTime, Interval,
       Float, Numeric, DECIMAL,
       Enum, LargeBinary, JSON, PickleType, REAL,
       CHAR, VARCHAR, NCHAR, NVARCHAR,
       BINARY, VARBINARY, CLOB, BLOB, SchemaType, TupleType,
   )
   from sqlalchemy.sql.type_api import (
       TypeEngine, TypeDecorator, UserDefinedType, Variant, ExternalType,
   )
   from sqlalchemy.sql.sqltypes import NullType
   UUID = None
   Uuid = None
# SQLAlchemy <= 1.3
else:
   from sqlalchemy.sql.sqltypes import (
       Integer, SmallInteger, BigInteger, String, Text, Unicode, UnicodeText,
       Boolean, Date, Time, DateTime, Interval,
       Float, Numeric, DECIMAL,
       Enum, LargeBinary, JSON, PickleType, REAL,
       CHAR, VARCHAR, NCHAR, NVARCHAR,
       BINARY, VARBINARY, CLOB, BLOB, SchemaType,
   )
   from sqlalchemy.sql.type_api import (
       TypeEngine, TypeDecorator, UserDefinedType, Variant,
   )
   from sqlalchemy.sql.sqltypes import NullType
   # Not available in SQLAlchemy 1.3
   TupleType = None
   ExternalType = None
   UUID = None
   Uuid = None

# Stable aliases for internal use (all SA versions)
BOOLEAN = Boolean
INTEGER = Integer
SMALLINT = SmallInteger
BIGINT = BigInteger
NUMERIC = Numeric
FLOAT = Float
DATE = Date
TIME = Time
DATETIME = DateTime
TIMESTAMP = DateTime

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


class BOOLEAN(sa_types.Boolean):
    __visit_name__ = 'BOOLEAN'


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
    'BOOLEAN': BOOLEAN,
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
    'TIMESTMP': TIMESTAMP,
    'VARCHAR': VARCHAR,
    'LONGVARCHAR': LONGVARCHAR,
    'XML': XML,
    'GRAPHIC': GRAPHIC,
    'VARGRAPHIC': VARGRAPHIC,
    'LONGVARGRAPHIC': LONGVARGRAPHIC,
    'DBCLOB': DBCLOB
}


class DB2TypeCompiler(compiler.GenericTypeCompiler):
   @log_entry_exit
   def visit_TIMESTAMP(self, type_, **kw):
       sql = "TIMESTAMP"
       logger.debug(f"Type rendering -> TIMESTAMP -> {sql}")
       return sql

   @log_entry_exit
   def visit_DATE(self, type_, **kw):
       sql = "DATE"
       logger.debug(f"Type rendering -> DATE -> {sql}")
       return sql

   @log_entry_exit
   def visit_TIME(self, type_, **kw):
       sql = "TIME"
       logger.debug(f"Type rendering -> TIME -> {sql}")
       return sql

   def visit_DATETIME(self, type_, **kw):
       logger.debug("Redirecting DATETIME to TIMESTAMP")
       return self.visit_TIMESTAMP(type_, **kw)

   def visit_SMALLINT(self, type_, **kw):
       sql = "SMALLINT"
       logger.debug(f"Type rendering -> SMALLINT -> {sql}")
       return sql

   def visit_BOOLEAN(self, type_, **kw):
       sql = "BOOLEAN"
       logger.debug(f"Type rendering -> BOOLEAN -> {sql}")
       return sql

   def visit_INT(self, type_, **kw):
       sql = "INT"
       logger.debug(f"Type rendering -> INT -> {sql}")
       return sql

   def visit_BIGINT(self, type_, **kw):
       sql = "BIGINT"
       logger.debug(f"Type rendering -> BIGINT -> {sql}")
       return sql

   def visit_FLOAT(self, type_, **kw):
       precision = type_.precision
       if precision is None:
           sql = "FLOAT"
       else:
           sql = f"FLOAT({precision})"
       logger.debug(f"Type rendering -> FLOAT -> precision={precision}, sql={sql}")
       return sql

   def visit_DOUBLE(self, type_, **kw):
       sql = "DOUBLE"
       logger.debug(f"Type rendering -> DOUBLE -> {sql}")
       return sql

   def visit_XML(self, type_, **kw):
       sql = "XML"
       logger.debug(f"Type rendering -> XML -> {sql}")
       return sql

   def visit_CLOB(self, type_, **kw):
       sql = "CLOB"
       logger.debug(f"Type rendering -> CLOB -> {sql}")
       return sql

   def visit_BLOB(self, type_, **kw):
       length = type_.length
       sql = "BLOB(1M)" if length in (None, 0) else f"BLOB({length})"
       logger.debug(f"Type rendering -> BLOB -> length={length}, sql={sql}")
       return sql

   def visit_DBCLOB(self, type_, **kw):
       length = type_.length
       sql = "DBCLOB(1M)" if length in (None, 0) else f"DBCLOB({length})"
       logger.debug(f"Type rendering -> DBCLOB -> length={length}, sql={sql}")
       return sql

   def visit_VARCHAR(self, type_, **kw):
       length = type_.length
       sql = f"VARCHAR({length})"
       logger.debug(f"Type rendering -> VARCHAR -> length={length}, sql={sql}")
       return sql

   def visit_LONGVARCHAR(self, type_, **kw):
       sql = "LONG VARCHAR"
       logger.debug(f"Type rendering -> LONG VARCHAR -> {sql}")
       return sql

   def visit_VARGRAPHIC(self, type_, **kw):
       length = type_.length
       sql = f"VARGRAPHIC({length})"
       logger.debug(f"Type rendering -> VARGRAPHIC -> length={length}, sql={sql}")
       return sql

   def visit_LONGVARGRAPHIC(self, type_, **kw):
       sql = "LONG VARGRAPHIC"
       logger.debug(f"Type rendering -> LONG VARGRAPHIC -> {sql}")
       return sql

   def visit_CHAR(self, type_, **kw):
       length = type_.length
       sql = "CHAR" if length in (None, 0) else f"CHAR({length})"
       logger.debug(f"Type rendering -> CHAR -> length={length}, sql={sql}")
       return sql

   def visit_GRAPHIC(self, type_, **kw):
       length = type_.length
       sql = "GRAPHIC" if length in (None, 0) else f"GRAPHIC({length})"
       logger.debug(f"Type rendering -> GRAPHIC -> length={length}, sql={sql}")
       return sql

   @log_entry_exit
   def visit_DECIMAL(self, type_, **kw):
       precision = type_.precision
       scale = type_.scale
       if not precision:
           sql = "DECIMAL(31, 0)"
       elif not scale:
           sql = f"DECIMAL({precision}, 0)"
       else:
           sql = f"DECIMAL({precision}, {scale})"
       logger.debug(
           f"Type rendering -> DECIMAL -> "
           f"precision={precision}, scale={scale}, sql={sql}"
       )
       return sql

   def visit_numeric(self, type_, **kw):
       logger.debug("Redirecting numeric to DECIMAL")
       return self.visit_DECIMAL(type_, **kw)

   def visit_datetime(self, type_, **kw):
       logger.debug("Redirecting datetime to TIMESTAMP")
       return self.visit_TIMESTAMP(type_, **kw)

   def visit_date(self, type_, **kw):
       logger.debug("Redirecting date to DATE")
       return self.visit_DATE(type_, **kw)

   def visit_time(self, type_, **kw):
       logger.debug("Redirecting time to TIME")
       return self.visit_TIME(type_, **kw)

   def visit_integer(self, type_, **kw):
       logger.debug("Redirecting integer to INT")
       return self.visit_INT(type_, **kw)

   def visit_boolean(self, type_, **kw):
       logger.debug("Redirecting boolean to BOOLEAN")
       return self.visit_BOOLEAN(type_, **kw)

   def visit_float(self, type_, **kw):
       logger.debug("Redirecting float to FLOAT")
       return self.visit_FLOAT(type_, **kw)

   def visit_unicode(self, type_, **kw):
       check_server = getattr(DB2Dialect, "serverType")
       base_sql = self.visit_VARGRAPHIC(type_, **kw)
       if check_server == "DB2":
           sql = base_sql + " CCSID 1200"
       else:
           sql = base_sql
       logger.debug(
           f"Type rendering -> UNICODE -> "
           f"serverType={check_server}, sql={sql}"
       )
       return sql

   def visit_unicode_text(self, type_, **kw):
       logger.debug("Redirecting unicode_text to LONGVARGRAPHIC")
       return self.visit_LONGVARGRAPHIC(type_, **kw)

   def visit_string(self, type_, **kw):
       logger.debug("Redirecting string to VARCHAR")
       return self.visit_VARCHAR(type_, **kw)

   def visit_TEXT(self, type_, **kw):
       logger.debug("Redirecting TEXT to CLOB")
       return self.visit_CLOB(type_, **kw)

   def visit_large_binary(self, type_, **kw):
       logger.debug("Redirecting large_binary to BLOB")
       return self.visit_BLOB(type_, **kw)


class DB2Compiler(compiler.SQLCompiler):
    if SA_VERSION_MM < (0, 9):
        @log_entry_exit
        def visit_false(self, expr, **kw):
            logger.debug("Rendering FALSE literal as 0")
            return "0"

        @log_entry_exit
        def visit_true(self, expr, **kw):
            logger.debug("Rendering TRUE literal as 1")
            return "1"

    @log_entry_exit
    def get_cte_preamble(self, recursive):
        logger.debug(f"Generating CTE preamble -> recursive={recursive}")
        return "WITH"

    @log_entry_exit
    def visit_now_func(self, fn, **kw):
        logger.debug("Rendering NOW function as CURRENT_TIMESTAMP")
        return "CURRENT_TIMESTAMP"

    @log_entry_exit
    def for_update_clause(self, select, **kw):
        for_update = select.for_update
        logger.debug(f"Processing FOR UPDATE clause -> value={for_update}")
        if for_update is True:
            clause = " WITH RS USE AND KEEP UPDATE LOCKS"
        elif for_update == "read":
            clause = " WITH RS USE AND KEEP SHARE LOCKS"
        else:
            clause = ""
        logger.debug(f"Generated FOR UPDATE clause -> {clause}")
        return clause

    @log_entry_exit
    def visit_mod_binary(self, binary, operator, **kw):
        left_expr = binary.left
        right_expr = binary.right
        left = self.process(left_expr)
        right = self.process(right_expr)
        sql = f"mod({left}, {right})"
        logger.debug(
            f"Rendering MOD binary -> left={left}, right={right}, sql={sql}"
        )
        return sql

    def literalBindsFlagFrom_kw(self, kw=None):
        """Return True if literal_binds is requested in compile kwargs."""
        if not kw or not isinstance(kw, dict):
            logger.debug("literal_binds check -> False (invalid kw)")
            return False
        literal_binds = kw.get("literal_binds")
        if literal_binds:
            logger.debug("literal_binds detected at top level")
            return True
        compile_kwargs = kw.get("compile_kwargs")
        if isinstance(compile_kwargs, dict):
            if compile_kwargs.get("literal_binds"):
                logger.debug("literal_binds detected in compile_kwargs")
                return True
        logger.debug("literal_binds not requested")
        return False

    @log_entry_exit
    def limit_clause(self, select, **kw):
        text = ""
        limit_clause = select._limit_clause
        offset_clause = select._offset_clause
        literal_binds = self.literalBindsFlagFrom_kw(kw)
        logger.debug(
            f"Processing LIMIT/OFFSET -> "
            f"limit={limit_clause}, "
            f"offset={offset_clause}, "
            f"literal_binds={literal_binds}"
        )

        def _render_clause(clause):
            if clause is None:
                return None
            if select._simple_int_clause(clause):
                return self.process(clause.render_literal_execute(), **kw)
            if literal_binds:
                if hasattr(clause, "render_literal_execute"):
                    try:
                        return self.process(clause.render_literal_execute(), **kw)
                    except Exception:
                        pass
                try:
                    return self.process(clause, literal_binds=True, **kw)
                except Exception:
                    pass
                try:
                    if isinstance(clause, BindParameter):
                        value = getattr(clause, "value", None)
                        if value is not None:
                            if isinstance(value, str):
                                return f"'{value}'"
                            return str(value)
                except Exception:
                    pass
            try:
                return self.process(clause, **kw)
            except Exception as e:
                logger.error("Failed to render LIMIT/OFFSET clause")
                logger.exception("Stack trace in limit_clause")
                raise exc.CompileError(
                    "dialect 'ibm_db_sa' cannot render LIMIT/OFFSET for this clause; "
                    "ensure the clause is a simple integer or is processable by the compiler."
                ) from e

        limit_text = _render_clause(limit_clause)
        if limit_text is not None:
            text += f" LIMIT {limit_text}"
            logger.debug(f"Applied LIMIT -> {limit_text}")
        offset_text = _render_clause(offset_clause)
        if offset_text is not None:
            text += f" OFFSET {offset_text}"
            logger.debug(f"Applied OFFSET -> {offset_text}")
        logger.debug(f"Generated LIMIT/OFFSET clause -> {text}")
        return text

    @log_entry_exit
    def visit_select(self, select, **kw):
        try:
            sql_ori = compiler.SQLCompiler.visit_select(self, select, **kw)
            sql_upper = sql_ori.upper()
            logger.debug("Processing SELECT compilation.")
            if ("LIMIT" in sql_upper) or ("FETCH FIRST" in sql_upper):
                logger.debug("LIMIT/FETCH already present. Returning original SQL.")
                logger.debug(f"Final SELECT SQL -> {sql_ori}")
                return sql_ori
            limit_clause_obj = select._limit_clause
            offset_clause_obj = select._offset_clause
            logger.debug(
                f"SELECT limit/offset detection -> "
                f"limit={limit_clause_obj}, offset={offset_clause_obj}"
            )
            if limit_clause_obj is not None:
                limit_offset_clause = self.limit_clause(select, **kw)
                if limit_offset_clause:
                    final_sql = sql_ori + limit_offset_clause
                    logger.debug("Applying simple LIMIT/OFFSET clause.")
                    logger.debug(f"Final SELECT SQL -> {final_sql}")
                    return final_sql
            if offset_clause_obj is not None:
                logger.debug("Applying DB2 ROW_NUMBER based OFFSET rewrite.")
                __rownum = "Z.__ROWNUM"
                sql_work = re.sub(
                    r"FETCH FIRST \d+ ROWS ONLY",
                    "",
                    sql_ori,
                    flags=re.IGNORECASE,
                ).strip()
                sql_work = re.sub(
                    r"\s+OFFSET\s+(?:\d+|__\[POSTCOMPILE_[^\]]+\]|:[A-Za-z0-9_]+|\?)\s*$",
                    "",
                    sql_work,
                    flags=re.IGNORECASE,
                )
                sql_split = re.split(r"[\s+]FROM ", sql_work, 1)
                if len(sql_split) < 2:
                    logger.debug("Unable to split SELECT for OFFSET rewrite.")
                    logger.debug(f"Final SELECT SQL -> {sql_ori}")
                    return sql_ori
                sql_sec = f" \nFROM {sql_split[1]} "
                dummyVal = "Z.__db2_"
                sql_pri = ""
                sql_sel = "SELECT DISTINCT " if select._distinct else "SELECT "
                sql_select_token = sql_split[0].split(",")
                i = 0
                while i < len(sql_select_token):
                    token = sql_select_token[i]
                    if token.count("TIMESTAMP(DATE(SUBSTR(CHAR(") == 1:
                        sql_sel = f'{sql_sel} "{dummyVal}{i + 1}",'
                        sql_pri = (
                            f'{sql_pri} {sql_select_token[i]},'
                            f'{sql_select_token[i + 1]},'
                            f'{sql_select_token[i + 2]},'
                            f'{sql_select_token[i + 3]} AS "{dummyVal}{i + 1}",'
                        )
                        i += 4
                        continue
                    if token.count(" AS ") == 1:
                        temp_col_alias = token.split(" AS ")
                        sql_pri = f"{sql_pri} {token},"
                        sql_sel = f"{sql_sel} {temp_col_alias[1]},"
                        i += 1
                        continue
                    sql_pri = f'{sql_pri} {token} AS "{dummyVal}{i + 1}",'
                    sql_sel = f'{sql_sel} "{dummyVal}{i + 1}",'
                    i += 1
                sql_pri = sql_pri.rstrip(",")
                sql_pri = f"{sql_pri}{sql_sec}"
                sql_sel = sql_sel.rstrip(",")
                sql = (
                    f'{sql_sel}, ( ROW_NUMBER() OVER() ) AS "{__rownum}" '
                    f"FROM ( {sql_pri} ) AS M"
                )
                sql = f'{sql_sel} FROM ( {sql} ) Z WHERE'

                def _process_clause_text(clause):
                    if clause is None:
                        return None
                    if select._simple_int_clause(clause):
                        return self.process(
                            clause.render_literal_execute(), **kw
                        )
                    return self.process(clause, **kw)

                offset_text = _process_clause_text(offset_clause_obj)
                limit_text = _process_clause_text(limit_clause_obj)
                if offset_text is not None:
                    sql = f'{sql} "{__rownum}" > {offset_text}'
                if offset_text is not None and limit_text is not None:
                    sql = f"{sql} AND "
                if limit_text is not None:
                    if offset_text is not None:
                        sql = (
                            f'{sql} "{__rownum}" <= '
                            f"({offset_text} + {limit_text})"
                        )
                    else:
                        sql = f'{sql} "{__rownum}" <= {limit_text}'
                final_sql = f"( {sql} )"
                logger.debug("Generated ROW_NUMBER based pagination SQL.")
                logger.debug(f"Final SELECT SQL -> {final_sql}")
                return final_sql
            logger.debug("Returning original SELECT SQL.")
            logger.debug(f"Final SELECT SQL -> {sql_ori}")
            return sql_ori
        except Exception as e:
            logger.error(f"Error compiling SELECT statement: {e}")
            logger.exception("Stack trace in visit_select")
            raise

    @log_entry_exit
    def visit_sequence(self, sequence, **kw):
        try:
            schema = sequence.schema
            name = sequence.name
            logger.debug(f"Rendering sequence -> schema={schema}, name={name}")
            if schema:
                sql = f"NEXT VALUE FOR {schema}.{name}"
            else:
                sql = f"NEXT VALUE FOR {name}"
            logger.debug(f"Generated sequence SQL -> {sql}")
            return sql
        except Exception as e:
            logger.error(f"Error rendering sequence: {e}")
            logger.exception("Stack trace in visit_sequence")
            raise

    @log_entry_exit
    def default_from(self):
        # DB2 uses SYSIBM.SYSDUMMY1 table for row count
        logger.debug("Rendering default FROM clause (SYSIBM.SYSDUMMY1)")
        return " FROM SYSIBM.SYSDUMMY1"

    @log_entry_exit
    def visit_function(self, func, result_map=None, **kwargs):
        try:
            func_name = func.name.upper()
            logger.debug(f"Rendering function -> name={func_name}")
            if func_name == "AVG":
                args = self.function_argspec(func, **kwargs)
                sql = f"AVG(DOUBLE({args}))"
                logger.debug(f"Rewritten AVG function -> {sql}")
                return sql
            elif func_name == "CHAR_LENGTH":
                args = self.function_argspec(func, **kwargs)
                sql = f"CHAR_LENGTH({args}, OCTETS)"
                logger.debug(f"Rewritten CHAR_LENGTH function -> {sql}")
                return sql
            sql = compiler.SQLCompiler.visit_function(self, func, **kwargs)
            logger.debug(f"Default function rendering -> {sql}")
            return sql
        except Exception as e:
            logger.error(f"Error rendering function {func}: {e}")
            logger.exception("Stack trace in visit_function")
            raise

    # TODO: this is wrong but need to know what DB2 is expecting here
    #    if func.name.upper() == "LENGTH":
    #        return "LENGTH('%s')" % func.compile().params[func.name + '_1']
    #    else:
    #        return compiler.SQLCompiler.visit_function(self, func, **kwargs)

    @log_entry_exit
    def visit_cast(self, cast, **kw):
        try:
            type_ = cast.typeclause.type
            logger.debug(f"Rendering CAST -> type={type_}")
            if SA_VERSION_MM >= (2, 0):
                valid_types = (
                    CHAR, VARCHAR, CLOB, String, Text, Unicode, UnicodeText,
                    BLOB, LargeBinary, VARBINARY,
                    SMALLINT, SmallInteger,
                    INTEGER, Integer,
                    BIGINT, BigInteger,
                    DECIMAL, NUMERIC, Float, REAL, DOUBLE, Numeric,
                    DATE, Date, TIME, Time, TIMESTAMP, DateTime,
                    BOOLEAN, Boolean,
                    NullType
                )
            else:
                valid_types = (
                    CHAR, VARCHAR, CLOB, String, Text, Unicode, UnicodeText,
                    BLOB, LargeBinary, VARBINARY,
                    SMALLINT, SmallInteger,
                    INTEGER, Integer,
                    BIGINT, BigInteger,
                    DECIMAL, NUMERIC, Float, REAL, Numeric,
                    DATE, Date, TIME, Time, TIMESTAMP, DateTime,
                    BOOLEAN, Boolean,
                    NullType
                )
            if isinstance(type_, valid_types):
                sql = super(DB2Compiler, self).visit_cast(cast, **kw)
                logger.debug(f"Standard CAST rendering -> {sql}")
                return sql
            logger.debug("Unsupported CAST type, processing clause only.")
            return self.process(cast.clause)
        except Exception as e:
            logger.error(f"Error rendering CAST: {e}")
            logger.exception("Stack trace in visit_cast")
            raise

    def get_select_precolumns(self, select, **kwargs):
        distinct_value = select._distinct
        if isinstance(distinct_value, str):
            result = distinct_value.upper() + " "
        elif distinct_value:
            result = "DISTINCT "
        else:
            result = ""
        logger.debug(f"SELECT precolumns -> {result.strip()}")
        return result

    @log_entry_exit
    def visit_join(self, join, asfrom=False, **kwargs):
        try:
            join_type = " INNER JOIN "
            if join.full:
                join_type = " FULL OUTER JOIN "
            elif join.isouter:
                join_type = " LEFT OUTER JOIN "
            logger.debug(
                f"Rendering JOIN -> type={join_type.strip()}, "
                f"left={join.left}, right={join.right}"
            )
            sql = "".join(
                (
                    self.process(join.left, asfrom=True, **kwargs),
                    join_type,
                    self.process(join.right, asfrom=True, **kwargs),
                    " ON ",
                    self.process(join.onclause, **kwargs),
                )
            )
            logger.debug(f"Generated JOIN SQL -> {sql}")
            return sql
        except Exception as e:
            logger.error(f"Error rendering JOIN: {e}")
            logger.exception("Stack trace in visit_join")
            raise

    @log_entry_exit
    def visit_savepoint(self, savepoint_stmt):
        sid = self.preparer.format_savepoint(savepoint_stmt)
        sql = f"SAVEPOINT {sid} ON ROLLBACK RETAIN CURSORS"
        logger.debug(f"Generated SAVEPOINT SQL -> {sql}")
        return sql

    @log_entry_exit
    def visit_rollback_to_savepoint(self, savepoint_stmt):
        sid = self.preparer.format_savepoint(savepoint_stmt)
        sql = f"ROLLBACK TO SAVEPOINT {sid}"
        logger.debug(f"Generated ROLLBACK TO SAVEPOINT SQL -> {sql}")
        return sql

    @log_entry_exit
    def visit_release_savepoint(self, savepoint_stmt):
        sid = self.preparer.format_savepoint(savepoint_stmt)
        sql = f"RELEASE TO SAVEPOINT {sid}"
        logger.debug(f"Generated RELEASE SAVEPOINT SQL -> {sql}")
        return sql

    @log_entry_exit
    def visit_unary(self, unary, **kw):
        try:
            operator_ = unary.operator
            within_columns = kw.get("within_columns_clause", False)
            logger.debug(
                f"Rendering UNARY -> operator={operator_}, "
                f"within_columns_clause={within_columns}"
            )
            if operator_ == operators.exists and within_columns:
                usql = super(DB2Compiler, self).visit_unary(unary, **kw)
                sql = f"CASE WHEN {usql} THEN 1 ELSE 0 END"
                logger.debug(f"Rewritten EXISTS unary -> {sql}")
                return sql
            sql = super(DB2Compiler, self).visit_unary(unary, **kw)
            logger.debug(f"Standard unary rendering -> {sql}")
            return sql
        except Exception as e:
            logger.error(f"Error rendering unary expression: {e}")
            logger.exception("Stack trace in visit_unary")
            raise


class DB2DDLCompiler(compiler.DDLCompiler):

    @staticmethod
    @log_entry_exit
    def get_server_version_info(dialect):
        """Returns the DB2 server major and minor version as a list of ints."""
        try:
            if hasattr(dialect, 'dbms_ver') and dialect.dbms_ver:
                version_tokens = dialect.dbms_ver.split('.')[0:2]
                version_info = [int(ver_token) for ver_token in version_tokens]
                logger.debug(
                    f"Parsed server version -> raw={dialect.dbms_ver}, parsed={version_info}"
                )
                return version_info
            logger.warning("Dialect has no dbms_ver attribute or version is empty.")
            return []
        except Exception as e:
            logger.error(f"Failed to parse server version: {e}")
            logger.exception("Stack trace in get_server_version_info")
            raise

    @classmethod
    @log_entry_exit
    def _is_nullable_unique_constraint_supported(cls, dialect):
        """
        Checks to see if DB2 version is at least 10.5.
        Required to determine if unique constraints with nullable columns are supported.
        """
        try:
            dbms_name = getattr(dialect, 'dbms_name', None)
            logger.debug(f"Checking nullable unique constraint support -> dbms_name={dbms_name}")
            if not dbms_name:
                logger.warning("DBMS name not available for constraint capability check.")
                return False
            if 'DB2/' in dbms_name:
                version_info = cls.get_server_version_info(dialect)
                supported = version_info >= [10, 5]
                logger.info(
                    f"Nullable unique constraint support -> "
                    f"version={version_info}, supported={supported}"
                )
                return supported
            logger.debug("DBMS is not DB2 LUW. Nullable unique constraint not supported.")
            return False
        except Exception as e:
            logger.error(f"Error checking nullable unique constraint support: {e}")
            logger.exception("Stack trace in _is_nullable_unique_constraint_supported")
            raise

    @log_entry_exit
    def get_column_specification(self, column, **kw):
        try:
            column_name = column.name
            column_type = column.type
            logger.debug(
                f"Generating column specification -> "
                f"name={column_name}, type={column_type}, "
                f"nullable={column.nullable}, primary_key={column.primary_key}"
            )
            col_spec = [
                self.preparer.format_column(column),
                self.dialect.type_compiler.process(column.type, type_expression=column)]
            # NOT NULL handling
            if not column.nullable or column.primary_key:
                col_spec.append('NOT NULL')
                logger.debug("Applied NOT NULL constraint.")
            # DEFAULT handling
            default = self.get_column_default_string(column)
            if default is not None:
                col_spec.extend(['WITH DEFAULT', default])
                logger.debug(f"Applied default clause -> {default}")
            # AUTOINCREMENT handling
            auto_column = column.table._autoincrement_column
            if column is auto_column:
                logger.debug("Column is autoincrement column. Applying IDENTITY clause.")
                col_spec.extend([
                    'GENERATED BY DEFAULT',
                    'AS IDENTITY',
                    '(START WITH 1)'
                ])
            column_spec = ' '.join(col_spec)
            logger.debug(f"Final column specification generated -> {column_spec}")
            return column_spec
        except Exception as e:
            logger.error(f"Error generating column specification: {e}")
            logger.exception("Stack trace in get_column_specification")
            raise

    @log_entry_exit
    def define_constraint_cascades(self, constraint):
        try:
            constraint_name = getattr(constraint, "name", None)
            ondelete = constraint.ondelete
            onupdate = constraint.onupdate
            logger.debug(
                f"Defining constraint cascades -> "
                f"name={constraint_name}, "
                f"ondelete={ondelete}, "
                f"onupdate={onupdate}"
            )
            text = ""
            if ondelete is not None:
                text += f" ON DELETE {ondelete}"
                logger.debug(f"Applied ON DELETE clause -> {ondelete}")
            if onupdate is not None:
                logger.warning(
                    "DB2 does not support UPDATE CASCADE for foreign keys."
                )
                util.warn(
                    "DB2 does not support UPDATE CASCADE for foreign keys."
                )
            logger.debug(f"Cascade definition result -> {text}")
            return text
        except Exception as e:
            logger.error(f"Error defining constraint cascades: {e}")
            logger.exception("Stack trace in define_constraint_cascades")
            raise

    @log_entry_exit
    def visit_drop_constraint(self, drop, **kw):
        try:
            constraint = drop.element
            constraint_name = getattr(constraint, "name", None)
            constraint_table = getattr(constraint, "table", None)
            constraint_type = type(constraint).__name__
            logger.debug(
                f"Processing DROP constraint -> "
                f"type={constraint_type}, "
                f"name={constraint_name}"
            )
            if isinstance(constraint, sa_schema.ForeignKeyConstraint):
                qual = "FOREIGN KEY "
                const = self.preparer.format_constraint(constraint)
            elif isinstance(constraint, sa_schema.PrimaryKeyConstraint):
                qual = "PRIMARY KEY "
                const = ""
            elif isinstance(constraint, sa_schema.UniqueConstraint):
                qual = "UNIQUE "
                nullable_supported = self._is_nullable_unique_constraint_supported(
                    self.dialect
                )
                if nullable_supported:
                    for column in constraint:
                        column_nullable = column.nullable
                        if column_nullable:
                            constraint.uConstraint_as_index = True
                            logger.debug(
                                "Nullable column detected in UNIQUE constraint. "
                                "Marking as INDEX."
                            )
                    if getattr(constraint, "uConstraint_as_index", None):
                        qual = "INDEX "
                const = self.preparer.format_constraint(constraint)
            else:
                qual = ""
                const = self.preparer.format_constraint(constraint)
            use_index = getattr(constraint, "uConstraint_as_index", None)
            if use_index:
                drop_sql = f"DROP {qual}{const}"
            else:
                table_name = self.preparer.format_table(constraint_table)
                drop_sql = f"ALTER TABLE {table_name} DROP {qual}{const}"
            logger.debug(f"Generated DROP SQL -> {drop_sql}")
            return drop_sql
        except Exception as e:
            logger.error(f"Error generating DROP constraint SQL: {e}")
            logger.exception("Stack trace in visit_drop_constraint")
            raise

    @log_entry_exit
    def create_table_constraints(self, table, **kw):
        try:
            table_name = table.name
            logger.debug(f"Processing CREATE TABLE constraints -> table={table_name}")
            nullable_supported = self._is_nullable_unique_constraint_supported(
                self.dialect
            )
            if nullable_supported:
                for constraint in table._sorted_constraints:
                    if isinstance(constraint, sa_schema.UniqueConstraint):
                        constraint_name = constraint.name
                        logger.debug(f"Evaluating UniqueConstraint -> name={constraint_name}")
                        for column in constraint:
                            column_name = column.name
                            column_nullable = column.nullable
                            if column_nullable:
                                constraint.use_alter = True
                                constraint.uConstraint_as_index = True
                                logger.debug(
                                    f"Nullable column detected -> {column_name}. "
                                    "Converting UNIQUE constraint to INDEX."
                                )
                                break
                        use_index = getattr(constraint, "uConstraint_as_index", None)
                        if use_index:
                            if not constraint_name:
                                index_name = "%s_%s_%s" % (
                                    "ukey",
                                    self.preparer.format_table(constraint.table),
                                    "_".join(col.name for col in constraint),
                                )
                            else:
                                index_name = constraint_name
                            logger.debug(
                                f"Creating index for nullable UNIQUE constraint -> "
                                f"index_name={index_name}"
                            )
                            index = sa_schema.Index(index_name,*(col for col in constraint))
                            index.unique = True
                            index.uConstraint_as_index = True
            result = super(DB2DDLCompiler, self).create_table_constraints(table, **kw)
            logger.debug(f"Final CREATE TABLE constraints SQL fragment -> {result}")
            return result
        except Exception as e:
            logger.error(f"Error processing create_table_constraints: {e}")
            logger.exception("Stack trace in create_table_constraints")
            raise

    @log_entry_exit
    def visit_create_index(self, create, include_schema=True, include_table_schema=True, **kw):
        try:
            element = create.element
            index_name = getattr(element, "name", None)
            is_unique = getattr(element, "unique", None)
            use_index = getattr(element, "uConstraint_as_index", None)
            logger.debug(
                f"Processing CREATE INDEX -> "
                f"name={index_name}, "
                f"unique={is_unique}, "
                f"uConstraint_as_index={use_index}"
            )
            if SA_VERSION_MM < (0, 8):
                sql = super(DB2DDLCompiler, self).visit_create_index(create, **kw)
            else:
                sql = super(DB2DDLCompiler, self).visit_create_index(create,include_schema, include_table_schema, **kw)
            if use_index:
                sql += " EXCLUDE NULL KEYS"
                logger.debug("Applied EXCLUDE NULL KEYS for nullable unique constraint index.")
            logger.debug(f"Generated CREATE INDEX SQL -> {sql}")
            return sql
        except Exception as e:
            logger.error(f"Error generating CREATE INDEX SQL: {e}")
            logger.exception("Stack trace in visit_create_index")
            raise

    @log_entry_exit
    def visit_add_constraint(self, create, **kw):
        try:
            element = create.element
            constraint_type = type(element).__name__
            constraint_name = getattr(element, "name", None)
            logger.debug(
                f"Processing ADD CONSTRAINT -> "
                f"type={constraint_type}, "
                f"name={constraint_name}"
            )
            nullable_supported = self._is_nullable_unique_constraint_supported(self.dialect)
            if nullable_supported and isinstance(element, sa_schema.UniqueConstraint):
                for column in element:
                    column_name = column.name
                    column_nullable = column.nullable
                    if column_nullable:
                        element.uConstraint_as_index = True
                        logger.debug(
                            f"Nullable column detected -> {column_name}. "
                            "Converting UNIQUE constraint to INDEX."
                        )
                        break
                use_index = getattr(element, "uConstraint_as_index", None)
                if use_index:
                    table = element.table
                    table_name = self.preparer.format_table(table)
                    if not constraint_name:
                        index_name = "%s_%s_%s" % (
                            "uk_index",
                            table_name,
                            "_".join(col.name for col in element),
                        )
                    else:
                        index_name = constraint_name
                    logger.debug(
                        f"Creating index for nullable UNIQUE constraint -> "
                        f"index_name={index_name}"
                    )
                    index = sa_schema.Index(index_name,*(col for col in element))
                    index.unique = True
                    index.uConstraint_as_index = True
                    sql = self.visit_create_index(sa_schema.CreateIndex(index))
                    logger.debug(f"Generated SQL via index conversion -> {sql}")
                    return sql
            sql = super(DB2DDLCompiler, self).visit_add_constraint(create)
            logger.debug(f"Generated ADD CONSTRAINT SQL -> {sql}")
            return sql
        except Exception as e:
            logger.error(f"Error generating ADD CONSTRAINT SQL: {e}")
            logger.exception("Stack trace in visit_add_constraint")
            raise


class DB2IdentifierPreparer(compiler.IdentifierPreparer):
   reserved_words = RESERVED_WORDS
   illegal_initial_characters = set(range(0, 10)).union(["_", "$"])
   def __init__(self, dialect):
       logger.debug("Initializing DB2IdentifierPreparer")
       super(DB2IdentifierPreparer, self).__init__(dialect)
       logger.debug(
           f"IdentifierPreparer configuration -> "
           f"reserved_words_count={len(self.reserved_words)}, "
           f"illegal_initial_characters={self.illegal_initial_characters}"
       )


class _SelectLastRowIDMixin(object):
   _select_lastrowid = False
   _lastrowid = None

   @log_entry_exit
   def get_lastrowid(self):
       lastrowid = self._lastrowid
       logger.debug(f"Returning lastrowid -> {lastrowid}")
       return lastrowid

   @log_entry_exit
   def pre_exec(self):
       try:
           is_insert = self.isinsert
           compiled = self.compiled
           logger.debug(f"pre_exec invoked -> isinsert={is_insert}")
           if not is_insert:
               logger.debug("Statement is not INSERT. Skipping identity logic.")
               return
           statement = compiled.statement
           table = statement.table
           seq_column = table._autoincrement_column
           insert_has_sequence = seq_column is not None
           returning_enabled = compiled.returning
           inline_insert = compiled.inline
           logger.debug(
               f"Insert detected -> "
               f"table={table.name}, "
               f"autoincrement_column={getattr(seq_column, 'name', None)}, "
               f"returning={returning_enabled}, "
               f"inline={inline_insert}"
           )
           select_lastrowid = (
               insert_has_sequence
               and not returning_enabled
               and not inline_insert
           )
           self._select_lastrowid = select_lastrowid
           logger.debug(f"Will fetch identity after insert -> {select_lastrowid}")
       except Exception as e:
           logger.error(f"Error during pre_exec: {e}")
           logger.exception("Stack trace in pre_exec")
           raise

   @log_entry_exit
   def post_exec(self):
       try:
           select_lastrowid = self._select_lastrowid
           if not select_lastrowid:
               logger.debug("post_exec skipped identity fetch (not required)")
               return
           logger.debug("Fetching IDENTITY_VAL_LOCAL() after insert")
           conn = self.root_connection
           cursor = self.cursor
           identity_sql = "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1"
           logger.debug(f"Executing identity SQL -> {identity_sql}")
           conn._cursor_execute(
               cursor,
               identity_sql,
               (),
               self
           )
           row = cursor.fetchall()[0]
           identity_value = row[0]
           if identity_value is not None:
               lastrowid = int(identity_value)
               self._lastrowid = lastrowid
               logger.info(f"Identity value retrieved successfully -> {lastrowid}")
           else:
               logger.warning("IDENTITY_VAL_LOCAL() returned NULL")
       except Exception as e:
           logger.error(f"Error during post_exec identity fetch: {e}")
           logger.exception("Stack trace in post_exec")
           raise


class DB2ExecutionContext(_SelectLastRowIDMixin, default.DefaultExecutionContext):
    @log_entry_exit
    def fire_sequence(self, seq, type_):
        sequence_name = str(seq)
        try:
            formatted_seq = self.dialect.identifier_preparer.format_sequence(seq)
            sql = ("SELECT NEXTVAL FOR " + formatted_seq + " FROM SYSIBM.SYSDUMMY1")
            logger.debug(f"Firing sequence -> name={sequence_name}, Generated SQL={sql}")
            result = self._execute_scalar(sql, type_)
            logger.info(f"Sequence value generated -> name={sequence_name}, value={result}")
            return result
        except Exception as e:
            logger.error(f"Sequence execution failed -> name={sequence_name}, error={e}")
            logger.exception("Stack trace for sequence execution failure")
            raise


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
    if SA_VERSION_MM < (1, 4):
        returns_unicode_strings = False
    elif SA_VERSION_MM < (2, 0):
        returns_unicode_strings = sa_types.String.RETURNS_CONDITIONAL
    else:
        returns_unicode_strings = True
    postfetch_lastrowid = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_decimal = False
    supports_native_boolean = False
    supports_statement_cache = True
    preexecute_sequences = False
    supports_alter = True
    supports_sequences = True
    sequences_optional = True

    requires_name_normalize = True

    supports_default_values = False
    supports_empty_insert = False

    two_phase_transactions = False
    savepoints = True

    statement_compiler = DB2Compiler
    ddl_compiler = DB2DDLCompiler
    type_compiler = DB2TypeCompiler
    preparer = DB2IdentifierPreparer
    execution_ctx_cls = DB2ExecutionContext

    _reflector_cls = ibm_reflection.DB2Reflector
    serverType = ''

    def __init__(self, **kw):
        logger.debug("Creating DB2Dialect instance")
        super(DB2Dialect, self).__init__(**kw)
        self._reflector = self._reflector_cls(self)
        self.dbms_ver = None
        self.dbms_name = None

    # reflection: these all defer to an BaseDB2Reflector
    # object which selects between DB2 and AS/400 schemas
    @log_entry_exit
    def initialize(self, connection):
        logger.info("Initializing DB2Dialect")
        try:
            self.dbms_ver = getattr(connection.connection, 'dbms_ver', None)
            self.dbms_name = getattr(connection.connection, 'dbms_name', None)
            if not self.dbms_name:
                logger.warning("DBMS name not detected from connection")
            else:
                logger.info(
                    f"Connected to DB Server -> name={self.dbms_name}, version={self.dbms_ver}"
                )
            DB2Dialect.serverType = self.dbms_name
            super(DB2Dialect, self).initialize(connection)
            logger.debug(
                f"SQLAlchemy version branch -> SA_VERSION_MM={SA_VERSION_MM}, "
                f"returns_unicode_strings={self.returns_unicode_strings}"
            )
            selected_reflector = self._reflector_cls
            if self.dbms_name == 'AS':
                selected_reflector = ibm_reflection.AS400Reflector
            elif self.dbms_name == "DB2":
                selected_reflector = ibm_reflection.OS390Reflector
            elif self.dbms_name and "DB2/" in self.dbms_name:
                selected_reflector = ibm_reflection.DB2Reflector
            elif self.dbms_name and "IDS/" in self.dbms_name:
                selected_reflector = ibm_reflection.DB2Reflector
            elif self.dbms_name and self.dbms_name.startswith("DSN"):
                selected_reflector = ibm_reflection.OS390Reflector
            self._reflector = selected_reflector(self)
            logger.info(f"Reflector selected -> {selected_reflector.__name__}")
        except Exception as e:
            logger.critical(f"Dialect initialization failed: {e}")
            raise

    @log_entry_exit
    def get_columns(self, connection, table_name, schema=None, **kw):
        logger.debug(f"Fetching columns -> table={table_name}, schema={schema}")
        columns = self._reflector.get_columns(connection, table_name, schema=schema, **kw)
        if not columns:
            logger.warning(f"No columns found -> table={table_name}")
        else:
            logger.debug(f"Columns fetched -> count={len(columns)}")
        return columns

    @log_entry_exit
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        logger.debug(f"Fetching PK -> table={table_name}, schema={schema}")
        pk = self._reflector.get_pk_constraint(connection, table_name, schema=schema, **kw)
        if not pk or not pk.get("constrained_columns"):
            logger.warning(f"No primary key found -> table={table_name}")
        else:
            logger.debug(f"PK columns -> {pk.get('constrained_columns')}")
        return pk

    @log_entry_exit
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        logger.debug(f"Fetching foreign keys -> table={table_name}, schema={schema}")
        fks = self._reflector.get_foreign_keys(connection, table_name, schema=schema, **kw)
        logger.debug(f"Foreign keys fetched -> count={len(fks)}")
        return fks

    @log_entry_exit
    def get_table_names(self, connection, schema=None, **kw):
        logger.debug(f"Fetching table names -> schema={schema}")
        tables = self._reflector.get_table_names(connection, schema=schema, **kw)
        logger.debug(f"Tables fetched -> count={len(tables)}")
        return tables

    @log_entry_exit
    def get_view_names(self, connection, schema=None, **kw):
        logger.debug(f"Fetching view names -> schema={schema}")
        views = self._reflector.get_view_names(connection, schema=schema, **kw)
        logger.debug(f"Views fetched -> count={len(views)}")
        return views

    @log_entry_exit
    def get_sequence_names(self, connection, schema=None, **kw):
        logger.debug(f"Fetching sequence names -> schema={schema}")
        sequences = self._reflector.get_sequence_names(connection, schema=schema, **kw)
        logger.debug(f"Sequences fetched -> count={len(sequences)}")
        return sequences

    @log_entry_exit
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        logger.debug(f"Fetching view definition -> view={view_name}, schema={schema}")
        definition = self._reflector.get_view_definition(connection, view_name, schema=schema, **kw)
        if definition:
            logger.debug(f"View definition length -> {len(definition)} characters")
        else:
            logger.warning(f"View definition not found -> view={view_name}")
        return definition

    @log_entry_exit
    def get_indexes(self, connection, table_name, schema=None, **kw):
        logger.debug(f"Fetching indexes -> table={table_name}, schema={schema}")
        indexes = self._reflector.get_indexes(connection, table_name, schema=schema, **kw)
        logger.debug(f"Indexes fetched -> count={len(indexes)}")
        return indexes

    @log_entry_exit
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        logger.debug(f"Fetching unique constraints -> table={table_name}, schema={schema}")
        constraints = self._reflector.get_unique_constraints(connection, table_name, schema=schema, **kw)
        logger.debug(f"Unique constraints fetched -> count={len(constraints)}")
        return constraints

    @log_entry_exit
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        logger.debug(f"Fetching table comment -> table={table_name}, schema={schema}")
        comment = self._reflector.get_table_comment(connection, table_name, schema=schema, **kw)
        if comment:
            logger.debug("Table comment present")
        else:
            logger.debug("No table comment found")
        return comment

    @log_entry_exit
    def normalize_name(self, name):
        normalized = self._reflector.normalize_name(name)
        logger.debug(f"Normalize -> original={name}, normalized={normalized}")
        return normalized

    @log_entry_exit
    def denormalize_name(self, name):
        denormalized = self._reflector.denormalize_name(name)
        logger.debug(f"Denormalize -> original={name}, denormalized={denormalized}")
        return denormalized

    @log_entry_exit
    def has_table(self, connection, table_name, schema=None, **kw):
        exists = self._reflector.has_table(connection, table_name, schema=schema, **kw)
        logger.debug(f"Table exists -> {exists}")
        return exists

    @log_entry_exit
    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        exists = self._reflector.has_sequence(connection, sequence_name, schema=schema, **kw)
        logger.debug(f"Sequence exists -> {exists}")
        return exists

    @log_entry_exit
    def get_schema_names(self, connection, **kw):
        schemas = self._reflector.get_schema_names(connection, **kw)
        logger.debug(f"Schemas fetched -> count={len(schemas)}")
        return schemas

    @log_entry_exit
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        keys = self._reflector.get_primary_keys(connection, table_name, schema=schema, **kw)
        logger.debug(f"Primary keys fetched -> count={len(keys)}")
        return keys

    @log_entry_exit
    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        fks = self._reflector.get_incoming_foreign_keys(connection, table_name, schema=schema, **kw)
        logger.debug(f"Incoming foreign keys fetched -> count={len(fks)}")
        return fks


# legacy naming
IBM_DBCompiler = DB2Compiler
IBM_DBDDLCompiler = DB2DDLCompiler
IBM_DBIdentifierPreparer = DB2IdentifierPreparer
IBM_DBExecutionContext = DB2ExecutionContext
IBM_DBDialect = DB2Dialect

dialect = DB2Dialect
