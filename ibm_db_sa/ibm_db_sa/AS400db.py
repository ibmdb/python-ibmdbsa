from .base import DB2ExecutionContext, DB2Dialect
from sqlalchemy import processors, types as sa_types, util
from sqlalchemy import __version__ as SA_Version
from sqlalchemy.exc import ArgumentError
from ibm_db_sa.ibm_db import DB2Dialect_ibm_db
from ibm_db_sa.reflection import AS400Reflector

class AS400Dialect(DB2Dialect_ibm_db):
    _reflector_cls = AS400Reflector
