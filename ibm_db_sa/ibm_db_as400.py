from sqlalchemy import __version__ as SA_Version
SA_Version = [int(ver_token) for ver_token in SA_Version.split('.')[0:2]]
from .base import DB2ExecutionContext, DB2Dialect
if SA_Version < [2,0]:
    from sqlalchemy import processors, types as sa_types, util
else:
    from sqlalchemy import types as sa_types, util
    from sqlalchemy.engine import processors
from sqlalchemy.exc import ArgumentError
from ibm_db_sa.ibm_db import DB2Dialect_ibm_db
from ibm_db_sa.reflection import AS400Reflector

class AS400Dialect(DB2Dialect_ibm_db):
    _reflector_cls = AS400Reflector
