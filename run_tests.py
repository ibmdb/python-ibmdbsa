from sqlalchemy.dialects import registry

registry.register("db2", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("db2.ibm_db", "ibm_db_sa.ibm_db", "DB2Dialect_ibm_db")
registry.register("db2.pyodbc", "ibm_db_sa.pyodbc", "DB2Dialect_pyodbc")
registry.register("db2.pyodbc400", "ibm_db_sa.pyodbc", "AS400Dialect_pyodbc")

from sqlalchemy.testing import runner

runner.main()

