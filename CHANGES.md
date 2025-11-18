2025-11-18, Version 0.4.3
=========================

 * chore: add pyproject.toml and replace tests_require with extras_require (#185) (bchoudhary6415)

 * fix 'boolean value not defined' error on AS400 (#181) (Michael Maltese)

 * use correct AS400 reflector when dbms_name/_ver is none (#183) (Michael Maltese)

 * updated yml file to build project with every commit (#178) (bchoudhary6415)

 * fix get_schema_names to remove trailing spaces (#177) (Michael Maltese)

 * feat: make get_table_comment work on OS390 and AS400 (#176) (Michael Maltese)

 * fix get_table_comment to work for views (#175) (Michael Maltese)

 * Bump pypa/gh-action-pypi-publish in /.github/workflows (#169) (dependabot[bot])

 * Add support for standard LIMIT and OFFSET syntax (#167) (bchoudhary6415)

 * Fix: Add sqlalchemy.types.DOUBLE to dependency version specification (#166) (bchoudhary6415)

 * Resolved Boolean renders as smallint (#163) (bchoudhary6415)

 * Support for FULL OUTER JOIN (#162) (bchoudhary6415)


2025-06-23, Version 0.4.2
=========================

 * update yml file to upload package (#159) (bchoudhary6415)

 * Resolve Unicode issue with latest python3.13 (ibmdb#155) (#157) (devMalteK)

 * Resolve Unicode issue with latest python3.13 and added polaris.yml file (#155) (bchoudhary6415)

 * Fix for issue Casting to sa.Float (and some other types) silently does (#154) (bchoudhary6415)

 * Fix unique_constraints (#152) (IceS2)


2024-07-30, Version 0.4.1
=========================

 * Fix foreign key reflection when there are tables with the same name in different schemas (#128) (Xnot)

 * Resolved issue of round function on zos server (#130) (bchoudhary6415)

 * Resolved case-sensitive issue of round function (#131) (bchoudhary6415)

 * Update pyodbc.py (#133) (Murchurl)

 * Fix get_table_comment return value (#135) (andrasore-kodinfo)

 * Fix boolean type not recognized warning (#140) (Xnot)

 * Assign OS390Reflector for Db2 for z/OS (#147) (rhgit01)


2023-04-20, Version 0.4.0
=========================

 * Changes for support of SQLAlchemy 2.0.x (#127) (bchoudhary6415)

 * Some changes to support for SQLAlchemy 2.0 and resolved denormalise name error (#126) (bchoudhary6415)

 * Support for SQLAlchemy 2.0 (#124) (bchoudhary6415)


2023-02-27, Version 0.3.9
=========================

 * Made some changes for release 039 (#121) (bchoudhary6415)

 * Release 039 (#120) (bchoudhary6415)

 * resolve merge conflicts (Bimal Jha)

 * don't mutate URL object (#116) (Edwin Onuonga)

 * Fix offset condition (#117) (Edwin Onuonga)

 * indentation change for changes file (amukherjee)

 * Added missing columns for the previous commit (Sasa Tomic)

 * Fix for reflection get_primary_keys (Sasa Tomic)

 * DB2 may not return the column names in SYSCOL.INDEXES (Sasa Tomic)

 * pyodbc mods (openmax)

 * implemented iseries db2 dialect inside PASE environment (openmax)

2022-05-17, Version 0.3.8
=========================
- autoload bug fix with SQLAlchemy 1.4.x
- remove warning message while connection
- add columns reflection with comments
- other bug fixes reported

2021/07/19
- add support for sqlalchemy 1.4
- Missing none check for dbma_name
- Set issolation level
- Other bug fixes

2021/03/03
- issolation level bug fix.

2020/12/07
- Added ZOS server support for applications to connect
- Added Iseries server support for application to connect
- Add CurrentSchema key word as part of connection string support
- Added fix for multiple issues

2019/05/30
- Added fix for missing "CURRENT ISOLATION" register
- Fixed Autocommit not working for pyodbc
- Fixed NameError: name 'asbool' is not defined python 

2016/08/29
- Fixed multiple defects mentioned below
- Add documentation on alchemy url for conncetion over ssl 
- DB2 on AS400: An unexpected token "ISOLATION" was found on ibm_db_sa/ibm_db.py
- Getting AttributeError for AS400 
- name 'unicode' is not defined
- AttributeError when using pyodbc
- add capability to the driver to generate query with literals, compile_kwargs={"literal_binds": True} 

2016/08/30
-Added Support for Python 3.x

2014/10/20 (IBM_DB_SA adaptor 0.3.2)
- Added SSL support
- Added get_incoming_foreign_keys functionality with reflector 
- Added get_unique_constraints reflection feature 
- Added exist() unary operator support within select clause 
- Fixed incompatible issue of sql.true() for SQLAlchemy v0.7.x & 0.8.x 
- Fixed add_constraint incompatible issue with SQLAlchemy-0.9.x
- Fixed reflection function get_indexes to not return the unique constraint participating index 

2014/03/26 (IBM_DB_SA adapter 0.3.1)
- Handle Double Type in DDL Generator
- Translating 'update' and 'read' lock-mode with DB2 compatible SQL
- Added Stored procedure with outparam support in ibm_db_sa dialect
- Convert nullable unique constraint to unique index exclude nulls for DB2 10.5
- Fix to detect invalid connection
- Added support for CHAR_LENGTH function support
- Fix drop index implementation incompatibility with SQLAlchemy-0.8.x onwards
- Add/Fix support for zxjdbc for both IBM DB LUW and AS/400
- Add/Fix support for PyODBC for both IBM DB LUW and AS/400
- Fix reflection for get_lastrowid

2013/03/01 (IBM_DB_SA adapter 0.3.0)
- Add support for LIMIT/OFFSET
- Add support for savepoints
- Add support for double-precision floating-point number
- Fixed reflection for get_view_names and get_view_definition
 
2013/02/06
- Add support for SQLAlchemy 0.7/0.8
- Refactor code layout
- Now supporting "db2://" scheme as well as
  "ibm_db://" for backwards compatibility
- Add/fix support for explicit sequences

 2011/09/27 (IBM_DB_SA adapter 0.2.1):
 - fix reflection problem
 - support alternate DB2 LUW connection via PyODBC
 - support alternate DB2 i5/OS (iSeries) via PyODBC
 - support alternate DB2 i5/OS (iSeries) via ZxJDBC (Jython)

 2011/08/28 (IBM_DB_SA adapter 0.2.0):
 - Support of SQLAlchemy 0.6/0.7
 - Add Jython support

 2008/11/06 (IBM_DB_SA adapter 0.1.6):
 - fixed Metadata not loading any table info (defect #158705)
 - fixed problems while using different schema names (defect #163785)
 - fixed keyerror in length in visit_function (defect #166292)

2008/03/28 (IBM_DB_SA adapter 0.1.5):
 - fixed BIGINT driver return issue #5 (defect #150638)
 - fixed autocommit default issue #6 (defect #156919)
 - fixed _get_exception() tuple issue #8 (defect #156925)
 - fixed create_engine DSN support issue (defect #156930)

2008/02/15 (IBM_DB_SA adapter 0.1.1):
 - fixed .egg setup loading issue #1 (defect #154259)

2008/02/08 (IBM_DB_SA adapter 0.1.0):
 - initial alpha release
