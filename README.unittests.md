## Running the ibm_db_sa Test Suite

`ibm_db_sa` is tested with SQLAlchemy's own pytest-based test framework,
the same framework SQLAlchemy provides for all third-party dialects (see
SQLAlchemy's [README.unittests.rst](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.unittests.rst)).

You do not need a copy of the SQLAlchemy source code. The test framework
(`sqlalchemy.testing`) is part of the installed `sqlalchemy` package.

The test suite runs on SQLAlchemy 1.4.x, 2.0.x and 2.1.x.

**Contents**

- [Test files](#test-files)
- [Setup](#setup)
- [Configuring the database connection](#configuring-the-database-connection)
- [Running the tests](#running-the-tests)
- [Testing with a different SQLAlchemy version](#testing-with-a-different-sqlalchemy-version)
- [Investigating a failing test](#investigating-a-failing-test)
- [Common pytest options](#common-pytest-options)

---

### Test files

| File | Description | Requires Db2 |
|------|-------------|:------------:|
| `conftest.py` | Registers the Db2 dialect and loads SQLAlchemy's pytest plugin. | - |
| `setup.cfg` | Test configuration: requirements class, named database URLs and pytest options. | - |
| `ibm_db_sa/requirements.py` | Lists the features Db2 supports. Unsupported features are skipped. | - |
| `test/test_dialect.py` | SQL compilation and type tests. | No |
| `test/test_suite.py` | SQLAlchemy's dialect compliance suite (`sqlalchemy.testing.suite`). | Yes |
| `test/test_out_parameters.py` | Stored procedure OUT parameters. | Yes |
| `test/test_default_schema.py` | Default schema detection and reflection. | Yes |
| `test/test_numeric.py` | `NUMERIC` / `DECIMAL` handling. | Yes |
| `test/test_float.py` | `FLOAT` / `REAL` / `DOUBLE` handling and reflection. | Yes |
| `test/test_luw_reflection.py` | Db2 LUW reflection and `BINARY` / `XML` / `DECFLOAT` handling. | Yes |

Some files that require Db2 also contain tests that only compile SQL. Those
tests always run; the database tests in the same file are skipped when no
Db2 connection is configured.

### Setup

From the root of the repository, install the dialect in editable mode and
the test dependencies:

```bash
pip install -e .
pip install -r requirements-test.txt
```

`requirements-test.txt` installs `sqlalchemy`, `pytest` and the `ibm_db`
driver.

### Configuring the database connection

The database used by the tests can be set in `setup.cfg` or passed on the
command line. A URL passed on the command line takes precedence.

**Option 1: `setup.cfg`**

The `[db]` section contains named connection URLs:

```ini
[db]
default = sqlite:///:memory:
sqlite  = sqlite:///:memory:
db2     = ibm_db_sa://user:pass@host:port/DBNAME
```

Update the `db2` entry with your Db2 details, then select it with
`--db db2`:

```bash
pytest --db db2 test/test_suite.py
```

`default` is used when neither `--db` nor `--dburi` is given. It is set to
SQLite so that `test/test_dialect.py` can run without a Db2 server.

> **Note:** A plain `pytest` (without `--db` or `--dburi`) runs against
> SQLite. In that case the compliance suite (`test/test_suite.py`) and the
> Db2-only test classes are reported as `skipped`, not run.

> **Note:** Do not commit real user names or passwords to `setup.cfg`.

**Option 2: `--dburi`**

```bash
pytest --dburi "ibm_db_sa://user:pass@host:port/DBNAME" test/test_suite.py
```

**Supported URL formats**

Both of the following formats select the Db2 dialect:

```text
ibm_db_sa://user:pass@host:port/DBNAME
db2+ibm_db://user:pass@host:port/DBNAME
```

If the run fails at startup with
`ArgumentError: Expected string or URL object, got None`, the URL scheme is
not recognized. Use one of the formats above.

**Checking which database is used**

If neither `--db` nor `--dburi` is given, the tests run against SQLite, not
Db2. To confirm that Db2 is being used, check that:

- the start of the output contains `Adding database URL: ibm_db_sa+...`, or
- test names include the Db2 suffix when run with `-v`, for example
  `NumericTest_ibm_db_sa+ibm_db_sa_DB2/...`.

### Running the tests

**Without a database**

`test/test_dialect.py` only compiles SQL statements and never connects to a
database.

```bash
# all tests in the file
pytest test/test_dialect.py

# one test class
pytest test/test_dialect.py::CompileTest

# one test
pytest test/test_dialect.py::CompileTest::test_offset_uses_rownumber

# tests whose name matches an expression
pytest test/test_dialect.py -k "limit or offset"
```

`python run_tests.py` is a shortcut for `pytest` and accepts the same
arguments. Without arguments it runs `test/test_dialect.py`.

**With a Db2 database**

Add `--db db2` (or `--dburi <url>`) to the command:

```bash
# all tests
pytest --db db2 test/

# the compliance suite only
pytest --db db2 test/test_suite.py

# several files
pytest --db db2 test/test_suite.py test/test_out_parameters.py

# one test class
pytest --db db2 "test/test_suite.py::NumericTest"

# one test
pytest --db db2 "test/test_suite.py::NumericTest::test_numeric_as_decimal"
```

To list the available tests without running them:

```bash
pytest --db db2 test/test_suite.py --collect-only -q
```

**Saving the output to a file**

```bash
# Linux / macOS
pytest --db db2 test/ 2>&1 | tee test_output.txt
```

```powershell
# Windows PowerShell
pytest --db db2 test/ 2>&1 | Tee-Object -FilePath test_output.txt
```

### Testing with a different SQLAlchemy version

Install the SQLAlchemy version you want to test, then run the tests as
usual. No changes to the tests are needed.

```bash
pip install "sqlalchemy==1.4.*"
pytest --db db2 test/

pip install "sqlalchemy==2.0.*"
pytest --db db2 test/

pip install "sqlalchemy==2.1.*"
pytest --db db2 test/
```

Check the installed version with:

```bash
python -c "import sqlalchemy; print(sqlalchemy.__version__)"
```

### Investigating a failing test

A test run ends with a summary such as:

```text
12 failed, 1450 passed, 630 skipped, 3 errors
```

| Result | Meaning |
|--------|---------|
| `passed` | The test succeeded on Db2. |
| `skipped` | The feature is marked as not supported in `ibm_db_sa/requirements.py`, or the test does not apply to Db2. |
| `failed` | The test ran, but the result was not what was expected. |
| `error` | The test could not run, usually because of a problem in setup, a fixture or the database connection. One cause often produces many errors, so look at these first. |

**1. Run the failing test on its own**

Copy the class and test name from the output. Use `-k` to avoid typing the
long Db2 suffix in the test name:

```bash
pytest --db db2 "test/test_suite.py::NumericTest" -k "test_numeric_as_decimal" -x -vv
```

- `-x` stops at the first failure.
- `-vv` shows the full assertion and SQL details.
- `-s` shows `print` and driver output, which helps with encoding errors.

**2. Fix and re-run**

After changing code under `ibm_db_sa/`, re-run only the tests that failed
last time:

```bash
pytest --db db2 test/test_suite.py --lf -v
```

When they pass, run the complete file once more to make sure nothing else
has broken.

**3. Features that Db2 does not support**

If a test fails because Db2 does not support the feature being tested, do
not change the test. Mark the feature as unsupported in
`ibm_db_sa/requirements.py` instead. The test is then reported as
`skipped`, and the results continue to reflect what the dialect actually
supports.

### Common pytest options

| Option | Description |
|--------|-------------|
| `-v` | Show the name of each test. |
| `-vv` | Show full assertion and SQL details. |
| `-q` | Show less output. |
| `-x` | Stop at the first failure. |
| `-s` | Show `print` and driver output. |
| `-k "expr"` | Run only tests whose name matches `expr`. |
| `--db NAME` | Use a named URL from the `[db]` section of `setup.cfg`. |
| `--dburi URL` | Use the given database URL. |
| `--lf` | Run only the tests that failed in the previous run. |
| `--ff` | Run the previously failed tests first, then the rest. |
| `--collect-only -q` | List the tests without running them. |
