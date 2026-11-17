"""requirements.py


This file is used by the SQLAlchemy test suite (``sqlalchemy.testing``) to
mark various optional behaviors as supported or non-supported for the Db2
dialect. Open an exclusion to opt in to a capability, close it to skip the
corresponding compliance tests.

"""
from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions

class Requirements(SuiteRequirements):

    @property
    def views(self):
        """Target database must support VIEWs.

        Db2 fully supports CREATE VIEW, and the dialect's reflector implements
        ``get_view_names`` and ``get_view_definition`` (querying SYSCAT.VIEWS).
        The base ``SuiteRequirements.views`` is closed by default, which meant
        the ComponentReflectionTest view fixtures (``define_views``) never ran,
        so the ungated ``test_get_view_names`` expected three views but found
        none. Opening this enables view creation and the view-reflection tests
        (``view_reflection`` and ``view_column_reflection`` both derive from
        this property).
        """

        return exclusions.open()

    @property
    def reflects_pk_names(self):
        """Target driver reflects the name of primary key constraints.

        Db2 stores primary-key constraint names in the catalog (SYSCAT.TABCONST)
        and the dialect's ``get_pk_constraint`` returns that name. The base
        ``SuiteRequirements.reflects_pk_names`` is closed by default, which made
        ``test_get_pk_constraint`` wrap its name assertion in ``fail_if()`` and
        then report "Unexpected success" because the dialect does reflect the
        name correctly. Opening this both fixes that test and un-skips
        ``test_get_pk_constraint_quoted_name`` (PK names with mixed case / dots,
        handled by the dialect's identifier normalization).
        """

        return exclusions.open()

    @property
    def unique_constraints_reflect_as_index(self):
        """Target database reflects unique constraints as indexes.

        Db2 does not allow a nullable column in a true UNIQUE *constraint*, so
        for Db2 >= 10.5 the dialect creates a UNIQUE constraint that includes a
        nullable column as a unique INDEX with EXCLUDE NULL KEYS. get_indexes
        reports that index (flagged with ``duplicates_constraint``) and
        get_unique_constraints reports the matching constraint, so such unique
        constraints do round-trip as indexes. Foreign keys, by contrast, are not
        auto-indexed by Db2, so ``foreign_keys_reflect_as_index`` stays closed.
        """

        return exclusions.open()

    @property
    def on_update_cascade(self):
        """"target database must support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return exclusions.closed()

    @property
    def implicit_decimal_binds(self):
        """Selecting a bare Decimal bind, e.g. ``select(literal(Decimal(...)))``,
        returns it as a Decimal.

        Db2 rejects an untyped parameter marker in this position with SQL0418N
        ("invalid use of an untyped parameter marker"): ``SELECT ? FROM
        SYSIBM.SYSDUMMY1`` has no context from which Db2 can infer the type.
        The equivalent with an explicit cast works (and
        test_decimal_coerce_round_trip_w_cast passes), so this is specifically
        the uncast form Db2 does not support.
        """

        return exclusions.closed()

    @property
    def literal_float_coercion(self):
        """Selecting a bare float bind, e.g. ``select(literal(15.7563))``,
        returns the exact value.

        Same root cause as implicit_decimal_binds: Db2 raises SQL0418N for the
        untyped parameter marker in ``SELECT ? FROM SYSIBM.SYSDUMMY1``. The cast
        form works, so only the uncast float coercion is unsupported.
        """

        return exclusions.closed()

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return exclusions.closed()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()

    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        return exclusions.closed()

    @property
    def temp_table_reflection(self):
        """Target database must support reflection of a temporary table whose
        definition includes a primary key.

        Db2 "created global temporary tables" (CGTT) cannot define a PRIMARY
        KEY, UNIQUE, FOREIGN KEY or index; attempting it raises SQL0526N
        ("functionality that cannot be used with temporary tables"). The
        suite's temp-table fixture hard-codes ``primary_key=True``, so Db2
        cannot create the table in the required shape. Declaring this closed
        skips the temp-table reflection tests (and, via inheritance,
        ``temp_table_reflect_indexes``) instead of erroring on every one.
        """

        return exclusions.closed()

    #@property
    #def offset(self):
    #    return exclusions.closed()

    @property
    def window_functions(self):
        """Target database must support window functions."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        """target backend supports Decimal() objects using E notation
        to represent very small values."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values."""
        return exclusions.closed()

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """
        return exclusions.fails_if(lambda: True,
                    "Throws error SQL0604N, regarding Decimal(38, 12)"
            )

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return exclusions.open()
