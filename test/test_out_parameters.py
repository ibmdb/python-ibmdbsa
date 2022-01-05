from sqlalchemy.testing import fixtures, config
from sqlalchemy import text, bindparam, outparam
from sqlalchemy import Float, Integer, String
from sqlalchemy.testing.assertions import eq_


class OutParamTest(fixtures.TestBase):

    @classmethod
    def setup_class(cls):
        config.db.execute("""
                    create or replace procedure foo(IN x_in integer, OUT x_out integer, OUT y_out integer, OUT z_out varchar(20))
                    BEGIN
                    SET x_out = 10;
                    SET y_out = x_in * 15;
                    SET z_out = NULL;
                    END
                        """)

    def test_out_params(self):
        result = \
            config.db.execute(text('call foo(:x_in, :x_out, :y_out, '
                               ':z_out)',
                               bindparams=[bindparam('x_in'),
                               outparam('x_out'),
                               outparam('y_out'),
                               outparam('z_out')]), x_in=5, x_out=0, y_out=0, z_out='')
        eq_(result.out_parameters, {'x_out': 10, 'y_out': 75, 'z_out': None})
        assert isinstance(result.out_parameters['x_out'], long)

    @classmethod
    def teardown_class(cls):
         config.db.execute("DROP PROCEDURE foo")