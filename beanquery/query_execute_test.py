__copyright__ = "Copyright (C) 2014-2017  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import io
import unittest
import textwrap

from decimal import Decimal
from dateutil.relativedelta import relativedelta

from beancount.core.amount import from_string as A
from beancount.core.number import D
from beancount.core import inventory
from beancount.core.inventory import from_string as I
from beancount.parser import cmptest
from beancount import loader

import beanquery

from beanquery import CompilationError
from beanquery import query_compile as qc
from beanquery import query_env as qe
from beanquery import query_execute as qx
from beanquery import tables


class QueryBase(cmptest.TestCase):
    INPUT = ""
    maxDiff = 8192

    def setUp(self):
        entries, errors, options = loader.load_string(textwrap.dedent(self.INPUT))
        self.ctx = beanquery.connect('beancount:', entries=entries, errors=errors, options=options)

    def compile(self, query):
        return self.ctx.compile(self.ctx.parse(query))

    def check_query(self, input_string, query, expected_types, expected_rows):
        entries, errors, options = loader.load_string(input_string)
        ctx = beanquery.connect('beancount:', entries=entries, errors=errors, options=options)
        curs = ctx.execute(query)
        self.assertEqual(tuple(expected_types), curs.description)
        result_rows = curs.fetchall()
        self.assertEqual(expected_rows, result_rows)

    def assertResult(self, query, result, datatype=None):
        curs = self.ctx.execute(query)
        self.assertEqual([c.datatype for c in curs.description], [datatype or type(result)])
        self.assertEqual(curs.fetchall(), [(result, )])

    def assertError(self, query):
        with self.assertRaises(CompilationError):
            self.ctx.execute(query)


class CommonInputBase(unittest.TestCase):
    INPUT = textwrap.dedent("""

    2010-01-01 open Assets:Bank:Checking
    2010-01-01 open Assets:ForeignBank:Checking
    2010-01-01 open Assets:Bank:Savings

    2010-01-01 open Expenses:Restaurant

    2010-01-01 * "Dinner with Cero"
      Assets:Bank:Checking       100.00 USD
      Expenses:Restaurant       -100.00 USD

    2011-01-01 * "Dinner with Uno"
      Assets:Bank:Checking       101.00 USD
      Expenses:Restaurant       -101.00 USD

    2012-02-02 * "Dinner with Dos"
      Assets:Bank:Checking       102.00 USD
      Expenses:Restaurant       -102.00 USD

    2013-03-03 * "Dinner with Tres"
      Assets:Bank:Checking       103.00 USD
      Expenses:Restaurant       -103.00 USD

    2013-10-10 * "International Transfer"
      Assets:Bank:Checking         -50.00 USD
      Assets:ForeignBank:Checking  -60.00 CAD @ 1.20 USD

    2014-04-04 * "Dinner with Quatro"
      Assets:Bank:Checking       104.00 USD
      Expenses:Restaurant       -104.00 USD

    """)


class TestFundamentals(QueryBase):
    INPUT = textwrap.dedent("""
          2022-04-05 commodity TEST
            rate: 42
          2022-04-05 open Assets:Tests
          2022-04-05 * "Test"
            Assets:Tests  1.000 TEST
              int: 1
              decimal: 1.2
              bool: TRUE
              str: "str"
              str3: "3"
              str4: "4.0"
              date: 2022-04-05
              null: NULL
        """)

    def test_type_casting(self):
        # bool
        self.assertResult("SELECT bool(TRUE)", True)
        self.assertResult("SELECT bool(1)", True)
        self.assertResult("SELECT bool(1.1)", True)
        self.assertResult("SELECT bool('foo')", True)
        self.assertResult("SELECT bool(NULL)", None, bool)
        self.assertResult("SELECT bool(2022-04-05)", True)
        self.assertResult("SELECT bool(meta('int'))", True)
        self.assertResult("SELECT bool(meta('decimal'))", True)
        self.assertResult("SELECT bool(meta('bool'))", True)
        self.assertResult("SELECT bool(meta('str'))", True)
        self.assertResult("SELECT bool(meta('date'))", True)
        self.assertResult("SELECT bool(meta('null'))", None, bool)
        self.assertResult("SELECT bool(meta('missing'))", None, bool)

        # int
        self.assertResult("SELECT int(TRUE)", 1)
        self.assertResult("SELECT int(1)", 1)
        self.assertResult("SELECT int(1.2)", 1)
        self.assertResult("SELECT int('1')", 1)
        self.assertResult("SELECT int('foo')", None, int)
        self.assertResult("SELECT int(NULL)", None, int)
        self.assertError ("SELECT int(2022-04-05)")
        self.assertResult("SELECT int(meta('int'))", 1)
        self.assertResult("SELECT int(meta('decimal'))", 1)
        self.assertResult("SELECT int(meta('bool'))", 1)
        self.assertResult("SELECT int(meta('str'))", None, int)
        self.assertResult("SELECT int(meta('str3'))", 3)
        self.assertResult("SELECT int(meta('str4'))", None, int)
        self.assertResult("SELECT int(meta('date'))", None, int)
        self.assertResult("SELECT int(meta('null'))", None, int)
        self.assertResult("SELECT int(meta('missing'))", None, int)

        # decimal
        self.assertResult("SELECT decimal(TRUE)", Decimal(1))
        self.assertResult("SELECT decimal(1)", Decimal(1))
        self.assertResult("SELECT decimal(1.2)", Decimal('1.2'))
        self.assertResult("SELECT decimal('1.2')", Decimal('1.2'))
        self.assertResult("SELECT decimal('foo')", None, Decimal)
        self.assertResult("SELECT decimal(NULL)", None, Decimal)
        self.assertError ("SELECT decimal(2022-04-05)")
        self.assertResult("SELECT decimal(meta('int'))", Decimal(1))
        self.assertResult("SELECT decimal(meta('decimal'))", Decimal('1.2'))
        self.assertResult("SELECT decimal(meta('bool'))", Decimal(1))
        self.assertResult("SELECT decimal(meta('str'))", None, Decimal)
        self.assertResult("SELECT decimal(meta('str3'))", Decimal(3))
        self.assertResult("SELECT decimal(meta('str4'))", Decimal('4.0'))
        self.assertResult("SELECT decimal(meta('date'))", None, Decimal)
        self.assertResult("SELECT decimal(meta('null'))", None, Decimal)
        self.assertResult("SELECT decimal(meta('missing'))", None, Decimal)

        # str
        self.assertResult("SELECT str(TRUE)", 'TRUE')
        self.assertResult("SELECT str(FALSE)", 'FALSE')
        self.assertResult("SELECT str(1)", '1')
        self.assertResult("SELECT str(1.1)", '1.1')
        self.assertResult("SELECT str('foo')", 'foo')
        self.assertResult("SELECT str(NULL)", None, str)
        self.assertResult("SELECT str(2022-04-05)", '2022-04-05')
        self.assertResult("SELECT str(meta('int'))", '1')
        self.assertResult("SELECT str(meta('decimal'))", '1.2')
        self.assertResult("SELECT str(meta('bool'))", 'TRUE')
        self.assertResult("SELECT str(meta('str'))", 'str')
        self.assertResult("SELECT str(meta('date'))", '2022-04-05')
        self.assertResult("SELECT str(meta('null'))", None, str)
        self.assertResult("SELECT str(meta('missing'))", None, str)

        # date
        self.assertError ("SELECT date(TRUE)")
        self.assertError ("SELECT date(1)")
        self.assertError ("SELECT date(1.2)")
        self.assertResult("SELECT date('1.2')", None, datetime.date)
        self.assertResult("SELECT date('foo')", None, datetime.date)
        self.assertResult("SELECT date('2022-04-05')", datetime.date(2022, 4, 5))
        self.assertResult("SELECT date(NULL)", None, datetime.date)
        self.assertResult("SELECT date(2022-04-05)", datetime.date(2022, 4, 5))
        self.assertResult("SELECT date(2022, 4, 5)", datetime.date(2022, 4, 5))
        self.assertResult("SELECT date(meta('int'))", None, datetime.date)
        self.assertResult("SELECT date(meta('decimal'))", None, datetime.date)
        self.assertResult("SELECT date(meta('bool'))", None, datetime.date)
        self.assertResult("SELECT date(meta('str'))", None, datetime.date)
        self.assertResult("SELECT date(meta('date'))", datetime.date(2022, 4, 5))
        self.assertResult("SELECT date(meta('null'))", None, datetime.date)
        self.assertResult("SELECT date(meta('missing'))", None, datetime.date)

    def test_operators(self):
        # add
        self.assertResult("SELECT 1 + 1", 2)
        self.assertResult("SELECT 1.0 + 1", Decimal(2))
        self.assertResult("SELECT 1.0 + 2.00", Decimal(3))
        self.assertError ("SELECT 1970-01-01 + 2022-04-01")
        self.assertResult("SELECT 2022-04-01 + 1", datetime.date(2022, 4, 2))
        self.assertResult("SELECT 1 + 2022-04-01", datetime.date(2022, 4, 2))

        # sub
        self.assertResult("SELECT 1 - 1", 0)
        self.assertResult("SELECT 1.0 - 1", Decimal(0))
        self.assertResult("SELECT 1.0 - 2.00", Decimal(-1))
        self.assertResult("SELECT 2022-04-01 - 1", datetime.date(2022, 3, 31))
        self.assertResult("SELECT 2022-04-01 - 2022-03-31", 1)
        self.assertError ("SELECT 1 - 2022-04-01")

        # mul
        self.assertResult("SELECT 2 * 2", 4)
        self.assertResult("SELECT 2.0 * 2", Decimal(4))
        self.assertResult("SELECT 2 * 2.0", Decimal(4))
        self.assertResult("SELECT 2.0 * 2.0", Decimal(4))

        # div
        self.assertResult("SELECT 4 / 2", Decimal(2))
        self.assertResult("SELECT 4.0 / 2", Decimal(2))
        self.assertResult("SELECT 4 / 2.0", Decimal(2))
        self.assertResult("SELECT 4.0 / 2.0", Decimal(2))
        self.assertResult("SELECT 4 / 0", None, Decimal)
        self.assertResult("SELECT 4.0 / 0", None, Decimal)
        self.assertResult("SELECT 4 / 0.0", None, Decimal)

        # mod
        self.assertResult("SELECT 3 % 2", 1)
        self.assertResult("SELECT 3.0 % 2", Decimal(1))
        self.assertResult("SELECT 3 % 2.0", Decimal(1))
        self.assertResult("SELECT 3.0 % 2.0", Decimal(1))

        # match
        self.assertResult("SELECT 'foobarbaz' ~ 'bar'", True)
        self.assertResult("SELECT 'foobarbaz' ~ 'quz'", False)

        # mot match
        self.assertResult("SELECT 'foobarbaz' !~ 'bar'", False)
        self.assertResult("SELECT 'foobarbaz' !~ 'quz'", True)

        # and
        self.assertResult("SELECT 1 and FALSE", False)
        self.assertResult("SELECT 'something' and FALSE", False)
        self.assertResult("SELECT 1.0 and FALSE", False)
        self.assertResult("SELECT TRUE and meta('missing')", None, bool)
        self.assertResult("SELECT TRUE and not meta('missing')", True)

        # or
        self.assertResult("SELECT FALSE or 1", True)
        self.assertResult("SELECT FALSE or 'something'", True)
        self.assertResult("SELECT FALSE or 1.0", True)
        self.assertResult("SELECT TRUE or meta('missing')", True)
        self.assertResult("SELECT FALSE or not meta('missing')", True)

        # not
        self.assertResult("SELECT not TRUE", False)
        self.assertResult("SELECT not meta('missing')", True)

        # is null
        self.assertResult("SELECT date IS NULL", False)
        self.assertResult("SELECT meta('missing') IS NULL", True)
        self.assertResult("SELECT meta('int') IS NULL", False)

        # is not null
        self.assertResult("SELECT date IS NOT NULL", True)
        self.assertResult("SELECT meta('missing') IS NOT NULL", False)
        self.assertResult("SELECT meta('int') IS NOT NULL", True)

        # in
        self.assertResult("SELECT 'tag' IN tags", False)
        self.assertResult("SELECT 3 IN (2, 3, 4)", True)
        self.assertResult("SELECT 'x' IN ('a', 'b', 'c')", False)
        self.assertResult("SELECT 3 NOT IN (2, 3, 4)", False)
        self.assertResult("SELECT 1 NOT IN (2, 3, 4)", True)

        # between
        self.assertResult("SELECT 2 BETWEEN 1 AND 3", True)
        self.assertResult("SELECT 1 BETWEEN 2 AND 3", False)
        self.assertResult("SELECT 2.0 BETWEEN 1.0 AND 3.0", True)
        self.assertResult("SELECT 1.0 BETWEEN 2 AND 3.0", False)
        self.assertResult("SELECT 2023-08-02 BETWEEN 2023-08-01 AND 2023-08-03", True)
        self.assertResult("SELECT 'b' BETWEEN 'a' AND 'c'", True)
        self.assertError ("SELECT 2023-08-02 BETWEEN 'a' AND 2")

    def test_operators_type_inference(self):
        self.assertResult("SELECT 1 + meta('int')", Decimal(2))
        self.assertResult("SELECT 1 + meta('str3')", Decimal(4))
        self.assertResult("SELECT meta('int') > 0", True)

    def test_functions(self):
        # round
        self.assertResult("SELECT round(1.2)", Decimal(1))
        self.assertResult("SELECT round(1.234, 2)", Decimal('1.23'))
        self.assertResult("SELECT round(12)", 12)
        self.assertResult("SELECT round(12, -1)", 10)

        # neg
        self.assertResult("SELECT neg(1.0)", Decimal('-1.0'))

        # abs
        self.assertResult("SELECT abs(-1.0)", Decimal('1.0'))

        # safediv
        self.assertResult("SELECT safediv(1.0, 2.0)", Decimal('0.5'))
        self.assertResult("SELECT safediv(1.0, 0.0)", Decimal('0'))
        self.assertResult("SELECT safediv(0.0, 0.0)", Decimal('0'))

        # repr
        self.assertResult("SELECT repr(1.0)", 'Decimal(\'1.0\')')

        # lower
        self.assertResult('''SELECT lower('AaBbCcDdEe')''', 'aabbccddee')

        # upper
        self.assertResult('''SELECT upper('AaBbCcDdEe')''', 'AABBCCDDEE')

    def test_coalesce(self):
        # coalesce
        self.assertResult("SELECT COALESCE(str(meta('missing')), '!')", "!")
        self.assertError ("SELECT COALESCE(meta('missing'), '!')")

    def test_count(self):
        # count number of postings
        self.assertResult("SELECT COUNT(*)", 1)
        self.assertResult("SELECT COUNT(1)", 1)
        self.assertResult("SELECT COUNT(NULL)", 0)
        self.assertResult("SELECT COUNT(meta('missing'))", 0)

    def test_getitem(self):
        self.assertResult("SELECT meta['lineno']", 6, object)
        self.assertResult("SELECT 1 + meta['lineno']", Decimal('7'))
        self.assertResult("SELECT meta['filename']", '<string>', object)
        self.assertResult("SELECT meta['missing']", None, object)
        self.assertError ("SELECT position['foo']")

    def test_getatrr(self):
        self.assertResult("SELECT entry.flag", '*')
        self.assertResult("SELECT int(entry.meta['lineno'])", 5)
        self.assertError ("SELECT entry.foo")
        self.assertError ("SELECT position.foo")

    def test_parse_date(self):
        self.assertResult('''SELECT parse_date('2016/11/1')''', datetime.date(2016, 11, 1))
        self.assertResult('''SELECT parse_date('2016/11/1', '%Y/%d/%m')''', datetime.date(2016, 1, 11))

    def test_date_part(self):
        self.assertResult('''SELECT date_part('weekday', 2024-06-09)''', 6)
        self.assertResult('''SELECT date_part('dow', 2024-06-09)''', 6)
        self.assertResult('''SELECT date_part('isoweekday', 2024-06-09)''', 7)
        self.assertResult('''SELECT date_part('isodow', 2024-06-09)''', 7)
        self.assertResult('''SELECT date_part('week', 2024-06-09)''', 23)
        self.assertResult('''SELECT date_part('month', 2024-06-09)''', 6)
        self.assertResult('''SELECT date_part('quarter', 2024-06-09)''', 2)
        self.assertResult('''SELECT date_part('year', 2024-06-09)''', 2024)
        self.assertResult('''SELECT date_part('isoyear', 2024-06-09)''', 2024)
        self.assertResult('''SELECT date_part('decade', 2024-06-09)''', 202)
        self.assertResult('''SELECT date_part('century', 2024-06-09)''', 21)
        self.assertResult('''SELECT date_part('millennium', 2024-06-09)''', 3)
        self.assertResult('''SELECT date_part('epoch', 2024-06-09)''', 1717891200)
        self.assertResult('''SELECT date_part('baz', 2024-06-09)''', None, int)

    def test_date_trunc(self):
        self.assertResult('''SELECT date_trunc('week', 2016-11-14)''', datetime.date(2016, 11, 14)) # monday
        self.assertResult('''SELECT date_trunc('week', 2016-11-15)''', datetime.date(2016, 11, 14)) # tuesday
        self.assertResult('''SELECT date_trunc('week', 2016-11-20)''', datetime.date(2016, 11, 14)) # sunday

        self.assertResult('''SELECT date_trunc('month', 2016-11-20)''', datetime.date(2016, 11, 1))

        self.assertResult('''SELECT date_trunc('quarter', 2016-09-30)''', datetime.date(2016, 7, 1))
        self.assertResult('''SELECT date_trunc('quarter', 2016-10-01)''', datetime.date(2016, 10, 1))
        self.assertResult('''SELECT date_trunc('quarter', 2016-11-20)''', datetime.date(2016, 10, 1))

        self.assertResult('''SELECT date_trunc('year', 2016-11-20)''', datetime.date(2016, 1, 1))

        self.assertResult('''SELECT date_trunc('decade', 2016-11-20)''', datetime.date(2010, 1, 1))
        self.assertResult('''SELECT date_trunc('decade', 2020-11-20)''', datetime.date(2020, 1, 1))
        self.assertResult('''SELECT date_trunc('decade', 2029-11-20)''', datetime.date(2020, 1, 1))

        self.assertResult('''SELECT date_trunc('century', 1999-11-20)''', datetime.date(1901, 1, 1))
        self.assertResult('''SELECT date_trunc('century', 2000-11-20)''', datetime.date(1901, 1, 1))
        self.assertResult('''SELECT date_trunc('century', 2001-11-20)''', datetime.date(2001, 1, 1))
        self.assertResult('''SELECT date_trunc('century', 2016-11-20)''', datetime.date(2001, 1, 1))

        self.assertResult('''SELECT date_trunc('millennium', 1991-11-20)''', datetime.date(1001, 1, 1))
        self.assertResult('''SELECT date_trunc('millennium', 2000-11-20)''', datetime.date(1001, 1, 1))
        self.assertResult('''SELECT date_trunc('millennium', 2001-11-20)''', datetime.date(2001, 1, 1))
        self.assertResult('''SELECT date_trunc('millennium', 2016-11-20)''', datetime.date(2001, 1, 1))
        self.assertResult('''SELECT date_trunc('millennium', 3456-11-20)''', datetime.date(3001, 1, 1))

        self.assertResult('''SELECT date_trunc('foo', 2024-05-24)''', None, datetime.date)

    def test_interval_ops(self):
        self.assertResult('''SELECT 2016-11-20 + interval("1 months")''', datetime.date(2016, 12, 20))
        self.assertResult('''SELECT 2016-11-01 + interval("1 months") - interval("1 days")''', datetime.date(2016, 11, 30))
        self.assertResult('''SELECT 2024-02-01 + interval("1 month") + interval("-1 day")''', datetime.date(2024, 2, 29))
        self.assertResult('''SELECT 2024-02-05 + interval("-1 days")''', datetime.date(2024, 2, 4))
        self.assertResult('''SELECT 2024-02-05 + interval("1 day") + interval("2 days")''', datetime.date(2024, 2, 8))
        self.assertResult('''SELECT 2024-05-24 + interval("1 year")''', datetime.date(2025, 5, 24))
        self.assertResult('''SELECT 2024-05-24 + interval("-2 years")''', datetime.date(2022, 5, 24))
        self.assertResult('''SELECT interval("1 baz")''', None, relativedelta)
        self.assertResult('''SELECT interval("A days")''', None, relativedelta)


class TestBeancountFunctions(QueryBase):
    INPUT = textwrap.dedent("""
          2024-06-23 commodity TEST
            answer: "42"
          2024-06-23 open Assets:Tests
            key: "value"
          2024-06-23 open Expenses:Tests
          2024-06-23 price USD 2.0 TEST
          2024-06-23 * "Test"
            Assets:Tests  10.000 TEST
            Expenses:Tests
          2024-06-24 close Expenses:Tests
        """)

    def test_root(self):
        self.assertResult('''SELECT root('Assets:Foo:Bar', 2) FROM #''', 'Assets:Foo')
        self.assertResult('''SELECT root('Assets:Foo:Bar', 1) FROM #''', 'Assets')
        self.assertResult('''SELECT root('Assets:Foo:Bar') FROM #''', 'Assets')

    def test_open_date(self):
        self.assertResult('''SELECT open_date('Expenses:Tests') FROM #''', datetime.date(2024, 6, 23))
        self.assertResult('''SELECT open_date('Expenses:Foo') FROM #''', None, datetime.date)

    def test_close_date(self):
        self.assertResult('''SELECT close_date('Expenses:Tests') FROM #''', datetime.date(2024, 6, 24))
        self.assertResult('''SELECT close_date('Expenses:Foo') FROM #''', None, datetime.date)
        self.assertResult('''SELECT close_date('Assets:Tests') FROM #''', None, datetime.date)

    def test_open_meta(self):
        self.assertResult('''SELECT open_meta('Assets:Tests') FROM #''', {'filename': '<string>', 'lineno': 4, 'key': 'value'})
        self.assertResult('''SELECT open_meta('Assets:Tests', 'key') FROM #''', 'value', object)
        self.assertResult('''SELECT open_meta('Expenses:Tests', 'baz') FROM #''', None, object)
        self.assertResult('''SELECT open_meta('Expenses:Foo', 'key') FROM #''', None, object)

    def test_commodity_meta(self):
        self.assertResult('''SELECT commodity_meta('TEST', 'answer') FROM #''', '42', object)
        self.assertResult('''SELECT commodity_meta('TEST') FROM #''', {'filename': '<string>', 'lineno': 2, 'answer': '42'})
        self.assertResult('''SELECT commodity_meta('X') FROM #''', None, dict)

    def test_account_sortkey(self):
        self.assertResult('''SELECT account_sortkey('Assets:Foo') LIMIT 1''', '0-Assets:Foo')

    def test_has_account(self):
        self.assertResult('''SELECT has_account('Assets:Tests') LIMIT 1''', True)
        self.assertResult('''SELECT has_account('Assets:Foo') LIMIT 1''', False)

    def test_convert(self):
        self.assertResult('''SELECT convert(position, 'USD') LIMIT 1''', A('5.0 USD'))
        self.assertResult('''SELECT convert(sum(position), 'USD')''', I('0.0 USD'))
        self.assertResult('''SELECT convert(only('TEST', sum(position)), 'USD')''', A('0.0 USD'))


class TestBeancountMetadataFunctions(QueryBase):
    INPUT = """
        2016-11-20 * "Test"
          name: "The Name"
          address: "1 Wrong Way"
          empty: "Not Empty"
          Assets:Banking          1 USD
            color: "Green"
            address: "1 Right Way"
            empty:
        """

    def test_meta(self):
        self.assertResult('''SELECT meta('address')''', '1 Right Way', object)
        self.assertResult('''SELECT meta('baz')''', None, object)
        self.assertResult('''SELECT meta('color')''', 'Green', object)
        self.assertResult('''SELECT meta('empty')''', None, object)
        self.assertResult('''SELECT meta('name')''', None, object)

    def test_entry_meta(self):
        self.assertResult('''SELECT entry_meta('address')''', '1 Wrong Way', object)
        self.assertResult('''SELECT entry_meta('baz')''', None, object)
        self.assertResult('''SELECT entry_meta('color')''', None, object)
        self.assertResult('''SELECT entry_meta('empty')''', 'Not Empty', object)
        self.assertResult('''SELECT entry_meta('name')''', 'The Name', object)

    def test_any_meta(self):
        self.assertResult('''SELECT any_meta('address')''', '1 Right Way', object)
        self.assertResult('''SELECT any_meta('baz')''', None, object)
        self.assertResult('''SELECT any_meta('color')''', 'Green', object)
        self.assertResult('''SELECT any_meta('empty')''', None, object)
        self.assertResult('''SELECT any_meta('name')''', 'The Name', object)


class TestFilterEntries(CommonInputBase, QueryBase):

    @staticmethod
    def filter_entries(query):
        entries = []
        expr = query.c_where
        context = qe.Row(entries, query.table.options)
        for entry in query.table.prepare():
            context.entry = entry
            if expr is None or expr(context):
                entries.append(entry)
        return entries

    def test_filter_empty_from(self):
        # Check that no filter outputs the very same thing.
        filtered_entries = self.filter_entries(self.compile("""
          SELECT * ;
        """))
        self.assertEqualEntries(self.INPUT, filtered_entries)

    def test_filter_by_year(self):
        filtered_entries = self.filter_entries(self.compile("""
          SELECT date, type FROM year(date) = 2012;
        """))
        self.assertEqualEntries("""

          2012-02-02 * "Dinner with Dos"
            Assets:Bank:Checking              102.00 USD
            Expenses:Restaurant              -102.00 USD

        """, filtered_entries)

    def test_filter_by_expr1(self):
        filtered_entries = self.filter_entries(self.compile("""
          SELECT date, type
          FROM NOT (type = 'transaction' AND
                    (year(date) = 2012 OR year(date) = 2013));
        """))
        self.assertEqualEntries("""

          2010-01-01 open Assets:Bank:Checking
          2010-01-01 open Assets:Bank:Savings
          2010-01-01 open Expenses:Restaurant
          2010-01-01 open Assets:ForeignBank:Checking

          2010-01-01 * "Dinner with Cero"
            Assets:Bank:Checking              100.00 USD
            Expenses:Restaurant              -100.00 USD

          2011-01-01 * "Dinner with Uno"
            Assets:Bank:Checking              101.00 USD
            Expenses:Restaurant              -101.00 USD

          2014-04-04 * "Dinner with Quatro"
            Assets:Bank:Checking              104.00 USD
            Expenses:Restaurant              -104.00 USD

        """, filtered_entries)

    def test_filter_by_expr2(self):
        filtered_entries = self.filter_entries(self.compile("""
          SELECT date, type FROM date < 2012-06-01;
        """))
        self.assertEqualEntries("""

          2010-01-01 open Assets:Bank:Checking
          2010-01-01 open Assets:Bank:Savings
          2010-01-01 open Expenses:Restaurant
          2010-01-01 open Assets:ForeignBank:Checking

          2010-01-01 * "Dinner with Cero"
            Assets:Bank:Checking              100.00 USD
            Expenses:Restaurant              -100.00 USD

          2011-01-01 * "Dinner with Uno"
            Assets:Bank:Checking              101.00 USD
            Expenses:Restaurant              -101.00 USD

          2012-02-02 * "Dinner with Dos"
            Assets:Bank:Checking              102.00 USD
            Expenses:Restaurant              -102.00 USD

        """, filtered_entries)

    def test_filter_close_undated(self):
        filtered_entries = self.filter_entries(self.compile("""
          SELECT date, type FROM CLOSE;
        """))

        self.assertEqualEntries(self.INPUT + textwrap.dedent("""

          2014-04-04 C "Conversion for (-50.00 USD, -60.00 CAD)"
            Equity:Conversions:Current  50.00 USD @ 0 NOTHING
            Equity:Conversions:Current  60.00 CAD @ 0 NOTHING

        """), filtered_entries)

    def test_filter_close_dated(self):
        filtered_entries = self.filter_entries(self.compile("""
          SELECT date, type FROM CLOSE ON 2013-06-01;
        """))
        self.assertEqualEntries("""

          2010-01-01 open Assets:Bank:Checking
          2010-01-01 open Assets:ForeignBank:Checking
          2010-01-01 open Assets:Bank:Savings

          2010-01-01 open Expenses:Restaurant

          2010-01-01 * "Dinner with Cero"
            Assets:Bank:Checking       100.00 USD
            Expenses:Restaurant       -100.00 USD

          2011-01-01 * "Dinner with Uno"
            Assets:Bank:Checking       101.00 USD
            Expenses:Restaurant       -101.00 USD

          2012-02-02 * "Dinner with Dos"
            Assets:Bank:Checking       102.00 USD
            Expenses:Restaurant       -102.00 USD

          2013-03-03 * "Dinner with Tres"
            Assets:Bank:Checking       103.00 USD
            Expenses:Restaurant       -103.00 USD

        """, filtered_entries)

    def test_filter_open_dated(self):
        filtered_entries = self.filter_entries(self.compile("""
          SELECT date, type FROM OPEN ON 2013-01-01;
        """))

        self.assertEqualEntries("""

          2010-01-01 open Assets:Bank:Checking
          2010-01-01 open Assets:Bank:Savings
          2010-01-01 open Expenses:Restaurant
          2010-01-01 open Assets:ForeignBank:Checking

          2012-12-31 S "Opening balance for 'Assets:Bank:Checking' (Summarization)"
            Assets:Bank:Checking          303.00 USD
            Equity:Opening-Balances      -303.00 USD

          2012-12-31 S "Opening balance for 'Equity:Earnings:Previous' (Summarization)"
            Equity:Earnings:Previous     -303.00 USD
            Equity:Opening-Balances       303.00 USD

          2013-03-03 * "Dinner with Tres"
            Assets:Bank:Checking          103.00 USD
            Expenses:Restaurant          -103.00 USD

          2013-10-10 * "International Transfer"
            Assets:Bank:Checking          -50.00 USD            ; -50.00 USD
            Assets:ForeignBank:Checking   -60.00 CAD @ 1.20 USD ; -72.00 USD

          2014-04-04 * "Dinner with Quatro"
            Assets:Bank:Checking          104.00 USD
            Expenses:Restaurant          -104.00 USD

        """, filtered_entries)

    def test_filter_clear(self):
        filtered_entries = self.filter_entries(self.compile("""
          SELECT date, type FROM CLEAR;
        """))

        self.assertEqualEntries(self.INPUT + textwrap.dedent("""

          2014-04-04 T "Transfer balance for 'Expenses:Restaurant' (Transfer balance)"
            Expenses:Restaurant                                 510.00 USD
            Equity:Earnings:Current                            -510.00 USD

        """), filtered_entries)


class TestExecutePrint(CommonInputBase, QueryBase):

    def test_print_with_filter(self):
        oss = io.StringIO()
        qx.execute_print(self.compile("PRINT FROM year = 2012"), oss)

        self.assertEqualEntries("""

          2012-02-02 * "Dinner with Dos"
            Assets:Bank:Checking                                                   102.00 USD
            Expenses:Restaurant                                                   -102.00 USD

        """, oss.getvalue())

    def test_print_with_no_filter(self):
        oss = io.StringIO()
        qx.execute_print(self.compile("PRINT"), oss)
        self.assertEqualEntries(self.INPUT, oss.getvalue())


class TestAllocation(unittest.TestCase):

    def test_allocator(self):
        allocator = qx.Allocator()
        self.assertEqual(0, allocator.allocate())
        self.assertEqual(1, allocator.allocate())
        self.assertEqual(2, allocator.allocate())
        self.assertEqual([None, None, None], allocator.create_store())


class TestExecuteNonAggregatedQuery(QueryBase):

    INPUT = """

      2010-01-01 open Assets:Bank:Checking
      2010-01-01 open Expenses:Restaurant

      2010-02-23 * "Bla"
        Assets:Bank:Checking       100.00 USD
        Expenses:Restaurant       -100.00 USD

    """

    def test_non_aggregate__one(self):
        self.check_query(
            self.INPUT,
            """
            SELECT date;
            """,
            [('date', datetime.date)],
            [(datetime.date(2010, 2, 23),),
             (datetime.date(2010, 2, 23),)])

    def test_non_aggregate__many(self):
        self.check_query(
            self.INPUT,
            """
            SELECT date, flag, payee, narration;
            """,
            [
                ('date', datetime.date),
                ('flag', str),
                ('payee', str),
                ('narration', str),
                ],
            [
                (datetime.date(2010, 2, 23), '*', None, 'Bla'),
                (datetime.date(2010, 2, 23), '*', None, 'Bla'),
                ])

    def test_non_aggregated_order_by_visible(self):
        self.check_query(
            self.INPUT,
            """
            SELECT account, length(account) ORDER BY 2;
            """,
            [
                ('account', str),
                ('length(account)', int),
                ],
            [
                ('Expenses:Restaurant', 19),
                ('Assets:Bank:Checking', 20),
                ])

    def test_non_aggregated_order_by_invisible(self):
        self.check_query(
            self.INPUT,
            """
            SELECT account ORDER BY length(account);
            """,
            [
                ('account', str),
                ],
            [
                ('Expenses:Restaurant',),
                ('Assets:Bank:Checking',),
                ])

    def test_non_aggregated_order_by_none_date(self):
        self.check_query(
            self.INPUT,
            """
            SELECT account ORDER BY cost_date;
            """,
            [
                ('account', str),
                ],
            [
                ('Assets:Bank:Checking',),
                ('Expenses:Restaurant',),
                ])

    def test_non_aggregated_order_by_none_str(self):
        self.check_query(
            self.INPUT,
            """
            SELECT account ORDER BY posting_flag;
            """,
            [
                ('account', str),
                ],
            [
                ('Assets:Bank:Checking',),
                ('Expenses:Restaurant',),
                ])


class TestExecuteAggregatedQuery(QueryBase):

    INPUT = """

      2010-01-01 open Assets:Bank:Checking
      2010-01-01 open Expenses:Restaurant

      2010-02-23 * "Bla"
        Assets:Bank:Checking       100.00 USD
        Expenses:Restaurant       -100.00 USD

    """

    def test_aggregated_group_by_all_implicit(self):
        # There is no group-by, but all columns are aggregations.
        self.check_query(
            self.INPUT,
            """
            SELECT first(account), last(account);
            """,
            [
                ('first(account)', str),
                ('last(account)', str),
                ],
            [
                ('Assets:Bank:Checking', 'Expenses:Restaurant'),
                ])

    def test_aggregated_group_by_all_explicit(self):
        # All columns ('account', 'len') are subject of a group-by.
        self.check_query(
            self.INPUT,
            """
            SELECT account, length(account) as len
            GROUP BY account, len;
            """,
            [
                ('account', str),
                ('len', int),
                ],
            [
                ('Assets:Bank:Checking', 20),
                ('Expenses:Restaurant', 19),
                ])

        self.check_query(
            """
            2010-02-21 * "First"
              Assets:Bank:Checking       -1.00 USD
              Expenses:Restaurant         1.00 USD

            2010-02-23 * "Second"
              Liabilities:Credit-Card    -2.00 USD
              Expenses:Restaurant         2.00 USD
            """,
            """
            SELECT account, length(account) as len
            GROUP BY account, len;
            """,
            [
                ('account', str),
                ('len', int),
                ],
            [
                ('Assets:Bank:Checking', 20),
                ('Expenses:Restaurant', 19),
                ('Liabilities:Credit-Card', 23),
                ])

    def test_aggregated_group_by_visible(self):
        # GROUP-BY: 'account' is visible.
        self.check_query(
            self.INPUT,
            """
            SELECT account, sum(position) as amount
            GROUP BY account;
            """,
            [
                ('account', str),
                ('amount', inventory.Inventory),
                ],
            [
                ('Assets:Bank:Checking', inventory.from_string('100.00 USD')),
                ('Expenses:Restaurant', inventory.from_string('-100.00 USD')),
                ])

    def test_aggregated_group_by_invisible(self):
        # GROUP-BY: 'account' is invisible.
        self.check_query(
            self.INPUT,
            """
            SELECT count(position)
            GROUP BY account;
            """,
            [
                ('count(position)', int),
                ],
            [
                (1,),
                (1,),
                ])

    def test_aggregated_group_by_visible_order_by_non_aggregate_visible(self):
        # GROUP-BY: 'account' is visible.
        # ORDER-BY: 'account' is a non-aggregate and visible.
        self.check_query(
            self.INPUT,
            """
            SELECT account, sum(position) as amount
            GROUP BY account
            ORDER BY account;
            """,
            [
                ('account', str),
                ('amount', inventory.Inventory),
                ],
            [
                ('Assets:Bank:Checking', inventory.from_string('100.00 USD')),
                ('Expenses:Restaurant', inventory.from_string('-100.00 USD')),
                ])

    def test_aggregated_group_by_visible_order_by_non_aggregate_invisible(self):
        # GROUP-BY: 'account' and 'length(account)' are visible.
        # ORDER-BY: 'length(account)' is a non-aggregate and invisible.
        self.check_query(
            self.INPUT,
            """
            SELECT account, sum(position) as amount
            GROUP BY account, length(account)
            ORDER BY length(account);
            """,
            [
                ('account', str),
                ('amount', inventory.Inventory),
                ],
            [
                ('Expenses:Restaurant', inventory.from_string('-100.00 USD')),
                ('Assets:Bank:Checking', inventory.from_string('100.00 USD')),
                ])

    def test_aggregated_group_by_visible_order_by_aggregate_visible(self):
        # GROUP-BY: 'account' is visible.
        # ORDER-BY: 'sum(account)' is an aggregate and visible.
        self.check_query(
            """
            2010-02-21 * "First"
              Assets:Bank:Checking       -1.00 USD
              Expenses:Restaurant         1.00 USD

            2010-02-23 * "Second"
              Liabilities:Credit-Card    -2.00 USD
              Expenses:Restaurant         2.00 USD
            """,
            """
            SELECT account, count(account) as num, sum(number) as sum
            GROUP BY account
            ORDER BY sum(number);
            """,
            [
                ('account', str),
                ('num', int),
                ('sum', Decimal),
                ],
            [
                ('Liabilities:Credit-Card', 1, D('-2.00')),
                ('Assets:Bank:Checking', 1, D('-1.00')),
                ('Expenses:Restaurant', 2, D('3.00')),
                ])

    def test_aggregated_group_by_visible_order_by_aggregate_invisible(self):
        # GROUP-BY: 'account' is visible.
        # ORDER-BY: 'sum(number)' is an aggregate and invisible.
        self.check_query(
            """
            2010-02-21 * "First"
              Assets:Bank:Checking       -1.00 USD
              Expenses:Restaurant         1.00 USD

            2010-02-23 * "Second"
              Liabilities:Credit-Card    -2.00 USD
              Expenses:Restaurant         2.00 USD
            """,
            """
            SELECT account, count(account) as num
            GROUP BY account
            ORDER BY sum(number);
            """,
            [
                ('account', str),
                ('num', int),
                ],
            [
                ('Liabilities:Credit-Card', 1),
                ('Assets:Bank:Checking', 1),
                ('Expenses:Restaurant', 2),
                ])

    def test_aggregated_group_by_invisible_order_by_non_aggregate_visible(self):
        # GROUP-BY: 'account' is invisible.
        # ORDER-BY: 'len(account)' is a non-aggregate and visible.
        self.check_query(
            self.INPUT,
            """
            SELECT length(account) as len, sum(position) as amount
            GROUP BY account, len
            ORDER BY len;
            """,
            [
                ('len', int),
                ('amount', inventory.Inventory),
                ],
            [
                (19, inventory.from_string('-100.00 USD'),),
                (20, inventory.from_string('100.00 USD'),),
                ])

    def test_aggregated_group_by_invisible_order_by_non_aggregate_invis(self):
        # GROUP-BY: 'account' is invisible.
        # ORDER-BY: 'sum(number)' is an aggregate and invisible.
        self.check_query(
            """
            2010-02-21 * "First"
              Assets:Bank:Checking       -1.00 USD
              Expenses:Restaurant         1.00 USD

            2010-02-23 * "Second"
              Liabilities:Credit-Card    -2.00 USD
              Expenses:Restaurant         2.00 USD
            """,
            """
            SELECT count(account) as num
            GROUP BY account
            ORDER BY sum(number);
            """,
            [
                ('num', int),
                ],
            [
                (1,),
                (1,),
                (2,),
                ])

    def test_aggregated_group_by_invisible_order_by_aggregate_visible(self):
        # GROUP-BY: 'account' is invisible.
        # ORDER-BY: 'sum(account)' is an aggregate and visible.
        self.check_query(
            """
            2010-02-21 * "First"
              Assets:Bank:Checking       -1.00 USD
              Expenses:Restaurant         1.00 USD

            2010-02-23 * "Second"
              Liabilities:Credit-Card    -2.00 USD
              Expenses:Restaurant         2.00 USD
            """,
            """
            SELECT count(account) as num, sum(number) as sum
            GROUP BY account
            ORDER BY sum(number);
            """,
            [
                ('num', int),
                ('sum', Decimal),
                ],
            [
                (1, D('-2.00')),
                (1, D('-1.00')),
                (2, D('3.00')),
                ])

    def test_aggregated_group_by_invisible_order_by_aggregate_invisible(self):
        # GROUP-BY: 'account' is invisible.
        # ORDER-BY: 'sum(number)' is an aggregate and invisible.
        self.check_query(
            """
            2010-02-21 * "First"
              Assets:Bank:Checking       -1.00 USD
              Expenses:Restaurant         1.00 USD

            2010-02-23 * "Second"
              Liabilities:Credit-Card    -2.00 USD
              Expenses:Restaurant         2.00 USD
            """,
            """
            SELECT count(account) as num
            GROUP BY account
            ORDER BY sum(number);
            """,
            [
                ('num', int),
                ],
            [
                (1,),
                (1,),
                (2,),
                ])

    def test_aggregated_group_by_with_having(self):
        self.check_query(
            """
            2010-02-21 * "First"
              Assets:Bank:Checking       -1.00 USD
              Expenses:Foo                1.00 USD

            2010-02-23 * "Second"
              Liabilities:Credit-Card    -2.00 USD
              Expenses:Bar                2.00 USD
            """,
            """
            SELECT account, sum(number)
            GROUP BY account
            HAVING sum(number) > 0.0
            ORDER BY account
            """,
            [
                ('account', str),
                ('sum(number)', Decimal),
            ],
            [
                ('Expenses:Bar', D(2.0)),
                ('Expenses:Foo', D(1.0)),
            ])


class TestExecuteOptions(QueryBase):

    INPUT = """

      2010-02-23 *
        Assets:AssetA       5.00 USD
        Assets:AssetD       2.00 USD
        Assets:AssetB       4.00 USD
        Assets:AssetC       3.00 USD
        Assets:AssetE       1.00 USD
        Equity:Rest       -15.00 USD

    """

    def test_order_by_asc_implicit(self):
        self.check_query(
            self.INPUT,
            """
            SELECT account, number ORDER BY number;
            """,
            [
                ('account', str),
                ('number', Decimal),
                ],
            [
                ('Equity:Rest', D('-15.00')),
                ('Assets:AssetE', D('1.00')),
                ('Assets:AssetD', D('2.00')),
                ('Assets:AssetC', D('3.00')),
                ('Assets:AssetB', D('4.00')),
                ('Assets:AssetA', D('5.00')),
                ])

    def test_order_by_asc_explicit(self):
        self.check_query(
            self.INPUT,
            """
            SELECT account, number ORDER BY number ASC;
            """,
            [
                ('account', str),
                ('number', Decimal),
                ],
            [
                ('Equity:Rest', D('-15.00')),
                ('Assets:AssetE', D('1.00')),
                ('Assets:AssetD', D('2.00')),
                ('Assets:AssetC', D('3.00')),
                ('Assets:AssetB', D('4.00')),
                ('Assets:AssetA', D('5.00')),
                ])

    def test_order_by_desc(self):
        self.check_query(
            self.INPUT,
            """
            SELECT account, number ORDER BY number DESC;
            """,
            [
                ('account', str),
                ('number', Decimal),
                ],
            [
                ('Assets:AssetA', D('5.00')),
                ('Assets:AssetB', D('4.00')),
                ('Assets:AssetC', D('3.00')),
                ('Assets:AssetD', D('2.00')),
                ('Assets:AssetE', D('1.00')),
                ('Equity:Rest', D('-15.00')),
                ])

    def test_distinct(self):
        self.check_query(
            """
              2010-02-23 *
                Assets:AssetA       5.00 USD
                Assets:AssetA       2.00 USD
                Assets:AssetA       4.00 USD
                Equity:Rest
            """,
            """
            SELECT account ;
            """,
            [
                ('account', str),
                ],
            [
                ('Assets:AssetA',),
                ('Assets:AssetA',),
                ('Assets:AssetA',),
                ('Equity:Rest',),
                ])

        self.check_query(
            """
              2010-02-23 *
                Assets:AssetA       5.00 USD
                Assets:AssetA       2.00 USD
                Assets:AssetA       4.00 USD
                Equity:Rest        -5.00 USD
                Equity:Rest        -2.00 USD
                Equity:Rest        -4.00 USD
            """,
            """
            SELECT DISTINCT account ;
            """,
            [
                ('account', str),
                ],
            [
                ('Assets:AssetA',),
                ('Equity:Rest',),
                ])

    def test_limit(self):
        self.check_query(
            self.INPUT,
            """
            SELECT account, number ORDER BY number LIMIT 3;
            """,
            [
                ('account', str),
                ('number', Decimal),
                ],
            [
                ('Equity:Rest', D('-15.00')),
                ('Assets:AssetE', D('1.00')),
                ('Assets:AssetD', D('2.00')),
                ])


class TestOrderBy(QueryBase):
    data = """

    2022-03-28 * "Test"
      Assets:Tests  1.00 USD
        aa: 1
        bb: 1
      Assets:Tests  2.00 USD
        aa: 1
        bb: 2
      Assets:Tests  3.00 USD
        aa: 2
      Assets:Tests  4.00 USD
        aa: 2
        bb: 1
      Expenses:Tests

    """

    def test_order_by_asc_asc(self):
        self.check_query(self.data,
            """SELECT account, meta('aa') AS a, meta('bb') AS b ORDER BY 2, 3""",
            [
                ('account', str), ('a', object), ('b', object)
            ],
            [
                ('Expenses:Tests', None, None),
                ('Assets:Tests', 1, 1),
                ('Assets:Tests', 1, 2),
                ('Assets:Tests', 2, None),
                ('Assets:Tests', 2, 1),
            ])

    def test_order_by_asc_desc(self):
        self.check_query(self.data,
            """SELECT account, meta('aa') AS a, meta('bb') AS b ORDER BY 2, 3 DESC""",
            [
                ('account', str), ('a', object), ('b', object)
            ],
            [
                ('Expenses:Tests', None, None),
                ('Assets:Tests', 1, 2),
                ('Assets:Tests', 1, 1),
                ('Assets:Tests', 2, 1),
                ('Assets:Tests', 2, None),
            ])

    def test_order_by_desc_asc(self):
        self.check_query(self.data,
            """SELECT account, meta('aa') AS a, meta('bb') AS b ORDER BY 2 DESC, 3""",
            [
                ('account', str), ('a', object), ('b', object)
            ],
            [
                ('Assets:Tests', 2, None),
                ('Assets:Tests', 2, 1),
                ('Assets:Tests', 1, 1),
                ('Assets:Tests', 1, 2),
                ('Expenses:Tests', None, None),
            ])

    def test_order_by_desc_desc(self):
        self.check_query(self.data,
            """SELECT account, meta('aa') AS a, meta('bb') AS b ORDER BY 2 DESC, 3 DESC""",
            [
                ('account', str), ('a', object), ('b', object)
            ],
            [
                ('Assets:Tests', 2, 1),
                ('Assets:Tests', 2, None),
                ('Assets:Tests', 1, 2),
                ('Assets:Tests', 1, 1),
                ('Expenses:Tests', None, None),
            ])


class TestArithmeticFunctions(QueryBase):

    # You need some transactions in order to eval a simple arithmetic op.
    # This also properly sets the data type to Decimal.
    # In v2, revise this so that this works like a regular DB and support integers.

    def test_add(self):
        self.check_query(
            """
              2010-02-23 *
                Assets:Something       5.00 USD
            """,
            """
              SELECT number + 3 as result;
            """,
            [('result', Decimal)],
            [(D("8"),)])

    def test_sub(self):
        self.check_query(
            """
              2010-02-23 *
                Assets:Something       5.00 USD
            """,
            """
              SELECT number - 3 as result;
            """,
            [('result', Decimal)],
            [(D("2"),)])

    def test_mul(self):
        self.check_query(
            """
              2010-02-23 *
                Assets:Something       5.00 USD
            """,
            """
              SELECT number * 1.2 as result;
            """,
            [('result', Decimal)],
            [(D("6"),)])

    def test_div(self):
        self.check_query(
            """
              2010-02-23 *
                Assets:Something       5.00 USD
            """,
            """
              SELECT number / 2 as result;
            """,
            [('result', Decimal)],
            [(D("2.50"),)])

        self.check_query(
            """
              2010-02-23 *
                Assets:Something       5.00 USD
            """,
            """
              SELECT number / 0 as result;
            """,
            [('result', Decimal)],
            [(None,)])


class TestExecutePivot(QueryBase):

    def setUp(self):
        super().setUp()
        entries, errors, options = loader.load_string(self.data, dedent=True)
        self.assertFalse(errors)
        self.ctx = beanquery.connect('beancount:', entries=entries, errors=errors, options=options)

    def execute(self, query):
        curs = self.ctx.execute(query)
        rows = curs.fetchall()
        columns = [(c.name, c.datatype) for c in curs.description]
        return columns, rows

    data = """
      2012-01-01 open Assets:Cash
      2012-01-01 open Expenses:Aaa
      2012-01-01 open Expenses:Bbb

      2012-01-01 * "Test"
        Expenses:Bbb  1.00 USD
        Assets:Cash

      2012-01-01 * "Test"
        Expenses:Aaa  2.00 USD
        Assets:Cash

      2012-02-02 * "Test"
        Expenses:Aaa  3.00 USD
        Assets:Cash

      2013-01-01 * "Test"
        Expenses:Aaa  4.00 USD
        Assets:Cash

      2014-02-02 * "Test"
        Expenses:Bbb  5.00 USD
        Assets:Cash

      2014-03-03 * "Test"
        Expenses:Aaa  6.00 USD
        Assets:Cash

      2013-01-01 * "Test"
        Expenses:Bbb  7.00 USD
        Assets:Cash

      2015-04-04 * "Test"
        Expenses:Aaa  8.00 USD
        Assets:Cash
    """

    def test_pivot_one_column(self):
        self.assertEqual(self.execute("""
            SELECT
              account,
              year(date) AS year,
              sum(cost(position)) AS balance
            WHERE
              account ~ 'Expenses'
            GROUP BY 1, 2
            PIVOT BY 1, 2"""), (
            [
                ('account/year', str),
                ('2012', inventory.Inventory),
                ('2013', inventory.Inventory),
                ('2014', inventory.Inventory),
                ('2015', inventory.Inventory),
            ],
            [
                ('Expenses:Aaa', I('5.00 USD'), I('4.00 USD'), I('6.00 USD'), I('8.00 USD')),
                ('Expenses:Bbb', I('1.00 USD'), I('7.00 USD'), I('5.00 USD'), None),
            ]))

    def test_pivot_one_column_by_name(self):
        self.assertEqual(self.execute("""
            SELECT
              account,
              year(date) AS year,
              sum(cost(position)) AS balance
            WHERE
              account ~ 'Expenses'
            GROUP BY 1, 2
            PIVOT BY account, year"""), (
            [
                ('account/year', str),
                ('2012', inventory.Inventory),
                ('2013', inventory.Inventory),
                ('2014', inventory.Inventory),
                ('2015', inventory.Inventory),
            ],
            [
                ('Expenses:Aaa', I('5.00 USD'), I('4.00 USD'), I('6.00 USD'), I('8.00 USD')),
                ('Expenses:Bbb', I('1.00 USD'), I('7.00 USD'), I('5.00 USD'), None),
            ]))

    def test_pivot_two_column(self):
        self.assertEqual(self.execute("""
            SELECT
              account,
              year(date) AS year,
              sum(cost(position)) AS balance,
              last(date) AS updated
            WHERE
              account ~ 'Expenses' AND
              year >= 2014
            GROUP BY 1, 2
            PIVOT BY 1, 2"""), (
            [
                ('account/year', str),
                ('2014/balance', inventory.Inventory),
                ('2014/updated', datetime.date),
                ('2015/balance', inventory.Inventory),
                ('2015/updated', datetime.date),
            ],
            [
                ('Expenses:Aaa', I('6.00 USD'), datetime.date(2014, 3, 3), I('8.00 USD'), datetime.date(2015, 4, 4)),
                ('Expenses:Bbb', I('5.00 USD'), datetime.date(2014, 2, 2), None, None),
            ]))


class TestExecuteSubquery(QueryBase):

    def execute(self, query):
        curs = self.ctx.execute(query)
        rows = curs.fetchall()
        columns = [(c.name, c.datatype) for c in curs.description]
        return columns, rows

    def test_subquery(self):
        self.assertEqual(
            self.execute("""SELECT a + 2 AS b FROM (SELECT 3 AS a FROM #)"""),
            ([('b', int)], [(5, )]))


class SimpleColumn(qc.EvalColumn):
    def __init__(self, name, func, dtype):
        super().__init__(dtype)
        self.name = name
        self.func = func

    def __call__(self, row):
        return self.func(row)


class SimpleTable(tables.Table):
    columns = {
        'column': SimpleColumn('column', lambda row: row, int)
    }

    def __init__(self, name, nrows):
        self.name = name
        self.nrows = nrows

    def __iter__(self):
        yield from range(self.nrows)


class TestExecuteTables(QueryBase):

    def setUp(self):
        super().setUp()
        self.ctx.tables['test'] = SimpleTable('test', 16)

    def execute(self, query):
        query = self.compile(query)
        return qx.execute_query(query)

    def test_null_table(self):
        self.assertEqual(
            self.execute("""SELECT 1 + 1 AS test FROM #"""),
            ((('test', int), ), [(2, )]))

    def test_simple_table(self):
        self.assertEqual(
            self.execute("""SELECT column FROM #test WHERE column < 2"""),
            ((('column', int), ), [(0, ), (1, )]))

    def test_simple_table_aggregation(self):
        self.assertEqual(
            self.execute("""SELECT sum(column) FROM #test GROUP BY column % 2"""),
            ((('sum(column)', int), ), [(56, ), (64, )]))


class TestInSubquery(QueryBase):
    data = """
      2000-01-01 open Income:Contributions:One
      2000-01-01 open Income:Contributions:Two
      2000-01-01 open Income:Contributions:Three
      2000-01-01 open Assets:Cash

      2024-10-25 * "One"
        Income:Contributions:One                            100.00 USD
        Assets:Cash

      2024-10-26 * "Two"
        Income:Contributions:Two                             10.00 USD
        Assets:Cash

      2024-10-27 * "Three"
        Income:Contributions:Three                           50.00 USD
        Assets:Cash

      2024-10-28 * "Three"
        Income:Contributions:Three                            1.00 USD
        Assets:Cash
    """

    def test_in_subquery(self):
        self.check_query(self.data, """
            SELECT
              account,
              date
            FROM #postings
            WHERE root(account, 2) = 'Income:Contributions'
            AND account IN (
              SELECT account
              FROM #postings
              GROUP BY account
              HAVING number(only('USD', sum(position))) > 50.0
            )
            ORDER BY date
            """,
            (('account', str), ('date', datetime.date)),
            [
            ('Income:Contributions:One',   datetime.date(2024, 10, 25)),
            ('Income:Contributions:Three', datetime.date(2024, 10, 27)),
            ('Income:Contributions:Three', datetime.date(2024, 10, 28)),
            ]
        )
