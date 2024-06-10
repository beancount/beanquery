__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import unittest
from decimal import Decimal

from beancount.core.number import D
from beancount.parser import parser
from beancount import loader
from beanquery import query_compile as qc
from beanquery import query_env as qe
from beanquery import query


class TestCompileDataTypes(unittest.TestCase):

    def test_compile_Length(self):
        c_length = qe.Function('length', [qc.EvalConstant('testing')])
        self.assertEqual(int, c_length.dtype)

    def test_compile_Sum(self):
        c_sum = qe.SumInt([qc.EvalConstant(17)])
        self.assertEqual(int, c_sum.dtype)
        c_sum = qe.SumDecimal([qc.EvalConstant(D('17.'))])
        self.assertEqual(Decimal, c_sum.dtype)

    def test_compile_Count(self):
        c_count = qe.Count([qc.EvalConstant(17)])
        self.assertEqual(int, c_count.dtype)

    def test_compile_First(self):
        c_first = qe.First([qc.EvalConstant(17.)])
        self.assertEqual(float, c_first.dtype)

    def test_compile_Last(self):
        c_last = qe.Last([qc.EvalConstant(17.)])
        self.assertEqual(float, c_last.dtype)


class TestEnv(unittest.TestCase):

    @parser.parse_doc()
    def test_AnyMeta(self, entries, _, options_map):
        """
        2016-11-20 *
          name: "TheName"
          address: "1 Wrong Way"
          empty: "NotEmpty"
          Assets:Banking          1 USD
            color: "Green"
            address: "1 Right Way"
            empty:
        """
        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT ANY_META("name") as m')
        self.assertEqual([('TheName',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT ANY_META("color") as m')
        self.assertEqual([('Green',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT ANY_META("address") as m')
        self.assertEqual([('1 Right Way',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT ANY_META("empty") as m')
        self.assertEqual([(None,)], rrows)

    @parser.parse_doc()
    def test_GrepN(self, entries, _, options_map):
        """
        2016-11-20 * "prev match in context next"
          Assets:Banking          1 USD
        """
        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT GREPN("in", narration, 0) as m
        ''')
        self.assertEqual([('in',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT GREPN("match (.*) context", narration, 1) as m
        ''')
        self.assertEqual([('in',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT GREPN("(.*) in (.*)", narration, 2) as m
        ''')
        self.assertEqual([('context next',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT GREPN("ab(at)hing", "abathing", 1) as m
        ''')
        self.assertEqual([('at',)], rrows)

    @parser.parse_doc()
    def test_Subst(self, entries, _, options_map):
        """
        2016-11-20 * "I love candy"
          Assets:Banking       -1 USD

        2016-11-21 * "Buy thing thing"
          Assets:Cash          -1 USD
        """
        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT SUBST("[Cc]andy", "carrots", narration) as m where date = 2016-11-20
        ''')
        self.assertEqual([('I love carrots',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT SUBST("thing", "t", narration) as m where date = 2016-11-21
        ''')
        self.assertEqual([('Buy t t',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT SUBST("random", "t", narration) as m where date = 2016-11-21
        ''')
        self.assertEqual([('Buy thing thing',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT SUBST("(love)", "\\1 \\1", narration) as m where date = 2016-11-20
        ''')
        self.assertEqual([('I love love candy',)], rrows)

        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT SUBST("Assets:.*", "Savings", account) as a, str(sum(position)) as p
        ''')
        self.assertEqual([('Savings', '(-2 USD)')], rrows)

    @parser.parse_doc()
    def test_Upper(self, entries, _, options_map):
        """
        2016-11-20 * "I love candy"
          Assets:Banking       -1 USD
        """
        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT Upper(narration) as m where date = 2016-11-20
        ''')
        self.assertEqual([('I LOVE CANDY',)], rrows)

    @parser.parse_doc()
    def test_Lower(self, entries, _, options_map):
        """
        2016-11-20 * "I love candy"
          Assets:Banking       -1 USD
        """
        rtypes, rrows = query.run_query(entries, options_map, '''
          SELECT Lower(narration) as m where date = 2016-11-20
        ''')
        self.assertEqual([('i love candy',)], rrows)

    @parser.parse_doc()
    def test_Date(self, entries, _, options_map):
        """
        2016-11-20 * "ok"
          Assets:Banking          1 USD
        """
        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT date(2020, 1, 2) as m')
        self.assertEqual([(datetime.date(2020, 1, 2),)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT date(year, month, 1) as m')
        self.assertEqual([(datetime.date(2016, 11, 1),)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT date(2020, 2, 32) as m')
        self.assertEqual([(None,)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT date("2020-01-02") as m')
        self.assertEqual([(datetime.date(2020, 1, 2),)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT parse_date("2016/11/1") as m')
        self.assertEqual([(datetime.date(2016, 11, 1),)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT parse_date("2016/11/1", "%Y/%d/%m") as m')
        self.assertEqual([(datetime.date(2016, 1, 11),)], rrows)

    @parser.parse_doc()
    def test_DateDiffAdjust(self, entries, _, options_map):
        """
        2016-11-20 * "ok"
          Assets:Banking          -1 STOCK { 5 USD, 2016-10-30 }
        """
        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT date_diff(date, cost_date) as m')
        self.assertEqual([(21,)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT date_diff(cost_date, date) as m')
        self.assertEqual([(-21,)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT date_add(date, 1) as m')
        self.assertEqual([(datetime.date(2016, 11, 21),)], rrows)

        rtypes, rrows = query.run_query(entries, options_map,
                                        'SELECT date_add(date, -1) as m')
        self.assertEqual([(datetime.date(2016, 11, 19),)], rrows)

    def assertResult(self, expr, result):
            columns, rows = query.run_query([], {}, f'SELECT {expr} FROM #')
            self.assertEqual(rows[0][0], result)

    def test_date_part(self):
        self.assertResult('date_part("weekday", 2024-06-09)', 6)
        self.assertResult('date_part("dow", 2024-06-09)', 6)
        self.assertResult('date_part("isoweekday", 2024-06-09)', 7)
        self.assertResult('date_part("isodow", 2024-06-09)', 7)
        self.assertResult('date_part("week", 2024-06-09)', 23)
        self.assertResult('date_part("month", 2024-06-09)', 6)
        self.assertResult('date_part("quarter", 2024-06-09)', 2)
        self.assertResult('date_part("year", 2024-06-09)', 2024)
        self.assertResult('date_part("isoyear", 2024-06-09)', 2024)
        self.assertResult('date_part("decade", 2024-06-09)', 202)
        self.assertResult('date_part("century", 2024-06-09)', 21)
        self.assertResult('date_part("millennium", 2024-06-09)', 3)
        self.assertResult('date_part("epoch", 2024-06-09)', 1717891200)
        self.assertResult('date_part("baz", 2024-06-09)', None)

    def test_date_trunc(self):
        self.assertResult('date_trunc("week", 2016-11-14)', datetime.date(2016, 11, 14)) # monday
        self.assertResult('date_trunc("week", 2016-11-15)', datetime.date(2016, 11, 14)) # tuesday
        self.assertResult('date_trunc("week", 2016-11-20)', datetime.date(2016, 11, 14)) # sunday

        self.assertResult('date_trunc("month", 2016-11-20)', datetime.date(2016, 11, 1))

        self.assertResult('date_trunc("quarter", 2016-09-30)', datetime.date(2016, 7, 1))
        self.assertResult('date_trunc("quarter", 2016-10-01)', datetime.date(2016, 10, 1))
        self.assertResult('date_trunc("quarter", 2016-11-20)', datetime.date(2016, 10, 1))

        self.assertResult('date_trunc("year", 2016-11-20)', datetime.date(2016, 1, 1))

        self.assertResult('date_trunc("decade", 2016-11-20)', datetime.date(2010, 1, 1))
        self.assertResult('date_trunc("decade", 2020-11-20)', datetime.date(2020, 1, 1))
        self.assertResult('date_trunc("decade", 2029-11-20)', datetime.date(2020, 1, 1))

        self.assertResult('date_trunc("century", 1999-11-20)', datetime.date(1901, 1, 1))
        self.assertResult('date_trunc("century", 2000-11-20)', datetime.date(1901, 1, 1))
        self.assertResult('date_trunc("century", 2001-11-20)', datetime.date(2001, 1, 1))
        self.assertResult('date_trunc("century", 2016-11-20)', datetime.date(2001, 1, 1))

        self.assertResult('date_trunc("millennium", 1991-11-20)', datetime.date(1001, 1, 1))
        self.assertResult('date_trunc("millennium", 2000-11-20)', datetime.date(1001, 1, 1))
        self.assertResult('date_trunc("millennium", 2001-11-20)', datetime.date(2001, 1, 1))
        self.assertResult('date_trunc("millennium", 2016-11-20)', datetime.date(2001, 1, 1))
        self.assertResult('date_trunc("millennium", 3456-11-20)', datetime.date(3001, 1, 1))

        self.assertResult('date_trunc("foo", 2024-05-24)', None)

    def test_interval(self):
        self.assertResult('2016-11-20 + interval("1 months")', datetime.date(2016, 12, 20))
        self.assertResult('2016-11-01 + interval("1 months") - interval("1 days")', datetime.date(2016, 11, 30))
        self.assertResult('2024-02-01 + interval("1 month") + interval("-1 day")', datetime.date(2024, 2, 29))
        self.assertResult('2024-02-05 + interval("-1 days")', datetime.date(2024, 2, 4))
        self.assertResult('2024-02-05 + interval("1 day") + interval("2 days")', datetime.date(2024, 2, 8))
        self.assertResult('2024-05-24 + interval("1 year")', datetime.date(2025, 5, 24))
        self.assertResult('2024-05-24 + interval("-2 years")', datetime.date(2022, 5, 24))
        self.assertResult('interval("1 baz")', None)
        self.assertResult('interval("A days")', None)

    def test_root(self):
        self.assertResult('root("Assets:Foo:Bar", 2)', 'Assets:Foo')
        self.assertResult('root("Assets:Foo:Bar", 1)', 'Assets')
        self.assertResult('root("Assets:Foo:Bar")', 'Assets')

    def test_func_meta(self):
        # use the loader to have the pad transaction inserted
        entries, _, options = loader.load_string('''
          2019-01-01 open Assets:Main USD
          2019-01-01 open Assets:Other USD
          2019-01-14 * "Test"
            entry: 1
            both: 3
            Assets:Main  100.00 USD
              post: 2
              both: 3
            Assets:Other
              post: 4
          2019-01-15 pad Assets:Main Assets:Other
          2019-01-16 balance Assets:Main 1000.00 USD
        ''', dedent=True)
        rtypes, rrows = query.run_query(entries, options, '''
          SELECT
            entry_meta('post'),
            meta('post'),
            any_meta('post'),
            entry_meta('entry'),
            meta('entry'),
            any_meta('entry'),
            any_meta('both')
        ''')
        self.assertEqual([
            (None, D(2), D(2), D(1), None, D(1), D(3)),
            (None, D(4), D(4), D(1), None, D(1), D(3)),
            # postings from pad directive
            (None, None, None, None, None, None, None),
            (None, None, None, None, None, None, None),
        ], rrows)


if __name__ == '__main__':
    unittest.main()
