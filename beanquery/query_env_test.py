__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import unittest
from decimal import Decimal

from beancount.core.number import D
from beancount.parser import parser
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


if __name__ == '__main__':
    unittest.main()
