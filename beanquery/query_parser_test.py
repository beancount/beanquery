__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import unittest

from decimal import Decimal as D
from beanquery import query_parser as qp


def Select(targets, from_clause=None, where_clause=None, **kwargs):
    defaults = dict(targets=targets,
                    from_clause=from_clause,
                    where_clause=where_clause,
                    group_by=None,
                    order_by=None,
                    pivot_by=None,
                    limit=None,
                    distinct=None)
    defaults.update(kwargs)
    return qp.Select(**defaults)


class QueryParserTestBase(unittest.TestCase):

    def setUp(self):
        self.parser = qp.Parser()

    def parse(self, query):
        return self.parser.parse(query.strip())

    def assertParse(self, query, expected):
        self.assertEqual(self.parse(query), expected)

    def assertParseTarget(self, query, expected):
        expr = self.parse(query)
        self.assertIsInstance(expr, qp.Select)
        self.assertEqual(len(expr.targets), 1)
        self.assertEqual(expr.targets[0].expression, expected)

    def assertParseFrom(self, query, expected):
        expr = self.parse(query)
        self.assertIsInstance(expr, qp.Select)
        self.assertEqual(expr.from_clause, expected)


class TestParseSelect(QueryParserTestBase):

    def test_select(self):
        with self.assertRaises(qp.ParseError):
            self.parse("SELECT")

        with self.assertRaises(qp.ParseError):
            self.parse("SELECT ; ")

        self.assertParse(
            "SELECT *;",
            Select(qp.Wildcard()))

        self.assertParse(
            "SELECT date;",
            Select([
                qp.Target(qp.Column('date'), None)
            ]))

        self.assertParse(
            "SELECT date, account",
            Select([
                qp.Target(qp.Column('date'), None),
                qp.Target(qp.Column('account'), None)
            ]))

        self.assertParse(
            "SELECT date as xdate;",
            Select([
                qp.Target(qp.Column('date'), 'xdate')
            ]))

        self.assertParse(
            "SELECT date as x, account, position as y;",
            Select([
                qp.Target(qp.Column('date'), 'x'),
                qp.Target(qp.Column('account'), None),
                qp.Target(qp.Column('position'), 'y')
            ]))

    def test_literals(self):
        # null
        self.assertParseTarget("SELECT NULL;", qp.Constant(None))

        # bool
        self.assertParseTarget("SELECT TRUE;", qp.Constant(True))
        self.assertParseTarget("SELECT FALSE;", qp.Constant(False))

        # int
        self.assertParseTarget("SELECT 17;", qp.Constant(17))

        # decimal
        self.assertParseTarget("SELECT 17.345;", qp.Constant(D('17.345')))
        self.assertParseTarget("SELECT .345;", qp.Constant(D('.345')))
        self.assertParseTarget("SELECT 17.;", qp.Constant(D('17.')))

        # string
        self.assertParseTarget("SELECT 'rainy-day';", qp.Constant('rainy-day'))

        # date
        self.assertParseTarget("SELECT 1972-05-28;", qp.Constant(datetime.date(1972, 5, 28)))
        self.assertParseTarget("SELECT #'May 28, 1972';", qp.Constant(datetime.date(1972, 5, 28)))

        # not a list
        self.assertParseTarget("SELECT (1);", qp.Constant(1))

        # list
        self.assertParseTarget("SELECT (1, );", qp.Constant([1]))
        self.assertParseTarget("SELECT (1, 2);", qp.Constant([1, 2]))
        self.assertParseTarget("SELECT (1, 2, );", qp.Constant([1, 2]))
        self.assertParseTarget("SELECT ('x', 'y', 'z');", qp.Constant(['x', 'y', 'z']))

        # column
        self.assertParseTarget("SELECT date;", qp.Column('date'))

    def test_expressions(self):
        # comparison operators
        self.assertParseTarget("SELECT a = 42;", qp.Equal(qp.Column('a'), qp.Constant(42)))
        self.assertParseTarget("SELECT a != 42;", qp.NotEqual(qp.Column('a'), qp.Constant(42)))
        self.assertParseTarget("SELECT a > 42;", qp.Greater(qp.Column('a'), qp.Constant(42)))
        self.assertParseTarget("SELECT a >= 42;", qp.GreaterEq(qp.Column('a'), qp.Constant(42)))
        self.assertParseTarget("SELECT a < 42;", qp.Less(qp.Column('a'), qp.Constant(42)))
        self.assertParseTarget("SELECT a <= 42;", qp.LessEq(qp.Column('a'), qp.Constant(42)))
        self.assertParseTarget("SELECT a ~ 'abc';", qp.Match(qp.Column('a'), qp.Constant('abc')))
        self.assertParseTarget("SELECT not a;", qp.Not(qp.Column('a')))
        self.assertParseTarget("SELECT a IS NULL;", qp.IsNull(qp.Column('a')))
        self.assertParseTarget("SELECT a IS NOT NULL;", qp.IsNotNull(qp.Column('a')))

        # bool expressions
        self.assertParseTarget("SELECT a AND b;", qp.And(qp.Column('a'), qp.Column('b')))
        self.assertParseTarget("SELECT a OR b;", qp.Or(qp.Column('a'), qp.Column('b')))
        self.assertParseTarget("SELECT NOT a;", qp.Not(qp.Column('a')))

        # math expressions with identifiers
        self.assertParseTarget("SELECT a * b;", qp.Mul(qp.Column('a'), qp.Column('b')))
        self.assertParseTarget("SELECT a / b;", qp.Div(qp.Column('a'), qp.Column('b')))
        self.assertParseTarget("SELECT a + b;", qp.Add(qp.Column('a'), qp.Column('b')))
        self.assertParseTarget("SELECT a+b;", qp.Add(qp.Column('a'), qp.Column('b')))
        self.assertParseTarget("SELECT a - b;", qp.Sub(qp.Column('a'), qp.Column('b')))
        self.assertParseTarget("SELECT a-b;", qp.Sub(qp.Column('a'), qp.Column('b')))
        self.assertParseTarget("SELECT +a;", qp.Column('a'))
        self.assertParseTarget("SELECT -a;", qp.Neg(qp.Column('a')))

        # math expressions with numerals
        self.assertParseTarget("SELECT 2 * 3;", qp.Mul(qp.Constant(2), qp.Constant(3)))
        self.assertParseTarget("SELECT 2 / 3;", qp.Div(qp.Constant(2), qp.Constant(3)))
        self.assertParseTarget("SELECT 2+(3);", qp.Add(qp.Constant(2), qp.Constant(3)))
        self.assertParseTarget("SELECT (2)-3;", qp.Sub(qp.Constant(2), qp.Constant(3)))
        self.assertParseTarget("SELECT 2 + 3;", qp.Add(qp.Constant(2), qp.Constant(3)))
        self.assertParseTarget("SELECT 2+3;", qp.Add(qp.Constant(2), qp.Constant(3)))
        self.assertParseTarget("SELECT 2 - 3;", qp.Sub(qp.Constant(2), qp.Constant(3)))
        self.assertParseTarget("SELECT 2-3;", qp.Sub(qp.Constant(2), qp.Constant(3)))
        self.assertParseTarget("SELECT +2;", qp.Constant(2))
        self.assertParseTarget("SELECT -2;", qp.Neg(qp.Constant(2)))
        # silly, fails at compile time
        self.assertParseTarget("SELECT -'abc';", qp.Neg(qp.Constant('abc')))

        # functions
        self.assertParseTarget("SELECT random();", qp.Function('random', []))
        self.assertParseTarget("SELECT min(a);", qp.Function('min', [qp.Column('a')]))
        self.assertParseTarget("SELECT min(a, b);", qp.Function('min', [qp.Column('a'), qp.Column('b')]))

    def test_non_associative(self):
        # non associative operators
        self.assertRaises(qp.ParseError, self.parse, "SELECT 3 > 2 > 1")
        self.assertRaises(qp.ParseError, self.parse, "SELECT 3 = 2 = 1")

    def test_complex_expressions(self):
        self.assertParseTarget(
            "SELECT NOT a = (b != (42 AND 17));",
            qp.Not(
                qp.Equal(
                    qp.Column('a'),
                    qp.NotEqual(
                        qp.Column('b'),
                        qp.And(
                            qp.Constant(42),
                            qp.Constant(17))))))


class TestSelectPrecedence(QueryParserTestBase):

    def test_operators_precedence(self):

        self.assertParseTarget(
            "SELECT a AND b OR c AND d;",
            qp.Or(qp.And(qp.Column('a'), qp.Column('b')),
                  qp.And(qp.Column('c'), qp.Column('d'))))

        self.assertParseTarget(
            "SELECT a = 2 AND b != 3;",
            qp.And(qp.Equal(qp.Column('a'), qp.Constant(2)),
                   qp.NotEqual(qp.Column('b'), qp.Constant(3))))

        self.assertParseTarget(
            "SELECT not a AND b;",
            qp.And(qp.Not(qp.Column('a')), qp.Column('b')))

        self.assertParseTarget(
            "SELECT a + b AND c - d;",
            qp.And(qp.Add(qp.Column('a'), qp.Column('b')),
                   qp.Sub(qp.Column('c'), qp.Column('d'))))

        self.assertParseTarget(
            "SELECT a * b + c / d - 3;",
            qp.Sub(
                qp.Add(
                    qp.Mul(
                        qp.Column(name='a'),
                        qp.Column(name='b')),
                    qp.Div(qp.Column(name='c'),
                           qp.Column(name='d'))),
                qp.Constant(value=3)))

        self.assertParseTarget(
            "SELECT 'orange' IN tags AND 'bananas' IN tags;",
            qp.And(
                qp.Contains(
                    qp.Constant('orange'),
                    qp.Column('tags')),
                qp.Contains(
                    qp.Constant('bananas'),
                    qp.Column('tags'))))


class TestSelectFrom(QueryParserTestBase):

    def test_select_from(self):
        expr = qp.Equal(qp.Column('d'), qp.And(qp.Function('max', [qp.Column('e')]), qp.Constant(17)))

        with self.assertRaises(qp.ParseError):
            self.parse("SELECT a, b FROM;")

        # simple
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17);",
            qp.From(expr, None, None, None))

        # open dated
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) OPEN ON 2014-01-01;",
            qp.From(expr, datetime.date(2014, 1, 1), None, None))

        # close default
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) CLOSE;",
            qp.From(expr, None, True, None))

        # close dated
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) CLOSE ON 2014-10-18;",
            qp.From(expr, None, datetime.date(2014, 10, 18), None))

        # close no expression
        self.assertParseFrom(
            "SELECT a, b FROM CLOSE;",
            qp.From(None, None, True, None))

        # close no expression dated
        self.assertParseFrom(
            "SELECT a, b FROM CLOSE ON 2014-10-18;",
            qp.From(None, None, datetime.date(2014, 10, 18), None))

        # clear default
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) CLEAR;",
            qp.From(expr, None, None, True))

        # open close clear
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) OPEN ON 2013-10-25 CLOSE ON 2014-10-25 CLEAR;",
            qp.From(expr, datetime.date(2013, 10, 25), datetime.date(2014, 10, 25), True))


class TestSelectWhere(QueryParserTestBase):

    def test_where(self):
        expr = qp.Equal(qp.Column('d'), qp.And(qp.Function('max', [qp.Column('e')]), qp.Constant(17)))
        self.assertParse(
            "SELECT a, b WHERE d = (max(e) and 17);",
            Select([
                qp.Target(qp.Column('a'), None),
                qp.Target(qp.Column('b'), None)
            ], None, expr))

        with self.assertRaises(qp.ParseError):
            self.parse("SELECT a, b WHERE;")


class TestSelectFromAndWhere(QueryParserTestBase):

    def test_from_and_where(self):
        expr = qp.Equal(qp.Column('d'), qp.And(qp.Function('max', [qp.Column('e')]), qp.Constant(17)))
        self.assertParse(
            "SELECT a, b FROM d = (max(e) and 17) WHERE d = (max(e) and 17);",
            Select([
                qp.Target(qp.Column('a'), None),
                qp.Target(qp.Column('b'), None)
            ], qp.From(expr, None, None, None), expr))


class TestSelectFromSelect(QueryParserTestBase):

    def test_from_select(self):
        self.assertParse("""
            SELECT a, b FROM (
              SELECT * FROM date = 2014-05-02
            ) WHERE c = 5 LIMIT 100;""",
            Select([
                qp.Target(qp.Column('a'), None),
                qp.Target(qp.Column('b'), None)],
            Select(
                qp.Wildcard(),
                qp.From(
                    qp.Equal(
                        qp.Column('date'),
                        qp.Constant(datetime.date(2014, 5, 2))),
                    None, None, None)),
            qp.Equal(qp.Column('c'), qp.Constant(5)),
            limit=100))


class TestSelectGroupBy(QueryParserTestBase):

    def test_groupby_one(self):
        self.assertParse(
            "SELECT * GROUP BY a;",
            Select(qp.Wildcard(),
                   group_by=qp.GroupBy([qp.Column('a')], None)))

    def test_groupby_many(self):
        self.assertParse(
            "SELECT * GROUP BY a, b, c;",
            Select(qp.Wildcard(),
                   group_by=qp.GroupBy([
                       qp.Column('a'),
                       qp.Column('b'),
                       qp.Column('c')], None)))

    def test_groupby_expr(self):
        self.assertParse(
            "SELECT * GROUP BY length(a) > 0, b;",
            Select(qp.Wildcard(),
                   group_by=qp.GroupBy([
                       qp.Greater(
                           qp.Function('length', [
                               qp.Column('a')]),
                           qp.Constant(0)),
                       qp.Column('b')], None)))

    def test_groupby_having(self):
        self.assertParse(
            "SELECT * GROUP BY a HAVING sum(x) = 0;",
            Select(qp.Wildcard(),
                   group_by=qp.GroupBy([qp.Column('a')],
                        qp.Equal(
                            qp.Function('sum', [
                                qp.Column('x')]),
                            qp.Constant(0)))))

    def test_groupby_numbers(self):
        self.assertParse(
            "SELECT * GROUP BY 1;",
            Select(qp.Wildcard(),
                   group_by=qp.GroupBy([1], None)))

        self.assertParse(
            "SELECT * GROUP BY 2, 4, 5;",
            Select(qp.Wildcard(),
                   group_by=qp.GroupBy([2, 4, 5], None)))

    def test_groupby_empty(self):
        with self.assertRaises(qp.ParseError):
            self.parse("SELECT * GROUP BY;")


class TestSelectOrderBy(QueryParserTestBase):

    def test_orderby_one(self):
        self.assertParse(
            "SELECT * ORDER BY a;",
            Select(qp.Wildcard(),
                   order_by=[
                       qp.OrderBy(qp.Column('a'), qp.Ordering.ASC)]))

    def test_orderby_many(self):
        self.assertParse(
            "SELECT * ORDER BY a, b, c;",
            Select(qp.Wildcard(),
                   order_by=[
                       qp.OrderBy(qp.Column('a'), qp.Ordering.ASC),
                       qp.OrderBy(qp.Column('b'), qp.Ordering.ASC),
                       qp.OrderBy(qp.Column('c'), qp.Ordering.ASC)]))

    def test_orderby_asc(self):
        self.assertParse(
            "SELECT * ORDER BY a ASC;",
            Select(qp.Wildcard(),
                   order_by=[
                       qp.OrderBy(qp.Column('a'), qp.Ordering.ASC)]))

    def test_orderby_desc(self):
        self.assertParse(
            "SELECT * ORDER BY a DESC;",
            Select(qp.Wildcard(),
                   order_by=[
                       qp.OrderBy(qp.Column('a'), qp.Ordering.DESC)]))

    def test_orderby_many_asc_desc(self):
        self.assertParse(
            "SELECT * ORDER BY a ASC, b DESC, c;",
            Select(qp.Wildcard(),
                   order_by=[
                       qp.OrderBy(qp.Column('a'), qp.Ordering.ASC),
                       qp.OrderBy(qp.Column('b'), qp.Ordering.DESC),
                       qp.OrderBy(qp.Column('c'), qp.Ordering.ASC)]))

    def test_orderby_empty(self):
        with self.assertRaises(qp.ParseError):
            self.parse("SELECT * ORDER BY;")


class TestSelectPivotBy(QueryParserTestBase):

    def test_pivotby(self):
        with self.assertRaises(qp.ParseError):
            self.parse("SELECT * PIVOT BY;")

        with self.assertRaises(qp.ParseError):
            self.parse("SELECT * PIVOT BY a;")

        with self.assertRaises(qp.ParseError):
            self.parse("SELECT * PIVOT BY a, b, c")

        self.assertParse(
            "SELECT * PIVOT BY a, b",
            Select(qp.Wildcard(), pivot_by=qp.PivotBy([qp.Column('a'), qp.Column('b')])))

        self.assertParse(
            "SELECT * PIVOT BY 1, 2",
            Select(qp.Wildcard(), pivot_by=qp.PivotBy([1, 2])))


class TestSelectOptions(QueryParserTestBase):

    def test_distinct(self):
        self.assertParse(
            "SELECT DISTINCT x;", Select([qp.Target(qp.Column('x'), None)], distinct=True))

    def test_limit_present(self):
        self.assertParse(
            "SELECT * LIMIT 45;", Select(qp.Wildcard(), limit=45))

    def test_limit_empty(self):
        with self.assertRaises(qp.ParseError):
            self.parse("SELECT * LIMIT;")


class TestBalances(QueryParserTestBase):

    def test_balances_empty(self):
        self.assertParse(
            "BALANCES;", qp.Balances(None, None, None))

    def test_balances_from(self):
        self.assertParse(
            "BALANCES FROM date = 2014-01-01 CLOSE;",
            qp.Balances(
                None,
                qp.From(
                    qp.Equal(
                        qp.Column('date'),
                        qp.Constant(datetime.date(2014, 1, 1))),
                    None, True, None),
                None))

    def test_balances_from_with_transformer(self):
        self.assertParse(
            "BALANCES AT units FROM date = 2014-01-01 CLOSE;",
            qp.Balances('units',
                qp.From(
                    qp.Equal(
                        qp.Column('date'),
                        qp.Constant(datetime.date(2014, 1, 1))),
                    None, True, None),
                None))

    def test_balances_from_with_transformer_simple(self):
        self.assertParse(
            "BALANCES AT units WHERE date = 2014-01-01;",
            qp.Balances('units',
                None,
                qp.Equal(
                    qp.Column('date'),
                    qp.Constant(datetime.date(2014, 1, 1)))))


class TestJournal(QueryParserTestBase):

    def test_journal_empty(self):
        self.assertParse(
            "JOURNAL;",
            qp.Journal(None, None, None))

    def test_journal_account(self):
        self.assertParse(
            "JOURNAL 'Assets:Checking';",
            qp.Journal('Assets:Checking', None, None))

    def test_journal_summary(self):
        self.assertParse(
            "JOURNAL AT cost;",
            qp.Journal(None, 'cost', None))

    def test_journal_account_and_summary(self):
        self.assertParse(
            "JOURNAL 'Assets:Foo' AT cost;",
            qp.Journal('Assets:Foo', 'cost', None))

    def test_journal_from(self):
        self.assertParse(
            "JOURNAL FROM date = 2014-01-01 CLOSE;",
            qp.Journal(None, None,
                qp.From(
                    qp.Equal(
                        qp.Column('date'),
                        qp.Constant(datetime.date(2014, 1, 1))
                    ), None, True, None)))


class TestPrint(QueryParserTestBase):

    def test_print_empty(self):
        self.assertParse(
            "PRINT;", qp.Print(None))

    def test_print_from(self):
        self.assertParse(
            "PRINT FROM date = 2014-01-01 CLOSE;",
            qp.Print(
                qp.From(
                    qp.Equal(
                        qp.Column('date'),
                        qp.Constant(datetime.date(2014, 1, 1))
                    ), None, True, None)))


class TestComments(QueryParserTestBase):

    def test_comments(self):
        self.assertParse(
            """SELECT first, /* comment */ second""",
            Select([
                qp.Target(qp.Column('first'), None),
                qp.Target(qp.Column('second'), None)
            ]))

        self.assertParse(
            """SELECT first, /*
                   comment
                   */ second;""",
            Select([
                qp.Target(qp.Column('first'), None),
                qp.Target(qp.Column('second'), None),
            ]))

        self.assertParse(
            """SELECT first, /**/ second;""",
            Select([
                qp.Target(qp.Column('first'), None),
                qp.Target(qp.Column('second'), None),
            ]))

        self.assertParse(
            """SELECT first, /* /* */ second;""",
            Select([
                qp.Target(qp.Column('first'), None),
                qp.Target(qp.Column('second'), None),
            ]))

        self.assertParse(
            """SELECT first, /* ; */ second;""",
            Select([
                qp.Target(qp.Column('first'), None),
                qp.Target(qp.Column('second'), None),
            ]))


class TestExpressionName(QueryParserTestBase):

    def test_column(self):
        name = qp.get_expression_name(qp.Column('date'))
        self.assertEqual(name, 'date')

    def test_function(self):
        name = qp.get_expression_name(qp.Function('length', [qp.Column('date')]))
        self.assertEqual(name, 'length(date)')

    def test_constant(self):
        name = qp.get_expression_name(qp.Constant(17))
        self.assertEqual(name, '17')
        name = qp.get_expression_name(qp.Constant(datetime.date(2014, 1, 1)))
        self.assertEqual(name, '2014-01-01')
        name = qp.get_expression_name(qp.Constant('abc'))
        self.assertEqual(name, "'abc'")

    def test_unary(self):
        name = qp.get_expression_name(qp.Not(qp.Column('account')))
        self.assertEqual(name, 'not(account)')

    def test_binary(self):
        name = qp.get_expression_name(qp.And(qp.Column('a'), qp.Column('b')))
        self.assertEqual(name, 'and(a, b)')

    def test_unknown(self):
        with self.assertRaises(RuntimeError):
            name = qp.get_expression_name(None)


class TestRepr(unittest.TestCase):
    # 100% branch test coverage is hard...

    def test_ordering(self):
        self.assertEqual(repr(qp.Ordering.ASC), 'Ordering.ASC')
        self.assertEqual(repr(qp.Ordering.DESC), 'Ordering.DESC')

    def test_ast_node(self):
        self.assertEqual(repr(qp.Constant(1)), 'Constant(value=1)')
        self.assertEqual(repr(qp.Not(qp.Constant(False))), 'Not(operand=Constant(value=False))')
