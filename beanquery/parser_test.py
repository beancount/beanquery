__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import textwrap
import unittest

from decimal import Decimal as D
from beanquery import parser
from beanquery.parser import ast


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
    return ast.Select(**defaults)


class QueryParserTestBase(unittest.TestCase):

    def parse(self, query):
        return parser.parse(query.strip())

    def assertParse(self, query, expected):
        self.assertEqual(parser.parse(query), expected)

    def assertParseTarget(self, query, expected):
        expr = parser.parse(query)
        self.assertIsInstance(expr, ast.Select)
        self.assertEqual(len(expr.targets), 1)
        self.assertEqual(expr.targets[0].expression, expected)

    def assertParseFrom(self, query, expected):
        expr = parser.parse(query)
        self.assertIsInstance(expr, ast.Select)
        self.assertEqual(expr.from_clause, expected)


class TestParseSelect(QueryParserTestBase):

    def test_select(self):
        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT")

        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT ; ")

        self.assertParse(
            "SELECT *;",
            Select(ast.Asterisk()))

        self.assertParse(
            "SELECT date;",
            Select([
                ast.Target(ast.Column('date'), None)
            ]))

        self.assertParse(
            "SELECT date, account",
            Select([
                ast.Target(ast.Column('date'), None),
                ast.Target(ast.Column('account'), None)
            ]))

        self.assertParse(
            "SELECT date as xdate;",
            Select([
                ast.Target(ast.Column('date'), 'xdate')
            ]))

        self.assertParse(
            "SELECT date as x, account, position as y;",
            Select([
                ast.Target(ast.Column('date'), 'x'),
                ast.Target(ast.Column('account'), None),
                ast.Target(ast.Column('position'), 'y')
            ]))

    def test_quoted_identifier(self):
        self.assertParse(
            """SELECT 1 AS "foo";""",
            Select([
                ast.Target(ast.Constant(1), 'foo'),
            ]))

        self.assertParse(
            """SELECT 1 AS "foo bar";""",
            Select([
                ast.Target(ast.Constant(1), 'foo bar'),
            ]))

        self.assertParse(
            """SELECT 2 AS "1 + 1";""",
            Select([
                ast.Target(ast.Constant(2), '1 + 1'),
            ]))

    def test_literals(self):
        # null
        self.assertParseTarget("SELECT NULL;", ast.Constant(None))

        # bool
        self.assertParseTarget("SELECT TRUE;", ast.Constant(True))
        self.assertParseTarget("SELECT FALSE;", ast.Constant(False))

        # int
        self.assertParseTarget("SELECT 17;", ast.Constant(17))

        # decimal
        self.assertParseTarget("SELECT 17.345;", ast.Constant(D('17.345')))
        self.assertParseTarget("SELECT .345;", ast.Constant(D('.345')))
        self.assertParseTarget("SELECT 17.;", ast.Constant(D('17.')))

        # string
        self.assertParseTarget("SELECT 'rainy-day';", ast.Constant('rainy-day'))

        # date
        self.assertParseTarget("SELECT 1972-05-28;", ast.Constant(datetime.date(1972, 5, 28)))

        # not a list
        self.assertParseTarget("SELECT (1);", ast.Constant(1))

        # list
        self.assertParseTarget("SELECT (1, );", ast.Constant([1]))
        self.assertParseTarget("SELECT (1, 2);", ast.Constant([1, 2]))
        self.assertParseTarget("SELECT (1, 2, );", ast.Constant([1, 2]))
        self.assertParseTarget("SELECT ('x', 'y', 'z');", ast.Constant(['x', 'y', 'z']))

        # column
        self.assertParseTarget("SELECT date;", ast.Column('date'))

    def test_expressions(self):
        # comparison operators
        self.assertParseTarget("SELECT a = 42;", ast.Equal(ast.Column('a'), ast.Constant(42)))
        self.assertParseTarget("SELECT a != 42;", ast.NotEqual(ast.Column('a'), ast.Constant(42)))
        self.assertParseTarget("SELECT a > 42;", ast.Greater(ast.Column('a'), ast.Constant(42)))
        self.assertParseTarget("SELECT a >= 42;", ast.GreaterEq(ast.Column('a'), ast.Constant(42)))
        self.assertParseTarget("SELECT a < 42;", ast.Less(ast.Column('a'), ast.Constant(42)))
        self.assertParseTarget("SELECT a <= 42;", ast.LessEq(ast.Column('a'), ast.Constant(42)))
        self.assertParseTarget("SELECT a ~ 'abc';", ast.Match(ast.Column('a'), ast.Constant('abc')))
        self.assertParseTarget("SELECT not a;", ast.Not(ast.Column('a')))
        self.assertParseTarget("SELECT a IS NULL;", ast.IsNull(ast.Column('a')))
        self.assertParseTarget("SELECT a IS NOT NULL;", ast.IsNotNull(ast.Column('a')))

        # bool expressions
        self.assertParseTarget("SELECT a AND b;", ast.And([ast.Column('a'), ast.Column('b')]))
        self.assertParseTarget("SELECT a AND b AND c;", ast.And([ast.Column('a'), ast.Column('b'), ast.Column('c')]))
        self.assertParseTarget("SELECT a OR b;", ast.Or([ast.Column('a'), ast.Column('b')]))
        self.assertParseTarget("SELECT a OR b OR c;", ast.Or([ast.Column('a'), ast.Column('b'), ast.Column('c')]))
        self.assertParseTarget("SELECT a AND b OR c;", ast.Or([ast.And([ast.Column('a'), ast.Column('b')]), ast.Column('c')]))
        self.assertParseTarget("SELECT NOT a;", ast.Not(ast.Column('a')))

        # math expressions with identifiers
        self.assertParseTarget("SELECT a * b;", ast.Mul(ast.Column('a'), ast.Column('b')))
        self.assertParseTarget("SELECT a / b;", ast.Div(ast.Column('a'), ast.Column('b')))
        self.assertParseTarget("SELECT a + b;", ast.Add(ast.Column('a'), ast.Column('b')))
        self.assertParseTarget("SELECT a+b;", ast.Add(ast.Column('a'), ast.Column('b')))
        self.assertParseTarget("SELECT a - b;", ast.Sub(ast.Column('a'), ast.Column('b')))
        self.assertParseTarget("SELECT a-b;", ast.Sub(ast.Column('a'), ast.Column('b')))
        self.assertParseTarget("SELECT +a;", ast.Column('a'))
        self.assertParseTarget("SELECT -a;", ast.Neg(ast.Column('a')))

        # math expressions with numerals
        self.assertParseTarget("SELECT 2 * 3;", ast.Mul(ast.Constant(2), ast.Constant(3)))
        self.assertParseTarget("SELECT 2 / 3;", ast.Div(ast.Constant(2), ast.Constant(3)))
        self.assertParseTarget("SELECT 2+(3);", ast.Add(ast.Constant(2), ast.Constant(3)))
        self.assertParseTarget("SELECT (2)-3;", ast.Sub(ast.Constant(2), ast.Constant(3)))
        self.assertParseTarget("SELECT 2 + 3;", ast.Add(ast.Constant(2), ast.Constant(3)))
        self.assertParseTarget("SELECT 2+3;", ast.Add(ast.Constant(2), ast.Constant(3)))
        self.assertParseTarget("SELECT 2 - 3;", ast.Sub(ast.Constant(2), ast.Constant(3)))
        self.assertParseTarget("SELECT 2-3;", ast.Sub(ast.Constant(2), ast.Constant(3)))
        self.assertParseTarget("SELECT +2;", ast.Constant(2))
        self.assertParseTarget("SELECT -2;", ast.Neg(ast.Constant(2)))
        # silly, fails at compile time
        self.assertParseTarget("SELECT -'abc';", ast.Neg(ast.Constant('abc')))

        # functions
        self.assertParseTarget("SELECT random();", ast.Function('random', []))
        self.assertParseTarget("SELECT min(a);", ast.Function('min', [ast.Column('a')]))
        self.assertParseTarget("SELECT min(a, b);", ast.Function('min', [ast.Column('a'), ast.Column('b')]))
        self.assertParseTarget("SELECT count(*);", ast.Function('count', [ast.Asterisk()]))

    def test_non_associative(self):
        # non associative operators
        self.assertRaises(parser.ParseError, self.parse, "SELECT 3 > 2 > 1")
        self.assertRaises(parser.ParseError, self.parse, "SELECT 3 = 2 = 1")

    def test_complex_expressions(self):
        self.assertParseTarget(
            "SELECT NOT a = (b != (42 AND 17));",
            ast.Not(
                ast.Equal(
                    ast.Column('a'),
                    ast.NotEqual(
                        ast.Column('b'),
                        ast.And([
                            ast.Constant(42),
                            ast.Constant(17)])))))


class TestSelectPrecedence(QueryParserTestBase):

    def test_operators_precedence(self):

        self.assertParseTarget(
            "SELECT a AND b OR c AND d;",
            ast.Or([ast.And([ast.Column('a'), ast.Column('b')]),
                   ast.And([ast.Column('c'), ast.Column('d')])]))

        self.assertParseTarget(
            "SELECT a = 2 AND b != 3;",
            ast.And([ast.Equal(ast.Column('a'), ast.Constant(2)),
                    ast.NotEqual(ast.Column('b'), ast.Constant(3))]))

        self.assertParseTarget(
            "SELECT not a AND b;",
            ast.And([ast.Not(ast.Column('a')), ast.Column('b')]))

        self.assertParseTarget(
            "SELECT a + b AND c - d;",
            ast.And([ast.Add(ast.Column('a'), ast.Column('b')),
                    ast.Sub(ast.Column('c'), ast.Column('d'))]))

        self.assertParseTarget(
            "SELECT a * b + c / d - 3;",
            ast.Sub(
                ast.Add(
                    ast.Mul(
                        ast.Column(name='a'),
                        ast.Column(name='b')),
                    ast.Div(ast.Column(name='c'),
                           ast.Column(name='d'))),
                ast.Constant(value=3)))

        self.assertParseTarget(
            "SELECT 'orange' IN tags AND 'bananas' IN tags;",
            ast.And([
                ast.In(ast.Constant('orange'), ast.Column('tags')),
                ast.In(ast.Constant('bananas'), ast.Column('tags'))]))


class TestSelectFrom(QueryParserTestBase):

    def test_select_from(self):
        expr = ast.Equal(ast.Column('d'), ast.And([ast.Function('max', [ast.Column('e')]), ast.Constant(17)]))

        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT a, b FROM;")

        # simple
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17);",
            ast.From(expr, None, None, None))

        # open dated
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) OPEN ON 2014-01-01;",
            ast.From(expr, datetime.date(2014, 1, 1), None, None))

        # close default
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) CLOSE;",
            ast.From(expr, None, True, None))

        # close dated
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) CLOSE ON 2014-10-18;",
            ast.From(expr, None, datetime.date(2014, 10, 18), None))

        # close no expression
        self.assertParseFrom(
            "SELECT a, b FROM CLOSE;",
            ast.From(None, None, True, None))

        # close no expression dated
        self.assertParseFrom(
            "SELECT a, b FROM CLOSE ON 2014-10-18;",
            ast.From(None, None, datetime.date(2014, 10, 18), None))

        # clear default
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) CLEAR;",
            ast.From(expr, None, None, True))

        # open close clear
        self.assertParseFrom(
            "SELECT a, b FROM d = (max(e) and 17) OPEN ON 2013-10-25 CLOSE ON 2014-10-25 CLEAR;",
            ast.From(expr, datetime.date(2013, 10, 25), datetime.date(2014, 10, 25), True))


class TestSelectWhere(QueryParserTestBase):

    def test_where(self):
        expr = ast.Equal(ast.Column('d'), ast.And([ast.Function('max', [ast.Column('e')]), ast.Constant(17)]))
        self.assertParse(
            "SELECT a, b WHERE d = (max(e) and 17);",
            Select([
                ast.Target(ast.Column('a'), None),
                ast.Target(ast.Column('b'), None)
            ], None, expr))

        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT a, b WHERE;")


class TestSelectFromAndWhere(QueryParserTestBase):

    def test_from_and_where(self):
        expr = ast.Equal(ast.Column('d'), ast.And([ast.Function('max', [ast.Column('e')]), ast.Constant(17)]))
        self.assertParse(
            "SELECT a, b FROM d = (max(e) and 17) WHERE d = (max(e) and 17);",
            Select([
                ast.Target(ast.Column('a'), None),
                ast.Target(ast.Column('b'), None)
            ], ast.From(expr, None, None, None), expr))


class TestSelectFromSelect(QueryParserTestBase):

    def test_from_select(self):
        self.assertParse("""
            SELECT a, b FROM (
              SELECT * FROM date = 2014-05-02
            ) WHERE c = 5 LIMIT 100;""",
            Select([
                ast.Target(ast.Column('a'), None),
                ast.Target(ast.Column('b'), None)],
            Select(
                ast.Asterisk(),
                ast.From(
                    ast.Equal(
                        ast.Column('date'),
                        ast.Constant(datetime.date(2014, 5, 2))),
                    None, None, None)),
            ast.Equal(ast.Column('c'), ast.Constant(5)),
            limit=100))


class TestSelectGroupBy(QueryParserTestBase):

    def test_groupby_one(self):
        self.assertParse(
            "SELECT * GROUP BY a;",
            Select(ast.Asterisk(),
                   group_by=ast.GroupBy([ast.Column('a')], None)))

    def test_groupby_many(self):
        self.assertParse(
            "SELECT * GROUP BY a, b, c;",
            Select(ast.Asterisk(),
                   group_by=ast.GroupBy([
                       ast.Column('a'),
                       ast.Column('b'),
                       ast.Column('c')], None)))

    def test_groupby_expr(self):
        self.assertParse(
            "SELECT * GROUP BY length(a) > 0, b;",
            Select(ast.Asterisk(),
                   group_by=ast.GroupBy([
                       ast.Greater(
                           ast.Function('length', [
                               ast.Column('a')]),
                           ast.Constant(0)),
                       ast.Column('b')], None)))

    def test_groupby_having(self):
        self.assertParse(
            "SELECT * GROUP BY a HAVING sum(x) = 0;",
            Select(ast.Asterisk(),
                   group_by=ast.GroupBy([ast.Column('a')],
                        ast.Equal(
                            ast.Function('sum', [
                                ast.Column('x')]),
                            ast.Constant(0)))))

    def test_groupby_numbers(self):
        self.assertParse(
            "SELECT * GROUP BY 1;",
            Select(ast.Asterisk(),
                   group_by=ast.GroupBy([1], None)))

        self.assertParse(
            "SELECT * GROUP BY 2, 4, 5;",
            Select(ast.Asterisk(),
                   group_by=ast.GroupBy([2, 4, 5], None)))

    def test_groupby_empty(self):
        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT * GROUP BY;")


class TestSelectOrderBy(QueryParserTestBase):

    def test_orderby_one(self):
        self.assertParse(
            "SELECT * ORDER BY a;",
            Select(ast.Asterisk(),
                   order_by=[
                       ast.OrderBy(ast.Column('a'), ast.Ordering.ASC)]))

    def test_orderby_many(self):
        self.assertParse(
            "SELECT * ORDER BY a, b, c;",
            Select(ast.Asterisk(),
                   order_by=[
                       ast.OrderBy(ast.Column('a'), ast.Ordering.ASC),
                       ast.OrderBy(ast.Column('b'), ast.Ordering.ASC),
                       ast.OrderBy(ast.Column('c'), ast.Ordering.ASC)]))

    def test_orderby_asc(self):
        self.assertParse(
            "SELECT * ORDER BY a ASC;",
            Select(ast.Asterisk(),
                   order_by=[
                       ast.OrderBy(ast.Column('a'), ast.Ordering.ASC)]))

    def test_orderby_desc(self):
        self.assertParse(
            "SELECT * ORDER BY a DESC;",
            Select(ast.Asterisk(),
                   order_by=[
                       ast.OrderBy(ast.Column('a'), ast.Ordering.DESC)]))

    def test_orderby_many_asc_desc(self):
        self.assertParse(
            "SELECT * ORDER BY a ASC, b DESC, c;",
            Select(ast.Asterisk(),
                   order_by=[
                       ast.OrderBy(ast.Column('a'), ast.Ordering.ASC),
                       ast.OrderBy(ast.Column('b'), ast.Ordering.DESC),
                       ast.OrderBy(ast.Column('c'), ast.Ordering.ASC)]))

    def test_orderby_empty(self):
        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT * ORDER BY;")


class TestSelectPivotBy(QueryParserTestBase):

    def test_pivotby(self):
        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT * PIVOT BY;")

        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT * PIVOT BY a;")

        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT * PIVOT BY a, b, c")

        self.assertParse(
            "SELECT * PIVOT BY a, b",
            Select(ast.Asterisk(), pivot_by=ast.PivotBy([ast.Column('a'), ast.Column('b')])))

        self.assertParse(
            "SELECT * PIVOT BY 1, 2",
            Select(ast.Asterisk(), pivot_by=ast.PivotBy([1, 2])))


class TestSelectOptions(QueryParserTestBase):

    def test_distinct(self):
        self.assertParse(
            "SELECT DISTINCT x;", Select([ast.Target(ast.Column('x'), None)], distinct=True))

    def test_limit_present(self):
        self.assertParse(
            "SELECT * LIMIT 45;", Select(ast.Asterisk(), limit=45))

    def test_limit_empty(self):
        with self.assertRaises(parser.ParseError):
            parser.parse("SELECT * LIMIT;")


class TestBalances(QueryParserTestBase):

    def test_balances_empty(self):
        self.assertParse(
            "BALANCES;", ast.Balances(None, None, None))

    def test_balances_from(self):
        self.assertParse(
            "BALANCES FROM date = 2014-01-01 CLOSE;",
            ast.Balances(
                None,
                ast.From(
                    ast.Equal(
                        ast.Column('date'),
                        ast.Constant(datetime.date(2014, 1, 1))),
                    None, True, None),
                None))

    def test_balances_from_with_transformer(self):
        self.assertParse(
            "BALANCES AT units FROM date = 2014-01-01 CLOSE;",
            ast.Balances('units',
                ast.From(
                    ast.Equal(
                        ast.Column('date'),
                        ast.Constant(datetime.date(2014, 1, 1))),
                    None, True, None),
                None))

    def test_balances_from_with_transformer_simple(self):
        self.assertParse(
            "BALANCES AT units WHERE date = 2014-01-01;",
            ast.Balances('units',
                None,
                ast.Equal(
                    ast.Column('date'),
                    ast.Constant(datetime.date(2014, 1, 1)))))


class TestJournal(QueryParserTestBase):

    def test_journal_empty(self):
        self.assertParse(
            "JOURNAL;",
            ast.Journal(None, None, None))

    def test_journal_account(self):
        self.assertParse(
            "JOURNAL 'Assets:Checking';",
            ast.Journal('Assets:Checking', None, None))

    def test_journal_summary(self):
        self.assertParse(
            "JOURNAL AT cost;",
            ast.Journal(None, 'cost', None))

    def test_journal_account_and_summary(self):
        self.assertParse(
            "JOURNAL 'Assets:Foo' AT cost;",
            ast.Journal('Assets:Foo', 'cost', None))

    def test_journal_from(self):
        self.assertParse(
            "JOURNAL FROM date = 2014-01-01 CLOSE;",
            ast.Journal(None, None,
                ast.From(
                    ast.Equal(
                        ast.Column('date'),
                        ast.Constant(datetime.date(2014, 1, 1))
                    ), None, True, None)))


class TestPrint(QueryParserTestBase):

    def test_print_empty(self):
        self.assertParse(
            "PRINT;", ast.Print(None))

    def test_print_from(self):
        self.assertParse(
            "PRINT FROM date = 2014-01-01 CLOSE;",
            ast.Print(
                ast.From(
                    ast.Equal(
                        ast.Column('date'),
                        ast.Constant(datetime.date(2014, 1, 1))
                    ), None, True, None)))


class TestComments(QueryParserTestBase):

    def test_comments(self):
        self.assertParse(
            """SELECT first, /* comment */ second""",
            Select([
                ast.Target(ast.Column('first'), None),
                ast.Target(ast.Column('second'), None)
            ]))

        self.assertParse(
            """SELECT first, /*
                   comment
                   */ second;""",
            Select([
                ast.Target(ast.Column('first'), None),
                ast.Target(ast.Column('second'), None),
            ]))

        self.assertParse(
            """SELECT first, /**/ second;""",
            Select([
                ast.Target(ast.Column('first'), None),
                ast.Target(ast.Column('second'), None),
            ]))

        self.assertParse(
            """SELECT first, /* /* */ second;""",
            Select([
                ast.Target(ast.Column('first'), None),
                ast.Target(ast.Column('second'), None),
            ]))

        self.assertParse(
            """SELECT first, /* ; */ second;""",
            Select([
                ast.Target(ast.Column('first'), None),
                ast.Target(ast.Column('second'), None),
            ]))


class TestRepr(unittest.TestCase):

    def test_ordering(self):
        self.assertEqual(repr(ast.Ordering.ASC), 'Ordering.ASC')
        self.assertEqual(repr(ast.Ordering.DESC), 'Ordering.DESC')

    def test_ast_node(self):
        self.assertEqual(repr(ast.Constant(1)), 'Constant(value=1)')
        self.assertEqual(repr(ast.Not(ast.Constant(False))), 'Not(operand=Constant(value=False))')

    def test_tosexp(self):
        sexp = parser.parse('SELECT a + 1 FROM #test WHERE a > 42 ORDER BY b DESC').tosexp()
        self.assertEqual(sexp, textwrap.dedent('''\
            (select
              targets: (
                (target
                  expression: (add
                    left: (column
                      name: 'a')
                    right: (constant
                      value: 1))))
              from-clause: (table
                name: 'test')
              where-clause: (greater
                left: (column
                  name: 'a')
                right: (constant
                  value: 42))
              order-by: (
                (orderby
                  column: (column
                    name: 'b')
                  ordering: desc)))'''))

    def test_walk(self):
        query = parser.parse('SELECT a + 1 FROM #test WHERE a > %(foo)s ORDER BY b DESC')
        placeholders = [node for node in query.walk() if isinstance(node, ast.Placeholder)]
        self.assertEqual(placeholders, [ast.Placeholder(name='foo')])


class TestNodeText(unittest.TestCase):

    def test_text(self):
        select = parser.parse('SELECT date + 1')
        self.assertEqual(select.text, 'SELECT date + 1')
        self.assertEqual(select.targets[0].expression.text, 'date + 1')
        self.assertEqual(select.targets[0].expression.left.text, 'date')
        self.assertEqual(select.targets[0].expression.right.text, '1')

    def test_synthetic(self):
        node = ast.And([ast.Constant(False), ast.Constant(True)])
        self.assertIsNone(node.text)
