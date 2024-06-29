__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import unittest
from decimal import Decimal

from beanquery import Connection, CompilationError, ProgrammingError
from beanquery import compiler
from beanquery import query_compile as qc
from beanquery import query_env as qe
from beanquery import parser
from beanquery import tables
from beanquery.parser import ast


class Table:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        if not isinstance(other, tables.Table):
            return NotImplemented
        return other.name == self.name


class TestCompileExpression(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        context = Connection()
        cls.compiler = compiler.Compiler(context)
        cls.compiler.table = qe.PostingsEnvironment()

    def compile(self, expr):
        return self.compiler.compile(expr)

    def test_expr_invalid(self):
        with self.assertRaises(CompilationError):
            self.compile(ast.Column('invalid'))

    def test_expr_column(self):
        self.assertEqual(
            qe.Column('filename'),
            self.compile(ast.Column('filename')))

    def test_expr_function(self):
        self.assertEqual(
            qe.SumPosition(None, [qe.Column('position')]),
            self.compile(ast.Function('sum', [ast.Column('position')])))

    def test_expr_unaryop(self):
        self.assertEqual(
            qc.Operator(ast.Not, [qe.Column('account')]),
            self.compile(ast.Not(ast.Column('account'))))

    def test_expr_binaryop(self):
        self.assertEqual(
            qc.Operator(ast.Equal, [
                qe.Column('date'),
                qc.EvalConstant(datetime.date(2014, 1, 1))
            ]),
            self.compile(ast.Equal(ast.Column('date'), ast.Constant(datetime.date(2014, 1, 1)))))

    def test_expr_constant(self):
        self.assertEqual(
            qc.EvalConstant(Decimal(17)),
            self.compile(ast.Constant(Decimal(17))))

    def test_expr_function_arity(self):
        # Compile with the correct number of arguments.
        self.compile(ast.Function('sum', [ast.Column('number')]))

        # Compile with an incorrect number of arguments.
        with self.assertRaises(CompilationError):
            self.compile(ast.Function('sum', [ast.Column('date'), ast.Column('account')]))

    def test_constants_folding(self):
        # unary op
        self.assertEqual(
            self.compile(ast.Neg(ast.Constant(2))),
            qc.EvalConstant(-2))
        # binary op
        self.assertEqual(
            self.compile(ast.Add(ast.Constant(2), ast.Constant(2))),
            qc.EvalConstant(4))
        # funtion
        self.assertEqual(
            self.compile(ast.Function('root', [ast.Constant('Assets:Cash'), ast.Constant(1)])),
            qc.EvalConstant('Assets'))


class TestCompileAggregateChecks(unittest.TestCase):

    def test_is_aggregate_derived(self):
        columns, aggregates = compiler.get_columns_and_aggregates(
            qc.EvalAnd([
                qc.Operator(ast.Equal, [
                    qe.Column('lineno'),
                    qc.EvalConstant(42),
                ]),
                qc.EvalOr([
                    qc.Operator(ast.Not, [
                        qc.Operator(ast.Equal, [
                            qe.Column('date'),
                            qc.EvalConstant(datetime.date(2014, 1, 1)),
                        ]),
                    ]),
                    qc.EvalConstant(False),
                ]),
            ]))
        self.assertEqual((2, 0), (len(columns), len(aggregates)))

        columns, aggregates = compiler.get_columns_and_aggregates(
            qc.EvalAnd([
                qc.Operator(ast.Equal, [
                    qe.Column('lineno'),
                    qc.EvalConstant(42),
                ]),
                qc.EvalOr([
                    qc.Operator(ast.Not, [
                        qc.Operator(ast.Not, [
                            qc.Operator(ast.Equal, [
                                qe.Column('date'),
                                qc.EvalConstant(datetime.date(2014, 1, 1)),
                            ]),
                        ]),
                    ]),
                    # Aggregation node deep in the tree.
                    qe.SumInt(None, [qc.EvalConstant(1)]),
                ]),
            ]))
        self.assertEqual((2, 1), (len(columns), len(aggregates)))

    def test_get_columns_and_aggregates(self):
        # Simple column.
        c_query = qe.Column('position')
        columns, aggregates = compiler.get_columns_and_aggregates(c_query)
        self.assertEqual((1, 0), (len(columns), len(aggregates)))
        self.assertFalse(compiler.is_aggregate(c_query))

        # Multiple columns.
        c_query = qc.EvalAnd([qe.Column('position'), qe.Column('date')])
        columns, aggregates = compiler.get_columns_and_aggregates(c_query)
        self.assertEqual((2, 0), (len(columns), len(aggregates)))
        self.assertFalse(compiler.is_aggregate(c_query))

        # Simple aggregate.
        c_query = qe.SumPosition(None, [qe.Column('position')])
        columns, aggregates = compiler.get_columns_and_aggregates(c_query)
        self.assertEqual((0, 1), (len(columns), len(aggregates)))
        self.assertTrue(compiler.is_aggregate(c_query))

        # Multiple aggregates.
        c_query = qc.EvalAnd([qe.First(None, [qe.Column('date')]), qe.Last(None, [qe.Column('flag')])])
        columns, aggregates = compiler.get_columns_and_aggregates(c_query)
        self.assertEqual((0, 2), (len(columns), len(aggregates)))
        self.assertTrue(compiler.is_aggregate(c_query))

        # Simple non-aggregate function.
        c_query = qe.Function('length', [qe.Column('account')])
        columns, aggregates = compiler.get_columns_and_aggregates(c_query)
        self.assertEqual((1, 0), (len(columns), len(aggregates)))
        self.assertFalse(compiler.is_aggregate(c_query))

        # Mix of column and aggregates (this is used to detect this illegal case).
        c_query = qc.EvalAnd([
            qe.Function('length', [qe.Column('account')]),
            qe.SumPosition(None, [qe.Column('position')]),
        ])
        columns, aggregates = compiler.get_columns_and_aggregates(c_query)
        self.assertEqual((1, 1), (len(columns), len(aggregates)))
        self.assertTrue(compiler.is_aggregate(c_query))


class CompileSelectBase(unittest.TestCase):

    maxDiff = 8192

    def setUp(self):
        self.ctx = Connection()
        self.ctx.tables['entries'] = qe.EntriesTable(None, None)
        self.ctx.tables['postings'] = qe.PostingsTable(None, None)

    def compile(self, query):
        """Parse one query and compile it.

        Args:
          query: An SQL query to be parsed.
        Returns:
          The AST.
        """
        c_query = self.ctx.compile(self.ctx.parse(query))
        if isinstance(c_query, ast.Select):
            self.assertSelectInvariants(c_query)
        return c_query

    def assertSelectInvariants(self, query):
        """Assert the invariants on the query.

        Args:
          query: An instance of EvalQuery, a compiled query statement.
        Raises:
          AssertionError: if the check fails.
        """
        # Check that the group references cover all the simple indexes.
        if query.group_indexes is not None:
            non_aggregate_indexes = [index
                                     for index, c_target in enumerate(query.c_targets)
                                     if not compiler.is_aggregate(c_target.c_expr)]

            self.assertEqual(set(non_aggregate_indexes), set(query.group_indexes),
                             "Invalid indexes: {}".format(query))

    def assertIndexes(self,
                      query,
                      expected_simple_indexes,
                      expected_aggregate_indexes,
                      expected_group_indexes,
                      expected_order_spec):
        """Check the four lists of indexes for comparison.

        Args:
          query: An instance of EvalQuery, a compiled query statement.
          expected_simple_indexes: The expected visible non-aggregate indexes.
          expected_aggregate_indexes: The expected visible aggregate indexes.
          expected_group_indexes: The expected group_indexes.
          expected_order_spec: The expected order_spec.
        Raises:
          AssertionError: if the check fails.
        """
        # Compute the list of _visible_ aggregates and non-aggregates.
        simple_indexes = [index
                          for index, c_target in enumerate(query.c_targets)
                          if c_target.name and not compiler.is_aggregate(c_target.expression)]
        aggregate_indexes = [index
                             for index, c_target in enumerate(query.c_targets)
                             if c_target.name and compiler.is_aggregate(c_target.expression)]

        self.assertEqual(set(expected_simple_indexes), set(simple_indexes))

        self.assertEqual(set(expected_aggregate_indexes), set(aggregate_indexes))

        self.assertEqual(
            set(expected_group_indexes) if expected_group_indexes is not None else None,
            set(query.group_indexes) if query.group_indexes is not None else None)

        self.assertEqual(
            set(expected_order_spec) if expected_order_spec is not None else None,
            set(query.order_spec) if query.order_spec is not None else None)

    def assertCompile(self, expected, query, debug=False):
        """Assert parsed and compiled contents from 'query' is 'expected'.

        Args:
          expected: An expected AST to compare against the parsed value.
          query: An SQL query to be parsed.
          debug: A boolean, if true, print extra debugging information on the console.
        Raises:
          AssertionError: If the actual AST does not match the expected one.
        """
        actual = self.compile(query)
        if debug:
            print()
            print()
            print(actual)
            print()
        try:
            self.assertEqual(expected, actual)
            return actual
        except AssertionError:
            print()
            print("Expected: {}".format(expected))
            print("Actual  : {}".format(actual))
            raise


class TestCompileFundamentals(CompileSelectBase):

    def test_operaotors(self):
        expr = self.compile("SELECT 1 + 1 AS expr")
        self.assertEqual(expr, qc.EvalQuery(Table('postings'), [
            qc.EvalTarget(qc.EvalConstant(2), 'expr', False)
        ], None, None, None, None, None, None))

        expr = self.compile("SELECT 1 + meta['int'] AS expr")
        self.assertEqual(expr, qc.EvalQuery(Table('postings'), [
            qc.EvalTarget(
                qc.Operator(ast.Add, [
                    qc.EvalConstant(1),
                    qe.Function('decimal', [
                        qc.EvalGetItem(qe.Column('meta'), 'int')
                    ]),
                ]), 'expr', False)
        ], None, None, None, None, None, None))

    def test_coalesce(self):
        expr = self.compile("SELECT coalesce(narration, str(date), '~') AS expr")
        self.assertEqual(expr, qc.EvalQuery(Table('postings'), [
            qc.EvalTarget(
                qc.EvalCoalesce([
                    qe.Column('narration'),
                    qe.Function('str', [qe.Column('date')]),
                    qc.EvalConstant('~'),
                ]), 'expr', False)
        ], None, None, None, None, None, None))

        with self.assertRaises(CompilationError):
            self.compile("SELECT coalesce(narration, date, 1)")


class TestCompileSelect(CompileSelectBase):

    def test_compile_from(self):
        # Test the compilation of from.

        query = self.compile("SELECT account FROM CLOSE;")
        self.assertEqual(query.table.close, True)

        query = self.compile("SELECT account FROM length(payee) != 0;")
        self.assertTrue(isinstance(query.c_where, qc.EvalNode))

        with self.assertRaises(CompilationError):
            query = self.compile("SELECT account FROM sum(payee) != 0;")

    def test_compile_from_invalid_dates(self):
        self.compile("""
          SELECT account FROM  OPEN ON 2014-03-01  CLOSE ON 2014-03-02;
        """)

        self.compile("""
          SELECT account FROM  OPEN ON 2014-03-02  CLOSE ON 2014-03-02;
        """)

        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT account FROM  OPEN ON 2014-03-03  CLOSE ON 2014-03-02;
            """)

    def test_compile_targets_wildcard(self):
        # Test the wildcard expansion.
        query = self.compile("SELECT *;")
        self.assertTrue(list, type(query.c_targets))
        self.assertGreater(len(query.c_targets), 3)
        self.assertTrue(all(isinstance(target.c_expr, qc.EvalColumn)
                            for target in query.c_targets))

    def test_compile_targets_named(self):
        # Test the wildcard expansion.
        query = self.compile("SELECT length(account), account as a, date;")
        self.assertEqual(
            [qc.EvalTarget(qe.Function('length', [qe.Column('account')]), 'length(account)', False),
             qc.EvalTarget(qe.Column('account'), 'a', False),
             qc.EvalTarget(qe.Column('date'), 'date', False)],
            query.c_targets)

    def test_compile_mixed_aggregates(self):
        # Check mixed aggregates and non-aggregates in a target.
        with self.assertRaises(CompilationError) as assertion:
            self.compile("""
              SELECT length(account) and sum(length(account));
            """)
        self.assertRegex(str(assertion.exception), 'mixed aggregates and non-aggregates')

    def test_compile_aggregates_of_aggregates(self):
        # Check mixed aggregates and non-aggregates in a target.
        with self.assertRaises(CompilationError) as assertion:
            self.compile("""
              SELECT sum(sum(length(account)));
            """)
        self.assertRegex(str(assertion.exception), 'aggregates of aggregates')

    def test_compile_having_non_aggregate(self):
        with self.assertRaises(CompilationError) as assertion:
            self.compile("""
              SELECT account, sum(number) GROUP BY account HAVING flag;
            """)
        self.assertRegex(str(assertion.exception), 'the HAVING clause must be an aggregate')

    def test_compile_group_by_inventory(self):
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT sum(number), balance GROUP BY balance;
            """)


class TestCompileSelectGroupBy(CompileSelectBase):

    def test_compile_group_by_non_aggregates(self):
        self.compile("""
          SELECT payee GROUP BY payee, length(account);
        """)

        with self.assertRaises(CompilationError) as assertion:
            self.compile("""
              SELECT payee GROUP BY payee, last(account);
            """)
        self.assertRegex(str(assertion.exception), 'may not be aggregates')

    def test_compile_group_by_reference_by_name(self):
        # Valid references to target names.
        self.compile("""
          SELECT payee, last(account) GROUP BY payee;
        """)
        self.compile("""
          SELECT payee as a, last(account) as len GROUP BY a;
        """)

        # References to non-targets have to be valid.
        self.compile("""
          SELECT payee, last(account) as len GROUP BY payee, date;
        """)

        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT payee, last(account) as len GROUP BY something;
            """)

    def test_compile_group_by_reference_by_number(self):
        self.compile("""
          SELECT date, payee, narration GROUP BY 1, 2, 3;
        """)

        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT date, payee, narration GROUP BY 1, 2, 3, 4;
            """)

    def test_compile_group_by_reference_an_aggregate(self):
        # By name.
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT payee, last(account) as last GROUP BY last;
            """)
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT account, sum(number) as sum_num GROUP BY account, sum_num;
            """)

        # By number.
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT payee, last(account) as last GROUP BY 2;
            """)

        # Explicit aggregate in group-by clause.
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT account, sum(number) GROUP BY account, sum(number);
            """)

    def test_compile_group_by_implicit(self):
        self.compile("""
          SELECT payee, last(account);
        """)

        self.compile("""
          SELECT first(account), last(account);
        """)

    def test_compile_group_by_coverage(self):
        # Non-aggregates.
        query = self.compile("SELECT account, length(account);")
        self.assertEqual(None, query.group_indexes)
        self.assertEqual(None, query.order_spec)

        # Aggregates only.
        query = self.compile("SELECT first(account), last(account);")
        self.assertEqual([], query.group_indexes)

        # Mixed with non-aggregates in group-by clause.
        query = self.compile("SELECT account, sum(number) GROUP BY account;")
        self.assertEqual([0], query.group_indexes)

        # Mixed with non-aggregates in group-by clause with non-aggregates a
        # strict subset of the group-by columns. 'account' is a subset of
        # {'account', 'flag'}.
        query = self.compile("""
          SELECT account, sum(number) GROUP BY account, flag;
        """)
        self.assertEqual([0, 2], query.group_indexes)

        # Non-aggregates not covered by group-by clause.
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT account, date, sum(number) GROUP BY account;
            """)
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT payee, last(account) as len GROUP BY date;
            """)

        # Non-aggregates not covered by group-by clause, and no aggregates in
        # the list of targets.
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT date, flag, account, number GROUP BY date, flag;
            """)

        # All non-aggregates and matching list of aggregates (this is a
        # pointless list of aggregates, essentially).
        query = self.compile("""
          SELECT date, flag, account GROUP BY date, flag, account;
        """)
        self.assertEqual([0, 1, 2], query.group_indexes)

    def test_compile_group_by_reconcile(self):
        # Check that no invisible column is created if redundant.
        query = self.compile("""
          SELECT account, length(account), sum(number)
          GROUP BY account, length(account);
        """)
        self.assertEqual([0, 1], query.group_indexes)


class TestCompileSelectOrderBy(CompileSelectBase):

    def test_compile_order_by_simple(self):
        query = self.compile("""
          SELECT account, sum(number) GROUP BY account ORDER BY account;
        """)
        self.assertEqual([0], query.group_indexes)
        self.assertEqual([(0, False)], query.order_spec)

    def test_compile_order_by_simple_2(self):
        query = self.compile("""
          SELECT account, length(narration) GROUP BY account, 2 ORDER BY 1, 2;
        """)
        self.assertEqual([0, 1], query.group_indexes)
        self.assertEqual([(0, False), (1, False)], query.order_spec)

        query = self.compile("""
          SELECT account, length(narration) as l GROUP BY account, l ORDER BY l;
        """)
        self.assertEqual([0, 1], query.group_indexes)
        self.assertEqual([(1, False)], query.order_spec)

    def test_compile_order_by_create_non_agg(self):
        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT account, last(narration) GROUP BY account ORDER BY year(date);
            """)

        with self.assertRaises(CompilationError):
            self.compile("""
              SELECT account GROUP BY account ORDER BY year(date);
            """)

        query = self.compile("""
          SELECT account, year(date) GROUP BY 1, 2 ORDER BY 2;
        """)
        self.assertEqual([0, 1], query.group_indexes)
        self.assertEqual([(1, False)], query.order_spec)

        # We detect similarity between order-by and targets yet.
        self.compile("""
          SELECT account, year(date) GROUP BY 1, 2 ORDER BY year(date);
        """)

    def test_compile_order_by_reconcile(self):
        # Check that no invisible column is created if redundant.
        query = self.compile("""
          SELECT account, length(account)
          ORDER BY length(account);
        """)
        self.assertEqual([(1, False)], query.order_spec)

    def test_compile_order_by_reference_invisible(self):
        # So this is an interesting case: the grouping expression is an
        # invisible non-aggregate (length(account)) and the ordering expression
        # refers to the same non-aggregate expression. If they are reconciled to
        # the same invisible expression, the condition that the grouping
        # expressions cover all the non-aggregates is fulfilled. Otherwise, it
        # would fail. In order to support the compilation of this, we must
        # reconcile the grouping and ordering columns by comparing their values.
        query = self.compile("""
          SELECT count(account) as num, first(account) as first
          GROUP BY length(account)
          ORDER BY length(account);
        """)
        self.assertEqual([2], query.group_indexes)
        self.assertEqual([(2, False)], query.order_spec)

    def test_compile_order_by_aggregate(self):
        query = self.compile("""
          SELECT account, first(narration) GROUP BY account ORDER BY 2;
        """)
        self.assertEqual([0], query.group_indexes)
        self.assertEqual([(1, False)], query.order_spec)

        query = self.compile("""
          SELECT account, first(narration) as f GROUP BY account ORDER BY f;
        """)
        self.assertEqual([0], query.group_indexes)
        self.assertEqual([(1, False)], query.order_spec)

        query = self.compile("""
          SELECT account, first(narration) GROUP BY account ORDER BY sum(number);
        """)
        self.assertEqual([0], query.group_indexes)
        self.assertEqual([(2, False)], query.order_spec)

        query = self.compile("""
          SELECT account GROUP BY account ORDER BY sum(number);
        """)
        self.assertEqual([0], query.group_indexes)
        self.assertEqual([(1, False)], query.order_spec)


class TestTranslationJournal(CompileSelectBase):

    maxDiff = 4096

    def test_journal(self):
        journal = parser.parse("JOURNAL;")
        select = compiler.transform_journal(journal)
        self.assertEqual(select,
            ast.Select([
                ast.Target(ast.Column('date'), None),
                ast.Target(ast.Column('flag'), None),
                ast.Target(ast.Function('maxwidth', [
                    ast.Column('payee'), ast.Constant(48)]), None),
                ast.Target(ast.Function('maxwidth', [
                    ast.Column('narration'), ast.Constant(80)]), None),
                ast.Target(ast.Column('account'), None),
                ast.Target(ast.Column('position'), None),
                ast.Target(ast.Column('balance'), None),
            ],
            None, None, None, None, None, None, None))

    def test_journal_with_account(self):
        journal = parser.parse("JOURNAL 'liabilities';")
        select = compiler.transform_journal(journal)
        self.assertEqual(select, ast.Select([
            ast.Target(ast.Column('date'), None),
            ast.Target(ast.Column('flag'), None),
            ast.Target(ast.Function('maxwidth', [
                ast.Column('payee'),
                ast.Constant(48)]), None),
            ast.Target(ast.Function('maxwidth', [
                ast.Column('narration'),
                ast.Constant(80)]), None),
            ast.Target(ast.Column('account'), None),
            ast.Target(ast.Column('position'), None),
            ast.Target(ast.Column('balance'), None),
        ],
        None,
        ast.Match(ast.Column('account'), ast.Constant('liabilities')),
        None, None, None, None, None))

    def test_journal_with_account_and_from(self):
        journal = parser.parse("JOURNAL 'liabilities' FROM year = 2014;")
        select = compiler.transform_journal(journal)
        self.assertEqual(select, ast.Select([
            ast.Target(ast.Column('date'), None),
            ast.Target(ast.Column('flag'), None),
            ast.Target(ast.Function('maxwidth', [
                ast.Column('payee'),
                ast.Constant(48)]), None),
            ast.Target(ast.Function('maxwidth', [
                ast.Column('narration'),
                ast.Constant(80)]), None),
            ast.Target(ast.Column('account'), None),
            ast.Target(ast.Column('position'), None),
            ast.Target(ast.Column('balance'), None),
        ],
        ast.From(ast.Equal(ast.Column('year'), ast.Constant(2014)), None, None, None),
        ast.Match(ast.Column('account'), ast.Constant('liabilities')),
        None, None, None, None, None))

    def test_journal_with_account_func_and_from(self):
        journal = parser.parse("JOURNAL 'liabilities' AT cost FROM year = 2014;")
        select = compiler.transform_journal(journal)
        self.assertEqual(select, ast.Select([
            ast.Target(ast.Column('date'), None),
            ast.Target(ast.Column('flag'), None),
            ast.Target(ast.Function('maxwidth', [
                ast.Column('payee'),
                ast.Constant(48)]), None),
            ast.Target(ast.Function('maxwidth', [
                ast.Column('narration'),
                ast.Constant(80)]), None),
            ast.Target(ast.Column('account'), None),
            ast.Target(ast.Function('cost', [ast.Column('position')]), None),
            ast.Target(ast.Function('cost', [ast.Column('balance')]), None),
        ],
        ast.From(ast.Equal(ast.Column('year'), ast.Constant(2014)), None, None, None),
        ast.Match(ast.Column('account'), ast.Constant('liabilities')),
        None, None, None, None, None))


class TestTranslationBalance(CompileSelectBase):

    group_by = ast.GroupBy([
        ast.Column('account'),
        ast.Function('account_sortkey', [
            ast.Column(name='account')])], None)

    order_by = [ast.OrderBy(ast.Function('account_sortkey', [ast.Column('account')]), ast.Ordering.ASC)]

    def test_balance(self):
        balance = parser.parse("BALANCES;")
        select = compiler.transform_balances(balance)
        self.assertEqual(select, ast.Select([
            ast.Target(ast.Column('account'), None),
            ast.Target(ast.Function('sum', [
                ast.Column('position')
            ]), None),
        ],
        None, None, self.group_by, self.order_by, None, None, None))

    def test_balance_with_units(self):
        balance = parser.parse("BALANCES AT cost;")
        select = compiler.transform_balances(balance)
        self.assertEqual(select, ast.Select([
            ast.Target(ast.Column('account'), None),
            ast.Target(ast.Function('sum', [
                ast.Function('cost', [
                    ast.Column('position')
                ])
            ]), None)
        ],
        None, None, self.group_by, self.order_by, None, None, None))

    def test_balance_with_units_and_from(self):
        balance = parser.parse("BALANCES AT cost FROM year = 2014;")
        select = compiler.transform_balances(balance)
        self.assertEqual(select, ast.Select([
            ast.Target(ast.Column('account'), None),
            ast.Target(ast.Function('sum', [
                ast.Function('cost', [
                    ast.Column('position')
                ])
            ]), None),
        ],
        ast.From(ast.Equal(ast.Column('year'), ast.Constant(2014)), None, None, None),
        None, self.group_by, self.order_by, None, None, None))

    def test_print(self):
        self.assertCompile(qc.EvalPrint(Table('entries'), None), "PRINT;")

    def test_print_from(self):
        self.assertCompile(
            qc.EvalPrint(
                Table('entries'),
                qc.Operator(ast.Equal, [
                    qe.Column('year'),
                    qc.EvalConstant(2014),
                ])),
            "PRINT FROM year = 2014;")


class TestCompileParameters(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.context = Connection()

    def compile(self, query, params):
        c = compiler.Compiler(self.context)
        c.table = self.context.tables.get('')
        return c.compile(parser.parse(query), params)

    def test_named_parameters(self):
        query = self.compile('''SELECT %(x)s + %(y)s''', {'x': 1, 'y': 2})
        self.assertEqual(query, qc.EvalQuery(
            Table(''), [
                # addition of constants is optimized away
                qc.EvalTarget(qc.EvalConstant(3), '%(x)s + %(y)s', False)
            ], None, None, None, None, None, None))

    def test_positional_parameters(self):
        query = self.compile('''SELECT %s + %s''', (1, 2, ))
        self.assertEqual(query, qc.EvalQuery(
            Table(''), [
                # addition of constants is optimized away
                qc.EvalTarget(qc.EvalConstant(3), '%s + %s', False)
            ], None, None, None, None, None, None))

    def test_mixing_parameters(self):
        with self.assertRaises(ProgrammingError):
            self.compile('''SELECT %s + %(foo)s''', (1, 2))

    def test_missing_parameters_positional(self):
        with self.assertRaises(ProgrammingError):
            self.compile('''SELECT %s + %s''', (1, ))

    def test_missing_parameters_named(self):
        with self.assertRaises(ProgrammingError):
            self.compile('''SELECT %(x)s + %(y)s''', {'x': 1})
