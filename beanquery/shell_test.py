__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import functools
import re
import sys
import textwrap
import unittest

import click.testing

from beancount import loader
from beancount.utils import test_utils

from beanquery import shell
from beanquery.sources.beancount import add_beancount_tables


@functools.lru_cache(None)
def load():
    entries, errors, options = loader.load_string(textwrap.dedent("""
      2022-01-01 open Assets:Checking         USD
      2022-01-01 open Assets:Federal:401k     IRAUSD
      2022-01-01 open Assets:Gold             GLD
      2022-01-01 open Assets:Vacation         VACHR
      2022-01-01 open Assets:Vanguard:RGAGX   RGAGX
      2022-01-01 open Expenses:Commissions    USD
      2022-01-01 open Expenses:Food           USD
      2022-01-01 open Expenses:Home:Rent      USD
      2022-01-01 open Expenses:Taxes:401k     IRAUSD
      2022-01-01 open Expenses:Taxes:Federal  USD
      2022-01-01 open Expenses:Tests          USD
      2022-01-01 open Expenses:Vacation       VACHR
      2022-01-01 open Income:ACME             USD
      2022-01-01 open Income:Gains            USD
      2022-01-01 open Income:Vacation         VACHR

      2022-01-01 * "ACME" "Salary"
        Assets:Checking           10.00 USD
        Income:ACME              -11.00 USD
        Expenses:Taxes:Federal     1.00 USD
        Assets:Federal:401k       -2.00 IRAUSD
        Expenses:Taxes:401k        2.00 IRAUSD
        Assets:Vacation               5 VACHR
        Income:Vacation              -5 VACHR

      2022-01-01 * "Rent"
        Assets:Checking           42.00 USD
        Expenses:Home:Rent        42.00 USD

      2022-01-02 * "Holidays"
        Assets:Vacation              -1 VACHR
        Expenses:Vacation

      2022-01-03 * "Test 01"
        Assets:Checking            1.00 USD
        Expenses:Tests

      2022-01-04 * "My Fovorite Plase" "Eating out alone"
        Assets:Checking            4.00 USD
        Expenses:Food

      2022-01-05 * "Invest"
        Assets:Checking         -359.94 USD
        Assets:Vanguard:RGAGX     2.086 RGAGX {172.55 USD}

      2013-10-23 * "Buy Gold"
        Assets:Checking        -1278.67 USD
        Assets:Gold                   9 GLD {141.08 USD}
        Expenses:Commissions          8.95 USD

      2022-01-07 * "Sell Gold"
        Assets:Gold                 -16 GLD {147.01 USD} @ 135.50 USD
        Assets:Checking         2159.05 USD
        Expenses:Commissions       8.95 USD
        Income:Gains             184.16 USD

      2022-01-08 * "Sell Gold"
        Assets:Gold                 -16 GLD {147.01 USD} @ 135.50 USD
        Assets:Checking         2159.05 USD
        Expenses:Commissions       8.95 USD
        Income:Gains             184.16 USD

      2022-02-01 * "ACME" "Salary"
        Assets:Checking           10.00 USD
        Income:ACME              -11.00 USD
        Expenses:Taxes:Federal     1.00 USD
        Assets:Federal:401k       -2.00 IRAUSD
        Expenses:Taxes:401k        2.00 IRAUSD
        Assets:Vacation               5 VACHR
        Income:Vacation              -5 VACHR

      2022-02-01 * "Rent"
        Assets:Checking           43.00 USD
        Expenses:Home:Rent        43.00 USD

      2022-02-02 * "Test 02"
        Assets:Checking            2.00 USD
        Expenses:Tests

      2030-01-01 query "taxes" "
        SELECT
          date, description, position, balance
        WHERE
          account ~ 'Taxes'
        ORDER BY date DESC
        LIMIT 20"

      2015-01-01 query "home" "
        SELECT
          last(date) as latest,
          account,
          sum(position) as total
        WHERE
          account ~ ':Home:'
        GROUP BY account"

    """))
    return entries, errors, options


def run_shell_command(cmd):
    """Run a shell command and return its output."""
    with test_utils.capture('stdout') as stdout, test_utils.capture('stderr') as stderr:
        shell_obj = shell.BQLShell(None, sys.stdout)
        entries, errors, options = load()
        add_beancount_tables(shell_obj.context, entries, errors, options)
        shell_obj._extract_queries(entries)  # pylint: disable=protected-access
        shell_obj.onecmd(cmd)
    return stdout.getvalue(), stderr.getvalue()


def runshell(function):
    """Decorate a function to run the shell and return the output."""
    def wrapper(self):
        out, err = run_shell_command(function.__doc__)
        return function(self, out, err)
    return wrapper


class TestUseCases(unittest.TestCase):
    """Testing all the use cases from the proposal here.
    I'm hoping to replace reports by these queries instead."""

    @runshell
    def test_print_from(self, out, err):
        """
        PRINT FROM narration ~ 'alone'
        """
        self.assertRegex(out, 'Eating out alone')

    @runshell
    def test_accounts(self, out, err):
        """
        SELECT DISTINCT account, open_date(account)
        ORDER BY account_sortkey(account);
        """
        self.assertRegex(out, 'Assets:Checking *2022-01-01')
        self.assertRegex(out, 'Income:ACME *2022-01-01')

    @runshell
    def test_commodities(self, out, err):
        """
        SELECT DISTINCT currency ORDER BY 1;
        """
        self.assertRegex(out, 'USD')
        self.assertRegex(out, 'IRAUSD')
        self.assertRegex(out, 'VACHR')

    @runshell
    def test_commodities_cost(self, out, err):
        """
        SELECT DISTINCT cost_currency ORDER BY 1;
        """
        self.assertRegex(out, 'USD')

    @runshell
    def test_commodities_pairs(self, out, err):
        """
        SELECT DISTINCT currency, cost_currency ORDER BY 1, 2;
        """
        self.assertRegex(out, 'GLD *USD')

    @runshell
    def test_balances(self, out, err):
        """
        BALANCES AT cost;
        """
        self.assertRegex(out, r'Assets:Gold *\d+\.\d+ USD')

    @runshell
    def test_balances_with_where(self, out, err):
        """
        JOURNAL 'Assets:Checking';
        """
        self.assertRegex(out, 'Salary')

    @runshell
    def test_balance_sheet(self, out, err):
        """
        BALANCES AT cost
        FROM OPEN ON 2022-01-02 CLOSE ON 2022-02-01 CLEAR;
        """
        self.assertRegex(out, r'Assets:Gold * \d+\.\d+ USD')

    @runshell
    def test_income_statement(self, out, err):
        """
        SELECT account, cost(sum(position))
        FROM OPEN ON 2022-01-01 CLOSE ON 2023-01-01
        WHERE account ~ '(Income|Expenses):*'
        GROUP BY account, account_sortkey(account)
        ORDER BY account_sortkey(account);
        """
        self.assertRegex(
            out, 'Expenses:Taxes:401k *4.00 IRAUSD')

    @runshell
    def test_journal(self, out, err):
        """
        JOURNAL 'Assets:Checking'
        FROM OPEN ON 2022-02-01 CLOSE ON 2022-03-01;
        """
        self.assertRegex(out, "2022-01-31 +S +Opening balance for 'Assets:Checking'")
        self.assertRegex(out, "Test 02")

    @runshell
    def test_conversions(self, out, err):
        """
        SELECT date, payee, narration, position, balance
        FROM OPEN ON 2022-01-01 CLOSE ON 2023-01-01
        WHERE flag = 'C'
        """
        self.assertRegex(out, "2022-12-31 *Conversion for")

    @runshell
    def test_documents(self, out, err):
        """
        SELECT date, account, narration
        WHERE type = 'Document';
        """
        ## FIXME: Make this possible, we need an example with document entries.

    @runshell
    def test_holdings(self, out, err):
        """
        SELECT account, currency, cost_currency, sum(position)
        GROUP BY account, currency, cost_currency;
        """
        ## FIXME: Here we need to finally support FLATTEN to make this happen properly.


class TestRun(unittest.TestCase):

    @runshell
    def test_run_custom__list(self, out, err):
        """
        .run
        """
        self.assertEqual("home taxes",
                         re.sub(r'[] \n\t]+', ' ', out).strip())

    @runshell
    def test_run_custom__query_not_exists(self, out, err):
        """
        .run something
        """
        self.assertEqual('error: query "something" not found', err.strip())

    @runshell
    def test_run_custom__query_id(self, out, err):
        """
        .run taxes
        """
        self.assertRegex(out, 'date +description +position +balance')
        self.assertRegex(out, r'ACME \| Salary')

    @runshell
    def test_run_custom__query_string(self, out, err):
        """
        RUN "taxes"
        """
        self.assertRegex(out, 'date +description +position +balance')
        self.assertRegex(out, r'ACME \| Salary')

    @runshell
    def test_run_custom__all(self, out, err):
        """
        RUN *
        """
        self.assertRegex(out, 'date +description +position +balance')
        self.assertRegex(out, r'ACME \| Salary')
        self.assertRegex(out, 'account +total')
        self.assertRegex(out, 'Expenses:Home:Rent')


class TestHelp(unittest.TestCase):

    def test_help_functions(self):
        for name in dir(shell.BQLShell):
            if name.startswith('help_'):
                run_shell_command('help ' + name[5:])


class ClickTestCase(unittest.TestCase):
    """Base class for command-line program test cases."""

    def main(self, *args):
        init_filename = shell.INIT_FILENAME
        history_filename = shell.HISTORY_FILENAME
        try:
            shell.INIT_FILENAME = ''
            shell.HISTORY_FILENAME = ''
            runner = click.testing.CliRunner()
            result = runner.invoke(shell.main, args, catch_exceptions=False)
            self.assertEqual(result.exit_code, 0)
            return result
        finally:
            shell.INIT_FILENAME = init_filename
            shell.HISTORY_FILENAME = history_filename


class TestShell(ClickTestCase):

    @test_utils.docfile
    def test_success(self, filename):
        """
        2013-01-01 open Assets:Account1
        2013-01-01 open Assets:Account2
        2013-01-01 open Assets:Account3
        2013-01-01 open Equity:Unknown

        2013-04-05 *
          Equity:Unknown
          Assets:Account1     5000 USD

        2013-04-05 *
          Assets:Account1     -3000 USD
          Assets:Account2        30 BOOG {100 USD}

        2013-04-05 *
          Assets:Account1     -1000 USD
          Assets:Account3       800 EUR @ 1.25 USD
        """
        result = self.main(filename, "SELECT 1;")
        self.assertTrue(result.stdout)

    @test_utils.docfile
    def test_format_csv(self, filename):
        """
        """
        r = self.main(filename, '--format=csv', "SELECT 111 AS one, 222 AS two FROM #")
        self.assertEqual(r.stdout, textwrap.dedent('''\
            one,two
            111,222
        '''))

    @test_utils.docfile
    def test_format_text(self, filename):
        """
        """
        r = self.main(filename, '--format=text', "SELECT 111 AS one, 222 AS two FROM #")
        self.assertEqual(r.stdout, textwrap.dedent('''\
            one  two
            ---  ---
            111  222
        '''))


if __name__ == '__main__':
    unittest.main()
