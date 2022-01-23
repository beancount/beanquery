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
from beancount.parser import printer

from beanquery import shell


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
    if errors:
        printer.print_errors(errors)
    return entries, errors, options


def runshell(function):
    """Decorate a function to run the shell and return the output."""
    def wrapper(self):
        with test_utils.capture('stdout') as stdout:
            shell_obj = shell.BQLShell(False, load, sys.stdout)
            shell_obj.do_reload()
            shell_obj.onecmd(function.__doc__)
        return function(self, stdout.getvalue())
    return wrapper


class TestUseCases(unittest.TestCase):
    """Testing all the use cases from the proposal here.
    I'm hoping to replace reports by these queries instead."""

    @runshell
    def test_print_from(self, output):
        """
        PRINT FROM narration ~ 'alone'
        """
        self.assertRegex(output, 'Eating out alone')

    @runshell
    def test_accounts(self, output):
        """
        SELECT DISTINCT account, open_date(account)
        ORDER BY account_sortkey(account);
        """
        self.assertRegex(output, 'Assets:Checking *2022-01-01')
        self.assertRegex(output, 'Income:ACME *2022-01-01')

    @runshell
    def test_commodities(self, output):
        """
        SELECT DISTINCT currency ORDER BY 1;
        """
        self.assertRegex(output, 'USD')
        self.assertRegex(output, 'IRAUSD')
        self.assertRegex(output, 'VACHR')

    @runshell
    def test_commodities_cost(self, output):
        """
        SELECT DISTINCT cost_currency ORDER BY 1;
        """
        self.assertRegex(output, 'USD')

    @runshell
    def test_commodities_pairs(self, output):
        """
        SELECT DISTINCT currency, cost_currency ORDER BY 1, 2;
        """
        self.assertRegex(output, 'GLD *USD')

    @runshell
    def test_balances(self, output):
        """
        BALANCES AT cost;
        """
        self.assertRegex(output, r'Assets:Gold *\d+\.\d+ USD')

    @runshell
    def test_balances_with_where(self, output):
        """
        JOURNAL 'Assets:Checking';
        """
        self.assertRegex(output, 'Salary')

    @runshell
    def test_balance_sheet(self, output):
        """
        BALANCES AT cost
        FROM OPEN ON 2022-01-02 CLOSE ON 2022-02-01 CLEAR;
        """
        self.assertRegex(output, r'Assets:Gold * \d+\.\d+ USD')

    @runshell
    def test_income_statement(self, output):
        """
        SELECT account, cost(sum(position))
        FROM OPEN ON 2022-01-01 CLOSE ON 2023-01-01
        WHERE account ~ '(Income|Expenses):*'
        GROUP BY account, account_sortkey(account)
        ORDER BY account_sortkey(account);
        """
        self.assertRegex(
            output, 'Expenses:Taxes:401k *4.00 IRAUSD')

    @runshell
    def test_journal(self, output):
        """
        JOURNAL 'Assets:Checking'
        FROM OPEN ON 2022-02-01 CLOSE ON 2022-03-01;
        """
        self.assertRegex(output, "2022-01-31 S *Opening balance for 'Assets:Checking'")
        self.assertRegex(output, "Test 02")

    @runshell
    def test_conversions(self, output):
        """
        SELECT date, payee, narration, position, balance
        FROM OPEN ON 2022-01-01 CLOSE ON 2023-01-01
        WHERE flag = 'C'
        """
        self.assertRegex(output, "2022-12-31 *Conversion for")

    @runshell
    def test_documents(self, output):
        """
        SELECT date, account, narration
        WHERE type = 'Document';
        """
        ## FIXME: Make this possible, we need an example with document entries.

    @runshell
    def test_holdings(self, output):
        """
        SELECT account, currency, cost_currency, sum(position)
        GROUP BY account, currency, cost_currency;
        """
        ## FIXME: Here we need to finally support FLATTEN to make this happen properly.


class TestRun(unittest.TestCase):

    @runshell
    def test_run_custom__list(self, output):
        """
        RUN
        """
        self.assertEqual("home taxes",
                         re.sub(r'[] \n\t]+', ' ', output).strip())

    @runshell
    def test_run_custom__query_not_exists(self, output):
        """
        RUN something
        """
        self.assertEqual("ERROR: Query 'something' not found.", output.strip())

    @runshell
    def test_run_custom__query_id(self, output):
        """
        RUN taxes
        """
        self.assertRegex(output, 'date +description +position +balance')
        self.assertRegex(output, r'ACME \| Salary')

    @runshell
    def test_run_custom__query_string(self, output):
        """
        RUN "taxes"
        """
        self.assertRegex(output, 'date +description +position +balance')
        self.assertRegex(output, r'ACME \| Salary')

    @runshell
    def test_run_custom__all(self, output):
        """
        RUN *
        """
        self.assertRegex(output, 'date +description +position +balance')
        self.assertRegex(output, r'ACME \| Salary')
        self.assertRegex(output, 'account +total')
        self.assertRegex(output, 'Expenses:Home:Rent')


class ClickTestCase(unittest.TestCase):
    """Base class for command-line program test cases."""

    def run_with_args(self, function, *args):
        runner = click.testing.CliRunner()
        result = runner.invoke(function, args, catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)
        return result


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
        result = self.run_with_args(shell.main, filename, "SELECT 1;")
        self.assertTrue(result.stdout)


if __name__ == '__main__':
    unittest.main()
