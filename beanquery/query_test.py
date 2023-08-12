__copyright__ = "Copyright (C) 2015-2017  Martin Blais"
__license__ = "GNU GPLv2"

import unittest

from beancount import loader
from beanquery import query


class TestSimple(unittest.TestCase):

    @loader.load_doc()
    def test_run_query(self, entries, _, options):
        """
        2022-01-01 open Assets:Checking         USD
        2022-01-01 open Income:ACME             USD
        2022-01-01 open Expenses:Taxes:Federal  USD
        2022-01-01 open Assets:Federal:401k     IRAUSD
        2022-01-01 open Expenses:Taxes:401k     IRAUSD
        2022-01-01 open Assets:Vacation         VACHR
        2022-01-01 open Income:Vacation         VACHR
        2022-01-01 open Expenses:Vacation       VACHR
        2022-01-01 open Expenses:Tests          USD

        2022-01-01 * "ACME" "Salary"
          Assets:Checking           10.00 USD
          Income:ACME              -11.00 USD
          Expenses:Taxes:Federal     1.00 USD
          Assets:Federal:401k       -2.00 IRAUSD
          Expenses:Taxes:401k        2.00 IRAUSD
          Assets:Vacation               5 VACHR
          Income:Vacation              -5 VACHR

        2022-01-02 * "Holidays"
          Assets:Vacation              -1 VACHR
          Expenses:Vacation

        2022-01-03 * "Test"
          Assets:Checking            3.00 USD
          Expenses:Tests

        """

        sql_query = r"""
          SELECT
            account,
            sum(position) AS amount
          WHERE root(account, 1) = '{0}'
          GROUP BY 1
          ORDER BY 2 DESC
        """

        rtypes, rrows = query.run_query(entries, options, sql_query, 'Expenses', numberify=True)
        columns = [c.name for c in rtypes]
        self.assertEqual(columns, ['account', 'amount (USD)', 'amount (VACHR)', 'amount (IRAUSD)'])
        self.assertEqual(len(rrows[0]), 4)


if __name__ == '__main__':
    unittest.main()
