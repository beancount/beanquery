__copyright__ = "Copyright (C) 2015-2017  Martin Blais"
__license__ = "GNU GPLv2"

import unittest

from beancount import loader
from beanquery import query
from beanquery.test_utils import EXAMPLE_LEDGER_PATH


class TestSimple(unittest.TestCase):

    def test_run_query(self):
        entries, errors, options_map = loader.load_file(EXAMPLE_LEDGER_PATH)
        assert not errors
        sql_query = r"""
          SELECT
            account,
            SUM(position) AS amount
          WHERE account ~ 'Expenses:'
          GROUP BY 1
          ORDER BY 2 DESC
        """
        rtypes, rrows = query.run_query(entries, options_map,
                                        sql_query, 'Martin',
                                        numberify=True)
        self.assertEqual(['account', 'amount (USD)', 'amount (IRAUSD)', 'amount (VACHR)'],
                         [rt[0] for rt in rtypes])
        self.assertEqual(len(rrows[0]), 4)


if __name__ == '__main__':
    unittest.main()
