__license__ = "GNU GPLv2"

import datetime
import textwrap
import unittest

from beancount import loader
from beancount.core import amount

import beanquery


def load(source):
    entries, errors, options = loader.load_string(textwrap.dedent(source))
    assert not errors, errors
    conn = beanquery.connect('beancount:', entries=entries, errors=errors, options=options)
    return conn


class TestCustomTable(unittest.TestCase):

    def test_columns(self):
        conn = load("")
        cur = conn.execute('SELECT meta, date, type, values FROM custom')
        names = [d[0] for d in cur.description]
        self.assertEqual(names, ['meta', 'date', 'type', 'values'])

    def test_wildcard_excludes_meta(self):
        conn = load("""
          2024-01-01 custom "budget" "expenses" 500.00 USD
        """)
        cur = conn.execute('SELECT * FROM custom')
        names = [d[0] for d in cur.description]
        self.assertEqual(names, ['date', 'type', 'values'])

    def test_select_rows(self):
        conn = load("""
          2024-01-01 custom "budget" "expenses" "monthly" 500.00 USD
          2024-02-01 custom "budget" "income" 1000.00 USD
          2024-03-01 custom "fiscal-year-end" 2024-12-31
        """)
        cur = conn.execute('SELECT date, type, values FROM custom ORDER BY date')
        rows = cur.fetchall()
        self.assertEqual(len(rows), 3)

        self.assertEqual(rows[0][0], datetime.date(2024, 1, 1))
        self.assertEqual(rows[0][1], 'budget')
        self.assertIsInstance(rows[0][2], list)
        self.assertEqual([v.value for v in rows[0][2]],
                         ['expenses', 'monthly', amount.Amount.from_string('500.00 USD')])

        self.assertEqual(rows[1][1], 'budget')
        self.assertEqual(len(rows[1][2]), 2)

        self.assertEqual(rows[2][1], 'fiscal-year-end')
        self.assertEqual([v.value for v in rows[2][2]], [datetime.date(2024, 12, 31)])

    def test_filter_by_type(self):
        conn = load("""
          2024-01-01 custom "budget" "expenses" 500.00 USD
          2024-02-01 custom "note" "hello"
          2024-03-01 custom "budget" "income" 1000.00 USD
        """)
        cur = conn.execute("SELECT date FROM custom WHERE type = 'budget' ORDER BY date")
        rows = cur.fetchall()
        self.assertEqual([r[0] for r in rows],
                         [datetime.date(2024, 1, 1), datetime.date(2024, 3, 1)])

    def test_meta_access(self):
        conn = load("""
          2024-01-01 custom "budget" "expenses" 500.00 USD
        """)
        cur = conn.execute('SELECT meta FROM custom')
        meta = cur.fetchone()[0]
        self.assertIn('filename', meta)
        self.assertIn('lineno', meta)

    def test_empty(self):
        conn = load("""
          2024-01-01 open Assets:Cash USD
        """)
        cur = conn.execute('SELECT * FROM custom')
        self.assertEqual(cur.fetchall(), [])


if __name__ == '__main__':
    unittest.main()
