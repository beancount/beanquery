import unittest

from beanquery import types
from beanquery.sources import beancount


class TestName(unittest.TestCase):
    def test_transactions_names(self):
        table = beancount.TransactionsTable
        columns = {name: types.name(column.dtype) for name, column in table.columns.items()}
        self.assertEqual(columns, {
            'meta': 'metadata',
            'date': 'date',
            'flag': 'str',
            'payee': 'str',
            'narration': 'str',
            'tags': 'set',
            'links': 'set',
            'accounts': 'set[str]',
        })
