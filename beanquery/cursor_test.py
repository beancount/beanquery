import unittest
import sqlite3

import beanquery
from beanquery.tests import tables


class APITests:
    def test_description(self):
        curs = self.conn.cursor()
        self.assertIsNone(curs.description)
        curs.execute(f'SELECT x FROM {self.table} WHERE x = 0')
        self.assertEqual([c[0] for c in curs.description], ['x'])
        column = curs.description[0]
        self.assertEqual(len(column), 7)

    def test_cursor_not_initialized(self):
        curs = self.conn.cursor()
        self.assertIsNone(curs.fetchone())
        self.assertEqual(curs.fetchmany(), [])
        self.assertEqual(curs.fetchall(), [])

    def test_cursor_fetchone(self):
        curs = self.conn.cursor()
        curs.execute(f'SELECT x FROM {self.table} WHERE x < 2')
        row = curs.fetchone()
        self.assertEqual(row, (0, ))
        row = curs.fetchone()
        self.assertEqual(row, (1, ))
        row = curs.fetchone()
        self.assertIsNone(row)

    def test_cursor_fetchall(self):
        curs = self.conn.cursor()
        curs.execute(f'SELECT x FROM {self.table} WHERE x < 2')
        rows = curs.fetchall()
        self.assertEqual(rows, [(0, ), (1, )])
        rows = curs.fetchall()
        self.assertEqual(rows, [])

    def test_cursor_fethmany(self):
        curs = self.conn.cursor()
        curs.execute(f'SELECT x FROM {self.table} WHERE x < 2')
        rows = curs.fetchmany()
        self.assertEqual(rows, [(0, )])
        rows = curs.fetchmany()
        self.assertEqual(rows, [(1, )])
        rows = curs.fetchmany()
        self.assertEqual(rows, [])

    def test_cursor_iterator(self):
        curs = self.conn.cursor()
        o = object()
        row = next(iter(curs), o)
        self.assertIs(row, o)
        curs = self.conn.cursor()
        curs.execute(f'SELECT x FROM {self.table} WHERE x < 2')
        iterator = iter(curs)
        row = next(iterator)
        self.assertEqual(row, (0, ))
        row = next(iterator)
        self.assertEqual(row, (1, ))
        row = next(iterator, o)
        self.assertIs(row, o)


class TestSQLite(APITests, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.table = 'test'
        cls.conn = sqlite3.connect(':memory:')
        curs = cls.conn.cursor()
        curs.execute('CREATE TABLE test (x int)')
        curs.executemany('INSERT INTO test VALUES (?)', [(i, ) for i in range(16)])


class TestBeanquery(APITests, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.table = '#test'
        cls.conn = beanquery.Connection()
        cls.conn.tables['test'] = tables.TestTable(0, 16)
