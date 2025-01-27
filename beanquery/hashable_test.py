import dataclasses
import unittest

from beanquery import hashable
from beanquery.cursor import Column


class TestHashable(unittest.TestCase):

    def test_fundamental(self):
        columns = (Column('b', bool), Column('i', int), Column('s', str))
        wrap = hashable.make(columns)
        obj = (True, 42, 'universe')
        self.assertIs(wrap(obj), obj)
        hash(obj)

    def test_dict(self):
        columns = (Column('b', bool), Column('d', dict))
        wrap = hashable.make(columns)
        obja = (True, {'answer': 42})
        a = hash(wrap(obja))
        objb = (True, {'answer': 42})
        b = hash(wrap(objb))
        self.assertIsNot(obja, objb)
        self.assertEqual(a, b)
        objc = (False, {'answer': 42})
        c = hash(wrap(objc))
        self.assertNotEqual(a, c)
        objd = (True, {'answer': 43})
        d = hash(wrap(objd))
        self.assertNotEqual(a, d)

    def test_registered(self):

        @dataclasses.dataclass
        class Foo:
            xid: int
            meta: dict

        hashable.register(Foo, lambda obj: obj.xid)

        columns = (Column('b', bool), Column('foo', Foo))
        wrap = hashable.make(columns)
        obja = (True, Foo(1, {'test': 1}))
        a = hash(wrap(obja))
        objb = (True, Foo(1, {'test': 2}))
        b = hash(wrap(objb))
        self.assertEqual(a, b)
        objc = (True, Foo(2, {'test': 2}))
        c = hash(wrap(objc))
        self.assertNotEqual(a, c)
