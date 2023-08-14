from operator import attrgetter
from typing import Sequence

from . import types
from . import parser
from . import compiler
from . import query_execute


class Column(Sequence):
    __module__ = 'beanquery'

    def __init__(self, name, datatype):
        self._name = name
        self._type = datatype

    _vars = tuple(attrgetter(name) for name in 'name type_code display_size internal_size precision scale null_ok'.split())

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return tuple(self) == tuple(other)
        if isinstance(other, tuple):
            # Used in tests.
            return (self._name, self._type) == other
        return NotImplemented

    def __repr__(self):
        return f'{self.__module__}.{self.__class__.__name__}({self._name!r}, {types.name(self._type)})'

    def __len__(self):
        return 7

    def __getitem__(self, key):
        if isinstance(key, slice):
            return tuple(getter(self) for getter in self._vars(key))
        return self._vars[key](self)

    @property
    def name(self):
        return self._name

    @property
    def datatype(self):
        # Extension to the DB-API.
        return self._type

    @property
    def type_code(self):
        # The DB-API specification is vague on this point, but other
        # database connection libraries expose this as an int. It does
        # not make much sense to keep a mapping between int type code
        # and actual types, thus just return the hash of the type
        # object.
        return hash(self._type)

    @property
    def display_size(self):
        return None

    @property
    def internal_size(self):
        return None

    @property
    def precision(self):
        return None

    @property
    def scale(self):
        return None

    @property
    def null_ok(self):
        return None


class Cursor:
    def __init__(self, connection):
        self._context = connection
        self._description = None
        self._rows = None
        self._pos = 0
        self.arraysize = 1

    @property
    def connection(self):
        return self._context

    def execute(self, query, params=None):
        if not isinstance(query, parser.ast.Node):
            query = parser.parse(query)
        query = compiler.compile(self._context, query, params)
        description, rows = query_execute.execute_query(query)
        self._description = description
        self._rows = rows
        self._pos = 0
        return self

    def executemany(self, query, params=None):
        query = parser.parse(query)
        for p in params:
            self.execute(query, p)

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self):
        return len(self._rows) if self._rows is not None else -1

    @property
    def rownumber(self):
        return self._pos

    def fetchone(self):
        # This implementation pops items from the front of the results
        # rows list and is thus not efficient, especially for large
        # results sets.
        if self._rows is None or not len(self._rows):
            return None
        self._pos += 1
        return self._rows.pop(0)

    def fetchmany(self, size=None):
        if self._rows is None:
            return []
        n = size if size is not None else self.arraysize
        rows = self._rows[:n]
        self._rows = self._rows[n:]
        self._pos += len(rows)
        return rows

    def fetchall(self):
        if self._rows is None:
            return []
        rows = self._rows
        self._rows = []
        self._pos += len(rows)
        return rows

    def close(self):
        # Required by the DB-API.
        pass

    def setinputsizes(self, sizes):
        # Required by the DB-API.
        pass

    def setoutputsize(self, size, column=None):
        # Required by the DB-API.
        pass

    def __iter__(self):
        return iter(self._rows if self._rows is not None else [])
