import importlib

from urllib.parse import urlparse

from . import parser
from . import compiler
from . import tables

from .compiler import CompilationError
from .cursor import Cursor, Column
from .errors import Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError
from .errors import IntegrityError, InternalError, ProgrammingError, NotSupportedError
from .parser import ParseError


__version__ = '0.1.dev0'


def connect(dsn=None):
    return Connection(dsn)


class Connection:
    def __init__(self, dsn=None):
        self.tables = {'': tables.NullTable()}
        self.options = {}
        self.errors = []
        if dsn is not None:
            self.attach(dsn)

    def attach(self, dsn):
        scheme = urlparse(dsn).scheme
        source = importlib.import_module(f'beanquery.sources.{scheme}')
        source.attach(self, dsn)

    def close(self):
        # Required by the DB-API.
        pass

    def parse(self, query):
        return parser.parse(query)

    def compile(self, query):
        return compiler.compile(self, query)

    def execute(self, query, params=None):
        return self.cursor().execute(query, params)

    def cursor(self):
        return Cursor(self)


__all__ = [
    'Column',
    'CompilationError',
    'Connection',
    'Cursor',
    'DataError',
    'DatabaseError',
    'Error',
    'IntegrityError',
    'InterfaceError',
    'InternalError',
    'NotSupportedError',
    'OperationalError',
    'ParseError',
    'ProgrammingError',
    'Warning',
    'connet',
]
