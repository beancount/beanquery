import importlib

from urllib.parse import urlparse

from . import parser
from . import compiler
from . import tables
from . import cursor

from .parser import ParseError  # noqa: F401
from .compiler import CompilationError  # noqa: F401


__version__ = '0.1.dev0'


def connect(dsn=None):
    return Connection(dsn)


class Connection:
    def __init__(self, dsn=None):
        self.tables = {None: tables.NullTable()}
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
        return cursor.Cursor(self)
