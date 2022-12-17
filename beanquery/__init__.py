import importlib

from urllib.parse import urlparse

from . import parser
from . import query_compile
from . import query_execute
from . import tables

from .parser import ParseError  # noqa: F401
from .query_compile import CompilationError  # noqa: F401


def connect(uri):
    return Connection(uri)


class Connection:
    def __init__(self, uri=None):
        self.tables = {None: tables.NullTable()}
        self.options = {}
        self.errors = []
        if uri is not None:
            self.attach(uri)

    def attach(self, uri):
        scheme = urlparse(uri).scheme
        source = importlib.import_module(f'beanquery.sources.{scheme}')
        source.attach(self, uri)

    def parse(self, statement):
        return parser.parse(statement)

    def compile(self, statement):
        return query_compile.compile(self, statement)

    def execute(self, statement):
        if not isinstance(statement, parser.ast.Node):
            statement = parser.parse(statement)
        query = query_compile.compile(self, statement)
        return query_execute.execute_query(query)
