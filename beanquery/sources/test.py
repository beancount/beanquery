import re
from urllib.parse import urlparse, parse_qsl

from ..query_compile import EvalColumn
from .. import tables


class Column(EvalColumn):
    def __init__(self, datatype, func):
        super().__init__(datatype)
        self.func = func

    def __call__(self, row):
        return self.func(row)


class TableMeta(type):
    def __new__(mcs, name, bases, dct):
        columns = {}
        members = {}
        for key, value in dct.items():
            if isinstance(value, EvalColumn):
                value.name = key
                columns[key] = value
                continue
            members[key] = value
        assert 'columns' not in members
        members['columns'] = columns
        return super().__new__(mcs, name, bases, members)


class Table(tables.Table, metaclass=TableMeta):
    x = Column(int, lambda row: row)

    def __init__(self, *args):
        self.rows = range(*args)

    def __iter__(self):
        return iter(self.rows)


class MagicColumnsRegistry(dict):
    def get(self, key, default=None):
        if re.fullmatch(r'x+', key):
            return Column(int, lambda x: x * len(key))
        return default


class MagicTable(tables.Table):
    columns = MagicColumnsRegistry()

    def __init__(self, *args):
        self.rows = range(*args)

    def __iter__(self):
        return iter(self.rows)


def attach(context, dsn):
    parts = urlparse(dsn)
    params = dict(parse_qsl(parts.query, strict_parsing=True))
    name = params.get('name', 'test')
    cls = MagicTable if parts.path == 'magic' else Table
    context.tables[name] = cls(int(params.get('start', 0)), int(params.get('stop', 11)), int(params.get('step', 1)))
