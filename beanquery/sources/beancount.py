import typing

from urllib.parse import urlparse

from beancount import loader
from beancount.core import data
from beancount.core import position
from beancount.core.getters import get_account_open_close

from beanquery import tables
from beanquery import types
from beanquery import query_compile
from beanquery import query_env
from beanquery import query_render


TABLES = [query_env.EntriesTable, query_env.PostingsTable]


def add_beancount_tables(context, entries, errors, options):
    for table in TABLES:
        context.tables[table.name] = table(entries, options)
    context.options.update(options)
    context.errors.extend(errors)


def attach(context, dsn):
    filename = urlparse(dsn).path
    entries, errors, options = loader.load_file(filename)
    add_beancount_tables(context, entries, errors, options)


class Metadata(dict):
    pass


class MetadataRenderer(query_render.ObjectRenderer):
    dtype = Metadata

    def format(self, value):
        return str({k: v for k, v in value.items() if k not in {'filename', 'lineno'}})


class GetAttrColumn(query_compile.EvalColumn):
    def __init__(self, name, dtype):
        super().__init__(dtype)
        self.name = name

    def __call__(self, context, env):
        return getattr(context, self.name)


def _typed_namedtuple_to_columns(cls):
    columns = {}
    for name, dtype in typing.get_type_hints(cls).items():
        while True:
            origin = typing.get_origin(dtype)
            # Extract the underlying type from Optional[x] annotations,
            # which are effectively Union[x, None] annotations.
            if origin is typing.Union:
                args = typing.get_args(dtype)
                # Ensure that there is just one type other than None.
                dtypes = [t for t in args if t is not type(None)]
                assert len(dtypes) == 1
                dtype = dtypes[0]
            elif origin is not None:
                dtype = origin
            else:
                break
        if name == 'meta' and dtype is dict:
            dtype = Metadata
        columns[name] = GetAttrColumn(name, dtype)
    return columns


class Position(types.Structure):
    name = 'position'
    columns = _typed_namedtuple_to_columns(position.Position)


class Cost(types.Structure):
    name = 'cost'
    columns = _typed_namedtuple_to_columns(data.Cost)


class Amount(types.Structure):
    name = 'amount'
    columns = _typed_namedtuple_to_columns(data.Amount)


types.ALIASES[position.Position] = Position
types.ALIASES[data.Cost] = Cost
types.ALIASES[data.Amount] = Amount


class Open(types.Structure):
    name = 'open'
    columns = _typed_namedtuple_to_columns(data.Open)


class Close(types.Structure):
    name = 'close'
    columns = _typed_namedtuple_to_columns(data.Close)


class Table(tables.Table):
    datatype = None

    def __init__(self, entries, options):
        self.entries = entries
        self.options = options

    def __init_subclass__(cls):
        TABLES.append(cls)

    def __iter__(self):
        datatype = self.datatype
        for entry in self.entries:
            if isinstance(entry, datatype):
                yield entry

    @property
    def wildcard_columns(self):
        return tuple(col for col in self.columns.keys() if col != 'meta')


class PricesTable(Table):
    name = 'prices'
    datatype = data.Price
    columns = _typed_namedtuple_to_columns(datatype)


class BalancesTable(Table):
    name = 'balances'
    datatype = data.Balance
    columns = _typed_namedtuple_to_columns(datatype)
    columns['discrepancy'] = columns.pop('diff_amount')


class NotesTable(Table):
    name = 'notes'
    datatype = data.Note
    columns = _typed_namedtuple_to_columns(datatype)


class EventsTable(Table):
    name = 'events'
    datatype = data.Event
    columns = _typed_namedtuple_to_columns(datatype)


class DocumentsTable(Table):
    name = 'documents'
    datatype = data.Document
    columns = _typed_namedtuple_to_columns(datatype)


class GetItemColumn(query_compile.EvalColumn):
    def __init__(self, key, dtype):
        super().__init__(dtype)
        self.key = key

    def __call__(self, row, env):
        return row[self.key]


class AccountsTable(tables.Table):
    name = 'accounts'
    columns = {
        'account': GetItemColumn(0, str),
        'open': GetItemColumn(1, Open),
        'close': GetItemColumn(2, Close)
    }

    def __init__(self, entries, options):
        self.accounts = [(name, value[0], value[1]) for name, value in get_account_open_close(entries).items()]

    def __iter__(self):
        return iter(self.accounts)

TABLES.append(AccountsTable)
