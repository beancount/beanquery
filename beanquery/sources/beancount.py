import typing

from urllib.parse import urlparse

from beancount import loader
from beancount import parser
from beancount.core import data
from beancount.core import position
from beancount.core import prices
from beancount.core.getters import get_account_open_close, get_commodity_directives

from beanquery import tables
from beanquery import types
from beanquery import query_compile
from beanquery import query_env
from beanquery import query_render


TABLES = [query_env.EntriesTable, query_env.PostingsTable]


def attach(context, dsn, entries=None, errors=None, options=None):
    filename = urlparse(dsn).path
    if filename:
        entries, errors, options = loader.load_file(filename)
    for table in TABLES:
        context.tables[table.name] = table(entries, options)
    context.options.update(options)
    context.errors.extend(errors)


class Metadata(dict):
    pass


class MetadataRenderer(query_render.ObjectRenderer):
    dtype = Metadata

    def format(self, value):
        return str({k: v for k, v in value.items() if k not in {'filename', 'lineno'} and not k.startswith('__')})


class GetAttrColumn(query_compile.EvalColumn):
    def __init__(self, name, dtype):
        super().__init__(dtype)
        self.name = name

    def __call__(self, context):
        return getattr(context, self.name)


def _typed_namedtuple_to_columns(cls, renames=None):
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
        colname = renames.get(name, name) if renames is not None else name
        columns[colname] = GetAttrColumn(name, dtype)
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


class Transaction(types.Structure):
    name = 'transaction'
    columns = _typed_namedtuple_to_columns(data.Transaction)
    del columns['postings']


types.ALIASES[position.Position] = Position
types.ALIASES[data.Cost] = Cost
types.ALIASES[data.Amount] = Amount
types.ALIASES[data.Transaction] = Transaction


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


class TransactionsTable(Table):
    name = 'transactions'
    datatype = data.Transaction
    # There is not a way to inherit the __init_subclass__() and
    # __iter__() methods while inheriting the columns attribute
    # definition from the Transaction class.
    columns = Transaction.columns


class PricesTable(Table):
    name = 'prices'
    datatype = data.Price
    columns = _typed_namedtuple_to_columns(datatype)

    def __init__(self, entries, options):
        super().__init__(entries, options)
        self.price_map = prices.build_price_map(entries)


class BalancesTable(Table):
    name = 'balances'
    datatype = data.Balance
    columns = _typed_namedtuple_to_columns(datatype, {'diff_amount': 'discrepancy'})


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

    def __call__(self, row):
        return row[self.key]


class AccountsTable(tables.Table):
    name = 'accounts'
    columns = {
        'account': GetItemColumn(0, str),
        'open': GetItemColumn(1, Open),
        'close': GetItemColumn(2, Close),
    }

    def __init__(self, entries, options):
        self.accounts = get_account_open_close(entries)
        self.types = parser.options.get_account_types(options)

    def __iter__(self):
        return ((name, value[0], value[1]) for name, value in self.accounts.items())

TABLES.append(AccountsTable)


class CommoditiesTable(tables.Table):
    name = 'commodities'
    columns = _typed_namedtuple_to_columns(data.Commodity, {'currency': 'name'})

    def __init__(self, entries, options):
        self.commodities = get_commodity_directives(entries)

    def __iter__(self):
        return iter(self.commodities.values())

TABLES.append(CommoditiesTable)
