import copy
import datetime
import sys
import types as _types
import typing

from decimal import Decimal
from functools import lru_cache as cache
from urllib.parse import urlparse

from beancount import loader
from beancount import parser
from beancount.core import amount
from beancount.core import convert
from beancount.core import data
from beancount.core import getters
from beancount.core import inventory
from beancount.core import position
from beancount.core import prices
from beancount.core.compare import hash_entry
from beancount.core.getters import get_account_open_close, get_commodity_directives
from beancount.ops import summarize

from beanquery import tables
from beanquery import types
from beanquery import query_compile
from beanquery import query_render
from beanquery import hashable
from beanquery.query_env import ColumnsRegistry


if sys.version_info >= (3, 10):
    _UNIONTYPES = {typing.Union, _types.UnionType}
else:
    _UNIONTYPES = {typing.Union}


hashable.register(data.Transaction, lambda t: (t.date, t.flag, t.narration))


_TABLES = []


def attach(context, dsn, *, entries=None, errors=None, options=None):
    filename = urlparse(dsn).path
    if filename:
        entries, errors, options = loader.load_file(filename)
    for table in _TABLES:
        context.tables[table.name] = table(entries, options)
    context.options.update(options)
    context.errors.extend(errors)
    # Set the default table. This eventually will have to be removed.
    context.tables[None] = context.tables['postings']


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


def _simplify_typing_annotation(dtype):
    if typing.get_origin(dtype) in _UNIONTYPES:
        args = typing.get_args(dtype)
        if len(args) == 2:
            if args[0] is type(None):
                return args[1], True
            if args[1] is type(None):
                return args[0], True
        raise NotImplementedError
    return typing.get_origin(dtype) or dtype, False


def _typed_namedtuple_to_columns(cls, renames=None):
    columns = {}
    for name, dtype in typing.get_type_hints(cls).items():
        dtype, nullable = _simplify_typing_annotation(dtype)
        if name == 'meta' and dtype is dict:
            dtype = Metadata
        if dtype is frozenset:
            dtype = set
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
    columns = ColumnsRegistry(_typed_namedtuple_to_columns(data.Transaction))
    del columns['postings']

    @columns.register(typing.Set[str])
    def accounts(row):
        return {p.account for p in row.postings}


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
        _TABLES.append(cls)

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

_TABLES.append(AccountsTable)


class CommoditiesTable(tables.Table):
    name = 'commodities'
    columns = _typed_namedtuple_to_columns(data.Commodity, {'currency': 'name'})

    def __init__(self, entries, options):
        self.commodities = get_commodity_directives(entries)

    def __iter__(self):
        return iter(self.commodities.values())

_TABLES.append(CommoditiesTable)


class _BeancountTable(tables.Table):
    def __init__(self, entries, options, open=None, close=None, clear=None):
        super().__init__()
        self.entries = entries
        self.options = options
        self.open = open
        self.close = close
        self.clear = clear

    def evolve(self, **kwargs):
        table = copy.copy(self)
        for name, value in kwargs.items():
            setattr(table, name, value)
        return table

    def prepare(self):
        """Filter the entries applying the FROM clause qualifiers OPEN, CLOSE, CLEAR."""
        entries = self.entries
        options = self.options

        # Process the OPEN clause.
        if self.open is not None:
            entries, index = summarize.open_opt(entries, self.open, options)

        # Process the CLOSE clause.
        if self.close is not None:
            if isinstance(self.close, datetime.date):
                entries, index = summarize.close_opt(entries, self.close, options)
            elif self.close is True:
                entries, index = summarize.close_opt(entries, None, options)

        # Process the CLEAR clause.
        if self.clear is not None:
            entries, index = summarize.clear_opt(entries, None, options)

        return entries


class EntriesTable(_BeancountTable):
    name = 'entries'
    columns = ColumnsRegistry()

    def __iter__(self):
        entries = self.prepare()
        yield from iter(entries)

    @columns.register(str)
    def id(entry):
        """Unique id of a directive."""
        return hash_entry(entry)

    @columns.register(str)
    def type(entry):
        """The data type of the directive."""
        return type(entry).__name__.lower()

    @columns.register(str)
    def filename(entry):
        """The filename where the directive was parsed from or created."""
        return entry.meta["filename"]

    @columns.register(int)
    def lineno(entry):
        """The line number from the file the directive was parsed from."""
        return entry.meta["lineno"]

    @columns.register(datetime.date)
    def date(entry):
        """The date of the directive."""
        return entry.date

    @columns.register(int)
    def year(entry):
        """The year of the date year of the directive."""
        return entry.date.year

    @columns.register(int)
    def month(entry):
        """The year of the date month of the directive."""
        return entry.date.month

    @columns.register(int)
    def day(entry):
        """The year of the date day of the directive."""
        return entry.date.day

    @columns.register(str)
    def flag(entry):
        """The flag the transaction."""
        if not isinstance(entry, data.Transaction):
            return None
        return entry.flag

    @columns.register(str)
    def payee(entry):
        """The payee of the transaction."""
        if not isinstance(entry, data.Transaction):
            return None
        return entry.payee

    @columns.register(str)
    def narration(entry):
        """The narration of the transaction."""
        if not isinstance(entry, data.Transaction):
            return None
        return entry.narration

    @columns.register(str)
    def description(entry):
        """A combination of the payee + narration of the transaction, if present."""
        if not isinstance(entry, data.Transaction):
            return None
        return ' | '.join(filter(None, [entry.payee, entry.narration]))

    @columns.register(set)
    def tags(entry):
        """The set of tags of the transaction."""
        return getattr(entry, 'tags', None)

    @columns.register(set)
    def links(entry):
        """The set of links of the transaction."""
        return getattr(entry, 'links', None)

    @columns.register(dict)
    def meta(entry):
        return entry.meta

    @columns.register(typing.Set[str])
    def accounts(entry):
        return getters.get_entry_accounts(entry)

_TABLES.append(EntriesTable)


class _PostingsTableRow:
    """A dumb container for information used by a row expression."""

    def __init__(self):
        self.rowid = 0
        self.balance = inventory.Inventory()

        # The current transaction of the posting being evaluated.
        self.entry = None

        # The current posting being evaluated.
        self.posting = None

    def __hash__(self):
        # The context hash is used in caching column accessor functions.
        # Instead than hashing the row context content, use the rowid as
        # hash.
        return self.rowid


class PostingsTable(_BeancountTable):
    name = 'postings'
    columns = ColumnsRegistry()
    wildcard_columns = 'date flag payee narration position'.split()

    def __iter__(self):
        entries = self.prepare()
        context = _PostingsTableRow()
        for entry in entries:
            if isinstance(entry, data.Transaction):
                context.entry = entry
                for posting in entry.postings:
                    context.rowid += 1
                    context.posting = posting
                    yield context

    @columns.register(str)
    def type(context):
        return 'transaction'

    @columns.register(str)
    def id(context):
        """Unique id of a directive."""
        return hash_entry(context.entry)

    @columns.register(datetime.date)
    def date(context):
        """The date of the directive."""
        return context.entry.date

    @columns.register(int)
    def year(context):
        """The year of the date year of the directive."""
        return context.entry.date.year

    @columns.register(int)
    def month(context):
        """The year of the date month of the directive."""
        return context.entry.date.month

    @columns.register(int)
    def day(context):
        """The year of the date day of the directive."""
        return context.entry.date.day

    @columns.register(str)
    def filename(context):
        """The ledger where the posting is defined."""
        meta = context.posting.meta
        # Postings for pad transactions have their meta fields set to
        # None. See https://github.com/beancount/beancount/issues/767
        if meta is None:
            return None
        return meta["filename"]

    @columns.register(int)
    def lineno(context):
        """The line number in the ledger file where the posting is defined."""
        meta = context.posting.meta
        # Postings for pad transactions have their meta fields set to
        # None. See https://github.com/beancount/beancount/issues/767
        if meta is None:
            return None
        return meta["lineno"]

    @columns.register(str)
    def location(context):
        """The filename:lineno location where the posting is defined."""
        meta = context.posting.meta
        # Postings for pad transactions have their meta fields set to
        # None. See https://github.com/beancount/beancount/issues/767
        if meta is None:
            return None
        return '{:s}:{:d}:'.format(meta['filename'], meta['lineno'])

    @columns.register(str)
    def flag(context):
        """The flag of the parent transaction for this posting."""
        return context.entry.flag

    @columns.register(str)
    def payee(context):
        """The payee of the parent transaction for this posting."""
        return context.entry.payee

    @columns.register(str)
    def narration(context):
        """The narration of the parent transaction for this posting."""
        return context.entry.narration

    @columns.register(str)
    def description(context):
        """A combination of the payee + narration for the transaction of this posting."""
        return ' | '.join(filter(None, [context.entry.payee, context.entry.narration]))

    @columns.register(set)
    def tags(context):
        """The set of tags of the parent transaction for this posting."""
        return context.entry.tags

    @columns.register(set)
    def links(context):
        """The set of links of the parent transaction for this posting."""
        return context.entry.links

    @columns.register(str)
    def posting_flag(context):
        """The flag of the posting itself."""
        return context.posting.flag

    @columns.register(str)
    def account(context):
        """The account of the posting."""
        return context.posting.account

    @columns.register(set)
    def other_accounts(context):
        """The list of other accounts in the transaction, excluding that of this posting."""
        return sorted({posting.account for posting in context.entry.postings if posting is not context.posting})

    @columns.register(Decimal)
    def number(context):
        """The number of units of the posting."""
        return context.posting.units.number

    @columns.register(str)
    def currency(context):
        """The currency of the posting."""
        return context.posting.units.currency

    @columns.register(Decimal)
    def cost_number(context):
        """The number of cost units of the posting."""
        cost = context.posting.cost
        return cost.number if cost else None

    @columns.register(str)
    def cost_currency(context):
        """The cost currency of the posting."""
        cost = context.posting.cost
        return cost.currency if cost else None

    @columns.register(datetime.date)
    def cost_date(context):
        """The cost currency of the posting."""
        cost = context.posting.cost
        return cost.date if cost else None

    @columns.register(str)
    def cost_label(context):
        """The cost currency of the posting."""
        cost = context.posting.cost
        return cost.label if cost else ''

    @columns.register(position.Position)
    def position(context):
        """The position for the posting. These can be summed into inventories."""
        posting = context.posting
        return position.Position(posting.units, posting.cost)

    @columns.register(amount.Amount)
    def price(context):
        """The price attached to the posting."""
        return context.posting.price

    @columns.register(amount.Amount)
    def weight(context):
        """The computed weight used for this posting."""
        return convert.get_weight(context.posting)

    @columns.register(inventory.Inventory)
    @cache(maxsize=1)  # noqa: B019
    def balance(context):
        """The balance for the posting. These can be summed into inventories."""
        # Caching protects against multiple balance updates per row when
        # the columns appears more than once in the execurted query. The
        # rowid in the row context guarantees that otherwise identical
        # rows do not hit the cache and thus that the balance is correctly
        # updated.
        context.balance.add_position(context.posting)
        return copy.copy(context.balance)

    @columns.register(dict)
    def meta(context):
        return context.posting.meta

    @columns.register(data.Transaction)
    def entry(context):
        return context.entry

    @columns.register(typing.Set[str])
    def accounts(context):
        return {p.account for p in context.entry.postings}

_TABLES.append(PostingsTable)
