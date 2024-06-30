"""Environment object for compiler.

This module contains the various column accessors and function evaluators that
are made available by the query compiler via their compilation context objects.
Define new columns and functions here.
"""
__copyright__ = "Copyright (C) 2014-2017  Martin Blais"
__license__ = "GNU GPLv2"

import copy
import datetime
import decimal
import re
import textwrap

from functools import lru_cache as cache
from decimal import Decimal

import dateutil.parser
from dateutil.relativedelta import relativedelta, weekday

from beancount.core.number import ZERO
from beancount.core.compare import hash_entry
from beancount.core import amount
from beancount.core import position
from beancount.core import inventory
from beancount.core import account
from beancount.core import data
from beancount.core import getters
from beancount.core import convert
from beancount.core import prices
from beancount.core.account_types import get_account_sign, get_account_sort_key
from beancount.ops import summarize

from beanquery import query_compile
from beanquery import tables
from beanquery import types


def function(intypes, outtype, pass_row=False, pass_context=None, name=None):
    assert not (pass_row and pass_context)
    def decorator(func):
        class Func(query_compile.EvalFunction):
            __intypes__ = intypes
            pure = not pass_row and not pass_context
            def __init__(self, context, operands):
                super().__init__(context, operands, outtype)
            def __call__(self, row):
                args = [operand(row) for operand in self.operands]
                for arg in args:
                    if arg is None:
                        return None
                if pass_row:
                    return func(row, *args)
                if pass_context:
                    return func(self.context, *args)
                return func(*args)
        Func.__name__ = name if name is not None else func.__name__
        Func.__doc__ = func.__doc__
        query_compile.FUNCTIONS[Func.__name__].append(Func)
        return func
    return decorator


def register(name=None):
    def decorator(cls):
        if name is not None:
            cls.__name__ = name
        query_compile.FUNCTIONS[cls.__name__].append(cls)
        return cls
    return decorator


@register('getitem')
class GetItem2(query_compile.EvalFunction):
    __intypes__ = [dict, str]

    def __init__(self, context, operands):
        super().__init__(context, operands, object)

    def __call__(self, row):
        obj, key = self.operands
        obj = obj(row)
        if obj is None:
            return None
        return obj.get(key(row))


@register('getitem')
class GetItem3(query_compile.EvalFunction):
    __intypes__ = [dict, str, types.Any]

    def __init__(self, context, operands):
        super().__init__(context, operands, object)

    def __call__(self, row):
        obj, key, default = self.operands
        obj = obj(row)
        if obj is None:
            return None
        return obj.get(key(row), default(row))


def Function(name, args):
    func = types.function_lookup(query_compile.FUNCTIONS, name, args)
    if func is not None:
        return func(None, args)
    raise KeyError


## Type casting


@function([types.Any], bool, name='bool')
def bool_(x):
    """Convert to bool value."""
    return bool(x)


@function([int], int, name='int')
@function([bool], int, name='int')
@function([Decimal], int, name='int')
@function([str], int, name='int')
@function([object], int, name='int')
def int_(x):
    try:
        return int(x)
    except (ValueError, TypeError):
        return None


@function([Decimal], Decimal, name='decimal')
@function([int], Decimal, name='decimal')
@function([bool], Decimal, name='decimal')
@function([str], Decimal, name='decimal')
@function([object], Decimal, name='decimal')
def decimal_(x):
    try:
        return Decimal(x)
    except (ValueError, TypeError, decimal.InvalidOperation):
        return None


@function([types.Any], str, name='str')
def str_(x):
    if x is True:
        return 'TRUE'
    if x is False:
        return 'FALSE'
    return str(x)


@function([datetime.date], datetime.date, name='date')
@function([str], datetime.date, name='date')
@function([object], datetime.date, name='date')
def date_(x):
    if isinstance(x, datetime.date):
        return x
    if isinstance(x, str):
        try:
            return datetime.datetime.strptime(x, '%Y-%m-%d').date()
        except ValueError:
            pass
    return None


@function([int, int, int], datetime.date, name='date')
def date_from_ymd(year, month, day):
    """Construct a date with year, month, day arguments."""
    try:
        return datetime.date(year, month, day)
    except ValueError:
        return None


## Functions


@function([Decimal], Decimal)
@function([amount.Amount], amount.Amount)
@function([position.Position], position.Position)
@function([inventory.Inventory], inventory.Inventory)
def neg(x):
    """Negative value."""
    return -x


@function([Decimal], Decimal, name='abs')
@function([position.Position], position.Position, name='abs')
@function([inventory.Inventory], inventory.Inventory, name='abs')
def abs_(x):
    """Absolute value."""
    return abs(x)


@function([Decimal, Decimal], Decimal)
@function([Decimal, int], Decimal)
def safediv(x, y):
    """A division operation that traps division by zero exceptions and outputs zero instead."""
    if y == 0:
        return ZERO
    return x / y


@function([Decimal], Decimal, name='round')
@function([Decimal, int], Decimal, name='round')
@function([int], int, name='round')
@function([int, int], int, name='round')
def round_(num, digits=0):
    """Round the argument."""
    return round(num, digits)


@function([list], int)
@function([set], int)
@function([str], int)
def length(x):
    """Compute the length of the argument. This works on sequences."""
    return len(x)


@function([types.Any], str, name='repr')
def repr_(x):
    """Convert the argument to a string via repr()."""
    return repr(x)


@function([str, int], str)
def maxwidth(x, n):
    """Convert the argument to a substring. This can be used to ensure
    maximum width. This will insert ellipsis ([...]) if necessary."""
    return textwrap.shorten(x, width=n)


@function([str, int, int], str)
def substr(string, start, end):
    """Extract a substring of the argument."""
    return string[start:end]


@function([str, str, int], str)
def splitcomp(string, delim, index):
    """Split a string and extract one of its components."""
    return string.split(delim)[index]


# Operations on dates.

@function([datetime.date], int)
def year(x):
    """Extract the year from a date."""
    return x.year


@function([datetime.date], int)
def month(x):
    """Extract the month from a date."""
    return x.month


@function([datetime.date], int)
def day(x):
    """Extract the day from a date."""
    return x.day


@function([datetime.date], datetime.date)
def yearmonth(x):
    """Extract the year and month from a date."""
    return datetime.date(x.year, x.month, 1)


@function([datetime.date], str)
def quarter(x):
    """Extract the quarter from a date."""
    return '{:04d}-Q{:1d}'.format(x.year, (x.month - 1) // 3 + 1)


@function([datetime.date], str, name='weekday')
def weekday_(x):
    """Extract a 3-letter weekday from a date."""
    return x.strftime('%a')


@function([], datetime.date)
def today():
    """Today's date"""
    return datetime.date.today()


# Operations on accounts.

@function([str], str)
@function([str, int], str)
def root(acc, n=1):
    """Get the root name(s) of the account."""
    return account.root(n, acc)


@function([str], str)
def parent(acc):
    """Get the parent name of the account."""
    return account.parent(acc)


@function([str], str)
def leaf(acc):
    """Get the name of the leaf subaccount."""
    return account.leaf(acc)


@function([str, str], str)
def grep(pattern, string):
    """Match a regular expression against a string and return only the matched portion."""
    match = re.search(pattern, string)
    if match:
        return match.group(0)
    return None


@function([str, str, int], str)
def grepn(pattern, string, n):
    """Match a pattern with subgroups against a string and return the subgroup at the index."""
    match = re.search(pattern, string)
    if match:
        return match.group(n)
    return None


@function([str, str, str], str)
def subst(pattern, repl, string):
    """Substitute leftmost non-overlapping occurrences of pattern by replacement."""
    return re.sub(pattern, repl, string)


@function([str], str)
def upper(string):
    """Convert string to uppercase."""
    return string.upper()


@function([str], str)
def lower(string):
    """Convert string to lowercase."""
    return string.lower()


# Avoid building a tuple for each function invocation.
NONENONE = None, None


@function([str], datetime.date, pass_context=True)
def open_date(context, acc):
    """Get the date of the open directive of the account."""
    open_entry, _ = context.tables['accounts'].accounts.get(acc, NONENONE)
    if open_entry is None:
        return None
    return open_entry.date


@function([str], datetime.date, pass_context=True)
def close_date(context, acc):
    """Get the date of the close directive of the account."""
    _, close_entry = context.tables['accounts'].accounts.get(acc, NONENONE)
    if close_entry is None:
        return None
    return close_entry.date


@function([str], dict, pass_context=True)
@function([str, str], object, pass_context=True)
def open_meta(context, account, key=None):
    """Get the metadata dict of the open directive of the account."""
    open_entry, _ = context.tables['accounts'].accounts.get(account, NONENONE)
    if open_entry is None:
        return None
    if key is None:
        return open_entry.meta
    return open_entry.meta.get(key)


# Stub kept only for function type checking and for generating documentation.
@function([str], object)
def meta(context, key):
    """Get some metadata key of the posting."""
    raise NotImplementedError


# Stub kept only for function type checking and for generating documentation.
@function([str], object)
def entry_meta(context, key):
    """Get some metadata key of the transaction."""
    raise NotImplementedError


# Stub kept only for function type checking and for generating documentation.
@function([str], object, pass_row=True)
def any_meta(context, key):
    """Get metadata from the posting or its parent transaction if not present."""
    raise NotImplementedError


@function([str], dict, pass_context=True)
@function([str, str], object, pass_context=True)
@function([str], dict, pass_context=True, name='commodity_meta')
@function([str, str], object, pass_context=True, name='commodity_meta')
def currency_meta(context, commodity, key=None):
    """Get the metadata dict of the commodity directive of the currency."""
    entry = context.tables['commodities'].commodities.get(commodity)
    if entry is None:
        return None
    if key is None:
        return entry.meta
    return entry.meta.get(key)


@function([str], str, pass_context=True)
def account_sortkey(context, acc):
    """Get a string to sort accounts in order taking into account the types."""
    account_types = context.tables['accounts'].types
    index, name = get_account_sort_key(account_types, acc)
    return '{}-{}'.format(index, name)


@function([str], bool, pass_row=True)
def has_account(context, pattern):
    """True if the transaction has at least one posting matching the regular expression argument."""
    search = re.compile(pattern, re.IGNORECASE).search
    return any(search(account) for account in getters.get_entry_accounts(context.entry))


# Note: Don't provide this, because polymorphic multiplication on Amount,
# Position, Inventory isn't supported yet.
#
# class AccountSign(query_compile.EvalFunction):
#     "Produce a +1 / -1 signed value to multiply with to correct balances."
#     __intypes__ = [str]
#
#     def __init__(self, operands):
#         super().__init__(operands, Decimal)
#
#     def __call__(self, context):
#         args = self.eval_args(context)
#         return Decimal(account_types.get_account_sign(args[0], context.account_types))

# Operation on inventories, positions and amounts.


@function([position.Position], amount.Amount, name='units')
def position_units(pos):
    """Get the number of units of a position (stripping cost)."""
    return convert.get_units(pos)


@function([inventory.Inventory], inventory.Inventory, name='units')
def inventory_units(inv):
    """Get the number of units of an inventory (stripping cost)."""
    return inv.reduce(convert.get_units)


@function([position.Position], amount.Amount, name='cost')
def position_cost(pos):
    """Get the cost of a position."""
    return convert.get_cost(pos)


@function([inventory.Inventory], inventory.Inventory, name='cost')
def inventory_cost(inv):
    """Get the cost of an inventory."""
    return inv.reduce(convert.get_cost)


@function([amount.Amount, str], amount.Amount, pass_context=True, name='convert')
@function([amount.Amount, str, datetime.date], amount.Amount, pass_context=True, name='convert')
def convert_amount(context, amount_, currency, date=None):
    """Coerce an amount to a particular currency."""
    price_map = context.tables['prices'].price_map
    return convert.convert_amount(amount_, currency, price_map, date)


@function([position.Position, str], amount.Amount, pass_context=True, name='convert')
@function([position.Position, str, datetime.date], amount.Amount, pass_context=True, name='convert')
def convert_position(context, pos, currency, date=None):
    """Coerce an amount to a particular currency."""
    price_map = context.tables['prices'].price_map
    return convert.convert_position(pos, currency, price_map, date)


@function([inventory.Inventory, str], inventory.Inventory, pass_context=True, name='convert')
@function([inventory.Inventory, str, datetime.date], inventory.Inventory, pass_context=True, name='convert')
def convert_inventory(context, inv, currency, date=None):
    """Coerce an inventory to a particular currency."""
    price_map = context.tables['prices'].price_map
    return inv.reduce(convert.convert_position, currency, price_map, date)


@function([position.Position], amount.Amount, pass_context=True, name='value')
@function([position.Position, datetime.date], amount.Amount, pass_context=True, name='value')
def position_value(context, pos, date=None):
    """Convert a position to its cost currency at the market value."""
    price_map = context.tables['prices'].price_map
    return convert.get_value(pos, price_map, date)


@function([inventory.Inventory], inventory.Inventory, pass_context=True, name='value')
@function([inventory.Inventory, datetime.date], inventory.Inventory, pass_context=True, name='value')
def inventory_value(context, inv, date=None):
    """Coerce an inventory to its market value."""
    price_map = context.tables['prices'].price_map
    return inv.reduce(convert.get_value, price_map, date)


@function([str, str], Decimal, pass_context=True)
@function([str, str, datetime.date], Decimal, pass_context=True, name='getprice')
def getprice(context, base, quote, date=None):
    """Fetch a price."""
    price_map = context.tables['prices'].price_map
    pair = (base.upper(), quote.upper())
    _, price = prices.get_price(price_map, pair, date)
    return price


@function([amount.Amount], Decimal)
def number(x):
    """Extract the number from an Amount."""
    return x.number


@function([amount.Amount], str)
@function([amount.Amount], str, name='commodity')
def currency(x):
    """Extract the currency from an Amount."""
    return x.currency


@function([str, set], str)
def findfirst(pattern, values):
    """Filter a string sequence by regular expression and return the first match."""
    if not values:
        return None
    for value in sorted(values):
        if re.match(pattern, value):
            return value
    return None


@function([set], str)
def joinstr(values):
    """Join a sequence of strings to a single comma-separated string."""
    return ','.join(values)


@function([str, inventory.Inventory], amount.Amount, name='only')
def only_inventory(currency, inventory_):
    """Get one currency's amount from the inventory."""
    return inventory_.get_currency_units(currency)


@function([inventory.Inventory], bool, name='empty')
def empty_inventory(inventory_):
    """Determine whether the inventiry is empty."""
    return inventory_.is_empty()


@function([position.Position, str], position.Position, name='filter_currency')
def filter_currency_position(pos, currency):
    """Filter an inventory to just the specified currency."""
    return pos if pos.units.currency == currency else None


@function([inventory.Inventory, str], inventory.Inventory, name='filter_currency')
def filter_currency_inventory(inv, currency):
    """Filter an inventory to just the specified currency."""
    return inventory.Inventory(pos for pos in inv if pos.units.currency == currency)


@function([Decimal, str], Decimal, pass_context=True)
@function([amount.Amount, str], amount.Amount, pass_context=True)
@function([position.Position, str], position.Position, pass_context=True)
@function([inventory.Inventory, str], inventory.Inventory, pass_context=True)
def possign(context, x, account):
    """Correct sign of an Amount based on the usual balance of associated account."""
    account_types = context.tables['accounts'].types
    sign = get_account_sign(account, account_types)
    return x if sign >= 0  else -x


@function([str], datetime.date)
@function([str, str], datetime.date)
def parse_date(string, frmt=None):
    """Parse date from string."""
    if frmt is None:
        return dateutil.parser.parse(string).date()
    return datetime.datetime.strptime(string, frmt).date()


@function([datetime.date, datetime.date], int)
def date_diff(x, y):
    """Calculates the difference (in days) between two dates."""
    return (x - y).days


@function([datetime.date, int], datetime.date)
def date_add(x, y):
    """Adds/subtracts number of days from the given date."""
    return x + datetime.timedelta(days=y)


@function([str, datetime.date], datetime.date)
def date_trunc(field, x):
    """Truncate a date to the specified precision."""
    if field == 'week':
        return x - relativedelta(weekday=weekday(0, -1))
    if field == 'month':
        return datetime.date(x.year, x.month, 1)
    if field == 'quarter':
        return datetime.date(x.year, x.month - (x.month - 1) % 3, 1)
    if field == 'year':
        return datetime.date(x.year, 1, 1)
    if field == 'decade':
        return datetime.date(x.year - x.year % 10, 1, 1)
    if field == 'century':
        return datetime.date(x.year - (x.year - 1) % 100, 1, 1)
    if field == 'millennium':
        return datetime.date(x.year - (x.year - 1) % 1000, 1, 1)
    return None


@function([str, datetime.date], int)
def date_part(field, x):
    """Extract the specified field from a date."""
    if field == 'weekday' or field == 'dow':
        return x.weekday()
    if field == 'isoweekday' or field == 'isodow':
        return x.isoweekday()
    if field == 'week':
        # isocalendar() returns a named tuple only in Python >= 3.9.
        return x.isocalendar()[1]
    if field == 'month':
        return x.month
    if field == 'quarter':
        return (x.month - 1) // 3 + 1
    if field == 'year':
        return x.year
    if field == 'isoyear':
        # isocalendar() returns a named tuple only in Python >= 3.9.
        return x.isocalendar()[0]
    if field == 'decade':
        return x.year // 10
    if field == 'century':
        return (x.year - 1) // 100 + 1
    if field == 'millennium':
        return (x.year - 1) // 1000 + 1
    if field == 'epoch':
        return int((x - datetime.date(1970, 1, 1)).total_seconds())
    return None


@function([str], relativedelta)
def interval(x):
    """Construct a relative time interval."""
    m = re.fullmatch(r'([-+]?[0-9]+)\s+(day|month|year)s?', x)
    if not m:
        return None
    number = int(m.group(1))
    unit = m.group(2)
    if unit == 'day':
        return relativedelta(days=number)
    if unit == 'month':
        return relativedelta(months=number)
    if unit == 'year':
        return relativedelta(years=number)
    return None


def aggregator(intypes, name=None):
    def decorator(cls):
        cls.__intypes__ = intypes
        if name is not None:
            cls.__name__ = name
        query_compile.FUNCTIONS[cls.__name__].append(cls)
        return cls
    return decorator


@aggregator([types.Asterisk], name='count')
class Count(query_compile.EvalAggregator):
    """Count the number of input rows."""
    def __init__(self, context, operands):
        super().__init__(context, operands, int)

    def update(self, store, context):
        store[self.handle] += 1


@aggregator([types.Any], name='count')
class CountArg(query_compile.EvalAggregator):
    """Count the number of non-NULL occurrences of the argument."""
    def __init__(self, context, operands):
        super().__init__(context, operands, int)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle] += 1


@aggregator([int], name='sum')
class SumInt(query_compile.EvalAggregator):
    """Calculate the sum of the numerical argument."""
    def __init__(self, context, operands):
        super().__init__(context, operands, operands[0].dtype)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle] += value


@aggregator([Decimal], name='sum')
class SumDecimal(query_compile.EvalAggregator):
    """Calculate the sum of the numerical argument."""
    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle] += value


@aggregator([amount.Amount], name='sum')
class SumAmount(query_compile.EvalAggregator):
    """Calculate the sum of the amount. The result is an Inventory."""
    def __init__(self, context, operands):
        super().__init__(context, operands, inventory.Inventory)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle].add_amount(value)


@aggregator([position.Position], name='sum')
class SumPosition(query_compile.EvalAggregator):
    """Calculate the sum of the position. The result is an Inventory."""
    def __init__(self, context, operands):
        super().__init__(context, operands, inventory.Inventory)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle].add_position(value)


@aggregator([inventory.Inventory], name='sum')
class SumInventory(query_compile.EvalAggregator):
    """Calculate the sum of the inventories. The result is an Inventory."""
    def __init__(self, context, operands):
        super().__init__(context, operands, inventory.Inventory)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle].add_inventory(value)


@aggregator([types.Any], name='first')
class First(query_compile.EvalAggregator):
    """Keep the first of the values seen."""
    def initialize(self, store):
        store[self.handle] = None

    def update(self, store, context):
        if store[self.handle] is None:
            value = self.operands[0](context)
            store[self.handle] = value


@aggregator([types.Any], name='last')
class Last(query_compile.EvalAggregator):
    """Keep the last of the values seen."""
    def initialize(self, store):
        store[self.handle] = None

    def update(self, store, context):
        value = self.operands[0](context)
        store[self.handle] = value


@aggregator([types.Any], name='min')
class Min(query_compile.EvalAggregator):
    """Compute the minimum of the values."""
    def initialize(self, store):
        store[self.handle] = None

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            cur = store[self.handle]
            if cur is None or value < cur:
                store[self.handle] = value


@aggregator([types.Any], name='max')
class Max(query_compile.EvalAggregator):
    """Compute the maximum of the values."""
    def initialize(self, store):
        store[self.handle] = None

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            cur = store[self.handle]
            if cur is None or value > cur:
                store[self.handle] = value


class Row:
    """A dumb container for information used by a row expression."""

    rowid = None

    # The current posting being evaluated.
    posting = None

    # The current transaction of the posting being evaluated.
    entry = None

    # The context hash is used in caching column accessor functions.
    # Instead than hashing the row context content, use the rowid as
    # hash.
    def __hash__(self):
        return self.rowid

    def __init__(self, entries, options):
        self.rowid = 0
        self.balance = inventory.Inventory()


class BeanTable(tables.Table):
    def __init__(self, entries, options, open=None, close=None, clear=None):
        super().__init__()
        self.entries = entries
        self.options = options
        self.open = open
        self.close = close
        self.clear = clear

    @classmethod
    def column(cls, dtype, name=None, help=None):
        def decorator(func):
            class Col(query_compile.EvalColumn):
                def __init__(self):
                    super().__init__(dtype)
                __call__ = staticmethod(func)
            Col.__name__ = name or func.__name__
            Col.__doc__ = help or func.__doc__
            cls.columns[Col.__name__] = Col()
            return func
        return decorator

    def update(self, **kwargs):
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


class EntriesTable(BeanTable):
    name = 'entries'
    columns = {}

    def __iter__(self):
        entries = self.prepare()
        context = Row(entries, self.options)
        for entry in entries:
            context.entry = entry
            context.rowid += 1
            yield context


column = EntriesTable.column


@column(str, 'id')
def id_(context):
    """Unique id of a directive."""
    return hash_entry(context.entry)


@column(str, 'type')
def type_(context):
    """The data type of the directive."""
    return type(context.entry).__name__.lower()


@column(str)
def filename(context):
    """The filename where the directive was parsed from or created."""
    return context.entry.meta["filename"]


@column(int)
def lineno(context):
    """The line number from the file the directive was parsed from."""
    return context.entry.meta["lineno"]


@column(datetime.date)
def date(context):
    """The date of the directive."""
    return context.entry.date


@column(int)
def year(context):
    """The year of the date year of the directive."""
    return context.entry.date.year


@column(int)
def month(context):
    """The year of the date month of the directive."""
    return context.entry.date.month


@column(int)
def day(context):
    """The year of the date day of the directive."""
    return context.entry.date.day


@column(str)
def flag(context):
    """The flag the transaction."""
    if not isinstance(context.entry, data.Transaction):
        return None
    return context.entry.flag


@column(str)
def payee(context):
    """The payee of the transaction."""
    if not isinstance(context.entry, data.Transaction):
        return None
    return context.entry.payee


@column(str)
def narration(context):
    """The narration of the transaction."""
    if not isinstance(context.entry, data.Transaction):
        return None
    return context.entry.narration


@column(str)
def description(context):
    """A combination of the payee + narration of the transaction, if present."""
    if not isinstance(context.entry, data.Transaction):
        return None
    return ' | '.join(filter(None, [context.entry.payee, context.entry.narration]))


@column(set)
def tags(context):
    """The set of tags of the transaction."""
    if not isinstance(context.entry, data.Transaction):
        return None
    return context.entry.tags


@column(set)
def links(context):
    """The set of links of the transaction."""
    if not isinstance(context.entry, data.Transaction):
        return None
    return context.entry.links


@column(dict)
def meta(context):
    return context.entry.meta


class PostingsTable(EntriesTable):
    name = 'postings'
    columns = EntriesTable.columns.copy()
    wildcard_columns = 'date flag payee narration position'.split()

    def __iter__(self):
        entries = self.prepare()
        context = Row(entries, self.options)
        for entry in entries:
            if isinstance(entry, data.Transaction):
                context.entry = entry
                for posting in entry.postings:
                    context.rowid += 1
                    context.posting = posting
                    yield context


column = PostingsTable.column


# redefine EntriesTable's column definition to return posting information
@column(str)
def filename(context):
    """The ledger where the posting is defined."""
    meta = context.posting.meta
    # Postings for pad transactions have their meta fields set to
    # None. See https://github.com/beancount/beancount/issues/767
    if meta is None:
        return None
    return meta["filename"]


# redefine EntriesTable's column definition to return posting information
@column(int)
def lineno(context):
    """The line number in the ledger file where the posting is defined."""
    meta = context.posting.meta
    # Postings for pad transactions have their meta fields set to
    # None. See https://github.com/beancount/beancount/issues/767
    if meta is None:
        return None
    return meta["lineno"]


@column(str)
def location(context):
    """The filename:lineno location where the posting is defined."""
    meta = context.posting.meta
    # Postings for pad transactions have their meta fields set to
    # None. See https://github.com/beancount/beancount/issues/767
    if meta is None:
        return None
    return '{:s}:{:d}:'.format(meta['filename'], meta['lineno'])


# redefine EntriesEnvironment's column dropping the entry type check.
@column(str)
def flag(context):
    """The flag of the parent transaction for this posting."""
    return context.entry.flag


# redefine EntriesEnvironment's column dropping the entry type check.
@column(str)
def payee(context):
    """The payee of the parent transaction for this posting."""
    return context.entry.payee


# redefine EntriesEnvironment's column dropping the entry type check.
@column(str)
def narration(context):
    """The narration of the parent transaction for this posting."""
    return context.entry.narration


# redefine EntriesEnvironment's column dropping the entry type check.
@column(str)
def description(context):
    "A combination of the payee + narration for the transaction of this posting."
    return ' | '.join(filter(None, [context.entry.payee, context.entry.narration]))


# redefine EntriesEnvironment's column dropping the entry type check.
@column(set)
def tags(context):
    "The set of tags of the parent transaction for this posting."
    return context.entry.tags


# redefine EntriesEnvironment's column dropping the entry type check.
@column(set)
def links(context):
    """The set of links of the parent transaction for this posting."""
    return context.entry.links


@column(str)
def posting_flag(context):
    """The flag of the posting itself."""
    return context.posting.flag


@column(str, 'account')
def account_(context):
    """The account of the posting."""
    return context.posting.account


@column(set)
def other_accounts(context):
    """The list of other accounts in the transaction, excluding that of this posting."""
    return sorted({posting.account for posting in context.entry.postings if posting is not context.posting})


@column(Decimal)
def number(context):
    """The number of units of the posting."""
    return context.posting.units.number


@column(str)
def currency(context):
    """The currency of the posting."""
    return context.posting.units.currency


@column(Decimal)
def cost_number(context):
    """The number of cost units of the posting."""
    cost = context.posting.cost
    return cost.number if cost else None


@column(str)
def cost_currency(context):
    """The cost currency of the posting."""
    cost = context.posting.cost
    return cost.currency if cost else None


@column(datetime.date)
def cost_date(context):
    """The cost currency of the posting."""
    cost = context.posting.cost
    return cost.date if cost else None


@column(str)
def cost_label(context):
    """The cost currency of the posting."""
    cost = context.posting.cost
    return cost.label if cost else ''


@column(position.Position, 'position')
def position_(context):
    """The position for the posting. These can be summed into inventories."""
    posting = context.posting
    return position.Position(posting.units, posting.cost)


@column(amount.Amount)
def price(context):
    """The price attached to the posting."""
    return context.posting.price


@column(amount.Amount)
def weight(context):
    """The computed weight used for this posting."""
    return convert.get_weight(context.posting)


@column(inventory.Inventory)
@cache(maxsize=1)
def balance(context):
    """The balance for the posting. These can be summed into inventories."""
    # Caching protects against multiple balance updates per row when
    # the columns appears more than once in the execurted query. The
    # rowid in the row context guarantees that otherwise identical
    # rows do not hit the cache and thus that the balance is correctly
    # updated.
    context.balance.add_position(context.posting)
    return copy.copy(context.balance)


@column(dict)
def meta(context):
    return context.posting.meta


@column(data.Transaction)
def entry(context):
    return context.entry


# Backward compatibility definitions for use in tests. These work
# because the tests only access the columns definitions and these are
# attached to the classes and not to the instances.

def Column(name):
    return PostingsTable.columns.get(name)

def EntriesEnvironment():
    return EntriesTable

def PostingsEnvironment():
    return PostingsTable
