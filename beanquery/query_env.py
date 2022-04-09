"""Environment object for compiler.

This module contains the various column accessors and function evaluators that
are made available by the query compiler via their compilation context objects.
Define new columns and functions here.
"""
__copyright__ = "Copyright (C) 2014-2017  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import copy
import datetime
import decimal
import re
import textwrap

from decimal import Decimal

from beancount.core.number import ZERO
from beancount.core.data import Transaction
from beancount.core.compare import hash_entry
from beancount.core import amount
from beancount.core import position
from beancount.core import inventory
from beancount.core import account
from beancount.core import account_types
from beancount.core import data
from beancount.core import getters
from beancount.core import convert
from beancount.core import prices
from beancount.utils.date_utils import parse_date_liberally

from beanquery import query_compile
from beanquery import types


# Non-aggregating functions.
SIMPLE_FUNCTIONS = collections.defaultdict(list)


def function(intypes, outtype, pass_context=False, name=None):
    def decorator(func):
        class Func(query_compile.EvalFunction):
            __intypes__ = intypes
            def __init__(self, operands):
                super().__init__(operands, outtype)
            def __call__(self, context):
                args = [operand(context) for operand in self.operands]
                for arg in args:
                    if arg is None:
                        return None
                if pass_context:
                    return func(context, *args)
                return func(*args)
        Func.__name__ = name if name is not None else func.__name__
        Func.__doc__ = func.__doc__
        SIMPLE_FUNCTIONS[Func.__name__].append(Func)
        return func
    return decorator


def Function(name, args):
    func = types.function_lookup(SIMPLE_FUNCTIONS, name, args)
    if func is not None:
        return func(args)
    func = types.function_lookup(AGGREGATE_FUNCTIONS, name, args)
    if func is not None:
        return func(args)
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


@function([datetime.date], str)
def weekday(x):
    """Extract a 3-letter weekday from a date."""
    return x.strftime('%a')


@function([], datetime.date)
def today():
    """Today's date"""
    return datetime.date.today()


# Operations on accounts.

@function([str, int], str)
def root(acc, n):
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


@function([str], datetime.date, pass_context=True)
def open_date(context, acc):
    """Get the date of the open directive of the account."""
    open_entry, _ = context.open_close_map[acc]
    return open_entry.date if open_entry else None


@function([str], datetime.date, pass_context=True)
def close_date(context, acc):
    """Get the date of the close directive of the account."""
    _, close_entry = context.open_close_map[acc]
    return close_entry.date if close_entry else None


@function([str], object, pass_context=True)
def meta(context, key):
    """Get some metadata key of the Posting."""
    try:
        return context.posting.meta[key]
    except (AttributeError, KeyError):
        pass
    return None


@function([str], object, pass_context=True)
def entry_meta(context, key):
    """Get some metadata key of the parent directive (Transaction)."""
    try:
        return context.entry.meta[key]
    except (AttributeError, KeyError):
        pass
    return None


@function([str], object, pass_context=True)
def any_meta(context, key):
    """Get metadata from the posting or its parent transaction's metadata if not present."""
    try:
        return context.posting.meta[key]
    except (AttributeError, KeyError):
        pass
    try:
        return context.entry.meta[key]
    except (AttributeError, KeyError):
        pass
    return None


@function([str], dict, pass_context=True)
@function([str, str], object, pass_context=True)
def open_meta(context, account, key=None):
    """Get the metadata dict of the open directive of the account."""
    entry, _ = context.open_close_map[account]
    if entry is None:
        return None
    if key is None:
        return entry.meta
    return entry.meta.get(key)


@function([str], dict, pass_context=True)
@function([str, str], object, pass_context=True)
@function([str], dict, pass_context=True, name='commodity_meta')
@function([str, str], object, pass_context=True, name='commodity_meta')
def currency_meta(context, commodity, key=None):
    """Get the metadata dict of the commodity directive of the currency."""
    entry = context.commodity_map.get(commodity)
    if entry is None:
        return None
    if key is None:
        return entry.meta
    return entry.meta.get(key)


@function([str], str, pass_context=True)
def account_sortkey(context, acc):
    """Get a string to sort accounts in order taking into account the types."""
    index, name = account_types.get_account_sort_key(context.account_types, acc)
    return '{}-{}'.format(index, name)

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
    return convert.convert_amount(amount_, currency, context.price_map, date)


@function([position.Position, str], amount.Amount, pass_context=True, name='convert')
@function([position.Position, str, datetime.date], amount.Amount, pass_context=True, name='convert')
def convert_position(context, pos, currency, date=None):
    """Coerce an amount to a particular currency."""
    return convert.convert_position(pos, currency, context.price_map, date)


@function([inventory.Inventory, str], inventory.Inventory, pass_context=True, name='convert')
@function([inventory.Inventory, str, datetime.date], inventory.Inventory, pass_context=True, name='convert')
def convert_inventory(context, inv, currency, date=None):
    """Coerce an inventory to a particular currency."""
    return inv.reduce(convert.convert_position, currency, context.price_map, date)


@function([position.Position], amount.Amount, pass_context=True, name='value')
@function([position.Position, datetime.date], amount.Amount, pass_context=True, name='value')
def position_value(context, pos, date=None):
    """Convert a position to its cost currency at the market value."""
    return convert.get_value(pos, context.price_map, date)


@function([inventory.Inventory], inventory.Inventory, pass_context=True, name='value')
@function([inventory.Inventory, datetime.date], inventory.Inventory, pass_context=True, name='value')
def inventory_value(context, inv, date=None):
    """Coerce an inventory to its market value."""
    return inv.reduce(convert.get_value, context.price_map, date)


@function([str, str], Decimal, pass_context=True)
@function([str, str, datetime.date], Decimal, pass_context=True, name='getprice')
def getprice(context, base, quote, date=None):
    """Fetch a price."""
    pair = (base.upper(), quote.upper())
    _, price = prices.get_price(context.price_map, pair, date)
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


@function([dict, str], object, name='getitem')
def getitem_(x, key):
    """Get the string value of a dict. The value is always converted to a string."""
    return x.get(key)


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
    sign = account_types.get_account_sign(account, context.account_types)
    return x if sign >= 0  else -x


@function([str], datetime.date)
@function([str, str], datetime.date)
def parse_date(string, frmt=None):
    """Parse date from string."""
    if frmt is None:
        return parse_date_liberally(string)
    return datetime.datetime.strptime(string, frmt).date()


@function([datetime.date, datetime.date], int)
def date_diff(x, y):
    """Calculates the difference (in days) between two dates."""
    return (x - y).days


@function([datetime.date, int], datetime.date)
def date_add(x, y):
    """Adds/subtracts number of days from the given date."""
    return x + datetime.timedelta(days=y)


# Aggregate functions.
AGGREGATE_FUNCTIONS = collections.defaultdict(list)


def aggregator(intypes, name=None):
    def decorator(cls):
        cls.__intypes__ = intypes
        if name is not None:
            cls.__name__ = name
        AGGREGATE_FUNCTIONS[cls.__name__].append(cls)
        return cls
    return decorator


@aggregator([types.Any], name='count')
class Count(query_compile.EvalAggregator):
    """Count the number of occurrences of the argument."""
    def __init__(self, operands):
        super().__init__(operands, int)

    def update(self, store, _):
        store[self.handle] += 1


@aggregator([int], name='sum')
class SumInt(query_compile.EvalAggregator):
    """Calculate the sum of the numerical argument."""
    def __init__(self, operands):
        super().__init__(operands, operands[0].dtype)

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
    def __init__(self, operands):
        super().__init__(operands, inventory.Inventory)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle].add_amount(value)


@aggregator([position.Position], name='sum')
class SumPosition(query_compile.EvalAggregator):
    """Calculate the sum of the position. The result is an Inventory."""
    def __init__(self, operands):
        super().__init__(operands, inventory.Inventory)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle].add_position(value)


@aggregator([inventory.Inventory], name='sum')
class SumInventory(query_compile.EvalAggregator):
    """Calculate the sum of the inventories. The result is an Inventory."""
    def __init__(self, operands):
        super().__init__(operands, inventory.Inventory)

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


# Column accessors for entries.

class IdEntryColumn(query_compile.EvalColumn):
    "Unique id of a directive."
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return hash_entry(context.entry)

class TypeEntryColumn(query_compile.EvalColumn):
    "The data type of the directive."
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return type(context.entry).__name__.lower()

class FilenameEntryColumn(query_compile.EvalColumn):
    "The filename where the directive was parsed from or created."
    __equivalent__ = 'entry.meta["filename"]'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return context.entry.meta["filename"]

class LineNoEntryColumn(query_compile.EvalColumn):
    "The line number from the file the directive was parsed from."
    __equivalent__ = 'entry.meta["lineno"]'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(int)

    def __call__(self, context):
        return context.entry.meta["lineno"]

class DateEntryColumn(query_compile.EvalColumn):
    "The date of the directive."
    __equivalent__ = 'entry.date'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(datetime.date)

    def __call__(self, context):
        return context.entry.date

class YearEntryColumn(query_compile.EvalColumn):
    "The year of the date of the directive."
    __equivalent__ = 'entry.date.year'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(int)

    def __call__(self, context):
        return context.entry.date.year

class MonthEntryColumn(query_compile.EvalColumn):
    "The month of the date of the directive."
    __equivalent__ = 'entry.date.month'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(int)

    def __call__(self, context):
        return context.entry.date.month

class DayEntryColumn(query_compile.EvalColumn):
    "The day of the date of the directive."
    __equivalent__ = 'entry.date.day'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(int)

    def __call__(self, context):
        return context.entry.date.day

class FlagEntryColumn(query_compile.EvalColumn):
    "The flag the transaction."
    __equivalent__ = 'entry.flag'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return (context.entry.flag
                if isinstance(context.entry, Transaction)
                else None)

class PayeeEntryColumn(query_compile.EvalColumn):
    "The payee of the transaction."
    __equivalent__ = 'entry.payee'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return (context.entry.payee or ''
                if isinstance(context.entry, Transaction)
                else None)

class NarrationEntryColumn(query_compile.EvalColumn):
    "The narration of the transaction."
    __equivalent__ = 'entry.narration'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return (context.entry.narration or ''
                if isinstance(context.entry, Transaction)
                else None)

# This is convenient, because many times the payee is empty and using a
# combination produces more compact listings.
class DescriptionEntryColumn(query_compile.EvalColumn):
    "A combination of the payee + narration of the transaction, if present."
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return (' | '.join(filter(None, [context.entry.payee,
                                         context.entry.narration]))
                if isinstance(context.entry, Transaction)
                else None)


# A globally available empty set to fill in for None's.
EMPTY_SET = frozenset()

class TagsEntryColumn(query_compile.EvalColumn):
    "The set of tags of the transaction."
    __equivalent__ = 'entry.tags'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(set)

    def __call__(self, context):
        return (context.entry.tags or EMPTY_SET
                if isinstance(context.entry, Transaction)
                else EMPTY_SET)

class LinksEntryColumn(query_compile.EvalColumn):
    "The set of links of the transaction."
    __equivalent__ = 'entry.links'
    __intypes__ = [data.Transaction]

    def __init__(self):
        super().__init__(set)

    def __call__(self, context):
        return (context.entry.links or EMPTY_SET
                if isinstance(context.entry, Transaction)
                else EMPTY_SET)



class MatchAccount(query_compile.EvalFunction):
    """A predicate, true if the transaction has at least one posting matching
    the regular expression argument."""
    __intypes__ = [str]

    def __init__(self, operands):
        super().__init__(operands, bool)

    def __call__(self, context):
        pattern = self.operands[0](context)
        search = re.compile(pattern, re.IGNORECASE).search
        return any(search(account) for account in getters.get_entry_accounts(context.entry))


# Functions defined only on entries.
ENTRY_FUNCTIONS = { 'has_account': [MatchAccount], }


class FilterEntriesEnvironment(query_compile.CompilationEnvironment):
    """An execution context that provides access to attributes on Transactions
    and other entry types.
    """
    context_name = 'FROM clause'
    columns = {
        'id'          : IdEntryColumn,
        'type'        : TypeEntryColumn,
        'filename'    : FilenameEntryColumn,
        'lineno'      : LineNoEntryColumn,
        'date'        : DateEntryColumn,
        'year'        : YearEntryColumn,
        'month'       : MonthEntryColumn,
        'day'         : DayEntryColumn,
        'flag'        : FlagEntryColumn,
        'payee'       : PayeeEntryColumn,
        'narration'   : NarrationEntryColumn,
        'description' : DescriptionEntryColumn,
        'tags'        : TagsEntryColumn,
        'links'       : LinksEntryColumn,
        }
    functions = copy.copy(SIMPLE_FUNCTIONS)
    functions.update(ENTRY_FUNCTIONS)




# Column accessors for postings.

class IdColumn(query_compile.EvalColumn):
    "The unique id of the parent transaction for this posting."
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return hash_entry(context.entry)

class TypeColumn(query_compile.EvalColumn):
    "The data type of the parent transaction for this posting."
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return type(context.entry).__name__.lower()

class FilenameColumn(query_compile.EvalColumn):
    "The filename where the posting was parsed from or created."
    __equivalent__ = 'entry.meta["filename"]'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return context.entry.meta["filename"]

class LineNoColumn(query_compile.EvalColumn):
    "The line number from the file the posting was parsed from."
    __equivalent__ = 'entry.meta["lineno"]'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(int)

    def __call__(self, context):
        return context.entry.meta["lineno"]

class FileLocationColumn(query_compile.EvalColumn):
    """The filename:lineno where the posting was parsed from or created.

    If you select this column as the first column, because it renders like
    errors, Emacs is able to pick those up and you can navigate between an
    arbitrary list of transactions with next-error and previous-error.
    """
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        if context.posting.meta is not None:
            return '{}:{:d}:'.format(context.posting.meta.get("filename", "N/A"),
                                     context.posting.meta.get("lineno", 0))
        return '' # Unknown.

class DateColumn(query_compile.EvalColumn):
    "The date of the parent transaction for this posting."
    __equivalent__ = 'entry.date'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(datetime.date)

    def __call__(self, context):
        return context.entry.date

class YearColumn(query_compile.EvalColumn):
    "The year of the date of the parent transaction for this posting."
    __equivalent__ = 'entry.date.year'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(int)

    def __call__(self, context):
        return context.entry.date.year

class MonthColumn(query_compile.EvalColumn):
    "The month of the date of the parent transaction for this posting."
    __equivalent__ = 'entry.date.month'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(int)

    def __call__(self, context):
        return context.entry.date.month

class DayColumn(query_compile.EvalColumn):
    "The day of the date of the parent transaction for this posting."
    __equivalent__ = 'entry.date.day'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(int)

    def __call__(self, context):
        return context.entry.date.day

class FlagColumn(query_compile.EvalColumn):
    "The flag of the parent transaction for this posting."
    __equivalent__ = 'entry.flag'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return context.entry.flag

class PayeeColumn(query_compile.EvalColumn):
    "The payee of the parent transaction for this posting."
    __equivalent__ = 'entry.payee'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return context.entry.payee or ''

class NarrationColumn(query_compile.EvalColumn):
    "The narration of the parent transaction for this posting."
    __equivalent__ = 'entry.narration'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return context.entry.narration

# This is convenient, because many times the payee is empty and using a
# combination produces more compact listings.
class DescriptionColumn(query_compile.EvalColumn):
    "A combination of the payee + narration for the transaction of this posting."
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        entry = context.entry
        return (' | '.join(filter(None, [entry.payee, entry.narration]))
                if isinstance(entry, Transaction)
                else None)

class TagsColumn(query_compile.EvalColumn):
    "The set of tags of the parent transaction for this posting."
    __equivalent__ = 'entry.tags'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(set)

    def __call__(self, context):
        return context.entry.tags or EMPTY_SET

class LinksColumn(query_compile.EvalColumn):
    "The set of links of the parent transaction for this posting."
    __equivalent__ = 'entry.links'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(set)

    def __call__(self, context):
        return context.entry.links or EMPTY_SET

class PostingFlagColumn(query_compile.EvalColumn):
    "The flag of the posting itself."
    __equivalent__ = 'posting.flag'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return context.posting.flag

class AccountColumn(query_compile.EvalColumn):
    "The account of the posting."
    __equivalent__ = 'posting.account'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return context.posting.account

class OtherAccountsColumn(query_compile.EvalColumn):
    "The list of other accounts in the transaction, excluding that of this posting."
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(set)

    def __call__(self, context):
        return sorted({posting.account
                       for posting in context.entry.postings
                       if posting is not context.posting})


class NumberColumn(query_compile.EvalColumn):
    "The number of units of the posting."
    __equivalent__ = 'posting.units.number'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(Decimal)

    def __call__(self, context):
        return context.posting.units.number

class CurrencyColumn(query_compile.EvalColumn):
    "The currency of the posting."
    __equivalent__ = 'posting.units.currency'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        return context.posting.units.currency

class CostNumberColumn(query_compile.EvalColumn):
    "The number of cost units of the posting."
    __equivalent__ = 'posting.cost.number'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(Decimal)

    def __call__(self, context):
        cost = context.posting.cost
        return cost.number if cost else None

class CostCurrencyColumn(query_compile.EvalColumn):
    "The cost currency of the posting."
    __equivalent__ = 'posting.cost.currency'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        cost = context.posting.cost
        return cost.currency if cost else ''

class CostDateColumn(query_compile.EvalColumn):
    "The cost currency of the posting."
    __equivalent__ = 'posting.cost.date'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(datetime.date)

    def __call__(self, context):
        cost = context.posting.cost
        return cost.date if cost else None

class CostLabelColumn(query_compile.EvalColumn):
    "The cost currency of the posting."
    __equivalent__ = 'posting.cost.label'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(str)

    def __call__(self, context):
        cost = context.posting.cost
        return cost.label if cost else ''

class PositionColumn(query_compile.EvalColumn):
    "The position for the posting. These can be summed into inventories."
    __equivalent__ = 'posting'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(position.Position)

    def __call__(self, context):
        posting = context.posting
        return position.Position(posting.units, posting.cost)

class PriceColumn(query_compile.EvalColumn):
    "The price attached to the posting."
    __equivalent__ = 'posting.price'
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(amount.Amount)

    def __call__(self, context):
        return context.posting.price

class WeightColumn(query_compile.EvalColumn):
    "The computed weight used for this posting."
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(amount.Amount)

    def __call__(self, context):
        return convert.get_weight(context.posting)

class BalanceColumn(query_compile.EvalColumn):
    "The balance for the posting. These can be summed into inventories."
    __intypes__ = [data.Posting]

    def __init__(self):
        super().__init__(inventory.Inventory)

    def __call__(self, context):
        return copy.copy(context.balance)


class FilterPostingsEnvironment(query_compile.CompilationEnvironment):
    """An execution context that provides access to attributes on Postings.
    """
    context_name = 'WHERE clause'
    columns = {
        'id'             : IdColumn,
        'type'           : TypeColumn,
        'filename'       : FilenameColumn,
        'lineno'         : LineNoColumn,
        'location'       : FileLocationColumn,
        'date'           : DateColumn,
        'year'           : YearColumn,
        'month'          : MonthColumn,
        'day'            : DayColumn,
        'flag'           : FlagColumn,
        'payee'          : PayeeColumn,
        'narration'      : NarrationColumn,
        'description'    : DescriptionColumn,
        'tags'           : TagsColumn,
        'links'          : LinksColumn,
        'posting_flag'   : PostingFlagColumn,
        'account'        : AccountColumn,
        'other_accounts' : OtherAccountsColumn,
        'number'         : NumberColumn,
        'currency'       : CurrencyColumn,
        'cost_number'    : CostNumberColumn,
        'cost_currency'  : CostCurrencyColumn,
        'cost_date'      : CostDateColumn,
        'cost_label'     : CostLabelColumn,
        'position'       : PositionColumn,
        'change'         : PositionColumn,  # Backwards compatible.
        'price'          : PriceColumn,
        'weight'         : WeightColumn,
        'balance'        : BalanceColumn,
        }
    functions = copy.copy(SIMPLE_FUNCTIONS)

class TargetsEnvironment(FilterPostingsEnvironment):
    """An execution context that provides access to attributes on Postings.
    """
    context_name = 'SELECT list'
    functions = copy.copy(FilterPostingsEnvironment.functions)
    functions.update(AGGREGATE_FUNCTIONS)

    # The list of columns that a wildcard will expand into.
    wildcard_columns = 'date flag payee narration position'.split()
