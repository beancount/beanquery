"""Environment object for compiler.

This module contains the various column accessors and function evaluators that
are made available by the query compiler via their compilation context objects.
Define new columns and functions here.
"""
__copyright__ = "Copyright (C) 2014-2017  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import decimal
import inspect
import re
import textwrap

from decimal import Decimal

import dateutil.parser
from dateutil.relativedelta import relativedelta, weekday

from beancount.core.number import ZERO
from beancount.core import amount
from beancount.core import position
from beancount.core import inventory
from beancount.core import account
from beancount.core import convert
from beancount.core import prices
from beancount.core.account_types import get_account_sign, get_account_sort_key

from beanquery import query_compile
from beanquery import types

# Type categories for function classification
# These types are matched against the first input type of the function.
# Functions can be force-assigned to one or more of these categories
# by use of the group argument in the function decorators 'function',
# 'register' or 'aggregate' in the 'query_env' module.
TYPE_CATEGORIES = {
    'amount': [amount.Amount],
    'account': [],  # Must be manually assigned as account names are strings
    'position': [position.Position, inventory.Inventory],
    'metadata': [], # Must be manually assigned, signatures in query_env.py: 'object'
    'date': [datetime.date],
    'atomic': []  # fallback, default category
}

# Function groups for documentation. BQL functions are added to the appropriate
# list inside this dictionary by the @function, @register and @aggregate
# decorators.
FUNCTION_DOC_GROUPS = {
    x : [] for x in TYPE_CATEGORIES
}

class ColumnsRegistry(dict):

    def register(self, dtype):
        def decorator(func):
            class Col(query_compile.EvalColumn):
                def __init__(self):
                    super().__init__(dtype)
                __call__ = staticmethod(func)
            Col.__name__ = func.__name__
            Col.__doc__ = func.__doc__
            self[Col.__name__] = Col()
            return func
        return decorator


def _extract_param_names(func):
    """Extract parameter names from a function. Used to generate function
    documentation.

    Args:
        func: A function to extract parameter names from.
        skip_self: If True, skip 'self' parameter (for class methods).

    Returns:
        A list of parameter names, optionally excluding 'self'
    """
    sig = inspect.signature(func)
    param_names = list(sig.parameters.keys())

    return param_names


def _add_to_doc_groups(func_class, intypes, groups):
    """Add a function class to the appropriate documentation groups.

    If explicit groups are provided, use those. Otherwise, determine the group
    based on the first input type using TYPE_CATEGORIES.

    Args:
        func_class: The function class to add to documentation groups.
        intypes: List of input types for the function.
        groups: Explicit groups specified by the decorator, or None.
    """
    target_groups = groups if groups else []

    # If no explicit groups specified, determine from first input type
    if not target_groups and intypes:
        first_type = intypes[0]
        for category, type_set in TYPE_CATEGORIES.items():
            if first_type in type_set:
                target_groups = [category]
                break
        # Default to 'atomic' if no match found
        if not target_groups:
            target_groups = ['atomic']

    # Add to documentation groups
    for group in target_groups:
        if group in FUNCTION_DOC_GROUPS:
            FUNCTION_DOC_GROUPS[group].append(func_class)


def function(intypes, outtype, pass_context=None, name=None, groups=None):
    """Decorator to register a function in the query environment. Expects
    to decorate a function that takes operands as arguments.

    Args:
      intypes: List of input types.
      outtype: Return value.
      pass_context: Whether the function receives the context as first argument
           or is pure.
      name: Name of the function.
      groups: In which group(s) the function in the help output (list). See
           TYPE_CATEGORIES for valid group names.
    """
    def decorator(func):
        # Extract parameter names from the original function, excluding 'context' if present
        param_names = _extract_param_names(func)
        if pass_context:
            param_names = param_names[1:]  # Remove 'context' from param names

        class Func(query_compile.EvalFunction):
            __intypes__ = intypes
            __outtype__ = outtype
            __param_names__ = param_names
            pure = not pass_context
            def __init__(self, context, operands):
                super().__init__(context, operands, outtype)
            def __call__(self, row):
                args = [operand(row) for operand in self.operands]
                for arg in args:
                    if arg is None:
                        return None
                if pass_context:
                    return func(self.context, *args)
                return func(*args)
        Func.__name__ = name if name is not None else func.__name__
        Func.__doc__ = func.__doc__
        query_compile.FUNCTIONS[Func.__name__].append(Func)
        _add_to_doc_groups(Func, intypes, groups)

        return func
    return decorator


def register(name=None, groups=None):
    """Decorator to register a function in the query environment with
    more fine-grained control. Expects to decorate a class that implements
    the query_compile.EvalFunction interface. Setting __intypes__,
    __outtype__ and __param_names__ is left to the decorated class.

    Args:
      name: Name of the function.
      groups: In which group(s) the function in the help output (list). See
           TYPE_CATEGORIES for valid group names.
    """
    def decorator(cls):
        if name is not None:
            cls.__name__ = name
        query_compile.FUNCTIONS[cls.__name__].append(cls)
        _add_to_doc_groups(cls, cls.__intypes__, groups)

        return cls
    return decorator


@register('getitem')
class GetItem2(query_compile.EvalFunction):
    """Get one item from a dict object if it exists, otherwise a default value."""
    __intypes__ = [dict, str]
    __param_names__ = ['d', 'key']

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
    """Get one item from a dict object if it exists, otherwise a default value."""
    __intypes__ = [dict, str, types.Any]
    __param_names__ = ['d', 'key', 'default']

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
    """Convert the object to an integer number."""
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
    """Convert the object to a decimal number."""
    try:
        return Decimal(x)
    except (ValueError, TypeError, decimal.InvalidOperation):
        return None


@function([types.Any], str, name='str')
def str_(x):
    """Convert any object to a string."""
    if x is True:
        return 'TRUE'
    if x is False:
        return 'FALSE'
    return str(x)


@function([datetime.date], datetime.date, name = 'date', groups=['date'])
@function([str], datetime.date, name='date', groups=['atomic', 'date'])
@function([object], datetime.date, name='date', groups=['atomic', 'date'])
def date_(x):
    """Convert the argument to a date. The argument should be
    a string in the format YYYY-MM-DD. Date objects are passed
    unchanged. Objects are converted to None."""
    if isinstance(x, datetime.date):
        return x
    if isinstance(x, str):
        try:
            return datetime.datetime.strptime(x, '%Y-%m-%d').date()
        except ValueError:
            pass
    return None


@function([int, int, int], datetime.date, name='date', groups=['date'])
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


@function([], datetime.date, groups=['date'])
def today():
    """Today's date"""
    return datetime.date.today()


# Operations on accounts.

@function([str], str, groups=['account'])
@function([str, int], str, groups=['account'])
def root(acc, n=1):
    """Get the root name(s) of the account."""
    return account.root(n, acc)


@function([str], str, groups=['account'])
def parent(acc):
    """Get the parent name of the account."""
    return account.parent(acc)


@function([str], str, groups=['account'])
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


@function([str], datetime.date, pass_context=True, groups=['account', 'metadata'])
def open_date(context, acc):
    """Get the date of the open directive of the account."""
    open_entry, _ = context.tables['accounts'].accounts.get(acc, NONENONE)
    if open_entry is None:
        return None
    return open_entry.date


@function([str], datetime.date, pass_context=True, groups=['account', 'metadata'])
def close_date(context, acc):
    """Get the date of the close directive of the account."""
    _, close_entry = context.tables['accounts'].accounts.get(acc, NONENONE)
    if close_entry is None:
        return None
    return close_entry.date


@function([str], dict, pass_context=True, groups=['account', 'metadata'])
@function([str, str], object, pass_context=True, groups=['account', 'metadata'])
def open_meta(context, account, key=None):
    """Get the metadata dict of the open directive of the account.
    With one argument, returns all metadata as a dict object. With two
    arguments, returns the value of a specific metadata key."""
    open_entry, _ = context.tables['accounts'].accounts.get(account, NONENONE)
    if open_entry is None:
        return None
    if key is None:
        return open_entry.meta
    return open_entry.meta.get(key)


# Stub kept only for function type checking and for generating documentation.
@function([str], object, pass_context = True, groups = ['metadata'])
def meta(context, key):
    """Get some metadata key of the posting."""
    raise NotImplementedError


# Stub kept only for function type checking and for generating documentation.
@function([str], object, pass_context = True, groups = ['metadata'])
def entry_meta(context, key):
    """Get some metadata key of the transaction."""
    raise NotImplementedError


# Stub kept only for function type checking and for generating documentation.
@function([str], object, pass_context=True, groups = ['metadata'])
def any_meta(context, key):
    """Get metadata from the posting or its parent transaction if not present."""
    raise NotImplementedError


@function([str], dict, pass_context=True, groups = ['amount', 'metadata'])
@function([str, str], object, pass_context=True, groups = ['amount', 'metadata'])
@function([str], dict, pass_context=True, name='commodity_meta', groups = ['amount', 'metadata'])
@function([str, str], object, pass_context=True, name='commodity_meta', groups = ['amount', 'metadata'])
def currency_meta(context, commodity, key=None):
    """Get the metadata dict of the commodity directive of the currency."""
    entry = context.tables['commodities'].commodities.get(commodity)
    if entry is None:
        return None
    if key is None:
        return entry.meta
    return entry.meta.get(key)


@function([str], str, pass_context=True, groups=['account'])
def account_sortkey(context, acc):
    """Get a string to sort accounts in order taking into account the types."""
    account_types = context.tables['accounts'].types
    index, name = get_account_sort_key(account_types, acc)
    return '{}-{}'.format(index, name)


# Stub kept only for function type checking and for generating documentation.
@function([str], bool, groups=['account'])
def has_account(context, pattern):
    """True if the transaction has at least one posting matching the regular expression argument."""
    raise NotImplementedError


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
    """Get the number of units. Returns the amount, stripping cost."""
    return convert.get_units(pos)


@function([inventory.Inventory], inventory.Inventory, name='units')
def inventory_units(inv):
    """For all position in the inventory, strip the information about
    at which cost they were acquired. The result is another inventory."""
    return inv.reduce(convert.get_units)


@function([position.Position], amount.Amount, name='cost')
def position_cost(pos):
    """Get the cost of a position."""
    return convert.get_cost(pos)


@function([inventory.Inventory], inventory.Inventory, name='cost')
def inventory_cost(inv):
    """Get the cost of all positions in an inventory. Returns an
    inventory with as many positions as there were currencies by which
    the positions in the original inventory were acquired."""
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


@function([str, str], Decimal, pass_context=True, groups = ['position'])
@function([str, str, datetime.date], Decimal, pass_context=True, name='getprice', groups = ['position'])
def getprice(context, base, quote, date=None):
    """Fetch a price. Arguments: Base currency, e.g. 'EUR'; Commodity name (string);
    Date: Price as of this date. Default: Latest price."""
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


@function([str, inventory.Inventory], amount.Amount, name='only',
  groups=['amount'])
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


# ``date`` type

class Date(types.Structure):
    name = 'date'
    columns = ColumnsRegistry()

    @columns.register(int)
    def year(x):
        return x.year

    @columns.register(int)
    def month(x):
        return x.month

    @columns.register(int)
    def day(x):
        return x.day

types.ALIASES[datetime.date] = Date


@function([str], datetime.date, groups=['atomic', 'date'])
@function([str, str], datetime.date, groups=['atomic', 'date'])
def parse_date(string, frmt=None):
    """Parse date from string (first argument). Without second argument,
    the 'dateutil' library is used to parse the string, and can deal with
    several time stamp formats. The optional second argument specifies the
    format as in the 'datetime' library, for example:
    '%Y-%m-%d' to parse '2022-01-20'."""
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


@function([str, datetime.date], datetime.date, groups = ['date'])
def date_trunc(field, x):
    """Truncate a date to the specified precision. Example: date_trunc('month',
    date). Make sure to use single quotes in the first argument, as
    double-quoted strings are parsed as column names for backwards compatibility
    reasons."""
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


@function([str, datetime.date], int, groups = ['date'])
def date_part(field, x):
    """Extract the specified field from a date.

    Arguments:

      field: Date part to extract, for example, 'year', 'month', 'week', 'day'. Details below.
        since the UNIX epoch.
      x: The date to extract the field from.

    Details:
        The 'field' argument can be any of

        * 'weekday'/'dow', 'week', 'month', 'quarter', 'year', 'decade', 'century', 'millennium', or
        * 'epoch': returns the number of seconds since the UNIX epoch.
        * 'isoweekday'/'isodow', 'isoyear': The ISO 8601 week number or year, which
          might differ from the conventional understanding around New Year's eve.

        Make sure to use single quotes for 'field', as double-quoted strings are parsed
        as column names for backwards compatibility reasons.
    """
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


@function([str], relativedelta, groups = ['date'])
def interval(x):
    """Construct a relative time interval.

    Arguments:
      x: A string of the form 'N unit' where unit is one of 'day', 'month', 'year'
        (Plural forms are also accepted). Examples: '1 month', '-20 days'.
    """
    m = re.fullmatch(r'([-+]?[0-9]+)\s+(day|month|year)s?', x)
    if not m:
        return None
    number = int(m.group(1))
    unit = m.group(2)
    if unit == 'day':
        return relativedelta(days=number)
    if unit == 'week':
        return relativedelta(weeks=number)
    if unit == 'month':
        return relativedelta(months=number)
    if unit == 'year':
        return relativedelta(years=number)
    if unit == 'decade':
        return relativedelta(years=number * 10)
    if unit == 'century':
        return relativedelta(years=number * 100)
    if unit == 'millennium':
        return relativedelta(years=number * 1000)
    return None


@function([relativedelta, datetime.date, datetime.date], datetime.date, groups = ['date'])
def date_bin(stride, source, origin):

    if stride.months or stride.years:
        if origin + stride <= origin:
            # FIXME: this should raise and error: stride must be greater than zero
            return None
        if source >= origin:
            d = n = origin
            while True:
                n += stride
                if n >= source:
                    return d
                d = n
        else:
            n = origin
            while True:
                n -= stride
                if n <= source:
                    return n
    else:
        seconds = stride.days * 86400 + stride.hours * 3600 + stride.minutes * 60 + stride.seconds
        if seconds < 0:
            # FIXME: this should raise and error: stride must be greater than zero
            return None
        diff = (source - origin).total_seconds()
        modulo = diff % seconds
        delta = diff - modulo
        result = origin + datetime.timedelta(seconds=delta)
        if modulo < 0:
            result -= datetime.timedelta(seconds=seconds)
        return result


@function([str, datetime.date, datetime.date], datetime.date, name='date_bin', groups = ['date'])
def date_bin_str(stride, source, origin):
    """Bin a date into the specified stride aligned with the specified origin.

    As an extension to the the SQL standard ``date_bin()`` function this
    function also accepts strides containing units of months and years.

    Arguments:
      stride: A string representing a time interval, e.g. '1 day', '1 month',
        '1 year'; Make sure to use single quotes in the first argument, as
        double-quoted strings are parsed as column names for backwards
        compatibility.
      source: The date to bin; origin: The start of the binning interval.
      relativedelta: Relative time interval, as generated by interval().
    """
    return date_bin(interval(stride), source, origin)


def aggregator(intypes, outtype = None, name=None, groups = None):
    """Decorator to register an aggregator function.

    Args:
      intypes: A list of types that the aggregator can accept.
      outtype: The output type of the aggregator function. "None"
        means no type annotation. To indicate that the function
        returns None, use types.NoneType.
      name: The name of the aggregator function.
      groups: A list of groups that the aggregator belongs to. See
           TYPE_CATEGORIES for valid group names.
    """
    def decorator(cls):
        cls.__intypes__ = intypes
        cls.__outtype__ = outtype
        # The decorated functions do not have explicit parameter names in the signature
        # We use single lowercase letters a, b, c, ... as placeholders
        cls.__param_names__ = "abcdefghijklmnopqrstuvwxyz"[:len(intypes)]

        if name is not None:
            cls.__name__ = name
        query_compile.FUNCTIONS[cls.__name__].append(cls)
        _add_to_doc_groups(cls, intypes, groups)

        return cls
    return decorator


@aggregator([types.Asterisk], int, name='count')
class Count(query_compile.EvalAggregator):
    """Count the number of input rows."""
    def __init__(self, context, operands):
        super().__init__(context, operands, int)

    def update(self, store, context):
        store[self.handle] += 1


@aggregator([types.Any], int, name='count')
class CountArg(query_compile.EvalAggregator):
    """Count the number of non-NULL occurrences of the argument."""
    def __init__(self, context, operands):
        super().__init__(context, operands, int)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle] += 1


@aggregator([int], int, name='sum')
class SumInt(query_compile.EvalAggregator):
    """Calculate the sum of the numerical argument."""
    def __init__(self, context, operands):
        super().__init__(context, operands, operands[0].dtype)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle] += value


@aggregator([Decimal], Decimal, name='sum')
class SumDecimal(query_compile.EvalAggregator):
    """Calculate the sum of the numerical argument."""
    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle] += value


@aggregator([amount.Amount], inventory.Inventory, name='sum')
class SumAmount(query_compile.EvalAggregator):
    """Calculate the sum of the amount. The result is an Inventory."""
    def __init__(self, context, operands):
        super().__init__(context, operands, inventory.Inventory)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle].add_amount(value)


@aggregator([position.Position], inventory.Inventory, name='sum', groups = ['position'])
class SumPosition(query_compile.EvalAggregator):
    """Calculate the sum of the position. The result is an Inventory."""
    def __init__(self, context, operands):
        super().__init__(context, operands, inventory.Inventory)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle].add_position(value)


@aggregator([inventory.Inventory], inventory.Inventory, name='sum')
class SumInventory(query_compile.EvalAggregator):
    """Calculate the sum of the inventories. The result is an Inventory."""
    def __init__(self, context, operands):
        super().__init__(context, operands, inventory.Inventory)

    def update(self, store, context):
        value = self.operands[0](context)
        if value is not None:
            store[self.handle].add_inventory(value)


@aggregator([types.Any], types.Any, name='first')
class First(query_compile.EvalAggregator):
    """Keep the first of the values seen."""
    def initialize(self, store):
        store[self.handle] = None

    def update(self, store, context):
        if store[self.handle] is None:
            value = self.operands[0](context)
            store[self.handle] = value


@aggregator([types.Any], types.Any, name='last')
class Last(query_compile.EvalAggregator):
    """Keep the last of the values seen."""
    def initialize(self, store):
        store[self.handle] = None

    def update(self, store, context):
        value = self.operands[0](context)
        store[self.handle] = value


@aggregator([types.Any], types.Any, name='min')
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


@aggregator([types.Any], types.Any, name='max')
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

def _describe_functions(functions, aggregates=False, type_filter=None):
    """Describe functions, optionally filtered by input type category.

    Args:
        functions: Dictionary of (function name: EvalFunction subclass),
          the actual class, not an object, which would represent a particular
          function call.
        aggregates: If True, show aggregates; if False, show regular functions
        type_filter: Optional filter by input type category (see TYPE_CATEGORIES)
    """
    # Determine which functions to iterate over
    if type_filter:
        # Use the pre-populated FUNCTION_DOC_GROUPS for filtering
        funcs_to_process = FUNCTION_DOC_GROUPS.get(type_filter, [])
    else:
        # Collect all functions from all groups
        funcs_to_process = []
        for name, funcs in functions.items():
            funcs_to_process.extend(funcs)

    entries = []
    for func in funcs_to_process:
        # Filter by aggregate vs non-aggregate
        if aggregates != issubclass(func, query_compile.EvalAggregator):
            continue

        # Get the function name
        name = func.__name__.lower()

        # Assemble function signature for output using parameter names
        args = ', '.join(f'{param_name}: {types.name(dtype)}'
                        for param_name, dtype in zip(func.__param_names__, func.__intypes__))

        if func.__outtype__:
            outtype = types.name(func.__outtype__)
        else:
            outtype = None

        doc = func.__doc__ or ''
        entries.append((name, doc, args, outtype))

    entries.sort()
    return entries
