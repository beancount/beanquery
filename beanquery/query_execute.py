"""Execution of interpreter on data rows.
"""
__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import datetime
import itertools
import operator

from beancount.core import data
from beancount.core import inventory
from beancount.core import getters
from beancount.core import display_context
from beancount.parser import printer
from beancount.parser import options
from beancount.ops import summarize
from beancount.core import prices
from beancount.utils import misc_utils

from beanquery import query_compile


def apply_from_qualifiers(c_from, entries, options_map):
    """Filter the entries by the given compiled FROM clause qualifiers.

    Args:
      c_from: A compiled From clause instance.
      entries: A list of directives.
      options_map: A parser's option_map.
    Returns:
      A list of filtered entries.
    """
    if c_from is None:
        return entries

    # Process the OPEN clause.
    if c_from.open is not None:
        assert isinstance(c_from.open, datetime.date)
        open_date = c_from.open
        entries, index = summarize.open_opt(entries, open_date, options_map)

    # Process the CLOSE clause.
    if c_from.close is not None:
        if isinstance(c_from.close, datetime.date):
            close_date = c_from.close
            entries, index = summarize.close_opt(entries, close_date, options_map)
        elif c_from.close is True:
            entries, index = summarize.close_opt(entries, None, options_map)

    # Process the CLEAR clause.
    if c_from.clear is not None:
        entries, index = summarize.clear_opt(entries, None, options_map)

    return entries


def filter_entries(expr, entries, options):
    """Filter entries by the given expression.

    This is kept mostly for backward compatibility and use in tests.

    Args:
      expr: Expression used to filter the entries, EvalNode instance.
      entries: List of directives.
      options: Options as returned by the Beancount parser.
    Returns:
      List of filtered entries.
    """
    if expr is not None:
        r = []
        context = create_row_context(entries, options)
        for entry in entries:
            context.entry = entry
            if expr(context):
                r.append(entry)
        entries = r
    return entries


def execute_print(c_print, entries, options_map, file):
    """Print entries from a print statement specification.

    Args:
      c_print: An instance of a compiled EvalPrint statement.
      entries: A list of directives.
      options_map: A parser's option_map.
      file: The output file to print to.
    """
    # Apply OPEN, CLOSE, CLEAR qualifiers.
    entries = apply_from_qualifiers(c_print.c_from, entries, options_map)

    # Filter the entries with the FROM clause expression.
    entries = filter_entries(c_print.c_where, entries, options_map)

    # Create a context that renders all numbers with their natural
    # precision, but honors the commas option. This is kept in sync with
    # {2c694afe3140} to avoid a dependency.
    dcontext = display_context.DisplayContext()
    dcontext.set_commas(options_map['dcontext'].commas)
    printer.print_entries(entries, dcontext, file=file)


class Allocator:
    """A helper class to count slot allocations and return unique handles to them.
    """
    def __init__(self):
        self.size = 0

    def allocate(self):
        """Allocate a new slot to store row aggregation information.

        Returns:
          A unique handle used to index into an row-aggregation store (an integer).
        """
        handle = self.size
        self.size += 1
        return handle

    def create_store(self):
        """Create a new row-aggregation store suitable to contain all the node allocations.

        Returns:
          A store that can accommodate and be indexed by all the allocated slot handles.
        """
        return [None] * self.size


class RowContext:
    """A dumb container for information used by a row expression."""

    rowid = None

    # The current posting being evaluated.
    posting = None

    # The current transaction of the posting being evaluated.
    entry = None

    # The current running balance *after* applying the posting.
    balance = None

    # The parser's options_map.
    options_map = None

    # An AccountTypes tuple of the account types.
    account_types = None

    # A dict of account name strings to (open, close) entries for those accounts.
    open_close_map = None

    # A dict of currency name strings to the corresponding Commodity entry.
    commodity_map = None

    # A price dict as computed by build_price_map()
    price_map = None

    # A storage area for computing aggregate expression.
    store = None

    # The context hash is used in caching column accessor functions.
    # Instead than hashing the row context content, use the rowid as
    # hash.
    def __hash__(self):
        return self.rowid


class NullType:
    """An object that compares smaller than anything.

    An instance of this class is used to replace None in BQL query
    results in sort keys to obtain sorting semantics similar to SQL
    where NULL is sortet at the beginning.

    """
    __slots__ = ()

    def __repr__(self):
        return 'NULL'

    __str__ = __repr__

    def __lt__(self, other):
        # Make sure that instances of this class compare equal.
        if isinstance(other, NullType):
            return False
        return True

    def __gt__(self, other):
        # Make sure that instances of this class compare equal.
        if isinstance(other, NullType):
            return True
        return False


NULL = NullType()


def nullitemgetter(item, *items):
    """An itemgetter() that replaces None values with NULL."""
    if items:
        items = (item, ) + items
        def func(obj):
            r = []
            for i in items:
                value = obj[i]
                r.append(value if value is not None else NULL)
            return tuple(r)
        return func
    # pylint: disable=function-redefined
    def func(obj):
        value = obj[item]
        return value if value is not None else NULL
    return func


def create_row_context(entries, options_map):
    """Create the context container which we will use to evaluate rows."""
    context = RowContext()
    context.rowid = 0
    context.balance = inventory.Inventory()
    context.balance_update_rowid = -1

    # Initialize some global properties for use by some of the accessors.
    context.options_map = options_map
    context.account_types = options.get_account_types(options_map)
    context.open_close_map = getters.get_account_open_close(entries)
    context.commodity_map = getters.get_commodity_directives(entries)
    context.price_map = prices.build_price_map(entries)

    return context


def execute_query(query, entries, options):
    """Given a compiled select statement, execute the query.

    Args:
      query: The query to execute.
      entries: A list of directives.
      options: A parser's option_map.

    Returns:
        A list of (name, dtype) tuples describing the results set
        table and a list of ResultRow tuples with the data.item pairs.

    """

    if isinstance(query, query_compile.EvalQuery):
        return execute_select(query, entries, options)

    if isinstance(query, query_compile.EvalPivot):
        columns, rows = execute_select(query.query, entries, options)

        col1, col2 = query.pivots
        othercols = [i for i in range(len(columns)) if i not in query.pivots]
        nother = len(othercols)
        other = lambda x: tuple(x[i] for i in othercols)
        keys = sorted(set(row[col2] for row in rows))

        # Compute the new column names and dtypes.
        if nother > 1:
            it = itertools.product(keys, other(columns))
            names = [f'{columns[col1].name}/{columns[col2].name}'] + [f'{key}/{col.name}' for key, col in it]
        else:
            names = [f'{columns[col1].name}/{columns[col2].name}'] + [f'{key}' for key in keys]
        dtypes = ([columns[col1].dtype] + [col.dtype for col in other(columns)] * len(keys))
        columns = [Column(name, dtype) for name, dtype in zip(names, dtypes)]

        # Populate the pivoted table.
        pivoted = []
        rows.sort(key=operator.itemgetter(col1))
        for field1, group in itertools.groupby(rows, key=operator.itemgetter(col1)):
            outrow = [field1] + [None] * (len(columns) - 1)
            for row in group:
                index = keys.index(row[col2]) * nother + 1
                outrow[index:index+nother] = other(row)
            pivoted.append(tuple(outrow))

        return columns, pivoted

    # Not reached.
    raise RuntimeError


Column = collections.namedtuple('Column', 'name dtype')


def execute_select(query, entries, options_map):
    """Given a compiled select statement, execute the query.

    Args:
      query: An instance of a query_compile.Query
      entries: A list of directives.
      options_map: A parser's option_map.
    Returns:
      A pair of:
        result_types: A list of (name, data-type) item pairs.
        result_rows: A list of ResultRow tuples of length and types described by
          'result_types'.
    """
    # Figure out the result types that describe what we return.
    result_types = [Column(target.name, target.c_expr.dtype)
                    for target in query.c_targets
                    if target.name is not None]

    # Pre-compute lists of the expressions to evaluate.
    group_indexes = (set(query.group_indexes)
                     if query.group_indexes is not None
                     else query.group_indexes)

    # Indexes of the columns for result rows and order rows.
    result_indexes = [index
                      for index, c_target in enumerate(query.c_targets)
                      if c_target.name]
    order_spec = query.order_spec

    context = create_row_context(entries, options_map)

    # Apply OPEN, CLOSE, CLEAR clauses.
    entries = apply_from_qualifiers(query.c_from, entries, options_map)

    # Dispatch between the non-aggregated queries and aggregated queries.
    c_where = query.c_where
    rows = []

    # Precompute a list of expressions to be evaluated.
    c_target_exprs = [c_target.c_expr for c_target in query.c_targets]

    if query.group_indexes is None:
        # This is a non-aggregated query.

        # Iterate over all the postings once.
        for entry in misc_utils.filter_type(entries, data.Transaction):
            context.entry = entry
            for posting in entry.postings:
                context.rowid += 1
                context.posting = posting
                if c_where is None or c_where(context):
                    values = [c_expr(context) for c_expr in c_target_exprs]
                    rows.append(values)
    else:
        # This is an aggregated query.

        # Precompute lists of non-aggregate and aggregate expressions to
        # evaluate. For aggregate targets, we hunt down the aggregate
        # sub-expressions to evaluate, to avoid recursion during iteration.
        c_nonaggregate_exprs = []
        c_aggregate_exprs = []
        for index, c_expr in enumerate(c_target_exprs):
            if index in group_indexes:
                c_nonaggregate_exprs.append(c_expr)
            else:
                _, aggregate_exprs = query_compile.get_columns_and_aggregates(c_expr)
                c_aggregate_exprs.extend(aggregate_exprs)
        # Note: it is possible that there are no aggregates to compute here. You could
        # have all columns be non-aggregates and group-by the entire list of columns.

        # Pre-allocate handles in aggregation nodes.
        allocator = Allocator()
        for c_expr in c_aggregate_exprs:
            c_expr.allocate(allocator)

        # Iterate over all the postings to evaluate the aggregates.
        agg_store = {}
        for entry in misc_utils.filter_type(entries, data.Transaction):
            context.entry = entry
            for posting in entry.postings:
                context.rowid += 1
                context.posting = posting
                if c_where is None or c_where(context):

                    # Compute the non-aggregate expressions.
                    row_key = tuple(c_expr(context) for c_expr in c_nonaggregate_exprs)

                    # Get an appropriate store for the unique key of this row.
                    try:
                        store = agg_store[row_key]
                    except KeyError:
                        # This is a row; create a new store.
                        store = allocator.create_store()
                        for c_expr in c_aggregate_exprs:
                            c_expr.initialize(store)
                        agg_store[row_key] = store

                    # Update the aggregate expressions.
                    for c_expr in c_aggregate_exprs:
                        c_expr.update(store, context)

        # Iterate over all the aggregations.
        for key, store in agg_store.items():
            key_iter = iter(key)
            values = []

            # Finalize the store.
            for c_expr in c_aggregate_exprs:
                c_expr.finalize(store)
            context.store = store

            for index, c_expr in enumerate(c_target_exprs):
                if index in group_indexes:
                    value = next(key_iter)
                else:
                    value = c_expr(context)
                values.append(value)

            # Skip row if HAVING clause expression is false.
            if query.having_index is not None:
                if not values[query.having_index]:
                    continue

            rows.append(values)

    # Order results if requested.
    if order_spec is not None:
        # Process the order-by clauses grouped by their ordering direction.
        for reverse, spec in itertools.groupby(reversed(order_spec), key=operator.itemgetter(1)):
            indexes = reversed([i[0] for i in spec])
            # The rows may contain None values: nullitemgetter()
            # replaces these with a special value that compares
            # smaller than anything else.
            rows.sort(key=nullitemgetter(*indexes), reverse=reverse)

    # Convert results into list of tuples.
    rows = [tuple(row[i] for i in result_indexes) for row in rows]

    # Apply distinct.
    if query.distinct:
        rows = list(misc_utils.uniquify(rows))

    # Apply limit.
    if query.limit is not None:
        rows = rows[:query.limit]

    return result_types, rows
