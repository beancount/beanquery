"""Execution of interpreter on data rows.
"""
__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import itertools
import operator

from . import compiler
from . import hashable
from . import cursor



class Unique:
    """Generator that yields only the first occurrence of each unique row.

    Handles non-hashable column types (e.g., Inventory) by wrapping values
    into hashable representations via hashable.make().

    Args:
      columns: Column types for hashable wrapping.
      key: Optional function to extract the value to hash from each row.
    """

    def __init__(self, columns, key=None):
        self.wrap = hashable.make(columns)
        self.key = key

    def __call__(self, iterable):
        wrap = self.wrap
        key = self.key
        seen = set()
        add = seen.add
        for obj in iterable:
            k = key(obj) if key else obj
            h = wrap(k)
            if h not in seen:
                add(h)
                yield obj


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
        items = (item, *items)
        def func(obj):
            r = []
            for i in items:
                value = obj[i]
                r.append(value if value is not None else NULL)
            return tuple(r)
        return func
    def func(obj):
        value = obj[item]
        return value if value is not None else NULL
    return func


def execute_query(query):
    """Execute a compiled query with ORDER BY and LIMIT.

    Args:
      query: An instance of EvalQuery wrapping an EvalSelect.
    Returns:
      A pair of (result_types, result_rows).
    """
    result_types, rows, visible_mask = query.select()

    # ORDER BY requires materialization.
    if query.order_spec:
        rows = list(rows)
        for reverse, spec in itertools.groupby(reversed(query.order_spec), key=operator.itemgetter(1)):
            indexes = reversed([i[0] for i in spec])
            rows.sort(key=nullitemgetter(*indexes), reverse=reverse)

    # Extract visible columns.
    visible_indexes = [i for i, v in enumerate(visible_mask) if v]
    result_types = tuple(result_types[i] for i in visible_indexes)
    rows = (tuple(row[i] for i in visible_indexes) for row in rows)

    # Apply LIMIT.
    if query.limit is not None:
        rows = itertools.islice(rows, query.limit)

    return result_types, list(rows)


def execute_select(query):
    """Given a compiled select statement, execute the query.

    Args:
      query: An instance of EvalSelect.
    Returns:
      A tuple of:
        result_types: A list of Column(name, dtype) for ALL columns.
        result_rows: A list of tuples with ALL columns (including invisible).
        visible_mask: A list of bools, True if column is visible.
    """
    # Figure out the result types for ALL columns.
    result_types = tuple(cursor.Column(target.name, target.c_expr.dtype)
                         for target in query.c_targets)

    # Track which columns are visible (have a name).
    visible_mask = [target.name is not None for target in query.c_targets]

    # Pre-compute lists of the expressions to evaluate.
    group_indexes = (set(query.group_indexes)
                     if query.group_indexes is not None
                     else query.group_indexes)

    # Dispatch between the non-aggregated queries and aggregated queries.
    c_where = query.c_where
    rows = []

    # Precompute a list of expressions to be evaluated.
    c_target_exprs = [c_target.c_expr for c_target in query.c_targets]

    if query.group_indexes is None:
        # This is a non-aggregated query.

        # Iterate over all the postings once.
        for context in query.table:
            if c_where is None or c_where(context):
                values = tuple(c_expr(context) for c_expr in c_target_exprs)
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
                _, aggregate_exprs = compiler.get_columns_and_aggregates(c_expr)
                c_aggregate_exprs.extend(aggregate_exprs)
        # Note: it is possible that there are no aggregates to compute here. You could
        # have all columns be non-aggregates and group-by the entire list of columns.

        # Pre-allocate handles in aggregation nodes.
        allocator = Allocator()
        for c_expr in c_aggregate_exprs:
            c_expr.allocate(allocator)

        def create():
            # Create a new row in the aggregates store.
            store = allocator.create_store()
            for c_expr in c_aggregate_exprs:
                c_expr.initialize(store)
            return store

        context = None
        aggregates = collections.defaultdict(create)

        # Iterate over all the postings to evaluate the aggregates.
        for context in query.table:
            if c_where is None or c_where(context):

                # Compute the non-aggregate expressions.
                key = tuple(c_expr(context) for c_expr in c_nonaggregate_exprs)

                # Get an appropriate store for the unique key of this row.
                store = aggregates[key]

                # Update the aggregate expressions.
                for c_expr in c_aggregate_exprs:
                    c_expr.update(store, context)

        # Iterate over all the aggregations.
        for key, store in aggregates.items():
            key_iter = iter(key)
            values = []

            # Finalize the store.
            for c_expr in c_aggregate_exprs:
                c_expr.finalize(store)

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

            rows.append(tuple(values))

    # DISTINCT must operate on visible columns only, ignoring columns that were
    # auto-added for GROUP BY or ORDER BY. Unique returns a generator to create
    # a lazy pipeline so LIMIT can cut off early.
    if query.distinct:
        visible_indexes = [i for i, v in enumerate(visible_mask) if v]
        visible_types = tuple(result_types[i] for i in visible_indexes)
        unique = Unique(visible_types, key=lambda row: tuple(row[i] for i in visible_indexes))
        rows = unique(rows)

    return result_types, rows, visible_mask
