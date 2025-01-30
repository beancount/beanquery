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
    def __init__(self, columns):
        self.wrap = hashable.make(columns)

    def __call__(self, iterable):
        wrap = self.wrap
        seen = set()
        add = seen.add
        for obj in iterable:
            h = wrap(obj)
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


def execute_select(query):
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
    result_types = tuple(cursor.Column(target.name, target.c_expr.dtype)
                         for target in query.c_targets
                         if target.name is not None)

    # Pre-compute lists of the expressions to evaluate.
    group_indexes = (set(query.group_indexes)
                     if query.group_indexes is not None
                     else query.group_indexes)

    # Indexes of the columns for result rows and order rows.
    result_indexes = [index
                      for index, c_target in enumerate(query.c_targets)
                      if c_target.name]
    order_spec = query.order_spec

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

            rows.append(values)

    # Apply ORDER BY.
    if order_spec is not None:
        # Process the order-by clauses grouped by their ordering direction.
        for reverse, spec in itertools.groupby(reversed(order_spec), key=operator.itemgetter(1)):
            indexes = reversed([i[0] for i in spec])
            # The rows may contain None values: nullitemgetter()
            # replaces these with a special value that compares
            # smaller than anything else.
            rows.sort(key=nullitemgetter(*indexes), reverse=reverse)

    # Extract results set and convert into tuples.
    rows = (tuple(row[i] for i in result_indexes) for row in rows)

    # Apply DISTINCT.
    if query.distinct:
        unique = Unique(result_types)
        rows = unique(rows)

    # Apply LIMIT.
    if query.limit is not None:
        rows = itertools.islice(rows, query.limit)

    return result_types, list(rows)
