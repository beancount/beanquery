"""Interpreter for the query language's AST.

This code accepts the abstract syntax tree produced by the query parser,
resolves the column and function names, compiles and interpreter and prepares a
query to be run against a list of entries.
"""
__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import datetime
import re
import operator

from decimal import Decimal
from itertools import product

from dateutil.relativedelta import relativedelta

from beanquery.parser import ast
from beanquery import query_execute
from beanquery import types
from beanquery import tables


FUNCTIONS = collections.defaultdict(list)
OPERATORS = collections.defaultdict(list)


class EvalNode:
    __slots__ = ('dtype',)

    def __init__(self, dtype):
        # The output data type produce by this node. This is intended to be
        # inferred by the nodes on construction.
        assert dtype is not None, "Internal erro: Invalid dtype, must be deduced."
        self.dtype = dtype

    def __eq__(self, other):
        """Override the equality operator to compare the data type and a all attributes
        of this node. This is used by tests for comparing nodes.
        """
        return (isinstance(other, type(self))
                and all(
                    getattr(self, attribute) == getattr(other, attribute)
                    for attribute in self.__slots__))

    def __str__(self):
        return "{}({})".format(type(self).__name__,
                               ', '.join(repr(getattr(self, child))
                                         for child in self.__slots__))
    __repr__ = __str__

    def childnodes(self):
        """Returns the child nodes of this node.
        Yields:
          A list of EvalNode instances.
        """
        for attr in self.__slots__:
            child = getattr(self, attr)
            if isinstance(child, EvalNode):
                yield child
            elif isinstance(child, list):
                for element in child:
                    if isinstance(element, EvalNode):
                        yield element

    def __call__(self, context):
        """Evaluate this node. This is designed to recurse on its children.
        All subclasses must override and implement this method.

        Args:
          context: The evaluation object to which the evaluation need to apply.
            This is either an entry, a Posting instance, or a particular result
            set row from a sub-select. This is the provider for the underlying
            data.
        Returns:
          The evaluated value for this sub-expression tree.
        """
        raise NotImplementedError


class EvalConstant(EvalNode):
    __slots__ = ('value',)

    def __init__(self, value, dtype=None):
        super().__init__(type(value) if dtype is None else dtype)
        self.value = value

    def __call__(self, _):
        return self.value


class EvalUnaryOp(EvalNode):
    __slots__ = ('operand', 'operator')

    def __init__(self, operator, operand, dtype):
        super().__init__(dtype)
        self.operand = operand
        self.operator = operator

    def __call__(self, context):
        operand = self.operand(context)
        return self.operator(operand)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.operator!r})'


class EvalUnaryOpSafe(EvalUnaryOp):

    def __call__(self, context):
        operand = self.operand(context)
        if operand is None:
            return None
        return self.operator(operand)


class EvalBinaryOp(EvalNode):
    __slots__ = ('left', 'right', 'operator')

    def __init__(self, operator, left, right, dtype):
        super().__init__(dtype)
        self.operator = operator
        self.left = left
        self.right = right

    def __call__(self, context):
        left = self.left(context)
        if left is None:
            return None
        right = self.right(context)
        if right is None:
            return None
        return self.operator(left, right)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.left!r}, {self.right!r})'


class EvalBetween(EvalNode):
    __slots__ = ('operand', 'lower', 'upper')

    def __init__(self, operand, lower, upper):
        super().__init__(bool)
        self.operand = operand
        self.lower = lower
        self.upper = upper

    def __call__(self, context):
        operand = self.operand(context)
        if operand is None:
            return None
        lower = self.lower(context)
        if lower is None:
            return None
        upper = self.upper(context)
        if upper is None:
            return None
        return lower <= operand <= upper


def unaryop(op, intypes, outtype, nullsafe=False):
    def decorator(func):
        class Op(EvalUnaryOp if nullsafe else EvalUnaryOpSafe):
            __intypes__ = intypes
            def __init__(self, operand):
                super().__init__(func, operand, outtype)
        Op.__name__ = f'{op.__name__}[{intypes[0].__name__}]'
        OPERATORS[op].append(Op)
        return func
    return decorator


def binaryop(op, intypes, outtype):
    def decorator(func):
        class Op(EvalBinaryOp):
            __intypes__ = intypes
            def __init__(self, left, right):
                super().__init__(func, left, right, outtype)
        Op.__name__ = f'{op.__name__}[{intypes[0].__name__},{intypes[1].__name__}]'
        OPERATORS[op].append(Op)
        return func
    return decorator


def Operator(op, operands):
    op = types.function_lookup(OPERATORS, op, operands)
    if op is not None:
        return op(*operands)
    raise KeyError


unaryop(ast.Not, [types.Any], bool, nullsafe=True)(operator.not_)

@unaryop(ast.Neg, [int], int)
@unaryop(ast.Neg, [Decimal], Decimal)
def neg_(x):
    return -x


@unaryop(ast.IsNull, [types.Any], bool, nullsafe=True)
def null(x):
    return x is None


@unaryop(ast.IsNotNull, [types.Any], bool, nullsafe=True)
def not_null(x):
    return x is not None


@binaryop(ast.Mul, [Decimal, Decimal], Decimal)
@binaryop(ast.Mul, [Decimal, int], Decimal)
@binaryop(ast.Mul, [int, Decimal], Decimal)
@binaryop(ast.Mul, [int, int], int)
def mul_(x, y):
    return x * y


@binaryop(ast.Div, [Decimal, Decimal], Decimal)
@binaryop(ast.Div, [Decimal, int], Decimal)
@binaryop(ast.Div, [int, Decimal], Decimal)
def div_(x, y):
    if y == 0:
        return None
    return x / y


@binaryop(ast.Div, [int, int], Decimal)
def div_int(x, y):
    if y == 0:
        return None
    return Decimal(x) / y


@binaryop(ast.Mod, [int, int], int)
@binaryop(ast.Mod, [Decimal, int], Decimal)
@binaryop(ast.Mod, [int, Decimal], Decimal)
@binaryop(ast.Mod, [Decimal, Decimal], Decimal)
def mod_(x, y):
    if y == 0:
        return None
    return x % y


@binaryop(ast.Add, [Decimal, Decimal], Decimal)
@binaryop(ast.Add, [Decimal, int], Decimal)
@binaryop(ast.Add, [int, Decimal], Decimal)
@binaryop(ast.Add, [int, int], int)
@binaryop(ast.Add, [datetime.date, relativedelta], datetime.date)
@binaryop(ast.Add, [relativedelta, datetime.date], datetime.date)
@binaryop(ast.Add, [relativedelta, relativedelta], relativedelta)
def add_(x, y):
    return x + y


@binaryop(ast.Sub, [Decimal, Decimal], Decimal)
@binaryop(ast.Sub, [Decimal, int], Decimal)
@binaryop(ast.Sub, [int, Decimal], Decimal)
@binaryop(ast.Sub, [int, int], int)
@binaryop(ast.Sub, [datetime.date, relativedelta], datetime.date)
@binaryop(ast.Sub, [relativedelta, datetime.date], datetime.date)
@binaryop(ast.Sub, [relativedelta, relativedelta], datetime.date)
def sub_(x, y):
    return x - y


@binaryop(ast.Add, [datetime.date, int], datetime.date)
def add_date_int(x, y):
    return x + datetime.timedelta(days=y)


@binaryop(ast.Add, [int, datetime.date], datetime.date)
def add_int_date(x, y):
    return y + datetime.timedelta(days=x)


@binaryop(ast.Sub, [datetime.date, int], datetime.date)
def sub_date_int(x, y):
    return x - datetime.timedelta(days=y)


@binaryop(ast.Sub, [datetime.date, datetime.date], int)
def sub_date_date(x, y):
    return (x - y).days


@binaryop(ast.Match, [str, str], bool)
def match_(x, y):
    return bool(re.search(y, x, re.IGNORECASE))


@binaryop(ast.NotMatch, [str, str], bool)
def not_match_(x, y):
    return not bool(re.search(y, x, re.IGNORECASE))


@binaryop(ast.In, [types.Any, set], bool)
@binaryop(ast.In, [types.Any, list], bool)
@binaryop(ast.In, [types.Any, dict], bool)
def in_(x, y):
    return operator.contains(y, x)


@binaryop(ast.NotIn, [types.Any, set], bool)
@binaryop(ast.NotIn, [types.Any, list], bool)
@binaryop(ast.NotIn, [types.Any, dict], bool)
def not_in_(x, y):
    return not operator.contains(y, x)


_comparisons = [
    (ast.Equal, operator.eq),
    (ast.NotEqual, operator.ne),
    (ast.Greater, operator.gt),
    (ast.GreaterEq, operator.ge),
    (ast.Less, operator.lt),
    (ast.LessEq, operator.le),
]

_intypes = [
    [int, int],
    [Decimal, int],
    [int, Decimal],
    [Decimal, Decimal],
    [datetime.date, datetime.date],
    [str, str],
]

for node, op in _comparisons:
    for intypes in _intypes:
        binaryop(node, intypes, bool)(op)

_comparable = [
    # lists of types that can be compared with each other
    [int, Decimal],
    [datetime.date],
    [str],
]

for comparable in _comparable:
    for intypes in product(comparable, repeat=3):
        class Between(EvalBetween):
            __intypes__ = list(intypes)
        OPERATORS[ast.Between].append(Between)


class EvalAnd(EvalNode):
    __slots__ = ('args',)

    def __init__(self, args):
        super().__init__(bool)
        self.args = args

    def __call__(self, context):
        for arg in self.args:
            value = arg(context)
            if value is None:
                return None
            if not value:
                return False
        return True


class EvalOr(EvalNode):
    __slots__ = ('args',)

    def __init__(self, args):
        super().__init__(bool)
        self.args = args

    def __call__(self, context):
        r = False
        for arg in self.args:
            value = arg(context)
            if value is None:
                r = None
            if value:
                return True
        return r


class EvalCoalesce(EvalNode):
    __slots__ = ('args',)

    def __init__(self, args):
        super().__init__(args[0].dtype)
        self.args = args

    def __call__(self, context):
        for arg in self.args:
            value = arg(context)
            if value is not None:
                return value
        return None


class EvalFunction(EvalNode):
    __slots__ = ('operands',)

    # Type constraints on the input arguments.
    __intypes__ = []

    def __init__(self, operands, dtype):
        super().__init__(dtype)
        self.operands = operands


class EvalGetItem(EvalNode):
    __slots__ = ('operand', 'key')

    def __init__(self, operand, key):
        super().__init__(object)
        self.operand = operand
        self.key = key

    def __call__(self, context):
        operand = self.operand(context)
        if operand is None:
            return None
        return operand.get(self.key)


class EvalGetter(EvalNode):
    __slots__ = ('operand', 'getter')

    def __init__(self, operand, getter, dtype):
        super().__init__(dtype)
        self.operand = operand
        self.getter = getter

    def __call__(self, context):
        operand = self.operand(context)
        if operand is None:
            return None
        return self.getter(operand)


class EvalColumn(EvalNode):
    pass


class EvalAggregator(EvalFunction):
    pure = False

    def __init__(self, operands, dtype=None):
        super().__init__(operands, dtype or operands[0].dtype)
        self.value = None

    def allocate(self, allocator):
        """Allocate handles to store data for a node's aggregate storage.

        This is called once before beginning aggregations. If you need any
        kind of per-aggregate storage during the computation phase, get it
        in this method.

        Args:
          allocator: An instance of Allocator, on which you can call allocate() to
            obtain a handle for a slot to store data on store objects later on.
        """
        self.handle = allocator.allocate()

    def initialize(self, store):
        """Initialize this node's aggregate data.

        Args:
          store: An object indexable by handles appropriated during allocate().
        """
        store[self.handle] = self.dtype()
        self.value = None

    def update(self, store, context):
        """Evaluate this node. This is designed to recurse on its children.

        Args:
          store: An object indexable by handles appropriated during allocate().
          context: The object to which the evaluation need to apply (see __call__).
        """
        # Do nothing by default.

    def finalize(self, store):
        """Finalize this node's aggregate data.

        Args:
          store: An object indexable by handles appropriated during allocate().
        """
        self.value = store[self.handle]

    def __call__(self, context):
        """Return the value on evaluation.

        Args:
          context: The evaluation object to which the evaluation need to apply.
        Returns:
          The final aggregated value.
        """
        return self.value


class SubqueryTable(tables.Table):
    def __init__(self, subquery):
        self.columns = {}
        self.subquery = subquery
        for i, target in enumerate(target for target in subquery.c_targets if target.name is not None):
            column = self.column(i, target.name, target.c_expr.dtype)
            self.columns[target.name] = column()

    @staticmethod
    def column(i, name, dtype):
        class Column(EvalColumn):
            def __init__(self):
                super().__init__(dtype)
            __call__ = staticmethod(operator.itemgetter(i))
        return Column

    def __iter__(self):
        columns, rows = query_execute.execute_query(self.subquery)
        return iter(rows)


# A compiled target.
#
# Attributes:
#   c_expr: A compiled expression tree (an EvalNode root node).
#   name: The name of the target. If None, this is an invisible
#     target that gets evaluated but not displayed.
#   is_aggregate: A boolean, true if 'c_expr' is an aggregate.
EvalTarget = collections.namedtuple('EvalTarget', 'c_expr name is_aggregate')


# A compiled query, ready for execution.
#
# Attributes:
#   c_targets: A list of compiled targets (instancef of EvalTarget).
#   c_where: An instance of EvalNode, a compiled expression tree, for postings.
#   group_indexes: A list of integers that describe which target indexes to
#     group by. All the targets referenced here should be non-aggregates. In fact,
#     this list of indexes should always cover all non-aggregates in 'c_targets'.
#     And this list may well include some invisible columns if only specified in
#     the GROUP BY clause.
#   order_spec: A list of (integer indexes, sort order) tuples.
#     This list may refer to either aggregates or non-aggregates.
#   limit: An optional integer used to cut off the number of result rows returned.
#   distinct: An optional boolean that requests we should uniquify the result rows.
EvalQuery = collections.namedtuple('EvalQuery', ('table c_targets c_where '
                                                 'group_indexes having_index '
                                                 'order_spec '
                                                 'limit distinct'))


# A compiled query with a PIVOT BY clause.
#
# The PIVOT BY clause causes the structure of the returned table to be
# fundamentally alterede, thus it makes sense to model it as a
# distinct operation.
#
# Attributes:
#   query: The underlying EvalQuery.
#   pivots: The pivot columns indexes
EvalPivot = collections.namedtuple('EvalPivot', 'query pivots')


# A compiled print statement, ready for execution.
#
# Attributes:
#   table: Table to print
#   where: Filtering expression, EvalNode instance.
EvalPrint = collections.namedtuple('EvalPrint', 'table where')
