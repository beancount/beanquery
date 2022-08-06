"""Interpreter for the query language's AST.

This code accepts the abstract syntax tree produced by the query parser,
resolves the column and function names, compiles and interpreter and prepares a
query to be run against a list of entries.
"""
__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import copy
import datetime
import re
import operator

from decimal import Decimal

from beancount.core import inventory
from beanquery import types
from beanquery import parser
from beanquery.parser import ast


# A global constant which sets whether we support inferred/implicit group-by
# semantics.
SUPPORT_IMPLICIT_GROUPBY = True


class CompilationError(Exception):
    """A compiler/interpreter error."""

    def __init__(self, message, ast=None):
        super().__init__(message)
        self.parseinfo = ast.parseinfo if ast is not None else None


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


# Note: We ought to implement implicit type promotion here,
# e.g., int -> float -> Decimal.

# Note(2): This does not support multiplication on Amount, Position, Inventory.
# We need to rewrite the evaluator to support types in order to do this
# properly.

OPERATORS = collections.defaultdict(list)


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


@unaryop(ast.IsNull, [object], bool, nullsafe=True)
def null(x):
    return x is None


@unaryop(ast.IsNotNull, [object], bool, nullsafe=True)
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
    return x / y


@binaryop(ast.Div, [int, int], Decimal)
def div_int(x, y):
    return Decimal(x) / y


@binaryop(ast.Add, [Decimal, Decimal], Decimal)
@binaryop(ast.Add, [Decimal, int], Decimal)
@binaryop(ast.Add, [int, Decimal], Decimal)
@binaryop(ast.Add, [int, int], int)
def add_(x, y):
    return x + y


@binaryop(ast.Sub, [Decimal, Decimal], Decimal)
@binaryop(ast.Sub, [Decimal, int], Decimal)
@binaryop(ast.Sub, [int, Decimal], Decimal)
@binaryop(ast.Sub, [int, int], int)
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
        for arg in args:
            if arg.dtype != args[0].dtype:
                dtypes = ', '.join(arg.dtype.__name__ for arg in args)
                raise CompilationError(
                    f"coalesce() function arguments must have uniform type, found: {dtypes}")
        super().__init__(args[0].dtype)
        self.args = args

    def __call__(self, context):
        for arg in self.args:
            value = arg(context)
            if value is not None:
                return value
        return None


# pylint: disable=abstract-method
class EvalFunction(EvalNode):
    __slots__ = ('operands',)

    # Type constraints on the input arguments.
    __intypes__ = []

    def __init__(self, operands, dtype):
        super().__init__(dtype)
        self.operands = operands


class EvalColumn(EvalNode):
    pass


class EvalAggregator(EvalFunction):
    def __init__(self, operands, dtype=None):
        super().__init__(operands, dtype or operands[0].dtype)

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
        """Initialize this node's aggregate data. If the node is not an aggregate,
        simply initialize the subnodes. Override this method in the aggregator
        if you need data for storage.

        Args:
          store: An object indexable by handles appropriated during allocate().
        """
        store[self.handle] = self.dtype()

    def update(self, store, context):
        """Evaluate this node. This is designed to recurse on its children.

        Args:
          store: An object indexable by handles appropriated during allocate().
          context: The object to which the evaluation need to apply (see __call__).
        """
        # Do nothing by default.

    def finalize(self, store):
        """Finalize this node's aggregate data and return it.

        For aggregate methods, this finalizes the node and returns the final
        value. The context node will be the alloc instead of the context object.

        Args:
          store: An object indexable by handles appropriated during allocate().
        """
        # Do nothing by default.

    def __call__(self, context):
        """Return the value on evaluation.

        Args:
          context: The evaluation object to which the evaluation need to apply.
            This is either an entry, a Posting instance, or a particular result
            set row from a sub-select. This is the provider for the underlying
            data.
        Returns:
          The final aggregated value.
        """
        return context.store[self.handle]


class CompilationEnvironment:
    """Base class for all compilation contexts. A compilation context provides
    column accessors specific to the particular row objects that we will access.
    """
    # Maps of names to evaluators for columns and functions.
    columns = {}
    functions = {}


def compile_expression(expr, environ):
    """Bind an expression to its execution context.

    Args:
      expr: The root node of an expression.
      environ: An CompilationEnvironment instance.
    Returns:
      The root node of a bound expression.
    """

    if expr is None:
        return None

    if isinstance(expr, ast.Column):
        column = environ.columns.get(expr.name)
        if column is not None:
            return column
        raise CompilationError(f'column "{expr.name}" does not exist', expr)

    if isinstance(expr, ast.Or):
        return EvalOr([compile_expression(arg, environ) for arg in expr.args])

    if isinstance(expr, ast.And):
        return EvalAnd([compile_expression(arg, environ) for arg in expr.args])

    if isinstance(expr, ast.Function):
        operands = [compile_expression(operand, environ) for operand in expr.operands]
        if expr.fname == 'coalesce':
            # coalesce() is parsed like a function call but it does
            # not really fit our model for function evaluation,
            # therefore it gets special threatment here.
            return EvalCoalesce(operands)
        function = types.function_lookup(environ.functions, expr.fname, operands)
        if function is None:
            sig = '{}({})'.format(expr.fname, ', '.join(f'{operand.dtype.__name__.lower()}' for operand in operands))
            raise CompilationError(f'no function matches "{sig}" name and argument types', expr)
        function = function(operands)
        # Constants folding.
        if all(isinstance(operand, EvalConstant) for operand in operands) and function.pure:
            return EvalConstant(function(None), function.dtype)
        return function

    if isinstance(expr, ast.UnaryOp):
        operand = compile_expression(expr.operand, environ)
        function = types.function_lookup(OPERATORS, type(expr), [operand])
        if function is None:
            raise CompilationError(
                f'Operator {type(expr).__name__.lower()}({operand.dtype.__name__}) not supported')
        function = function(operand)
        # Constants folding.
        if isinstance(operand, EvalConstant):
            return EvalConstant(function(None), function.dtype)
        return function

    if isinstance(expr, ast.BinaryOp):
        left = compile_expression(expr.left, environ)
        right = compile_expression(expr.right, environ)

        candidates = OPERATORS[type(expr)]
        while True:
            intypes = [left.dtype, right.dtype]
            for op in candidates:
                if op.__intypes__ == intypes:
                    function = op(left, right)
                    # Constants folding.
                    if isinstance(left, EvalConstant) and isinstance(right, EvalConstant):
                        return EvalConstant(function(None), function.dtype)
                    return function

            # Implement type inference when one of the operands is not strongly typed.
            if left.dtype is object and right.dtype is not object:
                target = right.dtype
                if target is int:
                    # The Beancount parser does not emit int typed
                    # values, thus casting to int is only going to
                    # loose information. Promote to decimal.
                    target = Decimal
                name = types.MAP.get(target)
                if name is None:
                    break
                left = types.function_lookup(environ.functions, name, [left])([left])
                continue
            if right.dtype is object and left.dtype is not object:
                target = left.dtype
                if target is int:
                    # The Beancount parser does not emit int typed
                    # values, thus casting to int is only going to
                    # loose information. Promote to decimal.
                    target = Decimal
                name = types.MAP.get(target)
                if name is None:
                    break
                right = types.function_lookup(environ.functions, name, [right])([right])
                continue

            # Failure.
            break

        raise CompilationError(
            f'Operator {type(expr).__name__.lower()}('
            f'{left.dtype.__name__}, {right.dtype.__name__}) not supported')

    if isinstance(expr, ast.Constant):
        return EvalConstant(expr.value)

    assert False, f"invalid expression: {expr}"


def get_columns_and_aggregates(node):
    """Find the columns and aggregate nodes below this tree.

    All nodes under aggregate nodes are ignored.

    Args:
      node: An instance of EvalNode.
    Returns:
      A pair of (columns, aggregates), both of which are lists of EvalNode instances.
        columns: The list of all columns accessed not under an aggregate node.
        aggregates: The list of all aggregate nodes.
    """
    columns = []
    aggregates = []
    _get_columns_and_aggregates(node, columns, aggregates)
    return columns, aggregates


def _get_columns_and_aggregates(node, columns, aggregates):
    """Walk down a tree of nodes and fetch the column accessors and aggregates.

    This function ignores all nodes under aggregate nodes.

    Args:
      node: An instance of EvalNode.
      columns: An accumulator for columns found so far.
      aggregate: An accumulator for aggregate notes found so far.
    """
    if isinstance(node, EvalAggregator):
        aggregates.append(node)
    elif isinstance(node, EvalColumn):
        columns.append(node)
    else:
        for child in node.childnodes():
            _get_columns_and_aggregates(child, columns, aggregates)


def is_aggregate(node):
    """Return true if the node is an aggregate.

    Args:
      node: An instance of EvalNode.
    Returns:
      A boolean.
    """
    # Note: We could be a tiny bit more efficient here, but it doesn't matter
    # much. Performance of the query compilation matters very little overall.
    _, aggregates = get_columns_and_aggregates(node)
    return bool(aggregates)


def is_hashable_type(node):
    """Return true if the node is of a hashable type.

    Args:
      node: An instance of EvalNode.
    Returns:
      A boolean.
    """
    return not issubclass(node.dtype, inventory.Inventory)


# A compiled target.
#
# Attributes:
#   c_expr: A compiled expression tree (an EvalNode root node).
#   name: The name of the target. If None, this is an invisible
#     target that gets evaluated but not displayed.
#   is_aggregate: A boolean, true if 'c_expr' is an aggregate.
EvalTarget = collections.namedtuple('EvalTarget', 'c_expr name is_aggregate')


def get_target_name(target):
    """Compute the target name.

    This uses the same algorithm used by SQLite. If the target has an
    AS clause assigning it a name, that will be the name used. If the
    target refers directly to a column, then the target name is the
    column name. Otherwise use the expression text.

    """
    if target.name is not None:
        return target.name
    if isinstance(target.expression, ast.Column):
        return target.expression.name
    return target.expression.text.strip()


def compile_targets(targets, environ):
    """Compile the targets and check for their validity. Process wildcard.

    Args:
      targets: A list of target expressions from the parser.
      environ: A compilation context for the targets.
    Returns:
      A list of compiled target expressions with resolved names.
    """
    # Bind the targets expressions to the execution context.
    if isinstance(targets, ast.Wildcard):
        # Insert the full list of available columns.
        targets = [ast.Target(ast.Column(name), None)
                   for name in environ.wildcard_columns]

    # Compile targets.
    c_targets = []
    for target in targets:
        c_expr = compile_expression(target.expression, environ)
        name = get_target_name(target)
        c_targets.append(EvalTarget(c_expr, name, is_aggregate(c_expr)))

        columns, aggregates = get_columns_and_aggregates(c_expr)

        # Check for mixed aggregates and non-aggregates.
        if columns and aggregates:
            raise CompilationError(
                "Mixed aggregates and non-aggregates are not allowed")

        # Check for aggregates of aggregates.
        for aggregate in aggregates:
            for child in aggregate.childnodes():
                if is_aggregate(child):
                    raise CompilationError(
                        "Aggregates of aggregates are not allowed")

    return c_targets


def compile_group_by(group_by, c_targets, environ):
    """Process a group-by clause.

    Args:
      group_by: A GroupBy instance as provided by the parser.
      c_targets: A list of compiled target expressions.
      environ: A compilation context to be used to evaluate GROUP BY expressions.
    Returns:
      A tuple of
       new_targets: A list of new compiled target nodes.
       group_indexes: If the query is an aggregate query, a list of integer
         indexes to be used for processing grouping. Note that this list may be
         empty (in the case of targets with only aggregates). On the other hand,
         if this is not an aggregated query, this is set to None. So do
         distinguish the empty list vs. None.
    """
    new_targets = copy.copy(c_targets)
    c_target_expressions = [c_target.c_expr for c_target in c_targets]

    group_indexes = []
    having_index = None

    if group_by:
        assert group_by.columns, "Internal error with GROUP-BY parsing"

        # Compile group-by expressions and resolve them to their targets if
        # possible. A GROUP-BY column may be one of the following:
        #
        # * A reference to a target by name.
        # * A reference to a target by index (starting at one).
        # * A new, non-aggregate expression.
        #
        # References by name are converted to indexes. New expressions are
        # inserted into the list of targets as invisible targets.
        targets_name_map = {target.name: index for index, target in enumerate(c_targets)}
        for column in group_by.columns:
            index = None

            # Process target references by index.
            if isinstance(column, int):
                index = column - 1
                if not 0 <= index < len(c_targets):
                    raise CompilationError(
                        "Invalid GROUP-BY column index {}".format(column))

            else:
                # Process target references by name. These will be parsed as
                # simple Column expressions. If they refer to a target name, we
                # resolve them.
                if isinstance(column, ast.Column):
                    name = column.name
                    index = targets_name_map.get(name, None)

                # Otherwise we compile the expression and add it to the list of
                # targets to evaluate and index into that new target.
                if index is None:
                    c_expr = compile_expression(column, environ)

                    # Check if the new expression is an aggregate.
                    aggregate = is_aggregate(c_expr)
                    if aggregate:
                        raise CompilationError(
                            "GROUP-BY expressions may not be aggregates: '{}'".format(
                                column))

                    # Attempt to reconcile the expression with one of the existing
                    # target expressions.
                    try:
                        index = c_target_expressions.index(c_expr)
                    except ValueError:
                        # Add the new target. 'None' for the target name implies it
                        # should be invisible, not to be rendered.
                        index = len(new_targets)
                        new_targets.append(EvalTarget(c_expr, None, aggregate))
                        c_target_expressions.append(c_expr)

            assert index is not None, "Internal error, could not index group-by reference."
            group_indexes.append(index)

            # Check that the group-by column references a non-aggregate.
            c_expr = new_targets[index].c_expr
            if is_aggregate(c_expr):
                raise CompilationError(
                    "GROUP-BY expressions may not reference aggregates: '{}'".format(
                        column))

            # Check that the group-by column has a supported hashable type.
            if not is_hashable_type(c_expr):
                raise CompilationError(
                    "GROUP-BY a non-hashable type is not supported: '{}'".format(
                        column))

        # Compile HAVING clause.
        if group_by.having is not None:
            c_expr = compile_expression(group_by.having, environ)
            if not is_aggregate(c_expr):
                raise CompilationError("The HAVING clause must be an aggregate expression")
            having_index = len(new_targets)
            new_targets.append(EvalTarget(c_expr, None, True))
            c_target_expressions.append(c_expr)

    else:
        # If it does not have a GROUP-BY clause...
        aggregate_bools = [c_target.is_aggregate for c_target in c_targets]
        if any(aggregate_bools):
            # If the query is an aggregate query, check that all the targets are
            # aggregates.
            if all(aggregate_bools):
                # pylint: disable=use-implicit-booleaness-not-comparison
                # FIXME: shold we really be checking for the empty
                # list or is checking for a false value enough?
                assert group_indexes == []
            else:
                # If some of the targets aren't aggregates, automatically infer
                # that they are to be implicit group by targets. This makes for
                # a much more convenient syntax for our lightweight SQL, where
                # grouping is optional.
                if SUPPORT_IMPLICIT_GROUPBY:
                    group_indexes = [index
                                     for index, c_target in enumerate(c_targets)
                                     if not c_target.is_aggregate]
                else:
                    raise CompilationError(
                        "Aggregate query without a GROUP-BY should have only aggregates")
        else:
            # This is not an aggregate query; don't set group_indexes to
            # anything useful, we won't need it.
            group_indexes = None

    return new_targets[len(c_targets):], group_indexes, having_index


def compile_order_by(order_by, c_targets, environ):
    """Process an order-by clause.

    Args:
      order_by: A OrderBy instance as provided by the parser.
      c_targets: A list of compiled target expressions.
      environ: A compilation context to be used to evaluate ORDER BY expressions.
    Returns:
      A tuple of
       new_targets: A list of new compiled target nodes.
       order_spec: A list of (integer indexes, sort order) tuples.
    """
    if not order_by:
        return [], None

    new_targets = copy.copy(c_targets)
    c_target_expressions = [c_target.c_expr for c_target in c_targets]
    order_spec = []

    # Compile order-by expressions and resolve them to their targets if
    # possible. A ORDER-BY column may be one of the following:
    #
    # * A reference to a target by name.
    # * A reference to a target by index (starting at one).
    # * A new expression, aggregate or not.
    #
    # References by name are converted to indexes. New expressions are
    # inserted into the list of targets as invisible targets.
    targets_name_map = {target.name: index for index, target in enumerate(c_targets)}
    for spec in order_by:
        column = spec.column
        descending = spec.ordering
        index = None

        # Process target references by index.
        if isinstance(column, int):
            index = column - 1
            if not 0 <= index < len(c_targets):
                raise CompilationError(
                    "Invalid ORDER-BY column index {}".format(column))

        else:
            # Process target references by name. These will be parsed as
            # simple Column expressions. If they refer to a target name, we
            # resolve them.
            if isinstance(column, ast.Column):
                name = column.name
                index = targets_name_map.get(name, None)

            # Otherwise we compile the expression and add it to the list of
            # targets to evaluate and index into that new target.
            if index is None:
                c_expr = compile_expression(column, environ)

                # Attempt to reconcile the expression with one of the existing
                # target expressions.
                try:
                    index = c_target_expressions.index(c_expr)
                except ValueError:
                    # Add the new target. 'None' for the target name implies it
                    # should be invisible, not to be rendered.
                    index = len(new_targets)
                    new_targets.append(EvalTarget(c_expr, None, is_aggregate(c_expr)))
                    c_target_expressions.append(c_expr)

        assert index is not None, "Internal error, could not index order-by reference."
        order_spec.append((index, descending))

    return new_targets[len(c_targets):], order_spec


def compile_pivot_by(pivot_by, targets, group_indexes):
    """Compiles a PIVOT BY clause.

    Resolve and validate columns references in the PIVOT BY clause.
    The PIVOT BY clause accepts two name od index references to
    columns in the SELECT targets list. The second columns should be a
    GROUP BY column so that the values of the pivot column are unique.

    """
    if pivot_by is None:
        return None

    indexes = []
    names = {target.name: index for index, target in enumerate(targets)}

    for column in pivot_by.columns:

        # Process target references by index.
        if isinstance(column, int):
            index = column - 1
            if not 0 <= index < len(targets):
                raise CompilationError(f'Invalid PIVOT BY column index {column}')
            indexes.append(index)
            continue

        # Process target references by name.
        if isinstance(column, ast.Column):
            index = names.get(column.name, None)
            if index is None:
                raise CompilationError(f'PIVOT BY column {column!r} is not in the targets list')
            indexes.append(index)
            continue

        # Not reached.
        raise RuntimeError

    # Sanity checks.
    if indexes[0] == indexes[1]:
        raise CompilationError('The two PIVOT BY columns cannot be the same column')
    if indexes[1] not in group_indexes:
        raise CompilationError('The second PIVOT BY column must be a GROUP BY column')

    return indexes


# A compile FROM clause.
#
# Attributes:
#   c_expr: A compiled expression tree (an EvalNode root node).
#   close: (See ast.From.close).
EvalFrom = collections.namedtuple('EvalFrom', 'c_expr open close clear')

def compile_from(from_clause, environ):
    """Compiled a FROM clause as provided by the parser, in the given environment.

    Args:
      select: An instance of ast.Select.
      environ: : A compilation context for evaluating entry filters.
    Returns:
      An instance of Query, ready to be executed.
    """
    if from_clause is None:
        return None

    c_expression = compile_expression(from_clause.expression, environ)

    # Check that the FROM clause does not contain aggregates.
    if c_expression is not None and is_aggregate(c_expression):
        raise CompilationError("Aggregates are not allowed in FROM clause")

    if (isinstance(from_clause.open, datetime.date) and
        isinstance(from_clause.close, datetime.date) and
        from_clause.open > from_clause.close):
        raise CompilationError("Invalid dates: CLOSE date must follow OPEN date")

    return EvalFrom(c_expression,
                    from_clause.open,
                    from_clause.close,
                    from_clause.clear)


# A compiled query, ready for execution.
#
# Attributes:
#   c_targets: A list of compiled targets (instancef of EvalTarget).
#   c_from: An instance of EvalNode, a compiled expression tree, for directives.
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
EvalQuery = collections.namedtuple('EvalQuery', ('c_targets c_from c_where '
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


def compile_select(select, postings_environ, entries_environ):
    """Prepare an AST for a Select statement into a very rudimentary execution tree.
    The execution tree mostly looks much like an AST, but with some nodes
    replaced with knowledge specific to an execution context and eventually some
    basic optimizations.

    Args:
      select: An instance of ast.Select.
      postings_environ: : A compilation environment for evaluating postings expressions.
      entries_environ: : A compilation environment for evaluating entries expressions.
    Returns:
      An instance of EvalQuery, ready to be executed.
    """

    if isinstance(select.from_clause, ast.Select):
        raise CompilationError("Nested SELECT are not supported yet")

    # Bind the FROM clause expressions.
    c_from = compile_from(select.from_clause, entries_environ)

    # Compile the targets.
    c_targets = compile_targets(select.targets, postings_environ)

    # Bind the WHERE expression to the execution environment.
    c_where = compile_expression(select.where_clause, postings_environ)

    # Check that the FROM clause does not contain aggregates. This
    # should never trigger if the compilation environment does not
    # contain any aggregate.
    if c_where is not None and is_aggregate(c_where):
        raise CompilationError("Aggregates are not allowed in WHERE clause")

    # Process the GROUP-BY clause.
    new_targets, group_indexes, having_index = compile_group_by(select.group_by,
                                                                c_targets,
                                                                postings_environ)
    c_targets.extend(new_targets)

    # Process the ORDER-BY clause.
    new_targets, order_spec = compile_order_by(select.order_by,
                                               c_targets,
                                               postings_environ)
    c_targets.extend(new_targets)

    # If this is an aggregate query (it groups, see list of indexes), check that
    # the set of non-aggregates match exactly the group indexes. This should
    # always be the case at this point, because we have added all the necessary
    # targets to the list of group-by expressions and should have resolved all
    # the indexes.
    if group_indexes is not None:
        non_aggregate_indexes = set(index
                                    for index, c_target in enumerate(c_targets)
                                    if not c_target.is_aggregate)
        if non_aggregate_indexes != set(group_indexes):
            missing_names = ['"{}"'.format(c_targets[index].name)
                             for index in non_aggregate_indexes - set(group_indexes)]
            raise CompilationError(
                "All non-aggregates must be covered by GROUP-BY clause in aggregate query; "
                "the following targets are missing: {}".format(",".join(missing_names)))

    query = EvalQuery(c_targets,
                      c_from,
                      c_where,
                      group_indexes,
                      having_index,
                      order_spec,
                      select.limit,
                      select.distinct)

    pivots = compile_pivot_by(select.pivot_by, c_targets, group_indexes)
    if pivots:
        return EvalPivot(query, pivots)

    return query


def transform_journal(journal):
    """Translate a Journal entry into an uncompiled Select statement.

    Args:
      journal: An instance of a Journal object.
    Returns:
      An instance of an uncompiled Select object.
    """
    cooked_select = parser.parse("""

        SELECT
           date,
           flag,
           MAXWIDTH(payee, 48),
           MAXWIDTH(narration, 80),
           account,
           {summary_func}(position),
           {summary_func}(balance)
        {where}

    """.format(where=('WHERE account ~ "{}"'.format(journal.account)
                      if journal.account
                      else ''),
               summary_func=journal.summary_func or ''))

    return ast.Select(cooked_select.targets,
                      journal.from_clause,
                      cooked_select.where_clause,
                      None, None, None, None, None)


def transform_balances(balances):
    """Translate a Balances entry into an uncompiled Select statement.

    Args:
      balances: An instance of a Balance object.
    Returns:
      An instance of an uncompiled Select object.
    """
    ## FIXME: Change the aggregation rules to allow GROUP-BY not to include the
    ## non-aggregate ORDER-BY columns, so we could just GROUP-BY accounts here
    ## instead of having to include the sort-key. I think it should be fine if
    ## the first or last sort-order value gets used, because it would simplify
    ## the input statement.

    cooked_select = parser.parse("""

      SELECT account, SUM({}(position))
      GROUP BY account, ACCOUNT_SORTKEY(account)
      ORDER BY ACCOUNT_SORTKEY(account)

    """.format(balances.summary_func or ""))

    return ast.Select(cooked_select.targets,
                      balances.from_clause,
                      balances.where_clause,
                      cooked_select.group_by,
                      cooked_select.order_by,
                      None, None, None)


# A compiled print statement, ready for execution.
#
# Attributes:
#   c_from: An instance of EvalNode, a compiled expression tree, for directives.
EvalPrint = collections.namedtuple('EvalPrint', 'c_from')

def compile_print(print_stmt, env_entries):
    """Compile a Print statement.

    Args:
      statement: An instance of ast.Print.
      entries_environ: A compilation environment for evaluating entry filters.
    Returns:
      An instance of EvalPrint, ready to be executed.
    """
    c_from = compile_from(print_stmt.from_clause, env_entries)
    return EvalPrint(c_from)


# pylint: disable=redefined-builtin
def compile(statement, postings_environ, entries_environ):
    """Prepare an AST any of the statement into an executable statement.

    Args:
      statement: An instance of the parser's Select, Balances, Journal or Print.
      postings_environ: : A compilation environment for evaluating postings expressions.
      entries_environ: : A compilation environment for evaluating entries expressions.
    Returns:
      An instance of EvalQuery or EvalPrint, ready to be executed.
    Raises:
      CompilationError: If the statement cannot be compiled, or is not one of the
        supported statements.
    """
    if isinstance(statement, ast.Balances):
        statement = transform_balances(statement)
    elif isinstance(statement, ast.Journal):
        statement = transform_journal(statement)

    if isinstance(statement, ast.Select):
        return compile_select(statement, postings_environ, entries_environ)
    if isinstance(statement, ast.Print):
        return compile_print(statement, entries_environ)

    raise CompilationError("Cannot compile a statement of type '{}'".format(type(statement)))
