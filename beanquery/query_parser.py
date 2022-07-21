"""Parser for Beancount Query Language.
"""
__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import datetime
import decimal
import enum

import dateutil.parser
import tatsu

from beanquery.parser import parser


# Convenience alias. Mostly to encapsulate the parser implementation.
ParseError = tatsu.exceptions.ParseError


AST = {}


class Node:
    __slots__ = ()

    def __init_subclass__(cls):
        AST[cls.__name__] = cls

    def __eq__(self, other):
        return isinstance(other, type(self)) and self[:-1] == other[:-1]

    def __repr__(self):
        return self._frmt.format(self) # pylint: disable=no-member


def node(name, attributes):
    """Manufacture an AST node class.

    AST nodes are named tuples wrapped in a class that provides some
    utility methods. A ``parseinfo`` field is addedd to the named
    tuple fields to store parser location information.

    Args:
      name: Class name.
      attributes: Names of the attributes.

    Returns:
      An AST node class definition.

    """
    attributes = attributes.split()
    base = collections.namedtuple('_{}'.format(name), attributes + ['parseinfo'], defaults=[None])
    frmt = '{0.__class__.__name__}(' + ', '.join(f'{attr}={{0.{attr}!r}}' for attr in attributes) + ')'
    return type(name, (Node, base,), {'__slots__': (), '_frmt': frmt})


class BQLSemantics:
    def __init__(self, default_close_date=None):
        self.default_close_date = default_close_date
        self.constructors = AST

    def null(self, value):
        return None

    def integer(self, value):
        return int(value)

    def decimal(self, value):
        return decimal.Decimal(value)

    def date(self, value):
        if value.startswith('#'):
            return dateutil.parser.parse(value[2:-1]).date()
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()

    def string(self, value):
        return value[1:-1]

    def boolean(self, value):
        return value == 'TRUE'

    def identifier(self, value):
        return value.lower()

    def wildcard(self, value):
        return Wildcard()

    def list(self, value):
        return list(value)

    def ordering(self, value):
        return Ordering[value or 'ASC']

    def from_(self, ast, typename):
        if ast['expression'] is None and ast['open'] is None and ast['close'] is None and ast['clear_'] is None:
            raise ParseError('Empty FROM expression is not allowed')
        if ast['close'] is None:
            ast['close'] = self.default_close_date
        return self._default(ast, typename)

    def _default(self, ast, typename=None):
        if typename is not None:
            func = self.constructors[typename]
            return func(**{name.rstrip('_'): value for name, value in ast.items()})
        return ast


class Parser:
    def parse(self, text, default_close_date=None):
        return parser.BQLParser().parse(text, semantics=BQLSemantics(default_close_date))


# A 'select' query action.
#
# Attributes:
#   targets: Either a single 'Wildcard' instance of a list of 'Target'
#     instances.
#   from_clause: An instance of 'From', or None if absent.
#   where_clause: A root expression node, or None if absent.
#   group_by: An instance of 'GroupBy', or None if absent.
#   order_by: An instance of 'OrderBy', or None if absent.
#   pivot_by: An instance of 'PivotBy', or None if absent.
#   limit: An integer, or None is absent.
#   distinct: A boolean value (True), or None if absent.
Select = node('Select', 'targets from_clause where_clause group_by order_by pivot_by limit distinct')

# A select query that produces final balances for accounts.
# This is equivalent to
#
#   SELECT account, sum(position)
#   FROM ...
#   WHERE ...
#   GROUP BY account
#
# Attributes:
#   summary_func: A method on an inventory to call on the position column.
#     May be to extract units, value at cost, etc.
#   from_clause: An instance of 'From', or None if absent.
Balances = node('Balances', 'summary_func from_clause where_clause')

# A select query that produces a journal of postings.
# This is equivalent to
#
#   SELECT date, flag, payee, narration, ...  FROM <from_clause>
#   WHERE account = <account>
#
# Attributes:
#   account: A string, the name of the account to restrict to.
#   summary_func: A method on an inventory to call on the position column.
#     May be to extract units, value at cost, etc.
#   from_clause: An instance of 'From', or None if absent.
Journal = node('Journal', 'account summary_func from_clause')

# A query that will simply print the selected entries in Beancount format.
#
# Attributes:
#   from_clause: An instance of 'From', or None if absent.
Print = node('Print', 'from_clause')

# A parsed SELECT column or target.
#
# Attributes:
#   expression: A tree of expression nodes from the parser.
#   name: A string, the given name of the target (given by "AS <name>").
Target = node('Target', 'expression name')

# A wildcard target. This replaces the list in Select.targets.
Wildcard = node('Wildcard', '')

# A FROM clause.
#
# Attributes:
#   expression: A tree of expression nodes from the parser.
#   close: A CLOSE clause, either None if absent, a boolean if the clause
#     was present by no date was provided, or a datetime.date instance if
#     a date was provided.
From = node('From', 'expression open close clear')

# A GROUP BY clause.
#
# Attributes:
#   columns: A list of group-by expressions, simple Column() or otherwise.
#   having: An expression tree for the optional HAVING clause, or None.
GroupBy = node('GroupBy', 'columns having')

# An ORDER BY clause.
#
# Attributes:
#   column: order-by expression, simple Column() or otherwise.
#   ordering: The sort order as an Ordering enum value.
OrderBy = node('OrderBy', 'column ordering')

class Ordering(enum.IntEnum):
    # The enum values are chosen in this way to be able to use them
    # directly as the reverse parameter to the list sort() method.
    ASC = 0
    DESC = 1

    def __repr__(self):
        return "%s.%s" % (self.__class__.__name__, self.name)

# An PIVOT BY clause.
#
# Attributes:
#   columns: A list of group-by expressions, simple Column() or otherwise.
PivotBy = node('PivotBy', 'columns')

# Nodes used in expressions. The meaning should be self-explanatory. This is
# your run-of-the-mill hierarchical logical expression nodes. Any of these nodes
# equivalent form "an expression."

# A reference to a column.
#
# Attributes:
#   name: A string, the name of the column to access.
Column = node('Column', 'name')

# A function call.
#
# Attributes:
#   fname: A string, the name of the function.
#   operands: A list of other expressions, the arguments of the function to
#     evaluate. This is possibly an empty list.
Function = node('Function', 'fname operands')

# A constant node.
#
# Attributes:
#   value: The constant value this represents.
Constant = node('Constant', 'value')

# Base classes for unary operators.
#
# Attributes:
#   operand: An expression, the operand of the operator.
UnaryOp = node('UnaryOp', 'operand')

# Base classes for binary operators.
#
# Attributes:
#   left: An expression, the left operand.
#   right: An expression, the right operand.
BinaryOp = node('BinaryOp', 'left right')

# Base class for boolean operators.
BoolOp = node('BoolOp', 'args')

# Negation operator.
class Not(UnaryOp): pass

class IsNull(UnaryOp): pass
class IsNotNull(UnaryOp): pass

# Boolean operators.
class And(BoolOp): pass
class Or(BoolOp): pass

# Equality and inequality comparison operators.
class Equal(BinaryOp): pass
class NotEqual(BinaryOp): pass
class Greater(BinaryOp): pass
class GreaterEq(BinaryOp): pass
class Less(BinaryOp): pass
class LessEq(BinaryOp): pass

# A regular expression match operator.
class Match(BinaryOp): pass

# Membership operators.
class Contains(BinaryOp): pass

# Arithmetic operators.
class Neg(UnaryOp): pass
class Mul(BinaryOp): pass
class Div(BinaryOp): pass
class Add(BinaryOp): pass
class Sub(BinaryOp): pass


def get_expression_name(expr):
    """Come up with a reasonable identifier for an expression.

    Args:
      expr: An expression node.
    """
    if isinstance(expr, Column):
        return expr.name.lower()

    if isinstance(expr, Function):
        operands = ', '.join(get_expression_name(operand) for operand in expr.operands)
        return f'{expr.fname.lower()}({operands})'

    if isinstance(expr, Constant):
        if isinstance(expr.value, str):
            return repr(expr.value)
        return str(expr.value)

    if isinstance(expr, UnaryOp):
        operand = get_expression_name(expr.operand)
        return f'{type(expr).__name__.lower()}({operand})'

    if isinstance(expr, BinaryOp):
        operands = ', '.join(get_expression_name(operand) for operand in (expr.left, expr.right))
        return f'{type(expr).__name__.lower()}({operands})'

    raise NotImplementedError
