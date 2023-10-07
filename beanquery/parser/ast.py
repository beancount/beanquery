from __future__ import annotations

import dataclasses
import datetime
import enum
import sys
import textwrap
import typing


if typing.TYPE_CHECKING:
    from typing import Any, Optional, Union


def _indent(text):
    return textwrap.indent(text, '  ')


def _fields(node):
    for field in dataclasses.fields(node):
        if field.repr:
            yield field.name, getattr(node, field.name)


def tosexp(node):
    if isinstance(node, Node):
        return f'({node.__class__.__name__.lower()}\n' + _indent(
            '\n'.join(f'{name.replace("_", "-")}: {tosexp(value)}'
                      for name, value in _fields(node) if value is not None) + ')')
    if isinstance(node, list):
        return '(\n' + _indent('\n'.join(tosexp(i) for i in node)) + ')'
    if isinstance(node, enum.Enum):
        return node.name.lower()
    return repr(node)


def walk(node):
    if isinstance(node, Node):
        for name, child in _fields(node):
            yield from walk(child)
        yield node
    if isinstance(node, list):
        for child in node:
            yield from walk(child)


class Node:
    """Base class for BQL AST nodes."""
    __slots__ = ()
    parseinfo = None

    @property
    def text(self):
        if not self.parseinfo:
            return None
        text = self.parseinfo.tokenizer.text
        return text[self.parseinfo.pos:self.parseinfo.endpos]

    def tosexp(self):
        return tosexp(self)

    def walk(self):
        return walk(self)


def node(name, fields):
    """Manufacture an AST node class."""

    return dataclasses.make_dataclass(
        name,
        [*fields.split(), ('parseinfo', None, dataclasses.field(default=None, compare=False, repr=False))],
        bases=(Node,),
        **({'slots': True} if sys.version_info[:2] >= (3, 10) else {}))


# A 'select' query action.
#
# Attributes:
#   targets: Either a single 'Asterisk' instance of a list of 'Target'
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

# A placeholder in SELECT * or COUNT(*) constructs.
Asterisk = node('Asterisk', '')

# A FROM clause.
#
# Attributes:
#   expression: A tree of expression nodes from the parser.
#   close: A CLOSE clause, either None if absent, a boolean if the clause
#     was present by no date was provided, or a datetime.date instance if
#     a date was provided.
@dataclasses.dataclass(**({'slots': True} if sys.version_info[:2] >= (3, 10) else {}))
class From(Node):
    expression: Optional[Node] = None
    open: Optional[datetime.date] = None
    close: Optional[Union[datetime.date, bool]] = None
    clear: Optional[bool] = None
    parseinfo: Any = dataclasses.field(default=None, compare=False, repr=False)

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
        return f"{self.__class__.__name__}.{self.name}"

# An PIVOT BY clause.
#
# Attributes:
#   columns: A list of group-by expressions, simple Column() or otherwise.
PivotBy = node('PivotBy', 'columns')

# A reference to a table.
#
# Attributes:
#   name: The table name.
Table = node('Table', 'name')

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

Attribute = node('Attribute', 'operand name')

Subscript = node('Subscript', 'operand key')

# A constant node.
#
# Attributes:
#   value: The constant value this represents.
Constant = node('Constant', 'value')

# A query parameter placeholder.
#
# Attributes:
#   name: The placeholder name
Placeholder = node('Placeholder', 'name')

# Base class for unary operators.
#
# Attributes:
#   operand: An expression, the operand of the operator.
UnaryOp = node('UnaryOp', 'operand')

# Base class for binary operators.
#
# Attributes:
#   left: An expression, the left operand.
#   right: An expression, the right operand.
BinaryOp = node('BinaryOp', 'left right')

# Base class for boolean operators.
BoolOp = node('BoolOp', 'args')

# Between
Between = node('Between', 'operand lower upper')

# Negation operator.
class Not(UnaryOp):
    __slots__ = ()

class IsNull(UnaryOp):
    __slots__ = ()

class IsNotNull(UnaryOp):
    __slots__ = ()


# Boolean operators.

class And(BoolOp):
    __slots__ = ()

class Or(BoolOp):
    __slots__ = ()


# Equality and inequality comparison operators.

class Equal(BinaryOp):
    __slots__ = ()

class NotEqual(BinaryOp):
    __slots__ = ()

class Greater(BinaryOp):
    __slots__ = ()

class GreaterEq(BinaryOp):
    __slots__ = ()

class Less(BinaryOp):
    __slots__ = ()

class LessEq(BinaryOp):
    __slots__ = ()


# Regular expression match operator.

class Match(BinaryOp):
    __slots__ = ()


class NotMatch(BinaryOp):
    __slots__ = ()


# Membership operators.

class In(BinaryOp):
    __slots__ = ()

class NotIn(BinaryOp):
    __slots__ = ()


# Arithmetic operators.

class Neg(UnaryOp):
    __slots__ = ()

class Mul(BinaryOp):
    __slots__ = ()

class Div(BinaryOp):
    __slots__ = ()

class Mod(BinaryOp):
    __slots__ = ()

class Add(BinaryOp):
    __slots__ = ()

class Sub(BinaryOp):
    __slots__ = ()
