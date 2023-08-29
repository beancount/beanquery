import datetime
import decimal


class AnyType:
    """As written on the tin."""
    __slots__ = ()
    __name__ = 'any'

    def __eq__(self, other):
        """Compares equal to any other type."""
        return isinstance(other, type)


# Used in BQL functions signatures for arguments that can have any type.
Any = AnyType()


class AsteriskType:
    __slots__ = ()
    __name__ = '*'

    def __eq__(self, other):
        return isinstance(other, self.__class__)


# Used for COUNT(*)
Asterisk = AsteriskType()


# Keep track of the defined structured types to allow introspection.
TYPES = {}


class Structure:
    """Base class for structured data types."""
    name = None
    columns = {}

    def __init_subclass__(cls):
        if cls.name:
            TYPES[cls.name] = cls


def function_lookup(functions, name, operands):
    """Lookup a BQL function implementation.

    Args:
      functions: The functions registry to interrogate.
      name: The function name.
      operands: Function operands.

    Returns:
      A EvalNode (or subclass) instance or None if the function was not found.
    """
    intypes = [operand.dtype for operand in operands]
    for func in functions[name]:
        if func.__intypes__ == intypes:
            return func
    return None


# Map types to their BQL name. Used to find the name of the type cast funtion.
MAP = {
    bool: 'bool',
    datetime.date: 'date',
    decimal.Decimal: 'decimal',
    int: 'int',
    str: 'str',
}


# Map between Python types and BQL structured types. Functions and
# columns definitions can use Python types. The corresponding BQL
# structured type is looked up when compiling the subscrip operator.
ALIASES = {}


def name(datatype):
    return getattr(datatype, 'name', datatype.__name__.lower())
