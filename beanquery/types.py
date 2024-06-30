import datetime
import decimal
import itertools
import typing


# Only Python >= 3.10 exposes NoneType in the types module.
NoneType = type(None)


class AnyType:
    """As written on the tin."""
    __slots__ = ()
    __name__ = 'any'

    def __eq__(self, other):
        """Compares equal to any other type."""
        return isinstance(other, type)


# Used in BQL functions signatures for arguments that can have any type.
Any = AnyType()


# Used for COUNT(*)
Asterisk = typing.NewType('*', object)  # noqa: PLC0132
Asterisk.__mro__ = Asterisk,


# Keep track of the defined structured types to allow introspection.
TYPES = {}


class Structure:
    """Base class for structured data types."""
    name = None
    columns = {}

    def __init_subclass__(cls):
        if cls.name:
            TYPES[cls.name] = cls


def _bases(t):
    if t is NoneType:
        return (object,)
    bases = t.__mro__
    if len(bases) > 1 and bases[-1] is object:
        # All types that are not ``object`` have more than one class
        # in their ``__mro__``. BQL uses ``object`` for untypes
        # values. Do not return ``object`` as base for strict types,
        # to avoid functions taking untyped onjects to accept all
        # values.
        return bases[:-1]
    return bases


def function_lookup(functions, name, operands):
    """Lookup a BQL function implementation.

    Args:
      functions: The functions registry to interrogate.
      name: The function name.
      operands: Function operands.

    Returns:
      A EvalNode (or subclass) instance or None if the function was not found.
    """
    for signature in itertools.product(*(_bases(operand.dtype) for operand in operands)):
        for func in functions[name]:
            if func.__intypes__ == list(signature):
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
    if datatype is NoneType:
        return 'NULL'
    return getattr(datatype, 'name', datatype.__name__.lower())
