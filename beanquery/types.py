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
    """Return the type hierarchy for a given type, excluding ``object``.

    This function extracts the Method Resolution Order (MRO) for a type,
    which includes the type itself and all its base classes. The ``object``
    type is excluded from the hierarchy (except when the type IS ``object``)
    because BQL uses ``object`` to represent untyped values, not as a universal
    base type. This prevents functions registered for untyped values from
    matching all typed values.

    For generic types like ``typing.Set[str]``, the origin type (``set``) is
    extracted and its MRO is returned, allowing parameterized types to match
    functions/operators registered with unparameterized types.

    Args:
      t: A type object, which can be a plain type (str, int, set) or a
        generic type (typing.Set[str], typing.List[int]).

    Returns:
      A tuple of types representing the type hierarchy, with ``object`` excluded
      unless the input type is ``object`` itself or ``NoneType``.
    """
    if t is NoneType:
        return (object,)

    # Handle generic types like typing.Set[str], typing.List[int], etc.
    # Extract the origin type (e.g., set from Set[str]) and include it
    # in the bases so that functions registered with unparameterized types
    # (e.g., @function([set], ...)) can match parameterized types (Set[str])
    origin = typing.get_origin(t)
    if origin is not None:
        origin_bases = origin.__mro__
        if len(origin_bases) > 1 and origin_bases[-1] is object:
            return origin_bases[:-1]
        return origin_bases

    bases = t.__mro__
    if len(bases) > 1 and bases[-1] is object:
        return bases[:-1]
    return bases


def function_lookup(functions, name, operands):
    """Lookup a BQL function implementation.

    Args:
      functions: A dict mapping function names (str) to lists of function
        implementations. Each implementation has an __intypes__ attribute
        specifying the expected operand types.
      name: The function name (str).
      operands: Function operands, each with a .dtype attribute.

    Returns:
      A EvalNode (or subclass) instance or None if the function was not found.
    """
    for signature in itertools.product(*(_bases(operand.dtype) for operand in operands)):
        for func in functions[name]:
            if func.__intypes__ == list(signature):
                return func
    return None


def operator_lookup(operators, operand_types):
    """Lookup an operator implementation by matching operand types.

    Args:
      operators: A list of operator implementations. Each implementation has
        an __intypes__ attribute specifying the expected operand types as a
        list (e.g., [str, str] for a binary operator on strings).
      operand_types: Sequence of types for the operands (e.g., [str, str]).

    Returns:
      An operator implementation or None if not found.
    """
    for signature in itertools.product(*(_bases(t) for t in operand_types)):
        for op in operators:
            if op.__intypes__ == list(signature):
                return op
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
    if isinstance(datatype, typing._GenericAlias):
        return str(datatype).rsplit('.', 1)[-1].lower()
    return getattr(datatype, 'name', datatype.__name__.lower())


_STRING_TO_DATATYPE = {
    'bool': bool,
    'date': datetime.date,
    'decimal': decimal.Decimal,
    'int': int,
    'object': object,
    'str': str,
    'text': str,
    'varchar': str,
}


def parse(name):
    """Parse the string representation of a type into a type object.

    This does not (yet) work for all supprted data types.
    """
    return _STRING_TO_DATATYPE.get(name, None)
