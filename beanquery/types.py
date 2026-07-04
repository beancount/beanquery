import collections
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

_TypeProbe = collections.namedtuple('_TypeProbe', 'dtype')

def function_lookup_types(functions, name, dtypes):
    """Like function_lookup, but takes bare types instead of operand nodes.

    Args:
      functions: The functions registry to interrogate.
      name: The function name.
      dtypes: Candidate argument types (no operand instances required).

    Returns:
      A EvalNode (or subclass) instance or None if the function was not found.
    """
    return function_lookup(functions, name, [_TypeProbe(dtype) for dtype in dtypes])


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


def coercion_target_type(functions, source_type, target_type):
    """Resolve the type an operand of 'source_type' would have after being
    coerced towards 'target_type'.

    A BQL cast function is a normal, explicitly invocable function (e.g.
    ``decimal(x)``) that also doubles as an implicit coercion whenever a
    query mixes an operand of 'source_type' with one of 'target_type'.
    'target_type' itself may need normalizing before checking: the parser
    never produces 'int' typed values, so requesting a coercion to 'int'
    would only lose information; it is redirected to 'Decimal' instead.

    Args:
      functions: The functions registry to interrogate for cast functions.
      source_type: The type to coerce from.
      target_type: The requested type to coerce to.

    Returns:
      The actual (possibly normalized) target type if a cast function from
      'source_type' to it is registered, None if no such coercion exists.
    """
    if source_type == target_type:
        return target_type

    # The Beancount parser does not emit int typed values, thus casting to int
    # is only going to loose information. Promote to decimal.
    if target_type is int:
        target_type = decimal.Decimal

    name = MAP.get(target_type)
    if name is None:
        return None

    if function_lookup_types(functions, name, [source_type]) is None:
        return None

    return target_type


def common_set_type(type1, type2):
    """Resolve the common type of two 'set'/'typing.Set[T]' types.

    Set values need no value coercion (a 'set' is already a valid
    'typing.Set[T]' at runtime); this only widens the declared dtype:
      - set + set[type] -> set
      - set[x] + set[y] -> recursively check x and y compatibility

    Args:
      type1: First type.
      type2: Second type.

    Returns:
      The common type if compatible, None otherwise.
    """
    is_generic1 = isinstance(type1, typing._GenericAlias)
    is_generic2 = isinstance(type2, typing._GenericAlias)

    # set + set[type] -> set
    if type1 is set and is_generic2 and typing.get_origin(type2) is set:
        return set
    if type2 is set and is_generic1 and typing.get_origin(type1) is set:
        return set

    # set[x] + set[y] -> check x and y compatibility
    if is_generic1 and is_generic2:
        origin1 = typing.get_origin(type1)
        origin2 = typing.get_origin(type2)
        if origin1 is set and origin2 is set:
            args1 = typing.get_args(type1)
            args2 = typing.get_args(type2)
            if args1 and args2:
                inner_common = common_set_type(args1[0], args2[0])
                if inner_common is not None:
                    return typing.Set[inner_common]

    return None


def common_coercion_type(functions, type1, type2):
    """Resolve the common type of two types for implicit coercion.

    Mirrors the implicit coercion policy used for binary operators: an
    untyped ('object') operand may be coerced to the other operand's type,
    and 'int'/'Decimal' are symmetrically compatible. Falls back to
    'set'/'typing.Set[T]' widening (see 'common_set_type'), which needs no
    value coercion at all. Returns None if the types are incompatible.

    Args:
      functions: The functions registry to interrogate for cast functions.
      type1: First type.
      type2: Second type.

    Returns:
      The common type if compatible, None otherwise.
    """
    if type1 == type2:
        return type1
    if type1 is object and type2 is not object:
        return coercion_target_type(functions, type1, type2)
    if type2 is object and type1 is not object:
        return coercion_target_type(functions, type2, type1)
    if {type1, type2} == {int, decimal.Decimal}:
        return decimal.Decimal
    return common_set_type(type1, type2)