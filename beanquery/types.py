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

# Used for COUNT(*)
Star = '*'


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
