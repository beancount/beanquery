class AnyType:
    """As written on the tin."""
    __slots__ = ()

    def __eq__(self, other):
        """Compares equal to any other type."""
        return isinstance(other, type)


# Used in BQL functions signatures for arguments that can have any type.
Any = AnyType()


def function_lookup(functions, name, operands):
    """Lookup a BQL function implementation.

    Args:
      functions: The functions registry to interrogate.
      name: The function name.
      operands: Function operands.

    Returns:
      A EvalNode (or subclass) instance or None if the function was not found.
    """
    signature = [operand.dtype for operand in operands]
    for func in functions[name]:
        if func.__intypes__ == signature:
            return func
    return None
