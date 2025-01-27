import decimal
import pickle
import textwrap

from . import types

# Hashable types. Checking ``issubclass(T, types.Hashable)`` does not
# work because named tuples pass that test and beancount uses many named
# tuples that have dictionary members making them effectively not
# hashable.
FUNDAMENTAL = frozenset({
    bool,
    bytes,
    complex,
    decimal.Decimal,
    float,
    int,
    str,
    # These are hashable only if the contained objects are hashable.
    frozenset,
    tuple,
})

# Function reducing non-hashable types to something hashable.
REDUCERS = {}


def register(datatype, func):
    """Register reduce function for non-hashable type.

    The reduce function maps an non-hashable object into an hashable
    representation. This representation does not need to capture all the
    object facets, but it should retrurn someting unique enough to avoid
    too many hashing collisions.
    """
    REDUCERS[datatype] = func


def make(columns):
    """Build an hashable tuple subclass."""

    # When all columns are hashable, pass the input tuple through as is.
    if all(column.datatype in FUNDAMENTAL for column in columns):
        return lambda x: x

    datatypes = ', '.join(types.name(column.datatype) for column in columns)

    # Code generation inspired by standard library ``dataclasses.py``.
    parts = []
    locals = {}
    for i, column in enumerate(columns):
        if column.datatype in FUNDAMENTAL:
            parts.append(f'self[{i}]')
        elif column.datatype is dict:
            parts.append(f'*self[{i}].keys(), *self[{i}].values()')
        elif column.datatype is set:
            parts.append(f'*self[{i}]')
        else:
            func = REDUCERS.get(column.datatype, pickle.dumps)
            fname = f'func{i}'
            locals[fname] = func
            parts.append(f'{fname}(self[{i}])')

    objs = ', '.join(parts)
    names = ', '.join(locals.keys())
    code = textwrap.dedent(f'''
        def create({names}):
            def __hash__(self):
                return hash(({objs}))
            return __hash__
    ''')
    clsname = f'Hashable[{datatypes}]'

    ns = {}
    exec(code, globals(), ns)
    func = ns['create'](**locals)
    func.__qualname__ = f'{clsname}.{func.__name__}'

    members = dict(tuple.__dict__)
    members['__hash__'] = func

    return type(clsname, (tuple,), members)
