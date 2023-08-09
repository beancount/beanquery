import typing

from urllib.parse import urlparse

from beancount import loader
from beancount.core import data
from beancount.core import position

from beanquery import types
from beanquery import query_compile
from beanquery import query_env


def add_beancount_tables(context, entries, errors, options):
    for table in query_env.EntriesTable, query_env.PostingsTable:
        context.tables[table.name] = table(entries, options)
    context.options.update(options)
    context.errors.extend(errors)


def attach(context, uri):
    filename = urlparse(uri).path
    entries, errors, options = loader.load_file(filename)
    add_beancount_tables(context, entries, errors, options)


class GetAttrColumn(query_compile.EvalColumn):
    def __init__(self, name, dtype):
        super().__init__(dtype)
        self.name = name

    def __call__(self, context):
        return getattr(context, self.name)


def _typed_namedtuple_to_columns(cls):
    columns = {}
    for name, dtype in typing.get_type_hints(cls).items():
        origin = typing.get_origin(dtype)
        # Extract the underlying type from Optional[x] annotations,
        # which are effectively Union[x, None] annotations.
        if origin is typing.Union:
            args = typing.get_args(dtype)
            # Ensure that there is just one type other than None.
            dtypes = [t for t in args if t is not type(None)]
            assert len(dtypes) == 1
            dtype = dtypes[0]
        elif origin is not None:
            dtype = origin
        if name == 'meta' and dtype is dict:
            dtype = Meta
        columns[name] = GetAttrColumn(name, dtype)
    return columns


class Position(types.Structure):
    name = 'position'
    columns = _typed_namedtuple_to_columns(position.Position)


class Cost(types.Structure):
    name = 'cost'
    columns = _typed_namedtuple_to_columns(data.Cost)


class Amount(types.Structure):
    name = 'amount'
    columns = _typed_namedtuple_to_columns(data.Amount)


types.ALIASES[position.Position] = Position
types.ALIASES[data.Cost] = Cost
types.ALIASES[data.Amount] = Amount
