from beanquery import query_env

SCHEMES = {}


class Table:
    scheme = None
    functions = {}

    def __init__(self):
        self.columns = {}

    def __iter__(self):
        raise NotImplementedError

    def __init_subclass__(cls):
        SCHEMES[cls.scheme] = cls

    @property
    def wildcard_columns(self):
        return self.columns.keys()

    def get_column(self, name):
        """Return a column accessor for the given named column.
        Args:
          name: A string, the name of the column to access.
        """
        col = self.columns[name]
        if col is not None:
            return col()

        raise CompilationError(f'Invalid column name "{name}" in table "{self.name}')

    def get_function(self, name, operands):
        """Return a function accessor for the given named function.
        Args:
          name: A string, the name of the function to access.
        """
        func = types.function_lookup(self.functions, name, operands)
        if func is not None:
            return func(operands)

        sig = '{}({})'.format(name, ', '.join(operand.dtype.__name__ for operand in operands))
        raise CompilationError(f'Unknown function "{sig}" in {self.context_name}')


class NullTable(Table):
    functions = query_env.SIMPLE_FUNCTIONS

    def __iter__(self):
        yield from iter([None])


TABLES = {
     '_': NullTable(),
}


def get(name):
    return TABLES.get(name)


def register(name, table):
    TABLES[name] = table
