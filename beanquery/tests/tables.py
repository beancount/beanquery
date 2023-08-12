from .. import query_compile
from .. import tables


class TestColumn(query_compile.EvalColumn):
    def __init__(self, func, datatype):
        super().__init__(datatype)
        self.func = func

    def __call__(self, row):
        return self.func(row)


class TestTable(tables.Table):
    columns = {'x': TestColumn(lambda row: row, int)}

    def __init__(self, *args):
        self.rows = range(*args)

    def __iter__(self):
        return iter(self.rows)
