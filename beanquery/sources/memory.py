from beanquery import tables
from beanquery.sources.beancount import GetItemColumn


class Table(tables.Table):
    def __init__(self, name, columns):
        self.name = name
        self.data = []
        self.columns = {}
        for cname, ctype in columns:
            self.columns[cname] = GetItemColumn(len(self.columns), ctype)

    def __iter__(self):
        return iter(self.data)

    def insert(self, row):
        assert len(row) == len(self.columns)
        self.data.append(row)


def create(name, columns, *args, **kwargs):
    return Table(name, columns)
