import csv
import datetime

from os import path
from urllib.parse import urlparse, parse_qsl

from beanquery import tables
from beanquery import query_compile
from beanquery.parser import BQLParser, BQLSemantics

# Support conversions from all fundamental types supported by the BQL parser:
#
#   literal
#       =
#       | date
#       | decimal
#       | integer
#       | string
#       | null
#       | boolean
#       ;
#

def _guess_type(value):
    try:
        r = BQLParser().parse(value, start='literal', semantics=BQLSemantics())
    except Exception:
        # Everything that is not recognized as something else is a string.
        return str
    return type(r)


def _parse_bool(value):
    x = value.strip().lower()
    if x == '1' or x == 'true':
        return True
    if x == '0' or x == 'false':
        return False
    raise ValueError(value)


_TYPES_TO_PARSERS = {
    bool: _parse_bool,
    datetime.date: datetime.date.fromisoformat,
}


class Column(query_compile.EvalColumn):
    def __init__(self, key, datatype, func):
        super().__init__(datatype)
        self.key = key
        self.func = func

    def __call__(self, row):
        return self.func(row[self.key])


class Table(tables.Table):
    def __init__(self, name, columns, data, header=False, **fmtparams):
        self.name = name
        self.data = data
        self.header = header
        # Skip white space after field separator by default to make parsing
        # columns accordingly to their type easier, unless the setting is
        # overridden by the user.
        fmtparams.setdefault('skipinitialspace', True)
        self.reader = csv.reader(data, **fmtparams)
        self.columns = {}
        if columns is None:
            names = next(self.reader, [])
            values = next(self.reader, [])
            datatypes = (_guess_type(value) for value in values)
            columns = zip(names, datatypes)
        for cname, ctype in columns:
            converter = _TYPES_TO_PARSERS.get(ctype, ctype)
            self.columns[cname] = Column(len(self.columns), ctype, converter)

    def __del__(self):
        self.data.close()

    def __iter__(self):
        self.data.seek(0)
        it = iter(self.reader)
        if self.header:
            next(it)
        return it


def create(name, columns, using):
    parts = urlparse(using)
    filename = parts.path
    params = dict(parse_qsl(parts.query))
    encoding = params.pop('encoding', None)
    header = params.pop('header', columns is None)
    if filename:
        data = open(filename, encoding=encoding)
    return Table(name, columns, data, header=header, **params)


def attach(context, dsn, *, data=None):
    parts = urlparse(dsn)
    filename = parts.path
    params = dict(parse_qsl(parts.query))
    encoding = params.pop('encoding', None)
    if filename:
        data = open(filename, encoding=encoding)
    name = params.pop('name', None) or path.splitext(path.basename(filename))[0] or 'csv'
    context.tables[name] = Table(name, None, data, header=True, **params)
