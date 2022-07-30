"""Parser for Beancount Query Language.
"""
__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import decimal

import dateutil.parser
import tatsu

from beanquery.parser import parser
from beanquery.parser import ast

# Convenience alias. Mostly to encapsulate the parser implementation.
ParseError = tatsu.exceptions.ParseError


class BQLSemantics:
    def __init__(self, default_close_date=None):
        self.default_close_date = default_close_date

    def null(self, value):
        return None

    def integer(self, value):
        return int(value)

    def decimal(self, value):
        return decimal.Decimal(value)

    def date(self, value):
        if value.startswith('#'):
            return dateutil.parser.parse(value[2:-1]).date()
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()

    def string(self, value):
        return value[1:-1]

    def boolean(self, value):
        return value == 'TRUE'

    def identifier(self, value):
        return value.lower()

    def wildcard(self, value):
        return ast.Wildcard()

    def list(self, value):
        return list(value)

    def ordering(self, value):
        return ast.Ordering[value or 'ASC']

    def from_(self, value, typename):
        if value['expression'] is None and value['open'] is None and value['close'] is None and value['clear_'] is None:
            raise ParseError('Empty FROM expression is not allowed')
        if value['close'] is None:
            value['close'] = self.default_close_date
        return self._default(value, typename)

    def _default(self, value, typename=None):
        if typename is not None:
            func = getattr(ast, typename)
            return func(**{name.rstrip('_'): value for name, value in value.items()})
        return value


def parse(text, default_close_date=None):
    return parser.BQLParser().parse(text, semantics=BQLSemantics(default_close_date))
