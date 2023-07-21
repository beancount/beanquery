import datetime
import decimal

import tatsu

from . import ast
from . import parser


# Convenience alias. Mostly to encapsulate the parser implementation.
ParseError = tatsu.exceptions.ParseError


class BQLSemantics:
    def __init__(self, default_close_date=None):
        self.default_close_date = default_close_date

    def set_context(self, ctx):
        self._ctx = ctx

    def null(self, value):
        return None

    def integer(self, value):
        return int(value)

    def decimal(self, value):
        return decimal.Decimal(value)

    def date(self, value):
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()

    def string(self, value):
        return value[1:-1]

    def boolean(self, value):
        return value == 'TRUE'

    def identifier(self, value):
        return value.lower()

    def asterisk(self, value):
        return ast.Asterisk()

    def list(self, value):
        return list(value)

    def ordering(self, value):
        return ast.Ordering[value or 'ASC']

    def from_(self, value, typename):
        if value['expression'] is None and value['open'] is None and value['close'] is None and value['clear_'] is None:
            self._ctx._error('Empty FROM expression is not allowed')  # pylint: disable=protected-access
        return self._default(value, typename)

    def _default(self, value, typename=None):
        if typename is not None:
            func = getattr(ast, typename)
            return func(**{name.rstrip('_'): value for name, value in value.items()})
        return value


def parse(text, default_close_date=None):
    return parser.BQLParser().parse(text, semantics=BQLSemantics(default_close_date))
