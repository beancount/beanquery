import datetime
import decimal

import tatsu

from ..errors import ProgrammingError
from .parser import BQLParser
from . import ast


class BQLSemantics:

    def set_context(self, ctx):
        self._ctx = ctx

    def null(self, value):
        return None

    def integer(self, value):
        return int(value)

    def decimal(self, value):
        return decimal.Decimal(value)

    def date(self, value):
        return datetime.date.fromisoformat(value)

    def string(self, value):
        return value[1:-1]

    def boolean(self, value):
        return value == 'TRUE'

    def unquoted_identifier(self, value):
        return value.lower()

    def quoted_identifier(self, value):
        return value.replace('""', '"')

    def asterisk(self, value):
        return ast.Asterisk()

    def list(self, value):
        return list(value)

    def ordering(self, value):
        return ast.Ordering[value or 'ASC']

    def _default(self, value, typename=None):
        if typename is not None:
            func = getattr(ast, typename)
            return func(**{name.rstrip('_'): value for name, value in value.items()})
        return value


class ParseError(ProgrammingError):
    def __init__(self, parseinfo):
        super().__init__('syntax error')
        self.parseinfo = parseinfo


def parse(text):
    try:
        return BQLParser().parse(text, semantics=BQLSemantics())
    except tatsu.exceptions.ParseError as exc:
        line = exc.tokenizer.line_info(exc.pos).line
        parseinfo = tatsu.infos.ParseInfo(exc.tokenizer, exc.item, exc.pos, exc.pos + 1, line, [])
        raise ParseError(parseinfo) from exc
