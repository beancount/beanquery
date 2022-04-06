"""Compatibility patches to support Beancount v2 and v3."""

import re
import functools

import beancount

from beancount import loader
from beancount.parser import parser


if not beancount.__version__.startswith('3.'):
    def wrap(func):
        @functools.wraps(func)
        def hack(string, *args, **kwargs):
            # Turn v3 transaction flag syntax into v2 syntax
            string = re.sub(r'(\d{4}-\d{2}-\d{2})\s+\'([A-Z])\s', r'\1 \2 ', string)
            return func(string, *args, **kwargs)
        return hack

    loader.load_string = wrap(loader.load_string)
    parser.parse_string = wrap(parser.parse_string)
