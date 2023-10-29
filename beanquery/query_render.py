"""Rendering of rows.
"""
__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import csv
import datetime
import enum

from decimal import Decimal

from beancount.core import amount
from beancount.core import display_context
from beancount.core import inventory
from beancount.core import position


class Align(enum.Enum):
    LEFT = 0
    RIGHT = 1


class RenderContext:
    """Hold the query rendering configuration."""

    def __init__(self, dcontext, expand=False, listsep=', ', spaced=False, null=' '):
        self.dcontext = dcontext
        self.expand = expand
        self.listsep = listsep
        self.spaced = spaced
        self.null = null


# Map of data-types to renderer classes. This is populated by
# subclassing ColumnRenderer via an __init_subclass__ hook.
RENDERERS = {}


class ColumnRenderer:
    """Base class for classes that render column values.

    The column renderers are responsible to render uniform type values
    in a way that will align nicely in a column whereby all the values
    render to the same width.

    The formatters are instantiated and are feed all the values in the
    column via the ``update()`` method to accumulate the dimensions it
    will need to format them later on. The ``prepare()`` method then
    computes internal status required to format these values in
    consistent fashion. The ``width()`` method can then be used to
    retrieve the computer maximum width of the column. Individual
    values are formatted with the ``format()`` method. Values are
    assumed to be of the expected type for the formatter. Formatting
    values outside the set of the values fed via the ``update()``
    method is undefined behavior.

    """
    dtype = None
    align = Align.LEFT

    def __init__(self, ctx):
        self.maxwidth = 0
        self.prepared = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        RENDERERS[cls.dtype] = cls

    def update(self, value):
        """Update the rendered with the given value.

        Args:
          value: Any object of type ``dtype``.

        """

    def prepare(self):
        """Prepare to render the column.

        Returns:
          Computed column width.

        """
        self.prepared = True
        return self.maxwidth

    @property
    def width(self):
        if not self.prepared:
            raise RuntimeError('width property access before calling prepare()')
        return self.maxwidth

    def format(self, value):
        """Format the value.

        Args:
          value: Any object of type ``dtype``.

        Returns:
          A string or list of strings representing the rendered value.

        """
        raise NotImplementedError


class ObjectRenderer(ColumnRenderer):
    dtype = object

    def update(self, value):
        self.maxwidth = max(self.maxwidth, len(self.format(value)))

    def format(self, value):
        return str(value)


class DictRenderer(ObjectRenderer):
    dtype = dict


class BoolRenderer(ColumnRenderer):
    dtype = bool

    def update(self, value):
        self.maxwidth = max(self.maxwidth, 4 if value else 5)

    def format(self, value):
        return ('TRUE' if value else 'FALSE')


class StringRenderer(ObjectRenderer):
    dtype = str


class SetRenderer(ColumnRenderer):
    dtype = set

    def __init__(self, ctx):
        super().__init__(ctx)
        self.sep = ctx.listsep

    def update(self, value):
        self.maxwidth = max(self.maxwidth, sum(len(x) + len(self.sep) for x in value) - len(self.sep))

    def format(self, value):
        return self.sep.join(str(x) for x in sorted(value))


class DateRenderer(ColumnRenderer):
    dtype = datetime.date

    def update(self, value):
        self.maxwidth = 10

    def format(self, value):
        return value.strftime('%Y-%m-%d')


class IntRenderer(ObjectRenderer):
    dtype = int
    align = Align.RIGHT


class EnumRenderer(ObjectRenderer):
    dtype = enum.Enum

    def format(self, value):
        return value.name


class DecimalRenderer(ColumnRenderer):
    """Renderer for Decimal numbers.

    Numbers are left padded to align on the decimal point::

      -  123.40
      -    5.000
      -  -67

    """
    dtype = Decimal

    def __init__(self, ctx):
        super().__init__(ctx)
        # Max number of digits before the decimal point including sign.
        self.nintegral = 0
        # Max number of digits after the decimal point.
        self.nfractional = 0

    def update(self, value):
        n = value.as_tuple()
        if n.exponent > 0:
            # Special case for decimal numbers with positive exponent
            # and thus represented in scientific notation.
            self.nintegral = max(self.nintegral, len(str(value)))
        else:
            self.nintegral = max(self.nintegral, max(1, len(n.digits) + n.exponent) + n.sign)
            self.nfractional = max(self.nfractional, -n.exponent)

    def prepare(self):
        self.maxwidth = self.nintegral + self.nfractional + (1 if self.nfractional > 0 else 0)
        return super().prepare()

    def format(self, value):
        n = value.as_tuple()
        if n.exponent > 0:
            # Special case for decimal numbers with positive exponent
            # and thus represented in scientific notation.
            return str(value).rjust(self.nintegral).ljust(self.maxwidth)
        # Compute the padding required to align the decimal point.
        left = self.nintegral - (max(1, len(n.digits) + n.exponent) + n.sign)
        return f'{"":>{left}}{value:<{self.maxwidth - left}}'


class AmountRenderer(ColumnRenderer):
    """Renderer for Amount instances.

    The numerical part is formatted with the right quantization
    determined by ``dcontext`` in the rendering context and aligned on
    the decimal point across rows. Numbers are right padded with
    spaces to alignt the commodity symbols across rows::

      -  1234.00   USD
      -    42      TEST
      -     0.0001 ETH
      -   567.00   USD

    """
    dtype = amount.Amount

    def __init__(self, ctx):
        super().__init__(ctx)
        # Use the display context inferred from the input ledger to
        # determine the quantization of the column values.
        self.quantize = ctx.dcontext.quantize
        # Use column specific display context for formatting.
        self.dcontext = display_context.DisplayContext()
        # Maximum width of the commodity symbol.
        self.curwidth = 0

    def update(self, value):
        # Need to handle None to reuse this in PositionRenderer.
        if value is not None:
            number = self.quantize(value.number, value.currency)
            self.dcontext.update(number, value.currency)
            self.curwidth = max(self.curwidth, len(value.currency))

    def prepare(self):
        self.func = self.dcontext.build(display_context.Align.DOT)
        zero = Decimal()
        for commodity in self.dcontext.ccontexts:
            if commodity != '__default__':
                self.maxwidth = max(self.maxwidth, len(self.func(zero, commodity)) + 1 + self.curwidth)
        return super().prepare()

    def format(self, value):
        return f'{self.func(value.number, value.currency)} {value.currency:<{self.curwidth}}'


class CostRenderer(ObjectRenderer):
    dtype = position.Cost

    def __init__(self, ctx):
        super().__init__(ctx)
        self.amount_renderer = AmountRenderer(ctx)
        self.date_width = 0
        self.label_width = 0

    def update(self, value):
        self.amount_renderer.update(value)
        if value.date is not None:
            self.date_width = 10 + 2
        if value.label is not None:
            self.label_width = max(self.label_width, len(value.label) + 4)

    def prepare(self):
        cost_width = self.amount_renderer.prepare()
        self.maxwidth = cost_width + self.date_width + self.label_width
        return super().prepare()

    def format(self, value):
        parts = [self.amount_renderer.format(value)]
        if value.date is not None:
            parts.append(f'{value.date:%Y-%m-%d}')
        if value.label is not None:
            parts.append(f'"{value.label}"')
        return ', '.join(parts)


class PositionRenderer(ColumnRenderer):
    """Renderer for Position instrnaces.

    Both the unit numbers and the cost numbers are aligned::

      -    5.000 HOOL {500.23  USD }
      -  123     CA   {  1.000 HOOL}
      -    3.00  USD
      -   42.000 HOOL
      -    3.00  AAPL
      -    3.0   XY

    """
    dtype = position.Position

    def __init__(self, ctx):
        super().__init__(ctx)
        self.units_renderer = AmountRenderer(ctx)
        self.cost_renderer = AmountRenderer(ctx)

    def update(self, value):
        self.units_renderer.update(value.units)
        self.cost_renderer.update(value.cost)

    def prepare(self):
        units_width = self.units_renderer.prepare()
        cost_width = self.cost_renderer.prepare()
        self.maxwidth = units_width + cost_width + (3 if cost_width > 0 else 0)
        return super().prepare()

    def format(self, value):
        units = self.units_renderer.format(value.units)
        if value.cost is None:
            return units.ljust(self.maxwidth)
        cost = self.cost_renderer.format(value.cost)
        return f'{units} {{{cost}}}'


class InventoryRenderer(ColumnRenderer):
    """Renderer for Inventory instances.

    Inventories renders as a list of position strings. The format used
    differs whether expansion of list-like values to multiple rows in
    enabled or not.

    When row expansion is enabled, positions in each inventory values
    are sorted alphabetically by commodity symbol and are formatted
    with the same position formatter, resulting in all commodity
    strings to be aligned::

      -  1234.00   USD
           42      TEST
      -     0.0001 ETH
          567.00   USD

    When row expansion is disabled, the position formatters are unique
    for each commodity symbol and the values are rendered in a table
    like structure. The positions appear sorted by frequency of
    occurence in the column and alphabetically by commodity symbol::

      - 1234.00 USD 0.0001 ETH
      -  567.00 USD            42 TEST

    The separator between positions is determined by ``listsep`` in
    the rendering context.

    """
    dtype = inventory.Inventory

    def __init__(self, ctx):
        super().__init__(ctx)
        self.listsep = ctx.listsep
        # We look this up for each value, it makes sense to cache it
        # to avoid the attribute lookup in the context.
        self.expand = ctx.expand
        # How many times at most we have seen a commodity in an inventory.
        self.counts = collections.defaultdict(int)
        # Commodity specific renderers.
        self.renderers = collections.defaultdict(lambda: PositionRenderer(ctx))
        # How many distinct commodity need to be rendered.
        self.distinct = 0

    def update(self, value):
        for pos in value.get_positions():
            # We use the little indexing trick to do not have to
            # conditionalize this code on whether rows expansion is
            # enabled or not.
            self.renderers[self.expand or pos.units.currency].update(pos)
        counts = collections.Counter(pos.units.currency for pos in value.get_positions())
        for key, value in counts.items():
            self.counts[key] = max(self.counts[key], value)

    def prepare(self):
        if self.expand:
            self.maxwidth = self.renderers[self.expand].prepare()
        else:
            for commodity, renderer in self.renderers.items():
                w = renderer.prepare()
                self.distinct += self.counts[commodity]
                self.maxwidth += self.counts[commodity] * (w + len(self.listsep))
            self.maxwidth -= len(self.listsep)
        return super().prepare()

    @staticmethod
    def positionsortkey(position):
        # Sort positions combining fields in a more intuitive way than the default.
        return (position.units.currency, -position.units.number,
                (position.cost.currency, -position.cost.number, position.cost.date) if position.cost else ())

    def format(self, value):
        # Expanded row format.
        if self.expand:
            strings = []
            for pos in sorted(value.get_positions(), key=self.positionsortkey):
                strings.append(self.renderers[self.expand].format(pos))
            return strings
        # Too many distinct commodities to present in tabular format.
        if self.distinct > 5:
            strings = []
            for pos in sorted(value.get_positions(), key=self.positionsortkey):
                strings.append(self.renderers[pos.units.currency].format(pos))
            return self.listsep.join(strings).ljust(self.maxwidth)
        # Tabular format with same commodity positions vertically aligned.
        positions = collections.defaultdict(list)
        for pos in sorted(value.get_positions(), key=self.positionsortkey):
            positions[pos.units.currency].append(pos)
        strings = []
        for commodity, renderer in sorted(self.renderers.items()):
            strings += [renderer.format(pos) for pos in positions[commodity]]
            strings += [''.ljust(renderer.width)] * (self.counts[commodity] - len(positions[commodity]))
        return self.listsep.join(strings)


def render_rows(rows, renderers, ctx):
    """Render results set row."""

    # Filler for NULL values.
    null = ctx.null

    # Spacing row.
    spacerow = [''] * len(renderers)

    for row in rows:

        # Render the row cells. Do not pass missing values to the
        # renderers but substitute them with the appropriate
        # placeholder string.
        cells = [render.format(value) if value is not None else null for render, value in zip(renderers, row)]

        if not any(isinstance(cell, list) for cell in cells):
            # No multi line cells. Yield the row.
            yield cells

        else:
            # At least one multi line cell. Ensure that all cells are lists.
            cells = [cell if isinstance(cell, list) else [cell] for cell in cells]

            # Compute the maximum number of lines in any cell.
            nlines = max(len(cell) for cell in cells)

            # Add placeholder lines to short multi line cells.
            for cell in cells:
                if len(cell) < nlines:
                    cell.extend([''] * (nlines - len(cell)))

            # Yield the obtained rows.
            yield from zip(*cells)

        # Add spacing row when needed.
        if ctx.spaced:
            yield spacerow


def _get_renderer(datatype, ctx):
    for d in datatype.__mro__:  # pragma: no branch
        renderer = RENDERERS.get(d)
        if renderer:
            return renderer(ctx)


def render_text(columns, rows, dcontext, file, expand=False, boxed=False,
                spaced=False, listsep='  ', nullvalue='', narrow=True, unicode=False, **kwargs):
    """Render the result of executing a query in text format.

    Args:
      columns: A list of beanquery.Column descrining the table columns.
      rows: Data to render.
      dcontext: A DisplayContext object prepared for rendering numbers.
      file: A file object to render the results to.
      expand: When true expand columns that render to lists to multiple rows.
      boxed: When true draw an ascii-art table borders.
      spaced: When true insert an empty line between rows.
      listsep: String to use to separate values in list-like column values.
      nullvalue: String to use to represent NULL values.
      narrow: When true truncate headers to the maximum column values width.
      unicode: When true use unicode box drawing characters to draw tables.

    """
    ctx = RenderContext(dcontext, expand=expand, spaced=spaced, listsep=listsep, null=nullvalue)
    renderers = [_get_renderer(column.datatype, ctx) for column in columns]
    headers = [column.name for column in columns]
    alignment = [renderer.align for renderer in renderers]

    # Prime the renderers.
    for row in rows:
        for value, renderer in zip(row, renderers):
            if value is not None:
                renderer.update(value)

    # Compute columns widths.
    widths = [max(1, narrow or len(header), len(nullvalue), render.prepare()) for header, render in zip(headers, renderers)]

    # Initialize table style. For unicode box drawing characters,
    # see https://www.unicode.org/charts/PDF/U2500.pdf
    if boxed:
        if unicode:
            frmt = '\u2502 {} \u2502\n'
            colsep = ' \u2502 '
            lines = [''.rjust(width, '\u2500') for width in widths]
            top =    '\u250C\u2500{}\u2500\u2510\n'.format('\u2500\u252C\u2500'.join(lines))
            hline =  '\u251C\u2500{}\u2500\u2524\n'.format('\u2500\u253C\u2500'.join(lines))
            bottom = '\u2514\u2500{}\u2500\u2518\n'.format('\u2500\u2534\u2500'.join(lines))
        else:
            frmt = '| {} |\n'
            colsep = ' | '
            top = hline = bottom = '+-{}-+\n'.format('-+-'.join(''.rjust(width, '-') for width in widths))
    else:
        frmt = '{}\n'
        colsep = '  '
        top = bottom = ''
        hline = '{}\n'.format(colsep.join(''.rjust(width, '\u2500' if unicode else '-') for width in widths))

    # Header.
    file.write(top)
    file.write(frmt.format(colsep.join(header[:width].center(width) for header, width in zip(headers, widths))))
    file.write(hline)

    # Rows.
    for row in render_rows(rows, renderers, ctx):
        file.write(frmt.format(colsep.join(x.ljust(w) if a == Align.LEFT else x.rjust(w)
                                           for x, w, a in zip(row, widths, alignment))))

    # Footer.
    file.write(bottom)


def render_csv(columns, rows, dcontext, file, expand=False, nullvalue='', **kwargs):
    """Render the result of executing a query in text format.

    Args:
      columns: A list of beanquery.Column describing the table columns.
      rows: Data to render.
      dcontext: A DisplayContext object prepared for rendering numbers.
      file: A file object to render the results to.
      expand: A boolean, if true, expand columns that render to lists on multiple rows.
      nullvalue: String to use to represent NULL values.
    """
    ctx = RenderContext(dcontext, expand=expand, spaced=False, listsep=',', null=nullvalue)
    renderers = [_get_renderer(column.datatype, ctx) for column in columns]
    headers = [column.name for column in columns]

    # Prime the renderers.
    for row in rows:
        for value, renderer in zip(row, renderers):
            if value is not None:
                renderer.update(value)

    # Prepare the renders.
    [render.prepare() for render in renderers]

    # Write the CSV file.
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(render_rows(rows, renderers, ctx))
