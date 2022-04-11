"""Rendering of rows.
"""
__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import csv
import datetime

from decimal import Decimal
from itertools import zip_longest

from beancount.core import amount
from beancount.core import display_context
from beancount.core import inventory
from beancount.core import position


class RenderContext:
    """Hold the query rendering configuration."""

    def __init__(self, dcontext, expand=False, listsep=', ', spaced=False):
        self.dcontext = dcontext
        self.expand = expand
        self.listsep = listsep
        self.spaced = spaced


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

    def __init__(self, ctx):
        pass

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        RENDERERS[cls.dtype] = cls

    def update(self, value):
        """Update the rendered with the given value.

        Args:
          value: Any object of type ``dtype``.

        """

    def prepare(self):
        """Prepare to render all values of a column."""

    def width(self):
        """Return the computed width of this column."""
        raise NotImplementedError

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

    def __init__(self, ctx):
        super().__init__(ctx)
        self.maxwidth = 0

    def update(self, value):
        self.maxwidth = max(self.maxwidth, len(str(value)))

    def width(self):
        return self.maxwidth

    def format(self, value):
        return str(value).ljust(self.maxwidth)


class BoolRenderer(ColumnRenderer):
    dtype = bool

    def __init__(self, ctx):
        super().__init__(ctx)
        # The minimum width required for "TRUE" or "FALSE".
        self.maxwidth = 4

    def update(self, value):
        if not value:
            # With at least one "FALSE" we need 5 characters.
            self.maxwidth = 5

    def width(self):
        return self.maxwidth

    def format(self, value):
        return ('TRUE' if value else 'FALSE').ljust(self.maxwidth)


class StringRenderer(ColumnRenderer):
    dtype = str

    def __init__(self, ctx):
        super().__init__(ctx)
        self.maxwidth = 0

    def update(self, value):
        self.maxwidth = max(self.maxwidth, len(value))

    def width(self):
        return self.maxwidth

    def format(self, value):
        return value.ljust(self.maxwidth)


class SetRenderer(ColumnRenderer):
    dtype = set

    def __init__(self, ctx):
        super().__init__(ctx)
        self.maxwidth = 0
        self.sep = ctx.listsep

    def update(self, value):
        self.maxwidth = max(self.maxwidth, sum(len(x) + len(self.sep) for x in value) - len(self.sep))

    def width(self):
        return self.maxwidth

    def format(self, value):
        return self.sep.join(str(x) for x in sorted(value)).ljust(self.maxwidth)


class DateRenderer(ColumnRenderer):
    dtype = datetime.date

    def width(self):
        return 10

    def format(self, value):
        return value.strftime('%Y-%m-%d')


class IntRenderer(ColumnRenderer):
    dtype = int

    def __init__(self, ctx):
        super().__init__(ctx)
        self.maxwidth = 0

    def update(self, value):
        self.maxwidth = max(self.maxwidth, len(str(value)))

    def prepare(self):
        self.frmt = str(self.maxwidth)

    def width(self):
        return self.maxwidth

    def format(self, value):
        return format(value, self.frmt)


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

    def width(self):
        return self.maxwidth

    def format(self, value):
        n = value.as_tuple()
        if n.exponent > 0:
            # Special case for decimal numbers with positive exponent
            # and thus represented in scientific notation.
            return str(value).rjust(self.nintegral).ljust(self.maxwidth)
        # Compute the padding required to align the decimal point.
        left = self.nintegral - (max(1, len(n.digits) + n.exponent) + n.sign)
        return f'{"":>{left}}{str(value):<{self.maxwidth - left}}'


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
        self.maxwidth = 0

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

    def width(self):
        return self.maxwidth

    def format(self, value):
        return f'{self.func(value.number, value.currency)} {value.currency:<{self.curwidth}}'


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
        self.units_renderer.prepare()
        self.cost_renderer.prepare()
        units_width = self.units_renderer.width()
        cost_width = self.cost_renderer.width()
        self.maxwidth = units_width + cost_width + (3 if cost_width > 0 else 0)

    def width(self):
        return self.maxwidth

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
        # How many times we have seen a commodity.
        self.counters = collections.defaultdict(int)
        # Commodity specific renderers.
        self.renderers = collections.defaultdict(lambda: PositionRenderer(ctx))

    def update(self, value):
        for pos in value.get_positions():
            # We use the little indexing trick to do not have to
            # conditionalize this code on whether rows expansion is
            # enabled or not.
            self.counters[self.expand or pos.units.currency] += 1
            self.renderers[self.expand or pos.units.currency].update(pos)

    def prepare(self):
        for renderer in self.renderers.values():
            renderer.prepare()
        self.maxwidth = sum(r.width() + len(self.listsep) for r in self.renderers.values()) - len(self.listsep)
        # We want to present inventory content sorted by frequency of
        # appearance and by currency name. Sorting the renderers here
        # allows to just iterate the renderers in format() method.
        self.renderers = dict(sorted(self.renderers.items(), key=lambda x: (-self.counters[x[0]], x[0])))

    def width(self):
        return self.maxwidth

    def format(self, value):
        strings = []
        if self.expand:
            for pos in sorted(value.get_positions(), key=lambda pos: pos.units.currency):
                strings.append(self.renderers[self.expand].format(pos))
            return strings
        positions = {pos.units.currency: pos for pos in value.get_positions()}
        for commodity, fmt in self.renderers.items():
            pos = positions.get(commodity)
            strings.append(fmt.format(pos) if pos is not None else ' ' * fmt.width())
        return self.listsep.join(strings)


def get_renderers(result_types, result_rows, ctx):
    """Create renderers for each column and prepare them with the given data.

    Args:
      result_types: A list of items describing the names and data types of the items in
        each column.
      result_rows: A list of ResultRow instances.
      ctx: A RdenderContext object holding configuration.
    Returns:
      A list of subclass instances of ColumnRenderer.
    """
    renderers = [RENDERERS[dtype](ctx) for name, dtype in result_types]

    # Prime and prepare each of the renderers with the date in order to be ready
    # to begin rendering with correct alignment.
    for row in result_rows:
        for value, renderer in zip(row, renderers):
            if value is not None:
                renderer.update(value)

    for renderer in renderers:
        renderer.prepare()

    return renderers


def render_rows(result_types, result_rows, ctx):
    """Render the result of executing a query in text format.

    Args:
      result_types: A list of items describing the names and data types of the items in
        each column.
      result_rows: A list of ResultRow instances.
      ctx: The rendering contect
    """
    # Important notes:
    #
    # * Some of the data fields must be rendered on multiple lines. This code
    #   deals with this.
    #
    # * Some of the fields must be split into multiple fields for certain
    #   formats in order to be importable in a spreadsheet in a way that numbers
    #   are usable.

    if result_rows:
        assert len(result_types) == len(result_rows[0])

    # Create column renderers.
    renderers = get_renderers(result_types, result_rows, ctx)

    # Precompute a spacing row.
    if ctx.spaced:
        spacing_row = [''] * len(renderers)

    # Render all the columns of all the rows to strings.
    str_rows = []
    for row in result_rows:
        # Rendering each row involves rendering all the columns, each of which
        # produces one or more lines for its value, and then aligning those
        # columns together to produce a final list of rendered row. This means
        # that a single result row may result in multiple rendered rows.

        # Render all the columns of a row into either strings or lists of
        # strings. This routine also computes the maximum number of rows that a
        # rendered value will generate.
        exp_row = []
        max_lines = 1
        for value, renderer in zip(row, renderers):
            # Update the column renderer.
            exp_lines = renderer.format(value) if value is not None else ''
            if isinstance(exp_lines, list):
                if ctx.expand:
                    max_lines = max(max_lines, len(exp_lines))
                else:
                    # Join the lines onto a single cell.
                    exp_lines = ctx.listsep.join(exp_lines)
            exp_row.append(exp_lines)

        # If all the values were rendered directly to strings, this is a row that
        # renders on a single line. Just append this one row. This is the common
        # case.
        if max_lines == 1:
            str_rows.append(exp_row)

        # Some of the values rendered to more than one line; we need to render
        # them on separate lines and insert filler.
        else:
            # Make sure all values in the column are wrapped in sequences.
            exp_row = [exp_value if isinstance(exp_value, list) else (exp_value,)
                       for exp_value in exp_row]

            # Create a matrix of the column.
            str_lines = [[] for _ in range(max_lines)]
            for exp_value in exp_row:
                for index, exp_line in zip_longest(range(max_lines), exp_value,
                                                   fillvalue=''):
                    str_lines[index].append(exp_line)
            str_rows.extend(str_lines)

        if ctx.spaced:
            str_rows.append(spacing_row)

    return str_rows, renderers


def render_text(result_types, result_rows, dcontext, file, expand=False, boxed=False, spaced=False):
    """Render the result of executing a query in text format.

    Args:
      result_types: A list of items describing the names and data types of the items in
        each column.
      result_rows: A list of ResultRow instances.
      dcontext: A DisplayContext object prepared for rendering numbers.
      file: A file object to render the results to.
      expand: A boolean, if true, expand columns that render to lists on multiple rows.
      boxed: A boolean, true if we should render the results in a fancy-looking ASCII box.
      spaced: If true, leave an empty line between each of the rows. This is useful if the
        results have a lot of rows that render over multiple lines.
    """
    ctx = RenderContext(dcontext, expand=expand, spaced=spaced, listsep=' ')
    str_rows, renderers = render_rows(result_types, result_rows, ctx)

    # Compute a final format strings.
    formats = ['{{:{}}}'.format(max(renderer.width(), 1))
               for renderer in renderers]
    header_formats = ['{{:^{}.{}}}'.format(renderer.width(), renderer.width())
                      for renderer in renderers]
    if boxed:
        line_formatter = '| ' + ' | '.join(formats) + ' |\n'
        line_body = '-' + '-+-'.join(('-' * len(fmt.format(''))) for fmt in formats) + "-"
        top_line = ",{}.\n".format(line_body)
        middle_line = "+{}+\n".format(line_body)
        bottom_line = "`{}'\n".format(line_body)

        # Compute the header.
        header_formatter = '| ' + ' | '.join(header_formats) + ' |\n'
        header_line = header_formatter.format(*[name for name, _ in result_types])
    else:
        line_formatter = ' '.join(formats) + '\n'
        line_body = ' '.join(('-' * len(fmt.format(''))) for fmt in formats)
        top_line = None
        middle_line = "{}\n".format(line_body)
        bottom_line = None

        # Compute the header.
        header_formatter = ' '.join(header_formats) + '\n'
        header_line = header_formatter.format(*[name for name, _ in result_types])

    # Render each string row to a single line.
    if top_line:
        file.write(top_line)
    file.write(header_line)
    file.write(middle_line)
    for str_row in str_rows:
        line = line_formatter.format(*str_row)
        file.write(line)
    if bottom_line:
        file.write(bottom_line)


def render_csv(result_types, result_rows, dcontext, file, expand=False):
    """Render the result of executing a query in text format.

    Args:
      result_types: A list of items describing the names and data types of the items in
        each column.
      result_rows: A list of ResultRow instances.
      dcontext: A DisplayContext object prepared for rendering numbers.
      file: A file object to render the results to.
      expand: A boolean, if true, expand columns that render to lists on multiple rows.
    """
    ctx = RenderContext(dcontext, expand=expand, spaced=False)
    str_rows, renderers = render_rows(result_types, result_rows, ctx)

    writer = csv.writer(file)
    header_row = [name for name, _ in result_types]
    writer.writerow(header_row)
    writer.writerows(str_rows)
