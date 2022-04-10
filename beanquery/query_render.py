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
    """A renderer for positions. Inventories renders as a list of position
    strings. Both the unit numbers and the cost numbers are aligned, if any.
    """
    dtype = position.Position

    def __init__(self, ctx):
        self.units_rdr = AmountRenderer(ctx)
        self.cost_rdr = AmountRenderer(ctx)

    def update(self, value):
        if value is None:
            return
        self.units_rdr.update(value.units)
        self.cost_rdr.update(value.cost)

    def prepare(self):
        self.units_rdr.prepare()
        self.cost_rdr.prepare()

        units_width = self.units_rdr.width()
        cost_width = self.cost_rdr.width()

        #fmt_units = '{{{}}}'.format(':{}'.format(units_width) if units_width > 0 else '')
        fmt_units = '{{:{}}}'.format(units_width)

        if cost_width == 0:
            self.fmt_with_cost = None # Will not get used.
            self.fmt_without_cost = fmt_units
            self.total_width = units_width
        else:
            fmt_cost = '{{{{{{:{}}}}}}}'.format(cost_width)
            self.fmt_with_cost = '{} {}'.format(fmt_units, fmt_cost)
            self.fmt_without_cost = '{} {}'.format(
                fmt_units, ' ' * len(fmt_cost.format(self.cost_rdr.format(None))))
            self.total_width = len(self.fmt_with_cost.format('', ''))

        self.empty = ' ' * self.total_width

    def width(self):
        return self.total_width

    def format(self, value):
        if value is None:
            return self.empty

        strings = []
        if self.fmt_with_cost is None:
            strings.append(
                self.fmt_without_cost.format(
                    self.units_rdr.format(value.units)))
        else:
            cost = value.cost
            if cost:
                strings.append(
                    self.fmt_with_cost.format(
                        self.units_rdr.format(value.units),
                        self.cost_rdr.format(cost)))
            else:
                strings.append(
                    self.fmt_without_cost.format(
                        self.units_rdr.format(value.units)))

        if len(strings) == 1:
            return strings[0]
        if len(strings) == 0:
            return self.empty
        return strings


class InventoryRenderer(PositionRenderer):
    """A renderer for Inventory instances. Inventories renders as a list of position
    strings. Both the unit numbers and the cost numbers are aligned, if any.
    """
    dtype = inventory.Inventory

    def update(self, value):
        if value is None:
            return
        for pos in value.get_positions():
            super().update(pos)

    def format(self, value):
        strings = []
        if self.fmt_with_cost is None:
            for pos in value.get_positions():
                strings.append(
                    self.fmt_without_cost.format(
                        self.units_rdr.format(pos.units)))
        else:
            for pos in value.get_positions():
                cost = pos.cost
                if cost:
                    strings.append(
                        self.fmt_with_cost.format(
                            self.units_rdr.format(pos.units),
                            self.cost_rdr.format(cost)))
                else:
                    strings.append(
                        self.fmt_without_cost.format(
                            self.units_rdr.format(pos.units)))

        if len(strings) == 1:
            return strings[0]
        if len(strings) == 0:
            return self.empty
        return strings


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


# A mapping of data-type -> (render-function, alignment)
RENDERERS = {renderer_cls.dtype: renderer_cls
             for renderer_cls in [ObjectRenderer,
                                  BoolRenderer,
                                  StringRenderer,
                                  SetRenderer,
                                  IntRenderer,
                                  DecimalRenderer,
                                  DateRenderer,
                                  AmountRenderer,
                                  PositionRenderer,
                                  InventoryRenderer]}
