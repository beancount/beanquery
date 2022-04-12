__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import io
import textwrap
import unittest

from decimal import Decimal

from beancount.core import display_context
from beancount.core.amount import A
from beancount.core.inventory import Inventory, from_string as I
from beancount.core.number import D
from beancount.core.position import from_string as P

from beanquery import query_render

def flatten(values):
    # Flatten lists of lists.
    for x in values:
        if isinstance(x, list):
            yield from flatten(x)
        else: yield x


class ColumnRendererBase(unittest.TestCase):

    # pylint: disable=not-callable
    renderer = None

    def setUp(self):
        dcontext = display_context.DisplayContext()
        self.ctx = query_render.RenderContext(dcontext, expand=True)

    def prepare(self, values):
        renderer = self.renderer(self.ctx)
        for value in values:
            renderer.update(value)
        width = renderer.prepare()
        return renderer, width

    def render(self, values):
        renderer, width = self.prepare(values)
        strings = [renderer.format(value) for value in values]
        self.assertTrue(all(len(s) == width for s in flatten(strings)))
        return strings


class TestColumnRenderer(ColumnRendererBase):

    renderer = query_render.ColumnRenderer

    def test_interface(self):
        renderer = self.renderer(self.ctx)
        with self.assertRaises(RuntimeError):
            w = renderer.width
        w = renderer.prepare()
        self.assertEqual(w, 0)
        self.assertEqual(renderer.width, 0)
        with self.assertRaises(NotImplementedError):
            renderer.format(None)


class ObjectRenderer(ColumnRendererBase):

    renderer = query_render.ObjectRenderer

    def test_object(self):
        self.assertEqual(self.render(["foo", 1, D('1.23'), datetime.date(1970, 1, 1)]), [
            'foo       ',
            '1         ',
            '1.23      ',
            '1970-01-01',
        ])


class TestBoolRenderer(ColumnRendererBase):

    renderer = query_render.BoolRenderer

    def test_bool(self):
        self.assertEqual(self.render([True, True]), [
            'TRUE',
            'TRUE',
        ])
        self.assertEqual(self.render([False, True]), [
            'FALSE',
            'TRUE ',
        ])
        self.assertEqual(self.render([False, False]), [
            'FALSE',
            'FALSE',
        ])


class TestStringRenderer(ColumnRendererBase):

    renderer = query_render.StringRenderer

    def test_string(self):
        self.assertEqual(self.render(['a', 'bb', 'ccc', '']), [
            'a  ',
            'bb ',
            'ccc',
            '   ',
        ])


class TestSetRenderer(ColumnRendererBase):

    renderer = query_render.SetRenderer

    def test_stringset(self):
        self.ctx.listsep = '+'
        self.assertEqual(self.render([{}, {'aaaa'}, {'bb', 'ccc'}]), [
            '      ',
            'aaaa  ',
            'bb+ccc',
        ])


class TestDateRenderer(ColumnRendererBase):

    renderer = query_render.DateRenderer

    def test_date(self):
        self.assertEqual(self.render([datetime.date(2014, 10, 3)]), ['2014-10-03'])


class TestIntRenderer(ColumnRendererBase):

    renderer = query_render.IntRenderer

    def test_integer(self):
        self.assertEqual(self.render([1, 22, 333]), [
            '  1',
            ' 22',
            '333',
        ])
        self.assertEqual(self.render([1, -22, 333]), [
            '  1',
            '-22',
            '333',
        ])
        self.assertEqual(self.render([1, 22, -333]), [
            '   1',
            '  22',
            '-333',
        ])


class TestDecimalRenderer(ColumnRendererBase):

    renderer = query_render.DecimalRenderer

    def test_integral(self):
        self.assertEqual(self.render([D('1'), D('12'), D('123'), D('1e4')]), [
            '   1',
            '  12',
            ' 123',
            '1E+4',
        ])
        self.assertEqual(self.render([D('1'), D('-12'), D('123')]), [
            '  1',
            '-12',
            '123',
        ])
        self.assertEqual(self.render([D('1'), D('12'), D('-123')]), [
            '   1',
            '  12',
            '-123',
        ])
        self.assertEqual(self.render([D('1'), D('12'), D('-1e3')]), [
            '    1',
            '   12',
            '-1E+3',
        ])

    def test_fractional(self):
        self.assertEqual(self.render([D('0.1'), D('1.2'), D('1.23'), D('1.234')]), [
            '0.1  ',
            '1.2  ',
            '1.23 ',
            '1.234',
        ])
        self.assertEqual(self.render([D('12'), D('1.2'), D('1.23'), D('12.345')]), [
            '12    ',
            ' 1.2  ',
            ' 1.23 ',
            '12.345',
        ])
        self.assertEqual(self.render([D('12'), D('1.2'), D('1.23'), D('-12.345')]), [
            ' 12    ',
            '  1.2  ',
            '  1.23 ',
            '-12.345',
        ])
        self.assertEqual(self.render([D('12'), D('1.2'), D('1.23'), D('-1.2e3')]), [
            '     12   ',
            '      1.2 ',
            '      1.23',
            '-1.2E+3   ',
        ])


class TestAmountRenderer(ColumnRendererBase):

    renderer = query_render.AmountRenderer

    def test_amount(self):
        self.assertEqual(self.render([A('100.00 USD')]), ['100.00 USD'])

    def test_quantization_one(self):
        self.ctx.dcontext.update(Decimal('1.0000'), 'ETH')
        self.assertEqual(self.ctx.dcontext.quantize(Decimal('1.0'), 'ETH'), Decimal('1.0000'))
        self.assertEqual(self.render([A('1 ETH')]), ['1.0000 ETH'])
        self.assertEqual(self.render([A('0.00001 ETH')]), ['0.0000 ETH'])

    def test_quantization_many(self):
        self.ctx.dcontext.update(Decimal('1.0000'), 'ETH')
        self.ctx.dcontext.update(Decimal('1.00'), 'USD')
        self.ctx.dcontext.update(Decimal('1'), 'XYZ')
        self.assertEqual(self.render([A('1.0 ETH')]), ['1.0000 ETH'])
        self.assertEqual(self.render([A('1.0 USD')]), ['1.00 USD'])
        self.assertEqual(self.render([A('1.0 XYZ')]), ['1 XYZ'])

    def test_number_padding(self):
        # FIXME: The leading space seems like a bug in
        # DisplayContext. Either it should always be there or it
        # should be there to support minus signs not encountered in
        # training or it shoudl not be there at all.
        self.assertEqual(self.render([A('1 XY'), A('12 XY'), A('123 XY'), A('-1 XY')]), [
            '   1 XY',
            '  12 XY',
            ' 123 XY',
            '  -1 XY',
        ])
        self.assertEqual(self.render([A('1 XY'), A('12 XY'), A('-12 XY')]), [
            '  1 XY',
            ' 12 XY',
            '-12 XY',
        ])

    def test_decimal_alignment(self):
        self.assertEqual(self.render([A('1.0 AA'), A('1.00 BB'), A('1.000 CC')]), [
            '1.0   AA',
            '1.00  BB',
            '1.000 CC',
        ])

    def test_currency_padding(self):
        self.assertEqual(self.render([A('1.00 XY'), A('1.00 XYZ'), A('1.00 XYZK')]), [
            '1.00 XY  ',
            '1.00 XYZ ',
            '1.00 XYZK',
        ])

    def test_many(self):
        self.assertEqual(self.render([A('0.0001 USD'), A('20.002 HOOL'), A('33 CA'), A('1098.20 AAPL')]), [
            '   0.0001 USD ',
            '  20.002  HOOL',
            '  33      CA  ',
            '1098.20   AAPL',
        ])


class TestPositionRenderer(ColumnRendererBase):

    renderer = query_render.PositionRenderer

    def setUp(self):
        super().setUp()
        # Prime the display context for some known commodities.
        self.ctx.dcontext = display_context.DisplayContext()
        self.ctx.dcontext.update(D('1.00'), 'USD')
        self.ctx.dcontext.update(D('1.00'), 'CAD')
        self.ctx.dcontext.update(D('1.000'), 'HOOL')
        self.ctx.dcontext.update(D('1'), 'CA')
        self.ctx.dcontext.update(D('1.00'), 'AAPL')

    def test_simple_poitions(self):
        self.assertEqual(self.render([P('3.0 USD'), P('3.0 CAD'), P('3.0 HOOL'), P('3.0 CA'), P('3.0 AAPL'), P('3.0 XY')]), [
            '3.00  USD ',
            '3.00  CAD ',
            '3.000 HOOL',
            '3     CA  ',
            '3.00  AAPL',
            '3.0   XY  ',
        ])

    def test_positions_with_price(self):
        self.assertEqual(self.render([P('5 HOOL {500.230000 USD}'), P('123.0 CA {1 HOOL}')]), [
            '  5.000 HOOL {500.23  USD }',
            '123     CA   {  1.000 HOOL}',
        ])


class TestInventoryRenderer(ColumnRendererBase):

    renderer = query_render.InventoryRenderer

    def setUp(self):
        super().setUp()
        # Prime the display context for some known commodities.
        self.ctx.dcontext = display_context.DisplayContext()
        self.ctx.dcontext.update(D('1.00'), 'USD')
        self.ctx.dcontext.update(D('1.00'), 'CAD')
        self.ctx.dcontext.update(D('1.000'), 'HOOL')
        self.ctx.dcontext.update(D('1'), 'CA')
        self.ctx.dcontext.update(D('1.00'), 'AAPL')

    def test_inventory(self):
        self.ctx.expand = True
        self.assertEqual(self.render([I('100 USD')]),[
            ['100.00 USD']
        ])
        self.assertEqual(self.render([I('5 HOOL {500.23 USD}')]), [
            ['5.000 HOOL {500.23 USD}']
        ])
        self.assertEqual(self.render([I('5 HOOL {500.23 USD}, 12.3456 CAAD')]), [
            ['12.3456 CAAD             ',
             ' 5.000  HOOL {500.23 USD}'],
        ])

    def test_inventory_no_expand(self):
        self.ctx.expand = False
        self.ctx.listsep = ' + '
        self.assertEqual(self.render([I('100 USD')]), [
            '100.00 USD'
        ])
        self.assertEqual(self.render([I('5 HOOL {500.23 USD}')]), [
            '5.000 HOOL {500.23 USD}'
        ])
        self.assertEqual(self.render([I('5 HOOL {500.23 USD}, 12.3456 CAAD')]), [
            '12.3456 CAAD + 5.000 HOOL {500.23 USD}',
        ])
        self.assertEqual(self.render([I('5 HOOL {500.23 USD}, 12.3456 CAAD'),
                                      I('55 HOOL {50.23 USD}, 2.3 CAAD')]), [
            '12.3456 CAAD +  5.000 HOOL {500.23 USD}',
            ' 2.3000 CAAD + 55.000 HOOL { 50.23 USD}',
        ])


class TestQueryRenderText(unittest.TestCase):

    def setUp(self):
        self.dcontext = display_context.DisplayContext()

    def render(self, types, rows, **kwargs):
        oss = io.StringIO()
        query_render.render_text(types, rows, self.dcontext, oss, **kwargs)
        return oss.getvalue()

    def test_render_simple(self):
        self.assertEqual(self.render(
            [('x', int), ('y', int), ('z', int)],
            [(1, 2, 3), (4, 5, 6)]), textwrap.dedent(
                """\
                x y z
                - - -
                1 2 3
                4 5 6
                """))

    def test_render_boxed(self):
        self.assertEqual(self.render(
            [('x', int), ('y', int), ('z', int)],
            [(1, 2, 3), (4, 5, 6)], boxed=True), textwrap.dedent(
                """\
                +---+---+---+
                | x | y | z |
                +---+---+---+
                | 1 | 2 | 3 |
                | 4 | 5 | 6 |
                +---+---+---+
                """))

    def test_render_header_centering(self):
        self.assertEqual(self.render(
            [('x', int), ('y', int), ('z', int)],
            [(1, 22222, 3), (4, 5, 6)]), textwrap.dedent(
                """\
                x   y   z
                - ----- -
                1 22222 3
                4     5 6
                """))

    def test_render_header_truncation(self):
        self.assertEqual(self.render(
            [('x', int), ('abcdefg', int), ('z', int)],
            [(1, 222, 3), (4, 5, 6)]), textwrap.dedent(
                """\
                x abc z
                - --- -
                1 222 3
                4   5 6
                """))

    def test_render_missing_values(self):
        self.assertEqual(self.render(
            [('xx', int), ('yy', int), ('zz', int)],
            [(12, None, 34), (None, 56, 78)]), textwrap.dedent(
                """\
                xx yy zz
                -- -- --
                12    34
                   56 78
                """))

    def test_render_missing_values_boxed(self):
        self.assertEqual(self.render(
            [('xx', int), ('yy', int), ('zz', int)],
            [(12, None, 34), (None, 56, 78)], boxed=True), textwrap.dedent(
                """\
                +----+----+----+
                | xx | yy | zz |
                +----+----+----+
                | 12 |    | 34 |
                |    | 56 | 78 |
                +----+----+----+
                """))

    def test_render_expand(self):
        self.assertEqual(self.render(
            [('x', int), ('inv', Inventory), ('q', int)],
            [(11, I('1.00 USD, 1.00 EUR, 1 TESTS'), 2),
             (33, I('2.00 EUR, 42 TESTS'), 4)], expand=True, boxed=True), textwrap.dedent(
                 """\
                 +----+-------------+---+
                 | x  |     inv     | q |
                 +----+-------------+---+
                 | 11 |  1.00 EUR   | 2 |
                 |    |  1    TESTS |   |
                 |    |  1.00 USD   |   |
                 | 33 |  2.00 EUR   | 4 |
                 |    | 42    TESTS |   |
                 +----+-------------+---+
                 """))

    def test_render_spaced(self):
        self.assertEqual(self.render(
            [('x', int)],
            [(1, ), (22, ), (333, )], spaced=True, boxed=True), textwrap.dedent(
                """\
                +-----+
                |  x  |
                +-----+
                |   1 |
                |     |
                |  22 |
                |     |
                | 333 |
                |     |
                +-----+
                """))


class TestQueryRenderCSV(unittest.TestCase):

    def setUp(self):
        self.dcontext = display_context.DisplayContext()

    def render(self, types, rows, **kwargs):
        oss = io.StringIO()
        query_render.render_csv(types, rows, self.dcontext, oss, **kwargs)
        # The csv modules emits DOS-style newlines.
        return oss.getvalue().replace('\r\n', '\n')

    def test_render_simple(self):
        self.assertEqual(self.render(
            [('x', int), ('y', int), ('z', int)],
            [(1, 2, 3), (4, 5, 6)]), textwrap.dedent(
                """\
                x,y,z
                1,2,3
                4,5,6
                """))

    def test_render_missing(self):
        self.assertEqual(self.render(
            [('x', int), ('y', int), ('z', int)],
            [(None, 2, 3), (4, None, 6)]), textwrap.dedent(
                """\
                x,y,z
                 ,2,3
                4, ,6
                """))

    def test_render_expand(self):
        self.assertEqual(self.render(
            [('x', int), ('inv', Inventory), ('q', int)],
            [(11, I('1.00 USD, 1.00 EUR, 1 TESTS'), 2),
             (33, I('2.00 EUR, 42 TESTS'), 4)], expand=True), "\n".join([
                 "x,inv,q",
                 "11, 1.00 EUR  ,2",
                 "  , 1    TESTS, ",
                 "  , 1.00 USD  , ",
                 "33, 2.00 EUR  ,4",
                 "  ,42    TESTS, ",
                 "",
             ]))
