#!/usr/bin/env python

# CAVEAT UTILITOR
#
# This file was automatically generated by TatSu.
#
#    https://pypi.python.org/pypi/tatsu/
#
# Any changes you make to it will be overwritten the next time
# the file is generated.

from __future__ import annotations

import sys

from tatsu.buffering import Buffer
from tatsu.parsing import Parser
from tatsu.parsing import tatsumasu
from tatsu.parsing import leftrec, nomemo, isname # noqa
from tatsu.infos import ParserConfig
from tatsu.util import re, generic_main  # noqa


KEYWORDS = {
    'AND',
    'AS',
    'ASC',
    'BY',
    'DESC',
    'DISTINCT',
    'FALSE',
    'FROM',
    'GROUP',
    'HAVING',
    'IN',
    'IS',
    'LIMIT',
    'NOT',
    'OR',
    'ORDER',
    'PIVOT',
    'SELECT',
    'TRUE',
    'WHERE',
    'BALANCES',
    'JOURNAL',
    'PRINT',
}  # type: ignore


class BQLBuffer(Buffer):
    def __init__(self, text, /, config: ParserConfig = None, **settings):
        config = ParserConfig.new(
            config,
            owner=self,
            whitespace=None,
            nameguard=None,
            comments_re='(\\/\\*([^*]|[\\r\\n]|(\\*+([^*\\/]|[\\r\\n])))*\\*+\\/)',
            eol_comments_re='\\;[^\\n]*?$',
            ignorecase=True,
            namechars='',
            parseinfo=True,
        )
        config = config.replace(**settings)
        super().__init__(text, config=config)


class BQLParser(Parser):
    def __init__(self, /, config: ParserConfig = None, **settings):
        config = ParserConfig.new(
            config,
            owner=self,
            whitespace=None,
            nameguard=None,
            comments_re='(\\/\\*([^*]|[\\r\\n]|(\\*+([^*\\/]|[\\r\\n])))*\\*+\\/)',
            eol_comments_re='\\;[^\\n]*?$',
            ignorecase=True,
            namechars='',
            parseinfo=True,
            keywords=KEYWORDS,
            start='bql',
        )
        config = config.replace(**settings)
        super().__init__(config=config)

    @tatsumasu()
    def _bql_(self):  # noqa
        self._statement_()
        self.name_last_node('@')
        with self._optional():
            self._token(';')
        self._check_eof()

    @tatsumasu()
    def _statement_(self):  # noqa
        with self._choice():
            with self._option():
                self._select_()
            with self._option():
                self._balances_()
            with self._option():
                self._journal_()
            with self._option():
                self._print_()
            self._error(
                'expecting one of: '
                "'BALANCES' 'JOURNAL' 'PRINT' 'SELECT'"
                '<balances> <journal> <print> <select>'
            )

    @tatsumasu('Select')
    def _select_(self):  # noqa
        self._token('SELECT')
        with self._optional():
            self._token('DISTINCT')
            self._constant(True)
            self.name_last_node('distinct')

            self._define(
                ['distinct'],
                []
            )
        with self._group():
            with self._choice():
                with self._option():

                    def sep3():
                        self._token(',')

                    def block3():
                        self._target_()
                    self._positive_gather(block3, sep3)
                with self._option():
                    self._asterisk_()
                self._error(
                    'expecting one of: '
                    '<asterisk> <target>'
                )
        self.name_last_node('targets')
        with self._optional():
            self._token('FROM')
            with self._group():
                with self._choice():
                    with self._option():
                        self._table_()
                    with self._option():
                        self._subselect_()
                    with self._option():
                        self._from_()
                    self._error(
                        'expecting one of: '
                        '<from> <subselect> <table>'
                    )
            self.name_last_node('from_clause')

            self._define(
                ['from_clause'],
                []
            )
        with self._optional():
            self._token('WHERE')
            self._expression_()
            self.name_last_node('where_clause')

            self._define(
                ['where_clause'],
                []
            )
        with self._optional():
            self._token('GROUP')
            self._token('BY')
            self._groupby_()
            self.name_last_node('group_by')

            self._define(
                ['group_by'],
                []
            )
        with self._optional():
            self._token('ORDER')
            self._token('BY')

            def sep9():
                self._token(',')

            def block9():
                self._order_()
            self._positive_gather(block9, sep9)
            self.name_last_node('order_by')

            self._define(
                ['order_by'],
                []
            )
        with self._optional():
            self._token('PIVOT')
            self._token('BY')
            self._pivotby_()
            self.name_last_node('pivot_by')

            self._define(
                ['pivot_by'],
                []
            )
        with self._optional():
            self._token('LIMIT')
            self._integer_()
            self.name_last_node('limit')

            self._define(
                ['limit'],
                []
            )

        self._define(
            ['distinct', 'from_clause', 'group_by', 'limit', 'order_by', 'pivot_by', 'targets', 'where_clause'],
            []
        )

    @tatsumasu()
    def _subselect_(self):  # noqa
        self._token('(')
        self._select_()
        self.name_last_node('@')
        self._token(')')

    @tatsumasu('From')
    @nomemo
    def _from_(self):  # noqa
        with self._choice():
            with self._option():
                self._token('OPEN')
                self._cut()
                self._token('ON')
                self._date_()
                self.name_last_node('open')
                with self._optional():
                    self._token('CLOSE')
                    with self._group():
                        with self._choice():
                            with self._option():
                                self._token('ON')
                                self._date_()
                                self.name_last_node('close')

                                self._define(
                                    ['close'],
                                    []
                                )
                            with self._option():
                                self._empty_closure()
                                self._constant(True)
                                self.name_last_node('close')

                                self._define(
                                    ['close'],
                                    []
                                )
                            self._error(
                                'expecting one of: '
                                "'ON'"
                            )

                    self._define(
                        ['close'],
                        []
                    )
                with self._optional():
                    self._token('CLEAR')
                    self._constant(True)
                    self.name_last_node('clear')

                    self._define(
                        ['clear'],
                        []
                    )

                self._define(
                    ['clear', 'close', 'open'],
                    []
                )
            with self._option():
                self._token('CLOSE')
                self._cut()
                with self._group():
                    with self._choice():
                        with self._option():
                            self._token('ON')
                            self._date_()
                            self.name_last_node('close')

                            self._define(
                                ['close'],
                                []
                            )
                        with self._option():
                            self._empty_closure()
                            self._constant(True)
                            self.name_last_node('close')

                            self._define(
                                ['close'],
                                []
                            )
                        self._error(
                            'expecting one of: '
                            "'ON'"
                        )
                with self._optional():
                    self._token('CLEAR')
                    self._constant(True)
                    self.name_last_node('clear')

                    self._define(
                        ['clear'],
                        []
                    )

                self._define(
                    ['clear', 'close'],
                    []
                )
            with self._option():
                self._token('CLEAR')
                self._cut()
                self._constant(True)
                self.name_last_node('clear')

                self._define(
                    ['clear'],
                    []
                )
            with self._option():
                self._expression_()
                self.name_last_node('expression')
                with self._optional():
                    self._token('OPEN')
                    self._token('ON')
                    self._date_()
                    self.name_last_node('open')

                    self._define(
                        ['open'],
                        []
                    )
                with self._optional():
                    self._token('CLOSE')
                    with self._group():
                        with self._choice():
                            with self._option():
                                self._token('ON')
                                self._date_()
                                self.name_last_node('close')

                                self._define(
                                    ['close'],
                                    []
                                )
                            with self._option():
                                self._empty_closure()
                                self._constant(True)
                                self.name_last_node('close')

                                self._define(
                                    ['close'],
                                    []
                                )
                            self._error(
                                'expecting one of: '
                                "'ON'"
                            )

                    self._define(
                        ['close'],
                        []
                    )
                with self._optional():
                    self._token('CLEAR')
                    self._constant(True)
                    self.name_last_node('clear')

                    self._define(
                        ['clear'],
                        []
                    )

                self._define(
                    ['clear', 'close', 'expression', 'open'],
                    []
                )
            self._error(
                'expecting one of: '
                "'CLEAR' 'CLOSE' 'OPEN' <conjunction>"
                '<disjunction> <expression>'
            )

    @tatsumasu('Table')
    def _table_(self):  # noqa
        self._pattern('#([a-zA-Z_][a-zA-Z0-9_]*)?')
        self.name_last_node('name')

    @tatsumasu('GroupBy')
    def _groupby_(self):  # noqa

        def sep1():
            self._token(',')

        def block1():
            with self._group():
                with self._choice():
                    with self._option():
                        self._integer_()
                    with self._option():
                        self._expression_()
                    self._error(
                        'expecting one of: '
                        '<expression> <integer>'
                    )
        self._positive_gather(block1, sep1)
        self.name_last_node('columns')
        with self._optional():
            self._token('HAVING')
            self._expression_()
            self.name_last_node('having')

            self._define(
                ['having'],
                []
            )

        self._define(
            ['columns', 'having'],
            []
        )

    @tatsumasu('OrderBy')
    def _order_(self):  # noqa
        with self._group():
            with self._choice():
                with self._option():
                    self._integer_()
                with self._option():
                    self._expression_()
                self._error(
                    'expecting one of: '
                    '<expression> <integer>'
                )
        self.name_last_node('column')
        self._ordering_()
        self.name_last_node('ordering')

        self._define(
            ['column', 'ordering'],
            []
        )

    @tatsumasu()
    def _ordering_(self):  # noqa
        with self._optional():
            with self._choice():
                with self._option():
                    self._token('DESC')
                with self._option():
                    self._token('ASC')
                self._error(
                    'expecting one of: '
                    "'ASC' 'DESC'"
                )

    @tatsumasu('PivotBy')
    def _pivotby_(self):  # noqa
        with self._group():
            with self._choice():
                with self._option():
                    self._integer_()
                with self._option():
                    self._column_()
                self._error(
                    'expecting one of: '
                    '<column> <integer>'
                )
        self.add_last_node_to_name('columns')
        self._token(',')
        with self._group():
            with self._choice():
                with self._option():
                    self._integer_()
                with self._option():
                    self._column_()
                self._error(
                    'expecting one of: '
                    '<column> <integer>'
                )
        self.add_last_node_to_name('columns')

        self._define(
            [],
            ['columns']
        )

    @tatsumasu('Target')
    def _target_(self):  # noqa
        self._expression_()
        self.name_last_node('expression')
        with self._optional():
            self._token('AS')
            self._identifier_()
            self.name_last_node('name')

            self._define(
                ['name'],
                []
            )

        self._define(
            ['expression', 'name'],
            []
        )

    @tatsumasu()
    @nomemo
    def _expression_(self):  # noqa
        with self._choice():
            with self._option():
                self._disjunction_()
            with self._option():
                self._conjunction_()
            self._error(
                'expecting one of: '
                '<and> <conjunction> <disjunction>'
                '<inversion> <or>'
            )

    @tatsumasu()
    @nomemo
    def _disjunction_(self):  # noqa
        with self._choice():
            with self._option():
                self._or_()
            with self._option():
                self._conjunction_()
            self._error(
                'expecting one of: '
                '<and> <conjunction> <inversion> <or>'
            )

    @tatsumasu('Or')
    @nomemo
    def _or_(self):  # noqa
        self._conjunction_()
        self.add_last_node_to_name('args')

        def block1():
            self._token('OR')
            self._conjunction_()
            self.add_last_node_to_name('args')

            self._define(
                [],
                ['args']
            )
        self._positive_closure(block1)

        self._define(
            [],
            ['args']
        )

    @tatsumasu()
    @nomemo
    def _conjunction_(self):  # noqa
        with self._choice():
            with self._option():
                self._and_()
            with self._option():
                self._inversion_()
            self._error(
                'expecting one of: '
                '<and> <comparison> <inversion> <not>'
            )

    @tatsumasu('And')
    @nomemo
    def _and_(self):  # noqa
        self._inversion_()
        self.add_last_node_to_name('args')

        def block1():
            self._token('AND')
            self._inversion_()
            self.add_last_node_to_name('args')

            self._define(
                [],
                ['args']
            )
        self._positive_closure(block1)

        self._define(
            [],
            ['args']
        )

    @tatsumasu()
    @nomemo
    def _inversion_(self):  # noqa
        with self._choice():
            with self._option():
                self._not_()
            with self._option():
                self._comparison_()
            self._error(
                'expecting one of: '
                "'NOT' <between> <comparison> <eq> <gt>"
                '<gte> <in> <isnotnull> <isnull> <lt>'
                '<lte> <match> <neq> <not> <notin>'
                '<notmatch> <sum>'
            )

    @tatsumasu('Not')
    def _not_(self):  # noqa
        self._token('NOT')
        self._inversion_()
        self.name_last_node('operand')

        self._define(
            ['operand'],
            []
        )

    @tatsumasu()
    @nomemo
    def _comparison_(self):  # noqa
        with self._choice():
            with self._option():
                self._lt_()
            with self._option():
                self._lte_()
            with self._option():
                self._gt_()
            with self._option():
                self._gte_()
            with self._option():
                self._eq_()
            with self._option():
                self._neq_()
            with self._option():
                self._in_()
            with self._option():
                self._notin_()
            with self._option():
                self._match_()
            with self._option():
                self._notmatch_()
            with self._option():
                self._isnull_()
            with self._option():
                self._isnotnull_()
            with self._option():
                self._between_()
            with self._option():
                self._sum_()
            self._error(
                'expecting one of: '
                '<add> <between> <eq> <gt> <gte> <in>'
                '<isnotnull> <isnull> <lt> <lte> <match>'
                '<neq> <notin> <notmatch> <sub> <sum>'
                '<term>'
            )

    @tatsumasu('Less')
    @nomemo
    def _lt_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('<')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('LessEq')
    def _lte_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('<=')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('Greater')
    def _gt_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('>')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('GreaterEq')
    def _gte_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('>=')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('Equal')
    def _eq_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('=')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('NotEqual')
    def _neq_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('!=')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('In')
    def _in_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('IN')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('NotIn')
    def _notin_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('NOT')
        self._token('IN')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('Match')
    def _match_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('~')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('NotMatch')
    def _notmatch_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('!~')
        self._sum_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('IsNull')
    def _isnull_(self):  # noqa
        self._sum_()
        self.name_last_node('operand')
        self._token('IS')
        self._token('NULL')

        self._define(
            ['operand'],
            []
        )

    @tatsumasu('IsNotNull')
    def _isnotnull_(self):  # noqa
        self._sum_()
        self.name_last_node('operand')
        self._token('IS')
        self._token('NOT')
        self._token('NULL')

        self._define(
            ['operand'],
            []
        )

    @tatsumasu('Between')
    def _between_(self):  # noqa
        self._sum_()
        self.name_last_node('operand')
        self._token('BETWEEN')
        self._sum_()
        self.name_last_node('lower')
        self._token('AND')
        self._sum_()
        self.name_last_node('upper')

        self._define(
            ['lower', 'operand', 'upper'],
            []
        )

    @tatsumasu()
    @leftrec
    def _sum_(self):  # noqa
        with self._choice():
            with self._option():
                self._add_()
            with self._option():
                self._sub_()
            with self._option():
                self._term_()
            self._error(
                'expecting one of: '
                '<add> <div> <factor> <mod> <mul> <sub>'
                '<sum> <term>'
            )

    @tatsumasu('Add')
    @nomemo
    def _add_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('+')
        self._cut()
        self._term_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('Sub')
    @nomemo
    def _sub_(self):  # noqa
        self._sum_()
        self.name_last_node('left')
        self._token('-')
        self._cut()
        self._term_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu()
    @leftrec
    def _term_(self):  # noqa
        with self._choice():
            with self._option():
                self._mul_()
            with self._option():
                self._div_()
            with self._option():
                self._mod_()
            with self._option():
                self._factor_()
            self._error(
                'expecting one of: '
                "'(' <div> <factor> <mod> <mul> <term>"
                '<unary>'
            )

    @tatsumasu('Mul')
    @nomemo
    def _mul_(self):  # noqa
        self._term_()
        self.name_last_node('left')
        self._token('*')
        self._cut()
        self._factor_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('Div')
    @nomemo
    def _div_(self):  # noqa
        self._term_()
        self.name_last_node('left')
        self._token('/')
        self._cut()
        self._factor_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu('Mod')
    @nomemo
    def _mod_(self):  # noqa
        self._term_()
        self.name_last_node('left')
        self._token('%')
        self._cut()
        self._factor_()
        self.name_last_node('right')

        self._define(
            ['left', 'right'],
            []
        )

    @tatsumasu()
    @nomemo
    def _factor_(self):  # noqa
        with self._choice():
            with self._option():
                self._unary_()
            with self._option():
                self._token('(')
                self._expression_()
                self.name_last_node('@')
                self._token(')')
            self._error(
                'expecting one of: '
                "'(' <primary> <uminus> <unary> <uplus>"
            )

    @tatsumasu()
    @nomemo
    def _unary_(self):  # noqa
        with self._choice():
            with self._option():
                self._uplus_()
            with self._option():
                self._uminus_()
            with self._option():
                self._primary_()
            self._error(
                'expecting one of: '
                "'+' '-' <atom> <attribute> <primary>"
                '<subscript> <uminus> <uplus>'
            )

    @tatsumasu()
    def _uplus_(self):  # noqa
        self._token('+')
        self._atom_()
        self.name_last_node('@')

    @tatsumasu('Neg')
    def _uminus_(self):  # noqa
        self._token('-')
        self._factor_()
        self.name_last_node('operand')

        self._define(
            ['operand'],
            []
        )

    @tatsumasu()
    @leftrec
    def _primary_(self):  # noqa
        with self._choice():
            with self._option():
                self._attribute_()
            with self._option():
                self._subscript_()
            with self._option():
                self._atom_()
            self._error(
                'expecting one of: '
                "'SELECT' <atom> <attribute> <column>"
                '<constant> <function> <placeholder>'
                '<primary> <select> <subscript>'
            )

    @tatsumasu('Attribute')
    @nomemo
    def _attribute_(self):  # noqa
        self._primary_()
        self.name_last_node('operand')
        self._token('.')
        self._identifier_()
        self.name_last_node('name')

        self._define(
            ['name', 'operand'],
            []
        )

    @tatsumasu('Subscript')
    @nomemo
    def _subscript_(self):  # noqa
        self._primary_()
        self.name_last_node('operand')
        self._token('[')
        self._string_()
        self.name_last_node('key')
        self._token(']')

        self._define(
            ['key', 'operand'],
            []
        )

    @tatsumasu()
    def _atom_(self):  # noqa
        with self._choice():
            with self._option():
                self._select_()
            with self._option():
                self._function_()
            with self._option():
                self._constant_()
            with self._option():
                self._column_()
            with self._option():
                self._placeholder_()
            self._error(
                'expecting one of: '
                "'%(' '%s' 'SELECT' <boolean> <column>"
                '<constant> <date> <decimal> <function>'
                '<identifier> <integer> <list> <literal>'
                '<null> <placeholder> <select> <string>'
            )

    @tatsumasu('Placeholder')
    def _placeholder_(self):  # noqa
        with self._choice():
            with self._option():
                self._token('%s')
                self._constant('')
                self.name_last_node('name')

                self._define(
                    ['name'],
                    []
                )
            with self._option():
                self._token('%(')
                self._identifier_()
                self.name_last_node('name')
                self._token(')s')

                self._define(
                    ['name'],
                    []
                )
            self._error(
                'expecting one of: '
                "'%(' '%s'"
            )

    @tatsumasu('Function')
    def _function_(self):  # noqa
        with self._choice():
            with self._option():
                self._identifier_()
                self.name_last_node('fname')
                self._token('(')

                def sep3():
                    self._token(',')

                def block3():
                    self._expression_()
                self._gather(block3, sep3)
                self.name_last_node('operands')
                self._token(')')

                self._define(
                    ['fname', 'operands'],
                    []
                )
            with self._option():
                self._identifier_()
                self.name_last_node('fname')
                self._token('(')
                self._asterisk_()
                self.add_last_node_to_name('operands')
                self._token(')')

                self._define(
                    ['fname'],
                    ['operands']
                )
            self._error(
                'expecting one of: '
                '<identifier> [a-zA-Z_][a-zA-Z0-9_]*'
            )

    @tatsumasu('Column')
    def _column_(self):  # noqa
        self._identifier_()
        self.name_last_node('name')

    @tatsumasu()
    def _literal_(self):  # noqa
        with self._choice():
            with self._option():
                self._date_()
            with self._option():
                self._decimal_()
            with self._option():
                self._integer_()
            with self._option():
                self._string_()
            with self._option():
                self._null_()
            with self._option():
                self._boolean_()
            self._error(
                'expecting one of: '
                "'FALSE' 'NULL' 'TRUE'"
                '([0-9]+\\.[0-9]*|[0-9]*\\.[0-9]+)'
                '(\\"[^\\"]*\\"|\\\'[^\\\']*\\\')'
                '(\\d{4}-\\d{2}-\\d{2}) <boolean> <date>'
                '<decimal> <integer> <null> <string> \\d+'
            )

    @tatsumasu('Constant')
    def _constant_(self):  # noqa
        with self._group():
            with self._choice():
                with self._option():
                    self._literal_()
                with self._option():
                    self._list_()
                self._error(
                    'expecting one of: '
                    "'(' 'FALSE' 'NULL' 'TRUE'"
                    '([0-9]+\\.[0-9]*|[0-9]*\\.[0-9]+)'
                    '(\\"[^\\"]*\\"|\\\'[^\\\']*\\\')'
                    '(\\d{4}-\\d{2}-\\d{2}) <boolean> <date>'
                    '<decimal> <integer> <list> <literal>'
                    '<null> <string> \\d+'
                )
        self.name_last_node('value')

    @tatsumasu()
    def _list_(self):  # noqa
        self._token('(')
        with self._if():
            with self._group():
                self._literal_()
                self._token(',')

        def sep1():
            self._token(',')

        def block1():
            with self._group():
                with self._choice():
                    with self._option():
                        self._literal_()
                    with self._option():
                        self._void()
                    self._error(
                        'expecting one of: '
                        '<literal>'
                    )
        self._positive_gather(block1, sep1)
        self.name_last_node('@')
        self._token(')')

    @tatsumasu()
    @isname
    def _identifier_(self):  # noqa
        self._pattern('[a-zA-Z_][a-zA-Z0-9_]*')

    @tatsumasu()
    def _asterisk_(self):  # noqa
        self._token('*')

    @tatsumasu()
    def _string_(self):  # noqa
        self._pattern('(\\"[^\\"]*\\"|\\\'[^\\\']*\\\')')

    @tatsumasu()
    def _boolean_(self):  # noqa
        with self._choice():
            with self._option():
                self._token('TRUE')
            with self._option():
                self._token('FALSE')
            self._error(
                'expecting one of: '
                "'FALSE' 'TRUE'"
            )

    @tatsumasu()
    def _null_(self):  # noqa
        self._token('NULL')

    @tatsumasu()
    def _integer_(self):  # noqa
        self._pattern('\\d+')

    @tatsumasu()
    def _decimal_(self):  # noqa
        self._pattern('([0-9]+\\.[0-9]*|[0-9]*\\.[0-9]+)')

    @tatsumasu()
    def _date_(self):  # noqa
        self._pattern('(\\d{4}-\\d{2}-\\d{2})')

    @tatsumasu('Balances')
    def _balances_(self):  # noqa
        self._token('BALANCES')
        with self._optional():
            self._token('AT')
            self._identifier_()
            self.name_last_node('summary_func')

            self._define(
                ['summary_func'],
                []
            )
        with self._optional():
            self._token('FROM')
            self._from_()
            self.name_last_node('from_clause')

            self._define(
                ['from_clause'],
                []
            )
        with self._optional():
            self._token('WHERE')
            self._expression_()
            self.name_last_node('where_clause')

            self._define(
                ['where_clause'],
                []
            )

        self._define(
            ['from_clause', 'summary_func', 'where_clause'],
            []
        )

    @tatsumasu('Journal')
    def _journal_(self):  # noqa
        self._token('JOURNAL')
        with self._optional():
            self._string_()
            self.name_last_node('account')
        with self._optional():
            self._token('AT')
            self._identifier_()
            self.name_last_node('summary_func')

            self._define(
                ['summary_func'],
                []
            )
        with self._optional():
            self._token('FROM')
            self._from_()
            self.name_last_node('from_clause')

            self._define(
                ['from_clause'],
                []
            )

        self._define(
            ['account', 'from_clause', 'summary_func'],
            []
        )

    @tatsumasu('Print')
    def _print_(self):  # noqa
        self._token('PRINT')
        with self._optional():
            self._token('FROM')
            self._from_()
            self.name_last_node('from_clause')

            self._define(
                ['from_clause'],
                []
            )

        self._define(
            ['from_clause'],
            []
        )


class BQLSemantics:
    def bql(self, ast):  # noqa
        return ast

    def statement(self, ast):  # noqa
        return ast

    def select(self, ast):  # noqa
        return ast

    def subselect(self, ast):  # noqa
        return ast

    def from_(self, ast):  # noqa
        return ast

    def table(self, ast):  # noqa
        return ast

    def groupby(self, ast):  # noqa
        return ast

    def order(self, ast):  # noqa
        return ast

    def ordering(self, ast):  # noqa
        return ast

    def pivotby(self, ast):  # noqa
        return ast

    def target(self, ast):  # noqa
        return ast

    def expression(self, ast):  # noqa
        return ast

    def disjunction(self, ast):  # noqa
        return ast

    def or_(self, ast):  # noqa
        return ast

    def conjunction(self, ast):  # noqa
        return ast

    def and_(self, ast):  # noqa
        return ast

    def inversion(self, ast):  # noqa
        return ast

    def not_(self, ast):  # noqa
        return ast

    def comparison(self, ast):  # noqa
        return ast

    def lt(self, ast):  # noqa
        return ast

    def lte(self, ast):  # noqa
        return ast

    def gt(self, ast):  # noqa
        return ast

    def gte(self, ast):  # noqa
        return ast

    def eq(self, ast):  # noqa
        return ast

    def neq(self, ast):  # noqa
        return ast

    def in_(self, ast):  # noqa
        return ast

    def notin(self, ast):  # noqa
        return ast

    def match(self, ast):  # noqa
        return ast

    def notmatch(self, ast):  # noqa
        return ast

    def isnull(self, ast):  # noqa
        return ast

    def isnotnull(self, ast):  # noqa
        return ast

    def between(self, ast):  # noqa
        return ast

    def sum(self, ast):  # noqa
        return ast

    def add(self, ast):  # noqa
        return ast

    def sub(self, ast):  # noqa
        return ast

    def term(self, ast):  # noqa
        return ast

    def mul(self, ast):  # noqa
        return ast

    def div(self, ast):  # noqa
        return ast

    def mod(self, ast):  # noqa
        return ast

    def factor(self, ast):  # noqa
        return ast

    def unary(self, ast):  # noqa
        return ast

    def uplus(self, ast):  # noqa
        return ast

    def uminus(self, ast):  # noqa
        return ast

    def primary(self, ast):  # noqa
        return ast

    def attribute(self, ast):  # noqa
        return ast

    def subscript(self, ast):  # noqa
        return ast

    def atom(self, ast):  # noqa
        return ast

    def placeholder(self, ast):  # noqa
        return ast

    def function(self, ast):  # noqa
        return ast

    def column(self, ast):  # noqa
        return ast

    def literal(self, ast):  # noqa
        return ast

    def constant(self, ast):  # noqa
        return ast

    def list(self, ast):  # noqa
        return ast

    def identifier(self, ast):  # noqa
        return ast

    def asterisk(self, ast):  # noqa
        return ast

    def string(self, ast):  # noqa
        return ast

    def boolean(self, ast):  # noqa
        return ast

    def null(self, ast):  # noqa
        return ast

    def integer(self, ast):  # noqa
        return ast

    def decimal(self, ast):  # noqa
        return ast

    def date(self, ast):  # noqa
        return ast

    def balances(self, ast):  # noqa
        return ast

    def journal(self, ast):  # noqa
        return ast

    def print(self, ast):  # noqa
        return ast


def main(filename, **kwargs):
    if not filename or filename == '-':
        text = sys.stdin.read()
    else:
        with open(filename) as f:
            text = f.read()
    parser = BQLParser()
    return parser.parse(
        text,
        filename=filename,
        **kwargs
    )


if __name__ == '__main__':
    import json
    from tatsu.util import asjson

    ast = generic_main(main, BQLParser, name='BQL')
    data = asjson(ast)
    print(json.dumps(data, indent=2))
