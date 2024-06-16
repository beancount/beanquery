import collections.abc

from decimal import Decimal
from functools import singledispatchmethod
from typing import Optional, Sequence, Mapping

from . import types
from . import parser
from .errors import ProgrammingError
from .parser import ast

from .query_compile import (
    EvalAggregator,
    EvalAnd,
    EvalCoalesce,
    EvalColumn,
    EvalConstant,
    EvalGetItem,
    EvalGetter,
    EvalOr,
    EvalPivot,
    EvalPrint,
    EvalQuery,
    EvalTarget,
    FUNCTIONS,
    OPERATORS,
    SubqueryTable,
)


# A global constant which sets whether we support inferred/implicit group-by
# semantics.
SUPPORT_IMPLICIT_GROUPBY = True


class CompilationError(ProgrammingError):
    def __init__(self, message, ast=None):
        super().__init__(message)
        self.parseinfo = ast.parseinfo if ast is not None else None


class Compiler:
    def __init__(self, context):
        self.context = context
        self.table = context.tables.get('postings')

    def compile(self, query, parameters=None):
        """Compile an AST into an executable statement."""
        self.parameters = parameters

        placeholders = [node for node in query.walk() if isinstance(node, ast.Placeholder)]
        if placeholders:
            names = {placeholder.name for placeholder in placeholders}
            if all(names):
                if not isinstance(parameters, Mapping):
                    raise TypeError('query parameters should be a mapping when using named placeholders')
                if names - parameters.keys():
                    missing = ', '.join(sorted(names - parameters.keys()))
                    raise ProgrammingError(f'query parameter missing: {missing}')
            elif not any(names):
                if not isinstance(parameters, Sequence):
                    raise TypeError('query parameters should be a sequence when using positional placeholders')
                if len(placeholders) != len(parameters):
                    raise ProgrammingError(
                        f'the query has {len(placeholders)} placeholders but {len(parameters)} parameters were passed')
                for i, placeholder in enumerate(sorted(placeholders, key=lambda node: node.parseinfo.pos)):
                    placeholder.name = i
            else:
                raise ProgrammingError('positional and named parameters cannot be mixed')

        return self._compile(query)

    @singledispatchmethod
    def _compile(self, node: Optional[ast.Node]):
        if node is None:
            return None
        raise NotImplementedError

    @_compile.register
    def _select(self, node: ast.Select):

        # Compile the FROM clause.
        c_from_expr = self._compile_from(node.from_clause)

        # Compile the targets.
        c_targets = self._compile_targets(node.targets)

        # Bind the WHERE expression to the execution environment.
        c_where = self._compile(node.where_clause)

        # Check that the FROM clause does not contain aggregates. This
        # should never trigger if the compilation environment does not
        # contain any aggregate.
        if c_where is not None and is_aggregate(c_where):
            raise CompilationError('aggregates are not allowed in WHERE clause')

        # Combine FROM and WHERE clauses
        if c_from_expr is not None:
            c_where = c_from_expr if c_where is None else EvalAnd([c_from_expr, c_where])

        # Process the GROUP-BY clause.
        new_targets, group_indexes, having_index = self._compile_group_by(node.group_by, c_targets)
        c_targets.extend(new_targets)

        # Process the ORDER-BY clause.
        new_targets, order_spec = self._compile_order_by(node.order_by, c_targets)
        c_targets.extend(new_targets)

        # If this is an aggregate query (it groups, see list of indexes), check that
        # the set of non-aggregates match exactly the group indexes. This should
        # always be the case at this point, because we have added all the necessary
        # targets to the list of group-by expressions and should have resolved all
        # the indexes.
        if group_indexes is not None:
            non_aggregate_indexes = {index for index, c_target in enumerate(c_targets)
                                     if not c_target.is_aggregate}
            if non_aggregate_indexes != set(group_indexes):
                missing_names = ['"{}"'.format(c_targets[index].name)
                                 for index in non_aggregate_indexes - set(group_indexes)]
                raise CompilationError(
                    'all non-aggregates must be covered by GROUP-BY clause in aggregate query: '
                    'the following targets are missing: {}'.format(','.join(missing_names)))

        query = EvalQuery(self.table,
                          c_targets,
                          c_where,
                          group_indexes,
                          having_index,
                          order_spec,
                          node.limit,
                          node.distinct)

        pivots = self._compile_pivot_by(node.pivot_by, c_targets, group_indexes)
        if pivots:
            return EvalPivot(query, pivots)

        return query

    def _compile_from(self, node):
        if node is None:
            return None

        # Subquery.
        if isinstance(node, ast.Select):
            self.table = SubqueryTable(self._compile(node))
            return None

        # Table reference.
        if isinstance(node, ast.Table):
            self.table = self.context.tables.get(node.name)
            if self.table is None:
                raise CompilationError(f'table "{node.name}" does not exist', node)
            return None

        # FROM expression.
        if isinstance(node, ast.From):
            c_expression = self._compile(node.expression)

            # Check that the FROM clause does not contain aggregates.
            if c_expression is not None and is_aggregate(c_expression):
                raise CompilationError('aggregates are not allowed in FROM clause')

            if node.open and node.close and node.open > node.close:
                raise CompilationError('CLOSE date must follow OPEN date')

            # Apply OPEN, CLOSE, and CLEAR clauses.
            self.table = self.table.update(open=node.open, close=node.close, clear=node.clear)

            return c_expression

        raise NotImplementedError

    def _compile_targets(self, targets):
        """Compile the targets and check for their validity. Process wildcard.

        Args:
          targets: A list of target expressions from the parser.
        Returns:
          A list of compiled target expressions with resolved names.
        """
        # Bind the targets expressions to the execution context.
        if isinstance(targets, ast.Asterisk):
            # Insert the full list of available columns.
            targets = [ast.Target(ast.Column(name), None)
                       for name in self.table.wildcard_columns]

        # Compile targets.
        c_targets = []
        for target in targets:
            c_expr = self._compile(target.expression)
            name = get_target_name(target)
            c_targets.append(EvalTarget(c_expr, name, is_aggregate(c_expr)))

            columns, aggregates = get_columns_and_aggregates(c_expr)

            # Check for mixed aggregates and non-aggregates.
            if columns and aggregates:
                raise CompilationError('mixed aggregates and non-aggregates are not allowed')

            # Check for aggregates of aggregates.
            for aggregate in aggregates:
                for child in aggregate.childnodes():
                    if is_aggregate(child):
                        raise CompilationError('aggregates of aggregates are not allowed')

        return c_targets

    def _compile_order_by(self, order_by, c_targets):
        """Process an order-by clause.

        Args:
          order_by: A OrderBy instance as provided by the parser.
          c_targets: A list of compiled target expressions.
        Returns:
          A tuple of
           new_targets: A list of new compiled target nodes.
           order_spec: A list of (integer indexes, sort order) tuples.
        """
        if not order_by:
            return [], None

        new_targets = c_targets[:]
        c_target_expressions = [c_target.c_expr for c_target in c_targets]
        order_spec = []

        # Compile order-by expressions and resolve them to their targets if
        # possible. A ORDER-BY column may be one of the following:
        #
        # * A reference to a target by name.
        # * A reference to a target by index (starting at one).
        # * A new expression, aggregate or not.
        #
        # References by name are converted to indexes. New expressions are
        # inserted into the list of targets as invisible targets.
        targets_name_map = {target.name: index for index, target in enumerate(c_targets)}
        for spec in order_by:
            column = spec.column
            descending = spec.ordering
            index = None

            # Process target references by index.
            if isinstance(column, int):
                index = column - 1
                if not 0 <= index < len(c_targets):
                    raise CompilationError(f'invalid ORDER-BY column index {column}')

            else:
                # Process target references by name. These will be parsed as
                # simple Column expressions. If they refer to a target name, we
                # resolve them.
                if isinstance(column, ast.Column):
                    name = column.name
                    index = targets_name_map.get(name, None)

                # Otherwise we compile the expression and add it to the list of
                # targets to evaluate and index into that new target.
                if index is None:
                    c_expr = self._compile(column)

                    # Attempt to reconcile the expression with one of the existing
                    # target expressions.
                    try:
                        index = c_target_expressions.index(c_expr)
                    except ValueError:
                        # Add the new target. 'None' for the target name implies it
                        # should be invisible, not to be rendered.
                        index = len(new_targets)
                        new_targets.append(EvalTarget(c_expr, None, is_aggregate(c_expr)))
                        c_target_expressions.append(c_expr)

            assert index is not None, "Internal error, could not index order-by reference."
            order_spec.append((index, descending))

        return new_targets[len(c_targets):], order_spec

    def _compile_pivot_by(self, pivot_by, targets, group_indexes):
        """Compiles a PIVOT BY clause.

        Resolve and validate columns references in the PIVOT BY clause.
        The PIVOT BY clause accepts two name od index references to
        columns in the SELECT targets list. The second columns should be a
        GROUP BY column so that the values of the pivot column are unique.

        """
        if pivot_by is None:
            return None

        indexes = []
        names = {target.name: index for index, target in enumerate(targets)}

        for column in pivot_by.columns:

            # Process target references by index.
            if isinstance(column, int):
                index = column - 1
                if not 0 <= index < len(targets):
                    raise CompilationError(f'invalid PIVOT BY column index {column}')
                indexes.append(index)
                continue

            # Process target references by name.
            if isinstance(column, ast.Column):
                index = names.get(column.name, None)
                if index is None:
                    raise CompilationError(f'PIVOT BY column {column!r} is not in the targets list')
                indexes.append(index)
                continue

            # Not reached.
            raise RuntimeError

        # Sanity checks.
        if indexes[0] == indexes[1]:
            raise CompilationError('the two PIVOT BY columns cannot be the same column')
        if indexes[1] not in group_indexes:
            raise CompilationError('the second PIVOT BY column must be a GROUP BY column')

        return indexes

    def _compile_group_by(self, group_by, c_targets):
        """Process a group-by clause.

        Args:
          group_by: A GroupBy instance as provided by the parser.
          c_targets: A list of compiled target expressions.
        Returns:
          A tuple of
           new_targets: A list of new compiled target nodes.
           group_indexes: If the query is an aggregate query, a list of integer
             indexes to be used for processing grouping. Note that this list may be
             empty (in the case of targets with only aggregates). On the other hand,
             if this is not an aggregated query, this is set to None. So do
             distinguish the empty list vs. None.
        """
        new_targets = c_targets[:]
        c_target_expressions = [c_target.c_expr for c_target in c_targets]

        group_indexes = []
        having_index = None

        if group_by:
            assert group_by.columns, "Internal error with GROUP-BY parsing"

            # Compile group-by expressions and resolve them to their targets if
            # possible. A GROUP-BY column may be one of the following:
            #
            # * A reference to a target by name.
            # * A reference to a target by index (starting at one).
            # * A new, non-aggregate expression.
            #
            # References by name are converted to indexes. New expressions are
            # inserted into the list of targets as invisible targets.
            targets_name_map = {target.name: index for index, target in enumerate(c_targets)}
            for column in group_by.columns:
                index = None

                # Process target references by index.
                if isinstance(column, int):
                    index = column - 1
                    if not 0 <= index < len(c_targets):
                        raise CompilationError(f'invalid GROUP-BY column index {column}')

                else:
                    # Process target references by name. These will be parsed as
                    # simple Column expressions. If they refer to a target name, we
                    # resolve them.
                    if isinstance(column, ast.Column):
                        name = column.name
                        index = targets_name_map.get(name, None)

                    # Otherwise we compile the expression and add it to the list of
                    # targets to evaluate and index into that new target.
                    if index is None:
                        c_expr = self._compile(column)

                        # Check if the new expression is an aggregate.
                        aggregate = is_aggregate(c_expr)
                        if aggregate:
                            raise CompilationError(f'GROUP-BY expressions may not be aggregates: "{column}"')

                        # Attempt to reconcile the expression with one of the existing
                        # target expressions.
                        try:
                            index = c_target_expressions.index(c_expr)
                        except ValueError:
                            # Add the new target. 'None' for the target name implies it
                            # should be invisible, not to be rendered.
                            index = len(new_targets)
                            new_targets.append(EvalTarget(c_expr, None, aggregate))
                            c_target_expressions.append(c_expr)

                assert index is not None, "Internal error, could not index group-by reference."
                group_indexes.append(index)

                # Check that the group-by column references a non-aggregate.
                c_expr = new_targets[index].c_expr
                if is_aggregate(c_expr):
                    raise CompilationError(f'GROUP-BY expressions may not reference aggregates: "{column}"')

                # Check that the group-by column has a supported hashable type.
                if not issubclass(c_expr.dtype, collections.abc.Hashable):
                    raise CompilationError(f'GROUP-BY a non-hashable type is not supported: "{column}"')

            # Compile HAVING clause.
            if group_by.having is not None:
                c_expr = self._compile(group_by.having)
                if not is_aggregate(c_expr):
                    raise CompilationError('the HAVING clause must be an aggregate expression')
                having_index = len(new_targets)
                new_targets.append(EvalTarget(c_expr, None, True))
                c_target_expressions.append(c_expr)

        else:
            # If it does not have a GROUP-BY clause...
            aggregate_bools = [c_target.is_aggregate for c_target in c_targets]
            if any(aggregate_bools):
                # If the query is an aggregate query, check that all the targets are
                # aggregates.
                if all(aggregate_bools):
                    # FIXME: shold we really be checking for the empty
                    # list or is checking for a false value enough?
                    assert group_indexes == []
                elif SUPPORT_IMPLICIT_GROUPBY:
                    # If some of the targets aren't aggregates, automatically infer
                    # that they are to be implicit group by targets. This makes for
                    # a much more convenient syntax for our lightweight SQL, where
                    # grouping is optional.
                    group_indexes = [
                        index for index, c_target in enumerate(c_targets)
                        if not c_target.is_aggregate]
                else:
                    raise CompilationError('aggregate query without a GROUP-BY should have only aggregates')
            else:
                # This is not an aggregate query; don't set group_indexes to
                # anything useful, we won't need it.
                group_indexes = None

        return new_targets[len(c_targets):], group_indexes, having_index

    @_compile.register
    def _column(self, node: ast.Column):
        column = self.table.columns.get(node.name)
        if column is not None:
            return column
        raise CompilationError(f'column "{node.name}" does not exist', node)

    @_compile.register
    def _or(self, node: ast.Or):
        return EvalOr([self._compile(arg) for arg in node.args])

    @_compile.register
    def _and(self, node: ast.And):
        return EvalAnd([self._compile(arg) for arg in node.args])

    @_compile.register
    def _function(self, node: ast.Function):
        operands = [self._compile(operand) for operand in node.operands]
        if node.fname == 'coalesce':
            # coalesce() is parsed like a function call but it does
            # not really fit our model for function evaluation,
            # therefore it gets special threatment here.
            for operand in operands:
                if operand.dtype != operands[0].dtype:
                    dtypes = ', '.join(operand.dtype.__name__ for operand in operands)
                    raise CompilationError(f'coalesce() function arguments must have uniform type, found: {dtypes}', node)
            return EvalCoalesce(operands)
        function = types.function_lookup(FUNCTIONS, node.fname, operands)
        if function is None:
            sig = '{}({})'.format(node.fname, ', '.join(f'{operand.dtype.__name__.lower()}' for operand in operands))
            raise CompilationError(f'no function matches "{sig}" name and argument types', node)
        function = function(operands)
        # Constants folding.
        if all(isinstance(operand, EvalConstant) for operand in operands) and function.pure:
            return EvalConstant(function(None), function.dtype)
        return function

    @_compile.register
    def _subscript(self, node: ast.Subscript):
        operand = self._compile(node.operand)
        if issubclass(operand.dtype, dict):
            return EvalGetItem(operand, node.key)
        raise CompilationError('column type is not subscriptable', node)

    @_compile.register
    def _attribute(self, node: ast.Attribute):
        operand = self._compile(node.operand)
        dtype = types.ALIASES.get(operand.dtype, operand.dtype)
        if issubclass(dtype, types.Structure):
            getter = dtype.columns.get(node.name)
            if getter is None:
                raise CompilationError(f'structured type has no attribute "{node.name}"', node)
            return EvalGetter(operand, getter, getter.dtype)
        raise CompilationError('column type is not structured', node)

    @_compile.register
    def _unaryop(self, node: ast.UnaryOp):
        operand = self._compile(node.operand)
        function = types.function_lookup(OPERATORS, type(node), [operand])
        if function is None:
            raise CompilationError(
                f'operator "{type(node).__name__.lower()}({operand.dtype.__name__})" not supported', node)
        function = function(operand)
        # Constants folding.
        if isinstance(operand, EvalConstant):
            return EvalConstant(function(None), function.dtype)
        return function

    @_compile.register
    def _between(self, node: ast.Between):
        operand = self._compile(node.operand)
        lower = self._compile(node.lower)
        upper = self._compile(node.upper)
        intypes = [operand.dtype, lower.dtype, upper.dtype]
        for candidate in OPERATORS[type(node)]:
            if candidate.__intypes__ == intypes:
                func = candidate(operand, lower, upper)
                return func
        raise CompilationError(
            f'operator "{types.name(operand.dtype)} BETWEEN {types.name(lower.dtype)} '
            f'AND {types.name(upper.dtype)}" not supported', node)

    @_compile.register
    def _binaryop(self, node: ast.BinaryOp):
        left = self._compile(node.left)
        right = self._compile(node.right)

        candidates = OPERATORS[type(node)]
        while True:
            intypes = [left.dtype, right.dtype]
            for op in candidates:
                if op.__intypes__ == intypes:
                    function = op(left, right)
                    # Constants folding.
                    if isinstance(left, EvalConstant) and isinstance(right, EvalConstant):
                        return EvalConstant(function(None), function.dtype)
                    return function

            # Implement type inference when one of the operands is not strongly typed.
            if left.dtype is object and right.dtype is not object:
                target = right.dtype
                if target is int:
                    # The Beancount parser does not emit int typed
                    # values, thus casting to int is only going to
                    # loose information. Promote to decimal.
                    target = Decimal
                name = types.MAP.get(target)
                if name is None:
                    break
                left = types.function_lookup(FUNCTIONS, name, [left])([left])
                continue
            if right.dtype is object and left.dtype is not object:
                target = left.dtype
                if target is int:
                    # The Beancount parser does not emit int typed
                    # values, thus casting to int is only going to
                    # loose information. Promote to decimal.
                    target = Decimal
                name = types.MAP.get(target)
                if name is None:
                    break
                right = types.function_lookup(FUNCTIONS, name, [right])([right])
                continue

            # Failure.
            break

        raise CompilationError(
            f'operator "{type(node).__name__.lower()}('
            f'{left.dtype.__name__}, {right.dtype.__name__})" not supported', node)

    @_compile.register
    def _constant(self, node: ast.Constant):
        return EvalConstant(node.value)

    @_compile.register
    def _placeholder(self, node: ast.Placeholder):
        return EvalConstant(self.parameters[node.name])

    @_compile.register
    def _asterisk(self, node: ast.Asterisk):
        return EvalConstant(None, dtype=types.Asterisk)

    @_compile.register
    def _balances(self, node: ast.Balances):
        return self._compile(transform_balances(node))

    @_compile.register
    def _journal(self, node: ast.Journal):
        return self._compile(transform_journal(node))

    @_compile.register
    def _print(self, node: ast.Print):
        self.table = self.context.tables.get('entries')
        expr = self._compile_from(node.from_clause)
        return EvalPrint(self.table, expr)


def transform_journal(journal):
    """Translate a Journal entry into an uncompiled Select statement.

    Args:
      journal: An instance of a Journal object.
    Returns:
      An instance of an uncompiled Select object.
    """
    where = """WHERE account ~ '{}'""".format(journal.account) if journal.account else ''
    summary_func = journal.summary_func or ''

    cooked_select = parser.parse(f"""
        SELECT
           date,
           flag,
           MAXWIDTH(payee, 48),
           MAXWIDTH(narration, 80),
           account,
           {summary_func}(position),
           {summary_func}(balance)
        {where}
    """)

    return ast.Select(cooked_select.targets, journal.from_clause, cooked_select.where_clause, None, None, None, None, None)


def transform_balances(balances):
    """Translate a Balances entry into an uncompiled Select statement.

    Args:
      balances: An instance of a Balance object.
    Returns:
      An instance of an uncompiled Select object.
    """
    ## FIXME: Change the aggregation rules to allow GROUP-BY not to include the
    ## non-aggregate ORDER-BY columns, so we could just GROUP-BY accounts here
    ## instead of having to include the sort-key. I think it should be fine if
    ## the first or last sort-order value gets used, because it would simplify
    ## the input statement.

    cooked_select = parser.parse("""

      SELECT account, SUM({}(position))
      GROUP BY account, ACCOUNT_SORTKEY(account)
      ORDER BY ACCOUNT_SORTKEY(account)

    """.format(balances.summary_func or ""))

    return ast.Select(cooked_select.targets,
                      balances.from_clause,
                      balances.where_clause,
                      cooked_select.group_by,
                      cooked_select.order_by,
                      None, None, None)


def get_target_name(target):
    """Compute the target name.

    This uses the same algorithm used by SQLite. If the target has an
    AS clause assigning it a name, that will be the name used. If the
    target refers directly to a column, then the target name is the
    column name. Otherwise use the expression text.

    """
    if target.name is not None:
        return target.name
    if isinstance(target.expression, ast.Column):
        return target.expression.name
    return target.expression.text.strip()


def get_columns_and_aggregates(node):
    """Find the columns and aggregate nodes below this tree.

    All nodes under aggregate nodes are ignored.

    Args:
      node: An instance of EvalNode.
    Returns:
      A pair of (columns, aggregates), both of which are lists of EvalNode instances.
        columns: The list of all columns accessed not under an aggregate node.
        aggregates: The list of all aggregate nodes.
    """
    columns = []
    aggregates = []
    _get_columns_and_aggregates(node, columns, aggregates)
    return columns, aggregates


def _get_columns_and_aggregates(node, columns, aggregates):
    """Walk down a tree of nodes and fetch the column accessors and aggregates.

    This function ignores all nodes under aggregate nodes.

    Args:
      node: An instance of EvalNode.
      columns: An accumulator for columns found so far.
      aggregate: An accumulator for aggregate notes found so far.
    """
    if isinstance(node, EvalAggregator):
        aggregates.append(node)
    elif isinstance(node, EvalColumn):
        columns.append(node)
    else:
        for child in node.childnodes():
            _get_columns_and_aggregates(child, columns, aggregates)


def is_aggregate(node):
    """Return true if the node is an aggregate.

    Args:
      node: An instance of EvalNode.
    Returns:
      A boolean.
    """
    # Note: We could be a tiny bit more efficient here, but it doesn't matter
    # much. Performance of the query compilation matters very little overall.
    _, aggregates = get_columns_and_aggregates(node)
    return bool(aggregates)


def compile(context, statement, parameters=None):
    return Compiler(context).compile(statement, parameters)
