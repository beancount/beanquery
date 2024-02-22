__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import atexit
import cmd
import importlib
import io
import itertools
import os
import pkgutil
import re
import shlex
import sys
import textwrap
import traceback
import warnings

from contextlib import nullcontext, suppress
from dataclasses import dataclass, asdict
from os import path

import click
import beancount

from beancount.parser import printer
from beancount.core import data
from beancount.utils import pager
from beancount.utils.misc_utils import get_screen_height

import beanquery

from beanquery import parser
from beanquery import query_compile
from beanquery import render
from beanquery import types
from beanquery.numberify import numberify_results
from beanquery.query_execute import execute_print

try:
    import readline
except ImportError:
    readline = None


HISTORY_FILENAME = '~/.config/beanquery/history'
INIT_FILENAME = '~/.config/beanquery/init'


class style:
    ERROR = '\033[31;1m'
    WARNING = '\033[31;1m'
    RESET = '\033[0m'

    ESCAPES = re.compile(r'\033\[[;?0-9]*[a-zA-Z]')

    @classmethod
    def strip(cls, x):
        return cls.ESCAPES.sub('', x)


def render_location(text, pos, endpos, lineno, indent, strip, out):
    length = endpos - pos
    lines = text.splitlines(True)
    for line in lines[:lineno]:
        pos -= len(line)
        if strip and not line.rstrip():
            continue
        strip = False
        out.append(indent + line.rstrip().expandtabs())
    out.append(indent + lines[lineno].rstrip().expandtabs())
    out.append(indent + ' ' * pos + '^' * length)


# FIXME: move the error formatting into the exception classes themselves
def render_exception(exc, indent='| ', strip=True):
    if isinstance(exc, (beanquery.CompilationError, beanquery.ParseError)) and exc.parseinfo:
        out = [str(exc)]
        pos = exc.parseinfo.pos
        endpos = exc.parseinfo.endpos
        lineno = exc.parseinfo.line
        render_location(exc.parseinfo.tokenizer.text, pos, endpos, lineno, indent, strip, out)
        return '\n'.join(out)
    return '\n' + traceback.format_exc()


FORMATS = {
    name: importlib.import_module('beanquery.render.' + name).render
    for finder, name, ispkg
    in pkgutil.iter_modules(render.__path__)
}


@dataclass
class Settings:
    boxed: bool = False
    expand: bool = False
    format: str = 'text'
    narrow: bool = True
    nullvalue: str = ''
    numberify: bool = False
    pager: str = ''
    spaced: bool = False
    unicode: bool = False

    def _parse_bool(self, value):
        if value in {True, False}:
            return value
        norm = value.strip().lower()
        if norm in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if norm in {"0", "false", "f", "no", "n", "off"}:
            return False
        raise ValueError(f'"{value}" is not a valid boolean')

    def _parse_format(self, value):
        if value not in FORMATS:
            raise ValueError(f'"{value}" is not a valid format')
        return value

    def getstr(self, name):
        value = getattr(self, name)
        if isinstance(value, str):
            return repr(value)
        if isinstance(value, bool):
            return 'true' if value else 'false'
        return str(value)

    def setstr(self, name, value):
        vtype = type(getattr(self, name))
        parse = getattr(self, f'_parse_{name}', getattr(self, f'_parse_{vtype.__name__}', vtype))
        setattr(self, name, parse(value))

    def todict(self):
        return asdict(self)

    def __iter__(self):
        return iter(self.todict().keys())


class DispatchingShell(cmd.Cmd):
    """A usable convenient shell for interpreting commands, with history."""

    # Header for parsed commands.
    doc_header = "Shell utility commands (type help <topic>):"
    misc_header = "Beancount query commands:"

    def __init__(self, outfile, interactive, runinit, settings):
        """Create a shell with history.

        Args:
          outfile: An output file object to write communications to.
          interactive: A boolean, true if this serves an interactive tty.
          runinit: When true, execute the commands from the user init file.
          settings: The shell settings.
        """
        super().__init__()
        self.identchars += '.'
        self.outfile = outfile
        self.interactive = interactive
        self.settings = settings
        self.color = interactive and os.environ.get('TERM', 'dumb') != 'dumb'
        self.add_help()

        if interactive and readline is not None:
            readline.parse_and_bind("tab: complete")
            # Readline is used to complete command names, which are
            # strictly alphanumeric strings, and named query
            # identifiers, which may contain any ascii characters. To
            # enable completion of the latter, reduce the set of
            # completion word delimiters to the shell default. Notably
            # remove "-" from the delimiters list setup by Python.a
            readline.set_completer_delims(" \t\n\"\\'`@$><=;|&{(")
            history_filepath = path.expanduser(HISTORY_FILENAME)
            os.makedirs(path.dirname(history_filepath), exist_ok=True)
            with suppress(FileNotFoundError):
                readline.read_history_file(history_filepath)
                readline.set_history_length(2048)
            atexit.register(readline.write_history_file, history_filepath)

        warnings.showwarning = self.warning

        if runinit:
            with suppress(FileNotFoundError):
                with open(path.expanduser(INIT_FILENAME)) as f:
                    for line in f:
                        self.onecmd(line)

    def echo(self, message, file=sys.stdout):
        if not self.color:
            message = style.strip(message)
        print(message, file=file)

    def error(self, message):
        self.echo(f'{style.ERROR}error:{style.RESET} {message}', file=sys.stderr)

    def warning(self, message, *args):
        self.echo(f'{style.WARNING}warning:{style.RESET} {message}', file=sys.stderr)

    def add_help(self):
        "Attach help functions for each of the parsed token handlers."
        for attrname, func in list(self.__class__.__dict__.items()):
            match = re.match('on_(.*)', attrname)
            if not match:
                continue
            command_name = match.group(1)
            setattr(self.__class__, f'help_{command_name.lower()}',
                    lambda _, fun=func: print(textwrap.dedent(fun.__doc__).strip(),
                                              file=self.outfile))

    def get_pager(self):
        """Create and return a context manager to write to, a pager subprocess if required.

        Returns:
          A context manager.

        """
        if self.interactive:
            return pager.ConditionalPager(self.settings.pager, minlines=get_screen_height())
        return pager.flush_only(sys.stdout)

    @property
    def output(self):
        """Where to direct command output.

        When the output stream is connected to the standard output,
        and we are running interactively, use an indirection that can
        send the output to a pager when the number of lines emitted is
        greater than a threshold.

        Returns:
          A context manager that returns a file descriptor on enter.

        """
        if self.outfile is sys.stdout:
            return self.get_pager()
        return nullcontext(self.outfile)

    def cmdloop(self, intro=None):
        """Override cmdloop to handle keyboard interrupts."""
        while True:
            try:
                super().cmdloop(intro)
                break
            except KeyboardInterrupt:
                print('\n(interrupted)', file=self.stderr)
            except Exception as exc:
                self.error(render_exception(exc))

    def parseline(self, line):
        cmd, arg, line = super().parseline(line)
        if not cmd:
            return cmd, arg, line
        if cmd.startswith('.'):
            cmd = cmd[1:]
        if cmd == 'EOF':
            line = '.EOF'
        return cmd, arg, line

    def onecmd(self, line):
        cmd, arg, line = self.parseline(line)
        if not cmd:
            return
        if not line.startswith('.'):
            cmd = cmd.lower()
            if cmd not in {'clear', 'errors', 'exit', 'help', 'history', 'parse', 'quit', 'run', 'set'}:
                return self.execute(line)
            warnings.warn(f'commands without "." prefix are deprecated. use ".{cmd}" instead', stacklevel=0)
        func = getattr(self, 'do_' + cmd, None)
        if func is not None:
            return func(arg)
        self.error(f'unknown command "{cmd}"')

    def completenames(self, text, *ignored):
        if text.startswith('.'):
            dotext = 'do_' + text[1:]
            return ['.' + a[3:] for a in self.get_names() if a.startswith(dotext)]

    def do_help(self, arg):
        """List available commands with "help" or detailed help with "help cmd"."""
        super().do_help(arg.lower())

    def do_history(self, arg):
        """Print the command-line history."""
        if readline is not None:
            num_entries = readline.get_current_history_length()
            try:
                max_entries = int(arg)
                start = max(0, num_entries - max_entries)
            except ValueError:
                start = 0
            for index in range(start, num_entries):
                line = readline.get_history_item(index + 1)
                print(line, file=self.outfile)

    def do_clear(self, arg):
        """Clear the command-line history."""
        if readline is not None:
            readline.clear_history()

    def do_set(self, arg):
        """Set shell settings variables."""
        if not arg:
            for name in self.settings:
                value = self.settings.getstr(name)
                print(f'{name}: {value}', file=self.outfile)
        else:
            components = shlex.split(arg)
            name = components[0]
            if len(components) == 1:
                try:
                    value = self.settings.getstr(name)
                    print(f'{name}: {value}', file=self.outfile)
                except AttributeError:
                    self.error(f'variable "{name}" does not exist')
            elif len(components) == 2:
                value = components[1]
                try:
                    self.settings.setstr(name, value)
                except ValueError as ex:
                    self.error(str(ex))
                except AttributeError:
                    self.error(f'variable "{name}" does not exist')
            else:
                self.error('invalid number of arguments')

    def complete_set(self, text, _line, _begidx, _endidx):
        return [name for name in self.settings if name.startswith(text)]

    def do_parse(self, arg):
        """Run the parser on the following command and print the output."""
        print(self.parse(arg).tosexp())

    def parse(self, query, **kwargs):
        raise NotImplementedError

    def execute(self, query, **kwargs):
        """Handle statements via our parser instance and dispatch to appropriate methods.

        Args:
          query: The string to be parsed.
        """
        statement = self.parse(query, **kwargs)
        name = type(statement).__name__
        method = getattr(self, f'on_{name}')
        return method(statement)

    def do_exit(self, arg):
        """Exit the command interpreter."""
        return True

    do_quit = do_exit

    def do_EOF(self, arg):
        """Exit the command interpreter."""
        print('exit', file=self.outfile)
        return self.do_exit(arg)


class BQLShell(DispatchingShell):
    """An interactive shell interpreter for the Beancount query language."""
    prompt = 'beanquery> '

    def __init__(self, filename, outfile, interactive=False, runinit=False, format='text', numberify=False):
        settings = Settings(format=format, numberify=numberify)
        super().__init__(outfile, interactive, runinit, settings)
        self.context = beanquery.connect(None)
        self.filename = filename
        self.queries = {}
        self.do_reload()

    def parse(self, line, default_close_date=None, **kwargs):
        statement = self.context.parse(line)
        if (isinstance(statement, parser.ast.Select) and
            isinstance(statement.from_clause, parser.ast.From) and
            not statement.from_clause.close):
            statement.from_clause.close = default_close_date
        return statement

    def do_reload(self, arg=None):
        "Reload the Beancount input file."
        if not self.filename:
            return
        self.context.errors.clear()
        self.context.options.clear()
        self.context.attach('beancount:' + self.filename)
        table = self.context.tables['entries']
        self._extract_queries(table.entries)
        if self.context.errors:
            printer.print_errors(self.context.errors, file=sys.stderr)
        if self.interactive:
            print_statistics(table.entries, table.options, self.outfile)

    def _extract_queries(self, entries):
        self.queries = {}
        for entry in entries:
            if isinstance(entry, data.Query):
                x = self.queries.setdefault(entry.name, entry)
                if x is not entry:
                    warnings.warn(f'duplicate query name "{entry.name}"', stacklevel=0)

    def do_errors(self, arg=None):
        "Print the errors that occurred during Beancount input file parsing."
        if self.context.errors:
            printer.print_errors(self.context.errors)
        else:
            print('(no errors)', file=self.outfile)

    def do_run(self, arg):
        "Run a named query defined in the Beancount input file."

        arg = arg.rstrip('; \t')
        if not arg:
            # List the available queries.
            if self.queries:
                print('\n'.join(name for name in sorted(self.queries)))
            return

        if arg == "*":
            # Execute all.
            for name, query in sorted(self.queries.items()):
                print(f'{name}:')
                self.execute(query.query_string, default_close_date=query.date)
                print()
                print()
            return

        name, *args = shlex.split(arg)
        if args:
            self.error('too many arguments for "run" command')
            return

        query = self.queries.get(name)
        if not query:
            self.error(f'query "{name}" not found')
            return
        self.execute(query.query_string, default_close_date=query.date)

    def complete_run(self, text, line, begidx, endidx):
        return [name for name in self.queries if name.startswith(text)]

    def do_tables(self, arg):
        """List tables."""
        print('\n'.join(name for name in sorted(self.context.tables.keys()) if name), file=self.outfile)

    def do_describe(self, arg):
        """Describe table or structured type."""
        def describe(obj):
            return '\n'.join(f'  {name} ({types.name(column.dtype)})' for name, column in obj.columns.items())
        names = shlex.split(arg)
        for name in names:
            table = self.context.tables.get(name)
            if table:
                print(f'table {name}:', file=self.outfile)
                print(describe(table), file=self.outfile)
            datatype = types.TYPES.get(name)
            if datatype:
                print(f'structured type {name}:', file=self.outfile)
                print(describe(datatype), file=self.outfile)

    def complete_describe(self, text, line, begidx, endidx):
        names = itertools.chain(self.context.tables.keys(), types.TYPES.keys())
        return [name for name in names if name and name.startswith(text)]

    def do_explain(self, arg):
        """Compile and print a compiled statement for debugging."""

        p = lambda x: print(x, file=self.outfile)

        statement = self.context.parse(arg)
        p('parsed statement')
        p('----------------')
        p(textwrap.indent(statement.tosexp(), '  '))
        p('')

        query = self.context.compile(statement)
        p('compiled query')
        p('--------------')
        p(f'  {query}')
        p('')

        p('targets')
        p('-------')
        for target in query.c_targets:
            name = target.name or ''
            datatype = types.name(target.c_expr.dtype)
            if target.is_aggregate:
                datatype += ', aggregate'
            p(f'  {name}: {datatype}')

    def on_Print(self, statement):
        """
        Print entries in Beancount format.

        The general form of a PRINT statement includes an SQL-like FROM
        selector:

           PRINT [FROM <from_expr> ...]

        Where:

          from_expr: A logical expression that matches on the attributes of
            the directives. See SELECT command for details (this FROM expression
            supports all the same expressions including its OPEN, CLOSE and
            CLEAR operations).

        """
        # Compile the print statement.
        query = self.context.compile(statement)
        with self.output as out:
            execute_print(query, out)

    def on_Select(self, statement):
        """
        Extract data from a query on the postings.

        The general form of a SELECT statement loosely follows SQL syntax, with
        some mild and idiomatic extensions:

           SELECT [DISTINCT] [<targets>|*]
           [FROM <from_expr> [OPEN ON <date>] [CLOSE [ON <date>]] [CLEAR]]
           [WHERE <where_expr>]
           [GROUP BY <groups>]
           [ORDER BY <groups> [ASC|DESC]]
           [LIMIT num]

        Where:

          targets: A list of desired output attributes from the postings, and
            expressions on them. Some of the attributes of the parent transaction
            directive are made available in this context as well. Simple functions
            (that return a single value per row) and aggregation functions (that
            return a single value per group) are available. For the complete
            list of supported columns and functions, see help on "targets".
            You can also provide a wildcard here, which will select a reasonable
            default set of columns for rendering a journal.

          from_expr: A logical expression that matches on the attributes of
            the directives (not postings). This allows you to select a subset of
            transactions, so the accounting equation is respected for balance
            reports. For the complete list of supported columns and functions,
            see help on "from".

          where_expr: A logical expression that matches on the attributes of
            postings. The available columns are similar to those in the targets
            clause, without the aggregation functions.

          OPEN clause: replace all the transactions before the given date by
            summarizing entries and transfer Income and Expenses balances to
            Equity.

          CLOSE clause: Remove all the transactions after the given date and

          CLEAR: Transfer final Income and Expenses balances to Equity.

        """
        cursor = self.context.execute(statement)
        desc = cursor.description
        rows = cursor.fetchall()
        dcontext = self.context.options['dcontext']

        if self.settings.numberify:
            desc, rows = numberify_results(desc, rows, dcontext.build())

        with self.output as out:
            render = FORMATS.get(self.settings.format)
            if render is not None:
                return render(desc, rows, out, dcontext=dcontext, **self.settings.todict())
            raise NotImplementedError

    def on_Journal(self, journal):
        """
        Select a journal of some subset of postings. This command is a
        convenience and converts into an equivalent Select statement, designed
        to extract the most sensible list of columns for the register of a list
        of entries as a table.

        The general form of a JOURNAL statement loosely follows SQL syntax:

           JOURNAL <account-regexp> [FROM_CLAUSE]

        See the SELECT query help for more details on the FROM clause.
        """
        return self.on_Select(journal)

    def on_Balances(self, balance):
        """
        Select balances of some subset of postings. This command is a
        convenience and converts into an equivalent Select statement, designed
        to extract the most sensible list of columns for the register of a list
        of entries as a table.

        The general form of a JOURNAL statement loosely follows SQL syntax:

           BALANCE [FROM_CLAUSE]

        See the SELECT query help for more details on the FROM clause.
        """
        return self.on_Select(balance)

    def help_targets(self):
        template = textwrap.dedent("""

          The list of comma-separated target expressions may consist of columns,
          simple functions and aggregate functions. If you use any aggregate
          function, you must also provide a GROUP-BY clause.

          Columns
          -------

          {columns}

          Functions
          ---------

          {functions}

          Aggregate functions
          -------------------

          {aggregates}

        """)
        print(template.format(**_describe(self.context.tables['postings'],
                                          query_compile.FUNCTIONS)), file=self.outfile)

    def help_from(self):
        template = textwrap.dedent("""

          A logical expression that consist of columns on directives (mostly
          transactions) and simple functions.

          Columns
          -------

          {columns}

          Functions
          ---------

          {functions}

        """)
        print(template.format(**_describe(self.context.tables['entries'],
                                          query_compile.FUNCTIONS)),
              file=self.outfile)

    def help_where(self):
        template = textwrap.dedent("""

          A logical expression that consist of columns on postings and simple
          functions.

          Columns
          -------

          {columns}

          Functions
          ---------

          {functions}

        """)
        print(template.format(**_describe(self.context.tables['postings'],
                                          query_compile.FUNCTIONS)), file=self.outfile)


def _describe_columns(columns):
    out = io.StringIO()
    wrapper = textwrap.TextWrapper(initial_indent='  ', subsequent_indent='  ', width=80)
    for name, column in columns.items():
        print(f'{name}: {column.dtype.__name__.lower()}', file=out)
        print(wrapper.fill(re.sub(r'[ \n\t]+', ' ', column.__doc__ or '')), file=out)
        print(file=out)
    return out.getvalue().rstrip()


def _describe_functions(functions, aggregates=False):
    entries = []
    for name, funcs in functions.items():
        if aggregates != issubclass(funcs[0], query_compile.EvalAggregator):
            continue
        name = name.lower()
        for func in funcs:
            args = ', '.join(d.__name__.lower() for d in func.__intypes__)
            doc = re.sub(r'[ \n\t]+', ' ', func.__doc__ or '')
            entries.append((name, doc, args))
    entries.sort()
    out = io.StringIO()
    wrapper = textwrap.TextWrapper(initial_indent='  ', subsequent_indent='  ', width=80)
    for key, entries in itertools.groupby(entries, key=lambda x: x[:2]):  # noqa: B020
        for name, doc, args in entries:
            print(f'{name}({args})', file=out)
        print(wrapper.fill(doc), file=out)
        print(file=out)
    return out.getvalue().rstrip()


def _describe(table, functions):
    return dict(
        columns=_describe_columns(table.columns),
        functions=_describe_functions(functions, aggregates=False),
        aggregates=_describe_functions(functions, aggregates=True))


def summary_statistics(entries):
    """Calculate basic summary statistics to output a brief welcome message.

    Args:
      entries: A list of directives.
    Returns:
      A tuple of three integers, the total number of directives parsed, the total number
      of transactions and the total number of postings there in.
    """
    num_directives = len(entries)
    num_transactions = 0
    num_postings = 0
    for entry in entries:
        if isinstance(entry, data.Transaction):
            num_transactions += 1
            num_postings += len(entry.postings)
    return (num_directives, num_transactions, num_postings)


def print_statistics(entries, options, outfile):
    """Print summary statistics to stdout.

    Args:
      entries: A list of directives.
      options: An options map. as produced by the parser.
      outfile: A file object to write to.
    """
    num_directives, num_transactions, num_postings = summary_statistics(entries)
    if 'title' in options:
        print(f'''Input file: "{options['title']}"''', file=outfile)
    print(f"Ready with {num_directives} directives",
          f"({num_postings} postings in {num_transactions} transactions).",
          file=outfile)


@click.command()
@click.argument('filename')
@click.argument('query', nargs=-1)
@click.option('--numberify', '-m', is_flag=True,
              help="Numberify the output, removing the currencies.")
@click.option('--format', '-f', type=click.Choice(FORMATS.keys()), default='text',
              help="Output format.")
@click.option('--output', '-o', type=click.File('w'), default='-',
              help="Output filename.")
@click.option('--no-errors', '-q', is_flag=True,
              help="Do not report errors.")
@click.version_option('', message=f'beanquery {beanquery.__version__}, beancount {beancount.__version__}')
def main(filename, query, numberify, format, output, no_errors):
    """An interactive interpreter for the Beancount Query Language.

    Load Beancount ledger FILENAME and run Beancount Query Language
    QUERY on it, if specified, or drop into the interactive shell. If
    not explicitly set with the dedicated option, the output format is
    inferred from the output file name, if specified.

    """
    # Create the shell.
    interactive = sys.stdin.isatty() and not query
    shell = BQLShell(filename, output, interactive, True, format, numberify)

    # Run interactively if we're a TTY and no query is supplied.
    if interactive:
        warnings.filterwarnings('always')
        shell.cmdloop()
    else:
        # Run in batch mode (Non-interactive).
        if query:
            # We have a query to run.
            query = ' '.join(query)
        else:
            # If we have no query and we're not a TTY, read the BQL command from
            # standard input.
            query = sys.stdin.read()

        shell.onecmd(query)
