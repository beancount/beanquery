__copyright__ = "Copyright (C) 2014-2016  Martin Blais"
__license__ = "GNU GPLv2"

import atexit
import cmd
import contextlib
import io
import logging
import operator
import os
import re
import readline
import sys
import shlex
import textwrap
import traceback
from os import path

import click

from beancount.parser import printer
from beancount.core import data
from beancount.utils import misc_utils
from beancount.utils import pager
from beancount import loader

from beanquery import parser
from beanquery import query_compile
from beanquery import query_execute
from beanquery import query_render
from beanquery import numberify

# Load environment definitions.
from beanquery import query_env  # pylint: disable=unused-import


HISTORY_FILENAME = "~/.bean-shell-history"


# The same as contextlib.nullcontext in Python >= 3.7.
@contextlib.contextmanager
def nullcontext(result):
    yield result


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


# FIXME: It makes sense to move this into the exception classes
# themselves to make the error location reporting independent of the
# execution in the shell. The best way to do that would be to make all
# exceptions to have a common base class and to store location
# information uniformly. This requires translating TatSu exceptions
# into something else.
def render_exception(exc, indent='  ', strip=True):
    if isinstance(exc, query_compile.CompilationError) and exc.parseinfo:
        out = [f'error: {exc}', '']
        pos = exc.parseinfo.pos
        endpos = exc.parseinfo.endpos
        lineno = exc.parseinfo.line
        render_location(exc.parseinfo.tokenizer.text, pos, endpos, lineno, indent, strip, out)
        return '\n'.join(out)

    if isinstance(exc, parser.ParseError):
        out = ['error: syntax error', '']
        info = exc.tokenizer.line_info(exc.pos)
        render_location(exc.tokenizer.text, exc.pos, exc.pos + 1, info.line, indent, strip, out)
        return '\n'.join(out)

    return f'error: {exc}'


def convert_bool(string):
    """Convert a string to a boolean.

    Args:
      string: A string representing a boolean.
    Returns:
      The corresponding boolean.
    """
    return not string.lower() in ('f', 'false', '0')


class DispatchingShell(cmd.Cmd):
    """A usable convenient shell for interpreting commands, with history."""

    # Header for parsed commands.
    doc_header = "Shell utility commands (type help <topic>):"
    misc_header = "Beancount query commands:"

    def __init__(self, is_interactive, parser, outfile, default_format, do_numberify):
        """Create a shell with history.

        Args:
          is_interactive: A boolean, true if this serves an interactive tty.
          parser: A command parser.
          outfile: An output file object to write communications to.
          default_format: A string, the default output format.
        """
        super().__init__()
        if is_interactive:
            readline.parse_and_bind("tab: complete")
            # Readline is used to complete command names, which are
            # strictly alphanumeric strings, and named query
            # identifiers, which may contain any ascii characters. To
            # enable completion of the latter, reduce the set of
            # completion word delimiters to the shell default. Notably
            # remove "-" from the delimiters list setup by Python.
            readline.set_completer_delims(" \t\n\"\\'`@$><=;|&{(")
            history_filepath = path.expanduser(HISTORY_FILENAME)
            try:
                readline.read_history_file(history_filepath)
                readline.set_history_length(2048)
            except FileNotFoundError:
                pass
            atexit.register(readline.write_history_file, history_filepath)
        self.is_interactive = is_interactive
        self.parser = parser
        self.initialize_vars(default_format, do_numberify)
        self.add_help()
        self.outfile = outfile

    def initialize_vars(self, default_format, do_numberify):
        """Initialize the setting variables of the interactive shell."""
        self.vars_types = {
            'pager': str,
            'format': str,
            'boxed': convert_bool,
            'spaced': convert_bool,
            'expand': convert_bool,
            'numberify': convert_bool,
            }
        self.vars = {
            'pager': os.environ.get('PAGER', None),
            'format': default_format,
            'boxed': False,
            'spaced': False,
            'expand': False,
            'numberify': do_numberify,
            }

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
        if self.is_interactive:
            return pager.ConditionalPager(self.vars.get('pager', None),
                                          minlines=misc_utils.get_screen_height())
        return pager.flush_only(sys.stdout)

    def get_output(self):
        """Return where to direct command output.

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
                print('\n(interrupted)', file=self.outfile)

    def parseline(self, line):
        """Override command line parsing for case insensitive commands lookup."""
        cmd, arg, line = super().parseline(line)
        if cmd != 'EOF':
            cmd = cmd.lower()
        return cmd, arg, line

    def do_history(self, line):
        "Print the command-line history statement."
        num_entries = readline.get_current_history_length()
        try:
            max_entries = int(line)
            start = max(0, num_entries - max_entries)
        except ValueError:
            start = 0
        for index in range(start, num_entries):
            line = readline.get_history_item(index + 1)
            print(line, file=self.outfile)

    def do_clear(self, _):
        "Clear the history."
        readline.clear_history()

    def do_set(self, line):
        "Get/set shell settings variables."
        if not line:
            for varname, value in sorted(self.vars.items()):
                print(f'{varname}: {value}', file=self.outfile)
        else:
            components = shlex.split(line)
            varname = components[0]
            if len(components) == 1:
                try:
                    value = self.vars[varname]
                    print(f'{varname}: {value}', file=self.outfile)
                except KeyError:
                    print(f"Variable '{varname}' does not exist.", file=self.outfile)
            elif len(components) == 2:
                value = components[1]
                try:
                    converted_value = self.vars_types[varname](value)
                    self.vars[varname] = converted_value
                    print(f'{varname}: {converted_value}', file=self.outfile)
                except KeyError:
                    print(f"Variable '{varname}' does not exist.", file=self.outfile)
            else:
                print("Invalid number of arguments.", file=self.outfile)

    def do_parse(self, line):
        "Just run the parser on the following command and print the output."
        try:
            statement = self.parser.parse(line, True)
            print(statement, file=self.outfile)
        except parser.ParseError as exc:
            print(render_exception(exc), file=sys.stderr)
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)

    def dispatch(self, statement):
        """Dispatch the given statement to a suitable method.

        Args:
          statement: An instance provided by the parser.
        Returns:
          Whatever the invoked method happens to return.
        """
        name = type(statement).__name__
        method = getattr(self, f'on_{name}')
        return method(statement)

    def default(self, line):
        """Default handling of lines which aren't recognized as native shell commands.

        Args:
          line: The string to be parsed.
        """
        self.run_parser(line)

    def run_parser(self, line, default_close_date=None):
        """Handle statements via our parser instance and dispatch to appropriate methods.

        Args:
          line: The string to be parsed.
          default_close_date: A datetimed.date instance, the default close date.
        """
        try:
            statement = self.parser.parse(line, default_close_date=default_close_date)
            self.dispatch(statement)
        except Exception as exc:
            print(render_exception(exc), file=sys.stderr)

    def emptyline(self):
        """Do nothing on an empty line."""

    def exit(self, _):
        """Exit the parser."""
        print('exit', file=self.outfile)
        return 1

    # Commands to exit.
    do_exit = exit
    do_quit = exit
    do_EOF = exit


class BQLShell(DispatchingShell):
    """An interactive shell interpreter for the Beancount query language.
    """
    prompt = 'beancount> '

    def __init__(self, is_interactive, loadfun, outfile,
                 default_format='text', do_numberify=False):
        super().__init__(is_interactive, parser, outfile,
                         default_format, do_numberify)

        self.loadfun = loadfun
        self.entries = None
        self.errors = None
        self.options = None

    def do_reload(self, _line=None):
        "Reload the Beancount input file."
        self.entries, self.errors, self.options = self.loadfun()

        # Extract a mapping of the custom queries from the list of entries.
        self.named_queries = {}
        for entry in self.entries:
            if not isinstance(entry, data.Query):
                continue
            x = self.named_queries.setdefault(entry.name, entry)
            if x is not entry:
                logging.warning("Duplicate query name '%s'", entry.name)

        if self.is_interactive:
            print_statistics(self.entries, self.options, self.outfile)

    def do_errors(self, _line):
        "Print the errors that occurred during Beancount input file parsing."
        if self.errors:
            printer.print_errors(self.errors)
        else:
            print('(no errors)', file=self.outfile)

    def do_run(self, line):
        "Run a named query defined in the Beancount input file."

        line = line.rstrip('; \t')
        if not line:
            # List the available queries.
            print('\n'.join(name for name in sorted(self.named_queries)))
            return

        if line == "*":
            # Execute all.
            for name, query in sorted(self.named_queries.items()):
                print(f'{name}:')
                self.run_parser(query.query_string, default_close_date=query.date)
                print()
                print()
            return

        name, *args = shlex.split(line)
        if args:
            print("ERROR: Too many arguments for 'run' command.")
            return

        query = self.named_queries.get(name)
        if not query:
            print(f"ERROR: Query '{name}' not found.")
            return
        self.run_parser(query.query_string, default_close_date=query.date)

    def complete_run(self, text, _line, _begidx, _endidx):
        return [name for name in self.named_queries if name.startswith(text)]

    def do_explain(self, line):
        """Compile and print a compiled statement for debugging."""

        pr = lambda *args: print(*args, file=self.outfile)

        try:
            statement = self.parser.parse(line)
            pr("parsed statement:")
            pr(f"  {statement}")
            pr()

            query = query_compile.compile(statement)
            pr("compiled query:")
            pr(f"  {query}")
            pr()

            pr("Targets:")
            for c_target in query.c_targets:
                pr("  '{}'{}: {}".format(
                    c_target.name or '(invisible)',
                    ' (aggregate)' if query_compile.is_aggregate(c_target.c_expr) else '',
                    c_target.c_expr.dtype.__name__))
            pr()

        except (parser.ParseError, query_compile.CompilationError) as exc:
            print(render_exception(exc), file=sys.stderr)
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)


    def on_Print(self, print_stmt):
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
        c_print = query_compile.compile(print_stmt)
        with self.get_output() as out:
            query_execute.execute_print(c_print, self.entries, self.options, out)

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
        # Compile the SELECT statement.
        c_query = query_compile.compile(statement)

        # Execute it to obtain the result rows.
        rtypes, rrows = query_execute.execute_query(c_query, self.entries, self.options)

        # Output the resulting rows.
        if not rrows:
            print("(empty)", file=self.outfile)
        else:
            output_format = self.vars['format']
            if output_format == 'text':
                kwds = dict(boxed=self.vars['boxed'],
                            spaced=self.vars['spaced'],
                            expand=self.vars['expand'])
                with self.get_output() as out:
                    query_render.render_text(rtypes, rrows,
                                             self.options['dcontext'],
                                             out, **kwds)

            elif output_format == 'csv':
                # Numberify CSV output if requested.
                if self.vars['numberify']:
                    dformat = self.options['dcontext'].build()
                    rtypes, rrows = numberify.numberify_results(rtypes, rrows, dformat)

                query_render.render_csv(rtypes, rrows,
                                        self.options['dcontext'],
                                        self.outfile,
                                        expand=self.vars['expand'])

            else:
                assert output_format not in _SUPPORTED_FORMATS
                print(f"Unsupported output format: '{output_format}'.",
                      file=self.outfile)


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
        print(template.format(**generate_env_attribute_list(query_compile.ENVS['postings'])), file=self.outfile)

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
        print(template.format(**generate_env_attribute_list(query_compile.ENVS['entries'])), file=self.outfile)

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
        print(template.format(**generate_env_attribute_list(query_compile.ENVS['postings'])), file=self.outfile)

    def help_attributes(self):
        template = textwrap.dedent("""

          The attribute names on postings and directives equivalent to the names
          of columns that we make available for query.

          Entries
          -------

          {entry_attributes}

          Postings
          --------

          {posting_attributes}

        """)

        entry_pairs = sorted(
            (getattr(column_cls, '__equivalent__', '-'), name)
            for name, column_cls in sorted(query_compile.ENVS['entries'].columns.items()))

        posting_pairs = sorted(
            (getattr(column_cls, '__equivalent__', '-'), name)
            for name, column_cls in sorted(query_compile.ENVS['postings'].columns.items()))

        # pylint: disable=possibly-unused-variable
        entry_attributes = ''.join(
            "  {:40}: {}\n".format(*pair) for pair in entry_pairs)
        posting_attributes = ''.join(
            "  {:40}: {}\n".format(*pair) for pair in posting_pairs)
        print(template.format(**locals()), file=self.outfile)


def generate_env_attribute_list(env):
    """Generate a dictionary of rendered attribute lists for help.

    Args:
      env: An instance of an environment.
    Returns:
      A dict with keys 'columns', 'functions' and 'aggregates' to rendered
      and formatted strings.
    """
    wrapper = textwrap.TextWrapper(initial_indent='  ', subsequent_indent='  ', width=80)

    columns = generate_env_attributes(wrapper, env.columns)
    functions = generate_env_attributes(wrapper, env.functions, aggregates=False)
    aggregates = generate_env_attributes(wrapper, env.functions, aggregates=True)
    return dict(columns=columns, functions=functions, aggregates=aggregates)


def generate_env_attributes(wrapper, fields, aggregates=False):
    """Generate a string of all the help functions of the attributes.

    Args:
      wrapper: A TextWrapper instance to format the paragraphs.
      field_dict: A dict of the field-names to the node instances, fetch from an
        environment.
      filter_pred: A predicate to filter the desired columns. This is applied to
        the evaluator node instances.
    Returns:
      A formatted multiline string, ready for insertion in a help text.
    """
    entries = []
    for name, field in fields.items():
        if isinstance(field, list):
            # Entry in functions registry.
            if aggregates != issubclass(field[0], query_compile.EvalAggregator):
                continue
            name = name.upper()
            # FIXME: Render the __intypes__ here nicely instead of the key.
            def _format(f):
                # pylint: disable=cell-var-from-loop
                return '{}({})'.format(name, ', '.join(d.__name__.lower() for d in f.__intypes__))
            signature = '\n'.join(_format(func) for func in field)
            doc = field[0].__doc__ or ''
        else:
            signature = '{} [{}]'.format(name, field.dtype.__name__.lower())
            doc = field.__doc__ or ''
        entries.append((name, signature, wrapper.fill(re.sub(r'[ \n\t]+', ' ', doc))))

    oss = io.StringIO()
    for name, signature, text in sorted(entries, key=operator.itemgetter(0)):
        print(signature, file=oss)
        print(text, file=oss)
        print(file=oss)
    return oss.getvalue().rstrip()


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


_SUPPORTED_FORMATS = ('text', 'csv')


@click.command()
@click.argument('filename')
@click.argument('query', nargs=-1)
@click.option('--numberify', '-m', is_flag=True,
              help="Numberify the output, removing the currencies.")
@click.option('--format', '-f', 'output_format',
              type=click.Choice(_SUPPORTED_FORMATS),
              default=_SUPPORTED_FORMATS[0], help="Output format.")
@click.option('--output', '-o', type=click.File('w'), default='-',
              help="Output filename.")
@click.option('--no-errors', '-q', is_flag=True,
              help="Do not report errors.")
@click.version_option()
def main(filename, query, numberify, output_format, output, no_errors):
    """An interactive interpreter for the Beancount Query Language.

    Load Beancount ledger FILENAME and run Beancount Query Language
    QUERY on it, if specified, or drop into the interactive shell. If
    not explicitly set with the dedicated option, the output format is
    inferred from the output file name, if specified.

    """
    # Parse the input file.
    def load():
        errors_file = None if no_errors else sys.stderr
        with misc_utils.log_time('beancount.loader (total)', logging.info):
            return loader.load_file(filename,
                                    log_timings=logging.info,
                                    log_errors=errors_file)

    # Create the shell.
    is_interactive = sys.stdin.isatty() and not query
    shell_obj = BQLShell(is_interactive, load, output, output_format, numberify)
    shell_obj.do_reload()

    # Run interactively if we're a TTY and no query is supplied.
    if is_interactive:
        try:
            shell_obj.cmdloop()
        except KeyboardInterrupt:
            print('\nExit')
    else:
        # Run in batch mode (Non-interactive).
        if query:
            # We have a query to run.
            query = ' '.join(query)
        else:
            # If we have no query and we're not a TTY, read the BQL command from
            # standard input.
            query = sys.stdin.read()

        shell_obj.onecmd(query)
