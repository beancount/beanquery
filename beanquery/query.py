"""A library to run queries. This glues together all the parts of the query engine.
"""
__copyright__ = "Copyright (C) 2015-2017  Martin Blais"
__license__ = "GNU GPLv2"

import beanquery
import beanquery.numberify


def run_query(entries, options, query, *args, numberify=False):
    """Compile and execute a query, return the result types and rows.

    Args:
      entries: A list of entries, as produced by the loader.
      options: A dict of options, as produced by the loader.
      query: A string, a single BQL query, optionally containing some new-style
        (e.g., {}) formatting specifications.
      args: A tuple of arguments to be formatted in the query. This is
        just provided as a convenience.
      numberify: If true, numberify the results before returning them.
    Returns:
      A pair of result types and result rows.
    Raises:
      ParseError: If the statement cannot be parsed.
      CompilationError: If the statement cannot be compiled.
    """

    # Execute the query.
    ctx = beanquery.connect('beancount:', entries=entries, errors=[], options=options)
    curs = ctx.execute(query.format(*args))
    rrows = curs.fetchall()
    rtypes = curs.description

    # Numberify the results, if requested.
    if numberify:
        dformat = options['dcontext'].build()
        rtypes, rrows = beanquery.numberify.numberify_results(rtypes, rrows, dformat)

    return rtypes, rrows
