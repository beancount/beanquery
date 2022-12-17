from urllib.parse import urlparse
from beancount import loader
from beanquery import query_env


def add_beancount_tables(context, entries, errors, options):
    for table in query_env.EntriesTable, query_env.PostingsTable:
        context.tables[table.name] = table(entries, options)
    context.options.update(options)
    context.errors.extend(errors)


def attach(context, uri):
    filename = urlparse(uri).path
    entries, errors, options = loader.load_file(filename)
    add_beancount_tables(context, entries, errors, options)
