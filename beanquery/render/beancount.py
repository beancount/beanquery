from beancount.parser import printer


def render(desc, rows, file, *, dcontext, **kwargs):
    return printer.print_entries([entry for entry, in rows], dcontext, file=file)
