from beancount.core import display_context
from beancount.parser import printer


def render(desc, rows, file, *, dcontext, **kwargs):
    # Create a display context that renders all numbers with their
    # natural precision, but honors the commas option in the ledger.
    commas = dcontext.commas
    dcontext = display_context.DisplayContext()
    dcontext.set_commas(commas)
    return printer.print_entries([entry for entry, in rows], dcontext, file=file)
