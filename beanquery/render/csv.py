from ..query_render import render_csv


def render(desc, rows, file, *, dcontext, **kwargs):
    return render_csv(desc, rows, dcontext, file, **kwargs)
