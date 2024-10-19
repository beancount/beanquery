project = 'beanquery'
copyright = '2014-2022, beanquery Contributors'
author = 'beanquery Contributors'
version = '0.1'
language = 'en'
html_theme = 'furo'
html_title = f'{project} {version}'
html_logo = 'logo.svg'
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    'sphinx.ext.githubpages',
]
extlinks = {
    'issue': ('https://github.com/beancount/beanquery/issues/%s', '#'),
    'pull': ('https://github.com/beancount/beanquery/pull/%s', '#'),
}
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'beancount': ('https://beancount.github.io/docs/', None),
}
napoleon_google_docstring = True
napoleon_use_param = False
autodoc_typehints = 'none'
autodoc_member_order = 'bysource'
