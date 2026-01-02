
.. toctree::
    :hidden:

    Main Page<self>
    columns_entry
    columns_postings
    functions_index
    bql_functions

Beanquery: Customizable lightweight SQL query tool
==================================================

beanquery is a customizable and extensible lightweight SQL query tool
that works on tabular data, including `Beancount`__ ledger data.

__ https://beancount.github.io/

With this tool you can write *queries* to extract information from your
Beancount ledger. This is the documentation of functions and field names
which are available for queries. 

Please read the Manual of the `Beancount Query Language (BQL)`__ if you
do not yet know how to write queries.


__ http://furius.ca/beancount/doc/query

Targets
-------

In a BQL query, you can reference various data fields as targets. Here
you find the list of targets:

* ...for the ``SELECT ...`` and ``FROM ...`` clauses: :doc:`columns_entry`.
* ...for the ``WHERE ...`` clause: :doc:`columns_postings`

Functions in the Beancount query language
-----------------------------------------

If you are looking for a particular function, best start at the list of :doc:`functions_index`

You can also see the alphabetical list of :doc:`bql_functions`
