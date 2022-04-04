Version 0.1 (unreleased)
------------------------

- The ``HAVING`` clause for aggregate queries is now supported.

- The ``empty()`` BQL function to determine whether an Inventory
  object as returned by the ``sum()`` aggregate function is empty has
  been added.

- Added the ``round()`` BQL function.

- ``NULL`` values in ``SORT BY`` clause are now always considered to
  be smaller than any other values.  This may results in rows to be
  returned in a slightly different order.

- It is now possible to specify the direction of the ordering for each
  column in the ``SORT BY`` clause.  This brings BQL closer to SQL
  specification but queries written with the old behaviour in mind
  will return rows in a different order.  The query::

    SELECT date, narration ORDER BY date, narration DESC

  used to return rows in descending order by both ``date`` and
  ``narration`` while now it would order the rows ascending by
  ``date`` and descending by ``narration``.  To recover the old
  behavior, the query should be written::

    SELECT date, narration ORDER BY date DESC, narration DESC

- Type casting functions ``int()``, ``decimal()``, ``str()``,
  ``date()`` have been added.  These are mostly useful to convert the
  generic ``object`` type returned by the metadata retrieval functions
  but can also be used to convert between types.  If the conversion
  fails, ``NULL`` is returned.

- The ``str()`` BQL function used to return a string representation of
  its argument using the Python :py:func:`repr()` function.  This
  clashes with the use of ``str()`` as a type casting function.  The
  function is renamed ``repr()``.

- The ``date()`` BQL function used to extract a date from string
  arguments with a very relaxed parser.  This clashes with the use of
  ``date()`` as a type casting function.  The function is renamed
  ``parse_date()``.  Another form of ``parse_date()`` that accepts the
  date format as second argument has been added.

- The ``getitem()`` BQL function return type has been changed from a
  string to a generic ``object`` to match the return type of function
  retrieving entries from metadata dictionaries.  The old behavior can
  be obtained with ``str(getitem(x, key))``.
