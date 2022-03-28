Version 0.1 (unreleased)
------------------------

- Implemented the ``HAVING`` clause for aggregate queries.

- Added the ``empty()`` BQL function to determine whether an Inventory
  object as returned by the ``sum()`` aggregate function is empty.

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
