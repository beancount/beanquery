"""
Exceptions hierarchy defined by the DB-API:

    Exception
      Warning
      Error
        InterfaceError
        DatabaseError
          DataError
          OperationalError
          IntegrityError
          InternalError
          ProgrammingError
          NotSupportedError
"""


class Warning(Exception):
    """Exception raised for important warnings."""

    __module__ = 'beanquery'


class Error(Exception):
    """Base exception for all errors."""

    __module__ = 'beanquery'


class InterfaceError(Error):
    """An error related to the database interface rather than the database itself."""

    __module__ = 'beanquery'


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""

    __module__ = 'beanquery'


class DataError(DatabaseError):
    """An error caused by problems with the processed data."""

    __module__ = 'beanquery'


class OperationalError(DatabaseError):
    """An error related to the database's operation."""

    __module__ = 'beanquery'


class IntegrityError(DatabaseError):
    """An error caused when the relational integrity of the database is affected."""

    __module__ = 'beanquery'


class InternalError(DatabaseError):
    """An error generated when the database encounters an internal error."""

    __module__ = 'beanquery'


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors."""

    __module__ = 'beanquery'


class NotSupportedError(DatabaseError):
    """A method or database API was used which is not supported by the database."""

    __module__ = 'beanquery'
