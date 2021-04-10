__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from os import path


def find_repository_root(filename=None):
    """Return the path to the repository root.
    Args:
      filename: A string, the name of a file within the repository.
    Returns:
      A string, the root directory.
    """
    while not all(path.exists(path.join(filename, sigfile))
                  for sigfile in ('LICENSE', 'README.md')):
        prev_filename = filename
        filename = path.dirname(filename)
        if prev_filename == filename:
            raise ValueError("Failed to find the root directory.")
    return filename
