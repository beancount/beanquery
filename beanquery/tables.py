class Table:
    columns = {}
    name = ''

    def __getitem__(self, name):
        return self.columns[name]

    @property
    def wildcard_columns(self):
        # For backward compatibility. Remove once the postings table
        # is updated to return all columns upon ``SELECT *`` and the
        # query compiler is updated not to rely on this property
        return self.columns.keys()


class NullTable(Table):
    def __iter__(self):
        return iter([None])
