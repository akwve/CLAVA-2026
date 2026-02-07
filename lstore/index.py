"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        index = self.indices[column]

        # if there is no column
        if index is None:
            return []

        return index.get(value, [])

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        index = self.indices[column]
        
        # if there is no column
        if self.indices[column] is None:
            return []

        return index.range(begin, end)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # as long as the index doesn't exist already
        if self.indices[column_number] is None:
            self.indices[column_number] = {}

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        # make sure column exists
        if self.indices[column_number] is not None:
            self.indices[column_number] = None
