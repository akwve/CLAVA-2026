from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # find a record with the provided primary key exist
        if primary_key not in self.table.page_directory:
            return False
        
        # delete page directory so the record is no longer accessible
        del self.table.page_directory[primary_key]
        return True
        #pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        
        # check if the number of columns aligns with the table
        if len(columns) != self.table.num_columns:
            return False
        
        # determine the key range of new record
        
        # extract key
        key = columns[self.table.key]

        # check if key already exists in teh page_directory
        if key in self.table.page_directory:
            return False
        
        # create RID
        rid = len(self.table.page_directory) + 1
        
        # create new record that will be inserted
        new_record = Record(rid, key, columns, schema_encoding)
        
        # check if page is full
        if not self.table.page_directory[key].has_capacity():
            # create new page
            new_page = Page().write() #???

        # insert the record into the page

        # update the base page metadata

        # update the page directory to reflect the new RID of tail records to base record
        
        # update index

        # if insertion is successful
        #return True

        #pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # check if primary key exists
        if primary_key not in self.table.page_directory:
            return False
        
        # check if the number of columns aligns with the table
        if len(columns) != self.table.num_columns:
            return False
        
        # get RID of the target record with key

        # locate the data using page directory

        # fetch the corrensponding base record and its indirection pointer

        # assign a new RID for the tail record

        # write updated value

        # set indirection pointer of new tail record to previous tail record

        # update the base record's indirection pointer to new tail record

        # update a schematic encoding for base and tail records
        base_schema_list = list(schema_encoding)
        tail_schema_list = ['0'] * self.table.num_columns
        for i, val in enumerate(columns):
            if val is not None:
                base_schema_list[i] = '1'
                tail_schema_list[i] = '1'
        base_schema_encoding = ''.join(base_schema_list)
        tail_schema_encoding = ''.join(tail_schema_list)

        # update tail page metadata

        #update the page directory to reflect the new RID of tail records

        # set indirection column as new tail RID
        
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
