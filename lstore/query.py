from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from time import time


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
        
        # determine the primary key from list of columns
        key = columns[self.table.key]

        # check if key already exists in teh page_directory
        if key in self.table.page_directory:
            return False
        
        # create RID
        rid = self.table.get_unique_rid()
        
        # create new record that will be inserted (metadata + given columns)
        # all column must be integer
        new_record = [
            None,  # indirection
            rid, # rid
            int(time()), # timestamp
            int(schema_encoding, 2), # schema encoding
            *columns, # given columns
        ]

        # write record into base page for each column
        # iterate for each columns
        for i, record in enumerate(new_record):
            # check if there is base page for the column
            if self.table.base_page_range[i] == 0:
                self.table.base_pages[i].append(Page())
                self.table.base_page_range[i] = 1

            # get current latest base page
            current_page = self.table.base_pages[i][self.table.base_page_range[i] - 1]
            
            # check if page still have capacity, if not create a new page
            if not current_page.has_capacity():
                self.table.base_pages[i].append(Page())
                self.table.base_page_range[i] += 1
                # set new page as current base page
                current_page = self.table.base_pages[i][self.table.base_page_range[i] - 1]

            # update num_records in page and get where the record is stored in the page
            offset = current_page.write(record)
            

        # update the page directory to reflect the new RID and location
        # get the index of the key column in the table: metadata + index of key column among given columns
        key_col_index = 4 + self.table.key
        # get the page of key column to be stored in page directory
        page_index = self.table.base_page_range[key_col_index] - 1
        self.table.page_directory[key] = (rid, page_index, offset)
        
        # update index if applicable
        
        
        # if insertion is successful
        return True


    
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
        base_rid, page_index, offset = self.table.page_directory[primary_key]

        # fetch the corresponding base record and its indirection pointer
        base_indirection_pointer = self.table.base_pages[0][page_index] # indirection column = 0
        latest_tail_rid = base_indirection_pointer.read(offset)

        # assign a new RID for the tail record
        new_tail_rid = self.table.get_unique_rid()

        # update a schematic encoding for tail records
        tail_schema_list = ['0'] * self.table.num_columns
        for i, val in enumerate(columns):
            if val is not None:
                tail_schema_list[i] = '1'
        tail_schema_encoding = ''.join(tail_schema_list)

        # create new tail record for updated value
        new_tail_record = [
            latest_tail_rid,  # indirection: store previous tail RID
            new_tail_rid,  # rid
            int(time()),  # timestamp
            int(tail_schema_encoding, 2),  # schema encoding
            *columns,  # given columns: update column with value and non-updated column with None
        ]
        
        # write record into tail pages for each column
        # iterate for each columns
        for i, record in enumerate(new_tail_record):
            # skip None value column
            if record is None:
                continue  
            
            # check if there is tail page for column
            if self.table.tail_page_range[i] == 0:
                self.table.tail_pages[i].append(Page())
                self.table.tail_page_range[i] = 1

            # get current tail page
            current_tail_page = self.table.tail_pages[i][self.table.tail_page_range[i] - 1]

            # check if page still have capacity, if not create a new page
            if not current_tail_page.has_capacity():
                self.table.tail_pages[i].append(Page())
                self.table.tail_page_range[i] += 1
                # set new page as current tail page
                current_tail_page = self.table.tail_pages[i][self.table.tail_page_range[i] - 1]

            # make sure write for RID column so offset have value
            if i == 2:
                offset = current_tail_page.write(record)

        # update the base record's indirection pointer to new tail record
        base_indirection_pointer.data[offset * 8:offset * 8 + 8] = new_tail_rid.to_bytes(8, byteorder='little')

        return True


    
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
