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
        base_rid = self.table.get_unique_rid(base=True)
        
        # create new record that will be inserted (metadata + given columns)
        # all column must be integer except meta data column
        new_record = [
            0,  # indirection: 0 for no tail record
            base_rid, # rid
            int(time()*1000), # timestamp
            int(schema_encoding, 2), # schema encoding
            *columns, # given columns
        ]
        
        # store list for page directory
        mapping_list = []
        # write record into base page for each column
        # iterate for each columns
        for i, record in enumerate(new_record):
            # get current indexes for this column
            partition_index = len(self.table.base_pages[i]) - 1
            current_page_range = self.table.base_pages[i][partition_index]

            # check if current page range is empty, add first page
            if len(current_page_range) == 0:
                current_page_range.append(Page())
            
            # get latest page 
            current_page = current_page_range[-1]
            
            # check if page still have capacity, if not create a new page
            if not current_page.has_capacity():
                # check if current page range is full (16 pages max)
                if len(current_page_range) == 16:
                    # create new page range
                    self.table.base_pages[i].append([])
                    partition_index = len(self.table.base_pages[i]) - 1
                    current_page_range = self.table.base_pages[i][partition_index]
                
                # add new page to current range
                current_page_range.append(Page())
                current_page = current_page_range[-1]
            
            # create new records and store the info in mapping_list
            if record is not None:
                mapping_list.append((partition_index, current_page.write(record)))
            else:
                mapping_list.append((None, None))
            

        # update the page directory to reflect the new RID and location
        # rid -> page_index, list of offset for each column
        self.table.page_directory[base_rid] = mapping_list
        
        # update index if applicable:just primary key for milestone 1
        # index: key -> rid(for each column in list)
        self.table.key_directory[key] = base_rid

        # not sure if necessary; need to check with Lexi
        #if self.table.index.indices[self.table.key] is not None:
        #    index = self.table.index.indices[self.table.key]
        #    index.setdefault(key, []).append(base_rid)

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
        if primary_key not in self.table.key_directory:
            return False
        
        # check if the number of columns aligns with the table
        if len(columns) != self.table.num_columns:
            return False
        
        # get RID of the target record with key
        base_rid = self.table.key_directory[primary_key]
        partition_index, offset = self.table.page_directory[base_rid][0] # for indirection column
        page_index = offset // 511 # each page have 512 records max

        # fetch the corresponding base record and its indirection pointer
        base_indirection_pointer = self.table.base_pages[0][partition_index][page_index] # indirection column = 0
        latest_tail_rid = base_indirection_pointer.read(offset)

        # update a schematic encoding for tail records
        tail_schema_list = ['0'] * self.table.num_columns
        for i, val in enumerate(columns):
            if val is not None:
                tail_schema_list[i] = '1'
        tail_schema_encoding = ''.join(tail_schema_list)

        # create pre tail record if there is no tail record associated with base record yet
        if latest_tail_rid == 0:
            # for each given columns see whether 
            update_columns = [None] * self.table.num_columns
            for i, val in enumerate(columns):
                if val is not None:
                    # fetch the corresponding base record
                    base_record_loc = self.table.base_pages[i+4][partition_index][page_index]
                    base_record_value = base_record_loc.read(offset)
                    update_columns[i] = base_record_value

            # assign a new RID for the tail record
            new_tail_rid = self.table.get_unique_rid(base=False)
            
            # create new tail record that copy the base record
            # partial tail record with update columns
            new_tail_record = [
                base_rid,  # indirection: store base RID
                new_tail_rid,  # rid
                int(time()*1000),  # timestamp
                int(tail_schema_encoding, 2),  # schema encoding
                *update_columns,  # update columns
            ]
            
            # store list for page directory
            mapping_list = []
            # write record into tail pages for each column
            # iterate for each columns
            for i, record in enumerate(new_tail_record):
                # skip None value column
                if record is None:
                    continue  
                
                # check if there is tail page for column
                if len(self.table.tail_pages[i]) == 0:
                    self.table.tail_pages[i].append(Page())

                # get current tail page
                current_tail_page = self.table.tail_pages[i][-1]

                # check if page still have capacity, if not create a new page
                if not current_tail_page.has_capacity():
                    self.table.tail_pages[i].append(Page())
                    # set new page as current tail page
                    current_tail_page = self.table.tail_pages[i][-1]
                
                # current tail page index
                page_index = len(self.table.tail_pages[i]) - 1
                # create new records and store the info in mapping_list
                if record is not None:
                    mapping_list.append((page_index, current_tail_page.write(record)))
                else:
                    mapping_list.append((None, None))
            
            # update latest tail record
            latest_tail_rid = new_tail_rid

        # assign a new RID for the tail record
        new_tail_rid = self.table.get_unique_rid(base=False)

        # first tail record is the copy of base page of updating columns
        # create new tail record for updated value
        new_tail_record = [
            latest_tail_rid,  # indirection: store previous tail RID
            new_tail_rid,  # rid
            int(time()*1000),  # timestamp
            int(tail_schema_encoding, 2),  # schema encoding
            *columns,  # given columns: update column with value and non-updated column with None
        ]
        
        # store list for page directory
        mapping_list = []
        # write record into tail pages for each column
        # iterate for each columns
        for i, record in enumerate(new_tail_record):
            # skip None value column
            if record is None:
                continue  
            
            # check if there is tail page for column
            if len(self.table.tail_pages[i]) == 0:
                self.table.tail_pages[i].append(Page())

            # get current tail page
            current_tail_page = self.table.tail_pages[i][-1]

            # check if page still have capacity, if not create a new page
            if not current_tail_page.has_capacity():
                self.table.tail_pages[i].append(Page())
                # set new page as current tail page
                current_tail_page = self.table.tail_pages[i][-1]
            
            # current tail page index
            page_index = len(self.table.tail_pages[i]) - 1
            # create new records and store the info in mapping_list
            if record is not None:
                mapping_list.append((page_index, current_tail_page.write(record)))
            else:
                mapping_list.append((None, None))

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
