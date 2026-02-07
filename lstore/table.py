from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns, schema_encoding):
        # primary key
        self.key = key

        # detail context for each columns
        self.columns = columns

        # Metadata Columns

        # 0: indirection column 
        # base page: stores RID of latest tail record
        # tail page: stores the RID of the previous tail record
        self.indirection = None

        # 1: RID column
        self.rid = rid

        # 2: timestamp column
        self.timestamp = time()

        # 3: schema encoding column
        # base page: track total updates
        # tail page: track how many columns have been updated in that update
        self.schema_encoding = schema_encoding

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # list (page, offset)
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.total_columns = 4 + num_columns # metadata column + given columns

        # base pages
        #self.base_page_range = [[0]] * self.total_columns # track range and index for each columns
        self.base_pages = [[[]] for j in range(self.total_columns)] # store list of base pages for each columns: [column][range_index (16 max)][page_index (512 max)]

        # tail pages
        #self.tail_page_range = [0] * self.total_columns # track range index for each columns
        self.tail_pages = [[] for j in range(self.total_columns)] # store list of tail pages for each columns: [column][page_index]

        # count the RID for its uniqueness
        self.rid_count = [0, 1] # [base RID count, tail RID count]


    def __merge(self):
        print("merge is happening")
        pass
    
    # return a new unique RID: base RID starts with 'b', tail RID starts with 't'
    def get_unique_rid(self, base=False):
        if base:
            self.rid_count[0] += 1
            return ('b' + str(self.rid_count[0]))
        else:
            self.rid_count[1] += 1
            return ('t' + str(self.rid_count[1]))
        
