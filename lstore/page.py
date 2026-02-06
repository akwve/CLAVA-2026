
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # check if page have not reached capacity of 4096/8 = 512
        # page of 512 records for each
        if self.num_records < 512:
            return True
        return False
        

    def write(self, value):
        # write the value into bytearray and track the number of records stored
        self.num_records += 1
        self.data[self.num_records * 8 - 8:self.num_records * 8] = value.to_bytes(8, byteorder='little')
        
        # return the index where the value is stored
        return self.num_records - 1
    
    def read(self, index):
        # retrive the value from bytearray by converting
        return int.from_bytes(self.data[index * 8:index * 8 + 8], byteorder='little')


