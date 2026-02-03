
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # check if page have not reached capacity
        if self.num_records < 512:
            return True
        return False
        #pass

    def write(self, value):
        self.num_records += 1
        # track on bytes
        self.data[self.num_records * 8 - 8:self.num_records * 8] = value.to_bytes(8)
        #
        pass

