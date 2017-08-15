
class AbstractDataHandler(object):
    def __init__(self, tbl, fhandle):
        self.tbl = tbl
        self.fhandle = fhandle
        self.supported_types = set()

    def get_supported_types(self):
        return self.supported_types

    def set_string(self, key, value, type_value):
        self.fhandle.seek(0, 2)
        start = self.fhandle.tell()
        self.fhandle.write(value)
        end = self.fhandle.tell()
        length = end-start
        self.tbl.set(key, start, length, type_value)

    def get_string(self, key, start, length):
        self.fhandle.seek(int(start))
        return self.fhandle.read(int(length))

    def set(self, key, value):
        raise NotImplementedError('Classes that inherit from AbstractDataHandler need to implement the set method!')

    def get(self, key, start, length):
        raise NotImplementedError('Classes that inherit from AbstractDataHandler need to implement the set method!')

class StringDataHandler(AbstractDataHandler):
    def __init__(self, tbl, fhandle):
        super(StringDataHandler, self).__init__(tbl, fhandle)
        self.supported_types.add(unicode)
        self.supported_types.add(str)

    def set(self, key, value):
        self.set_string(key, value, type(value))

    def get(self, key, start, length):
        return self.get_string(key, start, length)

class IntDataHandler(AbstractDataHandler):
    def __init__(self, tbl, fhandle):
        super(IntDataHandler, self).__init__(tbl, fhandle)
        self.supported_types.add(int)

    def set(self, key, value):
        self.set_string(key, str(value), type(value))

    def get(self, key, start, length):
        value = self.get_string(key, start, length)
        if value is None: return None
        else: return int(value)

