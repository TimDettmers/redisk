import array
import ujson
import numpy as np

from redisk.util import Types
from os.path import join
import sys

types = Types()

class AbstractDataHandler(object):
    def __init__(self, tbl, fhandle):
        self.tbl = tbl
        self.fhandle = fhandle
        self.supported_types = set()

    def get_supported_types(self):
        return self.supported_types

    def set_bytes(self, key, value, type_value, *args):
        self.fhandle.seek(0, 2)
        start = self.fhandle.tell()
        self.fhandle.write(value)
        end = self.fhandle.tell()
        length = end-start
        self.tbl.set(key, start, length, type_value, *args)

    def get_bytes(self, key, start, length):
        self.fhandle.seek(int(start))
        value = self.fhandle.read(int(length))
        if value is None: return None
        return value

    def close(self):
        pass

    def set(self, key, value):
        raise NotImplementedError('Classes that inherit from AbstractDataHandler need to implement the set method!')

    def get(self, key, start, length, vargs):
        raise NotImplementedError('Classes that inherit from AbstractDataHandler need to implement the set method!')

class StringDataHandler(AbstractDataHandler):
    def __init__(self, tbl, fhandle):
        super(StringDataHandler, self).__init__(tbl, fhandle)
        self.supported_types.add(str)
        self.supported_types.add(bytes)

    def set(self, key, value):
        self.set_bytes(key, value.encode('utf8'), type(value))

    def get(self, key, start, length, vargs):
        return self.get_bytes(key, str(start), length).decode('utf8')

class IntDataHandler(AbstractDataHandler):
    def __init__(self, tbl, fhandle):
        super(IntDataHandler, self).__init__(tbl, fhandle)
        self.supported_types.add(int)

    def set(self, key, value):
        self.set_bytes(key, str(value).encode(), type(value))

    def get(self, key, start, length, vargs):
        value = self.get_bytes(key, start, length)
        if value is None: return None
        return int(value.decode())


class ListDataHandler(AbstractDataHandler):
    def __init__(self, tbl, fhandle):
        super(ListDataHandler, self).__init__(tbl, fhandle)
        self.supported_types.add(list)
        self.strType2ArrayType = {}
        self.strType2ArrayType['2'] = 'i'
        self.temp_store = {}
        self.temp_store_lengths = {}

    def set(self, key, value):
        strType = types.get_type_str(type(value[0]))
        if strType in self.strType2ArrayType:
            self.set_with_array(key, value, strType)
        elif strType in ['0', '1']:
            str_value = ujson.dumps(value)
            self.set_bytes(key, str_value.encode(), type(value), strType)
        else:
            raise Exception('Type not supported!')


    def get(self, key, start, length, vargs):
        value = self.get_bytes(key, start, length)
        if value is None: return None
        else:
            strType = str(vargs[0])
            if strType in self.strType2ArrayType:
                arrayType = vargs[0]
                return self.get_with_array(key, value, strType)
            elif strType in ['0', '1']:
                return ujson.loads(value)
            else:
                raise Exception('Type not supported!')

    def append(self, key, value, flush_length_threshold):
        if key not in self.temp_store:
            self.temp_store[key] = []
            self.temp_store_lengths[key] = 0

        self.temp_store[key].append(value)
        self.temp_store_lengths[key] += 1

        if self.temp_store_lengths[key] >= flush_length_threshold:
            self.flush(key)

    def flush(self, key):
        values = self.temp_store.pop(key)
        self.temp_store_lengths.pop(key)
        pointer = self.tbl.get_pointer(key)
        self.set(pointer, values)
        self.tbl.add_pointer(key, pointer)

    def set_with_array(self, key, value, strType):
        arrayType = self.strType2ArrayType[strType]
        str_value = array.array(arrayType, value).tobytes()
        self.set_bytes(key, str_value, type(value), strType)

    def get_with_array(self, key, value, strType):
        arrayType = self.strType2ArrayType[strType]
        return array.array(arrayType, value).tolist()

    def close(self):
        for key in list(self.temp_store.keys()):
            self.flush(key)


class NumpyDataHandler(AbstractDataHandler):
    def __init__(self, tbl, fhandle):
        super(NumpyDataHandler, self).__init__(tbl, fhandle)
        self.supported_types.add(np.ndarray)
        self.numpytype2byte = {}
        self.byte2numpytype = {}
        i = 0
        for _, types in np.sctypes.items():
            for t in types:
                self.numpytype2byte[np.dtype(t)] = i
                self.byte2numpytype[i] = np.dtype(t)
                i+= 1

    def set(self, key, value):
        strType = self.numpytype2byte[value.dtype]
        self.set_bytes(key, value.tobytes(), type(value), strType)

    def get(self, key, start, length, vargs):
        value = self.get_bytes(key, start, length)
        if value is None: return None
        else:
            strType = vargs
            dtype = self.byte2numpytype[strType]
            data = np.frombuffer(value, dtype=dtype)
            return data
