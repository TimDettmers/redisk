import array
import ujson
import numpy as np

from redisk.util import Types

types = Types()

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


class ListDataHandler(AbstractDataHandler):
    def __init__(self, tbl, fhandle):
        super(ListDataHandler, self).__init__(tbl, fhandle)
        self.supported_types.add(list)
        self.strType2ArrayType = {}
        self.strType2ArrayType['2'] = 'i'

    def set(self, key, value):
        strType = types.get_type_str(type(value[0]))
        if strType in self.strType2ArrayType:
            self.set_with_array(key, value, strType)
        elif strType in ['0', '1']:
            str_value = ujson.dumps(value)
            self.set_string(key, strType + str_value, type(value))
        else:
            raise Exception('Type not supported!')


    def get(self, key, start, length):
        value = self.get_string(key, start, length)
        if value is None: return None
        else:
            strType = value[0]
            if strType in self.strType2ArrayType:
                return self.get_with_array(key, value[1:], strType)
            elif strType in ['0', '1']:
                    return ujson.loads(value[1:])
            else:
                raise Exception('Type not supported!')

    def set_with_array(self, key, value, strType):
        arrayType = self.strType2ArrayType[strType]
        str_value = array.array(arrayType, value).tostring()
        self.set_string(key, strType + str_value, type(value))

    def get_with_array(self, key, value, strType):
        arrayType = self.strType2ArrayType[strType]
        return array.array(arrayType, value).tolist()


class NumpyDataHandler(AbstractDataHandler):
    def __init__(self, tbl, fhandle):
        super(NumpyDataHandler, self).__init__(tbl, fhandle)
        self.supported_types.add(np.ndarray)
        self.numpytype2byte = {}
        self.byte2numpytype = {}
        i = 0
        for _, types in np.sctypes.items():
            for t in types:
                self.numpytype2byte[np.dtype(t)] = str(i)
                self.byte2numpytype[str(i)] = np.dtype(t)
                i+= 1

    def set(self, key, value):
        strType = self.numpytype2byte[value.dtype]
        self.set_string(key, strType + value.tobytes(), type(value))

    def get(self, key, start, length):
        value = self.get_string(key, start, length)
        if value is None: return None
        else:
            strType = value[0]
            dtype = self.byte2numpytype[strType]
            data = np.frombuffer(value[1:], dtype=dtype)
            return data
