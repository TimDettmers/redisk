import numpy as np

class Types(object):
    def __init__(self):
        self.strType2type = {}
        self.type2StrType = {}

        self.strType2type['0'] = bytes
        self.strType2type['1'] = str
        self.strType2type['2'] = int
        self.strType2type['3'] = list
        self.strType2type['4'] = np.ndarray

        self.type2StrType[bytes] = '0'
        self.type2StrType[str] = '1'
        self.type2StrType[int] = '2'
        self.type2StrType[list] = '3'
        self.type2StrType[np.ndarray] = '4'

    def get_type_str(self, type_value):
        if type_value not in self.type2StrType:
            raise Exception('Type {0} not supported!'.format(type_value))
        else:
            return self.type2StrType[type_value]

    def get_type(self, str_value):
        if str_value not in self.strType2type:
            raise Exception('String type {0} not supported!'.format(str_value))
        else:
            return self.strType2type[str_value]
