from __future__ import print_function
from __future__ import unicode_literals

from os.path import join

from handlers import IntDataHandler, StringDataHandler, ListDataHandler, NumpyDataHandler
from util import Types

import redis
import os

types = Types()

class Table(object):
    def __init__(self, name, base_path=None):
        self.name = name
        self.db = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.fhandle = None
        home = os.environ['HOME']
        self.base_dir = base_path or join(home, '.redisk')
        self.make_table_path()

    def make_table_path(self):
        if not os.path.exists(self.base_dir):
            os.mkdir(self.base_dir)
        if not os.path.exists(join(self.base_dir, self.name)):
            with open(join(self.base_dir, self.name), 'a'):
                os.utime(join(self.base_dir, self.name), None)
        return join(self.base_dir, self.name)

    def open_connection(self):
        #assert not os.path.exists(join(self.base_dir, self.name + '_lock')), 'Connection to table already open!'
        #with open(join(self.base_dir, self.name + '_lock'), 'a'):
        #    os.utime(join(self.base_dir, self.name), None)

        self.fhandle = open(join(self.base_dir, self.name), 'wb+')
        return self.fhandle


    def close_connection(self):
        #os.remove(join(self.base_dir, self.name + '_lock'))
        assert self.fhandle is not None, 'Connection to table is not open!'
        self.fhandle.close()

    def __del__(self):
        self.close_connection()


    def set(self, key, start, length, type_value):
        strType = types.get_type_str(type_value)
        self.db.set(join(self.name, key), ' '.join([str(start), str(length), strType]))

    def get(self, key):
        return self.db.get(join(self.name, key))



class Redisk(object):
    def __init__(self, tbl):
        self.tbl = tbl
        self.processors = []
        self.type2processor = {}
        self.base_processor = None

        self.construct_processors()

    def construct_processors(self):
        fhandle = self.tbl.open_connection()
        self.processors.append(StringDataHandler(self.tbl, fhandle))
        self.processors.append(IntDataHandler(self.tbl, fhandle))
        self.processors.append(ListDataHandler(self.tbl, fhandle))
        self.processors.append(NumpyDataHandler(self.tbl, fhandle))
        for p in self.processors:
            for t in p.get_supported_types():
                self.type2processor[t] = p

    def get_from_redis(self, key):
        values = self.tbl.get(key)
        if values is None: return None
        start, length, strType = values.split(' ')
        return int(start), int(length), types.get_type(strType)

    def set(self, key, value):
        self.type2processor[type(value)].set(key, value)

    def get(self, key):
        start, length, type_value = self.get_from_redis(key)
        return self.type2processor[type_value].get(key, start, length)
