from __future__ import print_function
from __future__ import unicode_literals

from os.path import join

from redisk.handlers import IntDataHandler, StringDataHandler, ListDataHandler, NumpyDataHandler, DictDataHandler
from redisk.util import Types
from uuid import uuid4

import redis
import os
import ujson
import shutil

types = Types()

class Table(object):
    def __init__(self, name, base_dir, db_id=0, host='localhost'):
        self.name = name
        self.db = redis.StrictRedis(host='localhost', port=6379, db=db_id, decode_responses=True)
        self.read_fhandle = None
        self.write_path = join(base_dir, self.name)
        home = os.environ['HOME']
        self.base_dir = base_dir
        self.make_table_path()

    def make_table_path(self):
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        if not os.path.exists(join(self.base_dir, self.name)):
            with open(join(self.base_dir, self.name), 'a'):
                os.utime(join(self.base_dir, self.name), None)
        return join(self.base_dir, self.name)

    def open_connection(self):
        #assert not os.path.exists(join(self.base_dir, self.name + '_lock')), 'Connection to table already open!'
        #with open(join(self.base_dir, self.name + '_lock'), 'a'):
        #    os.utime(join(self.base_dir, self.name), None)

        self.read_fhandle = open(join(self.base_dir, self.name), 'rb+')
        return self.read_fhandle, self.write_path


    def close_connection(self):
        #os.remove(join(self.base_dir, self.name + '_lock'))
        print('connecting is being closed...')
        assert self.read_fhandle is not None, 'Connection to table is not open!'
        self.read_fhandle.close()

    def __exit__(self):
        self.close_connection()

    def add_pointer(self, key, pointer):
        if key == pointer: return
        start, length, strType, strPointers, strArgs = self.db.get(join(self.name, key)).split(' ')
        pointers = ujson.loads(strPointers)
        pointers.append(pointer)
        strPointers = ujson.dumps(pointers)
        self.db.set(join(self.name, key), ' '.join([start, length, strType, strPointers, strArgs]))


    def set(self, key, start, length, type_value, vargs=[]):
        strArgs = ujson.dumps(vargs)
        strType = types.get_type_str(type_value)
        self.db.set(join(self.name, key), ' '.join([str(start), str(length), strType, '[]', strArgs]))

    def get(self, key):
        value = self.db.get(join(self.name, key))
        if value is None: return
        else:
            start, length, strType, strPointers , strArgs = value.split(' ')
            start, length = int(start), int(length)
            type_value = types.get_type(strType)
            vargs = ujson.loads(strArgs)
            pointers = ujson.loads(strPointers)
            return start, length, type_value, pointers, vargs

    def sadd(self, key, value):
        self.db.sadd('{0}/{1}'.format(self.name, key), value)

    def get_members(self, key):
        return self.db.smembers('{0}/{1}'.format(self.name, key))

    def get_pointer(self, key):
        value = self.db.get(join(self.name, key))
        if value is None: return key
        else: return join(key, str(uuid4()))

    def key_col_iter(self):
        for key in self.db.scan_iter(match='{0}/*'.format(self.name)):
            key = key[len('{0}/'.format(self.name)):]
            if key.count('/') > 0:
                col = key[key.index('/')+1:]
                key = key[:key.index('/')]
            yield key, col


class Redisk(object):
    def __init__(self, tbl):
        self.tbl = tbl
        self.processors = []
        self.type2processor = {}
        self.base_processor = None

        self.construct_processors()

    def construct_processors(self):
        fhandle, wpath = self.tbl.open_connection()
        self.processors.append(StringDataHandler(self.tbl, fhandle, wpath))
        self.processors.append(IntDataHandler(self.tbl, fhandle, wpath))
        self.processors.append(ListDataHandler(self.tbl, fhandle, wpath))
        self.processors.append(DictDataHandler(self.tbl, fhandle, wpath))
        self.processors.append(NumpyDataHandler(self.tbl, fhandle, wpath))
        for p in self.processors:
            for t in p.get_supported_types():
                self.type2processor[t] = p

    def set(self, key, value, col=None, reference_id=None):
        if col is not None: key = '{0}/{1}'.format(key, col)
        self.type2processor[type(value)].set(key, value)
        if reference_id is not None:
            references_key = join('references', str(reference_id))
            if self.exists(references_key): references = self.get(references_key)
            else: references = []
            references.append(key)
            self.set(join('references', str(reference_id)), references)
            self.set(join(key, 'reference'), str(reference_id))

    def exists(self, key):
        return self.tbl.get(key) is not None

    def get(self, key, col=None):
        if col is not None: key = '{0}/{1}'.format(key, col)
        values = self.tbl.get(key)
        if values is None: return None
        start, length, type_value, pointers, vargs = values
        data = self.type2processor[type_value].get(key, start, length, vargs)
        if len(pointers) > 0:
            for p in pointers:
                data += self.get(p)
        return data

    def sadd(self, key, value):
        self.tbl.sadd(key, value)

    def get_members(self, key):
        return self.tbl.get_members(key)

    def key_col_pairs(self):
        for key, col in self.tbl.key_col_iter():
            yield key, col

    def batched_get(self, keys):
        triples = []
        type_value_batch = None
        for key in keys:
            start, length, type_value, pointers, vargs = self.tbl.get(key)
            if type_value_batch is None: type_value_batch = type_value
            assert type_value == type_value_batch, 'Batched queries only work for a single data type!'
            triples.append((key, int(start), int(length)))
        return self.type2processor[type_value].batched_get(triples, None)

    def get_with_reference(self, reference_id):
        references = self.get(join('references', str(reference_id)))
        data = []
        for key in references:
            data.append(self.get(key))
        return data

    def get_reference(self, key):
        return self.get(join(key, 'reference'))

    def append(self, key, value, flush_length_threshold=10000000):
        self.type2processor[list].append(key, value, flush_length_threshold)

    def close(self):
        for p in self.processors:
            p.close()

    def delete_db(self):
        if os.path.exists(self.tbl.base_dir):
            shutil.rmtree(self.tbl.base_dir)
        self.tbl.db.flushdb()

    def __exit__(self):
        self.close()
