from __future__ import unicode_literals, print_function

import os
import shutil
import pytest
import numpy as np

from redisk import Table, Redisk

from uuid import uuid4
from os.path import join, exists
from collections import OrderedDict


repeats = 10

base_path = '/tmp/redisk/unittests/'

if not exists(base_path):
    os.makedirs(base_path)

tbl = Table(name='test', base_dir=base_path)
db = Redisk(tbl)
db.delete_db()

np.random.seed(0)

def test_string_handler():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    for i in range(repeats):
        expected = str(uuid4())
        key = str(uuid4())
        db.set(key, expected)
        value = db.get(key)
        assert value == expected, 'String value from redisk different from the expected value!'

def test_make_long_path():
    tbl = Table(name='test', base_dir='/tmp/redisk/very/long/path/')
    db = Redisk(tbl)
    db.delete_db()

def test_close_open():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    keys = []
    expects = []
    for i in range(repeats):
        expected = str(uuid4())
        key = str(uuid4())
        db.set(key, expected)
        keys.append(key)
        expects.append(expected)

    db.close()
    tbl.close_connection()

    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)
    for key, expected in zip(keys, expects):
        value = db.get(key)
        assert value == expected, 'String value from redisk different from the expected value!'

    db.delete_db()

def test_string_handler_batched_get():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    for i in range(repeats):
        expected_values = []
        keys = []
        for i in range(10):
            expected = str(uuid4())
            key = str(uuid4())

            expected_values.append(expected)
            keys.append(key)

            db.set(key, expected)
        values = db.batched_get(keys)
        for value, expected in zip(values, expected_values):
            assert value == expected, 'String value from redisk different from the expected value!'
    db.delete_db()

def test_int_handler():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    for num in np.random.randint(0, 100, size=(repeats)):
        key = str(uuid4())
        db.set(key, int(num))
        value = db.get(key)
        assert value == num, 'Int value from redisk different from the expected value!'
    db.delete_db()

def test_col():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    for num in np.random.randint(0, 100, size=(repeats)):
        key = str(uuid4())
        db.set(key, int(num), col=1)
        db.set(key, int(num)+3, col='test')
        value = db.get(key)
        assert value is None
        value = db.get(key, col=1)
        assert value == num, 'Int value from redisk different from the expected value!'
        value = db.get(key, col='test')
        assert value == num+3, 'Int value from redisk different from the expected value!'
    db.delete_db()


def test_delete_db():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    keys = []
    for i in range(repeats):
        key = str(uuid4())
        db.set(key, i)
    db.delete_db()

    for key in keys:
        db.exists(key) == False, 'Key was not deleted!'
    db.delete_db()

def test_int_list_handler():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    for num in np.random.randint(0, 100, size=(repeats, 10)):
        num = num.tolist()
        key = str(uuid4())
        db.set(key, num)
        value = db.get(key)
        assert type(value) == type(num), 'Types are different'
        assert len(value) == 10, 'Length is different'
        assert type(value[0]) == type(num[0]), 'Inner types are different'
        for x1, x2 in zip(value, num):
            assert x1 == x2, 'Int value from redisk different from the expected value!'
    db.delete_db()

def test_int_str_handler():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    for major in range(repeats):
        data = []
        for minor in range(10):
            data.append(str(uuid4()))
        key = str(uuid4())
        db.set(key, data)
        value = db.get(key)
        assert type(value) == type(data), 'Types are different'
        assert len(value) == 10, 'Length is different'
        assert type(value[0]) == type(data[0]), 'Inner types are different'
        for x1, x2 in zip(value, data):
            assert x1 == x2, 'String value from redisk different from the expected value!'
    db.delete_db()

def test_numpy_handler():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    for arr in np.random.rand(repeats, 10):
        key = str(uuid4())
        db.set(key, arr)
        value = db.get(key)
        assert type(value) == type(arr), 'Types are different'
        np.testing.assert_array_equal(value, arr, 'Arrays are not equal!')
    db.delete_db()

def test_numpy_handler_2D():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    for arr in np.random.rand(repeats, 10, 5):
        key = str(uuid4())
        db.set(key, arr)
        value = db.get(key)
        assert type(value) == type(arr), 'Types are different'
        np.testing.assert_array_equal(value, arr, 'Arrays are not equal!')
    db.delete_db()

def test_dict_list_handler():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    dict1 = {}
    for j in range(repeats):
        # fill with some random data
        for i in range(10):
            dict1[str(uuid4())] = str(uuid4())
            dict1[str(uuid4())] = np.random.rand(1)[0]

        # test normal dict
        key = str(uuid4())
        db.set(key, dict1)
        dict2 = db.get(key)

        for key, value1 in dict1.items():
            value2 = dict2[key]
            assert value1 == value2

        # test OrderedDict
        key = str(uuid4())
        db.set(key, OrderedDict(dict1))
        dict2 = db.get(key)

        for key, value1 in dict1.items():
            value2 = dict2[key]
            assert value1 == value2

    db.delete_db()
    db.close()

def test_append():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)
    length = 3

    data = []
    for i in range(repeats):
        key = str(uuid4())
        lvalues = np.random.randint(0, 100, size=(10)).tolist()
        data.append((key, lvalues))
        for num in lvalues:
            db.append(key, num, length)

    db.close()

    for key, expected in data:
        value = db.get(key)
        assert type(value) == type(expected), 'Types are different'
        assert len(value) == 10, 'Length is different'
        assert type(value[0]) == type(expected[0]), 'Inner types are different'
        for x1, x2 in zip(value, expected):
            assert x1 == x2, 'Int value from redisk different from the expected value!'
    db.delete_db()

def test_references():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    keys = []
    for i in range(repeats):
        key1 = str(uuid4())
        key2 = str(uuid4())
        key3 = str(uuid4())
        ref = str(uuid4())

        value1 = 'a'
        value2 = [3, 5, 6, 7]
        value3 = ['aa', 'bb', 'cc']
        db.set(key1, value1, reference_id=ref)
        db.set(key2, value2, reference_id=ref)
        db.set(key3, value3, reference_id=ref)

        x1, x2, x3 = db.get_with_reference(ref)
        assert x1 == value1, 'Reference does not yield same data!'
        assert len(x2) == 4, 'Reference does not yield same data!'
        for a, b in zip(x2, value2):
            assert a == b, 'Reference does not yield same data!'
        for a, b in zip(x3, value3):
            assert a == b, 'Reference does not yield same data!'

        assert db.get_reference(key1) == ref, 'Wrong reference!'
        assert db.get_reference(key2) == ref, 'Wrong reference!'
        assert db.get_reference(key3) == ref, 'Wrong reference!'
    db.delete_db()


def test_sadd():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)
    db.sadd('test', 0.1)
    db.sadd('test', 0.2)
    db.sadd('test', 0.3)
    db.sadd('test', 0.3)
    db.sadd('test', 0.3)
    db.sadd('test', 0.2)

    setvalues = list(db.get_members('test'))
    setvalues.sort()
    setvalues = [float(s) for s in setvalues]
    assert all([s1==s2 for s1, s2 in zip(setvalues, [0.1, 0.2, 0.3])])
    db.delete_db()

def test_key_iter():
    tbl = Table(name='test', base_dir=base_path)
    db = Redisk(tbl)

    keys = [str(uuid4()) for i in range(10)]
    cols = [str(uuid4()) for i in range(10)]
    values = [str(uuid4()) for i in range(10)]
    for key, col, value in zip(keys, cols, values):
        db.set(key, value, col=col)

    keys, cols, values = set(keys), set(cols), set(values)

    count = 0
    for key, col in db.key_col_pairs():
        value = db.get(key, col=col)
        count += 1 if (key in keys and col in cols and value in values) else 0
    assert count == 10
    db.delete_db()
