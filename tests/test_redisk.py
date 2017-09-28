from __future__ import unicode_literals, print_function

import pytest
import numpy as np

from redisk.core import Table, Redisk

from uuid import uuid4

repeats = 10




def test_string_handler():
    tbl = Table('test')
    db = Redisk(tbl)

    for i in range(repeats):
        expected = str(uuid4())
        key = str(uuid4())
        db.set(key, expected)
        value = db.get(key)
        assert value == expected, 'String value from redisk different from the expected value!'

def test_close_open():
    tbl = Table('test')
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

    tbl = Table('test')
    db = Redisk(tbl)
    for key, expected in zip(keys, expects):
        value = db.get(key)
        assert value == expected, 'String value from redisk different from the expected value!'

def test_string_handler_batched_get():
    tbl = Table('test')
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

def test_int_handler():
    tbl = Table('test')
    db = Redisk(tbl)

    for num in np.random.randint(0, 100, size=(repeats)):
        key = str(uuid4())
        db.set(key, int(num))
        value = db.get(key)
        assert value == num, 'Int value from redisk different from the expected value!'

def test_clear_db():
    tbl = Table('test')
    db = Redisk(tbl)

    keys = []
    for i in range(repeats):
        key = str(uuid4())
        db.set(key, i)
    tbl.clear_db()

    for key in keys:
        db.exists(key) == False, 'Key was not deleted!'

def test_int_list_handler():
    tbl = Table('test')
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

def test_int_str_handler():
    tbl = Table('test')
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

def test_numpy_handler():
    tbl = Table('test')
    db = Redisk(tbl)

    for arr in np.random.rand(repeats, 10):
        key = str(uuid4())
        db.set(key, arr)
        value = db.get(key)
        assert type(value) == type(arr), 'Types are different'
        np.testing.assert_array_equal(value, arr, 'Arrays are not equal!')

def test_append():
    tbl = Table('test')
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

def test_references():
    tbl = Table('test')
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

        print(i)
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

