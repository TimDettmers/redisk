from __future__ import unicode_literals, print_function

import pytest
import numpy as np

from redisk.core import Table, Redisk

from uuid import uuid4


def test_string_handler():
    tbl = Table('test')
    db = Redisk(tbl)

    for i in range(1000):
        expected = str(uuid4())
        key = str(uuid4())
        db.set(key, expected)
        value = db.get(key)
        assert value == expected, 'String value from redisk different from the expected value!'

def test_int_handler():
    tbl = Table('test')
    db = Redisk(tbl)

    for num in np.random.randint(0, 1000, size=(1000)):
        key = str(uuid4())
        db.set(key, int(num))
        value = db.get(key)
        assert value == num, 'Int value from redisk different from the expected value!'


def test_int_list_handler():
    tbl = Table('test')
    db = Redisk(tbl)

    for num in np.random.randint(0, 1000, size=(1000, 10)):
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

    for major in range(1000):
        data = []
        for minor in range(10):
            data.append(unicode(uuid4()))
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

    for arr in np.random.rand(1000, 10):
        key = str(uuid4())
        db.set(key, arr)
        value = db.get(key)
        assert type(value) == type(arr), 'Types are different'
        np.testing.assert_array_equal(value, arr, 'Arrays are not equal!')
