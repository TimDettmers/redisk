from __future__ import unicode_literals, print_function

import pytest
import numpy as np

from redisk.core import Table, Redisk

from uuid import uuid4


def test_string_processor():
    tbl = Table('test')
    db = Redisk(tbl)

    for i in range(1000):
        expected = str(uuid4())
        key = str(uuid4())
        db.set(key, expected)
        value = db.get(key)
        assert value == expected, 'String value from redisk different from the expected value!'

def test_int_processor():
    tbl = Table('test')
    db = Redisk(tbl)

    for num in np.random.randint(0, 1000, size=(1000)):
        key = str(uuid4())
        db.set(key, int(num))
        value = db.get(key)
        assert value == num, 'Int value from redisk different from the expected value!'


def test_int_list_processor():
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

def test_int_str_processor():
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
