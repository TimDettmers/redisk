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


