import pytest

from datetime import datetime

from collections import OrderedDict
import subprocess

import numpy as np
import pandas as pd

import clickhouse as ch


DTYPE_TO_CLICKHOUSE = OrderedDict((
    ('int8', 'Int8'),
    ('uint8', 'UInt8'),
    ('int16', 'Int16'),
    ('uint16', 'UInt16'),
    ('int32', 'Int32'),
    ('uint32', 'UInt32'),
    ('int64', 'Int64'),
    ('uint64', 'UInt64'),
    ('float32', 'Float32'),
    ('float64', 'Float64'),
))


def execute_sql(sql):
    """helper function to create test data"""
    subprocess.check_call(['clickhouse-client', '--query', sql])


@pytest.fixture(scope="module")
def array_data():
    execute_sql('CREATE DATABASE IF NOT EXISTS test_pybind')
    execute_sql('DROP TABLE IF EXISTS test_pybind.array')
    execute_sql(
        "CREATE TABLE IF NOT EXISTS test_pybind.array (arr Array(UInt64)) "
        "ENGINE = Memory")
    execute_sql(
        "INSERT INTO test_pybind.array VALUES "
        "([0]), "
        "([0,1]), "
        "([0,1,2]), "
        "([0,1,2,3]), "
        "([0,1,2,3,4]), "
        "([0,1,2,3,4,5]), "
        "([0,1,2,3,4,5,6]), "
        "([0,1,2,3,4,5,6,7]), "
        "([0,1,2,3,4,5,6,7,8]), "
        "([0,1,2,3,4,5,6,7,8,9])"
    )

    data = [np.arange(i + 1, dtype='uint64') for i in range(10)]
    yield data

    # tear down
    execute_sql('DROP TABLE test_pybind.array')


@pytest.fixture(scope="module")
def enum_data():
    execute_sql('CREATE DATABASE IF NOT EXISTS test_pybind')
    execute_sql('DROP TABLE IF EXISTS test_pybind.enum')
    # XXX clickhouse-cpp currently only allows enum name containing alphanumeric
    # and underscore, which is more restrictive than ClickHouse.
    execute_sql(
        "CREATE TABLE test_pybind.enum ("
        "e8   Enum8('a'=-1, 'b'=0, 'c'=1, 'd'=-128), "
        "e16 Enum16('a'=-1, 'b'=0, 'c'=1, 'd'=-256)"
        ") ENGINE = Memory")
    execute_sql("INSERT INTO test_pybind.enum VALUES ('a', 'a')")
    execute_sql("INSERT INTO test_pybind.enum VALUES ('b', 'b')")
    execute_sql("INSERT INTO test_pybind.enum VALUES ('c', 'c')")
    execute_sql("INSERT INTO test_pybind.enum VALUES ('d', 'd')")

    name = {}
    name[8] = ['a', 'b', 'c', 'd']
    name[16] = name[8]
    val = {}
    val[8] = [-1, 0, 1, -128]
    val[16] = [-1, 0, 1, -256]

    yield name, val

    # tear down
    execute_sql('DROP TABLE test_pybind.enum')


class TestClientSelect(object):
    @pytest.mark.parametrize("dtype", DTYPE_TO_CLICKHOUSE.keys())
    def test_number(self, dtype):
        # ch_type is clickhouse typename
        limit = 10240

        client = ch.Client()
        block = client.select(
            "SELECT to{chtype}(number) as number FROM system.numbers LIMIT {limit}".format(
                chtype=DTYPE_TO_CLICKHOUSE[dtype], limit=limit))

        assert block.columns == ['number']
        assert block.shape == (limit, 1)
        assert block[0].dtype == np.dtype(dtype)
        assert np.array_equal(np.arange(limit, dtype=dtype), block['number'].values)


    @pytest.mark.parametrize("limit,string_size", [(100, 3), (10240, 5)])
    def test_fixed_string(self, limit, string_size):
        client = ch.Client()
        block = client.select(
            "SELECT toFixedString(toString(number), {string_size}) as fixstr "
            "FROM system.numbers LIMIT {limit}".format(
                string_size=string_size, limit=limit))

        assert block.columns == ['fixstr']
        assert block.shape == (limit, 1)
        assert block[0].dtype == np.dtype('S{}'.format(string_size))

        arr = block['fixstr']
        for i in range(limit):
            assert str(i) == arr[i]


    def test_string(self):
        limit = 10240

        client = ch.Client()
        block = client.select(
            "SELECT toString(number) as str "
            "FROM system.numbers LIMIT {limit}".format(
                limit=limit))

        assert block.columns == ['str']
        assert block.shape == (limit, 1)

        assert len(block['str']) == limit
        for i, v in enumerate(block['str']):
            assert str(i) == v


    def test_date(self):
        limit = 365 * 10

        client = ch.Client()
        block = client.select(
            "SELECT toDate(number) as date "
            "FROM system.numbers LIMIT {limit}".format(
                limit=limit))

        assert block.columns == ['date']
        assert block.shape == (limit, 1)

        EPOCH_DATE = np.datetime64('1970-01-01', 'D')
        arr = block[0]
        for d in range(limit):
            assert arr[d] == EPOCH_DATE + d


    def test_datetime(self):
        limit = 365 * 10

        client = ch.Client()
        block = client.select(
            "SELECT toDateTime(number * 86400) as datetime "
            "FROM system.numbers LIMIT {limit}".format(
                limit=limit))

        assert block.columns == ['datetime']
        assert block.shape == (limit, 1)

        # Assume using numpy >= 1.11, which uses timezone naive datetime.
        # For older version of numpy, we need to specify time as
        # '1970-01-01T00:00:00-00'
        EPOCH_DATETIME = np.datetime64('1970-01-01T00:00:00', 's')

        arr = block[0]
        for d in range(limit):
            assert arr[d] == (EPOCH_DATETIME + d * 86400)


    def test_array(self, array_data):
        client = ch.Client()
        block = client.select("SELECT arr FROM test_pybind.array")

        assert block.columns == ['arr']
        assert block.shape == (len(array_data), 1)

        for expected, actual in zip(array_data, block['arr']):
            assert expected.dtype == actual.dtype
            assert (expected == actual).all()


    @pytest.mark.parametrize("enum_size", [8, 16])
    def test_enum(self, enum_size, enum_data):
        client = ch.Client()
        block = client.select(
            "SELECT e{} as e "
            "FROM test_pybind.enum".format(enum_size))

        name, val = enum_data

        assert block.columns == ['e']
        assert block.shape == (len(name[enum_size]), 1)

        for expected, actual in zip(
                name[enum_size], block['e'].enum_values):
            assert expected == actual

        for expected, actual in zip(
                val[enum_size], block['e'].values):
            assert expected == actual


class TestColumnAppend(object):
    """Test append singe data item to column."""

    @pytest.mark.parametrize("dtype", DTYPE_TO_CLICKHOUSE.keys())
    def test_numeric(self, dtype):
        c = ch.Column(dtype=dtype)
        data = range(512)
        for i in data:
            c.append(i)

        assert len(data) == len(c)
        arr = np.array(data, dtype=dtype)
        for expected, actual in zip(arr, c):
            assert expected == actual


    def test_date(self):
        with pytest.raises(ValueError):
            ch.Column(dtype='datetime64[m]')

        c = ch.Column(dtype='datetime64[D]')
        data = ['1970-01-01', datetime(1970, 1, 2), np.datetime64('1970-01-03')]
        for d in data:
            c.append(d)

        arr = np.array(
            ['1970-01-01', '1970-01-02', '1970-01-03'],
            dtype='datetime64[D]')
        for expected, actual in zip(arr, c):
            assert expected == actual


    def test_datetime(self):
        c = ch.Column(dtype='datetime64[s]')
        data = ['1970-01-01', datetime(1970, 1, 1, 0, 0, 1), np.datetime64('1970-01-01 00:00:02')]
        for d in data:
            c.append(d)

        arr = np.array(
            ['1970-01-01 00:00:00', '1970-01-01 00:00:01', '1970-01-01 00:00:02'],
            dtype='datetime64[s]')
        for expected, actual in zip(arr, c):
            assert expected == actual


    def test_fixed_string(self):
        c = ch.Column(dtype='S4')
        slist = ['a', 'ab', 'abc', 'abcd', 'abcde', 'abcdef'] * 100
        for s in slist:
            c.append(s)

        arr = np.array(slist, dtype='S4')
        assert len(arr) == len(c)
        for expected, actual in zip(arr, c):
            assert expected == actual


    def test_string(self):
        c = ch.Column(dtype='O')
        slist = ['hello', 'welcome', 'ni hao', 'huan yin'] * 100
        for s in slist:
            c.append(s)

        assert len(slist) == len(c)
        for expected, actual in zip(slist, c):
            assert expected == actual


    @pytest.mark.parametrize("enum_size", [8, 16])
    def test_enum(self, enum_size):
        dtype = 'int{}'.format(enum_size)
        c = ch.Column(
            dtype=dtype,
            enum_item=(('a', 1), ('b', 2), ('c', 3)))

        slist = ['a', 'b', 'c', 1, 2, 3] * 10
        vlist = [1, 2, 3, 1, 2, 3] * 10
        strlist = ['a', 'b', 'c', 'a', 'b', 'c'] * 10
        for s in slist:
            c.append(s)

        assert len(slist) == len(c)
        for expected, actual in zip(vlist, c):
            assert expected, actual

        for expected, actual in zip(strlist, c.enum_values):
            assert expected, actual


class TestBlockAppend(object):
    """Test append array data to block."""

    @pytest.mark.parametrize("dtype", DTYPE_TO_CLICKHOUSE.keys())
    def test_numeric(self, dtype):
        b = ch.Block()
        # test for overriding column content
        b[dtype] = np.arange(10, dtype=dtype)

        b[dtype] = np.arange(100, dtype=dtype)
        assert np.dtype(dtype) == b[dtype].dtype
        assert np.array_equal(np.arange(100, dtype=dtype), b[dtype].values)

        b[dtype] += np.arange(100, 200, dtype=dtype)
        assert np.array_equal(np.arange(200, dtype=dtype), b[dtype].values)


    def test_numeric2(self):
        b = ch.Block()

        for idx, dtype in enumerate(DTYPE_TO_CLICKHOUSE.keys()):
            b[dtype] = np.arange(100, dtype=dtype)
            assert np.dtype(dtype) == b[idx].dtype
            assert np.array_equal(np.arange(100, dtype=dtype), b[idx].values)

            b[dtype] += np.arange(100, 200, dtype=dtype)

        # This also test for Block.__iter__
        for dtype, col in b:
            assert np.dtype(dtype) == col.dtype
            assert np.array_equal(np.arange(200, dtype=dtype), col.values)


    def test_date(self):
        b = ch.Block()

        arr = np.array(
            ['1970-01-01', '1970-01-02', '1970-01-03'],
            dtype='datetime64[D]')
        b['date'] = arr
        assert len(arr) == len(b['date'])
        for expected, actual in zip(arr, b['date']):
            assert expected == actual

        b['date'] += arr
        assert len(arr) * 2 == len(b['date'])
        for expected, actual in zip(np.append(arr, arr), b['date']):
            assert expected == actual


    def test_datetime(self):
        b = ch.Block()

        arr = np.array(
            ['1970-01-01 00:00:00', '1970-01-01 00:00:01', '1970-01-01 00:00:02'],
            dtype='datetime64[s]')
        b['datetime'] = arr
        assert len(arr) == len(b['datetime'])

        b['datetime'] += arr
        assert len(arr) * 2 == len(b['datetime'])
        for expected, actual in zip(np.append(arr, arr), b['datetime']):
            assert expected == actual


    @pytest.mark.parametrize("size", [1, 4, 10])
    def test_fixed_string(self, size):
        b = ch.Block()

        slist = ['foo', 'bar', 'hello', 'world', 'pybind11'] * 100
        dtype = 'S{}'.format(size)
        arr = np.array(slist, dtype=dtype)

        b[dtype] = arr
        assert len(slist) == len(b[dtype])
        assert np.dtype(dtype) == b[dtype].dtype
        assert np.array_equal(arr, b[dtype].values)

        b[dtype] += arr
        assert len(slist) * 2 == len(b[dtype])
        assert np.dtype(dtype) == b[dtype].dtype
        assert np.array_equal(np.append(arr, arr), b[dtype].values)


    def test_string(self):
        b = ch.Block()

        slist = ['hello', 'welcome', 'ni hao', 'huan yin'] * 100

        name = 'str'
        b[name] = np.array(slist, dtype='O')
        assert len(slist) == len(b[name])
        for expected, actual in zip(slist, b[name]):
            assert expected == actual

        # append by list of string
        slist2 = ['konnichiwa', 'youkoso']
        name = 'str2'
        b[name] = ch.Column(dtype='S')
        b[name] += slist
        b[name] += slist2
        lst = slist + slist2
        assert len(lst) == len(b[name])
        for expected, actual in zip(lst, b[name]):
            assert expected == actual


    @pytest.mark.parametrize("enum_size", [8, 16])
    def test_enum(self, enum_size):
        b = ch.Block()

        dtype = 'int{}'.format(enum_size)
        c = ch.Column(
            dtype=dtype,
            enum_item=(('a', 1), ('b', 2), ('c', 3)))

        b['e'] = c
        b['e'] += np.arange(1, 4, dtype=dtype)
        # Note: mixing str and value must start with string.
        b['e'] += ['a', 'b', 3]

        assert len(b['e']) == 6
        vlist = [1, 2, 3] * 2
        strlist = ['a', 'b', 'c'] * 2

        for expected, actual in zip(vlist, b['e']):
            assert expected == actual
        for expected, actual in zip(strlist, b['e'].enum_values):
            assert expected == actual


class TestBlockFromDataFrame(object):
    def test_reuse_with_mismatch_columns(self):
        b = ch.Block()
        df = pd.DataFrame({
            'int8': np.arange(10, dtype='int8'),
            'int16': np.arange(10, dtype='int16'),
        })
        b.from_dataframe(df)

        assert np.array_equal(b['int8'].values, df['int8'].values)

        df2 = pd.DataFrame({
            'int8': np.arange(10, dtype='int16'),
            'int16': np.arange(10, dtype='int16'),
        })
        with pytest.raises(ValueError):
            b.from_dataframe(df2)


    def test_reuse(self):
        b = ch.Block()
        df = pd.DataFrame({
            'int8': np.arange(10, dtype='int8'),
            'int16': np.arange(10, dtype='int16'),
        })
        b.from_dataframe(df)

        df = pd.DataFrame({
            'int8': np.arange(10, 20, dtype='int8'),
            'int16': np.arange(10, 20, dtype='int16'),
        })
        b.from_dataframe(df)
        assert np.array_equal(b['int8'].values, df['int8'].values)
        assert np.array_equal(b['int16'].values, df['int16'].values)


class TestClientInsert(object):
    # Only test insert for numeric and string, the underlying Client::Insert
    # function in clickhouse-cpp is tested in it's unit test.

    client = ch.Client()

    def prepare_table(self, tablename):
        self.client.execute('CREATE DATABASE IF NOT EXISTS test_pybind')
        self.client.execute('DROP TABLE IF EXISTS {}'.format(tablename))

    def test_numeric(self):
        tablename = 'test_pybind.insert_numeric'
        self.prepare_table(tablename)

        b = ch.Block()
        start = 0
        row = 1024
        dtypes = ['int8', 'uint16', 'int32', 'uint64', 'float32', 'float64']
        for dtype in dtypes:
            b[dtype] = np.arange(start, start + row, dtype=dtype)
            start += row

        self.client.execute(
            'CREATE TABLE {} ({}) ENGINE = Memory'.format(
                tablename, b.column_definition()))

        self.client.insert(tablename, b)

        # Select and return a new Block.
        b = self.client.select('SELECT * from {}'.format(tablename))
        start = 0
        row = 1024
        dtypes = ['int8', 'uint16', 'int32', 'uint64', 'float32', 'float64']
        for dtype in dtypes:
            expected = np.arange(start, start + row, dtype=dtype)
            assert np.array_equal(expected, b[dtype].values)
            start += row


    def test_string(self):
        tablename = 'test_pybind.insert_string'
        c = 'string'
        self.prepare_table(tablename)

        slist = ['hello', 'welcome', 'ni hao', 'huan yin', 'konnichiwa', 'youkoso'] * 100

        b = ch.Block()
        b[c] = ch.Column(dtype='S')
        b[c] += slist

        self.client.execute(
            'CREATE TABLE {} ({}) ENGINE = Memory'.format(
                tablename, b.column_definition()))

        self.client.insert(tablename, b)

        # Select and return a new Block.
        b = self.client.select('SELECT * from {}'.format(tablename))
        for expected, actual in zip(slist, b[c].values):
            assert expected == actual

