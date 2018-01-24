import collections
import numbers

import numpy as np

import _clickhouse

CompressionMethod = _clickhouse.CompressionMethod
ClientOptions = _clickhouse.ClientOptions


class Column(object):
    def __init__(self, colref=None, dtype=None, enum_item=None):
        """Create a Column object which wraps the underlying ColumnRef in
        clickhouse-cpp.

        enum_item: iterable with each element having 2 elements,
                     e.g. [(name1, value1), (name1, value2), ...]
        """
        if colref is not None:
            self.colref = colref
        elif dtype is not None:
            if isinstance(dtype, str):
                dtype = np.dtype(dtype)
            self.colref = _clickhouse.Column(dtype, enum_item)
        else:
            raise ValueError(
                'must provide either Column or dtype to create Column')

        self._cache = None
        self._enum_cache = None


    def __len__(self):
        return len(self.colref)


    def __iter__(self):
        return iter(self.values)


    @property
    def type(self):
        return self.colref.type()


    @property
    def dtype(self):
        return self.values.dtype


    def clear(self):
        """Remove all data in this column."""
        self.colref.clear()


    def _values(self, enum_as_str):
        """Return column data as numpy ndarray, string list or list of ndarray
        according to column data type.

        For numeric type, Date, DateTime and FixedString, return data as numpy array.
        For string, return string list.
        For Array, only numeric type is supported, return list of numpy array.

        enum_as_str: if True, return Enum8 and Enum16 as string list; return
                     numeric ndarray otherwise
        """
        type_code = self.type.code()

        if type_code == _clickhouse.TypeCode.String:
            arr = self.colref.as_str()
        elif type_code == _clickhouse.TypeCode.Date:
            arr = np.array(self.colref, copy=False).astype(dtype='datetime64[D]')
        elif type_code == _clickhouse.TypeCode.DateTime:
            arr = np.array(self.colref, copy=False).astype(dtype='datetime64[s]')
        elif type_code == _clickhouse.TypeCode.Array:
            arr = [np.array(self.colref.get_arr(i), copy=False)
                   for i in xrange(len(self.colref))]
        elif enum_as_str and \
                (type_code == _clickhouse.TypeCode.Enum8 or
                 type_code == _clickhouse.TypeCode.Enum16):
            arr = self.colref.as_enum_str()
        else:
            arr = np.array(self.colref, copy=False)

        return arr


    @property
    def values(self):
        if self._cache is None:
            self._cache = self._values(enum_as_str=False)
        return self._cache


    def is_enum(self):
        return self.type.code() in \
            (_clickhouse.TypeCode.Enum8, _clickhouse.TypeCode.Enum16)


    @property
    def enum_values(self):
        """Get enum value as their name, returns string list."""
        if not self.is_enum():
            raise ValueError('column is not Enum')

        if self._enum_cache is None:
            self._enum_cache = self._values(enum_as_str=True)
        return self._enum_cache


    def __getitem__(self, idx):
        return self.values[idx]


    def _append(self, data):
        type_code = self.type.code()
        if type_code in (_clickhouse.TypeCode.Date,
                         _clickhouse.TypeCode.DateTime):
            data = np.datetime64(data).astype('datetime64[s]').astype('uint64')
        self.colref.append(data)


    def _extend(self, data):
        if len(data) == 0:
            return

        type_code = self.type.code()
        if type_code == _clickhouse.TypeCode.String:
            if not isinstance(data[0], str):
                raise ValueError('data[0] is not str instance {} {}'.format(
                    data[0], data[0].__class__))
            # Convert string subtype to str.
            if data[0].__class__ is not str:
                data = [str(d) for d in data]
            self.colref.append_strarray(data)
        elif type_code in (_clickhouse.TypeCode.Enum8,
                           _clickhouse.TypeCode.Enum16) and \
            isinstance(data[0], str):
            # For enum array with string value, append one by one.
            for d in data:
                self.colref.append(d)
        elif data.__class__ is np.ndarray:
            if type_code in (_clickhouse.TypeCode.Date,
                             _clickhouse.TypeCode.DateTime):
                data = data.astype('datetime64[s]').astype('uint64')
            self.colref.append_ndarray(data)
        else:
            for d in data:
                self.colref.append(d)


    def append(self, data):
        """Append data to column.

        data: valid types: single object, iterable of string for ColumnString,
              ndarray for all other column types
        """
        if isinstance(data, str):
            if data.__class__ is not str:
                data = str(data)
            self._append(data)
        elif not isinstance(data, collections.Iterable):
            self._append(data)
        else:
            self._extend(data)

        self._cache = None
        self._enum_cache = None


    def __iadd__(self, data):
        self.append(data)
        return self


class Block(object):
    """Wrapper for clickhouse-cpp Block."""

    def __init__(self, block=None):
        """Create a new Block object.

        block: clickhouse-cpp Block instance, create a new instance if None
        """
        self.block = _clickhouse.Block() if block is None else block
        self._column_idx = collections.OrderedDict()
        self._column = []
        self._update()


    def _update(self):
        cols = self.block.cols()
        self._column_idx.clear()
        for i in range(cols):
            name = self.block.name(i)
            self._column_idx[name] = i
            self._column.append(Column(colref=self.block[i]))


    @property
    def rows(self):
        """Return number of rows."""
        return self.block.rows()


    @property
    def cols(self):
        """Return number of columns."""
        return self.block.cols()


    @property
    def shape(self):
        """Return shape as (rows, cols)."""
        return (self.block.rows(), self.block.cols())


    @property
    def columns(self):
        """Return column names."""
        return self._column_idx.keys()


    def _get_idx(self, index_or_name):
        idx = None
        if isinstance(index_or_name, numbers.Number):
            idx = index_or_name
        else:
            idx = self._column_idx.get(index_or_name)

        return idx


    def __repr__(self):
        return self.block.__repr__()


    def __iter__(self):
        for name, idx in self._column_idx.iteritems():
            yield name, self[idx]


    def __getitem__(self, index_or_name):
        """Get column at given index or name.

        index_or_name: integer index to nth column, or name of column.
        """
        idx = self._get_idx(index_or_name)
        if idx is None:
            raise ValueError('no such column {}'.format(index_or_name))

        return self._column[idx]


    def __setitem__(self, index_or_name, data):
        """Create or replace column with given data or column.

        index_or_name: column index as integer or column name, for adding new
                       column, must be column name
        data: for numeric and fixed string column, must pass numpy ndarray of
              correct type; for string column, any iterable for string; Column
              can also be used when createing new column
        """
        idx = self._get_idx(index_or_name)
        if idx is None:
            if data.__class__ is Column:
                column = data
            else:
                # Create new column.
                column = Column(dtype=data.dtype)

            self.block.append_column(index_or_name, column.colref)

            idx = len(self._column)
            self._column.append(column)
            self._column_idx[index_or_name] = idx
        else:
            column = self._column[idx]
            if column is data:
                # block[idx] += data lead to this
                return
            if data.__class__ is Column:
                raise ValueError(
                    'set Column for existing column is not allowed: {}'.format(
                        index_or_name))

        column.clear()
        if len(data) > 0:
            column.append(data)


    def to_dataframe(self):
        import pandas as pd
        return pd.DataFrame.from_items(
            zip(self.columns, (self[i].values for i in xrange(self.block.cols()))))


    def from_dataframe(self, df):
        """Fill Block columns with data from pandas DataFrame.

        For Block already have columns, data is completely replaced with data
        from DataFrame and column data type must match.
        """
        if len(self.columns) == 0:
            # Empty Block, create new columns.
            for c in df.columns:
                # XXX without .values call to convert to numpy ndarray,
                # Column.extend will call append for each item one by one.
                self[c] = df[c].values
            return

        if len(df.columns) != len(self.columns):
            raise ValueError(
                'from_dataframe reuse block must have same columns')
        for c in df.columns:
            if df[c].dtype != self[c].dtype:
                raise ValueError(
                    'from_dataframe reuse block column {} type mismatch {} {}'.format(
                    c, df[c].dtype, self[c].dtype))
            self[c].clear()
            self[c] += df[c].values


    def column_definition(self):
        """Return column definition that can be used to create table."""
        coldef = ['{} {}'.format(name, col.type.name()) for name, col in self]
        return ', '.join(coldef)


class Client(object):
    """Wrapper for clickhouse-cpp Client."""

    def __init__(self,
                 host='127.0.0.1', port=9000, user='default', password='',
                 option=None):
        """Create a new ClickHouse client with given option.

        host: ClickHouse server host name or ip address
        port: ClickHouse server port number
        user: user name
        password: password for user
        option: ClientOptions, use this if you need to specify options other
                than the common ones
        """
        if option is None:
            option = _clickhouse.ClientOptions(
                host=host, port=port, user=user, password=password)
        self.client = _clickhouse.Client(option)


    def execute(self, sql):
        """Execute arbitrary SQL query."""
        self.client.execute(sql)


    def select(self, sql, block=None):
        """Execute SELECT query.

        block: Block, reuse block to hold select result if not None.
               Note Block column type must match with those in SELECT query.
        """
        if block is None:
            block = Block(block=_clickhouse.Block())

        self.client.select(sql, block.block)
        block._update()
        return block


    def insert(self, db_table, block):
        """Insert block data into given table.

        db_table: table name in form of database.table
        block: Block object
        """
        self.client.insert(db_table, block.block)

