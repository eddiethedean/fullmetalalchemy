"""
Functions for selecting records from SQL tables.
"""

import typing as _t

import sqlalchemy as _sa
import sqlalchemy.sql.elements as sa_elements

import fullmetalalchemy.types as _types
import fullmetalalchemy.features as _features
import fullmetalalchemy.exceptions as exceptions
import fullmetalalchemy.exceptions as _ex


def select_records_all(
    sa_table: _t.Union[_sa.Table, str],
    connection: _t.Optional[_types.SqlConnection] = None,
    sorted: bool = False,
    include_columns: _t.Optional[_t.Sequence[str]] = None
) ->  _t.List[_types.Record]:
    """
    Select all records from table.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_records_all(table, engine)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    if include_columns is not None:
        columns = [_features.get_column(sa_table, column_name) for column_name in include_columns]
        query = _sa.select(*columns)
    else:
        query = _sa.select(sa_table)

    if sorted:
        query = query.order_by(*_features.primary_key_columns(sa_table))
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_records_chunks(
    sa_table:_t.Union[_sa.Table, str],
    connection: _t.Optional[_types.SqlConnection] = None,
    chunksize: int = 2,
    sorted: bool = False,
    include_columns: _t.Optional[_t.Sequence[str]] = None
) -> _t.Generator[ _t.List[_types.Record], None, None]:
    """
    Select chunks of records from table.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> records_chunks = fa.select.select_records_chunks(table, engine)
    >>> next(records_chunks)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4}]
    >>> next(records_chunks)
    [{'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    if include_columns is not None:
        columns = [_features.get_column(sa_table, column_name) for column_name in include_columns]
        query = _sa.select(*columns)
    else:
        query = _sa.select(sa_table)

    if sorted:
        query = query.order_by(*_features.primary_key_columns(sa_table))
    stream = connection.execute(query, execution_options={'stream_results': True})
    for results in stream.partitions(chunksize):
        yield [dict(r) for r in results]


def select_existing_values(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    values: _t.Sequence,
    connection: _t.Optional[_types.SqlConnection] = None
) -> list:
    """
    Select values that exist in named column that are
    also in passed values.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> values = [1, 2, 3, 4, 5]
    >>> fa.select.select_existing_values(table, 'x', values, engine)
    [1, 2, 4]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    column = _features.get_column(sa_table, column_name)
    query = _sa.select([column]).where(column.in_(values))
    connection = _ex.check_for_engine(sa_table, connection)
    return connection.execute(query).scalars().fetchall()


def select_column_values_all(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    connection: _t.Optional[_types.SqlConnection] = None
) -> list:
    """
    Select all values in named column.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_column_values_all(table, 'x', engine)
    [1, 2, 4, 8]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    query = _sa.select(_features.get_column(sa_table, column_name))
    connection = _ex.check_for_engine(sa_table, connection)
    return connection.execute(query).scalars().all()


def select_column_values_chunks(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    chunksize: int,
    connection: _t.Optional[_types.SqlConnection] = None,
) -> _t.Generator[list, None, None]:
    """
    Select chunks of values in named column.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> col_chunks = fa.select.select_column_values_chunks(table, engine, 'x', 2)
    >>> next(col_chunks)
    [1, 2]
    >>> next(col_chunks)
    [4, 8]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    query = _sa.select(_features.get_column(sa_table, column_name))
    stream = connection.execute(query, execution_options={'stream_results': True})
    for results in stream.scalars().partitions(chunksize):  # type: ignore
        yield results


def select_records_slice(
    sa_table: _t.Union[_sa.Table, str],
    start: _t.Optional[int] = None,
    stop: _t.Optional[int] = None,
    connection: _t.Optional[_types.SqlConnection] = None,
    sorted: bool = False,
    include_columns: _t.Optional[_t.Sequence[str]] = None
) ->  _t.List[_types.Record]:
    """
    Select a slice of records from table.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_records_slice(table, engine, start=1, stop=3)
    [{'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8}]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    start, stop = _convert_slice_indexes(sa_table, connection, start, stop)
    if stop < start:
        raise exceptions.SliceError('stop cannot be less than start.')
    if include_columns is not None:
        columns = [_features.get_column(sa_table, column_name) for column_name in include_columns]
        query = _sa.select(*columns)
    else:
        query = _sa.select(sa_table)
    if sorted:
        query = query.order_by(*_features.primary_key_columns(sa_table))
    query = query.slice(start, stop)
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_column_values_by_slice(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    start: _t.Optional[int] = None,
    stop: _t.Optional[int] = None,
    connection: _t.Optional[_types.SqlConnection] = None,
) -> list:
    """
    Select a slice of column values from table.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_column_values_by_slice(table, engine, 'y', start=1, stop=3)
    [4, 8]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    start, stop = _convert_slice_indexes(sa_table, connection, start, stop)
    if stop < start:
        raise exceptions.SliceError('stop cannot be less than start.')
    query = _sa.select(_features.get_column(sa_table, column_name)).slice(start, stop)
    return connection.execute(query).scalars().all()


def select_column_value_by_index(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    index: int,
    connection: _t.Optional[_types.SqlConnection] = None
) -> _t.Any:
    """
    Select a column value from table by index number.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_column_value_by_index(table, engine, 'y', 2)
    8
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    if index < 0:
        row_count = _features.get_row_count(sa_table, connection)
        if index < -row_count:
            raise IndexError('Index out of range.') 
        index = _calc_positive_index(index, row_count)
    query = _sa.select(_features.get_column(sa_table, column_name)).slice(index, index+1)
    return connection.execute(query).scalars().all()[0]


def select_record_by_index(
    sa_table: _t.Union[_sa.Table, str],
    index: int,
    connection: _t.Optional[_types.SqlConnection] = None,
) -> _t.Dict[str, _t.Any]:
    """
    Select a record from table by index number.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_record_by_index(table, 2, engine)
    {'id': 3, 'x': 4, 'y': 8}
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    if index < 0:
        row_count = _features.get_row_count(sa_table, connection)
        if index < -row_count:
            raise IndexError('Index out of range.') 
        index = _calc_positive_index(index, row_count)
    query = _sa.select(sa_table).slice(index, index+1)
    results = connection.execute(query)
    return [dict(x) for x in results][0]


def select_primary_key_records_by_slice(
    sa_table: _t.Union[_sa.Table, str],
    _slice: slice,
    connection: _t.Optional[_types.SqlConnection] = None,
    sorted: bool = False
) ->  _t.List[_types.Record]:
    """
    Select a slice of primary key records from table.
    Records only have primary key values.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_primary_key_records_by_slice(table, engine, slice(1, 3))
    [{'id': 2}, {'id': 3}]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    start = _slice.start
    stop = _slice.stop
    start, stop = _convert_slice_indexes(sa_table, connection, start, stop)
    if stop < start:
        raise exceptions.SliceError('stop cannot be less than start.')
    primary_key_values = _features.primary_key_columns(sa_table)
    if sorted:
        query = _sa.select(primary_key_values).order_by(*primary_key_values).slice(start, stop)
    else:
        query = _sa.select(primary_key_values).slice(start, stop)
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_record_by_primary_key(
    sa_table: _t.Union[_sa.Table, str],
    primary_key_value: _types.Record,
    connection: _t.Optional[_types.SqlConnection] = None,
    include_columns: _t.Optional[_t.Sequence[str]] = None
) -> _types.Record:
    """
    Select the first record that matches passed primary key values.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_record_by_primary_key(table, engine, {'id': 3})
    {'id': 3, 'x': 4, 'y': 8}
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    # TODO: check if primary key values exist
    where_clause = _features._get_where_clause(sa_table, primary_key_value)
    if len(where_clause) == 0:
        raise exceptions.MissingPrimaryKey('Primary key values missing in table.')
    if include_columns is not None:
        columns = [_features.get_column(sa_table, column_name) for column_name in include_columns]
        query = _sa.select(*columns).where((sa_elements.and_(*where_clause)))
    else:
        query = _sa.select(sa_table).where((sa_elements.and_(*where_clause)))
    results = connection.execute(query)
    results = [dict(x) for x in results]
    if len(results) == 0:
        raise exceptions.MissingPrimaryKey('Primary key values missing in table.')
    return results[0]


def select_records_by_primary_keys(
    sa_table: _t.Union[_sa.Table, str],
    primary_keys_values: _t.Sequence[_types.Record],
    connection: _t.Optional[_types.SqlConnection] = None,
    schema: _t.Optional[str] = None,
    include_columns: _t.Optional[_t.Sequence[str]] = None
) ->  _t.List[_types.Record]:
    """
    Select all the records that matches passed primary key values.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_records_by_primary_keys(table, engine, [{'id': 3}, {'id': 1}])
    [{'id': 1, 'x': 1, 'y': 2}, {'id': 3, 'x': 4, 'y': 8}]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    # TODO: check if primary key values exist
    where_clauses = []
    for record in primary_keys_values:
        where_clause = _features._get_where_clause(sa_table, record)
        where_clauses.append(sa_elements.and_(*where_clause))
    if len(where_clauses) == 0:
        return []
    if include_columns is not None:
        columns = [_features.get_column(sa_table, column_name) for column_name in include_columns]
        query = _sa.select(*columns).where((sa_elements.or_(*where_clauses)))
    else:
        query = _sa.select(sa_table).where((sa_elements.or_(*where_clauses)))
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_column_values_by_primary_keys(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    primary_keys_values: _t.Sequence[_types.Record],
    connection: _t.Optional[_types.SqlConnection] = None
) -> list:
    """
    Select all the column value that matches passed primary key values.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_column_values_by_primary_keys(table, engine, 'y', [{'id': 3}, {'id': 1}])
    [2, 8]
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    # TODO: check if primary key values exist
    where_clauses = []
    for record in primary_keys_values:
        where_clause = _features._get_where_clause(sa_table, record)
        where_clauses.append(sa_elements.and_(*where_clause))

    if len(where_clauses) == 0:
        return []
    query = _sa.select(_features.get_column(sa_table, column_name)).where((sa_elements.or_(*where_clauses)))
    results = connection.execute(query)
    return results.scalars().fetchall()


def select_value_by_primary_keys(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    primary_key_value: _types.Record,
    connection: _t.Optional[_types.SqlConnection] = None,
    schema: _t.Optional[str] = None
) -> _t.Any:
    """
    Select the first column value that matches passed primary key values.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_value_by_primary_keys(table, engine, 'y', {'id': 3})
    8
    """
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    # TODO: check if primary key values exist
    where_clause = _features._get_where_clause(sa_table, primary_key_value)
    if len(where_clause) == 0:
        raise KeyError('No such primary key values exist in table.')
    query = _sa.select(_features.get_column(sa_table, column_name)).where((sa_elements.and_(*where_clause)))
    return connection.execute(query).scalars().all()[0]


def _convert_slice_indexes(
    sa_table: _t.Union[_sa.Table, str],
    connection: _types.SqlConnection,
    start: _t.Optional[int] = None,
    stop: _t.Optional[int] = None
) -> _t.Tuple[int, int]:
    sa_table, connection = _ex.convert_table_connection(sa_table, connection)
    # start index is 0 if None
    start = 0 if start is None else start
    row_count = _features.get_row_count(sa_table, connection)
    
    # stop index is row count if None
    stop = row_count if stop is None else stop
    # convert negative indexes
    start = _calc_positive_index(start, row_count)
    start = _stop_underflow_index(start, row_count)
    stop = _calc_positive_index(stop, row_count)
    stop = _stop_overflow_index(stop, row_count)

    if row_count == 0:
        return 0, 0

    return start, stop


def _calc_positive_index(
    index: int,
    row_count: int
) -> int:
    # convert negative index to real index
    if index < 0:
        index = row_count + index
    return index


def _stop_overflow_index(
    index: int,
    row_count: int
) -> int:
    if index > row_count - 1:
        return row_count
    return index

    
def _stop_underflow_index(
    index: int,
    row_count: int
) -> int:
    if index < 0 and index < -row_count:
        return 0
    return index