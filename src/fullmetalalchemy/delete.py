"""
Functions for deleting records from SQL tables.
"""

import typing as _t

import sqlalchemy as _sa
import sqlalchemy.engine as _sa_engine
import sqlalchemy.orm.session as _sa_session
from sqlalchemy.sql.expression import Select as _Select

import fullmetalalchemy.features as _features
import fullmetalalchemy.types as _types
import fullmetalalchemy.exceptions as _ex


def delete_records_session(
    table: _t.Union[_sa.Table, str],
    column_name: str,
    values: _t.Sequence,
    session: _sa_session.Session
) -> None:
    """
    Delete records from SQL table that match passed values in column.
    Adds deletes to passed session.

    Parameters
    ----------
    table : sa.Table | str
        SqlAlchemy Table or table name
    column_name : str
        SQL table column name to match values
    values : Sequence
        values to match in SQL table column
    session : SqlAlchemy Session
        SqlAlchemy connection session

    Returns
    -------
    None

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]

    >>> session = sz.features.get_session(engine)
    >>> delete_records_session(table, 'id', [1], session)
    >>> session.commit()
    >>> sz.select.select_records_all(table)
    [{'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]

    See Also
    --------
    fullmetalalchemy.delete.delete_records_by_values_session
    """
    table = _features.str_to_table(table, session)
    col = _features.get_column(table, column_name)
    session.query(table).filter(col.in_(values)).delete(synchronize_session=False)

 
def delete_records(
    table: _t.Union[_sa.Table, str],
    column_name: str,
    values: _t.Sequence,
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Delete records from SQL table that match passed values in column.

    Parameters
    ----------
    table : sa.Table | str
        SqlAlchemy Table or name of SQL table
    column_name : str
        SQL table column name to match values
    values : Sequence
        values to match in SQL table column
    engine : SqlAlchemy Engine
        SqlAlchemy connection engine

    Returns
    -------
    None

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]

    >>> sz.delete.delete_records(table, 'id', [1])
    >>> sz.select.select_records_all(table)
    [{'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]

    See Also
    --------
    fullmetalalchemy.delete.delete_records_by_values
    """
    table, engine = _ex.convert_table_engine(table, engine)
    session = _features.get_session(engine)
    delete_records_session(table, column_name, values, session)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def delete_records_by_values(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[dict],
    engine: _t.Optional[_sa.engine.Engine] = None
) -> None:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]

    >>> sz.delete.delete_records_by_values(table, [{'id': 3}, {'x': 2}], engine)
    >>> sz.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 4, 'x': 8, 'y': 11}]
    """
    table, engine = _ex.convert_table_engine(table, engine)
    session = _features.get_session(engine)
    try:
        delete_records_by_values_session(table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def delete_record_by_values_session(
    table: _t.Union[_sa.Table, str],
    record: _types.Record,
    session: _sa_session.Session
) -> None:
    table = _features.str_to_table(table, session)
    delete = _build_delete_from_record(table, record)
    session.execute(delete)


def delete_records_by_values_session(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _sa_session.Session
) -> None:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]

    >>> session = sz.features.get_session(engine)
    >>> sz.delete.delete_records_by_values_session(table, [{'id': 3}, {'x': 2}], session)
    >>> session.commit()
    >>> sz.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 4, 'x': 8, 'y': 11}]
    """
    table = _features.str_to_table(table, session)
    for record in records:
        delete_record_by_values_session(table, record, session)

        
def _build_where_from_record(
    table: _sa.Table,
    record: _types.Record
) -> _Select:
    s = _sa.select(table)
    for col, val in record.items():
        s = s.where(table.c[col]==val)
    return s


def _build_delete_from_record(
    table: _sa.Table,
    record: _types.Record
) -> _sa.sql.Delete:
    d = _sa.delete(table)
    for column, value in record.items():
        d = d.where(table.c[column]==value)
    return d


def delete_all_records_session(
    table: _t.Union[_sa.Table, str],
    session: _sa_session.Session
) -> None:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2}, {'id': 2, 'x': 2, 'y': 4}]

    >>> session = sz.features.get_session(engine)
    >>> sz.delete.delete_all_records_session(table, session)
    >>> session.commit()
    >>> sz.select.select_records_all(table)
    []
    """
    table = _features.str_to_table(table, session)
    query = _sa.delete(table)
    session.execute(query)


def delete_all_records(
    table: _t.Union[_sa.Table, str],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2}, {'id': 2, 'x': 2, 'y': 4}]

    >>> sz.delete.delete_all_records(table)
    >>> sz.select.select_records_all(table)
    []
    """
    table, engine = _ex.convert_table_engine(table, engine)
    session = _features.get_session(engine)
    try:
        delete_all_records_session(table, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e