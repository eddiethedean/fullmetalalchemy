"""
Functions for inserting records into SQL tables.
"""

import typing as _t

import sqlalchemy as _sa
import sqlalchemy.orm.session as _sa_session
import sqlalchemy.engine as _sa_engine

import fullmetalalchemy.types as _types
import fullmetalalchemy.features as _features
import fullmetalalchemy.exceptions as _ex


def insert_from_table_session(
    sa_table1: _t.Union[_sa.Table, str],
    sa_table2: _t.Union[_sa.Table, str],
    session: _sa_session.Session
) -> None:
    """
    Insert all records from one table to another.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table1 = fa.features.get_table('xy', engine)
    >>> table2 = fa.features.get_table('xyz', engine)
    >>> fa.select.select_records_all(table2)
    []

    >>> session = fa.features.get_session(engine)
    >>> fa.insert.insert_from_table_session(table1, table2, session)
    >>> session.commit()
    >>> fa.select.select_records_all(table2)
    [{'id': 1, 'x': 1, 'y': 2, 'z': None},
     {'id': 2, 'x': 2, 'y': 4, 'z': None},
     {'id': 3, 'x': 4, 'y': 8, 'z': None},
     {'id': 4, 'x': 8, 'y': 11, 'z': None}]
    """
    sa_table1 = _features.str_to_table(sa_table1, session)
    sa_table2 = _features.str_to_table(sa_table2, session)
    session.execute(sa_table2.insert().from_select(sa_table1.columns.keys(), sa_table1))


def insert_from_table(
    sa_table1: _t.Union[_sa.Table, str],
    sa_table2: _t.Union[_sa.Table, str],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Insert all records from one table to another.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table1 = fa.features.get_table('xy', engine)
    >>> table2 = fa.features.get_table('xyz', engine)
    >>> fa.select.select_records_all(table2)
    []
    
    >>> fa.insert.insert_from_table(table1, table2, engine)
    >>> fa.select.select_records_all(table2)
    [{'id': 1, 'x': 1, 'y': 2, 'z': None},
     {'id': 2, 'x': 2, 'y': 4, 'z': None},
     {'id': 3, 'x': 4, 'y': 8, 'z': None},
     {'id': 4, 'x': 8, 'y': 11, 'z': None}]
    """
    engine = _ex.check_for_engine(sa_table1, engine)
    session = _features.get_session(engine)
    try:
        insert_from_table_session(sa_table1, sa_table2, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    

def insert_records_session(
    sa_table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _sa_session.Session
) -> None:
    """
    Insert records into table.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table = fa.features.get_table('xy', engine)

    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]
    
    >>> new_records = [{'id': 5, 'x': 11, 'y': 5}, {'id': 6, 'x': 9, 'y': 9}]
    >>> session = fa.features.get_session(engine)
    >>> fa.insert.insert_records_session(table, new_records, session)
    >>> session.commit()
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11},
     {'id': 5, 'x': 11, 'y': 5},
     {'id': 6, 'x': 9, 'y': 9}]
    """
    sa_table = _features.str_to_table(sa_table, session)
    if _features.missing_primary_key(sa_table):
        _insert_records_slow_session(sa_table, records, session)
    else:
        _insert_records_fast_session(sa_table, records, session)


def insert_records(
    sa_table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Insert records into table.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table = fa.features.get_table('xy', engine)

    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]
    
    >>> new_records = [{'id': 5, 'x': 11, 'y': 5}, {'id': 6, 'x': 9, 'y': 9}]
    >>> fa.insert.insert_records(table, new_records, engine)
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11},
     {'id': 5, 'x': 11, 'y': 5},
     {'id': 6, 'x': 9, 'y': 9}]
    """
    sa_table, engine = _ex.convert_table_engine(sa_table, engine)
    session = _features.get_session(engine)
    try:
        insert_records_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def _insert_records_fast(
    sa_table: _sa.Table,
    records: _t.Sequence[_types.Record],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """Fast insert needs primary key."""
    if _features.missing_primary_key(sa_table):
        raise _ex.MissingPrimaryKey()
    engine = _ex.check_for_engine(sa_table, engine)
    session = _features.get_session(engine)
    try:
        _insert_records_fast_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def _insert_records_fast_session(
    sa_table: _sa.Table,
    records: _t.Sequence[_types.Record],
    session: _sa_session.Session
) -> None:
    """Fast insert needs primary key."""
    if _features.missing_primary_key(sa_table):
        raise _ex.MissingPrimaryKey()
    table_class = _features.get_class(sa_table.name, session, schema=sa_table.schema)
    mapper = _sa.inspect(table_class)
    session.bulk_insert_mappings(mapper, records)


def _insert_records_slow_session(
    sa_table: _sa.Table,
    records: _t.Sequence[_types.Record],
    session: _sa_session.Session
) -> None:
    """Slow insert does not need primary key."""
    session.execute(sa_table.insert(), records)


def _insert_records_slow(
    sa_table: _sa.Table,
    records: _t.Sequence[_types.Record],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """Slow insert does not need primary key."""
    engine = _ex.check_for_engine(sa_table, engine)
    session = _features.get_session(engine)
    try:
        _insert_records_slow_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e