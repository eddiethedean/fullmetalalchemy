"""
Functions for updating records in SQL tables.
"""

import typing as _t

import sqlalchemy as _sa
import sqlalchemy.orm.session as _sa_session
import sqlalchemy.engine as _sa_engine

import fullmetalalchemy.records as _records
import fullmetalalchemy.types as _types
import fullmetalalchemy.features as _features
import fullmetalalchemy.exceptions as _ex


def update_matching_records_session(
    sa_table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    match_column_names: _t.Sequence[str],
    session: _sa_session.Session
) -> None:
    """
    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table = fa.features.get_table('xy', engine)
    >>> updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
    >>> session = fa.features.get_session(engine)
    >>> fa.update.update_matching_records_session(table, updated_records, ['id'], session)
    >>> session.commit()
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 11, 'y': 2},
    ... {'id': 2, 'x': 2, 'y': 4},
    ... {'id': 3, 'x': 4, 'y': 8},
    ... {'id': 4, 'x': 8, 'y': 111}]
    """
    sa_table = _features.str_to_table(sa_table, session)
    match_values = [_records.filter_record(record, match_column_names) for record in records]
    for values, record in zip(match_values, records):
        stmt = _make_update_statement(sa_table, values, record)
        session.execute(stmt)


def update_matching_records(
    sa_table:_t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    match_column_names: _t.Sequence[str],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table = fa.features.get_table('xy', engine)
    >>> updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
    >>> fa.update.update_matching_records(table, updated_records, ['id'], engine)
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 11, 'y': 2},
    ... {'id': 2, 'x': 2, 'y': 4},
    ... {'id': 3, 'x': 4, 'y': 8},
    ... {'id': 4, 'x': 8, 'y': 111}]
    """
    sa_table, engine = _ex.convert_table_engine(sa_table, engine)
    session = _features.get_session(engine)
    try:
        update_matching_records_session(sa_table, records, match_column_names, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def update_records_session(
    sa_table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _sa_session.Session,
    match_column_names: _t.Optional[_t.Sequence[str]] = None,
) -> None:
    """
    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table = fa.features.get_table('xy', engine)
    >>> updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
    >>> session = fa.features.get_session(engine)
    >>> fa.update.update_records_session(table, updated_records, session)
    >>> session.commit()
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 11, 'y': 2},
    ... {'id': 2, 'x': 2, 'y': 4},
    ... {'id': 3, 'x': 4, 'y': 8},
    ... {'id': 4, 'x': 8, 'y': 111}]
    """
    sa_table = _features.str_to_table(sa_table, session)
    if _features.missing_primary_key(sa_table):
        if match_column_names is None:
            raise ValueError('Must provide match_column_names if table has no primary key.')
        update_matching_records_session(sa_table, records, match_column_names, session)
    else:
        _update_records_fast_session(sa_table, records, session)


def update_records(
    sa_table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    engine: _t.Optional[_sa_engine.Engine] = None,
    match_column_names: _t.Optional[_t.Sequence[str]] = None,
) -> None:
    """
    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table = fa.features.get_table('xy', engine)
    >>> updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
    >>> fa.update.update_records(table, updated_records, engine)
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 11, 'y': 2},
    ... {'id': 2, 'x': 2, 'y': 4},
    ... {'id': 3, 'x': 4, 'y': 8},
    ... {'id': 4, 'x': 8, 'y': 111}]
    """
    sa_table, engine = _ex.convert_table_engine(sa_table, engine)
    session = _features.get_session(engine)
    try:
        update_records_session(sa_table, records, session, match_column_names)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def _update_records_fast_session(
    sa_table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _sa_session.Session
) -> None:
    """Fast update needs primary key."""
    sa_table = _features.str_to_table(sa_table, session)
    table_name = sa_table.name
    table_class = _features.get_class(table_name, session, schema=sa_table.schema)
    mapper = _sa.inspect(table_class)
    session.bulk_update_mappings(mapper, records)


def _make_update_statement(table, record_values, new_values):
    up = _sa.update(table)
    for col, val in record_values.items():
        up = up.where(table.c[col]==val)
    return up.values(**new_values)


def _make_update_statement_column_value(
    table: _sa.Table,
    column_name: str,
    value: _t.Any
):
    new_value = {column_name: value}
    return _sa.update(table).values(**new_value)


def set_column_values_session(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    value: _t.Any,
    session: _sa_session.Session
) -> None:
    """
    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table = fa.features.get_table('xy', engine)
    >>> new_value = 1
    >>> session = fa.features.get_session(engine)
    >>> fa.update.set_column_values_session(table, 'x', new_value, session)
    >>> session.commit()
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 1, 'y': 4},
     {'id': 3, 'x': 1, 'y': 8},
     {'id': 4, 'x': 1, 'y': 11}]
    """
    sa_table = _features.str_to_table(sa_table, session)
    stmt = _make_update_statement_column_value(sa_table, column_name, value)
    session.execute(stmt)


def set_column_values(
    sa_table: _t.Union[_sa.Table, str],
    column_name: str,
    value: _t.Any,
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table = fa.features.get_table('xy', engine)
    >>> new_value = 1
    >>> fa.update.set_column_values(table, 'x', new_value, engine)
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 1, 'y': 4},
     {'id': 3, 'x': 1, 'y': 8},
     {'id': 4, 'x': 1, 'y': 11}]
    """
    sa_table, engine = _ex.convert_table_engine(sa_table, engine)
    session = _features.get_session(engine)
    try:
        set_column_values_session(sa_table, column_name, value, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e