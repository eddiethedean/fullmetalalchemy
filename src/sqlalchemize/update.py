from typing import Iterable, Optional, Sequence

import sqlalchemy as sa
import sqlalchemy.orm.session as sa_session
import sqlalchemy.engine as sa_engine
from sqlalchemy import update

import sqlalchemize.types as types
import sqlalchemize.features as features
import sqlalchemize.exceptions as ex


def update_records_session(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    match_column_name: str,
    session: sa_session.Session
) -> None:
    match_column = features.get_column(sa_table, match_column_name)
    match_column.primary_key = True
    table_name = sa_table.name
    table_class = features.get_class(table_name, session, schema=sa_table.schema)
    mapper = sa.inspect(table_class)
    session.bulk_update_mappings(mapper, records)


def update_records_fast_session(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    session: sa_session.Session
) -> None:
    """Fast update needs primary key."""
    table_name = sa_table.name
    table_class = features.get_class(table_name, session, schema=sa_table.schema)
    mapper = sa.inspect(table_class)
    session.bulk_update_mappings(mapper, records)


def make_update_statement(table, record_values, new_values):
    up = update(table)
    for col, val in record_values.items():
        up = up.where(table.c[col]==val)
    return up.values(**new_values)


def update_record_slow_session(
    sa_table: sa.Table,
    match_record: dict,
    new_values: dict,
    session: sa_session.Session
) -> None:
    stmt = make_update_statement(sa_table, match_record, new_values)
    session.add(stmt)


def update_records_slow_session(
    sa_table: sa.Table,
    match_records: Iterable[dict],
    new_values: Iterable[dict],
    session: sa_session.Session
) -> None:
    """Slow update does not need primary key.
    """
    for record, new_value in zip(match_records, new_values):
        update_record_slow_session(sa_table, record, new_value, session)
    

def update_records_slow(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    match_column_name: str,
    engine: Optional[sa_engine.Engine] = None
) -> None:
    engine = ex.check_for_engine(sa_table, engine)
    session = sa_session.Session(engine)
    try:
        update_records_slow_session(sa_table, records, match_column_name, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def update_records(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    match_column_name: str,
    engine: Optional[sa_engine.Engine] = None
) -> None:
    engine = ex.check_for_engine(sa_table, engine)
    session = sa_session.Session(engine)
    try:
        update_records_session(sa_table, records, match_column_name, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def update_records_fast(
    sa_table: sa.Table,
    records: list[dict],
    engine
) -> None:
    """Only works with table with primary key"""
    session = sa_session.Session(engine)
    try:
        update_records_fast_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e