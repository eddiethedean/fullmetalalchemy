from typing import Sequence

import sqlalchemy as sa
import sqlalchemy.engine as sa_engine
import sqlalchemy.orm.session as sa_session
from sqlalchemy.sql.expression import Select

import sqlalchemize.features as features
import sqlalchemize.types as types


def delete_records_session(
    sa_table: sa.Table,
    col_name: str,
    values: Sequence,
    session: sa_session.Session
) -> None:
    col = features.get_column(sa_table, col_name)
    session.query(sa_table).filter(col.in_(values)).delete(synchronize_session=False)


def delete_records(
    sa_table: sa.Table,
    col_name: str,
    values: Sequence,
    engine: sa_engine.Engine
) -> None:
    session = sa_session.Session(engine)
    delete_records_session(sa_table, col_name, values, session)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def delete_record_by_values_session(
    sa_table: sa.Table,
    record: types.Record,
    session: sa_session.Session
) -> None:
    where = build_where_from_record(sa_table, record)
    session.query(sa_table).filter(where).delete(synchronize_session=False)


def delete_records_by_values_session(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    session: sa_session.Session
) -> None:
    for record in records:
        delete_record_by_values_session(sa_table, record, session)

        
def build_where_from_record(
    sa_table: sa.Table,
    record: types.Record
) -> Select:
    s = sa.select(sa_table)
    for col, val in record.items():
        s = s.where(sa_table.c[col]==val)
    return s


def delete_all_records_session(
    table: sa.Table,
    session: sa_session.Session
) -> None:
    session.query(table).delete()


def delete_all_records(
    sa_table: sa.Table,
    engine: sa_engine.Engine
) -> None:
    session = sa_session.Session(engine)
    try:
        delete_all_records_session(sa_table, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e