from typing import Sequence

import sqlalchemy as sa
import sqlalchemy.orm.session as sa_session
import sqlalchemy.engine as sa_engine

import sqlalchemize.types as types
import sqlalchemize.features as features


def insert_from_table_session(
    sa_table1: sa.Table,
    sa_table2: sa.Table,
    session: sa_session.Session
) -> None:
    session.execute(sa_table2.insert().from_select(sa_table1.columns.keys(), sa_table1))


def insert_from_table(
    sa_table1: sa.Table,
    sa_table2: sa.Table,
    engine: sa_engine.Engine
) -> None:
    session = sa_session.Session(engine)
    try:
        insert_from_table_session(sa_table1, sa_table2, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    

def insert_records_session(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    session: sa_session.Session
) -> None:
    table_class = features.get_class(sa_table.name, session, schema=sa_table.schema)
    mapper = sa.inspect(table_class)
    session.bulk_insert_mappings(mapper, records)


def insert_records(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    engine: sa_engine.Engine
) -> None:
    session = sa_session.Session(engine)
    try:
        insert_records_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e