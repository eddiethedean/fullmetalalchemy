from typing import Optional, Sequence

import sqlalchemy as sa
import sqlalchemy.orm.session as sa_session
import sqlalchemy.engine as sa_engine

import sqlalchemize.types as types
import sqlalchemize.features as features
import sqlalchemize.exceptions as ex


def update_records_session(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    session: sa_session.Session
) -> None:
    table_name = sa_table.name
    table_class = features.get_class(table_name, session, schema=sa_table.schema)
    mapper = sa.inspect(table_class)
    session.bulk_update_mappings(mapper, records)


def update_records(
    sa_table: sa.Table,
    records: Sequence[types.Record],
    engine: Optional[sa_engine.Engine] = None
) -> None:
    engine = ex.check_for_engine(sa_table, engine)
    session = sa_session.Session(engine)
    try:
        update_records_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e