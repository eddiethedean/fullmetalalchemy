from typing import Tuple

import sqlalchemy as sa

from sqlalchemize.test_setup import create_test_table, insert_test_records

Engine = sa.engine.Engine
Table = sa.Table


def create_table(connection_string: str) -> Tuple[Engine, Table]:
    engine = sa.create_engine(connection_string)
    table = create_test_table(engine)
    insert_test_records(table, engine)
    return engine, table

