from typing import Tuple

import sqlalchemy as sa

from sqlalchemize.create import create_table_from_records
from sqlalchemize.create import create_engine

Engine = sa.engine.Engine
Table = sa.Table


def create_table(connection_string: str) -> Tuple[Engine, Table]:
    engine = create_engine(connection_string)
    records = [
            {'id': 1, 'x': 1, 'y': 2},
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}]
    return engine, create_table_from_records(
            table_name='xy',
            records=records,
            primary_key=['id'],
            engine=engine,
            if_exists='replace')

