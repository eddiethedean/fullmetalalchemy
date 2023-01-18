from sqlalchemize.create import create_table
from sqlalchemize.insert import insert_records


def setup(engine):
    table = create_test_table(engine)
    insert_test_records(table, engine)
    return table


def create_test_table(engine):
    return create_table(table_name='xy',
                        column_names=['id', 'x', 'y'],
                        column_types=[int, int, int],
                        primary_key='id',
                        engine=engine,
                        if_exists='replace')


def insert_test_records(table, engine):
    records = [
        {'id': 1, 'x': 1, 'y': 2},
        {'id': 2, 'x': 2, 'y': 4},
        {'id': 3, 'x': 4, 'y': 8},
        {'id': 4, 'x': 8, 'y': 11}
    ]
    insert_records(table, records, engine)
    
    