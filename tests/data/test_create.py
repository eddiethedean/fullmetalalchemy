import unittest
import os

import sqlalchemy as sa
import sqlalchemize as sz
from sqlalchemize.create import create_table, create_engine, create_table_from_records


path = os.getcwd() + '/tests/data'
connection_string = f'sqlite:///{path}/test.db'


class TestCreateTable(unittest.TestCase):
    def create_table_sqlite(self):
        engine = create_engine(connection_string)
        results = create_table(
            table_name='xy',
            column_names=['id', 'x', 'y'],
            column_types=[int, int, int],
            primary_key=['id'],
            engine=engine,
            if_exists='replace')
        table = sz.features.get_table('xy', engine)
        expected = sa.Table('xy', sa.MetaData(bind=engine), 
            sa.Column('id', sa.sql.sqltypes.INTEGER(), table=table, primary_key=True, nullable=False),
            sa.Column('x', sa.sql.sqltypes.INTEGER(), table=table),
            sa.Column('y', sa.sql.sqltypes.INTEGER(), table=table), schema=None)
        self.assertEqual(results, expected)


class TestCreateTableFromRecords(unittest.TestCase):
    def create_table_from_records_sqlite(self):
        engine = create_engine(connection_string)
        records = [
            {'id': 1, 'x': 1, 'y': 2},
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}]
        results = create_table_from_records(
            table_name='xy',
            records=records,
            primary_key=['id'],
            engine=engine,
            if_exists='replace')
        table = sz.features.get_table('xy', engine)
        expected = sa.Table('xy', sa.MetaData(bind=engine), 
            sa.Column('id', sa.sql.sqltypes.INTEGER(), table=table, primary_key=True, nullable=False),
            sa.Column('x', sa.sql.sqltypes.INTEGER(), table=table),
            sa.Column('y', sa.sql.sqltypes.INTEGER(), table=table), schema=None)
        self.assertEqual(results, expected)
        selected = sz.select.select_records_all(table, engine)
        self.assertEqual(selected, records)