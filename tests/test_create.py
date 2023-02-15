import unittest
import os

import sqlalchemy as sa
import fullmetalalchemy as sz
from fullmetalalchemy.create import create_table, create_table_from_records
from fullmetalalchemy.features import tables_metadata_equal
from fullmetalalchemy.records import records_equal

connection_string = f'sqlite://'


class TestCreateTable(unittest.TestCase):
    def test_create_table_sqlite(self):
        engine = sa.create_engine(connection_string)
        results = create_table(
            table_name='xy',
            column_names=['id', 'x', 'y'],
            column_types=[int, int, int],
            primary_key=['id'],
            engine=engine,
            if_exists='replace')
        table = sz.features.get_table('xy', engine)
        expected = sa.Table('xy', sa.MetaData(bind=engine), 
            sa.Column('id', sa.sql.sqltypes.INTEGER(), primary_key=True, nullable=False),
            sa.Column('x', sa.sql.sqltypes.INTEGER()),
            sa.Column('y', sa.sql.sqltypes.INTEGER()), schema=None)
        metadata_same = tables_metadata_equal(table, expected)
        self.assertTrue(metadata_same)


class TestCreateTableFromRecords(unittest.TestCase):
    def test_create_table_from_records_sqlite(self):
        engine = sa.create_engine(connection_string)
        records = [
            {'id': 1, 'x': 1, 'y': 2},
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}]
        table = create_table_from_records(
            table_name='xy',
            records=records,
            primary_key=['id'],
            engine=engine,
            if_exists='replace')
        expected = sa.Table('xy', sa.MetaData(bind=engine), 
            sa.Column('id', sa.sql.sqltypes.INTEGER(), primary_key=True, nullable=False),
            sa.Column('x', sa.sql.sqltypes.INTEGER()),
            sa.Column('y', sa.sql.sqltypes.INTEGER()), schema=None)
        metadata_same = tables_metadata_equal(table, expected)
        self.assertTrue(metadata_same)
        selected = sz.select.select_records_all(table, engine)
        records_same = records_equal(selected, records)
        self.assertTrue(records_same)


if __name__ == '__main__':
    unittest.main()