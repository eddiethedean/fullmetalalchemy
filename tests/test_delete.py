import unittest
import os

import sqlalchemy as sa
import sqlalchemy.orm.session as sa_session

import sqlalchemize as sz
from sqlalchemize.records import records_equal
from setup_db import create_table
from sqlalchemize.delete import delete_all_records, delete_all_records_session
from sqlalchemize.select import select_records_all


path = os.getcwd() + '/tests/data'
connection_string = f'sqlite:///{path}/test.db'


class TestDelete(unittest.TestCase):
    def test_delete_all(self):
        engine, table = create_table(connection_string)
        delete_all_records(table, engine)
        results = select_records_all(table, engine)
        expected = []
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_all_session(self):
        engine, table = create_table(connection_string)
        session = sa_session.Session(engine)
        delete_all_records_session(table, session)
        expected = [
            {'id': 1, 'x': 1, 'y': 2},
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}]
        results = select_records_all(table, engine)
        equal = records_equal(results, expected)
        self.assertTrue(equal)
        session.commit()
        results = select_records_all(table, engine)
        expected = []
        equal = records_equal(results, expected)
        self.assertTrue(equal)



