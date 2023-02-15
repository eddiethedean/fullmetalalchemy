import unittest

import sqlalchemy.orm.session as sa_session

from fullmetalalchemy.records import records_equal
from setup_db import create_table
from fullmetalalchemy.delete import delete_all_records, delete_all_records_session, delete_records
from fullmetalalchemy.delete import delete_records_by_values_session, delete_records_by_values, delete_records_session
from fullmetalalchemy.select import select_records_all


CONNECTION_STRING = 'sqlite://'


class TestDelete(unittest.TestCase):
    def test_delete_all(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_all_records(table, engine)
        results = select_records_all(table, engine)
        expected = []
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_all_table_name(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_all_records('xy', engine)
        results = select_records_all(table, engine)
        expected = []
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_all_no_engine(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_all_records(table)
        results = select_records_all(table, engine)
        expected = []
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_all_session(self):
        engine, table = create_table(CONNECTION_STRING)
        session = sa_session.Session(engine)
        delete_all_records_session(table, session)
        results = select_records_all(table, engine)
        expected = []
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_all_session_table_name(self):
        engine, table = create_table(CONNECTION_STRING)
        session = sa_session.Session(engine)
        delete_all_records_session('xy', session)
        results = select_records_all(table, engine)
        expected = []
        equal = records_equal(results, expected)
        self.assertTrue(equal)
        
    def test_delete_records_session(self):
        engine, table = create_table(CONNECTION_STRING)
        session = sa_session.Session(engine)
        delete_records_session(table, 'id', [1], session)
        session.commit()
        results = select_records_all(table, engine)
        expected = [
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}
        ]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records_session_table_name(self):
        engine, table = create_table(CONNECTION_STRING)
        session = sa_session.Session(engine)
        delete_records_session('xy', 'id', [1], session)
        session.commit()
        results = select_records_all(table, engine)
        expected = [
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}
        ]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_records(table, 'id', [1], engine)
        results = select_records_all(table, engine)
        expected = [
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}
        ]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records_table_name(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_records('xy', 'id', [1], engine)
        results = select_records_all(table, engine)
        expected = [
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}
        ]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records_no_engine(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_records(table, 'id', [1])
        results = select_records_all(table, engine)
        expected = [
            {'id': 2, 'x': 2, 'y': 4},
            {'id': 3, 'x': 4, 'y': 8},
            {'id': 4, 'x': 8, 'y': 11}
        ]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records_by_values(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_records_by_values(table, [{'id': 3}, {'x': 2}], engine)
        results = select_records_all(table, engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 4, 'x': 8, 'y': 11}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records_by_values_no_engine(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_records_by_values(table, [{'id': 3}, {'x': 2}])
        results = select_records_all(table, engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 4, 'x': 8, 'y': 11}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records_by_values_table_name(self):
        engine, table = create_table(CONNECTION_STRING)
        delete_records_by_values('xy', [{'id': 3}, {'x': 2}], engine)
        results = select_records_all(table, engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 4, 'x': 8, 'y': 11}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records_by_values_session(self):
        engine, table = create_table(CONNECTION_STRING)
        session = sa_session.Session(engine)
        delete_records_by_values_session(table, [{'id': 3}, {'x': 2}], session)
        results = select_records_all(table, engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 4, 'x': 8, 'y': 11}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_delete_records_by_values_session_table_name(self):
        engine, table = create_table(CONNECTION_STRING)
        session = sa_session.Session(engine)
        delete_records_by_values_session('xy', [{'id': 3}, {'x': 2}], session)
        results = select_records_all(table, engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 4, 'x': 8, 'y': 11}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

