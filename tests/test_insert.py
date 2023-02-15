import unittest

import fullmetalalchemy as fa
from fullmetalalchemy.records import records_equal
from tests.setup_db import create_table
from fullmetalalchemy.test_setup import create_second_test_table


CONNECTION_STR = 'sqlite://'


class TestInsert(unittest.TestCase):
    def test_insert_from_table_session(self):
        engine, table1 = create_table(CONNECTION_STR)
        table2 = create_second_test_table(engine)
        results = fa.select.select_records_all(table2)
        self.assertEqual(results, [])
        session = fa.features.get_session(engine)
        fa.insert.insert_from_table_session(table1, table2, session)
        session.commit()
        results = fa.select.select_records_all(table2)
        expected = [{'id': 1, 'x': 1, 'y': 2, 'z': None},
                    {'id': 2, 'x': 2, 'y': 4, 'z': None},
                    {'id': 3, 'x': 4, 'y': 8, 'z': None},
                    {'id': 4, 'x': 8, 'y': 11, 'z': None}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_from_table_session_table_name(self):
        engine, table1 = create_table(CONNECTION_STR)
        table2 = create_second_test_table(engine)
        results = fa.select.select_records_all(table2)
        self.assertEqual(results, [])
        session = fa.features.get_session(engine)
        fa.insert.insert_from_table_session('xy', 'xyz', session)
        session.commit()
        results = fa.select.select_records_all(table2)
        expected = [{'id': 1, 'x': 1, 'y': 2, 'z': None},
                    {'id': 2, 'x': 2, 'y': 4, 'z': None},
                    {'id': 3, 'x': 4, 'y': 8, 'z': None},
                    {'id': 4, 'x': 8, 'y': 11, 'z': None}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_from_table(self):
        engine, table1 = create_table(CONNECTION_STR)
        table2 = create_second_test_table(engine)
        results = fa.select.select_records_all(table2)
        self.assertEqual(results, [])
        fa.insert.insert_from_table(table1, table2, engine)
        results = fa.select.select_records_all(table2)
        expected = [{'id': 1, 'x': 1, 'y': 2, 'z': None},
                    {'id': 2, 'x': 2, 'y': 4, 'z': None},
                    {'id': 3, 'x': 4, 'y': 8, 'z': None},
                    {'id': 4, 'x': 8, 'y': 11, 'z': None}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_from_table_table_name(self):
        engine, table1 = create_table(CONNECTION_STR)
        table2 = create_second_test_table(engine)
        results = fa.select.select_records_all(table2)
        self.assertEqual(results, [])
        fa.insert.insert_from_table('xy', 'xyz', engine)
        results = fa.select.select_records_all(table2)
        expected = [{'id': 1, 'x': 1, 'y': 2, 'z': None},
                    {'id': 2, 'x': 2, 'y': 4, 'z': None},
                    {'id': 3, 'x': 4, 'y': 8, 'z': None},
                    {'id': 4, 'x': 8, 'y': 11, 'z': None}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_from_table_no_engine(self):
        engine, table1 = create_table(CONNECTION_STR)
        table2 = create_second_test_table(engine)
        results = fa.select.select_records_all(table2)
        self.assertEqual(results, [])
        fa.insert.insert_from_table(table1, table2)
        results = fa.select.select_records_all(table2)
        expected = [{'id': 1, 'x': 1, 'y': 2, 'z': None},
                    {'id': 2, 'x': 2, 'y': 4, 'z': None},
                    {'id': 3, 'x': 4, 'y': 8, 'z': None},
                    {'id': 4, 'x': 8, 'y': 11, 'z': None}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_records_session(self):
        engine, table = create_table(CONNECTION_STR)
        new_records = [{'id': 5, 'x': 11, 'y': 5}, {'id': 6, 'x': 9, 'y': 9}]
        session = fa.features.get_session(engine)
        fa.insert.insert_records_session(table, new_records, session)
        session.commit()
        results = fa.select.select_records_all(table)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 11},
                    {'id': 5, 'x': 11, 'y': 5},
                    {'id': 6, 'x': 9, 'y': 9}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_records_session_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        new_records = [{'id': 5, 'x': 11, 'y': 5}, {'id': 6, 'x': 9, 'y': 9}]
        session = fa.features.get_session(engine)
        fa.insert.insert_records_session('xy', new_records, session)
        session.commit()
        results = fa.select.select_records_all(table)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 11},
                    {'id': 5, 'x': 11, 'y': 5},
                    {'id': 6, 'x': 9, 'y': 9}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_records(self):
        engine, table = create_table(CONNECTION_STR)
        new_records = [{'id': 5, 'x': 11, 'y': 5}, {'id': 6, 'x': 9, 'y': 9}]
        fa.insert.insert_records(table, new_records, engine)
        results = fa.select.select_records_all(table)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 11},
                    {'id': 5, 'x': 11, 'y': 5},
                    {'id': 6, 'x': 9, 'y': 9}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_records_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        new_records = [{'id': 5, 'x': 11, 'y': 5}, {'id': 6, 'x': 9, 'y': 9}]
        fa.insert.insert_records('xy', new_records, engine)
        results = fa.select.select_records_all(table)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 11},
                    {'id': 5, 'x': 11, 'y': 5},
                    {'id': 6, 'x': 9, 'y': 9}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)

    def test_insert_records_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        new_records = [{'id': 5, 'x': 11, 'y': 5}, {'id': 6, 'x': 9, 'y': 9}]
        fa.insert.insert_records(table, new_records)
        results = fa.select.select_records_all(table)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 11},
                    {'id': 5, 'x': 11, 'y': 5},
                    {'id': 6, 'x': 9, 'y': 9}]
        equal = records_equal(results, expected)
        self.assertTrue(equal)