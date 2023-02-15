import unittest

import fullmetalalchemy as fa
from fullmetalalchemy.records import records_equal
from tests.setup_db import create_table
from fullmetalalchemy.test_setup import create_second_test_table


CONNECTION_STR = 'sqlite://'


class TestUpdate(unittest.TestCase):
    def test_update_matching_records_session(self):
        engine, table = create_table(CONNECTION_STR)
        session = fa.features.get_session(engine)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_matching_records_session(table, updated_records, ['id'], session)
        session.commit()
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_matching_records_session_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        session = fa.features.get_session(engine)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_matching_records_session('xy', updated_records, ['id'], session)
        session.commit()
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_matching_records(self):
        engine, table = create_table(CONNECTION_STR)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_matching_records(table, updated_records, ['id'], engine)
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_matching_records_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_matching_records('xy', updated_records, ['id'], engine)
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_matching_records_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_matching_records(table, updated_records, ['id'])
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_records_session(self):
        engine, table = create_table(CONNECTION_STR)
        session = fa.features.get_session(engine)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_records_session(table, updated_records, session)
        session.commit()
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_records_session_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        session = fa.features.get_session(engine)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_records_session('xy', updated_records, session)
        session.commit()
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_records(self):
        engine, table = create_table(CONNECTION_STR)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_records(table, updated_records, engine)
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_records_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_records('xy', updated_records, engine)
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_update_records_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        updated_records = [{'id': 1, 'x': 11}, {'id': 4, 'y': 111}]
        fa.update.update_records(table, updated_records)
        expected = [{'id': 1, 'x': 11, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 111}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_set_column_values_session(self):
        engine, table = create_table(CONNECTION_STR)
        session = fa.features.get_session(engine)
        new_value = 1
        fa.update.set_column_values_session(table, 'x', new_value, session)
        session.commit()
        expected =[{'id': 1, 'x': 1, 'y': 2},
                   {'id': 2, 'x': 1, 'y': 4},
                   {'id': 3, 'x': 1, 'y': 8},
                   {'id': 4, 'x': 1, 'y': 11}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_set_column_values_session_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        session = fa.features.get_session(engine)
        new_value = 1
        fa.update.set_column_values_session('xy', 'x', new_value, session)
        session.commit()
        expected =[{'id': 1, 'x': 1, 'y': 2},
                   {'id': 2, 'x': 1, 'y': 4},
                   {'id': 3, 'x': 1, 'y': 8},
                   {'id': 4, 'x': 1, 'y': 11}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_set_column_values(self):
        engine, table = create_table(CONNECTION_STR)
        new_value = 1
        fa.update.set_column_values(table, 'x', new_value, engine)
        expected =[{'id': 1, 'x': 1, 'y': 2},
                   {'id': 2, 'x': 1, 'y': 4},
                   {'id': 3, 'x': 1, 'y': 8},
                   {'id': 4, 'x': 1, 'y': 11}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_set_column_values_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        new_value = 1
        fa.update.set_column_values('xy', 'x', new_value, engine)
        expected =[{'id': 1, 'x': 1, 'y': 2},
                   {'id': 2, 'x': 1, 'y': 4},
                   {'id': 3, 'x': 1, 'y': 8},
                   {'id': 4, 'x': 1, 'y': 11}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_set_column_values_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        new_value = 1
        fa.update.set_column_values(table, 'x', new_value)
        expected =[{'id': 1, 'x': 1, 'y': 2},
                   {'id': 2, 'x': 1, 'y': 4},
                   {'id': 3, 'x': 1, 'y': 8},
                   {'id': 4, 'x': 1, 'y': 11}]
        results = fa.select.select_records_all(table)
        equals = records_equal(results, expected)
        self.assertTrue(equals)
    