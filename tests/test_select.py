import unittest

import fullmetalalchemy as fa
from fullmetalalchemy.records import records_equal
from tests.setup_db import create_table
from fullmetalalchemy.test_setup import create_second_test_table


CONNECTION_STR = 'sqlite://'


class TestSelect(unittest.TestCase):
    def test_select_records_all(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_all(table, engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 11}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_records_chunks(self):
        engine, table = create_table(CONNECTION_STR)
        records_chunks = fa.select.select_records_chunks(table, engine)
        results = next(records_chunks)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)
        results = next(records_chunks)
        expected = [{'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 11}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_existing_values(self):
        engine, table = create_table(CONNECTION_STR)
        values = [1, 2, 3, 4, 5]
        results = set(fa.select.select_existing_values(table, 'x', values, engine))
        expected = set([1, 2, 4])
        self.assertSetEqual(results, expected)

    def test_select_column_values_all(self):
        engine, table = create_table(CONNECTION_STR)
        results = set(fa.select.select_column_values_all(table, 'x', engine))
        expected = set([1, 2, 4, 8])
        self.assertSetEqual(results, expected)

    def test_select_column_values_chunks(self):
        engine, table = create_table(CONNECTION_STR)
        col_chunks = fa.select.select_column_values_chunks(table, engine, 'x', 2)
        results = next(col_chunks)
        expected = [1, 2]
        self.assertListEqual(results, expected)
        results = next(col_chunks)
        expected = [4, 8]
        self.assertListEqual(results, expected)

    def test_select_records_slice(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_slice(table, engine, start=1, stop=3)
        expected = [{'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_column_values_by_slice(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_column_values_by_slice(table, engine, 'y', start=1, stop=3)
        expected = [4, 8]
        self.assertListEqual(results, expected)

    def test_select_column_value_by_index(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_column_value_by_index(table, engine, 'y', 2)
        expected = 8
        self.assertEqual(result, expected)

    def test_select_record_by_index(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_record_by_index(table, 2, engine)
        expected = {'id': 3, 'x': 4, 'y': 8}
        self.assertDictEqual(result, expected)

    def test_select_primary_key_records_by_slice(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_primary_key_records_by_slice(table, engine, slice(1, 3))
        expected = [{'id': 2}, {'id': 3}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_record_by_primary_key(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_record_by_primary_key(table, engine, {'id': 3})
        expected = {'id': 3, 'x': 4, 'y': 8}
        self.assertDictEqual(result, expected)

    def test_select_records_by_primary_keys(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_by_primary_keys(table, engine, [{'id': 3}, {'id': 1}])
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 3, 'x': 4, 'y': 8}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_column_values_by_primary_keys(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_column_values_by_primary_keys(table, engine, 'y', [{'id': 3}, {'id': 1}])
        expected = [2, 8]
        self.assertListEqual(results, expected)

    def test_select_value_by_primary_keys(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_value_by_primary_keys(table, engine, 'y', {'id': 3})
        expected = 8
        self.assertEqual(result, expected)