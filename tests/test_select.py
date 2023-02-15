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

    def test_select_records_all_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_all('xy', engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8},
                    {'id': 4, 'x': 8, 'y': 11}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_records_all_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_all(table)
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

    def test_select_records_chunks_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        records_chunks = fa.select.select_records_chunks('xy', engine)
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

    def test_select_records_chunks_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        records_chunks = fa.select.select_records_chunks(table)
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

    def test_select_existing_values_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        values = [1, 2, 3, 4, 5]
        results = set(fa.select.select_existing_values('xy', 'x', values, engine))
        expected = set([1, 2, 4])
        self.assertSetEqual(results, expected)

    def test_select_existing_values_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        values = [1, 2, 3, 4, 5]
        results = set(fa.select.select_existing_values(table, 'x', values))
        expected = set([1, 2, 4])
        self.assertSetEqual(results, expected)

    def test_select_column_values_all(self):
        engine, table = create_table(CONNECTION_STR)
        results = set(fa.select.select_column_values_all(table, 'x', engine))
        expected = set([1, 2, 4, 8])
        self.assertSetEqual(results, expected)

    def test_select_column_values_all_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        results = set(fa.select.select_column_values_all('xy', 'x', engine))
        expected = set([1, 2, 4, 8])
        self.assertSetEqual(results, expected)

    def test_select_column_values_all_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        results = set(fa.select.select_column_values_all(table, 'x'))
        expected = set([1, 2, 4, 8])
        self.assertSetEqual(results, expected)

    def test_select_column_values_chunks(self):
        engine, table = create_table(CONNECTION_STR)
        col_chunks = fa.select.select_column_values_chunks(table, 'x', 2, engine)
        results = next(col_chunks)
        expected = [1, 2]
        self.assertListEqual(results, expected)
        results = next(col_chunks)
        expected = [4, 8]
        self.assertListEqual(results, expected)

    def test_select_column_values_chunks_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        col_chunks = fa.select.select_column_values_chunks('xy', 'x', 2, engine)
        results = next(col_chunks)
        expected = [1, 2]
        self.assertListEqual(results, expected)
        results = next(col_chunks)
        expected = [4, 8]
        self.assertListEqual(results, expected)

    def test_select_column_values_chunks_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        col_chunks = fa.select.select_column_values_chunks(table, 'x', 2)
        results = next(col_chunks)
        expected = [1, 2]
        self.assertListEqual(results, expected)
        results = next(col_chunks)
        expected = [4, 8]
        self.assertListEqual(results, expected)

    def test_select_records_slice(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_slice(table, start=1, stop=3, connection=engine)
        expected = [{'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_records_slice_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_slice('xy', start=1, stop=3, connection=engine)
        expected = [{'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_records_slice_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_slice(table, start=1, stop=3)
        expected = [{'id': 2, 'x': 2, 'y': 4},
                    {'id': 3, 'x': 4, 'y': 8}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_column_values_by_slice(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_column_values_by_slice(table, 'y', start=1, stop=3, connection=engine)
        expected = [4, 8]
        self.assertListEqual(results, expected)

    def test_select_column_values_by_slice_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_column_values_by_slice('xy', 'y', start=1, stop=3, connection=engine)
        expected = [4, 8]
        self.assertListEqual(results, expected)

    def test_select_column_values_by_slice_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_column_values_by_slice(table, 'y', start=1, stop=3)
        expected = [4, 8]
        self.assertListEqual(results, expected)

    def test_select_column_value_by_index(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_column_value_by_index(table, 'y', 2, engine)
        expected = 8
        self.assertEqual(result, expected)

    def test_select_column_value_by_index_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_column_value_by_index('xy', 'y', 2, engine)
        expected = 8
        self.assertEqual(result, expected)

    def test_select_column_value_by_index_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_column_value_by_index(table, 'y', 2)
        expected = 8
        self.assertEqual(result, expected)

    def test_select_record_by_index(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_record_by_index(table, 2, engine)
        expected = {'id': 3, 'x': 4, 'y': 8}
        self.assertDictEqual(result, expected)

    def test_select_record_by_index_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_record_by_index('xy', 2, engine)
        expected = {'id': 3, 'x': 4, 'y': 8}
        self.assertDictEqual(result, expected)

    def test_select_record_by_index_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_record_by_index(table, 2)
        expected = {'id': 3, 'x': 4, 'y': 8}
        self.assertDictEqual(result, expected)

    def test_select_primary_key_records_by_slice(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_primary_key_records_by_slice(table, slice(1, 3), engine)
        expected = [{'id': 2}, {'id': 3}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_primary_key_records_by_slice_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_primary_key_records_by_slice('xy', slice(1, 3), engine)
        expected = [{'id': 2}, {'id': 3}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_primary_key_records_by_slice_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_primary_key_records_by_slice(table, slice(1, 3))
        expected = [{'id': 2}, {'id': 3}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_record_by_primary_key(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_record_by_primary_key(table, {'id': 3}, engine)
        expected = {'id': 3, 'x': 4, 'y': 8}
        self.assertDictEqual(result, expected)

    def test_select_record_by_primary_key_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_record_by_primary_key('xy', {'id': 3}, engine)
        expected = {'id': 3, 'x': 4, 'y': 8}
        self.assertDictEqual(result, expected)

    def test_select_record_by_primary_key_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_record_by_primary_key(table, {'id': 3})
        expected = {'id': 3, 'x': 4, 'y': 8}
        self.assertDictEqual(result, expected)

    def test_select_records_by_primary_keys(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_by_primary_keys(table, [{'id': 3}, {'id': 1}], engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 3, 'x': 4, 'y': 8}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_records_by_primary_keys_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_by_primary_keys('xy', [{'id': 3}, {'id': 1}], engine)
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 3, 'x': 4, 'y': 8}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_records_by_primary_keys_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_records_by_primary_keys(table, [{'id': 3}, {'id': 1}])
        expected = [{'id': 1, 'x': 1, 'y': 2},
                    {'id': 3, 'x': 4, 'y': 8}]
        equals = records_equal(results, expected)
        self.assertTrue(equals)

    def test_select_column_values_by_primary_keys(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_column_values_by_primary_keys(table, 'y', [{'id': 3}, {'id': 1}], engine)
        expected = [2, 8]
        self.assertListEqual(results, expected)

    def test_select_column_values_by_primary_keys_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_column_values_by_primary_keys('xy', 'y', [{'id': 3}, {'id': 1}], engine)
        expected = [2, 8]
        self.assertListEqual(results, expected)

    def test_select_column_values_by_primary_keys_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        results = fa.select.select_column_values_by_primary_keys(table, 'y', [{'id': 3}, {'id': 1}])
        expected = [2, 8]
        self.assertListEqual(results, expected)

    def test_select_value_by_primary_keys(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_value_by_primary_keys(table, 'y', {'id': 3}, engine)
        expected = 8
        self.assertEqual(result, expected)

    def test_select_value_by_primary_keys_table_name(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_value_by_primary_keys('xy', 'y', {'id': 3}, engine)
        expected = 8
        self.assertEqual(result, expected)

    def test_select_value_by_primary_keys_no_engine(self):
        engine, table = create_table(CONNECTION_STR)
        result = fa.select.select_value_by_primary_keys(table, 'y', {'id': 3})
        expected = 8
        self.assertEqual(result, expected)