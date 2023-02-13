import unittest

import fullmetalalchemy as fa
from fullmetalalchemy.records import records_equal
from tests.setup_db import create_table
from fullmetalalchemy.test_setup import create_second_test_table


CONNECTION_STR = 'sqlite://'


class TestSelect(unittest.TestCase):
    def test_select_records_all(self):
        ...

    def test_select_records_chunks(self):
        ...

    def test_select_existing_values(self):
        ...

    def test_select_column_values_all(self):
        ...

    def test_select_column_values_chunks(self):
        ...

    def test_select_records_slice(self):
        ...

    def test_select_column_values_by_slice(self):
        ...

    def test_select_column_value_by_index(self):
        ...

    def test_select_record_by_index(self):
        ...

    def test_select_primary_key_records_by_slice(self):
        ...

    def test_select_record_by_primary_key(self):
        ...

    def test_select_records_by_primary_keys(self):
        ...

    def test_select_column_values_by_primary_keys(self):
        ...

    def test_select_value_by_primary_keys(self):
        ...