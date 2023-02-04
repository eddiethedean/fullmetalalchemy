import unittest

from fullmetalalchemy.records import records_equal
from setup_db import create_table
from fullmetalalchemy.features import get_table_names
from fullmetalalchemy.drop import drop_table


connection_string = 'sqlite://'


class TestDrop(unittest.TestCase):
    def test_drop_table(self):
        engine, table = create_table(connection_string)
        names = get_table_names(engine)
        expected = ['xy']
        self.assertListEqual(expected, names)
        drop_table(table, engine)
        names = get_table_names(engine)
        expected = []
        self.assertListEqual(expected, names)