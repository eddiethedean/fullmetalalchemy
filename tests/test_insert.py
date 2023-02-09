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
