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

        session.commit()

    def test_update_matching_records(self):
        engine, table = create_table(CONNECTION_STR)

    def test_update_records_session(self):
        engine, table = create_table(CONNECTION_STR)
        session = fa.features.get_session(engine)

        session.commit()

    def test_update_records(self):
        engine, table = create_table(CONNECTION_STR)

    def test_set_column_values_session(self):
        engine, table = create_table(CONNECTION_STR)
        session = fa.features.get_session(engine)

        session.commit()

    def test_set_column_values(self):
        engine, table = create_table(CONNECTION_STR)
    