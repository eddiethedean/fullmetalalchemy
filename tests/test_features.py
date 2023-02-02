import unittest

from sqlalchemize.features import primary_key_columns
from tests.setup_db import create_table


connection_string = 'sqlite://'


class TestPrimaryKeyColumns(unittest.TestCase):
    def test_primary_key_columns(self):
        engine, table = create_table(connection_string)
        