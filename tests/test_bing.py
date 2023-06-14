import fullmetalalchemy as fa
import sqlalchemy as _sa
import unittest

# Create a test case class that inherits from unittest.TestCase
class TestInsertFromTableSession(unittest.TestCase):
    # Define a setUpClass method to set up the database connection and session once for all tests
    @classmethod
    def setUpClass(cls):
        # Create an in-memory SQLite database for testing
        cls.engine = fa.create_engine("sqlite:///:memory:")
        # Create some test tables with some data
        cls.table1 = _sa.Table(
            "table1",
            _sa.MetaData(),
            _sa.Column("id", _sa.Integer, primary_key=True),
            _sa.Column("name", _sa.String),
            _sa.Column("age", _sa.Integer),
        )
        cls.table2 = _sa.Table(
            "table2",
            _sa.MetaData(),
            _sa.Column("id", _sa.Integer, primary_key=True),
            _sa.Column("name",_sa.String),
            _sa.Column("age",_sa.Integer),
        )
        cls.table1.create(cls.engine)
        cls.table2.create(cls.engine)
        cls.table1.insert().values(
            [
                {"id": 1, "name": "Alice", "age": 20},
                {"id": 2, "name": "Bob", "age": 25},
                {"id": 3, "name": "Charlie", "age": 30},
                {"id": 4, "name": "David", "age": 35},
            ]
        ).execute()

        # Create a session for the database connection 
        cls.session = fa.features.get_session(cls.engine)

    # Define a tearDownClass method to tear down the database connection and session after all tests 
    @classmethod 
    def tearDownClass(cls):
        # Drop the tables and close the session after testing 
        cls.table1.drop(cls.engine)
        cls.table2.drop(cls.engine)
        cls.session.close()

    # Write a test method to check if the function inserts all rows from one table to another 
    def test_insert_from_table_session(self):
        # Call the function to insert all rows from table1 to table2 
        fa.insert.insert_from_table_session(self.table1 ,self.table2 ,self.session)

        # Commit the changes to the database 
        self.session.commit()

        # Query both tables and compare their records 
        records1 = fa.select.select_records_all(self.table1)
        records2 = fa.select.select_records_all(self.table2)
        self.assertEqual(records1 ,records2)

        # Check that both tables have the same number of rows 
        self.assertEqual(len(records1) ,len(records2) ,4)