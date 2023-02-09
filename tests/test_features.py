import unittest

from sqlalchemy import INTEGER, MetaData, Table
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session

from fullmetalalchemy.features import get_class, get_table, primary_key_columns, primary_key_names
from fullmetalalchemy.features import get_connection, get_session, get_metadata
from fullmetalalchemy.features import get_engine_table
from tests.setup_db import create_table


CONNECTION_STR = 'sqlite://'


class TestPrimaryKeys(unittest.TestCase):
    def test_primary_key_columns(self):
        engine, table = create_table(CONNECTION_STR)
        results = primary_key_columns(table)
        names = [c.name for c in results]
        types = [type(c.type) for c in results]
        self.assertListEqual(names, ['id'])
        self.assertListEqual(types, [INTEGER])

    def test_primary_key_names(self):
        engine, table = create_table(CONNECTION_STR)
        results = primary_key_names(table)
        expected = ['id']
        self.assertListEqual(results, expected)


class TestConnection(unittest.TestCase):
    def test_get_connection(self):
        engine, table = create_table(CONNECTION_STR)
        session = get_session(engine)
        con = get_connection(session)
        results = type(con)
        expected = Connection
        self.assertEqual(results, expected)

    def test_get_session(self):
        engine, table = create_table(CONNECTION_STR)
        session = get_session(engine)
        result = type(session)
        expected = Session
        self.assertEqual(result, expected)

class TestMetaData(unittest.TestCase):
    def test_get_metadata(self):
        engine, table = create_table(CONNECTION_STR)
        meta = get_metadata(engine)
        results = type(meta)
        expected = MetaData
        self.assertEqual(results, expected)
    
    def test_get_table(self):
        engine, table = create_table(CONNECTION_STR)
        result_table = get_table('xy', engine)
        results = result_table.name, result_table.bind, type(result_table)
        expected = 'xy', engine, Table
        self.assertEqual(results, expected)

    def test_get_engine_table(self):
        con_str = 'sqlite:///data/test.db'
        create_table(con_str)
        results = get_engine_table(con_str, 'xy')
        results = tuple(type(x) for x in results)
        expected = Engine, Table
        self.assertEqual(results, expected)

    def test_get_class(self):
        engine, table = create_table(CONNECTION_STR)
        result = get_class('xy', engine)
        results = type(result)
        expected = DeclarativeMeta
        self.assertEqual(results, expected)
        