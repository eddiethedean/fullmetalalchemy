"""Shared pytest fixtures for all tests."""
from typing import Tuple

import pytest
import sqlalchemy as sa

from fullmetalalchemy.test_setup import create_test_table, insert_test_records


@pytest.fixture
def engine_and_table() -> Tuple[sa.engine.Engine, sa.Table]:
    """Create a fresh in-memory SQLite engine and table with test data for each test.

    Returns
    -------
    Tuple[Engine, Table]
        A tuple of (engine, table) where the table contains 4 test records:
        - {'id': 1, 'x': 1, 'y': 2}
        - {'id': 2, 'x': 2, 'y': 4}
        - {'id': 3, 'x': 4, 'y': 8}
        - {'id': 4, 'x': 8, 'y': 11}
    """
    engine = sa.create_engine('sqlite://')
    table = create_test_table(engine)
    insert_test_records(table, engine)
    return engine, table

