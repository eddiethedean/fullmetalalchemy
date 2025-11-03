"""Multi-database tests for insert operations."""

import pytest
import sqlalchemy as sa

import fullmetalalchemy as fa
from fullmetalalchemy.insert import insert_records
from fullmetalalchemy.select import select_records_all
from fullmetalalchemy.test_setup import create_second_test_table, create_test_table


def _test_insert_records(engine: sa.engine.Engine) -> None:
    """Test inserting records into a table."""
    table = create_test_table(engine)

    records = [
        {"id": 1, "x": 10, "y": 20},
        {"id": 2, "x": 30, "y": 40},
    ]
    insert_records(table, records, engine)

    results = select_records_all(table, engine)
    assert len(results) == 2
    assert results[0]["id"] == 1
    assert results[1]["id"] == 2


def _test_insert_from_table(engine: sa.engine.Engine) -> None:
    """Test inserting records from one table to another."""
    table1 = create_test_table(engine)
    fa.insert.insert_records(
        table1,
        [
            {"id": 1, "x": 1, "y": 2},
            {"id": 2, "x": 2, "y": 4},
        ],
        engine,
    )

    table2 = create_second_test_table(engine)
    fa.insert.insert_from_table(table1, table2, engine)

    results = fa.select.select_records_all(table2, engine)
    assert len(results) == 2
    assert results[0]["id"] == 1
    assert results[0]["z"] is None  # Extra column should be None


@pytest.mark.postgres
def test_insert_records_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_insert_records(postgres_engine)


@pytest.mark.mysql
def test_insert_records_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_insert_records(mysql_engine)


@pytest.mark.postgres
def test_insert_from_table_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_insert_from_table(postgres_engine)


@pytest.mark.mysql
def test_insert_from_table_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_insert_from_table(mysql_engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_insert_records_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_insert_records(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_insert_from_table_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_insert_from_table(engine)
