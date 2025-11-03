"""Multi-database tests for update operations."""

import pytest
import sqlalchemy as sa

from fullmetalalchemy.select import select_records_all
from fullmetalalchemy.test_setup import create_test_table, insert_test_records
from fullmetalalchemy.update import update_matching_records, update_records


def _test_update_matching_records(engine: sa.engine.Engine) -> None:
    """Test updating records by matching columns."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    updated_records = [
        {"id": 1, "x": 99},
        {"id": 2, "y": 100},
    ]
    update_matching_records(table, updated_records, ["id"], engine)

    results = select_records_all(table, engine)
    row1 = next(r for r in results if r["id"] == 1)
    row2 = next(r for r in results if r["id"] == 2)
    assert row1["x"] == 99
    assert row2["y"] == 100


def _test_update_records(engine: sa.engine.Engine) -> None:
    """Test updating records using primary key."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    updated_records = [
        {"id": 3, "x": 200, "y": 300},
    ]
    update_records(table, updated_records, engine)

    results = select_records_all(table, engine)
    row3 = next(r for r in results if r["id"] == 3)
    assert row3["x"] == 200
    assert row3["y"] == 300


def _test_update_multiple_records(engine: sa.engine.Engine) -> None:
    """Test updating multiple records at once."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    updated_records = [
        {"id": 1, "y": 999},
        {"id": 4, "x": 888},
    ]
    update_matching_records(table, updated_records, ["id"], engine)

    results = select_records_all(table, engine)
    row1 = next(r for r in results if r["id"] == 1)
    row4 = next(r for r in results if r["id"] == 4)
    assert row1["y"] == 999
    assert row4["x"] == 888


@pytest.mark.postgres
def test_update_matching_records_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_update_matching_records(postgres_engine)


@pytest.mark.mysql
def test_update_matching_records_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_update_matching_records(mysql_engine)


@pytest.mark.postgres
def test_update_records_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_update_records(postgres_engine)


@pytest.mark.mysql
def test_update_records_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_update_records(mysql_engine)


@pytest.mark.postgres
def test_update_multiple_records_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_update_multiple_records(postgres_engine)


@pytest.mark.mysql
def test_update_multiple_records_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_update_multiple_records(mysql_engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_update_matching_records_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_update_matching_records(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_update_records_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_update_records(engine)
