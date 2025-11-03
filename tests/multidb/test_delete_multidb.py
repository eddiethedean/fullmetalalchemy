"""Multi-database tests for delete operations."""

import pytest
import sqlalchemy as sa

from fullmetalalchemy.delete import (
    delete_all_records,
    delete_records,
    delete_records_by_primary_keys,
)
from fullmetalalchemy.select import select_records_all
from fullmetalalchemy.test_setup import create_test_table, insert_test_records


def _test_delete_records(engine: sa.engine.Engine) -> None:
    """Test deleting records by column values."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    delete_records(table, "id", [1, 3], engine)

    results = select_records_all(table, engine)
    ids = {r["id"] for r in results}
    assert ids == {2, 4}


def _test_delete_records_by_primary_keys(engine: sa.engine.Engine) -> None:
    """Test deleting records by primary keys."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    # For single-column primary key, pass list of values
    delete_records_by_primary_keys(table, [2, 4], engine)

    results = select_records_all(table, engine)
    ids = {r["id"] for r in results}
    assert ids == {1, 3}


def _test_delete_all_records(engine: sa.engine.Engine) -> None:
    """Test deleting all records from a table."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    delete_all_records(table, engine)

    results = select_records_all(table, engine)
    assert len(results) == 0


def _test_delete_single_record(engine: sa.engine.Engine) -> None:
    """Test deleting a single record."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    delete_records(table, "id", [1], engine)

    results = select_records_all(table, engine)
    assert len(results) == 3
    ids = {r["id"] for r in results}
    assert 1 not in ids


@pytest.mark.postgres
def test_delete_records_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_delete_records(postgres_engine)


@pytest.mark.mysql
def test_delete_records_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_delete_records(mysql_engine)


@pytest.mark.postgres
def test_delete_records_by_primary_keys_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_delete_records_by_primary_keys(postgres_engine)


@pytest.mark.mysql
def test_delete_records_by_primary_keys_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_delete_records_by_primary_keys(mysql_engine)


@pytest.mark.postgres
def test_delete_all_records_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_delete_all_records(postgres_engine)


@pytest.mark.mysql
def test_delete_all_records_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_delete_all_records(mysql_engine)


@pytest.mark.postgres
def test_delete_single_record_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_delete_single_record(postgres_engine)


@pytest.mark.mysql
def test_delete_single_record_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_delete_single_record(mysql_engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_delete_records_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_delete_records(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_delete_all_records_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_delete_all_records(engine)

