"""Multi-database tests for select operations."""

import pytest
import sqlalchemy as sa

from fullmetalalchemy.select import (
    select_column_value_by_index,
    select_column_values_all,
    select_column_values_by_slice,
    select_existing_values,
    select_record_by_index,
    select_record_by_primary_key,
    select_records_all,
    select_records_by_primary_keys,
    select_records_chunks,
    select_records_slice,
)
from fullmetalalchemy.test_setup import create_test_table, insert_test_records


def _test_select_records_all(engine: sa.engine.Engine) -> None:
    """Test selecting all records."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    results = select_records_all(table, engine)
    assert len(results) == 4
    assert results[0]["id"] == 1
    assert results[3]["id"] == 4


def _test_select_records_chunks(engine: sa.engine.Engine) -> None:
    """Test selecting records in chunks."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    chunks = list(select_records_chunks(table, engine, chunksize=2))
    assert len(chunks) == 2
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 2


def _test_select_records_slice(engine: sa.engine.Engine) -> None:
    """Test selecting records by slice."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    results = select_records_slice(table, 1, 3, engine)
    assert len(results) == 2
    assert results[0]["id"] == 2
    assert results[1]["id"] == 3


def _test_select_record_by_primary_key(engine: sa.engine.Engine) -> None:
    """Test selecting a record by primary key."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    record = select_record_by_primary_key(table, {"id": 2}, engine)
    assert record is not None
    assert record["id"] == 2
    assert record["x"] == 2
    assert record["y"] == 4


def _test_select_records_by_primary_keys(engine: sa.engine.Engine) -> None:
    """Test selecting records by multiple primary keys."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    records = select_records_by_primary_keys(table, [{"id": 1}, {"id": 3}], engine)
    assert len(records) == 2
    ids = {r["id"] for r in records}
    assert ids == {1, 3}


def _test_select_column_values_all(engine: sa.engine.Engine) -> None:
    """Test selecting all values from a column."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    values = select_column_values_all(table, "x", engine)
    assert set(values) == {1, 2, 4, 8}


def _test_select_column_values_by_slice(engine: sa.engine.Engine) -> None:
    """Test selecting column values by slice."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    values = select_column_values_by_slice(table, "y", 1, 3, engine)
    assert len(values) == 2
    assert 4 in values
    assert 8 in values


def _test_select_existing_values(engine: sa.engine.Engine) -> None:
    """Test selecting existing values from a column."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    existing = select_existing_values(table, "y", [2, 4, 10, 11], engine)
    assert set(existing) == {2, 4, 11}


def _test_select_record_by_index(engine: sa.engine.Engine) -> None:
    """Test selecting a record by index."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    record = select_record_by_index(table, 1, engine)
    assert record is not None
    assert record["id"] == 2


def _test_select_column_value_by_index(engine: sa.engine.Engine) -> None:
    """Test selecting a column value by index."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    value = select_column_value_by_index(table, "x", 0, engine)
    assert value == 1


@pytest.mark.postgres
def test_select_records_all_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_select_records_all(postgres_engine)


@pytest.mark.mysql
def test_select_records_all_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_select_records_all(mysql_engine)


@pytest.mark.postgres
def test_select_records_chunks_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_select_records_chunks(postgres_engine)


@pytest.mark.mysql
def test_select_records_chunks_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_select_records_chunks(mysql_engine)


@pytest.mark.postgres
def test_select_records_slice_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_select_records_slice(postgres_engine)


@pytest.mark.mysql
def test_select_records_slice_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_select_records_slice(mysql_engine)


@pytest.mark.postgres
def test_select_record_by_primary_key_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_select_record_by_primary_key(postgres_engine)


@pytest.mark.mysql
def test_select_record_by_primary_key_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_select_record_by_primary_key(mysql_engine)


@pytest.mark.postgres
def test_select_records_by_primary_keys_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_select_records_by_primary_keys(postgres_engine)


@pytest.mark.mysql
def test_select_records_by_primary_keys_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_select_records_by_primary_keys(mysql_engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_select_records_all_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_select_records_all(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_select_record_by_primary_key_multidb(
    request: pytest.FixtureRequest, engine_name: str
) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_select_record_by_primary_key(engine)
