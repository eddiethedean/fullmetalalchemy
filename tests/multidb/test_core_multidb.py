import pytest
import sqlalchemy as sa

import fullmetalalchemy as fa
from fullmetalalchemy.delete import delete_records
from fullmetalalchemy.insert import insert_records
from fullmetalalchemy.select import select_records_all
from fullmetalalchemy.test_setup import create_test_table, insert_test_records
from fullmetalalchemy.update import update_matching_records


def _run_core_checks(engine: sa.engine.Engine) -> None:
    """Test basic CRUD operations."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    rows = select_records_all(table, engine)
    assert len(rows) == 4

    # Update a row
    update_matching_records(table, [{"id": 2, "y": 99}], ["id"], engine)
    rows = select_records_all(table, engine)
    row2 = next(r for r in rows if r["id"] == 2)
    assert row2["y"] == 99

    # Delete a row
    delete_records(table, "id", [1], engine)
    rows = select_records_all(table, engine)
    assert {r["id"] for r in rows} == {2, 3, 4}


def _test_full_workflow(engine: sa.engine.Engine) -> None:
    """Test a complete workflow: create, insert, select, update, delete."""
    # Create table
    table = create_test_table(engine)

    # Insert initial data
    insert_test_records(table, engine)

    # Verify initial data
    rows = select_records_all(table, engine)
    assert len(rows) == 4

    # Insert additional row
    insert_records(table, [{"id": 5, "x": 16, "y": 32}], engine)
    rows = select_records_all(table, engine)
    assert len(rows) == 5
    assert any(r["id"] == 5 for r in rows)

    # Update multiple rows
    update_matching_records(
        table,
        [{"id": 1, "x": 100}, {"id": 3, "y": 200}],
        ["id"],
        engine,
    )
    rows = select_records_all(table, engine)
    row1 = next(r for r in rows if r["id"] == 1)
    row3 = next(r for r in rows if r["id"] == 3)
    assert row1["x"] == 100
    assert row3["y"] == 200

    # Delete multiple rows
    delete_records(table, "id", [2, 4], engine)
    rows = select_records_all(table, engine)
    ids = {r["id"] for r in rows}
    assert ids == {1, 3, 5}


def _test_table_features(engine: sa.engine.Engine) -> None:
    """Test table metadata and features."""
    table = create_test_table(engine)
    insert_test_records(table, engine)

    # Test primary key detection
    pk_names = fa.features.primary_key_names(table)
    assert pk_names == ["id"]

    # Test table exists check
    assert fa.features.table_exists("xy", engine)

    # Test getting table
    retrieved_table = fa.features.get_table("xy", engine)
    assert retrieved_table.name == "xy"

    # Test table names list
    table_names = fa.features.get_table_names(engine)
    assert "xy" in table_names


@pytest.mark.postgres
def test_core_postgres(postgres_engine: sa.engine.Engine) -> None:
    _run_core_checks(postgres_engine)


@pytest.mark.mysql
def test_core_mysql(mysql_engine: sa.engine.Engine) -> None:
    _run_core_checks(mysql_engine)


@pytest.mark.postgres
def test_full_workflow_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_full_workflow(postgres_engine)


@pytest.mark.mysql
def test_full_workflow_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_full_workflow(mysql_engine)


@pytest.mark.postgres
def test_table_features_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_table_features(postgres_engine)


@pytest.mark.mysql
def test_table_features_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_table_features(mysql_engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_core_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _run_core_checks(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_full_workflow_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_full_workflow(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_table_features_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_table_features(engine)

