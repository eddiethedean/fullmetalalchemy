"""Multi-database tests for create operations."""

import pytest
import sqlalchemy as sa

import fullmetalalchemy as fa
from fullmetalalchemy.create import create_table, create_table_from_records
from fullmetalalchemy.features import table_exists


def _test_create_table_basic(engine: sa.engine.Engine) -> None:
    """Test creating a basic table."""
    # Use integer-only columns to avoid MySQL VARCHAR length requirement
    create_table(
        table_name="test_table",
        column_names=["id", "x", "y"],
        column_types=[int, int, int],
        primary_key=["id"],
        engine=engine,
        if_exists="replace",
    )

    assert table_exists("test_table", engine)
    table = fa.features.get_table("test_table", engine)
    assert len(table.columns) == 3
    assert "id" in table.columns
    assert "x" in table.columns
    assert "y" in table.columns


def _test_create_table_from_records(engine: sa.engine.Engine) -> None:
    """Test creating a table from records."""
    # Use integer-only records to avoid MySQL VARCHAR length requirement
    records = [
        {"id": 1, "x": 10, "y": 20},
        {"id": 2, "x": 30, "y": 40},
        {"id": 3, "x": 50, "y": 60},
    ]
    table = create_table_from_records(
        table_name="users",
        records=records,
        primary_key=["id"],
        engine=engine,
        if_exists="replace",
    )

    assert table_exists("users", engine)
    selected = fa.select.select_records_all(table, engine)
    assert len(selected) == 3
    assert selected[0]["id"] == 1


def _test_create_table_composite_key(engine: sa.engine.Engine) -> None:
    """Test creating a table with composite primary key."""
    # Use integer-only columns to avoid MySQL VARCHAR length requirement
    create_table(
        table_name="composite_test",
        column_names=["user_id", "org_id", "value"],
        column_types=[int, int, int],
        primary_key=["user_id", "org_id"],
        engine=engine,
        if_exists="replace",
    )

    assert table_exists("composite_test", engine)
    table = fa.features.get_table("composite_test", engine)
    pk_names = fa.features.primary_key_names(table)
    assert set(pk_names) == {"user_id", "org_id"}


@pytest.mark.postgres
def test_create_table_basic_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_create_table_basic(postgres_engine)


@pytest.mark.mysql
def test_create_table_basic_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_create_table_basic(mysql_engine)


@pytest.mark.postgres
def test_create_table_from_records_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_create_table_from_records(postgres_engine)


@pytest.mark.mysql
def test_create_table_from_records_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_create_table_from_records(mysql_engine)


@pytest.mark.postgres
def test_create_table_composite_key_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_create_table_composite_key(postgres_engine)


@pytest.mark.mysql
def test_create_table_composite_key_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_create_table_composite_key(mysql_engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_create_table_basic_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_create_table_basic(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_create_table_from_records_multidb(
    request: pytest.FixtureRequest, engine_name: str
) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_create_table_from_records(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_create_table_composite_key_multidb(
    request: pytest.FixtureRequest, engine_name: str
) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_create_table_composite_key(engine)

