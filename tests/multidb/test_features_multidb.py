"""Multi-database tests for features module."""

import pytest
import sqlalchemy as sa
from sqlalchemy import INTEGER

from fullmetalalchemy.features import (
    get_metadata,
    get_primary_key_names,
    get_session,
    get_table,
    get_table_names,
    primary_key_columns,
    primary_key_names,
    table_exists,
)
from fullmetalalchemy.test_setup import create_test_table


def _test_get_table(engine: sa.engine.Engine) -> None:
    """Test getting a table object."""
    create_test_table(engine)
    table = get_table("xy", engine)
    assert table.name == "xy"
    assert "id" in table.columns


def _test_table_exists(engine: sa.engine.Engine) -> None:
    """Test checking if a table exists."""
    create_test_table(engine)
    assert table_exists("xy", engine)
    assert not table_exists("nonexistent", engine)


def _test_get_table_names(engine: sa.engine.Engine) -> None:
    """Test getting all table names."""
    create_test_table(engine)
    names = get_table_names(engine)
    assert "xy" in names


def _test_primary_key_names(engine: sa.engine.Engine) -> None:
    """Test getting primary key names."""
    table = create_test_table(engine)
    pk_names = primary_key_names(table)
    assert pk_names == ["id"]


def _test_primary_key_columns(engine: sa.engine.Engine) -> None:
    """Test getting primary key columns."""
    table = create_test_table(engine)
    pk_cols = primary_key_columns(table)
    assert len(pk_cols) == 1
    assert pk_cols[0].name == "id"
    assert isinstance(pk_cols[0].type, INTEGER)


def _test_get_primary_key_names(engine: sa.engine.Engine) -> None:
    """Test getting primary key names via function."""
    create_test_table(engine)
    pk_names = get_primary_key_names("xy", engine)
    assert pk_names == ["id"]


def _test_get_session(engine: sa.engine.Engine) -> None:
    """Test getting a session from an engine."""
    session = get_session(engine)
    assert session is not None
    session.close()


def _test_get_metadata(engine: sa.engine.Engine) -> None:
    """Test getting metadata from an engine."""
    create_test_table(engine)
    metadata = get_metadata(engine)
    assert metadata is not None
    assert "xy" in metadata.tables


@pytest.mark.postgres
def test_get_table_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_get_table(postgres_engine)


@pytest.mark.mysql
def test_get_table_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_get_table(mysql_engine)


@pytest.mark.postgres
def test_table_exists_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_table_exists(postgres_engine)


@pytest.mark.mysql
def test_table_exists_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_table_exists(mysql_engine)


@pytest.mark.postgres
def test_primary_key_names_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_primary_key_names(postgres_engine)


@pytest.mark.mysql
def test_primary_key_names_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_primary_key_names(mysql_engine)


@pytest.mark.postgres
def test_get_session_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_get_session(postgres_engine)


@pytest.mark.mysql
def test_get_session_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_get_session(mysql_engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_get_table_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_get_table(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_table_exists_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_table_exists(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_primary_key_names_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_primary_key_names(engine)
