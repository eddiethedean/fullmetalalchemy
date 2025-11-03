"""Multi-database tests for drop operations."""

import pytest
import sqlalchemy as sa

from fullmetalalchemy.drop import drop_table
from fullmetalalchemy.features import table_exists
from fullmetalalchemy.test_setup import create_test_table


def _test_drop_table(engine: sa.engine.Engine) -> None:
    """Test dropping a table."""
    table = create_test_table(engine)
    assert table_exists("xy", engine)

    drop_table(table, engine)
    assert not table_exists("xy", engine)


def _test_drop_table_by_name(engine: sa.engine.Engine) -> None:
    """Test dropping a table by string name."""
    create_test_table(engine)
    assert table_exists("xy", engine)

    drop_table("xy", engine)
    assert not table_exists("xy", engine)


def _test_drop_table_if_exists(engine: sa.engine.Engine) -> None:
    """Test dropping a nonexistent table with if_exists=True."""
    # Should not raise error
    drop_table("nonexistent", engine, if_exists=True)
    assert not table_exists("nonexistent", engine)


@pytest.mark.postgres
def test_drop_table_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_drop_table(postgres_engine)


@pytest.mark.mysql
def test_drop_table_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_drop_table(mysql_engine)


@pytest.mark.postgres
def test_drop_table_by_name_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_drop_table_by_name(postgres_engine)


@pytest.mark.mysql
def test_drop_table_by_name_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_drop_table_by_name(mysql_engine)


@pytest.mark.postgres
def test_drop_table_if_exists_postgres(postgres_engine: sa.engine.Engine) -> None:
    _test_drop_table_if_exists(postgres_engine)


@pytest.mark.mysql
def test_drop_table_if_exists_mysql(mysql_engine: sa.engine.Engine) -> None:
    _test_drop_table_if_exists(mysql_engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_drop_table_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_drop_table(engine)


@pytest.mark.multidb
@pytest.mark.parametrize("engine_name", ["postgres_engine", "mysql_engine"])
def test_drop_table_by_name_multidb(request: pytest.FixtureRequest, engine_name: str) -> None:
    engine: sa.engine.Engine = request.getfixturevalue(engine_name)
    _test_drop_table_by_name(engine)

