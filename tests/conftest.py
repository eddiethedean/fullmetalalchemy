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
    engine = sa.create_engine("sqlite://")
    table = create_test_table(engine)
    insert_test_records(table, engine)
    return engine, table


# --- PostgreSQL fixtures ---
@pytest.fixture
def postgres_engine() -> sa.engine.Engine:
    """Ephemeral PostgreSQL engine using testing.postgresql.

    Skips if testing.postgresql is unavailable or if local PostgreSQL binaries are missing.
    """
    try:
        import testing.postgresql as tpostgresql  # type: ignore
    except Exception as exc:  # pragma: no cover - dependency not present in base runs
        pytest.skip(f"testing.postgresql not available: {exc}")

    try:
        with tpostgresql.Postgresql() as pg:
            # Build SQLAlchemy URL using psycopg driver
            url = pg.url().replace("postgresql://", "postgresql+psycopg://")
            engine = sa.create_engine(url)
            yield engine
            engine.dispose()
    except Exception as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL server could not start: {exc}")


@pytest.fixture
def engine_and_table_postgres(
    postgres_engine: sa.engine.Engine,
) -> Tuple[sa.engine.Engine, sa.Table]:
    table = create_test_table(postgres_engine)
    insert_test_records(table, postgres_engine)
    return postgres_engine, table


# --- MySQL fixtures ---
@pytest.fixture
def mysql_engine() -> sa.engine.Engine:
    """Ephemeral MySQL engine using testing.mysqld.

    Skips if testing.mysqld is unavailable.
    """
    try:
        import testing.mysqld as tmysqld  # type: ignore
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"testing.mysqld not available: {exc}")

    try:
        with tmysqld.Mysqld() as mysql:
            dsn = mysql.dsn()
            user = dsn.get("user", "root")
            password = dsn.get("password", "")
            host = dsn.get("host", "127.0.0.1")
            port = dsn.get("port")
            db = dsn.get("db", "test")
            auth = f"{user}:{password}@" if password else f"{user}@"
            url = f"mysql+pymysql://{auth}{host}:{port}/{db}"
            engine = sa.create_engine(url)
            yield engine
            engine.dispose()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"MySQL server could not start: {exc}")


@pytest.fixture
def engine_and_table_mysql(mysql_engine: sa.engine.Engine) -> Tuple[sa.engine.Engine, sa.Table]:
    table = create_test_table(mysql_engine)
    insert_test_records(table, mysql_engine)
    return mysql_engine, table
