"""
Async session-based operations for FullmetalAlchemy.

This module contains all async session-based CRUD operations that do not auto-commit,
allowing for fine-grained transaction control with AsyncSession.

Examples
--------
>>> import fullmetalalchemy as fa
>>> from fullmetalalchemy.async_api import session
>>>
>>> async def main():
...     engine = fa.async_api.create_async_engine('sqlite+aiosqlite:///data.db')
...     async_session = AsyncSession(engine, expire_on_commit=False)
...     table = await fa.async_api.get_table('users', engine)
...
...     # Perform multiple operations
...     await session.insert_records(table, records1, async_session)
...     await session.update_records(table, records2, async_session)
...     await session.delete_records(table, 'id', [1, 2], async_session)
...
...     # Commit all changes
...     await async_session.commit()
"""

import typing as _t

import sqlalchemy as _sa
from sqlalchemy.ext.asyncio import AsyncSession as _AsyncSession

import fullmetalalchemy.features as _features
import fullmetalalchemy.types as _types

# ================== INSERT OPERATIONS ==================


async def insert_records(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _AsyncSession,
) -> None:
    """
    Insert records into a table using an async session.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to insert into. Can be a Table object or table name string.
    records : Sequence[Record]
        Records to insert.
    session : AsyncSession
        The async session to use.

    Returns
    -------
    None

    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> from fullmetalalchemy.async_api import session as async_session
    >>>
    >>> async def example():
    ...     engine = fa.async_api.create_async_engine('sqlite+aiosqlite:///data.db')
    ...     sess = AsyncSession(engine, expire_on_commit=False)
    ...     table = await fa.async_api.get_table('users', engine)
    ...
    ...     records = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
    ...     await async_session.insert_records(table, records, sess)
    ...     await sess.commit()
    """
    if isinstance(table, str):
        # Get table from session's bind
        engine = session.get_bind()
        table = await _features.get_table(table, engine)

    stmt = _sa.insert(table).values(records)
    await session.execute(stmt)


async def insert_from_table(
    table1: _t.Union[_sa.Table, str],
    table2: _t.Union[_sa.Table, str],
    session: _AsyncSession,
) -> None:
    """
    Insert all rows from table1 into table2 using an async session.

    Parameters
    ----------
    table1 : Union[sqlalchemy.Table, str]
        Source table to copy from.
    table2 : Union[sqlalchemy.Table, str]
        Destination table to insert into.
    session : AsyncSession
        The async session to use.

    Returns
    -------
    None
    """
    engine = session.get_bind()

    if isinstance(table1, str):
        table1 = await _features.get_table(table1, engine)
    if isinstance(table2, str):
        table2 = await _features.get_table(table2, engine)

    stmt = table2.insert().from_select(table1.columns.keys(), _sa.select(table1))
    await session.execute(stmt)


# ================== DELETE OPERATIONS ==================


async def delete_records(
    table: _t.Union[_sa.Table, str],
    column_name: str,
    values: _t.Sequence[_t.Any],
    session: _AsyncSession,
) -> None:
    """
    Delete records from table that match values in the specified column.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to delete from.
    column_name : str
        Column name to match.
    values : Sequence
        Values to match for deletion.
    session : AsyncSession
        The async session to use.

    Returns
    -------
    None
    """
    if isinstance(table, str):
        engine = session.get_bind()
        table = await _features.get_table(table, engine)

    stmt = _sa.delete(table).where(table.c[column_name].in_(values))
    await session.execute(stmt)


async def delete_records_by_values(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _AsyncSession,
) -> None:
    """
    Delete records from table that match all column values in the provided records.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to delete from.
    records : Sequence[Record]
        Records with column-value pairs to match for deletion.
    session : AsyncSession
        The async session to use.

    Returns
    -------
    None
    """
    if isinstance(table, str):
        engine = session.get_bind()
        table = await _features.get_table(table, engine)

    for record in records:
        stmt = _sa.delete(table)
        for column, value in record.items():
            stmt = stmt.where(table.c[column] == value)
        await session.execute(stmt)


async def delete_all_records(
    table: _t.Union[_sa.Table, str],
    session: _AsyncSession,
) -> None:
    """
    Delete all records from table.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to delete from.
    session : AsyncSession
        The async session to use.

    Returns
    -------
    None
    """
    if isinstance(table, str):
        engine = session.get_bind()
        table = await _features.get_table(table, engine)

    stmt = _sa.delete(table)
    await session.execute(stmt)


# ================== UPDATE OPERATIONS ==================


async def update_records(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _AsyncSession,
    match_column_names: _t.Optional[_t.Sequence[str]] = None,
) -> None:
    """
    Update records in table using an async session.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to update.
    records : Sequence[Record]
        Records with updated values. Must include primary key or match columns.
    session : AsyncSession
        The async session to use.
    match_column_names : Optional[Sequence[str]], optional
        Column names to match on. If None, uses primary key.

    Returns
    -------
    None
    """
    if isinstance(table, str):
        engine = session.get_bind()
        table = await _features.get_table(table, engine)

    # Get primary key if match_column_names not provided
    if match_column_names is None:
        pk_cols = [col.name for col in table.primary_key.columns]
        if not pk_cols:
            raise ValueError("Must provide match_column_names if table has no primary key.")
        match_column_names = pk_cols

    for record in records:
        # Build WHERE clause from match columns
        stmt = _sa.update(table)
        for col_name in match_column_names:
            if col_name in record:
                stmt = stmt.where(table.c[col_name] == record[col_name])

        # Build SET clause from remaining columns
        update_values = {k: v for k, v in record.items() if k not in match_column_names}
        if update_values:
            stmt = stmt.values(**update_values)
            await session.execute(stmt)


async def update_matching_records(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    match_column_names: _t.Sequence[str],
    session: _AsyncSession,
) -> None:
    """
    Update records in table using specified match columns.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to update.
    records : Sequence[Record]
        Records with updated values.
    match_column_names : Sequence[str]
        Column names to use for matching records.
    session : AsyncSession
        The async session to use.

    Returns
    -------
    None
    """
    await update_records(table, records, session, match_column_names)


async def set_column_values(
    table: _t.Union[_sa.Table, str],
    column_name: str,
    value: _t.Any,
    session: _AsyncSession,
) -> None:
    """
    Set all rows in a column to the same value.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to update.
    column_name : str
        The column to update.
    value : Any
        The value to set.
    session : AsyncSession
        The async session to use.

    Returns
    -------
    None
    """
    if isinstance(table, str):
        engine = session.get_bind()
        table = await _features.get_table(table, engine)

    stmt = _sa.update(table).values(**{column_name: value})
    await session.execute(stmt)

