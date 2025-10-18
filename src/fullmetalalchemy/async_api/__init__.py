"""Async API for FullmetalAlchemy.

This package provides async/await versions of all FullmetalAlchemy operations
using SQLAlchemy's async engine capabilities.

Usage
-----
```python
import asyncio
from fullmetalalchemy import async_api

async def main():
    engine = async_api.create_async_engine('sqlite+aiosqlite:///data.db')
    records = await async_api.select.select_records_all(table, engine)
    print(records)

asyncio.run(main())
```

Modules
-------
- select: Async select operations
- insert: Async insert operations
- update: Async update operations
- delete: Async delete operations
- create: Async table creation and create_async_engine
- drop: Async drop operations

Classes
-------
- AsyncTable: Async table operations (coming soon)
- AsyncSessionTable: Async transaction management (coming soon)
"""

from fullmetalalchemy.async_api import create, delete, drop, insert, select, update

try:
    from sqlalchemy.ext.asyncio import create_async_engine
except ImportError:

    def create_async_engine(*args, **kwargs):  # type: ignore
        raise ImportError(
            "Async support requires SQLAlchemy with asyncio extra. "
            "Install with: pip install 'SQLAlchemy[asyncio]' aiosqlite"
        )


__all__ = [
    "create",
    "create_async_engine",
    "delete",
    "drop",
    "insert",
    "select",
    "update",
]
