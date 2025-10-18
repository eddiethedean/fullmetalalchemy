# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.3.0] - 2025-10-18

### Added
- **Session Module** - Fine-grained transaction control without auto-commit
  - `fullmetalalchemy.session` - Sync session-based operations
  - `fullmetalalchemy.async_api.session` - Async session-based operations
  - Functions: `insert_records`, `update_records`, `delete_records`, `insert_from_table`, `update_matching_records`, `set_column_values`, `delete_records_by_values`, `delete_all_records`
  - Allows multiple operations in a single transaction with manual commit/rollback
  - Perfect for complex transactions requiring ACID guarantees

### Changed
- **Code Architecture** - Refactored CRUD modules to eliminate duplication
  - `insert.py`, `delete.py`, `update.py` now use session module internally
  - Auto-commit functions wrap session functions for convenience
  - `SessionTable` and `AsyncSessionTable` use session module directly
  - Zero code duplication between session and auto-commit functions

### Improved
- **Documentation** - Added comprehensive session module examples
  - Sync session operations with manual commit/rollback
  - Async session operations with AsyncSession
  - Usage patterns for complex transactions
  - API reference for all session functions

## [2.2.3] - 2025-10-18

### Fixed
- Ruff formatting consistency across all files
- GitHub Actions format-check now passes
- All 336 tests verified on both SQLAlchemy 1.4 and 2.0

## [2.2.2] - 2025-10-18

### Fixed
- **Complete SQLAlchemy 1.4 compatibility** for all async features
  - Replaced `Select[T]`, `Insert`, `Update`, `Delete` generic types with `Any`
  - Replaced `ColumnElement[bool]` with `Any`
  - Added proper `async_sessionmaker` wrapper for SQLAlchemy 1.4
    - SQLAlchemy 1.4: Uses `sessionmaker(engine, class_=AsyncSession)`
    - SQLAlchemy 2.0+: Uses native `async_sessionmaker`
  - **Verified**: All 336 tests pass with both SQLAlchemy 1.4.54 and 2.0.44
  - GitHub Actions now pass on all SQLAlchemy versions (1.4.* and 2.x)

## [2.2.1] - 2025-10-18

### Fixed
- Partial SQLAlchemy 1.4 compatibility (superseded by 2.2.2)

## [2.2.0] - 2025-10-18

### Added
- **AsyncTable class** - Pythonic async interface with array-like access patterns
  - Support for `await table[0]`, `await table['column']`, `await table[0:10]`
  - Async iteration with `async for record in table`
  - Context manager support for resource management
  - All CRUD operations as async methods
- **AsyncSessionTable class** - Transaction-safe async operations
  - Async context manager with automatic commit/rollback
  - Manual commit() and rollback() methods
  - All session-based operations support
- **BatchProcessor class** - Efficient sync batch operations
  - Configurable batch sizes
  - Progress tracking support (optional tqdm integration)
  - Error handling modes ('raise' or 'continue')
  - Memory-efficient chunking for large datasets
- **AsyncBatchProcessor class** - Parallel async batch processing
  - Concurrent batch execution with configurable max_concurrent
  - Semaphore-based concurrency control
  - Sequential processing option when order matters
  - Async progress tracking support
- 55 new tests for async classes and batch operations

### Changed
- Updated async_api exports to include AsyncTable and AsyncSessionTable
- Main package now exports BatchProcessor and BatchResult
- Test coverage increased to 84% (336 total tests: 258 sync + 78 async)

### Fixed
- Proper async engine binding in AsyncSessionTable
- Array-like access returns coroutines that can be awaited
- Async iteration protocol properly implemented

### Documentation
- Added AsyncTable usage examples with verified outputs
- Added AsyncSessionTable transaction examples
- Added batch processing examples (sync and async)
- Added parallel processing examples with concurrency control
- Added error handling examples for batch operations

## [2.1.0] - 2025-10-18

### Added
- **🚀 Async/await support** for all CRUD operations via `fullmetalalchemy.async_api`
- Async versions of all select, insert, update, delete, create, and drop operations
- Internal shared logic modules (`_internal/`) to minimize code duplication between sync/async
- Optional async dependencies (`aiosqlite`, `greenlet`, `asyncpg`, `aiomysql`)
- Comprehensive async test suite (23 tests, 70%+ coverage of async code)
- `pytest-asyncio` configuration for async test support
- Async examples in documentation

### Changed
- Async dependencies are now optional (install with `pip install fullmetalalchemy[async]`)
- Internal architecture refactored to share query building logic between sync and async
- Code duplication reduced from ~45% to <5% between sync/async implementations

### Fixed
- All 281 tests pass (258 sync + 23 async)
- Maintained 100% backward compatibility with existing sync API
- Type checking passes with MyPy strict mode for async code
- Ruff linting passes for all async modules

### Documentation
- Added async usage examples to README
- Documented async driver support (aiosqlite, asyncpg, aiomysql)
- Added installation instructions for async extras

## [2.0.0] - 2025-10-18

### Added
- SQLAlchemy 2.0+ compatibility while maintaining 1.4 support
- Python 3.9-3.13 support
- Modern packaging using pyproject.toml (PEP 517/518)
- Ruff for linting and formatting (replacing flake8)
- GitHub Actions CI/CD workflow with matrix testing
- `__all__` exports for better IDE support
- Migration guide (MIGRATION.md)
- Comprehensive type hints with `py.typed` marker
- Context manager support for connection handling
- Better error messages with engine reference fallbacks

### Changed
- **BREAKING**: Minimum Python version is now 3.8 (was implicitly 3.8)
- **BREAKING**: Tables no longer have `.bind` attribute (use `table.info['engine']` or pass engine explicitly)
- Refactored metadata and table reflection for SQLAlchemy 2.0 compatibility
- Updated `MetaData()` construction to avoid deprecated `bind` parameter
- Connection handling now uses context managers (`with engine.connect()`)
- Row to dict conversion now uses `._mapping` for SQLAlchemy 2.0
- Replaced `session.query().delete()` with modern `delete()` statements
- Updated dependencies: `SQLAlchemy>=1.4,<3` (was pinned to `==1.4.46`)
- Modernized dev dependencies (pytest>=8, mypy>=1.10, ruff>=0.6)
- Improved code quality with 200+ automated fixes from ruff
- Test coverage improved to 67%

### Removed
- Removed `setup.py` and `setup.cfg` (configuration moved to pyproject.toml)
- Removed flake8 dependency (replaced with ruff)
- Removed outdated tox configuration

### Fixed
- Engine extraction from sessions now handles both 1.4 and 2.0 patterns
- Table reflection properly handles schemas in both SQLAlchemy versions
- Session commits/rollbacks work consistently across versions
- Proper connection lifecycle management prevents cursor errors

### Documentation
- Updated README with modern examples
- Added SessionTable usage examples
- Added Table class usage examples
- Improved inline documentation

## [1.0.0] - Previous Release

Initial stable release with SQLAlchemy 1.4 support.

