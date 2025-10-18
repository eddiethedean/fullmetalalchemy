# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

