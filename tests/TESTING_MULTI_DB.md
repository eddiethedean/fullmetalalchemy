# Multi-Database Testing (PostgreSQL & MySQL)

This project supports ephemeral PostgreSQL and MySQL tests using `testing.postgresql` and `testing.mysqld`, modeled after the pandalchemy setup (`https://github.com/eddiethedean/pandalchemy`).

## Prerequisites

- Python extras installed:
  - `pip install .[testing]`
- PostgreSQL server binaries (`initdb`, `postgres`) on PATH for `testing.postgresql`.
  - On macOS: `brew install postgresql`
  - On Ubuntu (CI): preinstalled on GitHub-hosted runners

## Running Tests

Run PostgreSQL-only tests:

```bash
pytest -m postgres
```

Run MySQL-only tests:

```bash
pytest -m mysql
```

Run the shared multi-DB tests:

```bash
pytest -m multidb
```

You can also execute the full suite including SQLite (default) plus multi-DB markers:

```bash
pytest -m "postgres or mysql or multidb"
``;

## Notes

- Ephemeral servers are started automatically by pytest fixtures in `tests/conftest.py` and disposed after each test.
- If a dependency or local server binary is missing, tests are skipped with a message explaining why.


