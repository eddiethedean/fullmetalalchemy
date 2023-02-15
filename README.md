![FullmetalAlchemy Logo](https://raw.githubusercontent.com/eddiethedean/fullmetalalchemy/main/docs/sqllogo.png)
-----------------

# FullmetalAlchemy: Easy to use functions for sql table changes
[![PyPI Latest Release](https://img.shields.io/pypi/v/fullmetalalchemy.svg)](https://pypi.org/project/fullmetalalchemy/)
![Tests](https://github.com/eddiethedean/fullmetalalchemy/actions/workflows/tests.yml/badge.svg)

## What is it?

**FullmetalAlchemy** is a Python package with easy to use functions for inserting, deleting, updating, selecting records in SQL tables.

## Where to get it
The source code is currently hosted on GitHub at:
https://github.com/eddiethedean/fullmetalalchemy

```sh
# PyPI
pip install fullmetalalchemy
```

## Dependencies
- [SQLAlchemy - Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL](https://www.sqlalchemy.org/)


## Example
```sh
import fullmetalalchemy as fa

# Create SqlAlchemy engine to connect to database.
engine = fa.create_engine('sqlite:///foo.db')

# Get a SqlAlchemy table to pass to FullmetalAlchemy functions
table = fa.features.get_table('xy', engine)

# Select records
fa.select.select_records_all(table)
[{'id': 1, 'x': 1, 'y': 2}, {'id': 1, 'x': 2, 'y': 3}]

# Insert records
fa.insert.insert_records(table, [{'id': 3, 'x': 3, 'y': 4}, {'id': 4, 'x': 5, 'y': 6}])

# Delete records
fa.delete.delete_records(table, 'id', [1, 3])

# Update records
fa.update.update_records(table, [{'id': 1, 'x': 11, 'y': 22}, {'id': 1, 'x': 23, 'y': 34}], 'id')
```