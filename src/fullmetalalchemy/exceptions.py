"""
Module for custom exceptions and exception handling.
"""
import typing as _t

import sqlalchemy as _sa
import sqlalchemy.engine as _sa_engine
import sqlalchemy.orm.session as _sa_session

import fullmetalalchemy.features as _features
import fullmetalalchemy.types as _types

Connection = _t.Union[_sa_session.Session, _sa_engine.Engine]


class ForceFail(Exception):
    pass


class SliceError(IndexError):
    pass


class MissingPrimaryKey(Exception):
    def __init__(self, message='Table must have primary key. Use alterize.create_primary_key to add a primary key to your table.', errors=None):
        super().__init__(message, errors)


def rollback_on_exception(method):
    def inner_method(self, *args, **kwargs):
        try:
            method(self, *args, **kwargs)
        except Exception as e:
            self.rollback()
            raise e
    return inner_method


def check_for_engine(sa_table, engine) -> _sa_engine.Engine:
    if engine is None:
        engine = sa_table.bind
    if engine is None:
        raise ValueError('sa_table must be bound to engine or pass engine parameter.')
    return engine


def convert_table_engine(
        sa_table: _t.Union[_sa.Table, str],
        engine: _t.Optional[_sa_engine.Engine]
) -> _t.Tuple[_sa.Table, _sa_engine.Engine]:
    """Convert possible str table name to SqlAlchemy Table.
       Pull out SqlAlchemy Engine from SqlAlchemy Table if engine is None.
       Return the table and engine.
    """
    sa_table = _features.str_to_table(sa_table, engine)
    engine = check_for_engine(sa_table, engine)
    return sa_table, engine


def convert_table_connection(
        sa_table: _t.Union[_sa.Table, str],
        connection: _t.Optional[_types.SqlConnection]
) -> _t.Tuple[_sa.Table, _types.SqlConnection]:
    """Convert possible str table name to SqlAlchemy Table.
       Pull out SqlAlchemy Engine from SqlAlchemy Table if engine is None.
       Return the table and engine.
    """
    sa_table = _features.str_to_table(sa_table, connection)
    if type(connection) is _sa_session.Session:
        return sa_table, connection
    connection = check_for_engine(sa_table, connection)
    return sa_table, connection


