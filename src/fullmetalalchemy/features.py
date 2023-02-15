"""
Functions for getting SQL table features and SqlAlchemy ORM objects.
"""

import typing as _t

import sqlalchemy as _sa
import sqlalchemy.orm.session as _sa_session
import sqlalchemy.ext.automap as _sa_automap
import sqlalchemy.engine as _sa_engine
from sqlalchemy.orm.decl_api import DeclarativeMeta as _DeclarativeMeta

import fullmetalalchemy.types as _types
import fullmetalalchemy.exceptions as _ex


def primary_key_columns(
    sa_table: _sa.Table
) ->  _t.List[_sa.Column]:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.primary_key_columns(table)
    [Column('id', INTEGER(), table=<xy>, primary_key=True, nullable=False)]
    """
    return list(sa_table.primary_key.columns)


def primary_key_names(
    sa_table: _sa.Table
) ->  _t.List[str]:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.primary_key_names(table)
    ['id']
    """
    return [c.name for c in primary_key_columns(sa_table)]


def get_connection(
    connection: _t.Union[_types.SqlConnection, _sa_session.Session]
) -> _types.SqlConnection:
    """
    If session connection is passed, return engine connection
    else, return connection

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine = sz.create_engine('sqlite:///data/test.db')
    >>> session = sz.features.get_session(engine)
    >>> sz.features.get_connection(session)
    <sqlalchemy.engine.base.Connection at 0x7f9568064550>
    """
    if isinstance(connection, _sa_session.Session):
        return connection.connection()
    return connection


def get_metadata(
    connection: _types.SqlConnection,
    schema: _t.Optional[str] = None
) -> _sa.MetaData:
    """
    Get metadata object from sql connection.

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine = sz.create_engine('sqlite:///data/test.db')
    >>> sz.features.get_metadata(engine)
    MetaData(bind=Engine(sqlite:///data/test.db))
    """
    return _sa.MetaData(bind=connection, schema=schema)


def get_table(
    name: str,
    connection: _types.SqlConnection,
    schema: _t.Optional[str] = None
) -> _sa.Table:
    """
    Get sqlalchemy Table object.

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine = sz.create_engine('sqlite:///data/test.db')
    >>> sz.get_table('xy', engine)
    Table('xy', MetaData(bind=Engine(sqlite:///data/test.db)),
        Column('id', INTEGER(), table=<xy>, primary_key=True, nullable=False),
        Column('x', INTEGER(), table=<xy>),
        Column('y', INTEGER(), table=<xy>), schema=None)
    """
    metadata = get_metadata(connection, schema)
    autoload_with = get_connection(connection)
    return _sa.Table(name,
                 metadata,
                 autoload=True,
                 autoload_with=autoload_with,
                 extend_existing=True,
                 schema=schema)


def get_engine_table(
    connection_string: str,
    table_name: str,
    schema: _t.Optional[str] = None
) -> _t.Tuple[_sa_engine.Engine, _sa.Table]:
    """
    Get both the engine and sql table with one function.

    Example
    -------
    >>> import sqlalchemize as sz

    >>> sz.get_engine_table('sqlite:///data/test.db', 'xy')
    (
     Engine(sqlite:///data/test.db),
     Table('xy', MetaData(bind=Engine(sqlite:///data/test.db)),
        Column('id', INTEGER(), table=<xy>, primary_key=True, nullable=False),
        Column('x', INTEGER(), table=<xy>),
        Column('y', INTEGER(), table=<xy>), schema=None)
    )
    """
    engine = _sa.create_engine(connection_string)
    table = get_table(table_name, engine, schema)
    return engine, table


def get_class(
    name: str,
    connection: _t.Union[_types.SqlConnection, _sa_session.Session],
    schema: _t.Optional[str] = None
) -> _DeclarativeMeta:
    """
    Get sqlalchemy table class object.

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine = sz.create_engine('sqlite:///data/test.db')
    >>> sz.features.get_class('xy', engine)
    sqlalchemy.ext.automap.xy
    """
    metadata = get_metadata(connection, schema)
    connection = get_connection(connection)

    metadata.reflect(connection, only=[name], schema=schema)
    Base = _sa_automap.automap_base(metadata=metadata)
    Base.prepare()
    if name not in Base.classes:
        raise _ex.MissingPrimaryKey()
    return Base.classes[name]


def get_session(
    engine: _sa_engine.Engine
) -> _sa_session.Session:
    """
    Start a session from engine then return session.

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine = sz.create_engine('sqlite:///data/test.db')
    >>> sz.features.get_session(engine)
    <sqlalchemy.orm.session.Session at 0x7f95999e1eb0>
    """
    return _sa_session.Session(engine)


def get_column(
    sa_table: _sa.Table,
    column_name: str
) -> _sa.Column:
    """
    Get a sqlalchemy column object from a sqlalchemy table.

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.get_column(table, 'x')
    Column('x', INTEGER(), table=<xy>)
    """
    return sa_table.c[column_name]


def get_table_constraints(sa_table: _sa.Table) -> set:
    """
    Get sql table constraints.

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.get_table_constraints(table)
    {PrimaryKeyConstraint(Column('id', INTEGER(), table=<xy>, primary_key=True, nullable=False))}
    """
    return sa_table.constraints


def get_primary_key_constraints(
    sa_table: _sa.Table
) -> _t.Tuple[str,  _t.List[str]]:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.get_primary_key_constraints(table)
    (None, ['id'])
    """
    cons = get_table_constraints(sa_table)
    for con in cons:
        if isinstance(con, _sa.PrimaryKeyConstraint):
            return con.name, [col.name for col in con.columns]
    return tuple()


def missing_primary_key(
    sa_table: _sa.Table,
) -> bool:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.missing_primary_key(table)
    False
    """
    pks = get_primary_key_constraints(sa_table)
    return pks[1] == []


def get_column_types(sa_table: _sa.Table) -> dict:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.get_column_types(table)
    {'id': INTEGER(), 'x': INTEGER(), 'y': INTEGER()}
    """
    return {c.name: c.type for c in sa_table.c}


def get_column_names(sa_table: _sa.Table) ->  _t.List[str]:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.get_column_names(table)
    ['id', 'x', 'y']
    """
    return [c.name for c in sa_table.columns]


def get_table_names(
    engine: _sa_engine.Engine,
    schema: _t.Optional[str] = None
) ->  _t.List[str]:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine = sz.create_engine('sqlite:///data/test.db')
    >>> sz.features.get_table_names(engine)
    ['xy']
    """
    return _sa.inspect(engine).get_table_names(schema)


def get_row_count(
    sa_table: _sa.Table,
    session: _t.Optional[_types.SqlConnection] = None
) -> int:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.get_row_count(table)
    0
    """
    session = _ex.check_for_engine(sa_table, session)
    col_name = get_column_names(sa_table)[0]
    col = get_column(sa_table, col_name)
    result = session.execute(_sa.func.count(col)).scalar()
    return result if result is not None else 0


def get_schemas(engine: _sa_engine.Engine) ->  _t.List[str]:
    """
    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine = sz.create_engine('sqlite:///data/test.db')
    >>> sz.features.get_schemas(engine)
    ['main']
    """
    insp = _sa.inspect(engine)
    return insp.get_schema_names()


def _get_where_clause(
    sa_table: _sa.Table,
    record: _types.Record
) ->  _t.List[bool]:
    return [sa_table.c[key_name]==key_value for key_name, key_value in record.items()]


def tables_metadata_equal(
    sa_table1: _sa.Table,
    sa_table2: _sa.Table
) -> bool:
    """
    Check if two sql tables have the same metadata.

    Example
    -------
    >>> import sqlalchemize as sz

    >>> engine, table = sz.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> sz.features.tables_metadata_equal(table, table)
    True
    """
    if sa_table1.name != sa_table2.name: return False

    column_types1 = get_column_types(sa_table1)
    column_types2 = get_column_types(sa_table2)
    # if column_types1 != column_types2: return False

    table1_keys = primary_key_names(sa_table1)
    table2_keys = primary_key_names(sa_table2)
    if set(table1_keys) != set(table2_keys): return False

    return True


def str_to_table(
    table_name: _t.Union[str, _sa.Table],
    connection: _t.Optional[_types.SqlConnection]
) -> _sa.Table:
    if type(table_name) is str:
        if connection is None:
            raise ValueError('table_name cannot be str while connection is None')
        return get_table(table_name, connection)
    elif type(table_name) is _sa.Table:
        return table_name
    else:
        raise TypeError('table_name can only be str or sa.Table')
