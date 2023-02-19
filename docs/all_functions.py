import typing as _t

import sqlalchemy as _sa
import sqlalchemy.orm.session as _sa_session
import sqlalchemy.ext.automap as _sa_automap
import sqlalchemy.engine as _sa_engine
from sqlalchemy.orm.decl_api import DeclarativeMeta as _DeclarativeMeta

import fullmetalalchemy.types as _types
import fullmetalalchemy.exceptions as _ex


def get_engine(connection) -> _sa_engine.Engine:
    """
    Returns a SQLAlchemy engine object for a given connection.

    Parameters
    ----------
    connection : Session or Engine
        A SQLAlchemy Session or Engine object.

    Returns
    -------
    Engine
        A SQLAlchemy Engine object that can be used to communicate with a database.

    Raises
    ------
    TypeError
        If `connection` is not an instance of either Session or Engine.

    Examples
    --------
    To get a SQLAlchemy Engine object for a given connection:

    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy.orm import sessionmaker
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> Session = sessionmaker(bind=engine)
    >>> session = Session()
    >>> engine = get_engine(session)

    """
    if isinstance(connection, _sa_session.Session):
        return connection.connection()
    else:
        return connection

def primary_key_columns(table: _sa.Table) ->  _t.List[_sa.Column]:
    """
    Returns a list of primary key columns for a given SQLAlchemy table.

    Parameters
    ----------
    table : Table
        A SQLAlchemy Table object.

    Returns
    -------
    List[Column]
        A list of SQLAlchemy Column objects that represent the primary key columns of the `table`.

    Examples
    --------
    To get the primary key columns of a SQLAlchemy table:

    >>> from sqlalchemy import create_engine, Table, Column, Integer, MetaData
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> metadata = MetaData(bind=engine)
    >>> mytable = Table('mytable', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String),
    ... )
    >>> primary_key_cols = primary_key_columns(mytable)

    """
    return list(table.primary_key.columns)

def primary_key_names(table: _sa.Table) ->  _t.List[str]:
    """
    Returns a list of names of the primary key columns for a given SQLAlchemy table.

    Parameters
    ----------
    table : Table
        A SQLAlchemy Table object.

    Returns
    -------
    List[str]
        A list of strings representing the names of the primary key columns of the `table`.

    Examples
    --------
    To get the names of primary key columns of a SQLAlchemy table:

    >>> from sqlalchemy import create_engine, Table, Column, Integer, MetaData
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> metadata = MetaData(bind=engine)
    >>> mytable = Table('mytable', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String),
    ... )
    >>> primary_key_names = primary_key_names(mytable)

    """
    return [c.name for c in primary_key_columns(table)]

def get_connection(connection: _t.Union[_types.SqlConnection, _sa_session.Session]) -> _types.SqlConnection:
    """
    Returns a SQLAlchemy connection object for a given SQLAlchemy Session or connection object.

    Parameters
    ----------
    connection : Union[SqlConnection, Session]
        A SQLAlchemy SqlConnection or Session object.

    Returns
    -------
    SqlConnection
        A SQLAlchemy SqlConnection object that can be used to communicate with a database.

    Raises
    ------
    TypeError
        If `connection` is not an instance of either SqlConnection or Session.

    Examples
    --------
    To get a SQLAlchemy SqlConnection object for a given Session or SqlConnection:

    >>> from sqlalchemy import create_engine, Session
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> session = Session(bind=engine)
    >>> conn = get_connection(session)

    """
    if isinstance(connection, _sa_session.Session):
        return connection.connection()
    return connection

def get_metadata(connection: _types.SqlConnection, schema: _t.Optional[str] = None) -> _sa.MetaData:
    return _sa.MetaData(bind=connection, schema=schema)

def get_table(table_name: str, connection: _types.SqlConnection, schema: _t.Optional[str] = None) -> _sa.Table:
    metadata = get_metadata(connection, schema)
    autoload_with = get_connection(connection)
    return _sa.Table(table_name, metadata, autoload=True, autoload_with=autoload_with, extend_existing=True, schema=schema)

def get_engine_table(connection_string: str, table_name: str, schema: _t.Optional[str] = None) -> _t.Tuple[_sa_engine.Engine, _sa.Table]:
    engine = _sa.create_engine(connection_string)
    table = get_table(table_name, engine, schema)
    return engine, table

def get_class(table_name: str, connection: _t.Union[_types.SqlConnection, _sa_session.Session], schema: _t.Optional[str] = None) -> _DeclarativeMeta:
    metadata = get_metadata(connection, schema)
    connection = get_connection(connection)
    metadata.reflect(connection, only=[table_name], schema=schema)
    Base = _sa_automap.automap_base(metadata=metadata)
    Base.prepare()
    if table_name not in Base.classes:
        raise _ex.MissingPrimaryKey()
    return Base.classes[table_name]

def get_session(engine: _sa_engine.Engine) -> _sa_session.Session:
    return _sa_session.Session(engine, future=True)

def get_column(table: _sa.Table, column_name: str) -> _sa.Column:
    return table.c[column_name]

def get_table_constraints(table: _sa.Table) -> set:
    return table.constraints

def get_primary_key_constraints(table: _sa.Table) -> _t.Tuple[str,  _t.List[str]]:
    cons = get_table_constraints(table)
    for con in cons:
        if isinstance(con, _sa.PrimaryKeyConstraint):
            return con.name, [col.name for col in con.columns]
    return tuple()

def missing_primary_key(table: _sa.Table,) -> bool:
    pks = get_primary_key_constraints(table)
    return pks[1] == []

def get_column_types(table: _sa.Table) -> dict:
    return {c.name: c.type for c in table.c}

def get_column_names(table: _sa.Table) ->  _t.List[str]:
    return [c.name for c in table.columns]

def get_table_names(engine: _sa_engine.Engine, schema: _t.Optional[str] = None) ->  _t.List[str]:
    return _sa.inspect(engine).get_table_names(schema)

def get_row_count(table: _sa.Table, session: _t.Optional[_types.SqlConnection] = None) -> int:
    session = _ex.check_for_engine(table, session)
    col_name = get_column_names(table)[0]
    col = get_column(table, col_name)
    result = session.execute(_sa.func.count(col)).scalar()
    return result if result is not None else 0

def get_schemas(engine: _sa_engine.Engine) ->  _t.List[str]:
    insp = _sa.inspect(engine)
    return insp.get_schema_names()

def tables_metadata_equal(table1: _sa.Table, table2: _sa.Table) -> bool:
    if table1.name != table2.name: return False
    table1_keys = primary_key_names(table1)
    table2_keys = primary_key_names(table2)
    if set(table1_keys) != set(table2_keys): return False
    return True

def str_to_table(table_name: _t.Union[str, _sa.Table], connection: _t.Optional[_types.SqlConnection]) -> _sa.Table:
    if type(table_name) is str:
        if connection is None:
            raise ValueError('table_name cannot be str while connection is None')
        return get_table(table_name, connection)
    elif type(table_name) is _sa.Table:
        return table_name
    else:
        raise TypeError('table_name can only be str or sa.Table')