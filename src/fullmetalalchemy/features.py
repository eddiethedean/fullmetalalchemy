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
        return connection.connection()# type: ignore
    else:
        return connection


def primary_key_columns(
    table: _sa.Table
) ->  _t.List[_sa.Column]:
    """
    Return the primary key columns of a SQLAlchemy Table.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table whose primary key columns will be returned.

    Returns
    -------
    List of sqlalchemy.Column
        The list of primary key columns for the input table.

    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.primary_key_columns(table)
    [Column('id', INTEGER(), table=<xy>, primary_key=True, nullable=False)]
    """
    return list(table.primary_key.columns)


def primary_key_names(
    table: _sa.Table
) ->  _t.List[str]:
    """
    Return the names of the primary key columns of a SQLAlchemy Table.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table whose primary key column names will be returned.

    Returns
    -------
    List of str
        The list of primary key column names for the input table.

    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.primary_key_names(table)
    ['id']
    """
    return [c.name for c in primary_key_columns(table)]


def get_connection(
    connection: _t.Union[_types.SqlConnection, _sa_session.Session]
) -> _types.SqlConnection:
    """
    Get the engine connection from a SQLAlchemy Session object or return the input connection.

    Parameters
    ----------
    connection : Union[sqlalchemy.engine.Connection, sqlalchemy.orm.Session]
        The connection or session to get the engine connection from.

    Returns
    -------
    sqlalchemy.engine.Connection
        The engine connection associated with the input session, or the input connection if it is already an engine connection.

    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> session = fa.features.get_session(engine)
    >>> fa.features.get_connection(session)
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
    Get a SQLAlchemy MetaData object associated with a given database connection and schema.

    Parameters
    ----------
    connection : fullmetalalchemy.types.SqlConnection
        The database connection to associate with the MetaData object.
    schema : Optional[str], default None
        The name of the schema to use with the MetaData object. If None, the default schema is used.

    Returns
    -------
    sqlalchemy.MetaData
        The MetaData object associated with the input connection and schema.

    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> fa.features.get_metadata(engine)
    MetaData(bind=Engine(sqlite:///data/test.db))
    """
    return _sa.MetaData(bind=connection, schema=schema)


def get_table(
    table_name: str,
    connection: _types.SqlConnection,
    schema: _t.Optional[str] = None
) -> _sa.Table:
    """
    Get a SQLAlchemy Table object associated with a given table name, database connection, and schema.

    Parameters
    ----------
    table_name : str
        The name of the table to retrieve.
    connection : fullmetalalchemy.types.SqlConnection
        The database connection to use to retrieve the table.
    schema : Optional[str], default None
        The name of the schema to use when retrieving the table. If None, the default schema is used.

    Returns
    -------
    sqlalchemy.Table
        The Table object associated with the input table name, database connection, and schema.

    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> fa.features.get_table('xy', engine)
    Table('xy', MetaData(bind=Engine(sqlite:///data/test.db)),
        Column('id', INTEGER(), table=<xy>, primary_key=True, nullable=False),
        Column('x', INTEGER(), table=<xy>),
        Column('y', INTEGER(), table=<xy>), schema=None)
    """
    metadata = get_metadata(connection, schema)
    autoload_with = get_connection(connection)
    return _sa.Table(table_name,
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
    Get the engine and table objects from a given connection string, table name, and optional schema.

    Parameters
    ----------
    connection_string : str
        The connection string to the database.
    table_name : str
        The name of the table to get.
    schema : str, optional
        The name of the schema the table belongs to.

    Returns
    -------
    Tuple[Engine, Table]
        The SQLAlchemy engine and table objects.

    Examples
    --------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> assert isinstance(engine, _sa_engine.Engine)
    >>> assert isinstance(table, _sa.Table)
    """
    engine = _sa.create_engine(connection_string)
    table = get_table(table_name, engine, schema)
    return engine, table


def get_class(
    table_name: str,
    connection: _t.Union[_types.SqlConnection, _sa_session.Session],
    schema: _t.Optional[str] = None
) -> _DeclarativeMeta:
    """
    Reflects the specified table and returns a declarative class that corresponds to it.

    Parameters
    ----------
    table_name : str
        The name of the table to reflect.
    connection : Union[SqlConnection, Session]
        The connection to use to reflect the table. This can be either an `SqlConnection`
        or an active `Session` object.
    schema : Optional[str], optional
        The name of the schema to which the table belongs, by default None.

    Returns
    -------
    DeclarativeMeta
        The declarative class that corresponds to the specified table.

    Raises
    ------
    MissingPrimaryKey
        If the specified table does not have a primary key.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> fa.features.get_class('xy', engine)
    sqlalchemy.ext.automap.xy
    """
    metadata = get_metadata(connection, schema)
    connection = get_connection(connection)

    metadata.reflect(connection, only=[table_name], schema=schema)
    Base = _sa_automap.automap_base(metadata=metadata)
    Base.prepare()
    if table_name not in Base.classes:
        raise _ex.MissingPrimaryKey()
    return Base.classes[table_name]


def get_session(
    engine: _sa_engine.Engine
) -> _sa_session.Session:
    """
    Creates and returns a new SQLAlchemy session object using the provided SQLAlchemy engine object.

    Parameters
    ----------
    engine : _sa_engine.Engine
        SQLAlchemy engine object to create a new session from.

    Returns
    -------
    _sa_session.Session
        New SQLAlchemy session object.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> fa.features.get_session(engine)
    <sqlalchemy.orm.session.Session at 0x7f95999e1eb0>
    """
    return _sa_session.Session(engine, future=True)


def get_column(
    table: _sa.Table,
    column_name: str
) -> _sa.Column:
    """
    Retrieve a SQLAlchemy column object from a SQLAlchemy table.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy table to retrieve the column from.
    column_name : str
        The name of the column to retrieve.

    Returns
    -------
    sqlalchemy.Column
        The SQLAlchemy column object corresponding to the given column name.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.get_column(table, 'x')
    Column('x', INTEGER(), table=<xy>)
    """
    return table.c[column_name]


def get_table_constraints(
    table: _sa.Table
) -> set:
    """
    Get a set of all constraints for a given SQLAlchemy Table object.

    Parameters
    ----------
    table : sqlalchemy.Table
        The Table object to get the constraints from.

    Returns
    -------
    set
        A set of all constraints for the given Table object.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.get_table_constraints(table)
    {PrimaryKeyConstraint(Column('id', INTEGER(), table=<xy>, primary_key=True, nullable=False))}
    """
    return table.constraints


def get_primary_key_constraints(
    table: _sa.Table
) -> _t.Tuple[str,  _t.List[str]]:
    """
    Get the primary key constraints of a SQLAlchemy table.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy table object to get the primary key constraints of.

    Returns
    -------
    Tuple[Optional[str], List[str]]
        A tuple with the primary key constraint name (if it exists) and a list of
        the column names that make up the primary key.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.get_primary_key_constraints(table)
    (None, ['id'])
    """
    cons = get_table_constraints(table)
    for con in cons:
        if isinstance(con, _sa.PrimaryKeyConstraint):
            return con.name, [col.name for col in con.columns]
    return tuple()


def missing_primary_key(
    table: _sa.Table,
) -> bool:
    """
    Check if a sqlalchemy table has a primary key.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table to check.

    Returns
    -------
    bool
        True if the table doesn't have a primary key, False otherwise.

    Examples
    --------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.missing_primary_key(table)
    False
    """
    pks = get_primary_key_constraints(table)
    return pks[1] == []


def get_column_types(
    table: _sa.Table
) -> dict:
    """
    Get the types of columns in a SQLAlchemy table.

    Parameters
    ----------
    table : sqlalchemy.Table
        SQLAlchemy table to get column types from.

    Returns
    -------
    dict
        A dictionary with the names of columns as keys and the SQLAlchemy
        types of the columns as values.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.get_column_types(table)
    {'id': INTEGER(), 'x': INTEGER(), 'y': INTEGER()}
    """
    return {c.name: c.type for c in table.c}


def get_column_names(
    table: _sa.Table
) ->  _t.List[str]:
    """
    Returns a list of the column names for the given SQLAlchemy table object.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy table object to get column names for.

    Returns
    -------
    List[str]
        A list of the column names for the given table.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.get_column_names(table)
    ['id', 'x', 'y']
    """
    return [c.name for c in table.columns]


def get_table_names(
    engine: _sa_engine.Engine,
    schema: _t.Optional[str] = None
) ->  _t.List[str]:
    """
    Get a list of the names of tables in the database connected to the given engine.

    Parameters
    ----------
    engine : _sa_engine.Engine
        An SQLAlchemy engine instance connected to a database.
    schema : Optional[str], optional
        The name of the schema to filter by, by default None.

    Returns
    -------
    List[str]
        A list of table names.

    Examples
    --------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> fa.features.get_table_names(engine)
    ['xy']
    """
    return _sa.inspect(engine).get_table_names(schema)


def get_row_count(
    table: _sa.Table,
    session: _t.Optional[_types.SqlConnection] = None
) -> int:
    """
    Returns the number of rows in a given table.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table to get the row count from.
    session : sqlalchemy.orm.Session, optional
        The session to use. If not provided, a new session is created.

    Returns
    -------
    int
        The number of rows in the table.

    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.get_row_count(table)
    0
    """
    session = _ex.check_for_engine(table, session)
    col_name = get_column_names(table)[0]
    col = get_column(table, col_name)
    result = session.execute(_sa.func.count(col)).scalar()
    return result if result is not None else 0


def get_schemas(
    engine: _sa_engine.Engine
) ->  _t.List[str]:
    """
    Get a list of all schemas in the database connected to the given engine.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        An instance of the SQLAlchemy engine connected to the database.

    Returns
    -------
    List[str]
        A list of all schemas in the connected database.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> fa.features.get_schemas(engine)
    ['main']
    """
    insp = _sa.inspect(engine)
    return insp.get_schema_names()


def tables_metadata_equal(
    table1: _sa.Table,
    table2: _sa.Table
) -> bool:
    """
    Check if two SQL tables have the same metadata.

    Parameters
    ----------
    table1 : sqlalchemy.Table
        First SQL table to compare
    table2 : sqlalchemy.Table
        Second SQL table to compare

    Returns
    -------
    bool
        True if the two SQL tables have the same metadata, otherwise False.

    Examples
    --------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.tables_metadata_equal(table, table)
    True
    """
    if table1.name != table2.name: return False

    column_types1 = get_column_types(table1)
    column_types2 = get_column_types(table2)
    # if column_types1 != column_types2: return False

    table1_keys = primary_key_names(table1)
    table2_keys = primary_key_names(table2)
    if set(table1_keys) != set(table2_keys): return False

    return True


def str_to_table(
    table_name: _t.Union[str, _sa.Table],
    connection: _t.Optional[_types.SqlConnection]
) -> _sa.Table:
    """
    Convert a table name to a SQLAlchemy table object.

    Parameters
    ----------
    table_name : str or sqlalchemy.Table
        If a string is passed, it should be the name of the table to be fetched.
        If a `sqlalchemy.Table` object is passed, it is simply returned.

    connection : SQLAlchemy connection
        Connection to the database.

    Returns
    -------
    sqlalchemy.Table
        The corresponding table object.

    Raises
    ------
    ValueError
        If `table_name` is a string and `connection` is `None`.

    TypeError
        If `table_name` is neither a string nor a `sqlalchemy.Table`.

    Example
    -------
    >>> import fullmetalalchemy as fa
    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table_name = 'xy'
    >>> table = fa.features.str_to_table(table_name, engine)
    >>> print(table.name)
    xy
    """
    if type(table_name) is str:
        if connection is None:
            raise ValueError('table_name cannot be str while connection is None')
        return get_table(table_name, connection)
    elif type(table_name) is _sa.Table:
        return table_name
    else:
        raise TypeError('table_name can only be str or sa.Table')


def _get_where_clause(
    sa_table: _sa.Table,
    record: _types.Record
) -> _t.List:
    """
    Given a record, return a list of SQLAlchemy binary expressions representing the WHERE clause for a SQL query.

    Parameters
    ----------
    sa_table : sqlalchemy.Table
        The table object that the WHERE clause is for.
    record : Dict[str, Any]
        The record to match against.

    Returns
    -------
    List[sqlalchemy.sql.elements.BinaryExpression]
        A list of SQLAlchemy binary expressions representing the WHERE clause.

    Examples
    --------
    >>> from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
    >>> engine = create_engine('sqlite://')
    >>> metadata = MetaData()
    >>> test_table = Table('test', metadata, Column('id', Integer, primary_key=True), Column('name', String))
    >>> metadata.create_all(engine)
    >>> record = {'id': 1, 'name': 'test_name'}
    >>> where_clause = _get_where_clause(test_table, record)
    >>> where_clause
    [test.id = :id_1, test.name = :name_1]

    """
    return [sa_table.c[key_name] == key_value for key_name, key_value in record.items()]
