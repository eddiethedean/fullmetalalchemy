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
    """
    Returns a SQLAlchemy MetaData object for a given SQLAlchemy connection.

    Parameters
    ----------
    connection : SqlConnection
        A SQLAlchemy SqlConnection object.

    schema : str, optional
        The name of the schema to be used. If `None`, the default schema is used.

    Returns
    -------
    MetaData
        A SQLAlchemy MetaData object that can be used to define tables and other schema constructs.

    Examples
    --------
    To get a SQLAlchemy MetaData object for a given SqlConnection:

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> conn = engine.connect()
    >>> metadata = get_metadata(conn)

    """
    return _sa.MetaData(bind=connection, schema=schema)

def get_table(table_name: str, connection: _types.SqlConnection, schema: _t.Optional[str] = None) -> _sa.Table:
    """
    Returns a SQLAlchemy Table object for a given table name and connection.

    Parameters
    ----------
    table_name : str
        The name of the table to retrieve.

    connection : SqlConnection
        A SQLAlchemy SqlConnection object.

    schema : str, optional
        The name of the schema to be used. If `None`, the default schema is used.

    Returns
    -------
    Table
        A SQLAlchemy Table object representing the table with the given name.

    Examples
    --------
    To get a SQLAlchemy Table object for a given table name and SqlConnection:

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> conn = engine.connect()
    >>> mytable = get_table('mytable', conn)

    """
    metadata = get_metadata(connection, schema)
    autoload_with = get_connection(connection)
    return _sa.Table(table_name, metadata, autoload=True, autoload_with=autoload_with, extend_existing=True, schema=schema)

def get_engine_table(connection_string: str, table_name: str, schema: _t.Optional[str] = None) -> _t.Tuple[_sa_engine.Engine, _sa.Table]:
    """
    Returns a tuple of a SQLAlchemy Engine object and a Table object for a given connection string and table name.

    Parameters
    ----------
    connection_string : str
        A string representing the connection URI to the database.

    table_name : str
        The name of the table to retrieve.

    schema : str, optional
        The name of the schema to be used. If `None`, the default schema is used.

    Returns
    -------
    Tuple[Engine, Table]
        A tuple of SQLAlchemy Engine object and Table object representing the connection and table.

    Examples
    --------
    To get a tuple of SQLAlchemy Engine and Table objects for a given connection string and table name:

    >>> conn_str = 'postgresql://user:password@localhost/mydatabase'
    >>> engine, mytable = get_engine_table(conn_str, 'mytable')

    """
    engine = _sa.create_engine(connection_string)
    table = get_table(table_name, engine, schema)
    return engine, table

def get_class(table_name: str, connection: _t.Union[_types.SqlConnection, _sa_session.Session], schema: _t.Optional[str] = None) -> _DeclarativeMeta:
    """
    Returns a declarative class object for a given table name and connection.

    Parameters
    ----------
    table_name : str
        The name of the table for which to generate the class.

    connection : Union[SqlConnection, Session]
        A SQLAlchemy connection object.

    schema : str, optional
        The name of the schema to be used. If `None`, the default schema is used.

    Returns
    -------
    DeclarativeMeta
        A SQLAlchemy declarative class object representing the table with the given name.

    Raises
    ------
    MissingPrimaryKey
        If the table does not have a primary key.

    Examples
    --------
    To generate a declarative class for a given table name and SqlConnection:

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> conn = engine.connect()
    >>> MyClass = get_class('mytable', conn)

    """
    metadata = get_metadata(connection, schema)
    connection = get_connection(connection)
    metadata.reflect(connection, only=[table_name], schema=schema)
    Base = _sa_automap.automap_base(metadata=metadata)
    Base.prepare()
    if table_name not in Base.classes:
        raise _ex.MissingPrimaryKey()
    return Base.classes[table_name]

def get_session(engine: _sa_engine.Engine) -> _sa_session.Session:
    """
    Returns a new session object for the given engine.

    Parameters
    ----------
    engine : Engine
        The SQLAlchemy engine object to use for the session.

    Returns
    -------
    Session
        A new SQLAlchemy session object.

    Examples
    --------
    To create a new session for a given engine:

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> session = get_session(engine)

    """
    return _sa_session.Session(engine, future=True)

def get_column(table: _sa.Table, column_name: str) -> _sa.Column:
    """
    Returns a column object for a given column name and table.

    Parameters
    ----------
    table : Table
        The SQLAlchemy Table object representing the table containing the column.

    column_name : str
        The name of the column to retrieve.

    Returns
    -------
    Column
        A SQLAlchemy Column object representing the column with the given name.

    Examples
    --------
    To retrieve a column object for a given table and column name:

    >>> from sqlalchemy import create_engine, MetaData, Table
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> metadata = MetaData(bind=engine)
    >>> mytable = Table('mytable', metadata, autoload=True)
    >>> mycolumn = get_column(mytable, 'mycolumn')

    """
    return table.c[column_name]

def get_table_constraints(table: _sa.Table) -> set:
    """
    Returns a set of constraints defined on a given table.

    Parameters
    ----------
    table : Table
        The SQLAlchemy Table object representing the table to retrieve constraints from.

    Returns
    -------
    set
        A set of SQLAlchemy Constraint objects representing the constraints defined on the table.

    Examples
    --------
    To retrieve the constraints defined on a given table:

    >>> from sqlalchemy import create_engine, MetaData, Table
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> metadata = MetaData(bind=engine)
    >>> mytable = Table('mytable', metadata, autoload=True)
    >>> constraints = get_table_constraints(mytable)

    """
    return table.constraints

def get_primary_key_constraints(table: _sa.Table) -> _t.Tuple[str,  _t.List[str]]:
    """
    Returns the name and list of columns of the primary key constraint for a given table.

    Parameters
    ----------
    table : Table
        The SQLAlchemy Table object representing the table to retrieve the primary key from.

    Returns
    -------
    Tuple[str, List[str]]
        A tuple containing the name of the primary key constraint and a list of column names for the primary key.

    Raises
    ------
    TypeError
        If the provided table does not have a primary key constraint.

    Examples
    --------
    To retrieve the name and list of columns for the primary key constraint on a given table:

    >>> from sqlalchemy import create_engine, MetaData, Table
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> metadata = MetaData(bind=engine)
    >>> mytable = Table('mytable', metadata, autoload=True)
    >>> pk_name, pk_columns = get_primary_key_constraints(mytable)
    >>> print(pk_name)
    'mytable_pkey'
    >>> print(pk_columns)
    ['id']

    """
    cons = get_table_constraints(table)
    for con in cons:
        if isinstance(con, _sa.PrimaryKeyConstraint):
            return con.name, [col.name for col in con.columns]
    return tuple()

def missing_primary_key(table: _sa.Table,) -> bool:
    """
    Returns a boolean indicating whether a given table has a primary key constraint.

    Parameters
    ----------
    table : Table
        The SQLAlchemy Table object representing the table to check for a primary key.

    Returns
    -------
    bool
        True if the table does not have a primary key, False otherwise.

    Examples
    --------
    To check whether a table has a primary key constraint:

    >>> from sqlalchemy import create_engine, MetaData, Table, Column, Integer
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> metadata = MetaData(bind=engine)
    >>> mytable = Table('mytable', metadata,
    ...                 Column('id', Integer, primary_key=True),
    ...                 Column('name', String),
    ...                 )
    >>> has_pk = missing_primary_key(mytable)
    >>> print(has_pk)
    False

    """
    pks = get_primary_key_constraints(table)
    return pks[1] == []

def get_column_types(table: _sa.Table) -> dict:
    """
    Returns a dictionary mapping column names to their corresponding SQLAlchemy column types for a given table.

    Parameters
    ----------
    table : Table
        The SQLAlchemy Table object representing the table to extract column types from.

    Returns
    -------
    dict
        A dictionary mapping column names to their corresponding SQLAlchemy column types.

    Examples
    --------
    To get the column types of a table:

    >>> from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> metadata = MetaData(bind=engine)
    >>> mytable = Table('mytable', metadata,
    ...                 Column('id', Integer, primary_key=True),
    ...                 Column('name', String),
    ...                 Column('age', Integer),
    ...                 )
    >>> col_types = get_column_types(mytable)
    >>> print(col_types)
    {'id': INTEGER(), 'name': VARCHAR(), 'age': INTEGER()}

    """
    return {c.name: c.type for c in table.c}

def get_column_names(table: _sa.Table) ->  _t.List[str]:
    """
    Get a list of column names from the given SQLAlchemy table object.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy table object to retrieve column names from.

    Returns
    -------
    List[str]
        A list of strings representing the names of the columns in the table.

    Raises
    ------
    None

    Examples
    --------
    >>> import sqlalchemy as sa
    >>> from fullmetalalchemy.utils import get_column_names
    >>> engine = sa.create_engine('sqlite:///example.db')
    >>> metadata = sa.MetaData()
    >>> users = sa.Table('users', metadata,
    ...     sa.Column('id', sa.Integer, primary_key=True),
    ...     sa.Column('name', sa.String),
    ...     sa.Column('age', sa.Integer),
    ... )
    >>> metadata.create_all(engine)
    >>> get_column_names(users)
    ['id', 'name', 'age']
    """
    return [c.name for c in table.columns]

def get_table_names(engine: _sa_engine.Engine, schema: _t.Optional[str] = None) ->  _t.List[str]:
    """
    Get a list of table names from the given SQLAlchemy engine object.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine object to retrieve table names from.
    schema : str or None, optional
        The name of the schema to retrieve tables from, if None all tables are retrieved.

    Returns
    -------
    List[str]
        A list of strings representing the names of the tables in the database.

    Raises
    ------
    None

    Examples
    --------
    >>> import sqlalchemy as sa
    >>> from fullmetalalchemy.utils import get_table_names
    >>> engine = sa.create_engine('sqlite:///example.db')
    >>> get_table_names(engine)
    ['users', 'orders', 'products']
    """
    return _sa.inspect(engine).get_table_names(schema)

def get_row_count(table: _sa.Table, session: _t.Optional[_types.SqlConnection] = None) -> int:
    """
    Get the number of rows in the given table using the provided SQLAlchemy session.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy table object to retrieve row count for.
    session : fullmetalalchemy.types.SqlConnection or None, optional
        The SQLAlchemy session to use to retrieve the row count. If None, a new session is created.

    Returns
    -------
    int
        The number of rows in the table.

    Raises
    ------
    None

    Examples
    --------
    >>> import sqlalchemy as sa
    >>> from fullmetalalchemy.utils import get_row_count
    >>> engine = sa.create_engine('sqlite:///example.db')
    >>> metadata = sa.MetaData()
    >>> users = sa.Table('users', metadata,
    ...     sa.Column('id', sa.Integer, primary_key=True),
    ...     sa.Column('name', sa.String),
    ...     sa.Column('age', sa.Integer),
    ... )
    >>> metadata.create_all(engine)
    >>> with engine.connect() as conn:
    ...     result = conn.execute(users.insert().values(name='Alice', age=25))
    ...     row_count = get_row_count(users, conn)
    >>> row_count
    1
    """
    session = _ex.check_for_engine(table, session)
    col_name = get_column_names(table)[0]
    col = get_column(table, col_name)
    result = session.execute(_sa.func.count(col)).scalar()
    return result if result is not None else 0

def get_schemas(engine: _sa_engine.Engine) ->  _t.List[str]:
    """
    Returns a list of schema names for a given SQLAlchemy engine.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine object representing the database connection.

    Returns
    -------
    List[str]
        A list of schema names in the connected database.

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> get_schemas(engine)
    ['public', 'schema1', 'schema2']
    """
    insp = _sa.inspect(engine)
    return insp.get_schema_names()

def tables_metadata_equal(table1: _sa.Table, table2: _sa.Table) -> bool:
    """
    Determines if two SQLAlchemy Table objects have identical metadata.

    Parameters
    ----------
    table1 : sqlalchemy.Table
        The first SQLAlchemy Table object to compare.
    table2 : sqlalchemy.Table
        The second SQLAlchemy Table object to compare.

    Returns
    -------
    bool
        True if the two tables have the same name and primary keys, False otherwise.

    Examples
    --------
    >>> from sqlalchemy import create_engine, MetaData, Table, Column, Integer
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> meta = MetaData()
    >>> table1 = Table('mytable', meta, Column('id', Integer, primary_key=True))
    >>> table2 = Table('mytable', meta, Column('id', Integer, primary_key=True))
    >>> tables_metadata_equal(table1, table2)
    True
    """
    if table1.name != table2.name: return False
    table1_keys = primary_key_names(table1)
    table2_keys = primary_key_names(table2)
    if set(table1_keys) != set(table2_keys): return False
    return True

def str_to_table(table_name: _t.Union[str, _sa.Table], connection: _t.Optional[_types.SqlConnection]) -> _sa.Table:
    """
    Converts a table name string or SQLAlchemy Table object to a Table object.

    Parameters
    ----------
    table_name : Union[str, sqlalchemy.Table]
        The name of the table as a string or the SQLAlchemy Table object.
    connection : Optional[fullmetalalchemy.types.SqlConnection]
        The database connection to use.

    Returns
    -------
    sqlalchemy.Table
        The SQLAlchemy Table object.

    Raises
    ------
    ValueError
        If `table_name` is a string and `connection` is None.
    TypeError
        If `table_name` is not a string or SQLAlchemy Table object.

    Examples
    --------
    >>> from sqlalchemy import create_engine, MetaData, Table, Column, Integer
    >>> engine = create_engine('postgresql://user:password@localhost/mydatabase')
    >>> conn = engine.connect()
    >>> meta = MetaData()
    >>> table1 = Table('mytable', meta, Column('id', Integer, primary_key=True))
    >>> str_to_table(table1, conn)
    <sqlalchemy.Table object at 0x7fdaf42f7b50>
    >>> str_to_table('mytable', conn)
    <sqlalchemy.Table object at 0x7fdaf4273cd0>
    """
    if type(table_name) is str:
        if connection is None:
            raise ValueError('table_name cannot be str while connection is None')
        return get_table(table_name, connection)
    elif type(table_name) is _sa.Table:
        return table_name
    else:
        raise TypeError('table_name can only be str or sa.Table')