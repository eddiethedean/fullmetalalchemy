"""
I am going to provide you with python functions for you to add numpy-style docstrings for.
add the following sections: Parameters, Returns, Examples. Only respond to my functions with the docstring for them.
"""

import datetime as _datetime
import decimal as _decimal
import typing as _t

import sqlalchemy as _sa
import sqlalchemy.engine as _sa_engine
import sqlalchemy.ext.automap as _sa_automap
import sqlalchemy.orm.session as _sa_session
import sqlalchemy.schema as _sa_schema
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.orm.decl_api import DeclarativeMeta as _DeclarativeMeta
from tinytim.data import column_names as _column_names
from tinytim.rows import row_dicts_to_data as _row_dicts_to_data

import fullmetalalchemy.exceptions as _ex
import fullmetalalchemy.features as _features
import fullmetalalchemy.insert as _insert
import fullmetalalchemy.type_convert as _type_convert
import fullmetalalchemy.types as _types


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
    return ()

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


_Record = _t.Dict[str, _t.Any]


def create_engine(url, *args, **kwargs) -> _sa_engine.Engine:
    """
    Create a SQLAlchemy engine using the specified URL.

    Parameters
    ----------
    url : str
        The URL to connect to the database.
    *args
        Additional positional arguments to pass to `sqlalchemy.create_engine`.
    **kwargs
        Additional keyword arguments to pass to `sqlalchemy.create_engine`.

    Returns
    -------
    sqlalchemy.engine.Engine
        A SQLAlchemy engine instance.

    Examples
    --------
    >>> engine = create_engine("sqlite:///example.db")
    >>> session = _sa_session.Session(bind=engine)
    """
    return _create_engine(url, future=True, *args, **kwargs)


def create_table(table_name: str, column_names:  _t.Sequence[str], column_types:  _t.Sequence, primary_key: _t.Sequence[str],
    engine: _sa_engine.Engine,
    schema:  _t.Optional[str] = None,
    autoincrement:  _t.Optional[bool] = False,
    if_exists:  _t.Optional[str] = 'error'
) -> _sa.Table:
    """
    Create a new database table with the specified column names and types.

    Parameters
    ----------
    table_name : str
        The name of the table to create.
    column_names : Sequence[str]
        The names of the columns in the table.
    column_types : Sequence
        The types of the columns in the table. The types should be specified using the Python data types defined in `fullmetalalchemy.types`.
    primary_key : Union[str, Sequence[str]]
        The name(s) of the column(s) that make up the primary key. If a single column is the primary key, this should be a string.
        If multiple columns make up the primary key, this should be a sequence of strings.
    engine : sqlalchemy.engine.Engine
        The database engine to use for creating the table.
    schema : Optional[str], default=None
        The name of the schema to create the table in.
    autoincrement : bool, default=False
        Whether to automatically increment the primary key.
    if_exists : str, default='error'
        What to do if the table already exists. Valid options are 'error', 'replace', and 'append'.

    Returns
    -------
    sqlalchemy.schema.Table
        A SQLAlchemy `Table` object representing the new table.

    Examples
    --------
    >>> from fullmetalalchemy.types import TEXT, INTEGER
    >>> engine = create_engine('sqlite:///example.db')
    >>> create_table('my_table', ['id', 'name', 'age'], [INTEGER, TEXT, INTEGER], 'id', engine)
    Table('my_table',
        MetaData(bind=None),
        Column('id', INTEGER(), table=<my_table>, primary_key=True, nullable=False),
        Column('name', TEXT(), table=<my_table>),
        Column('age', INTEGER(), table=<my_table>), schema=None)
    """
    cols = []
    for name, python_type in zip(column_names, column_types):
        sa_type = _type_convert._type_convert[python_type]
        if type(primary_key) is str:
            primary_key = [primary_key]
        if name in primary_key:
            col = _sa.Column(name, sa_type,
                            primary_key=True,
                            autoincrement=autoincrement)
        else:
            col = _sa.Column(name, sa_type)
        cols.append(col)
    metadata = _sa.MetaData(engine)
    table = _sa.Table(table_name, metadata, *cols, schema=schema)
    if if_exists == 'replace':
        drop_table_sql = _sa_schema.DropTable(table, if_exists=True)
        with engine.connect() as con:
            con.execute(drop_table_sql)
    table_creation_sql = _sa_schema.CreateTable(table)
    with engine.connect() as con:
        con.execute(table_creation_sql)
    return _features.get_table(table_name, engine, schema=schema)

def create_table_from_records(
    table_name: str,
    records:  _t.Sequence[_Record],
    primary_key: _t.Sequence[str],
    engine: _sa_engine.Engine,
    column_types:  _t.Optional[ _t.Sequence] = None,
    schema:  _t.Optional[str] = None,
    autoincrement:  _t.Optional[bool] = False,
    if_exists:  _t.Optional[str] = 'error',
    columns:  _t.Optional[ _t.Sequence[str]] = None,
    missing_value:  _t.Optional[_t.Any] = None
) -> _sa.Table:
    """
    Creates a table in the database and populates it with the data from the provided sequence of records.

    Parameters
    ----------
    table_name : str
        The name of the table to be created.
    records : Sequence[Dict[str, Any]]
        A sequence of dictionaries, where each dictionary represents a row of data to be inserted into the table.
    primary_key : Sequence[str]
        A sequence of strings, where each string represents a column name that should be used as a primary key.
    engine : sqlalchemy.engine.Engine
        A SQLAlchemy engine object used to create the table.
    column_types : Optional[Sequence]
        A sequence of SQLAlchemy column types corresponding to the columns in the data.
        If not provided, the column types will be inferred based on the data.
    schema : Optional[str]
        The schema of the table. If not provided, the default schema will be used.
    autoincrement : Optional[bool]
        Whether or not to automatically increment the primary key column. Defaults to False.
    if_exists : Optional[str]
        What to do if the table already exists. Valid options are 'error', 'replace', and 'append'. Defaults to 'error'.
    columns : Optional[Sequence[str]]
        A sequence of column names to include in the table. If not provided, all columns will be included.
    missing_value : Optional[Any]
        A value that represents missing or null values in the data.

    Returns
    -------
    sqlalchemy.schema.Table
        The SQLAlchemy Table object representing the newly created table.

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('sqlite:///example.db')
    >>> records = [{'name': 'John', 'age': 25, 'gender': 'Male'},
                {'name': 'Mary', 'age': 30, 'gender': 'Female'},
                {'name': 'Bob', 'age': 40, 'gender': 'Male'}]
    >>> create_table_from_records('people', records, ['name'], engine, columns=['name', 'age', 'gender'])

    """
    data = _row_dicts_to_data(records, columns, missing_value)
    if column_types is None:
        column_types = [_column_datatype(values) for values in data.values()]
    col_names = _column_names(data)
    table = create_table(table_name, col_names, column_types, primary_key, engine, schema, autoincrement, if_exists)
    _insert.insert_records(table, records, engine)
    return table

def _column_datatype(values: _t.Iterable) -> type:
    """
    Infer the appropriate SQLAlchemy datatype for a column from a sample of values.

    Parameters
    ----------
    values : Iterable
        An iterable containing sample values of the column for which to infer a datatype.

    Returns
    -------
    type
        The inferred SQLAlchemy datatype for the column.

    Examples
    --------
    >>> values = [1, 2, 3, 4]
    >>> _column_datatype(values)
    <class 'int'>

    >>> values = [1.0, 2.0, 3.0, 4.0]
    >>> _column_datatype(values)
    <class 'float'>

    >>> values = ["foo", "bar", "baz"]
    >>> _column_datatype(values)
    <class 'str'>
    """
    dtypes = [
        int, str, (int, float), _decimal.Decimal, _datetime.datetime,
        bytes, bool, _datetime.date, _datetime.time,
        _datetime.timedelta, list, dict
    ]
    for value in values:
        for dtype in list(dtypes):
            if not isinstance(value, dtype):
                dtypes.pop(dtypes.index(dtype))
    if len(dtypes) == 2:
        if {int, _t.Union[float, int]} == {int, _t.Union[float, int]}:
            return int
    if len(dtypes) == 1:
        if dtypes[0] == _t.Union[float, int]:
            return float
        return dtypes[0]
    return str

def copy_table(
    new_name: str,
    table: _sa.Table,
    engine: _sa_engine.Engine,
    if_exists: str = 'replace'
) -> _sa.Table:
    """
    Copy a table to a new table with a new name.

    Parameters
    ----------
    new_name : str
        The name of the new table.
    table : sqlalchemy.Table
        The table to copy.
    engine : sqlalchemy.engine.Engine
        The engine to use for the copying process.
    if_exists : str, optional
        What to do if the new table already exists, by default 'replace'.

    Returns
    -------
    sqlalchemy.Table
        The new table created.

    Examples
    --------
    # create a connection to a database and a table object
    engine = create_engine('sqlite:///mydatabase.db')
    metadata = MetaData()
    mytable = Table('mytable', metadata, Column('id', Integer, primary_key=True), Column('value', String))

    # create a copy of the table with a new name
    mynewtable = copy_table('mynewtable', mytable, engine, if_exists='replace')

    """
    src_engine = engine
    dest_engine = engine
    schema = table.schema
    src_name = table.name
    dest_schema = schema
    dest_name = new_name

    # reflect existing columns, and create table object for oldTable
    src_engine._metadata = _sa.MetaData(bind=src_engine, schema=schema)  # type: ignore
    src_engine._metadata.reflect(src_engine)  # type: ignore

    # get columns from existing table
    srcTable = _sa.Table(src_name, src_engine._metadata, schema=schema)  # type: ignore

    # create engine and table object for newTable
    dest_engine._metadata = _sa.MetaData(bind=dest_engine, schema=dest_schema)  # type: ignore
    destTable = _sa.Table(dest_name, dest_engine._metadata, schema=dest_schema)  # type: ignore

    if if_exists == 'replace':
        drop_table_sql = _sa_schema.DropTable(destTable, if_exists=True)
        with engine.connect() as con:
            con.execute(drop_table_sql)

    # copy schema and create newTable from oldTable
    for column in srcTable.columns:
        destTable.append_column(column.copy())
    destTable.create()

    # insert records from oldTable
    _insert.insert_from_table(srcTable, destTable, engine)
    return destTable

def delete_records_session(
    table: _t.Union[_sa.Table, str],
    column_name: str,
    values: _t.Sequence,
    session: _sa_session.Session
) -> None:
    """
    Delete rows from a table matching the given column name and values.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The name of the table as a string or the SQLAlchemy Table object.
    column_name : str
        The name of the column to match against the values.
    values : Sequence
        A sequence of values to match against the column.
    session : sqlalchemy.orm.Session
        The SQLAlchemy session object for the transaction.

    Returns
    -------
    None

    Examples
    --------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]

    >>> session = fa.features.get_session(engine)
    >>> fa.delete.delete_records_session(table, 'id', [1], session)
    >>> session.commit()
    >>> fa.select.select_records_all(table)
    [{'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]
    """
    table = _features.str_to_table(table, session)
    col = _features.get_column(table, column_name)
    session.query(table).filter(col.in_(values)).delete(synchronize_session=False)

def delete_records(
    table: _t.Union[_sa.Table, str],
    column_name: str,
    values: _t.Sequence,
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Deletes records from a given table with a given value in a given column.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        A SQLAlchemy Table object or the name of a table in the database.
    column_name : str
        The name of the column to filter the records.
    values : Sequence
        The values of the column to match against and delete the records.
    engine : Optional[sqlalchemy.engine.Engine], optional
        A SQLAlchemy Engine object representing the database connection, by default None.

    Returns
    -------
    None

    Examples
    --------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.select.select_records_all(table)
    [{'id': 1, 'x': 1, 'y': 2},
     {'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]

    >>> fa.delete.delete_records(table, 'id', [1])
    >>> fa.select.select_records_all(table)
    [{'id': 2, 'x': 2, 'y': 4},
     {'id': 3, 'x': 4, 'y': 8},
     {'id': 4, 'x': 8, 'y': 11}]
    """
    table, engine = _ex.convert_table_engine(table, engine)
    session = _features.get_session(engine)
    delete_records_session(table, column_name, values, session)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_records_by_values(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[dict],
    engine: _t.Optional[_sa.engine.Engine] = None
) -> None:
    """
    Delete records from a database table based on the provided dictionary of values.

    Parameters
    ----------
    table : Union[Table, str]
        The database table to delete records from. This can be either a SQLAlchemy `Table` object or the name of the table as a string.
    records : Sequence[dict]
        A list of dictionaries, where each dictionary represents a record to delete from the table. The keys of the dictionary should correspond to column names, and the values should correspond to the values to filter the records by.
    engine : Optional[Engine]
        The SQLAlchemy engine to use to execute the deletion. If not provided, a new engine will be created using the default configuration.

    Returns
    -------
    None
        This function does not return anything, it simply deletes records from the specified database table.

    Examples
    --------
    >>> engine = create_engine("sqlite:///:memory:")
    >>> metadata = MetaData()
    >>> user = Table('user', metadata, Column('id', Integer, primary_key=True), Column('name', String), Column('age', Integer))
    >>> metadata.create_all(engine)
    >>> with engine.begin() as conn:
    ...     conn.execute(user.insert(), [{'id': 1, 'name': 'foo', 'age': 20}, {'id': 2, 'name': 'bar', 'age': 30}, {'id': 3, 'name': 'baz', 'age': 40}])
    >>> delete_records_by_values(user, [{'name': 'foo'}, {'name': 'bar'}], engine)
    >>> with engine.connect() as conn:
    ...     result = conn.execute(select([user]))
    ...     rows = result.fetchall()
    ...     print(rows)
    [(3, 'baz', 40)]
    """
    table, engine = _ex.convert_table_engine(table, engine)
    session = _features.get_session(engine)
    try:
        delete_records_by_values_session(table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_record_by_values_session(
    table: _t.Union[_sa.Table, str],
    record: _types.Record,
    session: _sa_session.Session
) -> None:
    """
    Delete a single record from a database table based on the provided dictionary of values.

    Parameters
    ----------
    table : Union[Table, str]
        The database table to delete the record from. This can be either a SQLAlchemy `Table` object or the name of the table as a string.
    record : Record
        A dictionary representing the record to delete. The keys of the dictionary should correspond to column names, and the values should correspond to the values to filter the record by.
    session : Session
        The SQLAlchemy session to use to execute the deletion.

    Returns
    -------
    None
        This function does not return anything, it simply deletes a record from the specified database table.

    Examples
    --------
    >>> engine = create_engine("sqlite:///:memory:")
    >>> metadata = MetaData()
    >>> user = Table('user', metadata, Column('id', Integer, primary_key=True), Column('name', String), Column('age', Integer))
    >>> metadata.create_all(engine)
    >>> with engine.begin() as conn:
    ...     conn.execute(user.insert(), [{'id': 1, 'name': 'foo', 'age': 20}, {'id': 2, 'name': 'bar', 'age': 30}, {'id': 3, 'name': 'baz', 'age': 40}])
    >>> with sessionmaker(bind=engine)() as session:
    ...     delete_record_by_values_session(user, {'name': 'foo', 'age': 20}, session)
    ...     session.commit()
    >>> with engine.connect() as conn:
    ...     result = conn.execute(select([user]))
    ...     rows = result.fetchall()
    ...     print(rows)
    [(2, 'bar', 30), (3, 'baz', 40)]
    """
    table = _features.str_to_table(table, session)
    delete = _build_delete_from_record(table, record)
    session.execute(delete)

def delete_records_by_values_session(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _sa_session.Session
) -> None:
    """
    Delete multiple records from a database table based on a sequence of dictionaries of values.

    Parameters
    ----------
    table : Union[Table, str]
        The database table to delete the records from. This can be either a SQLAlchemy `Table` object or the name of the table as a string.
    records : Sequence[Record]
        A sequence of dictionaries representing the records to delete. Each dictionary should represent a single record and the keys of the dictionary should correspond to column names, and the values should correspond to the values to filter the record by.
    session : Session
        The SQLAlchemy session to use to execute the deletions.

    Returns
    -------
    None
        This function does not return anything, it simply deletes multiple records from the specified database table.

    Examples
    --------
    >>> engine = create_engine("sqlite:///:memory:")
    >>> metadata = MetaData()
    >>> user = Table('user', metadata, Column('id', Integer, primary_key=True), Column('name', String), Column('age', Integer))
    >>> metadata.create_all(engine)
    >>> with engine.begin() as conn:
    ...     conn.execute(user.insert(), [{'id': 1, 'name': 'foo', 'age': 20}, {'id': 2, 'name': 'bar', 'age': 30}, {'id': 3, 'name': 'baz', 'age': 40}])
    >>> with sessionmaker(bind=engine)() as session:
    ...     delete_records_by_values_session(user, [{'name': 'foo', 'age': 20}, {'name': 'bar', 'age': 30}], session)
    ...     session.commit()
    >>> with engine.connect() as conn:
    ...     result = conn.execute(select([user]))
    ...     rows = result.fetchall()
    ...     print(rows)
    [(3, 'baz', 40)]
    """
    table = _features.str_to_table(table, session)
    for record in records:
        delete_record_by_values_session(table, record, session)

def delete_all_records_session(
    table: _t.Union[_sa.Table, str],
    session: _sa_session.Session
) -> None:
    """
    Delete all records from a database table.

    Parameters
    ----------
    table : Union[Table, str]
        The database table to delete all records from. This can be either a SQLAlchemy `Table` object or the name of the table as a string.
    session : Session
        The SQLAlchemy session to use to execute the deletion.

    Returns
    -------
    None
        This function does not return anything, it simply deletes all records from the specified database table.

    Examples
    --------
    >>> engine = create_engine("sqlite:///:memory:")
    >>> metadata = MetaData()
    >>> user = Table('user', metadata, Column('id', Integer, primary_key=True), Column('name', String), Column('age', Integer))
    >>> metadata.create_all(engine)
    >>> with engine.begin() as conn:
    ...     conn.execute(user.insert(), [{'id': 1, 'name': 'foo', 'age': 20}, {'id': 2, 'name': 'bar', 'age': 30}, {'id': 3, 'name': 'baz', 'age': 40}])
    >>> with sessionmaker(bind=engine)() as session:
    ...     delete_all_records_session(user, session)
    ...     session.commit()
    >>> with engine.connect() as conn:
    ...     result = conn.execute(select([user]))
    ...     rows = result.fetchall()
    ...     print(rows)
    []
    """
    table = _features.str_to_table(table, session)
    query = _sa.delete(table)
    session.execute(query)

def delete_all_records(
    table: _t.Union[_sa.Table, str],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Delete all records from a database table.

    Parameters
    ----------
    table : Union[Table, str]
        The database table to delete all records from. This can be either a SQLAlchemy `Table` object or the name of the table as a string.
    engine : Engine, optional
        The SQLAlchemy engine to use for the session. If not provided, the default engine will be used.

    Returns
    -------
    None
        This function does not return anything, it simply deletes all records from the specified database table.

    Examples
    --------
    >>> engine = create_engine("sqlite:///:memory:")
    >>> metadata = MetaData()
    >>> user = Table('user', metadata, Column('id', Integer, primary_key=True), Column('name', String), Column('age', Integer))
    >>> metadata.create_all(engine)
    >>> with engine.begin() as conn:
    ...     conn.execute(user.insert(), [{'id': 1, 'name': 'foo', 'age': 20}, {'id': 2, 'name': 'bar', 'age': 30}, {'id': 3, 'name': 'baz', 'age': 40}])
    >>> delete_all_records(user, engine)
    >>> with engine.connect() as conn:
    ...     result = conn.execute(select([user]))
    ...     rows = result.fetchall()
    ...     print(rows)
    []
    """
    table, engine = _ex.convert_table_engine(table, engine)
    session = _features.get_session(engine)
    try:
        delete_all_records_session(table, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def drop_table(
    table: _t.Union[_sa.Table, str],
    engine: _t.Optional[_sa_engine.Engine] = None,
    if_exists: bool = True,
    schema: _t.Optional[str] = None
) -> None:
    """
    Drop a database table.

    Parameters:
    -----------
    table : Union[_sa.Table, str]
        The table to be dropped. This can be either the table object or the name of the table as a string.
    engine : Optional[_sa_engine.Engine]
        The SQLAlchemy engine instance to use. If not provided, the table object must be passed as the first argument.
    if_exists : bool, default True
        If True, does not raise an error if the table does not exist.
    schema : Optional[str], default None
        The schema of the table. If None, the default schema is used.

    Returns:
    --------
    None

    Raises:
    -------
    ValueError
        If table is a string and engine is not provided.

    Example:
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('sqlite:///example.db')
    >>> drop_table('my_table', engine)
    """
    if isinstance(table, str):
        if table not in _sa.inspect(engine).get_table_names(schema=schema):
            if if_exists:
                return
        if engine is None:
            raise ValueError('Must pass engine if table is str.')
        table = _features.get_table(table, engine, schema=schema)
    sql = _sa_schema.DropTable(table, if_exists=if_exists)
    engine = _ex.check_for_engine(table, engine)
    with engine.connect() as con:
        con.execute(sql)

def insert_from_table_session(
    table1: _t.Union[_sa.Table, str],
    table2: _t.Union[_sa.Table, str],
    session: _sa_session.Session
) -> None:
    """
    Inserts records from one table into another using a session.

    Parameters
    ----------
    table1 : Union[_sa.Table, str]
        The table to select records from.
    table2 : Union[_sa.Table, str]
        The table to insert records into.
    session : sqlalchemy.orm.session.Session
        The session to use for the database transaction.

    Returns
    -------
    None

    Examples
    --------
    >>> from sqlalchemy import create_engine, Table, Column, Integer
    >>> from sqlalchemy.orm import sessionmaker
    >>> engine = create_engine('sqlite://')
    >>> connection = engine.connect()
    >>> metadata = MetaData()
    >>> table1 = Table('table1', metadata, Column('id', Integer, primary_key=True))
    >>> table2 = Table('table2', metadata, Column('id', Integer, primary_key=True))
    >>> metadata.create_all(engine)
    >>> Session = sessionmaker(bind=engine)
    >>> session = Session()
    >>> session.add(table1.insert().values(id=1))
    >>> session.add(table1.insert().values(id=2))
    >>> session.commit()
    >>> insert_from_table_session(table1, table2, session)
    >>> result = connection.execute(table2.select())
    >>> list(result)
    [(1,), (2,)]
    """
    table1 = _features.str_to_table(table1, session)
    table2 = _features.str_to_table(table2, session)
    session.execute(table2.insert().from_select(table1.columns.keys(), table1))

def insert_from_table(
    table1: _t.Union[_sa.Table, str],
    table2: _t.Union[_sa.Table, str],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    """
    Insert rows from `table1` into `table2`.

    Parameters
    ----------
    table1 : Union[sqlalchemy.Table, str]
        The source table from which rows will be inserted into `table2`.
    table2 : Union[sqlalchemy.Table, str]
        The target table into which rows from `table1` will be inserted.
    engine : Optional[sqlalchemy.engine.Engine], optional
        The database engine to use, by default None.

    Returns
    -------
    None

    Examples
    --------
    >>> import fullmetalalchemy as fa

    # Create an engine and get the source and target tables
    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> table1 = fa.features.get_table('xy', engine)
    >>> table2 = fa.features.get_table('xyz', engine)

    # Verify that table2 is empty
    >>> fa.select.select_records_all(table2)
    []

    # Insert rows from table1 into table2 and verify that the rows have been added
    >>> fa.insert.insert_from_table(table1, table2, engine)
    >>> fa.select.select_records_all(table2)
    [{'id': 1, 'x': 1, 'y': 2, 'z': None},
     {'id': 2, 'x': 2, 'y': 4, 'z': None},
     {'id': 3, 'x': 4, 'y': 8, 'z': None},
     {'id': 4, 'x': 8, 'y': 11, 'z': None}]
    """
    engine = _ex.check_for_engine(table1, engine)
    session = _features.get_session(engine)
    try:
        insert_from_table_session(table1, table2, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def insert_records_session(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    session: _sa_session.Session
) -> None:
    table = _features.str_to_table(table, session)
    if _features.missing_primary_key(table):
        _insert_records_slow_session(table, records, session)
    else:
        _insert_records_fast_session(table, records, session)

def insert_records(
    table: _t.Union[_sa.Table, str],
    records: _t.Sequence[_types.Record],
    engine: _t.Optional[_sa_engine.Engine] = None
) -> None:
    table, engine = _ex.convert_table_engine(table, engine)
    session = _features.get_session(engine)
    try:
        insert_records_session(table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

