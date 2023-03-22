"""
Functions for converting between Python and SQL data types.
"""

import decimal as _decimal
import datetime as _datetime
import typing as _t

#from sqlalchemy import sql as _sql
from sqlalchemy import types as _sql


def sql_type(t):
    return _type_convert[t]


def python_type(t):
    return _sql_to_python[t]


_type_convert = {
    int: _sql.Integer,
    str: _sql.Unicode,
    float: _sql.Float,
    _decimal.Decimal: _sql.Numeric,
    _datetime.datetime: _sql.DateTime,
    bytes: _sql.LargeBinary,
    bool: _sql.Boolean,
    _datetime.date: _sql.Date,
    _datetime.time: _sql.Time,
    _datetime.timedelta: _sql.Interval,
    list: _sql.ARRAY,
    dict: _sql.JSON
}

_sql_to_python = {
    _sql.Integer: int,
    _sql.SmallInteger: int,
    _sql.SMALLINT: int,
    _sql.BigInteger: int,
    _sql.BIGINT: int,
    _sql.INTEGER: int,
    _sql.Unicode: str,
    _sql.NVARCHAR: str,
    _sql.NCHAR: str,
    _sql.Float: _decimal.Decimal,
    _sql.REAL: _decimal.Decimal,
    _sql.FLOAT: _decimal.Decimal,
    _sql.Numeric: _decimal.Decimal,
    _sql.NUMERIC: _decimal.Decimal,
    _sql.DECIMAL: _decimal.Decimal,
    _sql.DateTime: _datetime.datetime,
    _sql.TIMESTAMP: _datetime.datetime,
    _sql.DATETIME: _datetime.datetime,
    _sql.LargeBinary: bytes,
    _sql.BLOB: bytes,
    _sql.Boolean: bool,
    _sql.BOOLEAN: bool,
    _sql.MatchType: bool,
    _sql.Date: _datetime.date,
    _sql.DATE: _datetime.date,
    _sql.Time: _datetime.time,
    _sql.TIME: _datetime.time,
    _sql.Interval: _datetime.timedelta,
    _sql.ARRAY: list,
    _sql.JSON: dict
}


def get_sql_types(data: _t.Mapping[str, _t.Sequence]) -> list:
    return [get_sql_type(values) for values in data.values()]


def get_sql_type(values: _t.Sequence) -> _t.Any:
    for python_type in _type_convert:
        if all(type(val) == python_type for val in values):
            return _type_convert[python_type]
    return _type_convert[str]