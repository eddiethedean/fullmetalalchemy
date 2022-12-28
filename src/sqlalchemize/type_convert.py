import decimal
import datetime
from typing import Any, Mapping, Sequence

from sqlalchemy import sql


def sql_type(t):
    return _type_convert[t]


_type_convert = {
    int: sql.sqltypes.Integer,
    str: sql.sqltypes.Unicode,
    float: sql.sqltypes.Float,
    decimal.Decimal: sql.sqltypes.Numeric,
    datetime.datetime: sql.sqltypes.DateTime,
    bytes: sql.sqltypes.LargeBinary,
    bool: sql.sqltypes.Boolean,
    datetime.date: sql.sqltypes.Date,
    datetime.time: sql.sqltypes.Time,
    datetime.timedelta: sql.sqltypes.Interval,
    list: sql.sqltypes.ARRAY,
    dict: sql.sqltypes.JSON
}


def get_sql_types(data: Mapping[str, Sequence]) -> list:
    return [get_sql_type(values) for values in data.values()]


def get_sql_type(values: Sequence) -> Any:
    for python_type in _type_convert:
        if all(type(val) == python_type for val in values):
            return _type_convert[python_type]
    return _type_convert[str]