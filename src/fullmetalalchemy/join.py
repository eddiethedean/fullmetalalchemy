"""Module for joining SQL tables."""

import typing as _t

import sqlalchemy as _sa

import fullmetalalchemy.types as _types


def left_join_records_all(
        left_table: _sa.Table,
        right_table: _sa.Table,
        left_on: str,
        right_on: str
) -> _t.List[_types.Record]:
    """Left join two SQL tables.

    Parameters
    ----------
    left_table : _sa.Table
        Left table to join.
    right_table : _sa.Table
        Right table to join.
    left_on : str
        Column name of left table to join on.
    right_on : str
        Column name of right table to join on.

    Returns
    -------
    _t.List[_types.Record]
        Joined table results.
    """
    return left_table.join(right_table, left_table.c[left_on] == right_table.c[right_on])