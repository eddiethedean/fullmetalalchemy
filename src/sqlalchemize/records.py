import typing as _t
from frozendict import frozendict

Record = _t.Mapping[str, _t.Any]


def filter_record(
    record: Record,
    column_names: _t.Sequence[str]
) -> _t.Dict[str, _t.Any]:
    return {column_name: record[column_name] for column_name in column_names}


def records_equal(
    records1: _t.Sequence[Record],
    records2: _t.Sequence[Record],
) -> bool:
    """
    Check if two sets of records contain the same records.
    Order does not matter.

    Example
    -------
    >>> records1 = [{'id': 1, 'x': 1, 'y': 4},
    ...             {'id': 2, 'x': 1, 'y': 4},
    ...             {'id': 3, 'x': 1, 'y': 4}]

    >>> records2 = [{'id': 2, 'x': 1, 'y': 4},
    ...             {'id': 3, 'x': 1, 'y': 4},
    ...             {'id': 1, 'x': 1, 'y': 4}]
    >>> records_equal(records1, records2)
    True
    """
    if len(records1) != len(records2): return False
    if len(records1) == 0: return True

    frozen_records1 = set(frozendict(record) for record in records1)
    frozen_records2 = set(frozendict(record) for record in records2)
    return frozen_records1 == frozen_records2