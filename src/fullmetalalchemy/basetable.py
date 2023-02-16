from __future__ import annotations
import typing as _t

import sqlalchemy.engine as _sa_engine
import sqlalchemy as _sa
import sqlalchemy.orm.session as _sa_session
from hasattrs import has_iterable_attrs

import fullmetalalchemy.features as _features
import fullmetalalchemy.types as _types
import fullmetalalchemy.select as _select

Record = _t.Dict[str, _t.Any]


class BaseTable:
    """
    Base table class for fullmetalalchemy.table.Table 
    and fullmetalalchemy.sessiontable.SessionTable to inherit
    """
    def __init__(
        self,
        name: str,
        engine: _t.Union[_sa_engine.Engine, _sa_session.Session],
        schema: _t.Optional[str] = None
    ) -> None:
        self.name = name
        if type(engine) is _sa_session.Session:
            self.engine = engine.connection()
        self.engine = engine
        self.schema = schema

    def __len__(self) -> int:
        return _features.get_row_count(self.sa_table, self.engine)

    def __eq__(self, other) -> bool:
        return _features.tables_metadata_equal(self.sa_table, other)
    
    def __getitem__(self, key) -> _t.Union[dict, list]:
        """get records based on what type key is"""
        # int: a record at index
        if type(key) is int:
            return self.select_record_by_index(key)
        # str: a list of values for a named column
        elif type(key) is str:
            return self.select_column_values_all(key)
        # slice: a list of records
        elif type(key) is slice:
            return self.select_records_slice(key.start, key.stop)
        else:
            raise ValueError('key must be int, str, or slice')
        
    def __add__(self, other) -> None:
        """insert records from other"""
        # dict: insert a record
        if type(other) is dict:
            self.insert_records([other])
        # iterable[dict]: insert each record
        elif has_iterable_attrs(other):
            self.insert_records(list(other))
    
    def __sub__(self, other) -> None:
        """delete record that match other"""
        # dict: delete all records that match values
        if type(other) is dict:
            self.delete_records_by_values([other])
        # iterable[dict]: delete all records that match each record
        elif has_iterable_attrs(other):
            self.delete_records_by_values(list(other))
    
    def __delitem__(self, key) -> None:
        """delete records based on type of key"""
        # int: delete record at index
        if type(key) is int:
            record = self.select_record_by_index(key)
            self.delete_records_by_values([record])
        # slice: delete records in index slice
        elif type(key) is slice:
            records = self.select_records_slice(key.start, key.stop)
            self.delete_records_by_values(records)

    @property
    def records(self) -> _t.List[Record]:
        return self.select_records_all()
    
    @property
    def columns(self) -> _t.List[str]:
        return self.column_names

    def insert_records(self, records):
        raise NotImplemented
    
    def delete_records_by_values(self, records):
        raise NotImplemented
    
    @property
    def sa_table(self) -> _sa.Table:
        return _features.get_table(self.name, self.engine, self.schema)

    @property
    def row_count(self) -> int:
        return _features.get_row_count(self.sa_table, self.engine)

    @property
    def primary_key_names(self) -> _t.List[str]:
        return _features.primary_key_names(self.sa_table)

    @property
    def column_names(self) -> _t.List[str]:
        return _features.get_column_names(self.sa_table)

    @property
    def column_types(self) -> dict:
        return _features.get_column_types(self.sa_table)
    
    def select_records_all(
        self,
        sorted: bool = False,
        include_columns: _t.Optional[_t.Sequence[str]] = None
    ) ->  _t.List[_types.Record]:
        return _select.select_records_all(
            self.sa_table, self.engine, sorted, include_columns)
    
    def select_records_chunks(
        self,
        chunksize: int = 2,
        sorted: bool = False,
        include_columns: _t.Optional[_t.Sequence[str]] = None
    ) -> _t.Generator[ _t.List[_types.Record], None, None]:
        return _select.select_records_chunks(
            self.sa_table, self.engine, chunksize, sorted, include_columns)

    def select_existing_values(
        self,
        column_name: str,
        values: _t.Sequence
    ) -> list:
        return _select.select_existing_values(
            self.sa_table, column_name, values)

    def select_column_values_all(
        self,
        column_name: str
    ) -> list:
        return _select.select_column_values_all(
            self.sa_table, column_name, self.engine)

    def select_column_values_chunks(
        self,
        column_name: str,
        chunksize: int
    ) -> _t.Generator[list, None, None]:
        return _select.select_column_values_chunks(
            self.sa_table, column_name, chunksize, self.engine)

    def select_records_slice(
        self,
        start: _t.Optional[int] = None,
        stop: _t.Optional[int] = None,
        sorted: bool = False,
        include_columns: _t.Optional[_t.Sequence[str]] = None
    ) ->  _t.List[_types.Record]:
        return _select.select_records_slice(
            self.sa_table, start, stop, self.engine, sorted, include_columns)

    def select_column_values_by_slice(
        self,
        column_name: str,
        start: _t.Optional[int] = None,
        stop: _t.Optional[int] = None
    ) -> list:
        return _select.select_column_values_by_slice(
            self.sa_table, column_name, start, stop, self.engine)

    def select_column_value_by_index(
        self,
        column_name: str,
        index: int
    ) -> _t.Any:
        return _select.select_column_value_by_index(
            self.sa_table, column_name, index, self.engine)

    def select_record_by_index(
        self,
        index: int
    ) -> _t.Dict[str, _t.Any]:
        return _select.select_record_by_index(
            self.sa_table, index, self.engine)

    def select_primary_key_records_by_slice(
        self,
        _slice: slice,
        sorted: bool = False
    ) ->  _t.List[_types.Record]:
        return _select.select_primary_key_records_by_slice(
            self.sa_table, _slice, self.engine, sorted)
    
    def select_record_by_primary_key(
        self,
        primary_key_value: _types.Record,
        include_columns: _t.Optional[_t.Sequence[str]] = None
    ) -> _types.Record:
        return _select.select_record_by_primary_key(
            self.sa_table, primary_key_value, self.engine, include_columns)
    
    def select_records_by_primary_keys(
        self,
        primary_keys_values: _t.Sequence[_types.Record],
        schema: _t.Optional[str] = None,
        include_columns: _t.Optional[_t.Sequence[str]] = None
    ) ->  _t.List[_types.Record]:
        return _select.select_records_by_primary_keys(
            self.sa_table, primary_keys_values, self.engine, schema, include_columns)

    def select_column_values_by_primary_keys(
        self,
        column_name: str,
        primary_keys_values: _t.Sequence[_types.Record]
    ) -> list:
        return _select.select_column_values_by_primary_keys(
            self.sa_table, column_name, primary_keys_values, self.engine)

    def select_value_by_primary_keys(
        self,
        column_name: str,
        primary_key_value: _types.Record,
        schema: _t.Optional[str] = None
    ) -> _t.Any:
        return _select.select_value_by_primary_keys(
            self.sa_table, column_name, primary_key_value, self.engine, schema)