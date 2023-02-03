import typing as _t

import sqlalchemy.engine as _sa_engine
import sqlalchemy as _sa

import sqlalchemize.delete as _delete
import sqlalchemize.features as _features
import sqlalchemize.insert as _insert
import sqlalchemize.update as _update
import sqlalchemize.types as _types
import sqlalchemize.select as _select
import sqlalchemize.drop as _drop


class Table:
    def __init__(
        self,
        name: str,
        engine: _sa_engine.Engine,
        schema: _t.Optional[str] = None
    ) -> None:
        self.name = name
        self.engine = engine
        self.schema = schema

    def __eq__(self, other) -> bool:
        return _features.tables_metadata_equal(self.sa_table, other)

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

    def drop(
        self,
        if_exists: bool = True
    ) -> None:
        _drop.drop_table(self.sa_table, self.engine, if_exists, self.schema)

    def delete_records(
        self,
        column_name: str,
        values: _t.Sequence
    ) -> None:
        _delete.delete_records(self.sa_table, column_name, values)

    def delete_records_by_values(
        self,
        records: _t.List[dict]
    ) -> None:
        _delete.delete_records_by_values(self.sa_table, records, self.engine)

    def delete_all_records(self) -> None:
        _delete.delete_all_records(self.sa_table, self.engine)

    def insert_from_table(
        self,
        sa_table: _sa.Table
    ) -> None:
        _insert.insert_from_table(self.sa_table, sa_table, self.engine)

    def insert_records(
        self,
        records: _t.Sequence[_types.Record]
    ) -> None:
        _insert.insert_records(self.sa_table, records, self.engine)

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
            self.sa_table, self.engine, column_name, chunksize)

    def select_records_slice(
        self,
        start: _t.Optional[int] = None,
        stop: _t.Optional[int] = None,
        sorted: bool = False,
        include_columns: _t.Optional[_t.Sequence[str]] = None
    ) ->  _t.List[_types.Record]:
        return _select.select_records_slice(
            self.sa_table, self.engine, start, stop, sorted, include_columns)

    def select_column_values_by_slice(
        self,
        column_name: str,
        start: _t.Optional[int] = None,
        stop: _t.Optional[int] = None
    ) -> list:
        return _select.select_column_values_by_slice(
            self.sa_table, self.engine, column_name, start, stop)

    def select_column_value_by_index(
        self,
        column_name: str,
        index: int
    ) -> _t.Any:
        return _select.select_column_value_by_index(
            self.sa_table, self.engine, column_name, index)

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
            self.sa_table, self.engine, _slice, sorted)
    
    def select_record_by_primary_key(
        self,
        primary_key_value: _types.Record,
        include_columns: _t.Optional[_t.Sequence[str]] = None
    ) -> _types.Record:
        return _select.select_record_by_primary_key(
            self.sa_table, self.engine, primary_key_value, include_columns)
    
    def select_records_by_primary_keys(
        self,
        primary_keys_values: _t.Sequence[_types.Record],
        schema: _t.Optional[str] = None,
        include_columns: _t.Optional[_t.Sequence[str]] = None
    ) ->  _t.List[_types.Record]:
        return _select.select_records_by_primary_keys(
            self.sa_table, self.engine, primary_keys_values, schema, include_columns)

    def select_column_values_by_primary_keys(
        self,
        column_name: str,
        primary_keys_values: _t.Sequence[_types.Record]
    ) -> list:
        return _select.select_column_values_by_primary_keys(
            self.sa_table, self.engine, column_name, primary_keys_values)

    def select_value_by_primary_keys(
        self,
        column_name: str,
        primary_key_value: _types.Record,
        schema: _t.Optional[str] = None
    ) -> _t.Any:
        return _select.select_value_by_primary_keys(
            self.sa_table, self.engine, column_name, primary_key_value, schema)

    def update_matching_records(
        self,
        records: _t.Sequence[_types.Record],
        match_column_names: _t.Sequence[str]
    ) -> None:
        _update.update_matching_records(
            self.sa_table, records, match_column_names, self.engine)

    def update_records(
        self,
        records: _t.Sequence[_types.Record],
        match_column_names: _t.Optional[_t.Sequence[str]] = None,
    ) -> None:
        _update.update_records(
            self.sa_table, records, self.engine, match_column_names)

    def set_column_values(
        self,
        column_name: str,
        value: _t.Any
    ) -> None:
        _update.set_column_values(
            self.sa_table, column_name, value, self.engine)