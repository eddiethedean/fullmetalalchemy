from typing import Union, Any

import sqlalchemy as sa

from fullmetalalchemy.table import Table


class DataBase:
    def __init__(
        self,
        connection_string_or_engine: Union[str, sa.Engine]
    ) -> None:
        if type(connection_string_or_engine) is str:
            self.engine = sa.create_engine(connection_string_or_engine)
        else:
            self.engine = connection_string_or_engine

    def __getitem__(self, table_name: str) -> sa.Table:
        return Table(table_name, self.engine)

    def execute(self, query: str, *multiparams, **params) -> Union[Any, None]:
        with self.engine.connect() as connection:
            return connection.execute(query, *multiparams, **params)