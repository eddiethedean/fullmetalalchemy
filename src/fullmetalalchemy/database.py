from typing import Union, Any, List, Optional

import sqlalchemy as sa

from fullmetalalchemy.table import Table
from fullmetalalchemy.features import get_table_names


class DataBase:
    def __init__(
        self,
        connection_string_or_engine: Union[str, sa.engine.Engine]
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
        
    def table_names(self, schema: Optional[str] = None) -> List[str]:
        return get_table_names(self.engine, schema=schema)