from typing import Optional

import sqlalchemy as sa
import sqlalchemy.engine as sa_engine
import sqlalchemy.schema as sa_schema

import sqlalchemize.features as features


def drop_table(
    table: sa.Table | str,
    engine: sa_engine.Engine,
    if_exists: bool = True,
    schema: Optional[str] = None
) -> None:
    if isinstance(table, str):
        if table not in sa.inspect(engine).get_table_names(schema=schema):
            if if_exists:
                return
        table = features.get_table(table, engine, schema=schema)
    sql = sa_schema.DropTable(table, if_exists=if_exists)
    engine.execute(sql)