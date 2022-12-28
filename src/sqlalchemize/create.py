from typing import Optional, Sequence

import sqlalchemy as sa
import sqlalchemy.engine as sa_engine
import sqlalchemy.schema as sa_schema

import sqlalchemize.type_convert as type_convert
import sqlalchemize.features as features


def create_table(
    table_name: str,
    column_names: Sequence[str],
    column_types: Sequence,
    primary_key: str,
    engine: sa_engine.Engine,
    schema: Optional[str] = None,
    autoincrement: Optional[bool] = True,
    if_exists: Optional[str] = 'error'
) -> sa.Table:
    
    cols = []
    
    for name, python_type in zip(column_names, column_types):
        sa_type = type_convert._type_convert[python_type]
        if name == primary_key:
            col = sa.Column(name, sa_type,
                            primary_key=True,
                            autoincrement=autoincrement)
        else:
            col = sa.Column(name, sa_type)
        cols.append(col)

    metadata = sa.MetaData(engine)
    table = sa.Table(table_name, metadata, *cols, schema=schema)
    if if_exists == 'replace':
        drop_table_sql = sa_schema.DropTable(table, if_exists=True)
        engine.execute(drop_table_sql)
    table_creation_sql = sa_schema.CreateTable(table)
    engine.execute(table_creation_sql)
    return features.get_table(table_name, engine, schema=schema)