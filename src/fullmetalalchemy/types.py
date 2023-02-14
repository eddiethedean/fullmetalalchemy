"""
Module of custom data types.
"""

import typing as _t

import sqlalchemy.engine as sa_engine
import sqlalchemy.orm.session as sa_session


Record = _t.Dict[str, _t.Any]
SqlConnection = _t.Union[sa_engine.Engine, sa_session.Session, sa_engine.Connection]




