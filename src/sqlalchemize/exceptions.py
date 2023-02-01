class ForceFail(Exception):
    pass


class SliceError(IndexError):
    pass


class MissingPrimaryKey(Exception):
    def __init__(self, message='Table must have primary key. Use alterize.create_primary_key to add a primary key to your table.', errors=None):
        super().__init__(message, errors)


def rollback_on_exception(method):
    def inner_method(self, *args, **kwargs):
        try:
            method(self, *args, **kwargs)
        except Exception as e:
            self.rollback()
            raise e
    return inner_method


def check_for_engine(sa_table, engine):
    if engine is None:
        engine = sa_table.bind
    if engine is None:
        raise ValueError('sa_table must be bound to engine or pass engine parameter.')
    return engine


