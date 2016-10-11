from big_data_orm.resources.column import Column


class BaseModel(object):
    """
    Base DB model. All models should inherit this one.
    """
    a = Column(int, 'a')
    def __init__(self):
        pass

    def query(self, columns=None):
        pass

    def _get_all_columns(self):
        columns = [attr for attr in dir(BaseModel()) \
                   if type(getattr(BaseModel(), attr)) is Column]
        return columns
