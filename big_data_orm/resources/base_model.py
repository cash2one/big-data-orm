import logging

from big_data_orm.resources.query import Query
from big_data_orm.resources.column import Column


class BaseModel(object):
    """
    Base DB model. All models should inherit this one.
    """
    __tablename__ = None

    def __init__(self):
        pass

    def query(self, *args):
        if not self.__tablename__:
            logging.error("Table name not defined...")
            return None

        columns = []

        if not args:
            columns = self._get_all_columns()
        else:
            columns = [i for i in args]

        if not columns:
            return None

        return Query(columns, self.__tablename__)

    def _get_all_columns(self):
        columns = [attr for attr in dir(self.__class__())
                   if type(getattr(self.__class__(), attr)) is Column]
        if not columns:
            logging.error("Model is out of columns!")
            return []
        else:
            columns_obj = [getattr(self.__class__(), column) for column in columns]
            return columns_obj
