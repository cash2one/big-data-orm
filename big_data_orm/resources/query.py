import logging

from big_data_orm.resources.column import Column


class Query(object):
    def __init__(self, columns):
        self.query_data = {}
        self.columns = columns

    def filter(self, clause):
        """
        Filter method for ORM.
        Arg clause already arrive here as a dict with op information.
        """
        pass

    def order_by(self, column, desc=False):
        pass

    def all(self):
        pass

    def first(self):
        pass

    def limit(self, value):
        pass

    def assemble(self):
        pass

    def _get_model_columns(self):
        pass

    def _check_filter_columns(self, op):
        """
        Check if the columns present at operation are present at the
        query columns.
        """
        if not self._column_is_present(op['left_value']):
            logging.error("Column not present at query columns.")
            return False
        if op['right_value_type'] is Column:
            if not self._column_is_present(op['right_value']):
                logging.error("Column not present at query columns.")
                return False

    def _column_is_present(self, column_name):
        for column in self.columns:
            if column.name == column_name:
                return True
        return False
