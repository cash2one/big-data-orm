import logging


class Column(object):
    """
    Representation of a table column.
    """
    def __init__(self, column_type, name):
        self.column_type = column_type
        self.name = name

    def __eq__(self, value):
        return self._build_op_dict('=', value)

    def __le__(self, value):
        return self._build_op_dict('<=', value)

    def __ge__(self, value):
        return self._build_op_dict('>=', value)

    def __ne__(self, value):
        return self._build_op_dict('!=', value)

    def __gt__(self, value):
        return self._build_op_dict('>', value)

    def __lt__(self, value):
        return self._build_op_dict('<', value)

    def in_(self, values):
        if type(values) is not list:
            logging.error("Argument should be a list")
            return {}
        op = {
            'signal': 'IN',
            'left_value': self.name,
            'left_value_type': self.column_type,
            'right_value': values,
            'right_value_type': list
        }
        return op

    def _build_op_dict(self, op_signal, value):
        op = {
            'signal': op_signal,
            'left_value': self.name,
            'left_value_type': self.column_type,
        }
        if self._is_column_type(value):
            op['right_value'] = value.name
            op['right_value_type'] = Column
            if not self._check_op_types(value.column_type):
                return {}
        else:
            op['right_value'] = value
            op['right_value_type'] = type(value)
            if not self._check_op_types(type(value)):
                return {}
        return op

    def _is_column_type(self, value):
        return True if type(value) is Column else False

    def _check_op_types(self, right_type):
        """
        Check if the sides are comparable.
        """
        if self._is_number(self.column_type) and self._is_number(right_type):
            return True
        elif self.column_type is str and right_type is str:
            return True
        else:
            logging.error("Wrong type comparison. {} and {}".format(self.column_type,
                                                                    right_type))
            return False

    def _is_number(self, column_type):
        return True if column_type is float or column_type is int else False
