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

    def _is_valid_type(self, value):
        pass

    def _build_op_dict(self, op_signal, value):
        op = {
            'signal': op_signal,
            'left_value': self.name,
        }
        if self._is_column_type(value):
            op['right_value'] = value.name
            op['right_value_type'] = Column
        else:
            op['right_value'] = value
            op['right_value_type'] = type(value)
        return op

    def _is_column_type(self, value):
        if type(value) is Column:
            return True
        else:
            return False

    def _check_op_types(self, left_type, right_type):
        """
        Check if the sides are comparable.
        """
        pass
