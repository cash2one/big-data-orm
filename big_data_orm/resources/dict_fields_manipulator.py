import logging


class DictFieldsManipulator(object):
    def __init__(self):
        self.valid_fields = []

    @staticmethod
    def check_if_children_are_columns(children, column_type):
        """
        If the Columns have children, all of they must be Columns.
        """
        errors = 0
        for _key in children.keys():
            child = children.get(_key)
            if type(child) is not column_type:
                logging.error("Wrong field type at {}".format(_key))
                errors = errors + 1
        if errors > 0:
            return False
        return True

    def get_valid_dict_children(self, columns):
        for column in columns:
            if column.column_type is dict:
                self.get_leaf_fields(column)
            else:
                self.valid_fields.append(column)
        return self.valid_fields

    def get_leaf_fields(self, column):
        valid_leaf_field_types = [int, float, str]
        if column.column_type in valid_leaf_field_types:
            self.valid_fields.append(column)
        elif column.column_type is dict:
            for _key in column.children.keys():
                self.get_leaf_fields(column.children[_key])
        else:
            logging.error("Not valid field type: " +
                          "{} at Column {}".format(column.column_type, column.name))

    def change_dict_columns_names(self, columns):
        for column in columns:
            if column.column_type is dict:
                self.build_nested_names(column, '')
        return columns

    def build_nested_names(self, column, base_name):
        if column.column_type is dict:
            for _key in column.children.keys():
                if base_name == '':
                    next_base_name = column.name
                else:
                    next_base_name = base_name + '.' + column.name
                self.build_nested_names(column.children[_key], next_base_name)
        else:
            column.name = base_name + '.' + column.name
