import unittest

from big_data_orm.resources.utils.dict_fields_manipulator import DictFieldsManipulator
from big_data_orm.resources.column import Column


class DictManipulator(unittest.TestCase):

    def test_children_columns(self):
        children = {
            'field_name': Column(str, 'field_name'),
            'type': Column(str, 'type')
        }
        response = DictFieldsManipulator.check_if_children_are_columns(children, Column)
        self.assertEqual(response, True)

    def test_children_columns_wrong(self):
        children = {
            'field_name': Column(str, 'field_name'),
            'type': 10
        }
        response = DictFieldsManipulator.check_if_children_are_columns(children, Column)
        self.assertEqual(response, False)

    def test_children_columns_nesting(self):
        children = {
            'field_name': Column(str, 'field_name'),
            'type': Column(
                dict, 'type',
                {
                    'subkey_1': Column(str, 'subkey_1'),
                }
            )
        }
        response = DictFieldsManipulator.check_if_children_are_columns(children, Column)
        self.assertEqual(response, True)

    def test_get_children_leafs(self):
        columns = [
            Column(
                dict, 'field',
                {
                    'key_1': Column(
                            dict, 'field_1',
                            {
                                'key_1_1': Column(str, 'field_1_1'),
                                'key_1_2': Column(int, 'field_1_2'),
                            }
                        ),
                    'key_2': Column(str, 'field_2')
                }
            )
        ]
        response = DictFieldsManipulator().get_valid_dict_children_leafs(columns)
        self.assertEqual(len(response), 3)
        self.assertEqual(response[0].name, 'field_1_2')
        self.assertEqual(response[2].name, 'field_2')
