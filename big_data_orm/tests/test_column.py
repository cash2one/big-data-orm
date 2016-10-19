import unittest

from big_data_orm.resources.column import Column


class ColumnTestCase(unittest.TestCase):
    def test_eq_op(self):
        c_1 = Column(str, 'c_1')
        r = c_1 == 'test'
        expected_r = {
            'type': 'and',
            'signal': '=',
            'left_value': 'c_1',
            'left_value_type': str,
            'right_value': 'test',
            'right_value_type': str
        }
        self.assertEqual(expected_r, r)

    def test_eq_op_error(self):
        c_1 = Column(str, 'c_1')
        r = c_1 == 10
        expected_r = {}
        self.assertEqual(expected_r, r)

    def test_eq_op_error_2(self):
        c_1 = Column(str, 'c_1')
        c_2 = Column(int, 'c_2')
        r = c_1 == c_2
        expected_r = {}
        self.assertEqual(expected_r, r)

    def test_eq_op_2(self):
        c_1 = Column(int, 'c_1')
        c_2 = Column(int, 'c_2')
        r = c_1 == c_2
        expected_r = {
            'type': 'and',
            'signal': '=',
            'left_value': 'c_1',
            'left_value_type': int,
            'right_value': 'c_2',
            'right_value_type': Column
        }
        self.assertEqual(expected_r, r)

    def test_le_op(self):
        c_1 = Column(str, 'c_1')
        r = c_1 <= 'test'
        expected_r = {
            'type': 'and',
            'signal': '<=',
            'left_value': 'c_1',
            'left_value_type': str,
            'right_value': 'test',
            'right_value_type': str
        }
        self.assertEqual(expected_r, r)

    def test_ge_op(self):
        c_1 = Column(str, 'c_1')
        r = c_1 >= 'test'
        expected_r = {
            'type': 'and',
            'signal': '>=',
            'left_value': 'c_1',
            'left_value_type': str,
            'right_value': 'test',
            'right_value_type': str
        }
        self.assertEqual(expected_r, r)

    def test_ne_op(self):
        c_1 = Column(str, 'c_1')
        r = c_1 != 'test'
        expected_r = {
            'type': 'and',
            'signal': '!=',
            'left_value': 'c_1',
            'left_value_type': str,
            'right_value': 'test',
            'right_value_type': str
        }
        self.assertEqual(expected_r, r)

    def test_lt_op(self):
        c_1 = Column(str, 'c_1')
        r = c_1 > 'test'
        expected_r = {
            'type': 'and',
            'signal': '>',
            'left_value': 'c_1',
            'left_value_type': str,
            'right_value': 'test',
            'right_value_type': str
        }
        self.assertEqual(expected_r, r)

    def test_gt_op(self):
        c_1 = Column(str, 'c_1')
        r = c_1 < 'test'
        expected_r = {
            'type': 'and',
            'signal': '<',
            'left_value': 'c_1',
            'left_value_type': str,
            'right_value': 'test',
            'right_value_type': str
        }
        self.assertEqual(expected_r, r)

    def test_in_op(self):
        c_1 = Column(str, 'c_1')
        r = c_1.in_(['t1', 't2'])
        expected_r = {
            'type': 'and',
            'signal': 'IN',
            'left_value': 'c_1',
            'left_value_type': str,
            'right_value': ['t1', 't2'],
            'right_value_type': list
        }
        self.assertEqual(expected_r, r)

    def test_in_op_error(self):
        c_1 = Column(str, 'c_1')
        r = c_1.in_('t2')
        expected_r = {}
        self.assertEqual(expected_r, r)
