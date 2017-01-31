import unittest

from big_data_orm.resources.mock_data_generator import MockDataGenerator
from big_data_orm.resources.column import Column


class MockDataGeneratorTestCase(unittest.TestCase):

    def test_number_of_samples_1(self):
        c1 = Column(str, 'column_1')
        c2 = Column(str, 'column_2')
        m = MockDataGenerator()
        samples = 10
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(samples, len(response))
        samples = 20
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(samples, len(response))
        for r in response:
            self.assertEqual(str, type(r['column_1']))
            self.assertEqual(str, type(r['column_2']))

    def test_number_of_samples_2(self):
        c1 = Column(str, 'column_1')
        c2 = Column(int, 'column_2')
        m = MockDataGenerator()
        samples = 10
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(samples, len(response))
        samples = 20
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(samples, len(response))
        for r in response:
            self.assertEqual(str, type(r['column_1']))
            self.assertEqual(int, type(r['column_2']))

    def test_number_of_samples_3(self):
        c1 = Column(float, 'column_1')
        c2 = Column(int, 'column_2')
        m = MockDataGenerator()
        samples = 10
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(samples, len(response))
        samples = 20
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(samples, len(response))
        for r in response:
            self.assertEqual(float, type(r['column_1']))
            self.assertEqual(int, type(r['column_2']))

    def test_number_of_samples_4(self):
        c1 = Column(list, 'column_1')
        c2 = Column(int, 'column_2')
        m = MockDataGenerator()
        samples = 10
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(samples, len(response))
        samples = 20
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(samples, len(response))
        for r in response:
            self.assertEqual(str, type(r['column_1']))
            self.assertEqual(int, type(r['column_2']))

    def test_number_of_samples_negative(self):
        c1 = Column(str, 'column_1')
        c2 = Column(str, 'column_2')
        m = MockDataGenerator()
        samples = -10
        response = m.generate_data(samples, [c1, c2])
        self.assertEqual(0, len(response))
