import random
import string

from random import randint


class MockDataGenerator(object):
    def __init__(self):
        pass

    def generate_data(self, samples, all_columns):
        all_mocked_data = []
        for i in range(samples):
            single_mock_data = {}
            for column in all_columns:
                if column.column_type is str:
                    single_mock_data[column.name] = self.generate_string()
                elif column.column_type is int:
                    single_mock_data[column.name] = self.generate_integer()
                elif column.column_type is float:
                    single_mock_data[column.name] = self.generate_float()
                else:
                    single_mock_data[column.name] = self.generate_string()

                all_mocked_data.append(single_mock_data)
        return all_mocked_data

    def generate_string(self):
        random_str = \
            ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        return random_str

    def generate_integer(self):
        return randint(100, 900000)

    def generate_float(self):
        return random.uniform(1.5, 100.5)
