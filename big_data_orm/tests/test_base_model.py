import unittest

from big_data_orm.resources.base_model import BaseModel
from big_data_orm.resources.column import Column
from big_data_orm.resources.query import Query

DATABASE_NAME = 'adwords_data'


class FakeModel(BaseModel):
    def __init__(self):
        self.c_1 = Column(str, 'test_1')
        self.c_2 = Column(str, 'test_2')


class FakeModel2(BaseModel):
    def __init__(self):
        self.c_1 = Column(str, 'test_1')
        self.c_2 = Column(str, 'test_2')
        self.c_3 = Column(
            dict, 'test_3',
            {
                'c_3_1': Column(
                        dict, 'test_3_1',
                        {
                            'c_3_1_1': Column(int, 'test_3_1_1'),
                            'c_3_1_2': Column(str, 'test_3_1_2')
                        }
                    ),
                'c_3_2': Column(str, 'test_3_2')
            }
        )


class BaseModelTestCase(unittest.TestCase):
    def test_table_name(self):
        b = BaseModel()
        self.assertEqual(None, b.__tablename__)

    def test_query_tablename_none(self):
        b = BaseModel()
        c_1 = Column(str, 'testing')
        response = b.query([c_1], is_partitioned=False, dataset_id=DATABASE_NAME)
        self.assertEqual(None, response)

    def test_query_tablename_not_none(self):
        b = BaseModel()
        b.__tablename__ = 'Test'
        c_1 = Column(str, 'testing')
        response = b.query([c_1], dataset_id=DATABASE_NAME, is_partitioned=True)
        self.assertEqual(Query, type(response))

    def test_query_no_args(self):
        b = BaseModel()
        b.__tablename__ = 'Test'
        self.assertRaises(TypeError, b.query)

    def test_query_no_args_1(self):
        b = BaseModel()
        b.__tablename__ = 'Test'
        self.assertRaises(TypeError, b.query, True)

    def test_query_no_args_2(self):
        b = BaseModel()
        b.__tablename__ = 'Test'
        self.assertRaises(TypeError, b.query, DATABASE_NAME)

    def test_get_all_columns_1(self):
        b = BaseModel()
        response = b._get_all_columns()
        self.assertEqual([], response)

    def test_get_all_columns_2(self):
        b = FakeModel()
        response = b._get_all_columns()
        self.assertEqual([b.c_1, b.c_2], response)

    def test_get_all_columns_3(self):
        b = FakeModel2()
        response = b._get_all_columns()

        expected_response = [
            'test_1', 'test_2',
            'test_3.test_3_1.test_3_1_2',
            'test_3.test_3_1.test_3_1_1',
            'test_3.test_3_2'
        ]

        response_names = []
        for column in response:
            response_names.append(column.name)

        self.assertEqual(len(response), len(expected_response))
        self.assertListEqual(response_names, expected_response)
