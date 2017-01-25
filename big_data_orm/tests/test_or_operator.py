import unittest


from big_data_orm.resources.column import Column
from big_data_orm.resources.query import Query
from big_data_orm.resources.or_operator import or_


DATABASE_NAME = 'adwords_report_data'


class OROperatorTestCase(unittest.TestCase):
    def basic_test_1(self):
        c_1 = Column(str, 'c1')
        c_2 = Column(int, 'c2')
        q = Query([c_1, c_2], 'test')
        q.filter(or_(c_1 == 'test_1', c_2 == 10))
        expected_query = 'SELECT c1, c2 FROM ' +\
            'adwords_report_data.test WHERE _PARTITIONTIME BETWEEN ' +\
            'TIMESTAMP(\'2010-01-01\') AND ' +\
            'TIMESTAMP(\'2030-01-01\')' +\
            ' AND c1 = \'test_1\' OR c2 = 10'
        self.assertEquals(q.assemble(), expected_query)

    def basic_test_2(self):
        c_1 = Column(str, 'c1')
        c_2 = Column(int, 'c2')
        q = Query([c_1, c_2], 'test')
        q.filter(or_(c_1 == 10, c_2 == 10))
        expected_query = 'SELECT c1, c2 FROM ' +\
            ''+ DATABASE_NAME +'.test WHERE _PARTITIONTIME BETWEEN ' +\
            'TIMESTAMP(\'2010-01-01\') AND ' +\
            'TIMESTAMP(\'2030-01-01\')'
        self.assertEquals(q.assemble(), expected_query)

    def basic_test_3(self):
        c_1 = Column(str, 'c1')
        c_2 = Column(int, 'c2')
        q = Query([c_1, c_2], 'test')
        q.filter(or_(c_1 == 'test_1', c_2 == 10))
        q = q.filter(c_1 == 'test_2')
        expected_query = 'SELECT c1, c2 FROM ' +\
            ''+ DATABASE_NAME +'.test WHERE _PARTITIONTIME BETWEEN ' +\
            'TIMESTAMP(\'2010-01-01\') AND ' +\
            'TIMESTAMP(\'2030-01-01\')' +\
            ' AND c1 = \'test_1\' OR c2 = 10' +\
            ' AND c1 = \'test_2\''
        self.assertEquals(q.assemble(), expected_query)

    def basic_test_4(self):
        c_1 = Column(str, 'c1')
        c_2 = Column(int, 'c2')
        q = Query([c_1, c_2], 'test')
        q.filter(or_(c_1 == 10, c_2 == 10))
        q = q.filter(c_1 == 'test_2')
        expected_query = 'SELECT c1, c2 FROM ' +\
            ''+ DATABASE_NAME +'.test WHERE _PARTITIONTIME BETWEEN ' +\
            'TIMESTAMP(\'2010-01-01\') AND ' +\
            'TIMESTAMP(\'2030-01-01\')' +\
            ' AND c1 = \'test_2\''
        self.assertEquals(q.assemble(), expected_query)
