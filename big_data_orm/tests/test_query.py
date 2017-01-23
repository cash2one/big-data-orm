import unittest

from big_data_orm.resources.query import Query
from big_data_orm.resources.column import Column


class fakeSessionValidData():
    @staticmethod
    def run_query(query, newest_only=False, filter_key=''):
        return [
            {
                'a': 10
            },
            {
                'b': 20
            }
        ]


class fakeSessionEmptyData():
    @staticmethod
    def run_query(query, newest_only=False, filter_key=''):
        return {}


class QueryTestCase(unittest.TestCase):
    table_date_range = 'TABLE_DATE_RANGE(test_adwords_report_data.testing, ' +\
        'TIMESTAMP(\'2010-01-01\'), TIMESTAMP(\'2030-01-01\'))'

    def test_query_simple_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_limit_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.limit(100)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' LIMIT 100'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_limit_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.limit(100)
        q = q.limit(101)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' LIMIT 101'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_order_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.order_by(c_1)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' ORDER BY column_1'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_order_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.order_by(c_1)
        q = q.order_by(c_2)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' ORDER BY column_1 , column_2'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_order_3(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.order_by(c_1, desc=True)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' ORDER BY column_1 DESC'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_order_4(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.order_by(c_1, desc=True)
        q = q.order_by(c_2)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' ORDER BY ' +\
            'column_1 DESC , column_2'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_order_5(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.order_by(c_1, desc=True)
        q = q.order_by(c_2, desc=True)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' ORDER BY ' +\
            'column_1 DESC , column_2 DESC'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.filter(c_1 == 'test')
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' WHERE ' + \
            'column_1 = \'test\''
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        # Wrong comparision
        q = q.filter(c_1 == 10)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_3(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        # Wrong comparision
        q = q.filter(c_1 == 'test')
        q = q.filter(c_2 == 'test2')
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' WHERE ' + \
            'column_1 = \'test\' and column_2 = \'test2\''
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_4(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.filter(c_1 >= 'test')
        q = q.filter(c_2 == 'test2')
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' WHERE ' + \
            'column_1 >= \'test\' and column_2 = \'test2\''
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_5(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        q = q.filter(c_1 >= 'test')
        # Wrong comparision
        q = q.filter(c_2 == 100)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' WHERE ' + \
            'column_1 >= \'test\''
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_parse_in_list(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        response = q._parse_in_list(['v1', 'v2'])
        self.assertEqual('(\'v1\', \'v2\')', response)

    def test_parse_in_list_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        response = q._parse_in_list([10, 20])
        self.assertEqual('(10, 20)', response)

    def test_first_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        response = q.first(fakeSessionEmptyData)
        self.assertEqual({}, response)

    def test_first_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        response = q.first(fakeSessionValidData)
        self.assertEqual({'a': 10}, response)

    def test_all_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        response = q.all(fakeSessionValidData)
        self.assertEqual([{'a': 10}, {'b': 20}], response)

    def test_all_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name)
        response = q.all(fakeSessionEmptyData)
        self.assertEqual({}, response)
