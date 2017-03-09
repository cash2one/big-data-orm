import unittest

from mock import MagicMock
from big_data_orm.resources.query import Query
from big_data_orm.resources.column import Column


DATABASE_NAME = 'adwords_data'


class fakeSessionValidData():
    @staticmethod
    def run_query(query):
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
    def run_query(query):
        return {}


class fakeSessionWithMock():
    def __init__(self):
        self.run_query = MagicMock(return_value={})


class QueryTestCase(unittest.TestCase):
    table_date_range = '' + DATABASE_NAME + '.testing WHERE _PARTITIONTIME BETWEEN ' +\
        'TIMESTAMP(\'2010-01-01\') AND TIMESTAMP(\'2030-01-01\')'

    def test_query_simple_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_by_date(self):
        begin_date = '2015-12-01'
        end_date = '2016-01-01'

        table_date_range = '' + DATABASE_NAME + '.testing WHERE _PARTITIONTIME BETWEEN ' +\
            'TIMESTAMP(\'' + begin_date + '\') AND TIMESTAMP' +\
            '(\'' + end_date + '\')'

        table_name = 'testing'

        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')

        q = Query(
            [c_1, c_2],
            table_name,
            dataset_id=DATABASE_NAME,
            is_partitioned=True
        ).filter_by_date(begin_date, end_date)

        expected_response = 'SELECT column_1, column_2 FROM ' + \
            table_date_range

        response = q.assemble()

        self.assertEquals(expected_response, response)

    def test_query_limit_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        q = q.limit(100)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' LIMIT 100'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_limit_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
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
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        q = q.order_by(c_1)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' ORDER BY column_1'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_order_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
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
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        q = q.order_by(c_1, desc=True)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' ORDER BY column_1 DESC'
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_order_4(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
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
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
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
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        q = q.filter(c_1 == 'test')
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' AND ' + \
            'column_1 = \'test\''
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
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
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        # Wrong comparision
        q = q.filter(c_1 == 'test')
        q = q.filter(c_2 == 'test2')
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' AND ' + \
            'column_1 = \'test\' AND column_2 = \'test2\''
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_4(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        q = q.filter(c_1 >= 'test')
        q = q.filter(c_2 == 'test2')
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' AND ' + \
            'column_1 >= \'test\' AND column_2 = \'test2\''
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_query_filter_5(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        q = q.filter(c_1 >= 'test')
        # Wrong comparision
        q = q.filter(c_2 == 100)
        expected_response = 'SELECT column_1, column_2 FROM ' + \
            self.table_date_range + ' AND ' + \
            'column_1 >= \'test\''
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_parse_in_list(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        response = q._parse_in_list(['v1', 'v2'])
        self.assertEqual('(\'v1\', \'v2\')', response)

    def test_parse_in_list_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        response = q._parse_in_list([10, 20])
        self.assertEqual('(10, 20)', response)

    def test_first_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        response = q.first(fakeSessionEmptyData)
        self.assertEqual({}, response)

    def test_first_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        response = q.first(fakeSessionValidData)
        self.assertEqual({'a': 10}, response)

    def test_all_1(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        response = q.all(fakeSessionValidData)
        self.assertEqual([{'a': 10}, {'b': 20}], response)

    def test_all_2(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        response = q.all(fakeSessionEmptyData)
        self.assertEqual({}, response)

    def test_query_flatten(self):
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query(
            [c_1, c_2], table_name,
            dataset_id=DATABASE_NAME, is_partitioned=False
        )
        q = q.flatten(c_2)
        expected_response = str(
            "SELECT column_1, column_2 FROM "
            "FLATTEN(" + DATABASE_NAME + "." + table_name + ", column_2)"
        )
        response = q.assemble()
        self.assertEquals(expected_response, response)

    def test_session_call_1(self):
        fake_session = fakeSessionWithMock()
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        query_str = q.assemble()
        q.all(fake_session)
        fake_session.run_query.assert_called_with(query_str)

    def test_session_call_2(self):
        fake_session = fakeSessionWithMock()
        table_name = 'testing'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        query_str = q.assemble()
        q.all(fake_session)
        fake_session.run_query.assert_called_with(query_str)

    def test_session_call_3(self):
        fake_session = fakeSessionWithMock()
        table_name = 'adwords_account_report'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        query_str = q.assemble()
        q.all(fake_session)
        fake_session.run_query.assert_called_with(query_str)

    def test_session_call_4(self):
        fake_session = fakeSessionWithMock()
        table_name = 'adwords_account_report'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        self.assertRaises(TypeError, q.all, fake_session, newest_only=True, filter_key='nope')

    def test_session_call_5(self):
        fake_session = fakeSessionWithMock()
        table_name = 'some_table_name'
        c_1 = Column(str, 'column_1')
        c_2 = Column(str, 'column_2')
        q = Query([c_1, c_2], table_name, dataset_id=DATABASE_NAME, is_partitioned=True)
        query_str = q.assemble()
        q.all(fake_session)
        fake_session.run_query.assert_called_with(query_str)
