import unittest
import mock

from big_data_orm.big_query_connector.session import Session
from big_data_orm.tests.mock_data import big_query_response


class fakeStorage():
    @staticmethod
    def get():
        return fakeCredential


class fakeCredential():
    @staticmethod
    def authorize(http):
        return 'fake_authentication'


class fakeRequest():
    @staticmethod
    def execute():
        return None


class fakeService():
    @staticmethod
    def jobs():
        return fakeService

    @staticmethod
    def query(projectId, body):
        return fakeRequest


class fakeService2():
    @staticmethod
    def jobs():
        return fakeService2

    @staticmethod
    def get(projectId, jobId):
        return fakeRequestWithData


class fakeRequestWithData():
    @staticmethod
    def execute():
        return {
            'status': {
                'state': 'DONE'
            }
        }


class fakeService3():
    @staticmethod
    def jobs():
        return fakeService3

    @staticmethod
    def get(projectId, jobId):
        return fakeRequestWithData2


class fakeRequestWithData2():
    @staticmethod
    def execute():
        return {
            'status': {
                'state': 'NOT_DONE'
            }
        }


class SessionTestCase(unittest.TestCase):

    def _build_session(self):
        return Session(
            project_id='project_id', dataset_id='dataset_id',
            storage_file='file.dat'
        )

    def test_init(self):
        s = Session(
            project_id='project_id', dataset_id='dataset_id',
            storage_file='file.dat'
        )

        self.assertEquals(s.api_name, 'bigquery')
        self.assertEquals(s.api_version, 'v2')
        self.assertEqual(s.api_scope, [
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/cloud-platform'
        ])
        self.assertEqual(s.connected, False)
        self.assertEqual(s.project_id, 'project_id')
        self.assertEqual(s.dataset_id, 'dataset_id')
        self.assertEqual(s.storage_file, 'file.dat')
        self.assertEqual(None, s.service)

    @mock.patch('big_data_orm.big_query_connector.session.file.Storage')
    @mock.patch('big_data_orm.big_query_connector.session.discovery')
    @mock.patch('big_data_orm.big_query_connector.session.Http')
    def test_connect(self, fake_http, fake_discovery, fake_file_storage):
        fake_file_storage.return_value = fakeStorage
        s = Session(
            project_id='project_id', dataset_id='dataset_id',
            storage_file='file.dat'
        )
        s.connect()
        self.assertEqual(s.connected, True)

    def test_run_query(self):
        s = self._build_session()
        response = s.run_query('query')
        self.assertEqual(response, {})

    @mock.patch('big_data_orm.big_query_connector.session.file.Storage')
    @mock.patch('big_data_orm.big_query_connector.session.discovery')
    @mock.patch('big_data_orm.big_query_connector.session.Http')
    @mock.patch('big_data_orm.big_query_connector.session.Session._get_job_id')
    @mock.patch('big_data_orm.big_query_connector.session.Session._check_if_finished')
    @mock.patch('big_data_orm.big_query_connector.session.Session._get_query_result')
    def test_wait_response(self, fake_query_result, fake_finished, fake_get_job_id, fake_http,
                           fake_discovery, fake_file_storage):
        fake_file_storage.return_value = fakeStorage
        fake_get_job_id.return_value = 10
        fake_finished.return_value = True
        fake_query_result.return_value = [{'a': 'test'}]

        s = self._build_session()
        s.connect()

        fake_job = {'b': 'test'}

        s._wait_for_response(fake_job)
        fake_get_job_id.assert_called_with(fake_job)
        fake_finished.assert_called_with(10)
        fake_query_result.assert_called_with(10)

    @mock.patch('big_data_orm.big_query_connector.session.file.Storage')
    @mock.patch('big_data_orm.big_query_connector.session.discovery')
    @mock.patch('big_data_orm.big_query_connector.session.Http')
    def test_is_finished(self, fake_http, fake_discovery, fake_file_storage):
        fake_discovery.build.return_value = fakeService2
        fake_file_storage.return_value = fakeStorage

        s = self._build_session()
        s.connect()
        response = s._check_if_finished(10)
        self.assertEqual(True, response)

    @mock.patch('big_data_orm.big_query_connector.session.file.Storage')
    @mock.patch('big_data_orm.big_query_connector.session.discovery')
    @mock.patch('big_data_orm.big_query_connector.session.Http')
    def test_is_finished_nope(self, fake_http, fake_discovery, fake_file_storage):
        fake_discovery.build.return_value = fakeService3
        fake_file_storage.return_value = fakeStorage
        s = self._build_session()
        s.connect()
        response = s._check_if_finished(10)
        self.assertEqual(False, response)

    @mock.patch('big_data_orm.big_query_connector.session.file.Storage')
    @mock.patch('big_data_orm.big_query_connector.session.discovery')
    @mock.patch('big_data_orm.big_query_connector.session.Http')
    def test_get_job_id(self, fake_http, fake_discovery, fake_file_storage):
        fake_discovery.build.return_value = fakeService3
        fake_file_storage.return_value = fakeStorage

        s = self._build_session()
        fake_job = {
            'jobReference': {
                'jobId': 'TEST'
            }
        }
        response = s._get_job_id(fake_job)
        self.assertEquals('TEST', response)

    def test_query_data_extract_1(self):
        s = self._build_session()
        query = 'SELECT C1 FROM TABLE WHERE C1 != 100'
        response = s._extract_data_from_query(query)
        expected_response = {
            'table': 'TABLE',
            'fields': 'C1',
            'after_where': ' C1 != 100',
            'is_where': True
        }
        self.assertEqual(response, expected_response)

    def test_query_data_extract_2(self):
        s = self._build_session()
        query = 'SELECT C1 FROM TABLE'
        response = s._extract_data_from_query(query)
        expected_response = {
            'table': 'TABLE',
            'fields': 'C1',
            'after_where': '',
            'is_where': False
        }
        self.assertEqual(response, expected_response)

    def test_query_data_extract_3(self):
        s = self._build_session()
        query = 'SELECT_WOW C1 FROM TABLE'
        response = s._extract_data_from_query(query)
        expected_response = {}
        self.assertEqual(response, expected_response)

    def test_build_response_1(self):
        s = self._build_session()
        response = s._build_simple_dict(big_query_response)
        expected_response = [
            {
                'field_1': 'value_1',
            }
        ]
        self.assertEqual(expected_response, response)

    def test_build_response_2(self):
        s = self._build_session()
        wrong_response = {'tah': 'tum', 'tahtah': 'tum'}
        response = s._build_simple_dict(wrong_response)
        expected_response = wrong_response
        self.assertEqual(expected_response, response)
