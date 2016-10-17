import unittest
import mock

from big_data_orm.big_query_connector.session import Session


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
    def test_run_query_2(self, fake_http, fake_discovery, fake_file_storage):
        fake_file_storage.return_value = fakeStorage
        s = self._build_session()
        s.connect()
        response = s.run_query('query', newest_only=True)
        self.assertEqual(response, {})

    @mock.patch('big_data_orm.big_query_connector.session.file.Storage')
    @mock.patch('big_data_orm.big_query_connector.session.discovery')
    @mock.patch('big_data_orm.big_query_connector.session.Http')
    @mock.patch('big_data_orm.big_query_connector.session.Session._extract_data_from_query')
    @mock.patch('big_data_orm.big_query_connector.session.Session._build_newest_only_query')
    @mock.patch('big_data_orm.big_query_connector.session.Session._build_request_body')
    @mock.patch('big_data_orm.big_query_connector.session.Session._wait_for_response')
    def test_run_query_3(self, fake_wait, fake_request_builder, fake_builder,
                         fake_extractor, fake_http, fake_discovery, fake_file_storage):
        fake_discovery.build.return_value = fakeService
        fake_file_storage.return_value = fakeStorage
        fake_extractor.return_value = 'query_data'
        fake_builder.return_value = 'query_built'
        fake_request_builder.return_value = {'a': 'test'}

        s = self._build_session()
        s.connect()

        response = s.run_query('query', newest_only=True, filter_key='id')
        fake_extractor.assert_called_with('query')
        fake_builder.assert_called_with('query_data', 'id')
        fake_request_builder.assert_called_with('query_built')

        fake_wait.assert_called_with(None)

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
