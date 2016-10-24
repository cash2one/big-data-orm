import re
import time
import logging

from apiclient import discovery
from oauth2client import file
from httplib2 import Http

CHECK_JOB_INTERVAL = 10
API_VERSION = 'v2'


class Session(object):
    def __init__(self, project_id, dataset_id, storage_file):
        self.api_name = 'bigquery'
        self.api_version = API_VERSION
        self.api_scope = [
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/cloud-platform'
        ]
        self.connected = False
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.storage_file = storage_file
        self.service = None

    def connect(self):
        """
        Connect with BigQuery API and create a service.
        """
        storage = file.Storage(self.storage_file)
        credential = storage.get()
        base_authentication = credential.authorize(http=Http())
        self.service = discovery.build(self.api_name, self.api_version, http=base_authentication)
        self.connected = True

    def run_query(self, query, newest_only=False, filter_key=""):
        """
        Send a job request to BigQueryAPI with a SQL query.
        Args:
            query: (str) SQL Query
            newest_only: (bool) Get only the newest adwords objects.
            filter_key: (str) field used to group by the equal tuples.
        Return:
            (dict) Dict with all the tuples returned by the query.
        """
        if not self.connected:
            logging.error("Session is not connected...")
            return {}

        if newest_only:
            if not filter_key:
                print "Error: missing filter_key"
                return {}
            data_from_query = self._extract_data_from_query(query)
            query = self._build_newest_only_query(data_from_query, filter_key)

        request_body = self._build_request_body(query)
        request = self.service.jobs().query(projectId=self.project_id, body=request_body)
        response = request.execute()
        return self._wait_for_response(response)

    def _wait_for_response(self, job_json):
        """
        Wait until the job sent to BigQuery is finished.
        Args:
            project_id: (str) Project ID.
            job_json: (str) BigQuery job structure.
            storage_file: (str) path to *.dat file.
        """
        job_id = self._get_job_id(job_json)
        while not self._check_if_finished(job_id):
            time.sleep(CHECK_JOB_INTERVAL)
        return self._get_query_result(job_id)

    def _check_if_finished(self, job_id):
        """
        Get jib info from bigquery and check if is already finished.
        Args:
            job_id: (str) Job ID.
        Return:
            (bool) True if is finished. False otherwise.
        """
        self.connect()
        request = self.service.jobs().get(projectId=self.project_id, jobId=job_id)
        response = request.execute()
        if response['status']['state'] == 'DONE':
            return True
        else:
            return False

    def _get_job_id(self, job_json):
        """
        Find and return the job ID.
        """
        return job_json['jobReference']['jobId']

    def _get_query_result(self, job_id):
        """
        Since the job has finished, collect the query result.
        Args:
            project_id: (str) Project ID.
            job_id: (str) BigQuery job ID.
            storage_file: (str) path to *.dat file.
        """
        self.connect()
        request = self.service.jobs().getQueryResults(projectId=self.project_id, jobId=job_id)
        response = request.execute()
        return self._build_simple_dict(response)

    def _build_simple_dict(self, response):
        """
        Build a simple list of dicts from bigquery response.
        Args:
            response: (dict) BigQuery response.
        Return:
            (list) List of dicts. Each dict is a table tuple.
        """
        try:
            fields = response['schema']['fields']
            rows = response['rows']
            new_response = []
            for row in rows:
                new_row = {}
                for i in range(len(fields)):
                    new_row[fields[i]['name']] = row['f'][i]['v']
                new_response.append(new_row)
            return new_response
        except KeyError:
            logging.error("Invalid BigQuery response. Returning full response as it is.")
            return response

    def _extract_data_from_query(self, query):
        """
        Use regex operation to extract information from SQL query.
        Args:
            query: (str) SQL query.
        Return:
            (dict) Data from query.
        """
        regex = (r"(SELECT|select)\s(.*)\s(from|FROM)\s([a-z|\[|\]|_|\.]+)\s?(where|WHERE)?(.*)$")

        data = {}
        match = re.match(regex, query)
        data['fields'] = match.group(2)
        data['table'] = match.group(4)
        data['after_where'] = match.group(6)
        if match.group(5):
            data['is_where'] = True
        else:
            data['is_where'] = False

        return data

    def _build_newest_only_query(self, data, filter_key):
        """
        Insert in the middle of the query another select operation to retrieve
        only the newest tuples.
        Args:
            data: (dict) data extract from the query.
            filter_key: (str) tuple field used as a group by key.
        Return:
            (str) query modified.
        """
        base_query = "SELECT {} FROM ({}) {}"

        middle_query = "SELECT *, MAX(created_time) OVER (PARTITION BY " +\
            "{}) AS newest_time FROM {}".format(filter_key, data['table'])

        if data['is_where']:
            where_clause = "WHERE created_time = newest_time and {}".format(data['after_where'])
        else:
            where_clause = 'WHERE created_time = newest_time {}'.format(data['after_where'])

        base_query = base_query.format(data['fields'], middle_query, where_clause)
        return base_query

    def _build_request_body(self, query):
        """
        Build the dict that will be sent as payload to bigquery.
        """
        payload = {
            'query': query,
            'defaultDataset': {
                'datasetId': self.dataset_id,
                'projectId': self.project_id
            }
        }
        return payload
