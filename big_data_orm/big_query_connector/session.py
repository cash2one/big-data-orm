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

    def run_query(self, query):
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

        request_body = self._build_request_body(query)
        request = self.service.jobs().query(projectId=self.project_id, body=request_body)
        response = request.execute()
        return self._wait_for_response(response)

    def _wait_for_response(self, job_json):
        """
        Wait until the job sent to BigQuery is finished.
        Args:
            job_json: (str) BigQuery job structure.
        """
        job_id = self._get_job_id(job_json)
        while not self._check_if_finished(job_id):
            time.sleep(CHECK_JOB_INTERVAL)
        return self._get_query_result(job_id)

    def _check_if_finished(self, job_id):
        """
        Get job info from bigquery and check if is already finished.
        Args:
            job_id: (str) Job ID.
        Return:
            (bool) True if is finished. False otherwise.
        """
        self.connect()
        request = self.service.jobs().get(projectId=self.project_id, jobId=job_id)
        response = request.execute()
        return True if response['status']['state'] == 'DONE' else False

    def _get_job_id(self, job_json):
        """
        Find and return the job ID.
        """
        return job_json['jobReference']['jobId']

    def _get_query_result(self, job_id):
        """
        Since the job has finished, collect the query result.
        Args:
            job_id: (str) BigQuery job ID.
        """
        self.connect()
        request = self.service.jobs().getQueryResults(projectId=self.project_id, jobId=job_id)
        response = request.execute()
        formated_response = self._build_simple_dict(response)
        while response.get('pageToken'):
            logging.debug("Collecting next page. Token: {}".format(response.get('pageToken')))
            request = self.service.jobs().getQueryResults(projectId=self.project_id, jobId=job_id,
                                                          pageToken=response.get('pageToken'))
            response = request.execute()
            page_data = self._build_simple_dict(response)
            formated_response.extend(page_data)
        return formated_response

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
            logging.debug("Invalid BigQuery response. Returning full response as it is.")
            return response

    def _extract_data_from_query(self, query):
        """
        Use regex operation to extract information from SQL query.
        Args:
            query: (str) SQL query.
        Return:
            (dict) Data from query.
        """
        regex = (r"(SELECT|select)\s(.*)\s(from|FROM)" +
                 "\s([a-z|A-Z|\[|\]|_|\.]+)\s?(where|WHERE)?(.*)$")

        data = {}
        match = re.match(regex, query)
        if not match:
            logging.error("Invalid query: {}".format(query))
            return {}
        data['fields'] = match.group(2)
        data['table'] = match.group(4)
        data['after_where'] = match.group(6)
        data['is_where'] = True if match.group(5) else False

        return data
      
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
