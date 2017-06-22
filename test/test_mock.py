"""
Tests of the clients using a mock Job Queue service

No network access is needed.

"""

import unittest
from nmpi import nmpi_user

SERVER = "https://mock.hbpneuromorphic.eu"
ENTRYPOINT = SERVER + "/api/v2"
TESTUSERID = "999999"
TESTCOLLAB = "98765"
EMPTYCOLLAB = "54321"
NOTMYCOLLAB = "123456"
cache = {}
SCHEMA = {
    "dataitem": {"list_endpoint": "/api/v2/dataitem", "schema": "/api/v2/dataitem/schema"},
    "log": {"list_endpoint": "/api/v2/log", "schema": "/api/v2/log/schema"},
    "queue": {"list_endpoint": "/api/v2/queue", "schema": "/api/v2/queue/schema"},
    "results": {"list_endpoint": "/api/v2/results", "schema": "/api/v2/results/schema"},
}
JOB42 = {
    'code': 'this is the code',
    'collab_id': TESTCOLLAB,
    'command': 'run.py',
    'hardware_config': {},
    'hardware_platform': 'TESTPLATFORM',
    'input_data': [],
    'user_id': TESTUSERID,
    'resource_uri': "/api/v2/queue/42",
    'id': 42,
    'status': 'submitted'
}
DATAFILE1 = "foo.jpg"
DATAFILE2 = "results.h5"
JOB43 = {
    'code': 'this is the code',
    'collab_id': TESTCOLLAB,
    'command': 'run.py',
    'hardware_config': {},
    'hardware_platform': 'TESTPLATFORM',
    'input_data': [],
    'output_data': [{"url": "https://mockdatastore.humanbrainproject.eu/data/" + DATAFILE1},
                    {"url": "https://mockdatastore.humanbrainproject.eu/data/" + DATAFILE2}],
    'user_id': TESTUSERID,
    'resource_uri': "/api/v2/queue/43",
    'id': 43,
    'status': 'submitted'
}


class MockResponse(object):
    ok = True

    def __init__(self, return_value, headers=None, status_code=200):
        self.return_value = return_value
        self.headers = headers
        self.status_code = status_code
        if status_code >= 400:
            self.ok = False

    def json(self):
        return self.return_value


class MockRequestsModule(object):

    def get(self, url, auth=None, cert=None, verify=True):
        response_map = {
            nmpi_user.IDENTITY_SERVICE + "/user/me": MockResponse({"username": "testuser",
                                                      "id": TESTUSERID}),
            ENTRYPOINT: MockResponse(SCHEMA),
            ENTRYPOINT + "/queue?id=42": MockResponse({"objects": [JOB42]}),
            ENTRYPOINT + "/queue?id=43": MockResponse({"objects": []}),
            ENTRYPOINT + "/results?id=43": MockResponse({"objects": [JOB43]}),
            ENTRYPOINT + "/queue/42": MockResponse(JOB42),
            ENTRYPOINT + "/queue/43": MockResponse(JOB43),
            ENTRYPOINT + "/results/42": MockResponse({"error_message": "no such job"}, status_code=404),
            ENTRYPOINT + "/results/43": MockResponse({"error_message": "no such job"}, status_code=404),
            ENTRYPOINT + "/queue/submitted/?user_id=" + TESTUSERID: MockResponse([JOB42]),
            ENTRYPOINT + "/results?collab_id=" + TESTCOLLAB: MockResponse([JOB43]),
            ENTRYPOINT + "/results?collab_id=" + EMPTYCOLLAB: MockResponse([]),
            SERVER + "/copydata/collab/43": MockResponse([DATAFILE1, DATAFILE2]),
            ENTRYPOINT + "/queue/44?collab_id=" + NOTMYCOLLAB: MockResponse(
                                     {"error_message": "You are not a member of this Collab"}),
        }
        return response_map[url]

    def post(self, url, data, auth=None, cert=None, verify=True, headers=None):
        if url == SERVER + SCHEMA["queue"]["list_endpoint"]:
            return MockResponse({}, {'Location': 'NEW_JOB_URL'})
        else:
            raise Exception("invalid url: {}".format(url))

    def delete(self, url, auth=None, cert=None, verify=True):
        if url == ENTRYPOINT + "/queue/42":
            return MockResponse("", status_code=204)
        elif url == ENTRYPOINT + "/results/43":
            return MockResponse("", status_code=204)
        else:
            raise Exception("invalid url: {}".format(url))


def mock_urlretrieve(url, local_dir):
    pass


def setUp():
    """Replace modules/functions that access the network or
    filesystem with mock versions."""
    cache['requests'] = nmpi_user.requests
    cache['urlretrieve'] = nmpi_user.urlretrieve
    cache['_mkdir_p'] = nmpi_user._mkdir_p
    nmpi_user.__dict__['requests'] = MockRequestsModule()
    nmpi_user.urlretrieve = mock_urlretrieve
    nmpi_user._mkdir_p = lambda dir: None


def tearDown():
    """Restore the real versions of modules/functions that access
     the network or filesystem"""
    nmpi_user.__dict__['requests'] = cache['requests']
    nmpi_user.urlretrieve = cache['urlretrieve']
    nmpi_user._mkdir_p = cache['_mkdir_p']


class UserClientTest(unittest.TestCase):

    def setUp(self):
        self.client = nmpi_user.Client("testuser", job_service=ENTRYPOINT, token="TOKEN", verify=True)

    def test_create_client(self):
        self.assertEqual(self.client.username, "testuser")
        self.assertEqual(self.client.verify, True)
        self.assertEqual(self.client.token, "TOKEN")
        self.assertEqual(self.client.job_server, "https://mock.hbpneuromorphic.eu")
        self.assertEqual(self.client.auth.token, nmpi_user.HBPAuth("TOKEN").token)
        self.assertEqual(self.client.user_info["username"],
                         "testuser")
        self.assertEqual(self.client.user_info["id"],
                         TESTUSERID)

    def test_submit_job_string_no_inputs(self):
        response = self.client.submit_job("import foo", "TESTPLATFORM", "COLLAB_ID")
        self.assertEqual(response, 'NEW_JOB_URL')

    def test_job_status_integer(self):
        response = self.client.job_status(42)
        self.assertEqual(response, "submitted")

    def test_job_status_uri(self):
        response = self.client.job_status("/api/v2/queue/42")
        self.assertEqual(response, "submitted")

    def test_get_job(self):
         response = self.client.get_job(42)
         self.assertIsInstance(response, dict)
         self.assertEqual(response['id'], 42)

    def test_remove_completed_job(self):
         response = self.client.remove_completed_job(43)
         self.assertIsNone(response)

    def test_remove_queued_job(self):
         response = self.client.remove_queued_job(42)
         self.assertIsNone(response)

    def test_queued_jobs(self):
        response = self.client.queued_jobs()
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1)

    def test_completed_jobs(self):
        response = self.client.completed_jobs(TESTCOLLAB)
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1)

    def test_completed_jobs_empty_collab(self):
        response = self.client.completed_jobs(EMPTYCOLLAB)
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 0)

    def test_download_data(self):
        job = self.client.get_job(43)
        response = self.client.download_data(job, local_dir="testfoo")
        self.assertEqual(response, ["testfoo/job_43/" + DATAFILE1,
                                    "testfoo/job_43/" + DATAFILE2])

    def test_copy_data_to_storage(self):
        response = self.client.copy_data_to_storage(43, destination="collab")
        # todo: check the response

