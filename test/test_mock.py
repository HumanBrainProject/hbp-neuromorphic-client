"""
Tests of the clients using a mock Job Queue service

No network access is needed.

"""

import sys
import pytest

sys.path.append(".")
from nmpi import nmpi_user

SERVER = "https://mock.hbpneuromorphic.eu"
ENTRYPOINT = SERVER
TESTUSERID = "testuser"
TESTCOLLAB = "98765"
EMPTYCOLLAB = "54321"
NOTMYCOLLAB = "123456"
cache = {}
SCHEMA = {
    "about": "This is the EBRAINS Neuromorphic Computing Job Queue API.",
    "version": "3",
    "links": {"documentation": "/docs"},
}
JOB42 = {
    "code": "this is the code",
    "collab_id": TESTCOLLAB,
    "command": "run.py",
    "hardware_config": {},
    "hardware_platform": "TESTPLATFORM",
    "input_data": [],
    "user_id": TESTUSERID,
    "resource_uri": "/jobs/42",
    "id": 42,
    "status": "submitted",
}
DATAFILE1 = "foo.jpg"
DATAFILE2 = "results.h5"
JOB43 = {
    "code": "this is the code",
    "collab_id": TESTCOLLAB,
    "command": "run.py",
    "hardware_config": {},
    "hardware_platform": "TESTPLATFORM",
    "input_data": [],
    "output_data": {
        "files": [
            {
                "url": "https://mockdatastore.humanbrainproject.eu/data/job_43/" + DATAFILE1,
                "path": "job_43/" + DATAFILE1,
            },
            {
                "url": "https://mockdatastore.humanbrainproject.eu/data/job_43/" + DATAFILE2,
                "path": "job_43/" + DATAFILE2,
            },
        ]
    },
    "user_id": TESTUSERID,
    "resource_uri": "/jobs/43",
    "id": 43,
    "status": "finished",
}
JOB44 = {"id": 44, "resource_uri": "/jobs/44"}
USERINFO_ENDPOINT = "https://iam.ebrains.eu/auth/realms/hbp/protocol/openid-connect/userinfo"


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
            USERINFO_ENDPOINT: MockResponse({"preferred_username": TESTUSERID, "sub": None}),
            ENTRYPOINT: MockResponse(SCHEMA),
            ENTRYPOINT + "/jobs/42?with_log=false&with_comments=true": MockResponse(JOB42),
            ENTRYPOINT + "/jobs/42?with_log=true&with_comments=true": MockResponse(JOB42),
            ENTRYPOINT + "/jobs/43?with_log=false&with_comments=true": MockResponse(JOB43),
            ENTRYPOINT + "/jobs/43?with_log=true&with_comments=true": MockResponse(JOB43),
            # ENTRYPOINT + "/jobs/?status=submitted&user_id=" + TESTUSERID: MockResponse([JOB42]),
            ENTRYPOINT
            + "/jobs/?status=submitted&status=running&user_id="
            + TESTUSERID: MockResponse([JOB42]),
            ENTRYPOINT
            + "/jobs/?status=finished&status=error&collab=98765&size=100000": MockResponse(
                [JOB43]
            ),
            ENTRYPOINT
            + "/jobs/?status=finished&status=error&collab=54321&size=100000": MockResponse([]),
            ENTRYPOINT + "/jobs/?collab_id=" + TESTCOLLAB: MockResponse([JOB42, JOB43]),
            ENTRYPOINT + "/jobs/?collab_id=" + EMPTYCOLLAB: MockResponse([]),
            ENTRYPOINT + "/jobs/43/output_data": MockResponse([DATAFILE1, DATAFILE2]),
            ENTRYPOINT
            + "/jobs/44?collab_id="
            + NOTMYCOLLAB: MockResponse({"error_message": "You are not a member of this Collab"}),
        }
        return response_map[url]

    def put(self, url, data, auth=None, cert=None, verify=True, headers=None):
        response_map = {ENTRYPOINT + "/jobs/43/output_data": MockResponse(None)}
        return response_map[url]

    def post(self, url, data, auth=None, cert=None, verify=True, headers=None):
        if url == ENTRYPOINT + "/jobs/":
            return MockResponse(JOB44)
        else:
            raise Exception("invalid url: {}".format(url))

    def delete(self, url, auth=None, cert=None, verify=True):
        if url == ENTRYPOINT + "/jobs/42":
            return MockResponse("", status_code=204)
        elif url == ENTRYPOINT + "/jobs/43":
            return MockResponse("", status_code=204)
        else:
            raise Exception("invalid url: {}".format(url))


def mock_urlretrieve(url, local_dir):
    pass


@pytest.fixture
def client(mocker):
    mocker.patch("nmpi.nmpi_user.requests", MockRequestsModule())
    mocker.patch("nmpi.nmpi_user.urlretrieve")
    mocker.patch("nmpi.nmpi_user.os.makedirs")
    return nmpi_user.Client(TESTUSERID, job_service=ENTRYPOINT, token="TOKEN", verify=True)


class TestUserClient:
    def test_create_client(self, client):
        assert client.username == TESTUSERID
        assert client.verify is True
        assert client.token == "TOKEN"
        assert client.job_server == "https://mock.hbpneuromorphic.eu"
        assert client.auth.token == nmpi_user.EBRAINSAuth("TOKEN").token
        assert client.user_info["username"] == TESTUSERID

    def test_submit_job_string_no_inputs(self, client):
        response = client.submit_job("import foo", "TESTPLATFORM", "COLLAB_ID")
        assert response == "/jobs/44"

    def test_job_status_integer(self, client):
        response = client.job_status(42)
        assert response == "submitted"

    def test_job_status_uri(self, client):
        response = client.job_status("/jobs/42")
        assert response == "submitted"

    def test_get_job(self, client):
        response = client.get_job(42)
        assert isinstance(response, dict)
        assert response["id"] == 42

    def test_queued_jobs(self, client):
        response = client.queued_jobs()
        assert isinstance(response, list)
        assert len(response) == 1

    def test_completed_jobs(self, client):
        response = client.completed_jobs(TESTCOLLAB)
        assert isinstance(response, list)
        assert len(response) == 1

    def test_completed_jobs_empty_collab(self, client):
        response = client.completed_jobs(EMPTYCOLLAB)
        assert isinstance(response, list)
        assert len(response) == 0

    def test_download_data(self, client):
        job = client.get_job(43)
        response = client.download_data(job, local_dir="testfoo")
        assert response == ["testfoo/job_43/" + DATAFILE1, "testfoo/job_43/" + DATAFILE2]

    def test_copy_data_to_storage(self, client):
        response = client.copy_data_to_storage(43, destination="drive")
        # todo: check the response
