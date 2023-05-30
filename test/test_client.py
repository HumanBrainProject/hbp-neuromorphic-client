"""
Tests of the client using the production Job Queue service

(change the entrypoint to test against development or staging services).

Network access and a valid EBRAINS account are needed.

"""

import sys
import os.path
import pytest

sys.path.append(".")
from nmpi import nmpi_user


ENTRYPOINT = "https://nmpi-v3-staging.hbpneuromorphic.eu/"
TEST_SYSTEM = "Test"
TEST_USER = os.environ["NMPI_TEST_USER"]
TEST_PWD = os.environ["NMPI_TEST_PWD"]
# TEST_USER_NONMEMBER = os.environ["NMPI_TEST_USER_NONMEMBER"]
# TEST_PWD_NONMEMBER = os.environ["NMPI_TEST_PWD_NONMEMBER"]
TEST_COLLAB = "neuromorphic-testing-private"
VERIFY = True

simple_test_script = r"""
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

with open(timestamp + ".txt", 'w') as fp:
  fp.write(timestamp + " 42\n")

print("done")
"""

simulation_test_script = r"""
import sys
exec("import pyNN.%s as sim" % sys.argv[1])

sim.setup()

p = sim.Population(2, sim.IF_cond_exp(i_offset=0.1))
p.record("v")

sim.run(100.0)

p.write_data("simulation_data.pkl")

sim.end()
"""


@pytest.fixture(scope="module")
def member_client():
    return nmpi_user.Client(TEST_USER, job_service=ENTRYPOINT, password=TEST_PWD, verify=VERIFY)


# @pytest.fixture(scope="module")
# def nonmember_client():
#     return nmpi_user.Client(
#         TEST_USER_NONMEMBER, job_service=ENTRYPOINT, password=TEST_PWD_NONMEMBER, verify=VERIFY
#     )


@pytest.fixture
def submitted_job_id(member_client):
    job_id = member_client.submit_job(
        source=simple_test_script, platform=TEST_SYSTEM, collab_id=TEST_COLLAB
    )
    yield job_id
    # admin_client.delete_job(job_id)  # todo


class TestQueueInteraction:
    def test_submit_job(self, submitted_job_id):
        assert submitted_job_id[:6] == "/jobs/"

    def test_submit_job_without_compute_time_allocation(self, member_client):
        with pytest.raises(
            Exception,
            match="You do not have sufficient compute quota to submit this job",
        ):
            member_client.submit_job(
                source=simple_test_script, platform="not_a_platform", collab_id=TEST_COLLAB
            )

    def test_submit_job_with_tags(self, member_client):
        # submit tags as a list
        job1_id = member_client.submit_job(
            source=simple_test_script,
            platform=TEST_SYSTEM,
            collab_id=TEST_COLLAB,
            tags=["tag1", "tag2"],
        )
        job1 = member_client.get_job(job1_id)
        assert set(job1["tags"]) == set(["tag1", "tag2"])
        # submit tags not as a list
        with pytest.raises(ValueError):
            member_client.submit_job(
                source=simple_test_script,
                platform=TEST_SYSTEM,
                collab_id=TEST_COLLAB,
                tags=1234,
            )

    def test_queued_jobs_verbose(self, submitted_job_id, member_client):
        jobs = member_client.queued_jobs(verbose=True)
        for job in jobs:
            # jobs should all belong to the current user
            assert job["user_id"] == member_client.user_info["id"]
            assert job["status"] in ("submitted", "running")
            assert job["collab"] == TEST_COLLAB
        job_uris = [job["resource_uri"] for job in jobs]
        assert submitted_job_id in job_uris

    def test_queued_jobs_terse(self, submitted_job_id, member_client):
        new_job = submitted_job_id
        job_uris = member_client.queued_jobs(verbose=False)
        for job_uri in job_uris:
            assert job_uri[:6] == "/jobs/"
        assert new_job in job_uris

    def test_submit_comment_to_completed_job(self, member_client):
        job_uri = member_client.completed_jobs(TEST_COLLAB)[0]
        comment = member_client.submit_comment(job_uri, "test comment")
        comment_uri = comment["resource_uri"]
        assert isinstance(comment_uri, str)
        job = member_client.get_job(job_uri)
        found = False
        for comment in job["comments"]:
            if comment["resource_uri"] == comment_uri:
                found = True
                assert comment["content"] == "test comment"
                break
        if not found:
            pytest.fail()

    def test_submit_comment_to_queued_job(self, member_client):
        job_uri = member_client.queued_jobs()[0]
        response = member_client.submit_comment(job_uri, "test comment")
        assert response == (
            "Comment not submitted: job id must belong to a completed job"
            " (with status finished or error)."
        )

    def test_job_status(self, submitted_job_id, member_client):
        job_uri = submitted_job_id
        response = member_client.job_status(job_uri)
        assert response == "submitted"
        job_id_int = int(job_uri.split("/")[-1])
        response = member_client.job_status(job_id_int)
        assert response == "submitted"

    def test_get_job(self, submitted_job_id, member_client):
        job_uri = submitted_job_id
        job = member_client.get_job(job_uri)
        assert job["resource_uri"] == job_uri
        assert job["code"] == simple_test_script
        assert job["collab"] == TEST_COLLAB
        assert job["hardware_platform"] == TEST_SYSTEM
        assert job["status"] == "submitted"

    # def test_remove_queued_job(self, submitted_job_id):
    #     job_uri = submitted_job_id
    #     response = member_client.remove_queued_job(job_uri)
    #     assert response is None
    #     queued_jobs = member_client.queued_jobs(verbose=False)
    #     assert job_uri not in queued_jobs

    # def test_remove_completed_job(self):
    #      response = member_client.remove_completed_job(43)
    #      assert response is None
    #     # todo: get list, and check job is no longer in it


#    def test_download_data(self):


# class TestQueueInteractionAsNonMember:
#     """Tests with a user who is not a member of the test collab."""

#     def _submit_job(self):
#         job_id = nonmember_client.submit_job(
#             source=simple_test_script, platform=TEST_SYSTEM, collab_id=TEST_COLLAB
#         )
#         return job_id

#     def test_submit_job(self):
#         # the user is not a member of the Collab, so job submission is not allowed
#         try:
#             job_id = nonmember_client.submit_job(
#                 source=simple_test_script, platform=TEST_SYSTEM, collab_id=TEST_COLLAB
#             )
#         except Exception as err:
#             self.assertEqual(err.args[0], "Error 403: You do not have permission to create a job")
#         else:
#             self.fail("403 error not raised")

#     def test_queued_jobs(self):
#         # the user is not a member of the Collab, so the job list should be empty
#         jobs = nonmember_client.queued_jobs(verbose=True)
#         self.assertEqual(len(jobs), 0)

#     def test_completed_jobs(self):
#         # the user is not a member of the Collab, so the job list should be empty
#         jobs = nonmember_client.completed_jobs(TEST_COLLAB)
#         self.assertEqual(len(jobs), 0)
