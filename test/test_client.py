"""
Tests of the client using the production Job Queue service

(change the entrypoint to test against development or staging services).

Network access and a valid HBP Identity account are needed.

"""

import os.path
import unittest

from nmpi import nmpi_user


ENTRYPOINT = "https://nmpi.hbpneuromorphic.eu/api/v2/"
TEST_SYSTEM = "nosetest_platform"
TEST_USER = os.environ['NMPI_TEST_USER']
TEST_PWD = os.environ['NMPI_TEST_PWD']
TEST_USER_NONMEMBER = os.environ['NMPI_TEST_USER_NONMEMBER']
TEST_PWD_NONMEMBER = os.environ['NMPI_TEST_PWD_NONMEMBER']
TEST_COLLAB = 563
VERIFY = True

simple_test_script = r"""
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

with open(timestamp + ".txt", 'w') as fp:
  fp.write(timestamp + " 42\n")

print "done"
"""

simulation_test_script = r"""
import sys
exec("import pyNN.%s as sim" % sys.argv[1])

sim.setup()

p = sim.Population(2, sim.IF_cond_exp, {'i_offset': 0.1})
p.record_v()

sim.run(100.0)

p.print_v("simulation_data.v")

sim.end()
"""


def setUp():
    global member_client, nonmember_client, system_client
    member_client = nmpi_user.Client(TEST_USER, job_service=ENTRYPOINT,
                                     password=TEST_PWD, verify=VERIFY)
    nonmember_client = nmpi_user.Client(TEST_USER_NONMEMBER, job_service=ENTRYPOINT,
                                        password=TEST_PWD_NONMEMBER, verify=VERIFY)


class QueueInteractionTest(unittest.TestCase):

    def _submit_job(self):
        job_id = member_client.submit_job(source=simple_test_script,
                                             platform=TEST_SYSTEM,
                                             collab_id=TEST_COLLAB)
        return job_id

    def test_submit_job(self):
        job_id = self._submit_job()
        self.assertEqual(job_id[:14], '/api/v2/queue/')

    def test_submit_job_without_compute_time_allocation(self):
        try:
            member_client.submit_job(source=simple_test_script,
                                        platform="not_a_platform",
                                        collab_id=TEST_COLLAB)
        except Exception as err:
            self.assertEqual(err.args[0],
                             "Error 403: You do not have a compute time allocation for the not_a_platform platform. Please submit a resource request.")
        else:
            self.fail("403 error not raised")

    def test_queued_jobs_verbose(self):
        job_id = self._submit_job()
        jobs = member_client.queued_jobs(verbose=True)
        for job in jobs:
            # jobs should all belong to the current user
            self.assertEqual(job["user_id"], member_client.user_info['id'])
            self.assertIn(job["status"], ("submitted", "running"))
            self.assertEqual(job["collab_id"], str(TEST_COLLAB))
        job_uris = [job["resource_uri"] for job in jobs]
        self.assertIn(job_id, job_uris)

    def test_queued_jobs_terse(self):
        new_job = self._submit_job()
        job_uris = member_client.queued_jobs(verbose=False)
        for job_uri in job_uris:
            self.assertEqual(job_uri[:14], '/api/v2/queue/')
        self.assertIn(new_job, job_uris)

    def test_job_status(self):
        job_uri = self._submit_job()
        response = member_client.job_status(job_uri)
        self.assertEqual(response, "submitted")
        job_id_int = int(job_uri.split("/")[-1])
        response = member_client.job_status(job_id_int)
        self.assertEqual(response, "submitted")

    def test_get_job(self):
        job_uri = self._submit_job()
        job = member_client.get_job(job_uri)
        self.assertEqual(job['resource_uri'], job_uri)
        self.assertEqual(job['code'], simple_test_script)
        self.assertEqual(job['collab_id'], str(TEST_COLLAB))
        self.assertEqual(job['hardware_platform'], TEST_SYSTEM)
        self.assertEqual(job['status'], "submitted")

    def test_remove_queued_job(self):
        job_uri = self._submit_job()
        response = member_client.remove_queued_job(job_uri)
        self.assertIsNone(response)
        queued_jobs = member_client.queued_jobs(verbose=False)
        self.assertNotIn(job_uri, queued_jobs)

    # def test_remove_completed_job(self):
    #      response = member_client.remove_completed_job(43)
    #      self.assertIsNone(response)
    #     # todo: get list, and check job is no longer in it

#    def test_download_data(self):



class QueueInteractionAsNonMemberTest(unittest.TestCase):
    """Tests with a user who is not a member of the test collab."""

    def _submit_job(self):
        job_id = nonmember_client.submit_job(source=simple_test_script,
                                             platform=TEST_SYSTEM,
                                             collab_id=TEST_COLLAB)
        return job_id

    def test_submit_job(self):
        # the user is not a member of the Collab, so job submission is not allowed
        try:
            job_id = nonmember_client.submit_job(source=simple_test_script,
                                                 platform=TEST_SYSTEM,
                                                 collab_id=TEST_COLLAB)
        except Exception as err:
            self.assertEqual(err.args[0],
                             "Error 403: You do not have permission to create a job")
        else:
            self.fail("403 error not raised")

    def test_queued_jobs(self):
        # the user is not a member of the Collab, so the job list should be empty
        jobs = nonmember_client.queued_jobs(verbose=True)
        self.assertEqual(len(jobs), 0)

    def test_completed_jobs(self):
        # the user is not a member of the Collab, so the job list should be empty
        jobs = nonmember_client.completed_jobs(TEST_COLLAB)
        self.assertEqual(len(jobs), 0)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(QueueInteractionTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
