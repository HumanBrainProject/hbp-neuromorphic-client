"""


"""

import os.path
import unittest
import tempfile
import shutil
from datetime import datetime

import saga

from nmpi import nmpi_saga, nmpi_user


#ENTRYPOINT = "http://157.136.240.232/api/v1/"
#ENTRYPOINT = "http://nmpi-queue-server-apdavison.beta.tutum.io:49180/api/v1/"
ENTRYPOINT = "http://192.168.59.103:49161/api/v1/"

test_script = r"""
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

with open(timestamp + ".txt", 'w') as fp:
  fp.write(timestamp + " 42\n")

print "done"
"""


class SlurmTest(unittest.TestCase):

    def setUp(self):
        self.service = saga.job.Service("slurm://localhost")

    def tearDown(self):
        self.service.close()

    def test__run_job(self):
        # in creating the temporary directory, we assume that /home is shared across cluster nodes, but /tmp probably isn't
        tmpdir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        with open(os.path.join(tmpdir, "run.py"), "w") as fp:
            fp.write(test_script)
        job_desc = saga.job.Description()
        job_desc.working_directory = tmpdir
        # job_desc.spmd_variation    = "MPI" # to be commented out if not using MPI
        job_desc.executable = "/usr/bin/python"
        job_desc.queue = "intel"  # take from config
        job_desc.arguments = [os.path.join(tmpdir, "run.py")]
        job_desc.output = "saga_test.out"
        job_desc.error = "saga_test.err"

        job = nmpi_saga.run_job(job_desc, self.service)
        job.wait()

        self.assertEqual(job.get_state(), saga.job.DONE)

        shutil.rmtree(tmpdir)


class MockSagaJob(object):

    def __init__(self, _state):
        self.id = 42
        self._state = _state

    def get_state(self):
        return self._state


class QueueServerInteractionTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.user_client = nmpi_user.Client("testuser", "abc123",
                                            entrypoint=ENTRYPOINT)
        self.project_name = datetime.now().strftime("test_%Y%m%d_%H%M%S")
        self.user_client.create_project(self.project_name)

        self.hardware_client = nmpi_user.HardwareClient(username="nmpi",
                                                        password="Poh3Eip'",
                                                        entrypoint=ENTRYPOINT,
                                                        platform="nosetest")

    def _submit_test_job(self):
        self.user_client.submit_job(
            source=test_script,
            platform="nosetest",
            project=self.project_name)

    def test__get_next_job(self):
        self._submit_test_job()
        nmpi_job = nmpi_saga.get_next_job(self.hardware_client)
        self.assertEqual(nmpi_job["status"], "submitted")
        self.assertEqual(nmpi_job["user"], "/api/v1/user/testuser")
        self.assertEqual(nmpi_job["hardware_platform"], "nosetest")

    def test__update_status(self):
        self._submit_test_job()
        nmpi_job = nmpi_saga.get_next_job(self.hardware_client)
        nmpi_job = nmpi_saga.update_status(self.hardware_client, MockSagaJob(saga.job.RUNNING), nmpi_job)
        self.assertEqual(nmpi_job["status"], "running")
        self.assertEqual(self.user_client.get_job(nmpi_job['id']), nmpi_job)

    def test__update_final_service(self):
        self._submit_test_job()
        nmpi_job = nmpi_saga.get_next_job(self.hardware_client)
        nmpi_job = nmpi_saga.update_status(self.hardware_client, MockSagaJob(saga.job.DONE), nmpi_job)
        self.assertEqual(nmpi_job["status"], "finished")
        self.assertEqual(self.user_client.get_job(nmpi_job['id']), nmpi_job)
