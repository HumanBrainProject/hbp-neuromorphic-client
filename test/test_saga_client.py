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
ENTRYPOINT = "http://nmpi-queue-server-apdavison.beta.tutum.io:49180/api/v1/"

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


class QueueServerInteractionTest(unittest.TestCase):

    def setUp(self):
        self.user_client = nmpi_user.Client("nmpi", "Poh3Eip'",
                                            entrypoint=ENTRYPOINT)
        self.project_name = datetime.now().strftime("test_%Y%m%d_%H%M%S")
        self.user_client.create_project(self.project_name)

    def test__get_next_job(self):
        self.user_client.submit_job(
            source=test_script,
            platform="nosetest",
            project=self.project_name)

        job = nmpi_saga.get_next_job()
        self.assertEqual(job, {})
