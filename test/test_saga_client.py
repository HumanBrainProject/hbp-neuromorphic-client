"""


"""

import os.path
import unittest
import tempfile
import shutil
from datetime import datetime

import saga

from nmpi import nmpi_saga, nmpi_user


ENTRYPOINT = "https://www.hbpneuromorphic.eu/api/v1/"
#ENTRYPOINT = "http://127.0.0.1:8999/api/v1/"

TEST_TOKEN = "boIeArQtaH1Vwibq4AnaZE91diEQASN9ZV1BO-f2tFi7dJkwowIJP6Vhcf4b6uj0HtiyshEheugRek2EDFHiNZHlZtDAVNUTypnN0CnA5yPIPqv6CaMsjuByumMdIenw"
HARDWARE_TOKEN = "D7oyE7C8-TlwT88Xt9TyiCWwivUkes7lukaomwrfTq01RravZXeDHQhRSwSIvHACHZoJhbrxTqFr5ADe853SDvlVK9JGz8oQMqAaNUE7WH39J16sD5hFs91a0s2SGzuO"

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


class SlurmTest(unittest.TestCase):

    def setUp(self):
        try:
            self.service = saga.job.Service("slurm://localhost")
        except saga.NoSuccess:
            raise unittest.SkipTest("SLURM not available")

    def tearDown(self):
        self.service.close()

    def test__run_job(self):
        # in creating the temporary directory, we assume that /home is shared
        # across cluster nodes, but /tmp probably isn't
        tmpdir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        with open(os.path.join(tmpdir, "run.py"), "w") as fp:
            fp.write(simple_test_script)
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

    def test__run_PyNN_job(self):
        # in creating the temporary directory, we assume that /home is shared
        # across cluster nodes, but /tmp probably isn't
        tmpdir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        with open(os.path.join(tmpdir, "run.py"), "w") as fp:
            fp.write(simulation_test_script)
        job_desc = saga.job.Description()
        job_desc.working_directory = tmpdir
        # job_desc.spmd_variation    = "MPI" # to be commented out if not using MPI
        job_desc.executable = os.path.join(os.path.expanduser("~/"), "env", "nmpi_saga", "bin", "python")
        job_desc.queue = "intel"  # take from config
        job_desc.arguments = [os.path.join(tmpdir, "run.py"), "nest"]
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
        self.user_client = nmpi_user.Client("testuser", token=TEST_TOKEN,
                                            entrypoint=ENTRYPOINT)
        self.project_name = datetime.now().strftime("test_%Y%m%d_%H%M%S")
        self.user_client.create_project(self.project_name, members=['testuser', 'nmpi'])
        self.hardware_client = nmpi_user.HardwareClient(username="nmpi",
                                                        entrypoint=ENTRYPOINT,
                                                        platform="nosetest",
                                                        token=HARDWARE_TOKEN)

    def _submit_test_job(self):
        self.user_client.submit_job(
            source=simple_test_script,
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


class FullStackTest(unittest.TestCase):

    def setUp(self):
        try:
            self.service = saga.job.Service("slurm://localhost")
        except saga.NoSuccess:
            raise unittest.SkipTest("SLURM not available")
        self.user_client = nmpi_user.Client("testuser", token=TEST_TOKEN,
                                            entrypoint=ENTRYPOINT)
        self.project_name = datetime.now().strftime("test_%Y%m%d_%H%M%S")
        self.user_client.create_project(self.project_name, members=['testuser', 'nmpi'])
        self.hardware_client = nmpi_user.HardwareClient(username="nmpi",
                                                        entrypoint=ENTRYPOINT,
                                                        platform="nosetest",
                                                        token=HARDWARE_TOKEN)

    def tearDown(self):
        self.service.close()

    def _submit_test_job(self):
        self.user_client.submit_job(
            source=simple_test_script,
            platform="nosetest",
            project=self.project_name)

    def test_all__no_input_data(self):
        tmpdir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        self._submit_test_job()
        nmpi_job = nmpi_saga.get_next_job(self.hardware_client)
        config = {
            'WORKING_DIRECTORY': tmpdir,
            'JOB_EXECUTABLE': '/usr/bin/python',
            'JOB_QUEUE': 'intel',
            'DATA_DIRECTORY': tmpdir,
            'DEFAULT_PYNN_BACKEND': 'nest'
        }
        job_desc = nmpi_saga.build_job_description(nmpi_job, config)
        nmpi_saga.get_code(nmpi_job, job_desc)
        saga_job = nmpi_saga.run_job(job_desc, self.service)
        nmpi_job = nmpi_saga.update_status(self.hardware_client, saga_job, nmpi_job)
        saga_job.wait()
        self.assertEqual(saga_job.get_state(), saga.job.DONE)
        nmpi_saga.handle_output_data(self.hardware_client, config, job_desc, nmpi_job)
        nmpi_saga.update_final_service(self.hardware_client, saga_job, nmpi_job, job_desc)
        shutil.rmtree(tmpdir)
