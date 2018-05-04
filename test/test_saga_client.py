"""


"""

import os.path
import unittest
import tempfile
import shutil
from zipfile import ZipFile
import tarfile
from datetime import datetime

import saga

from nmpi import nmpi_saga, nmpi_user


NMPI_HOST = "https://nmpi-staging.hbpneuromorphic.eu"
#NMPI_HOST = "https://127.0.0.1:8000"
NMPI_API = "/api/v2"
ENTRYPOINT = NMPI_HOST + NMPI_API
VERIFY = True

TEST_TOKEN = "boIeArQtaH1Vwibq4AnaZE91diEQASN9ZV1BO-f2tFi7dJkwowIJP6Vhcf4b6uj0HtiyshEheugRek2EDFHiNZHlZtDAVNUTypnN0CnA5yPIPqv6CaMsjuByumMdIenw"
HARDWARE_TOKEN = "D7oyE7C8-TlwT88Xt9TyiCWwivUkes7lukaomwrfTq01RravZXeDHQhRSwSIvHACHZoJhbrxTqFr5ADe853SDvlVK9JGz8oQMqAaNUE7WH39J16sD5hFs91a0s2SGzuO"
TEST_COLLAB = 563

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
            self.job_runner = nmpi_saga.JobRunner(dict(
                JOB_SERVICE_ADAPTOR="slurm://localhost",
                AUTH_USER="nmpi",
                AUTH_TOKEN=HARDWARE_TOKEN,
                NMPI_HOST=NMPI_HOST,
                NMPI_API=NMPI_API,
                PLATFORM_NAME="nosetest_platform",
                VERIFY_SSL=VERIFY
            ))
        except saga.NoSuccess:
            raise unittest.SkipTest("SLURM not available")

    def tearDown(self):
        self.job_runner.close()

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

        job = self.job_runner.service.create_job(job_desc)
        job.run()
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

        job = self.job_runner.service.create_job(job_desc)
        job.run()
        job.wait()

        self.assertEqual(job.get_state(), saga.job.DONE)

        shutil.rmtree(tmpdir)


class MockSagaJob(object):

    def __init__(self, _state, working_directory=""):
        self.id = 42
        self._state = _state
        self.working_directory = working_directory
        self.arguments = [os.path.join(working_directory, "run.py")]

    def get_state(self):
        return self._state



class QueueServerInteractionTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.user_client = nmpi_user.Client("testuser", token=TEST_TOKEN,
                                            job_service=ENTRYPOINT,
                                            verify=VERIFY)
        self.collab_id = TEST_COLLAB
        self.job_runner = nmpi_saga.JobRunner(dict(
            JOB_SERVICE_ADAPTOR="fork://localhost",
            AUTH_USER="nmpi",
            AUTH_TOKEN=HARDWARE_TOKEN,
            NMPI_HOST=NMPI_HOST,
            NMPI_API=NMPI_API,
            PLATFORM_NAME="nosetest_platform",
            VERIFY_SSL=VERIFY
        ))

    def _submit_test_job(self):
        self.last_job = self.user_client.submit_job(
            source=simple_test_script,
            platform="nosetest_platform",
            collab_id=self.collab_id)

    def test__get_next_job(self):
        self._submit_test_job()
        nmpi_job = self.job_runner.client.get_job(self.last_job)
        self.assertEqual(nmpi_job["status"], "submitted")
        self.assertEqual(nmpi_job["user_id"], "testuser")
        self.assertEqual(nmpi_job["hardware_platform"], "nosetest_platform")

    def test__update_status(self):
        self._submit_test_job()
        nmpi_job = self.job_runner.client.get_job(self.last_job)
        nmpi_job = self.job_runner._update_status(nmpi_job, MockSagaJob(saga.job.RUNNING),
                                                  nmpi_saga.default_job_states)
        self.assertEqual(nmpi_job["status"], "running")
        self.assertEqual(self.user_client.get_job(nmpi_job['id'], with_log=False), nmpi_job)

    def test__remove_queued_job(self):
        # create a job
        self._submit_test_job()
        test_job = self.user_client.get_job(self.last_job)
        # check the job is on the queue
        jobs = self.user_client.queued_jobs()
        self.assertIn(test_job['resource_uri'], jobs)
        # remove the job
        self.user_client.remove_queued_job(self.last_job)
        # check the job is no longer on the queue
        jobs = self.user_client.queued_jobs()
        self.assertNotIn(test_job['resource_uri'], jobs)

    def test__remove_completed_job(self):
        self._submit_test_job()
        test_job = self.user_client.get_job(self.last_job)
        test_job['status'] = 'finished'
        test_job['resource_usage'] = 1.23
        self.job_runner.client.update_job(test_job)
        test_job['resource_uri'] = test_job['resource_uri'].replace('queue', 'results')
        jobs = self.user_client.completed_jobs()
        self.assertIn(test_job['resource_uri'], jobs)
        self.user_client.remove_completed_job(self.last_job)
        jobs = self.user_client.completed_jobs()
        self.assertNotIn(test_job['resource_uri'], jobs)


class FullStackTest(unittest.TestCase):

    def setUp(self):
        try:
            self.job_runner = nmpi_saga.JobRunner(dict(
                JOB_SERVICE_ADAPTOR="slurm://localhost",
                AUTH_USER="nmpi",
                AUTH_TOKEN=HARDWARE_TOKEN,
                NMPI_HOST=NMPI_HOST,
                NMPI_API=NMPI_API,
                PLATFORM_NAME="nosetest_platform",
                VERIFY_SSL=VERIFY
            ))
        except saga.NoSuccess:
            raise unittest.SkipTest("SLURM not available")
        self.user_client = nmpi_user.Client("testuser", token=TEST_TOKEN,
                                            job_service=ENTRYPOINT)
        self.collab_id = TEST_COLLAB

    def tearDown(self):
        self.job_runner.close()

    def _submit_test_job(self):
        self.user_client.submit_job(
            source=simple_test_script,
            platform="nosetest_platform",
            collab_id=self.collab_id)

    def test_all__no_input_data(self):
        tmpdir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        self._submit_test_job()

        try:
            job_runner = nmpi_saga.JobRunner(dict(
                JOB_SERVICE_ADAPTOR="slurm://localhost",
                AUTH_USER="nmpi",
                AUTH_TOKEN=HARDWARE_TOKEN,
                NMPI_HOST=NMPI_HOST,
                NMPI_API=NMPI_API,
                PLATFORM_NAME="nosetest_platform",
                VERIFY_SSL=VERIFY,
                WORKING_DIRECTORY=tmpdir,
                JOB_EXECUTABLE='/usr/bin/python',
                JOB_QUEUE='intel',
                DATA_DIRECTORY=tmpdir,
                DEFAULT_PYNN_BACKEND='nest'
            ))
        except saga.NoSuccess:
            raise unittest.SkipTest("SLURM not available")

        saga_job = job_runner.next()
        self.assertEqual(saga_job.get_state(), saga.job.DONE)
        shutil.rmtree(tmpdir)


class CodeRetrievalTest(unittest.TestCase):

    def setUp(self):
        self.tmp_src_dir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        self.tmp_run_dir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))

    def tearDown(self):
        shutil.rmtree(self.tmp_src_dir)
        shutil.rmtree(self.tmp_run_dir)

    def test_get_code_zip(self):
        zipfile = os.path.join(self.tmp_src_dir, "testcode.zip")
        zf = ZipFile(zipfile, "w")
        zf.writestr("run.py", simulation_test_script)
        zf.close()

        job_runner = nmpi_saga.JobRunner(dict(
            JOB_SERVICE_ADAPTOR="fork://localhost",
            AUTH_USER="nmpi",
            AUTH_TOKEN=HARDWARE_TOKEN,
            NMPI_HOST=NMPI_HOST,
            NMPI_API=NMPI_API,
            PLATFORM_NAME="nosetest_platform",
            VERIFY_SSL=VERIFY
        ))
        job = MockSagaJob("submitted", working_directory=self.tmp_run_dir)
        mock_nmpi_job = {
            "code": "file://{}".format(zipfile)
        }
        job_runner._get_code(mock_nmpi_job, job)
        self.assertEqual(os.listdir(self.tmp_run_dir),
                         ["run.py", "testcode.zip"])
        with open(os.path.join(self.tmp_run_dir, "run.py")) as fp:
            self.assertEqual(fp.read(), simulation_test_script)

    def test_get_code_tar(self):
        archive = os.path.join(self.tmp_src_dir, "testcode.tar.gz")
        with tarfile.open(archive, "w:gz") as tf:
            with open("run.py", "w") as fp:
                fp.write(simulation_test_script)
            tf.add("run.py")
            os.remove("run.py")
        job_runner = nmpi_saga.JobRunner(dict(
            JOB_SERVICE_ADAPTOR="fork://localhost",
            AUTH_USER="nmpi",
            AUTH_TOKEN=HARDWARE_TOKEN,
            NMPI_HOST=NMPI_HOST,
            NMPI_API=NMPI_API,
            PLATFORM_NAME="nosetest_platform",
            VERIFY_SSL=VERIFY
        ))
        job = MockSagaJob("submitted", working_directory=self.tmp_run_dir)
        mock_nmpi_job = {
            "code": "file://{}".format(archive)
        }
        job_runner._get_code(mock_nmpi_job, job)
        self.assertEqual(os.listdir(self.tmp_run_dir),
                         ["run.py", "testcode.tar.gz"])
        with open(os.path.join(self.tmp_run_dir, "run.py")) as fp:
            self.assertEqual(fp.read(), simulation_test_script)