"""


"""

import sys
import os.path
import unittest
import tempfile
import shutil
from zipfile import ZipFile
import tarfile
from datetime import datetime

import radical.saga as saga

sys.path.append(".")
from nmpi import nmpi_saga, nmpi_user


ENTRYPOINT = "https://nmpi-v3-staging.hbpneuromorphic.eu/"
TEST_SYSTEM = "Test"
TEST_USER = os.environ["NMPI_TEST_USER"]
TEST_PWD = os.environ["NMPI_TEST_PWD"]
HARDWARE_TOKEN = os.environ["NMPI_TESTING_APIKEY"]
TEST_COLLAB = "neuromorphic-testing-private"
TEST_PLATFORM = "Test"
VIRTUAL_ENV = "/Users/adavison/dev/simulation/env"


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


class JobRunnerTest(unittest.TestCase):
    def setUp(self):
        self.job_runner = nmpi_saga.JobRunner(
            dict(
                JOB_SERVICE_ADAPTOR="fork://localhost",
                AUTH_USER="nmpi",
                AUTH_TOKEN=HARDWARE_TOKEN,
                NMPI_HOST=ENTRYPOINT,
                PLATFORM_NAME=TEST_PLATFORM,
                VERIFY_SSL=True,
            )
        )

    def tearDown(self):
        self.job_runner.close()

    def test__run_job(self):
        tmpdir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        with open(os.path.join(tmpdir, "run.py"), "w") as fp:
            fp.write(simple_test_script)
        job_desc = saga.job.Description()
        job_desc.working_directory = tmpdir
        # job_desc.spmd_variation    = "MPI" # to be commented out if not using MPI
        job_desc.executable = "/usr/bin/env"
        job_desc.arguments = ["python3", os.path.join(tmpdir, "run.py")]
        job_desc.output = "saga_test.out"
        job_desc.error = "saga_test.err"

        job = self.job_runner.service.create_job(job_desc)
        job.run()
        job.wait()

        self.assertEqual(job.get_state(), saga.job.DONE)

        shutil.rmtree(tmpdir)

    def test__run_PyNN_job(self):
        tmpdir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        with open(os.path.join(tmpdir, "run.py"), "w") as fp:
            fp.write(simulation_test_script)
        job_desc = saga.job.Description()
        job_desc.working_directory = tmpdir
        job_desc.executable = os.path.join(VIRTUAL_ENV, "bin", "python")
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
        self.user_client = nmpi_user.Client(
            username=TEST_USER, password=TEST_PWD, job_service=ENTRYPOINT, verify=True
        )
        self.collab_id = TEST_COLLAB
        self.job_runner = nmpi_saga.JobRunner(
            dict(
                JOB_SERVICE_ADAPTOR="fork://localhost",
                AUTH_USER="nmpi",
                AUTH_TOKEN=HARDWARE_TOKEN,
                NMPI_HOST=ENTRYPOINT,
                PLATFORM_NAME=TEST_PLATFORM,
                VERIFY_SSL=True,
            )
        )

    def _submit_test_job(self):
        self.last_job = self.user_client.submit_job(
            source=simple_test_script, platform=TEST_PLATFORM, collab_id=self.collab_id
        )

    def test__get_next_job(self):
        self._submit_test_job()
        nmpi_job = self.job_runner.client.get_job(self.last_job)
        self.assertEqual(nmpi_job["status"], "submitted")
        self.assertEqual(nmpi_job["user_id"], TEST_USER)
        self.assertEqual(nmpi_job["hardware_platform"], TEST_PLATFORM)

    def test__update_status(self):
        self._submit_test_job()
        nmpi_job = self.job_runner.client.get_job(self.last_job, with_log=False)
        nmpi_job = self.job_runner._update_status(
            nmpi_job, MockSagaJob(saga.job.RUNNING), nmpi_saga.default_job_states
        )
        self.assertEqual(nmpi_job["status"], "running")
        retrieved_job = self.user_client.get_job(nmpi_job["id"], with_log=False)
        # don't want to compare logs here for simplicity/speed
        retrieved_job.pop("log")
        nmpi_job.pop("log")
        self.assertEqual(retrieved_job, nmpi_job)


class FullStackTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir=os.path.join(os.path.expanduser("~/"), "tmp"))
        self.job_runner = nmpi_saga.JobRunner(
            dict(
                JOB_SERVICE_ADAPTOR="fork://localhost",
                AUTH_USER="nmpi",
                AUTH_TOKEN=HARDWARE_TOKEN,
                NMPI_HOST=ENTRYPOINT,
                PLATFORM_NAME=TEST_PLATFORM,
                VERIFY_SSL=True,
                WORKING_DIRECTORY=self.tmpdir,
                JOB_EXECUTABLE_PYNN_11=os.path.join(VIRTUAL_ENV, "bin", "python"),
                DATA_DIRECTORY=self.tmpdir,
                DEFAULT_PYNN_BACKEND="nest",
                DATA_SERVER_IDENTIFIER="TestRepository",
                DATA_SERVER="http://example.com/",
            )
        )
        self.user_client = nmpi_user.Client(TEST_USER, password=TEST_PWD, job_service=ENTRYPOINT)
        self.collab_id = TEST_COLLAB

    def tearDown(self):
        self.job_runner.close()
        shutil.rmtree(self.tmpdir)

    def _submit_test_job(self):
        self.user_client.submit_job(
            source=simple_test_script, platform=TEST_PLATFORM, collab_id=self.collab_id
        )

    def test_all__no_input_data(self):
        self._submit_test_job()
        finished_jobs = self.job_runner.next()
        assert len(finished_jobs) >= 1
        for nmpi_job in finished_jobs:
            assert nmpi_job["status"] == "finished"
            assert datetime.today().date().isoformat() in nmpi_job["log"]
            assert "running" in nmpi_job["log"]
            assert "finished" in nmpi_job["log"]
        assert len(nmpi_job["output_data"]["files"]) >= 1


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

        job_runner = nmpi_saga.JobRunner(
            dict(
                JOB_SERVICE_ADAPTOR="fork://localhost",
                AUTH_USER="nmpi",
                AUTH_TOKEN=HARDWARE_TOKEN,
                NMPI_HOST=ENTRYPOINT,
                PLATFORM_NAME=TEST_PLATFORM,
                VERIFY_SSL=True,
            )
        )
        job = MockSagaJob("submitted", working_directory=self.tmp_run_dir)
        mock_nmpi_job = {"code": "file://{}".format(zipfile)}
        job_runner._get_code(mock_nmpi_job, job)
        self.assertEqual(os.listdir(self.tmp_run_dir), ["run.py", "testcode.zip"])
        with open(os.path.join(self.tmp_run_dir, "run.py")) as fp:
            self.assertEqual(fp.read(), simulation_test_script)

    def test_get_code_tar(self):
        archive = os.path.join(self.tmp_src_dir, "testcode.tar.gz")
        with tarfile.open(archive, "w:gz") as tf:
            with open("run.py", "w") as fp:
                fp.write(simulation_test_script)
            tf.add("run.py")
            os.remove("run.py")
        job_runner = nmpi_saga.JobRunner(
            dict(
                JOB_SERVICE_ADAPTOR="fork://localhost",
                AUTH_USER="nmpi",
                AUTH_TOKEN=HARDWARE_TOKEN,
                NMPI_HOST=ENTRYPOINT,
                PLATFORM_NAME=TEST_PLATFORM,
                VERIFY_SSL=True,
            )
        )
        job = MockSagaJob("submitted", working_directory=self.tmp_run_dir)
        mock_nmpi_job = {"code": "file://{}".format(archive)}
        job_runner._get_code(mock_nmpi_job, job)
        self.assertEqual(os.listdir(self.tmp_run_dir), ["run.py", "testcode.tar.gz"])
        with open(os.path.join(self.tmp_run_dir, "run.py")) as fp:
            self.assertEqual(fp.read(), simulation_test_script)
