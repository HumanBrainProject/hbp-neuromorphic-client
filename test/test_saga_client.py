"""


"""

import sys
import os.path
import io
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
VIRTUAL_ENV = "/usr/local"
# VIRTUAL_ENV = "/Users/adavison/dev/simulation/env"


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
        tmpdir = tempfile.mkdtemp()
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
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "run.py"), "w") as fp:
            fp.write(simulation_test_script)
        job_desc = saga.job.Description()
        job_desc.working_directory = tmpdir
        job_desc.executable = os.path.join(VIRTUAL_ENV, "bin", "python")
        job_desc.arguments = [os.path.join(tmpdir, "run.py"), "mock"]
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
        self.tmpdir = tempfile.mkdtemp()
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
                DEFAULT_PYNN_BACKEND="mock",
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
        self.tmp_src_dir = tempfile.mkdtemp()
        self.tmp_run_dir = tempfile.mkdtemp()

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
        self.assertEqual(sorted(os.listdir(self.tmp_run_dir)), ["run.py", "testcode.zip"])
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
        self.assertEqual(sorted(os.listdir(self.tmp_run_dir)), ["run.py", "testcode.tar.gz"])
        with open(os.path.join(self.tmp_run_dir, "run.py")) as fp:
            self.assertEqual(fp.read(), simulation_test_script)


class _FakeSagaJob(object):
    """Minimal stand-in for a SAGA job; wait() marks the job as no longer running."""

    def __init__(self, job_id, running):
        self.id = job_id
        self._running = running

    def wait(self):
        self._running.discard(self.id)


class ConcurrencyLimitTest(unittest.TestCase):
    """
    Unit tests for JobRunner.next() concurrency limiting.

    These do not touch the network or a real SAGA backend: __init__ is bypassed
    and the queue client / job execution are replaced with in-memory fakes, so we
    can assert how many jobs run at once for a given MAX_CONCURRENT_JOBS value.
    """

    def _make_runner(self, job_ids, max_concurrent_jobs):
        running = set()
        observed = {"max": 0}
        pending_ids = list(job_ids)

        runner = nmpi_saga.JobRunner.__new__(nmpi_saga.JobRunner)
        runner.max_concurrent_jobs = max_concurrent_jobs
        runner.config = {}  # CLEANUP_WORKDIR unset -> no workdir removal

        class _FakeClient(object):
            def get_next_job(self):
                if pending_ids:
                    return {"id": pending_ids.pop(0)}
                return None

        runner.client = _FakeClient()

        def fake_run(nmpi_job):
            running.add(nmpi_job["id"])
            observed["max"] = max(observed["max"], len(running))
            return _FakeSagaJob(nmpi_job["id"], running)

        # Replace the methods next() depends on with no-op/fake versions.
        runner.run = fake_run
        runner._update_status = lambda nmpi_job, saga_job, states: nmpi_job
        runner._handle_output_data = lambda nmpi_job, saga_job: None
        return runner, observed

    def test_sequential_when_limit_is_one(self):
        runner, observed = self._make_runner([1, 2, 3, 4], max_concurrent_jobs=1)
        completed = runner.next()
        self.assertEqual([j["id"] for j in completed], [1, 2, 3, 4])
        self.assertEqual(observed["max"], 1)

    def test_respects_limit_above_one(self):
        runner, observed = self._make_runner([1, 2, 3, 4, 5], max_concurrent_jobs=2)
        completed = runner.next()
        self.assertEqual([j["id"] for j in completed], [1, 2, 3, 4, 5])
        self.assertLessEqual(observed["max"], 2)

    def test_no_limit_starts_all_before_waiting(self):
        # The default (None) preserves the original behaviour: every queued job is
        # started before any is waited on.
        runner, observed = self._make_runner([1, 2, 3, 4], max_concurrent_jobs=None)
        completed = runner.next()
        self.assertEqual([j["id"] for j in completed], [1, 2, 3, 4])
        self.assertEqual(observed["max"], 4)


class CodeFetchSecurityTest(unittest.TestCase):
    """
    Unit tests for the Tier 1 code-retrieval hardening: safe archive extraction
    and the code-URL host allowlist. These are pure functions; no network needed.
    """

    def setUp(self):
        self.src_dir = tempfile.mkdtemp()
        self.dest_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.src_dir, ignore_errors=True)
        shutil.rmtree(self.dest_dir, ignore_errors=True)

    def test_safe_extract_zip_accepts_legitimate_archive(self):
        archive = os.path.join(self.src_dir, "ok.zip")
        with ZipFile(archive, "w") as zf:
            zf.writestr("run.py", "print('hi')")
        nmpi_saga.safe_extract_zip(archive, self.dest_dir)
        self.assertEqual(os.listdir(self.dest_dir), ["run.py"])

    def test_safe_extract_zip_rejects_path_traversal(self):
        archive = os.path.join(self.src_dir, "evil.zip")
        with ZipFile(archive, "w") as zf:
            zf.writestr("../escape.txt", "pwned")
        with self.assertRaises(ValueError):
            nmpi_saga.safe_extract_zip(archive, self.dest_dir)
        self.assertFalse(os.path.exists(os.path.join(os.path.dirname(self.dest_dir), "escape.txt")))

    def test_safe_extract_zip_rejects_absolute_path(self):
        archive = os.path.join(self.src_dir, "abs.zip")
        with ZipFile(archive, "w") as zf:
            zf.writestr("/tmp/escape.txt", "pwned")
        with self.assertRaises(ValueError):
            nmpi_saga.safe_extract_zip(archive, self.dest_dir)

    def test_safe_extract_tar_rejects_path_traversal(self):
        archive = os.path.join(self.src_dir, "evil.tar.gz")
        with tarfile.open(archive, "w:gz") as tf:
            data = b"pwned"
            info = tarfile.TarInfo("../escape.txt")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        with self.assertRaises(ValueError):
            nmpi_saga.safe_extract_tar(archive, self.dest_dir)

    def test_safe_extract_tar_rejects_symlink(self):
        archive = os.path.join(self.src_dir, "link.tar.gz")
        with tarfile.open(archive, "w:gz") as tf:
            info = tarfile.TarInfo("link")
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            tf.addfile(info)
        with self.assertRaises(ValueError):
            nmpi_saga.safe_extract_tar(archive, self.dest_dir)

    def test_check_code_url_allows_when_no_allowlist(self):
        # Empty allowlist -> any host permitted (backwards compatible).
        nmpi_saga.check_code_url("https://anywhere.example.com/repo.git", [])

    def test_check_code_url_enforces_allowlist(self):
        allowed = ["github.com", "gitlab.ebrains.eu"]
        nmpi_saga.check_code_url("https://github.com/user/repo.git", allowed)
        with self.assertRaises(ValueError):
            nmpi_saga.check_code_url("https://evil.example.com/repo.git", allowed)
