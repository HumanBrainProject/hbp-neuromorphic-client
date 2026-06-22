"""
Job request (using NMPI API) and execution (using SAGA)

0. This script is called by a cron job
1. it uses the nmpi api to retrieve the next nmpi_job (FIFO of nmpi_job with status='submitted')
2. reads the content of the nmpi_job
3. creates a folder for the nmpi_job
4. obtains the experiment source code specified in the nmpi_job description
5. retrieves input data, if any
7. submits the job to the cluster with SAGA
8. waits for the answer and updates the log and status of the nmpi_job
9. checks for newly created files in the nmpi_job folder and adds them to the list of nmpi_job output data
10. final nmpi_job status modification to 'finished' or 'error'

Authors: Domenico Guarino,
         Andrew Davison

All the personalization should happen in the config file.

"""

import os
import stat

from urllib.parse import urlparse
import shutil
import tarfile
import zipfile
from datetime import datetime
import time
import mimetypes
import hashlib
import logging
import codecs
import traceback

import radical.saga as saga
from sh import git, curl
from requests.auth import AuthBase

import nmpi


DEFAULT_SCRIPT_NAME = "run.py {system}"
DEFAULT_PYNN_VERSION = "0.11"
MAX_LOG_SIZE = 10000

# Untrusted job-code retrieval limits and recognised forms.
ARCHIVE_EXTENSIONS = (".tar.gz", ".tgz", ".zip")
REMOTE_URL_SCHEMES = ("http", "https", "git", "ssh", "git+ssh", "git+https")
MAX_INLINE_SCRIPT_SIZE = 1024 * 1024  # 1 MiB of inline Python
MAX_ARCHIVE_EXTRACTED_SIZE = 500 * 1024 * 1024  # 500 MiB unpacked (zip-bomb guard)

logger = logging.getLogger("NMPI")


# status functions
def job_pending(nmpi_job, saga_job):
    nmpi_job["status"] = "submitted"
    log = nmpi_job.pop("log", "") or ""
    log += "Job ID: {}\n".format(saga_job.id)
    log += "{}    pending\n".format(datetime.now().isoformat())
    nmpi_job["log"] = log
    return nmpi_job


def job_running(nmpi_job, saga_job):
    nmpi_job["status"] = "running"
    log = nmpi_job.pop("log", "") or ""
    log += "{}    running\n".format(datetime.now().isoformat())
    nmpi_job["log"] = log
    return nmpi_job


def _truncate(stream):
    # todo: where we truncate, should save the entire log to file
    if len(stream) > MAX_LOG_SIZE:
        return (
            stream[: MAX_LOG_SIZE // 2]
            + "\n\n... truncated...\n\n"
            + stream[-MAX_LOG_SIZE // 2 :]  # noqa: E203
        )
    else:
        return stream


def job_done(nmpi_job, saga_job):
    nmpi_job["status"] = "finished"
    timestamp = datetime.now()
    nmpi_job["timestamp_completion"] = timestamp.isoformat()
    duration = timestamp - nmpi_job["timestamp_started"]
    nmpi_job["resource_usage"] = {"value": duration.total_seconds() / 3600, "units": "hours"}
    nmpi_job["provenance"] = {}  # todo: report provenance information
    log = nmpi_job.pop("log", "") or ""
    log += "{}    finished\n".format(datetime.now().isoformat())
    stdout, stderr = read_output(saga_job)
    log += "\n\n"
    log += _truncate(stdout)
    log += "\n\n"
    log += _truncate(stderr)
    nmpi_job["log"] = log
    return nmpi_job


def job_failed(nmpi_job, saga_job):
    nmpi_job["status"] = "error"
    timestamp = datetime.now()
    nmpi_job["timestamp_completion"] = timestamp.isoformat()
    duration = timestamp - nmpi_job["timestamp_started"]
    nmpi_job["resource_usage"] = {"value": duration.total_seconds() / 3600, "units": "hours"}

    log = nmpi_job.pop("log", "") or ""
    log += "{}    failed\n\n".format(datetime.now().isoformat())
    stdout, stderr = read_output(saga_job)
    log += _truncate(stderr)
    log += "\n\nstdout\n------\n\n"
    log += _truncate(stdout)
    nmpi_job["log"] = log
    return nmpi_job


# states switch
default_job_states = {
    saga.job.PENDING: job_pending,
    saga.job.RUNNING: job_running,
    saga.job.DONE: job_done,
    saga.job.FAILED: job_failed,
}


def load_config(fullpath):
    conf = {}
    with open(fullpath) as f:
        for line in f:
            # leave out comment as python/bash
            if not line.startswith("#") and len(line) >= 5:
                (key, val) = line.split("=")
                conf[key.strip()] = val.strip()
    for key, val in conf.items():
        if val in ("True", "False", "None"):
            conf[key] = eval(val)
    logger.debug("Loaded configuration file '{}' with contents: {}".format(fullpath, conf))
    return conf


def sha1sum(filename):
    BUFFER_SIZE = 128 * 1024
    h = hashlib.sha1()
    with open(filename, "rb") as fp:
        while True:
            data = fp.read(BUFFER_SIZE)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


class NMPAuth(AuthBase):
    """Attaches ApiKey Authentication to the given Request object."""

    def __init__(self, username, api_key):
        # setup any auth-related data here
        self.username = username
        self.api_key = api_key

    def __call__(self, r):
        # modify and return the request
        r.headers["x-api-key"] = self.api_key
        return r


class HardwareClient(nmpi.Client):
    """
    Client for interacting from a specific hardware system with the EBRAINS Neuromorphic Computing Platform.

    This includes submitting jobs, tracking job status, retrieving the results of completed jobs,
    and creating and administering projects.

    Arguments
    ---------

    username, password : credentials for accessing the platform
    entrypoint : the base URL of the platform. Generally the default value should be used.

    """

    def __init__(
        self,
        username,
        platform,
        api_key,
        job_service="https://nmpi-v3.hbpneuromorphic.eu/",
        verify=True,
    ):
        self.username = username
        self.cert = None
        self.verify = verify
        self.api_key = api_key
        (scheme, netloc, path, params, query, fragment) = urlparse(job_service)
        self.job_server = f"{scheme}://{netloc}"
        self.auth = NMPAuth(self.username, self.api_key)
        self.platform = platform

    def get_next_job(self):
        """
        Get the next job by oldest date in the queue.
        """
        try:
            job_nmpi = self._query(f"{self.job_server}/jobs/next/{self.platform}")
        except Exception as err:
            if "404" in str(err):
                job_nmpi = None
            else:
                raise
        return job_nmpi

    def update_job(self, job):
        job_copy = job.copy()
        log = job_copy.pop("log", None)
        job_copy.pop("timestamp_started", None)
        response = self._put(f"{self.job_server}{job['resource_uri']}", job_copy)
        response2 = self._put(f"{self.job_server}{job['resource_uri']}/log", log, format="text")
        return response

    def reset_job(self, job):
        """
        If a job is stuck in the "running" state due to a problem on the backend,
        reset its status to "submitted".
        """
        job["status"] = "submitted"
        if job["log"] is None:
            job["log"] = ""
        job["log"] += "\nreset status to 'submitted'\n"
        return self._put(f"{self.job_server}{job['resource_uri']}", job)

    def kill_job(self, job, error_message=""):
        """
        Set the status of a queued or running job to "error".

        This should be used circumspectly. It is usually better to use
        `reset_job()`.
        """
        if job["status"] not in ("running", "submitted"):
            raise Exception(f"You cannot kill a job with status {job['status']}")
        job["status"] = "error"
        if job["log"] is None:
            job["log"] = ""
        job["log"] += "Internal error. Please resubmit the job\n"
        job["log"] += error_message
        response = self._put(f"{self.job_server}{job['resource_uri']}", job)
        return response

    def queued_jobs(self, verbose=False):
        """
        Return the list of submitted jobs for the current platform.

        Arguments
        ---------

        verbose : if False, return just the job URIs,
                  if True, return full details.
        """
        return self._query(
            f"{self.job_server}/jobs/?hardware_platform={self.platform}&status=submitted",
            verbose=verbose,
        )

    def running_jobs(self, verbose=False):
        """
        Return the list of running jobs for the current platform.

        Arguments
        ---------

        verbose : if False, return just the job URIs,
                  if True, return full details.
        """
        return self._query(
            f"{self.job_server}/jobs/?hardware_platform={self.platform}&status=running",
            verbose=verbose,
        )


# adapted from Sumatra
def _find_new_data_files(root, timestamp, ignoredirs=[".git"], ignore_extensions=[".pyc"]):
    """Finds newly created/changed files in root."""
    length_root = len(root) + len(os.path.sep)
    new_files = []
    for root, dirs, files in os.walk(root):
        for igdir in ignoredirs:
            if igdir in dirs:
                dirs.remove(igdir)
        for file in files:
            if os.path.splitext(file)[1] not in ignore_extensions:
                full_path = os.path.join(root, file)
                relative_path = os.path.join(root[length_root:], file)
                last_modified = datetime.fromtimestamp(os.stat(full_path).st_mtime)
                if last_modified >= timestamp:
                    new_files.append(relative_path)
    return new_files


def read_output(saga_job):
    """
    Read and return the contents of the stdout and stderr files
    created by the SAGA job.
    """
    job_desc = saga_job.get_description()
    outfile = os.path.join(job_desc.working_directory, job_desc.output)
    errfile = os.path.join(job_desc.working_directory, job_desc.error)
    try:
        with open(outfile) as fp:
            stdout = fp.read()
        with open(errfile) as fp:
            stderr = fp.read()
        return stdout, stderr
    except IOError:
        # weird things can happen...
        return "", ""


def check_code_url(url, allowed_hosts):
    """
    Validate the URL a job's code is fetched from, returning the parsed URL.

    ``allowed_hosts`` is a list of permitted host names. If it is empty, any host
    is allowed (backwards-compatible behaviour). Raises ValueError if the host is
    not permitted.
    """
    parsed = urlparse(url)
    if allowed_hosts:
        host = parsed.hostname
        if host is None or host not in allowed_hosts:
            raise ValueError(
                "Code URL host {!r} is not in the allowed list {}".format(host, allowed_hosts)
            )
    return parsed


def _is_within_directory(directory, target_name):
    """True if ``target_name`` resolves to a path inside ``directory``."""
    directory = os.path.realpath(directory)
    target = os.path.realpath(os.path.join(directory, target_name))
    return target == directory or target.startswith(directory + os.sep)


def _reject_unsafe_member(name, dest_dir):
    """Raise ValueError if an archive member name would escape ``dest_dir``."""
    if os.path.isabs(name) or os.path.normpath(name).startswith(".."):
        raise ValueError("Unsafe path in archive: {!r}".format(name))
    if not _is_within_directory(dest_dir, name):
        raise ValueError("Archive member escapes target directory: {!r}".format(name))


def safe_extract_zip(archive_path, dest_dir, max_total_bytes=MAX_ARCHIVE_EXTRACTED_SIZE):
    """
    Extract a zip archive into ``dest_dir``, rejecting path traversal (zip-slip),
    symlinks, and archives that expand beyond ``max_total_bytes`` (zip bombs).
    """
    total = 0
    with zipfile.ZipFile(archive_path) as zf:
        for info in zf.infolist():
            _reject_unsafe_member(info.filename, dest_dir)
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise ValueError("Archive contains a symlink: {!r}".format(info.filename))
            total += info.file_size
            if total > max_total_bytes:
                raise ValueError("Archive expands to more than the allowed size")
        zf.extractall(dest_dir)


def safe_extract_tar(archive_path, dest_dir, max_total_bytes=MAX_ARCHIVE_EXTRACTED_SIZE):
    """
    Extract a tar archive into ``dest_dir``, rejecting path traversal, links and
    device files, and archives that expand beyond ``max_total_bytes``.
    """
    total = 0
    with tarfile.open(archive_path) as tf:
        members = tf.getmembers()
        for member in members:
            _reject_unsafe_member(member.name, dest_dir)
            if member.issym() or member.islnk():
                raise ValueError("Archive contains a link: {!r}".format(member.name))
            if member.isdev():
                raise ValueError("Archive contains a device file: {!r}".format(member.name))
            total += member.size
            if total > max_total_bytes:
                raise ValueError("Archive expands to more than the allowed size")
        try:
            # Python >= 3.12: extra defence-in-depth filtering.
            tf.extractall(dest_dir, filter="data")
        except TypeError:
            tf.extractall(dest_dir)


class JobRunner(object):
    """ """

    def __init__(self, config):
        self.config = config
        self.service = saga.job.Service(config["JOB_SERVICE_ADAPTOR"])
        # Maximum number of jobs to run concurrently. The default, None, means no
        # limit, which is appropriate for batch-scheduler adaptors (e.g. SLURM) that
        # queue jobs themselves. For the local "fork://" adaptor, where each job is
        # a process forked on this machine, set this to a small number (1 for strict
        # one-at-a-time execution) to avoid overloading a resource-constrained host.
        max_concurrent = config.get("MAX_CONCURRENT_JOBS", None)
        if max_concurrent is not None:
            max_concurrent = int(max_concurrent)
            if max_concurrent < 1:
                raise ValueError("MAX_CONCURRENT_JOBS must be >= 1 (or None for no limit)")
        self.max_concurrent_jobs = max_concurrent
        self.client = HardwareClient(
            username=config["AUTH_USER"],
            api_key=config["AUTH_TOKEN"],
            job_service=config["NMPI_HOST"],
            platform=config["PLATFORM_NAME"],
            verify=config.get("VERIFY_SSL", True),
        )
        logger.info(f"JobRunner config: {config}")

    def next(self):
        """
        Get available jobs from the queue, oldest to newest, and run them.

        At most ``self.max_concurrent_jobs`` jobs run at the same time. When that
        limit is reached, we wait for the oldest in-flight job to finish (handling
        its output) before starting the next one. With ``max_concurrent_jobs`` set
        to 1 this gives strictly sequential execution; with ``None`` (the default)
        there is no limit and all queued jobs are started before any is waited on.
        """
        completed_jobs = []
        pending_jobs = []  # (nmpi_job, saga_job), oldest first
        seen_jobs = []

        def finish_oldest():
            nmpi_job, saga_job = pending_jobs.pop(0)
            saga_job.wait()
            logger.info("Job {} completed".format(saga_job.id))
            self._handle_output_data(nmpi_job, saga_job)
            self._update_status(nmpi_job, saga_job, default_job_states)
            logger.debug("Status of completed job updated")
            self._cleanup_working_directory(saga_job)
            completed_jobs.append(nmpi_job)

        while True:
            logger.info("Retrieving next job")
            nmpi_job = self.client.get_next_job()
            if nmpi_job is None or nmpi_job in seen_jobs:
                logger.info("No new jobs")
                break
            seen_jobs.append(nmpi_job)
            # Respect the concurrency limit before starting the job.
            if self.max_concurrent_jobs is not None:
                while len(pending_jobs) >= self.max_concurrent_jobs:
                    finish_oldest()
            try:
                saga_job = self.run(nmpi_job)
            except Exception:
                # A bad job (e.g. an unreachable repository or an oversized script)
                # must not stall the queue: mark it failed and move on.
                logger.exception("Could not start job %s", nmpi_job.get("id"))
                self._mark_job_failed(nmpi_job, traceback.format_exc())
                completed_jobs.append(nmpi_job)
                continue
            self._update_status(nmpi_job, saga_job, default_job_states)
            pending_jobs.append((nmpi_job, saga_job))
        # Wait for any jobs still running.
        while pending_jobs:
            finish_oldest()
        return completed_jobs

    def _mark_job_failed(self, nmpi_job, message):
        """Record a job as failed on the queue without raising."""
        nmpi_job["status"] = "error"
        nmpi_job["timestamp_completion"] = datetime.now().isoformat()
        log = nmpi_job.pop("log", "") or ""
        nmpi_job["log"] = log + "\n" + _truncate(message)
        try:
            self.client.update_job(nmpi_job)
        except Exception:
            logger.exception("Could not update status of failed job %s", nmpi_job.get("id"))

    def _cleanup_working_directory(self, saga_job):
        """Remove a job's working directory once its output has been collected."""
        if not self.config.get("CLEANUP_WORKDIR", False):
            return
        workdir = saga_job.get_description().working_directory
        base = self.config.get("WORKING_DIRECTORY")
        # Safety: only ever remove a per-job directory under the configured root.
        if base and os.path.realpath(workdir).startswith(os.path.realpath(base) + os.sep):
            shutil.rmtree(workdir, ignore_errors=True)
            logger.debug("Removed working directory %s", workdir)

    def run(self, nmpi_job):
        # Build the job description
        job_desc = self._build_job_description(nmpi_job)
        # Get the source code for the experiment
        self._get_code(nmpi_job, job_desc)
        # Download any input data
        self._get_input_data(nmpi_job, job_desc)
        # Submit a job to the cluster with SAGA."""
        saga_job = self.service.create_job(job_desc)
        # Run the job
        nmpi_job["timestamp_started"] = datetime.now()
        time.sleep(1)  # ensure output file timestamps are different from start_time
        logger.info("Running job {}".format(nmpi_job["id"]))
        saga_job.run()
        # todo: add logger.warning if job fails
        return saga_job

    def close(self):
        self.service.close()

    def _working_directory(self, nmpi_job):
        return os.path.join(self.config["WORKING_DIRECTORY"], "job_{}".format(nmpi_job["id"]))

    def _command_tokens(self, nmpi_job):
        """The job's command split into tokens, e.g. ["run.py", "nest"]."""
        script_name = nmpi_job.get("command", "") or DEFAULT_SCRIPT_NAME
        command_line = script_name.format(system=self.config["DEFAULT_PYNN_BACKEND"])
        return command_line.split(" ")

    def _main_script_path(self, nmpi_job):
        """Absolute path of the job's main script within its working directory."""
        return os.path.join(self._working_directory(nmpi_job), self._command_tokens(nmpi_job)[0])

    def _build_job_description(self, nmpi_job):
        """
        Construct a Saga job description based on an NMPI job description and
        the local configuration.
        """

        job_desc = saga.job.Description()
        job_id = nmpi_job["id"]
        working_directory = self._working_directory(nmpi_job)
        job_desc.working_directory = working_directory
        # job_desc.spmd_variation    = "MPI" # to be commented out if not using MPI

        if nmpi_job["hardware_config"] is None:
            pyNN_version = DEFAULT_PYNN_VERSION
        else:
            pyNN_version = nmpi_job["hardware_config"].get("pyNN_version", DEFAULT_PYNN_VERSION)

        if pyNN_version == "0.10":
            executable = self.config["JOB_EXECUTABLE_PYNN_10"]
        elif pyNN_version == "0.11":
            executable = self.config["JOB_EXECUTABLE_PYNN_11"]
        else:
            raise ValueError(
                "Supported PyNN versions: 0.10, 0.11. {} not supported".format(pyNN_version)
            )

        if self.config.get("JOB_QUEUE", None) is not None:
            job_desc.queue = self.config["JOB_QUEUE"]  # aka SLURM "partition"

        tokens = self._command_tokens(nmpi_job)
        main_script = os.path.join(working_directory, tokens[0])
        real_arguments = [main_script] + tokens[1:]

        sandbox = self.config.get("SANDBOX_WRAPPER")
        if sandbox:
            # Run the untrusted script inside the sandbox wrapper, which gives it no
            # network, its own namespaces, only its working directory writable, and
            # resource/time limits. The wrapper takes the working directory as its
            # first argument, then the real executable and its arguments.
            job_desc.executable = sandbox
            job_desc.arguments = [working_directory, executable] + real_arguments
        else:
            job_desc.executable = executable
            job_desc.arguments = real_arguments

        job_desc.output = f"saga_{job_id}.out"
        job_desc.error = f"saga_{job_id}.err"
        # job_desc.total_cpu_count
        # job_desc.number_of_processes = 1
        logger.info(" ".join([job_desc.executable] + job_desc.arguments))
        return job_desc

    def _create_working_directory(self, workdir):
        if not os.path.exists(workdir):
            logger.debug(f"Creating directory {workdir}")
            os.makedirs(workdir)
            logger.debug(f"Created directory {workdir}")
        else:
            logger.debug(f"Directory {workdir} already exists")

    def _allowed_code_hosts(self):
        raw = self.config.get("ALLOWED_CODE_HOSTS", "") or ""
        return [host.strip() for host in raw.split(",") if host.strip()]

    def _clone_repository(self, url, working_directory):
        """Shallow-clone a git repository, without (attacker-controlled) submodules."""
        git_env = dict(os.environ)
        git_env.update(
            {
                "GIT_TERMINAL_PROMPT": "0",  # never prompt for credentials
                "GIT_ASKPASS": "/bin/true",
                "GIT_SSH_COMMAND": "ssh -o BatchMode=yes",
                "GIT_LFS_SKIP_SMUDGE": "1",
            }
        )
        # No --recursive: submodule URLs are attacker-controlled (SSRF / arbitrary fetch).
        git.clone(
            "--depth",
            "1",
            url,
            working_directory,
            _env=git_env,
            _timeout=int(self.config.get("CLONE_TIMEOUT", 120)),
        )
        logger.info("Cloned repository %s", url)

    def _get_code(self, nmpi_job, job_desc):
        """
        Obtain the code and place it in the working directory.

        If "code" is the URL of a zip/tar.gz archive, download and safely unpack it.
        If it is the URL of a Git repository, shallow-clone it (no submodules).
        Otherwise the content of "code" is the script itself: write it to a file.
        """
        code = nmpi_job["code"]
        allowed_hosts = self._allowed_code_hosts()
        parsed = urlparse(code)
        logger.debug("Get code: %s %s", parsed.netloc, parsed.path)

        if parsed.scheme and parsed.path.endswith(ARCHIVE_EXTENSIONS):
            check_code_url(code, allowed_hosts)
            self._create_working_directory(job_desc.working_directory)
            target = os.path.join(job_desc.working_directory, os.path.basename(parsed.path))
            # urlretrieve(code, target) # not working via KIP https proxy
            curl(code, "-o", target, _timeout=int(self.config.get("FETCH_TIMEOUT", 120)))
            logger.info("Retrieved file from %s to local target %s", code, target)
            if parsed.path.endswith((".tar.gz", ".tgz")):
                safe_extract_tar(target, job_desc.working_directory)
            else:
                safe_extract_zip(target, job_desc.working_directory)
        elif parsed.scheme in REMOTE_URL_SCHEMES:
            check_code_url(code, allowed_hosts)
            self._clone_repository(code, job_desc.working_directory)
        else:
            # The content is the script itself.
            logger.info("The code field appears to be a script.")
            if len(code.encode("utf-8")) > MAX_INLINE_SCRIPT_SIZE:
                raise ValueError("Inline job script exceeds the maximum allowed size")
            self._create_working_directory(job_desc.working_directory)
            with codecs.open(self._main_script_path(nmpi_job), "w", encoding="utf8") as fp:
                fp.write(code)

    def _get_input_data(self, nmpi_job, job_desc):
        """
        Retrieve eventual additional input DataItem

        We assume that the script knows the input files are in the same folder
        """
        if "input_data" in nmpi_job and len(nmpi_job["input_data"]):
            self.client.download_data_url(nmpi_job, job_desc.working_directory, True)

    def _update_status(self, nmpi_job, saga_job, job_states):
        """Update the status of the nmpi job."""
        saga_state = saga_job.get_state()
        logger.debug(f"SAGA state: {saga_state}")
        set_status = job_states[saga_state]
        nmpi_job = set_status(nmpi_job, saga_job)
        self.client.update_job(nmpi_job)
        return nmpi_job

    def _handle_output_data(self, nmpi_job, saga_job):
        """
        Adds the contents of the nmpi_job folder to the list of nmpi_job
        output data
        """
        job_desc = saga_job.get_description()
        new_files = _find_new_data_files(job_desc.working_directory, nmpi_job["timestamp_started"])
        output_dir = self.config["DATA_DIRECTORY"]
        logger.debug(f"Copying files to {output_dir}: {', '.join(new_files)}")

        nmpi_job["output_data"] = {
            "repository": self.config["DATA_SERVER_IDENTIFIER"],
            "files": [],
        }

        if self.config["DATA_DIRECTORY"] != self.config["WORKING_DIRECTORY"]:
            if not os.path.exists(self.config["DATA_DIRECTORY"]):
                try:
                    os.makedirs(self.config["DATA_DIRECTORY"])
                except Exception as err:
                    logging.error(err.message)
        for new_file in new_files:
            try:
                full_relative_path = os.path.join(
                    nmpi_job["collab"], f"job_{nmpi_job['id']}", new_file
                )
                new_file_path = os.path.join(output_dir, full_relative_path)
                if not os.path.exists(os.path.dirname(new_file_path)):
                    os.makedirs(os.path.dirname(new_file_path))
                original_path = os.path.join(job_desc.working_directory, new_file)
                if self.config["DATA_DIRECTORY"] != self.config["WORKING_DIRECTORY"]:
                    shutil.copyfile(original_path, new_file_path)
            except Exception as err:
                logging.error(err.message)
            else:
                data_server = self.config["DATA_SERVER"]
                if not data_server.endswith("/"):
                    data_server += "/"
                content_type, encoding = mimetypes.guess_type(original_path, strict=False)
                nmpi_job["output_data"]["files"].append(
                    {
                        "url": f"{data_server}{full_relative_path}",
                        "path": full_relative_path,
                        "content_type": content_type,
                        "size": os.stat(original_path).st_size,
                        "hash": sha1sum(original_path),
                    }
                )
        logger.debug("Handling of output data complete")


def main():
    logger.info(os.environ.get("NMPI_CONFIG", "Using default config"))
    config_file = os.environ.get("NMPI_CONFIG", os.path.join(os.getcwd(), "nmpi.cfg"))
    logger.info(f"Loading config from {config_file}")
    config = load_config(config_file)
    runner = JobRunner(config)
    try:
        jobs = runner.next()
        if len(jobs) == 0:
            print("No new jobs")
        else:
            for job in jobs:
                print(f"{job['id']} ({job['user_id']}): {job['status']}")
    except Exception as err:
        traceback.print_exc()
        return 1
    else:
        return 0


if __name__ == "__main__":
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    retcode = main()
    sys.exit(retcode)
