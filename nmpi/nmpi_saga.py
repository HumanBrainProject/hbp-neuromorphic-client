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

from urllib.parse import urlparse
import shutil
from datetime import datetime
import time
import mimetypes
import hashlib
import radical.saga as saga
import logging
import sh
from sh import git, unzip, tar, curl
import nmpi
import codecs
from requests.auth import AuthBase


DEFAULT_SCRIPT_NAME = "run.py {system}"
DEFAULT_PYNN_VERSION = "0.11"
MAX_LOG_SIZE = 10000

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
            stream[: MAX_LOG_SIZE // 2] + "\n\n... truncated...\n\n" + stream[-MAX_LOG_SIZE // 2 :]
        )
    else:
        return stream


def job_done(nmpi_job, saga_job):
    nmpi_job["status"] = "finished"
    timestamp = datetime.now().isoformat()
    nmpi_job["timestamp_completion"] = timestamp
    nmpi_job["resource_usage"] = {"value": 1.0, "units": "litres"}  # todo: report the actual usage
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
        log = job.pop("log", None)
        response = self._put(f"{self.job_server}{job['resource_uri']}", job)
        response["log"] = log
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


class JobRunner(object):
    """ """

    def __init__(self, config):
        self.config = config
        self.service = saga.job.Service(config["JOB_SERVICE_ADAPTOR"])
        self.client = HardwareClient(
            username=config["AUTH_USER"],
            api_key=config["AUTH_TOKEN"],
            job_service=config["NMPI_HOST"],
            platform=config["PLATFORM_NAME"],
            verify=config.get("VERIFY_SSL", True),
        )

    def next(self):
        """
        Get the next job by oldest date in the queue, and run it.
        """
        pending_jobs = []
        while True:
            logger.info("Retrieving next job")
            nmpi_job = self.client.get_next_job()
            if nmpi_job is None or nmpi_job in [n for n, s in pending_jobs]:
                logger.info("No new jobs")
                break
            saga_job = self.run(nmpi_job)
            self._update_status(nmpi_job, saga_job, default_job_states)
            pending_jobs.append((nmpi_job, saga_job))
        for nmpi_job, saga_job in pending_jobs:
            saga_job.wait()
            logger.info("Job {} completed".format(saga_job.id))
            self._handle_output_data(nmpi_job, saga_job)
            self._update_status(nmpi_job, saga_job, default_job_states)
            logger.debug("Status of completed job updated")
        return [n for n, s in pending_jobs]

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
        self.start_time = datetime.now()
        time.sleep(1)  # ensure output file timestamps are different from start_time
        logger.info("Running job {}".format(nmpi_job["id"]))
        saga_job.run()
        # todo: add logger.warning if job fails
        return saga_job

    def close(self):
        self.service.close()

    def _build_job_description(self, nmpi_job):
        """
        Construct a Saga job description based on an NMPI job description and
        the local configuration.
        """

        job_desc = saga.job.Description()
        job_id = nmpi_job["id"]
        job_desc.working_directory = os.path.join(
            self.config["WORKING_DIRECTORY"], f"job_{job_id}"
        )
        # job_desc.spmd_variation    = "MPI" # to be commented out if not using MPI

        if nmpi_job["hardware_config"] is None:
            pyNN_version = DEFAULT_PYNN_VERSION
        else:
            pyNN_version = nmpi_job["hardware_config"].get("pyNN_version", DEFAULT_PYNN_VERSION)

        if pyNN_version == "0.10":
            job_desc.executable = self.config["JOB_EXECUTABLE_PYNN_10"]
        elif pyNN_version == "0.11":
            job_desc.executable = self.config["JOB_EXECUTABLE_PYNN_11"]
        else:
            raise ValueError(
                "Supported PyNN versions: 0.10, 0.11. {} not supported".format(pyNN_version)
            )

        if self.config.get("JOB_QUEUE", None) is not None:
            job_desc.queue = self.config["JOB_QUEUE"]  # aka SLURM "partition"
        script_name = nmpi_job.get("command", "")
        if not script_name:
            script_name = DEFAULT_SCRIPT_NAME
        command_line = script_name.format(
            system=self.config["DEFAULT_PYNN_BACKEND"]
        )  # TODO: allow choosing backend in "hardware_config
        command_line = os.path.join(job_desc.working_directory, command_line)
        job_desc.arguments = command_line.split(" ")
        job_desc.output = f"saga_{job_id}.out"
        job_desc.error = f"saga_{job_id}.err"
        # job_desc.total_cpu_count
        # job_desc.number_of_processes = 1
        # job_desc.processes_per_host
        # job_desc.threads_per_process
        # job_desc.wall_time_limit = 1
        # job_desc.total_physical_memory
        logger.info(command_line)
        return job_desc

    def _create_working_directory(self, workdir):
        if not os.path.exists(workdir):
            logger.debug(f"Creating directory {workdir}")
            os.makedirs(workdir)
            logger.debug(f"Created directory {workdir}")
        else:
            logger.debug(f"Directory {workdir} already exists")

    def _get_code(self, nmpi_job, job_desc):
        """
        Obtain the code and place it in the working directory.

        If the experiment description is the URL of a Git repository, try to clone it.
        If it is the URL of a zip or .tar.gz archive, download and unpack it.
        Otherwise, the content of "code" is the code: write it to a file.
        """
        url_candidate = urlparse(nmpi_job["code"])
        logger.debug("Get code: {url_candidate.netloc} {url_candidate.path}")
        if url_candidate.scheme and url_candidate.path.endswith((".tar.gz", ".zip", ".tgz")):
            self._create_working_directory(job_desc.working_directory)
            target = os.path.join(job_desc.working_directory, os.path.basename(url_candidate.path))
            # urlretrieve(nmpi_job['code'], target) # not working via KIP https proxy
            curl(nmpi_job["code"], "-o", target)
            logger.info(f"Retrieved file from {nmpi_job['code']} to local target {target}")
            if url_candidate.path.endswith((".tar.gz", ".tgz")):
                tar("xzf", target, directory=job_desc.working_directory)
            elif url_candidate.path.endswith(".zip"):
                try:
                    # -o for auto-overwrite
                    unzip("-o", target, d=job_desc.working_directory)
                except:
                    logger.error(f"Could not unzip file {target}")
        else:
            try:
                # Check the "code" field for a git url (clone it into the workdir) or a script (create a file into the workdir)
                # URL: use git clone
                git.clone("--recursive", nmpi_job["code"], job_desc.working_directory)
                logger.info(f"Cloned repository {nmpi_job['code']}")
            except (sh.ErrorReturnCode_128, sh.ErrorReturnCode):
                # SCRIPT: create file (in the current directory)
                logger.info("The code field appears to be a script.")
                self._create_working_directory(job_desc.working_directory)
                with codecs.open(job_desc.arguments[0], "w", encoding="utf8") as job_main_script:
                    job_main_script.write(nmpi_job["code"])

    def _get_input_data(self, nmpi_job, job_desc):
        """
        Retrieve eventual additional input DataItem

        We assume that the script knows the input files are in the same folder
        """
        if "input_data" in nmpi_job and len(nmpi_job["input_data"]):
            filelist = self.client.download_data_url(nmpi_job, job_desc.working_directory, True)

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
        new_files = _find_new_data_files(job_desc.working_directory, self.start_time)
        output_dir = os.path.join(
            self.config["DATA_DIRECTORY"], os.path.basename(job_desc.working_directory)
        )
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
    config = load_config(os.environ.get("NMPI_CONFIG", os.path.join(os.getcwd(), "nmpi.cfg")))
    runner = JobRunner(config)
    runner.next()
    return 0  # todo: handle exceptions


if __name__ == "__main__":
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    retcode = main()
    sys.exit(retcode)
