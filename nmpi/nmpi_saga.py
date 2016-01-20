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
from os import path
import logging
from urlparse import urlparse
from urllib import urlretrieve
import shutil
from datetime import datetime
import time
import saga
import sh
from sh import git, unzip, tar
import nmpi
import codecs

DEFAULT_SCRIPT_NAME = "run.py {system}"
DEFAULT_PYNN_VERSION = "0.7"
MAX_LOG_SIZE = 10000

logger = logging.getLogger("NMPI")

# status functions
def job_pending(nmpi_job, saga_job):
    nmpi_job['status'] = "submitted"
    nmpi_job['log'] += "Job ID: {}\n".format(saga_job.id)
    nmpi_job['log'] += "{}    pending\n".format(datetime.now().isoformat())
    return nmpi_job


def job_running(nmpi_job, saga_job):
    nmpi_job['status'] = "running"
    nmpi_job['log'] += "{}    running\n".format(datetime.now().isoformat())
    return nmpi_job


def _truncate(stream):
    # todo: where we truncate, should save the entire log to file
    if len(stream) > MAX_LOG_SIZE:
        return stream[:MAX_LOG_SIZE//2] + "\n\n... truncated...\n\n" + stream[-MAX_LOG_SIZE//2:]
    else:
        return stream


def job_done(nmpi_job, saga_job):
    nmpi_job['status'] = "finished"
    timestamp = datetime.now().isoformat()
    nmpi_job['timestamp_completion'] = timestamp
    nmpi_job['log'] += "{}    finished\n".format(datetime.now().isoformat())
    stdout, stderr = read_output(saga_job)
    nmpi_job['log'] += "\n\n"
    nmpi_job['log'] += _truncate(stdout)
    nmpi_job['log'] += "\n\n"
    nmpi_job['log'] += _truncate(stderr)
    return nmpi_job


def job_failed(nmpi_job, saga_job):
    nmpi_job['status'] = "error"
    nmpi_job['log'] += "{}    failed\n\n".format(datetime.now().isoformat())
    stdout, stderr = read_output(saga_job)
    nmpi_job['log'] += _truncate(stderr)
    nmpi_job['log'] += "\n\nstdout\n------\n\n"
    nmpi_job['log'] += _truncate(stdout)
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
            if not line.startswith('#') and len(line) >= 5:
                (key, val) = line.split('=')
                conf[key.strip()] = val.strip()
    for key, val in conf.items():
        if val in ("True", "False", "None"):
            conf[key] = eval(val)
    logger.debug("Loaded configuration file '{}' with contents: {}".format(fullpath, conf))
    return conf


class HardwareClient(nmpi.Client):
    """
    Client for interacting from a specific hardware, with the Neuromorphic Computing Platform of the Human Brain Project.

    This includes submitting jobs, tracking job status, retrieving the results of completed jobs,
    and creating and administering projects.

    Arguments
    ---------

    username, password : credentials for accessing the platform
    entrypoint : the base URL of the platform. Generally the default value should be used.

    """

    def __init__(self, username, platform, token, entrypoint="https://www.hbpneuromorphic.eu/api/v1/", verify=True):
        nmpi.Client.__init__(self, username, password=None, entrypoint=entrypoint, token=token, verify=verify)
        self.platform = platform

    def get_next_job(self):
        """
        Get the next job by oldest date in the queue.
        """
        job_nmpi = self._query(self.resource_map["queue"] + "/submitted/next/" + self.platform + "/")
        if 'warning' in job_nmpi:
            job_nmpi = None
        return job_nmpi

    def update_job(self, job):
        return self._put(job["resource_uri"], job)

    def reset_job(self, job):
        """
        If a job is stuck in the "running" state due to a problem on the backend,
        reset its status to "submitted".
        """
        job["status"] = "submitted"
        job["log"] += "reset status to 'submitted'\n"
        return self._put(job["resource_uri"], job)


# adapted from Sumatra
def _find_new_data_files(root, timestamp,
                         ignoredirs=[".smt", ".hg", ".svn", ".git", ".bzr"],
                         ignore_extensions=[".pyc"]):
    """Finds newly created/changed files in root."""
    length_root = len(root) + len(path.sep)
    new_files = []
    for root, dirs, files in os.walk(root):
        for igdir in ignoredirs:
            if igdir in dirs:
                dirs.remove(igdir)
        for file in files:
            if path.splitext(file)[1] not in ignore_extensions:
                full_path = path.join(root, file)
                relative_path = path.join(root[length_root:], file)
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
    outfile= path.join(job_desc.working_directory, job_desc.output)
    errfile = path.join(job_desc.working_directory, job_desc.error)
    with open(outfile) as fp:
        stdout = fp.read()
    with open(errfile) as fp:
        stderr = fp.read()
    return stdout, stderr


class JobRunner(object):
    """

    """

    def __init__(self, config):
        self.config = config
        self.service = saga.job.Service(config['JOB_SERVICE_ADAPTOR'])
        self.client = HardwareClient(username=config['AUTH_USER'],
                                     token=config['AUTH_TOKEN'],
                                     entrypoint=config['NMPI_HOST'] + config['NMPI_API'],
                                     platform=config['PLATFORM_NAME'],
                                     verify=config['VERIFY_SSL'])

    def next(self):
        """
        Get the next job by oldest date in the queue, and run it.
        """
        logger.info("Retrieving next job")
        nmpi_job = self.client.get_next_job()
        if nmpi_job is None:
            logger.info("No new jobs")
            return None
        saga_job = self.run(nmpi_job)
        self._update_status(nmpi_job, saga_job, default_job_states)
        saga_job.wait()
        logger.info("Job {} completed".format(saga_job.id))
        # TODO: the script should not wait for the job to finish.
        #       Rather it should submit the job, and then check
        #       whether any previously submitted jobs have completed,
        #       and update those.
        self._handle_output_data(nmpi_job, saga_job)
        self._update_status(nmpi_job, saga_job, default_job_states)
        logger.debug("Status of completed job updated")
        return nmpi_job

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
        logger.debug("Running job {}".format(saga_job.id))
        saga_job.run()
        return saga_job

    def close(self):
        self.service.close()

    def _build_job_description(self, nmpi_job):
        """
        Construct a Saga job description based on an NMPI job description and
        the local configuration.
        """
        #    Set all relevant parameters as in http://saga-project.github.io/saga-python/doc/library/job/index.html
        #    http://saga-project.github.io/saga-python/doc/tutorial/part5.html

        job_desc = saga.job.Description()
        job_id = nmpi_job['id']
        job_desc.working_directory = path.join(self.config['WORKING_DIRECTORY'], 'job_%s' % job_id)
        # job_desc.spmd_variation    = "MPI" # to be commented out if not using MPI

        if nmpi_job['hardware_config'] is None:
            pyNN_version = DEFAULT_PYNN_VERSION
        else:
            pyNN_version = nmpi_job['hardware_config'].get("pyNN_version", DEFAULT_PYNN_VERSION)

        if pyNN_version == "0.7":
            job_desc.executable = self.config['JOB_EXECUTABLE_PYNN_7']
        elif pyNN_version == "0.8":
            job_desc.executable = self.config['JOB_EXECUTABLE_PYNN_8']
        else:
            raise ValueError("Supported PyNN versions: 0.7, 0.8. {} not supported".format(pyNN_version))

        if self.config['JOB_QUEUE'] is not None:
            job_desc.queue = self.config['JOB_QUEUE']  # aka SLURM "partition"
        script_name = nmpi_job.get("command", "")
        if not script_name:
            script_name = DEFAULT_SCRIPT_NAME
        command_line = script_name.format(system=self.config['DEFAULT_PYNN_BACKEND'])  # TODO: allow choosing backend in "hardware_config
        command_line = path.join(job_desc.working_directory, command_line)
        job_desc.arguments = command_line.split(" ")
        job_desc.output = "saga_" + str(job_id) + '.out'
        job_desc.error = "saga_" + str(job_id) + '.err'
        # job_desc.total_cpu_count
        # job_desc.number_of_processes = 1
        # job_desc.processes_per_host
        # job_desc.threads_per_process
        # job_desc.wall_time_limit = 1
        # job_desc.total_physical_memory
        logger.debug(job_desc.arguments)
        return job_desc

    def _create_working_directory(self, workdir):
        if not path.exists(workdir):
            logger.debug("Creating directory %s" % workdir)
            os.makedirs(workdir)
            logger.info("Created directory %s" % workdir)
        else:
            logger.debug("Directory %s already exists" % workdir)


    def _get_code(self, nmpi_job, job_desc):
        """
        Obtain the code and place it in the working directory.

        If the experiment description is the URL of a Git repository, try to clone it.
        If it is the URL of a zip or .tar.gz archive, download and unpack it.
        Otherwise, the experiment_description is the code: write it to a file.
        """
        url_candidate = urlparse(nmpi_job['experiment_description'])
        logger.debug("Get code: %s %s", url_candidate.netloc, url_candidate.path)
        if url_candidate.scheme and url_candidate.path.endswith((".tar.gz", ".zip", ".tgz")):
            self._create_working_directory(job_desc.working_directory)
            target = os.path.join(job_desc.working_directory, os.path.basename(url_candidate.path))
            urlretrieve(nmpi_job['experiment_description'], target)
            logger.debug("Retrieved file from {} to local target {}".format(nmpi_job['experiment_description'], target))
            if url_candidate.path.endswith((".tar.gz", ".tgz")):
                tar("xzf", target, directory=job_desc.working_directory)
            elif url_candidate.path.endswith(".zip"):
                unzip(target, d=job_desc.working_directory)
        else:
            try:
                # Check the experiment_description for a git url (clone it into the workdir) or a script (create a file into the workdir)
                # URL: use git clone
                git.clone('--recursive', nmpi_job['experiment_description'], job_desc.working_directory)
                logger.debug("Cloned repository {}".format(nmpi_job['experiment_description']))
            except (sh.ErrorReturnCode_128, sh.ErrorReturnCode):
                # SCRIPT: create file (in the current directory)
                logger.debug("The experiment_description appears to be a script.")
                self._create_working_directory(job_desc.working_directory)
                with codecs.open(job_desc.arguments[0], 'w', encoding='utf8') as job_main_script:
                    job_main_script.write(nmpi_job['experiment_description'])

    def _get_input_data(self, nmpi_job, job_desc):
        """
        Retrieve eventual additional input DataItem

        We assume that the script knows the input files are in the same folder
        """
        if 'input_data' in nmpi_job and len(nmpi_job['input_data']):
            filelist = self.client.download_data_url(nmpi_job, job_desc.working_directory, True)

    def _update_status(self, nmpi_job, saga_job, job_states):
        """Update the status of the nmpi job."""
        saga_state = saga_job.get_state()
        logger.debug("SAGA state: {}".format(saga_state))
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
        output_dir = path.join(self.config['DATA_DIRECTORY'], path.basename(job_desc.working_directory))
        logger.debug("Copying files to {}: {}".format(output_dir,
                                                     ", ".join(new_files)))
        if self.config["DATA_DIRECTORY"] != self.config["WORKING_DIRECTORY"]:
            if not path.exists(self.config['DATA_DIRECTORY']):
                os.makedirs(self.config['DATA_DIRECTORY'])
            for new_file in new_files:
                new_file_path = path.join(output_dir, new_file)
                if not os.path.exists(os.path.dirname(new_file_path)):
                    os.makedirs(os.path.dirname(new_file_path))
                shutil.copyfile(path.join(job_desc.working_directory, new_file),
                                new_file_path)
        # append the new output to the list of item data and retrieve it
        # by POSTing to the DataItem list resource
        logger.debug("Posting data items")
        for new_file in new_files:
            url = "{}/{}/{}".format(self.config["DATA_SERVER"], os.path.basename(job_desc.working_directory), new_file)
            resource_uri = self.client.create_data_item(url)
            nmpi_job['output_data'].append(resource_uri)
        # ... and PUTting to the job resource
        self.client.update_job(nmpi_job)
        logger.debug("Handling of output data complete")

def main():
    config = load_config(
        os.environ.get("NMPI_CONFIG",
                       path.join(os.getcwd(), "nmpi.cfg"))
    )
    runner = JobRunner(config)
    runner.next()
    return 0   # todo: handle exceptions


if __name__ == "__main__":
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    retcode = main()
    sys.exit(retcode)
