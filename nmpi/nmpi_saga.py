#############################################
# JOB request (using NMPI API) and execution (using SAGA)
#
# 0. This script is called by a cron job
# 1. it uses the nmpi api to retrieve the next nmpi_job (FIFO of nmpi_job with status='submitted')
# 2. reads the content of the nmpi_job
# 3. creates a folder for the nmpi_job
# 4. tries to git clone a repository supplied in the nmpi_job description, if it does not work it create an executable file out
# 5. retrieves eventual additional input data
# 6. performs the simulation in a protected environment
# 7. submits the job to the cluster with SAGA using a unique account 'nmpi'
# 8. waits for the answer and updates the log and status of the nmpi_job
# 9. zips the whole nmpi_job folder and adds it to the list of nmpi_job output data
# 10. final nmpi_job status modification to 'finished' or 'error'
#
# Authors: Domenico Guarino,
#          Andrew Davison
#
# All the personalization should happen in the config file.

import sys
import os
import logging
import saga
import time
import datetime
import posixpath
import urlparse
import sh
from sh import git

from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED

#sys.path.append('./nmpi_client')
import nmpi

DEFAULT_SCRIPT_NAME = "run.py"


logger = logging.getLogger("NMPI")


#-----------------------------------------------------------------------------
# Helper functions
#-----------------------------------------------------------------------------

#-----------------------------
# status functions
def job_pending(job):
    job['status'] = "submitted"
    return job


def job_running(job):
    job['status'] = "running"
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S')
    job['timestamp_submission'] = unicode(st)
    return job


def job_done(job):
    job['status'] = "finished"
    return job


def job_failed(job):
    job['status'] = "error"
    return job


# states switch
job_states = {
    saga.job.PENDING: job_pending,
    saga.job.RUNNING: job_running,
    saga.job.DONE: job_done,
    saga.job.FAILED: job_failed,
}

#-----------------------------------


def zipdir(basedir, archivename):
    assert os.path.isdir(basedir)
    with closing(ZipFile(archivename, "w", ZIP_DEFLATED)) as z:
        for root, dirs, files in os.walk(basedir):
            # NOTE: ignore empty directories
            for fn in files:
                absfn = os.path.join(root, fn)
                zfn = absfn[len(basedir) + len(os.sep):]  # XXX: relative path
                z.write(absfn, zfn)


def process_error(line, stdin, process):
    print(line)
    #return True


def load_config(fullpath):
    conf = {}
    with open(fullpath) as f:
        for line in f:
            # leave out comment as python/bash
            if not line.startswith('#') and len(line) >= 5:
                (key, val) = line.split('=')
                conf[key] = val.strip()
    for key, val in conf.items():
        if val in ("True", "False", "None"):
            conf[key] = eval(val)
    logger.info("Loaded configuration file with contents: %s" % conf)
    return conf


#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------



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


#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------


def build_job_description(nmpi_job, config):
    """
    Construct a Saga job description based on an NMPI job description and the local configuration.
    """
    #    Set all relevant parameters as in http://saga-project.github.io/saga-python/doc/library/job/index.html
    #    http://saga-project.github.io/saga-python/doc/tutorial/part5.html
    # A job.Description object describes the executable/application and its requirements
    job_desc = saga.job.Description()
    job_id = nmpi_job['id']
    job_desc.working_directory = os.path.join(config['WORKING_DIRECTORY'], 'job_%s' % job_id)
    # job_desc.spmd_variation    = "MPI" # to be commented out if not using MPI
    pyNN_version = nmpi_job['hardware_config'].get("pyNN_version", "0.7")
    if pyNN_version == "0.7":
        job_desc.executable = config['JOB_EXECUTABLE_PYNN_7']
    elif pyNN_version == "0.8":
        job_desc.executable = config['JOB_EXECUTABLE_PYNN_8']
    else:
        raise ValueError("Supported PyNN versions: 0.7, 0.8. {} not supported".format(pyNN_version))
    if config['JOB_QUEUE'] is not None:
        job_desc.queue = config['JOB_QUEUE']  # aka SLURM "partition"
    job_desc.arguments = [os.path.join(job_desc.working_directory, DEFAULT_SCRIPT_NAME),
                          config['DEFAULT_PYNN_BACKEND']]  # TODO: allow choosing backend in "hardware_config
    job_desc.output = "saga_" + str(job_id) + '.out'
    job_desc.error = "saga_" + str(job_id) + '.err'
    # job_desc.total_cpu_count
    # job_desc.number_of_processes = 1
    # job_desc.processes_per_host
    # job_desc.threads_per_process
    # job_desc.wall_time_limit = 1
    # job_desc.total_physical_memory
    return job_desc


def run_job(job_desc, service):
    """Submits a job to the cluster with SAGA."""
    # A job is created on a service (resource manager) using the job description
    saga_job = service.create_job(job_desc)
    # Run the job
    saga_job.run()
    return saga_job


def get_client(config):
    hc = HardwareClient(username=config['AUTH_USER'],
                        token=config['AUTH_TOKEN'],
                        entrypoint=config['NMPI_HOST'] + config['NMPI_API'],
                        platform=config['PLATFORM_NAME'],
                        verify=config['VERIFY_SSL'])
    return hc


def get_next_job(hc):
    """
    Uses the nmpi api to retrieve the next nmpi_job (FIFO of nmpi_job with status='submitted')
    """
    try:
        nmpi_job = hc.get_next_job()
    except ValueError as ex:
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        raise

    # if the request is giving a job
    if (not nmpi_job) or (not 'id' in nmpi_job.keys()):
        print " \nNo new jobs"
        return None
    else:
        return nmpi_job


def create_working_directory(job_desc):
    """
    Create a folder for the nmpi_job

    Working directory in .../nmpi/job_<id>/
    """
    workdir = job_desc.working_directory
    if not os.path.exists(workdir):
        logger.info("Creating directory %s" % workdir)
        os.makedirs(workdir)
        logger.debug("Created directory %s" % workdir)
    else:
        logger.debug("Directory %s already exists" % workdir)


def get_code(nmpi_job, job_desc):
    """
    Obtain the code and place it in the working directory.

    If the experiment description is the URL of a Git repository, try to clone it.
    Otherwise, the experiment_description is the code: write it to a file.
    """
    try:
        # Check the experiment_description for a git url (clone it into the workdir) or a script (create a file into the workdir)
        # URL: use git clone
        git.clone(nmpi_job['experiment_description'], job_desc.working_directory)
    except sh.ErrorReturnCode_128 or sh.ErrorReturnCode:
        # SCRIPT: create file (in the current directory)
        print("NMPI: The experiment_description is not a valid URL (e.g. not a git repository, conflicting folder names). Defaulting to script ...")
        create_working_directory(job_desc)
        job_main_script = open(job_desc.arguments[0], 'w')
        job_main_script.write(nmpi_job['experiment_description'])
        job_main_script.close()


def get_data(hc, nmpi_job):
    """
    Retrieve eventual additional input DataItem

    We assume that the script knows the input files are in the same folder
    """
    if 'input_data' in nmpi_job and len(nmpi_job['input_data']):
        filelist = hc.download_data_url(nmpi_job, ".", True)  # shouldn't this be downloading to working directory, not "."?


def update_status(hc, saga_job, nmpi_job):
    # Check STATUS and use again the nmpi api to PUT a status modification
    print "Job ID    : %s" % (saga_job.id)
    # Status
    if saga_job.get_state() == saga.job.PENDING:
        desc = "NMPI: job " + str(saga_job.id) + " pending"
    elif saga_job.get_state() == saga.job.RUNNING:
        desc = "NMPI: job " + str(saga_job.id) + " running"
    else:
        desc = "NMPI: job " + str(saga_job.id) + " error"
    # PUT status
    nmpi_job = job_states[saga_job.get_state()](nmpi_job)
    # update remote resource
    hc._put(nmpi_job['resource_uri'], nmpi_job, desc, "")
    return nmpi_job


def handle_output_data(hc, config, job_desc, nmpi_job):
    """
    zips the whole nmpi_job folder (for the moment) and adds it to the list of nmpi_job output data
    """
    # can we perhaps add the zipname to the job_desc earlier in the process, to avoid passing config here
    if not os.path.exists(config['DATA_DIRECTORY']):
        os.makedirs(config['DATA_DIRECTORY'])
    zipname = os.path.join(config['DATA_DIRECTORY'],  os.path.basename(job_desc.working_directory) + '.zip')
    zipdir(job_desc.working_directory, zipname)
    # append the new output to the list of item data and retrieve it
    # by POSTing to the DataItem list resource
    uri = hc.create_data_item(zipname)
    if uri:
        # ... and PUTting to the job resource
        if 'output_data' in nmpi_job and isinstance(nmpi_job['output_data'], (list, tuple)):
            nmpi_job['output_data'].append({"url": zipname})
        else:
            nmpi_job['output_data'] = [{"url": zipname}]  # workdir+job_folder_name+'.zip'}]
        hc._put(nmpi_job['resource_uri'], nmpi_job, "NMPI log", "Added output_data in the working directory")


def update_final_service(hc, job, nmpi_job, job_desc):
    if job.get_state() == saga.job.DONE:
        desc = "NMPI: job " + str(job.id) + " finished"
        with open(os.path.join(job_desc.working_directory, job_desc.output), "r") as outfile:
            outtext = outfile.read()
    elif job.get_state() == saga.job.FAILED:
        desc = "NMPI: job " + str(job.id) + " error"
        with open(os.path.join(job_desc.working_directory, job_desc.error), "r") as outfile:
            outtext = outfile.read()
    else:
        desc = "NMPI: job " + str(job.id) + " error"
        outtext = "code:" + str(job.exit_code)
    # PUT status
    nmpi_job = job_states[job.get_state()](nmpi_job)
    hc._put(nmpi_job['resource_uri'], nmpi_job, desc, outtext)


def main():
    # set parameters
    config = load_config(os.environ.get("NMPI_CONFIG", "./nmpi.cfg"))

    hc = get_client(config)

    # A job.Service object represents the resource manager.
    service = saga.job.Service(config['JOB_SERVICE_ADAPTOR'])

    #-----------------------------------------------------------------------------
    # 1. it uses the nmpi api to retrieve the next nmpi_job (FIFO of nmpi_job with status='submitted')
    #try:
    nmpi_job = get_next_job(hc)
    #except Exception as err:
    #    print(err)
    #    return -1
    if nmpi_job is None:
        return 0

    #-----------------------------------------------------------------------------
    # 2. create a Saga job description
    print nmpi_job['resource_uri']                 #: u'/api/v1/queue/3/'
    job_desc = build_job_description(nmpi_job, config)

    #-----------------------------------------------------------------------------
    # 3. creates a folder for the nmpi_job
    # working directory in .../nmpi/job_<id>/
    create_working_directory(job_desc)

    #-----------------------------------------------------------------------------
    # 4. tries to git clone a repository supplied in the nmpi_job description, if it does not work it create an executable file out
    get_code(nmpi_job, job_desc)

    #-----------------------------------------------------------------------------
    # 5. retrieves eventual additional input DataItem
    # nmpi is assuming that the script posted knows the input files are in the same folder
    get_data(hc, nmpi_job)

    #-----------------------------------------------------------------------------
    # 6. performs the simulation in a protected environment
    # ...

    #-----------------------------------------------------------------------------
    # 7. submits the job to the cluster with SAGA

    saga_job = run_job(job_desc, service)

    nmpi_job = update_status(hc, saga_job, nmpi_job)

    #-----------------------------------------------------------------------------
    # 8. waits for the answer and updates the log and status of the nmpi_job
    saga_job.wait()
    # TODO: the script should not wait for the job to finish. Rather it should submit the job, and then check
    #       whether any previously submitted jobs have completed, and update those.


    # Get some info about the job
    print "Job State : %s" % (saga_job.state)
    #print "Exitcode  : %s" % (job.exit_code)

    #-----------------------------------------------------------------------------
    # 9. zips the whole nmpi_job folder (for the moment) and adds it to the list of nmpi_job output data

    handle_output_data(hc, config, job_desc, nmpi_job)

    #-----------------------------------------------------------------------------
    # 10. final nmpi_job status modification to 'finished' or 'error'

    update_final_service(hc, saga_job, nmpi_job, job_desc)

    service.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
