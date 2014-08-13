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
# All the personalization should happen in the parameter section below.

import sys
import os
import saga
import requests
import json
import time
import datetime
import zipfile
import posixpath
import urlparse 
import sh
from sh import git


#-----------------------------------------------------------------------------
# STATUS mapping

# PUT functions
def job_pending( job, desc, text ):
    job['status'] = "submitted"
    job = put_job_log( job, desc, text )
    return job

def job_running( job, desc, text ):
    job['status'] = "running"
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    job['timestamp_submission'] = st
    put_job_log( job, desc, text )
    return job

def job_done( job, desc, text ):
    job['status'] = "finished"
    job = put_job_log( job, desc, text )
    return job

def job_failed( job, desc, text ):
    job['status'] = "error"
    job = put_job_log( job, desc, text )
    return job

def put_job_log( job, desc, text ):
    #print job
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    job['timestamp_completion'] = st
    job['log'] += "\n\n" + desc + "\n-----------------\n" + st + "\n-----------------\n" + text
    try:
        r = requests.put( "http://157.136.240.232"+job['resource_uri'], data=json.dumps(job), headers={"content-type":"application/json"}, auth=("do","do") )
        print r.content
    except ValueError, ex:
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        print " \n*** Backtrace:\n %s" % ex.traceback
    return job

# STATE switch
job_states = {
    saga.job.PENDING : job_pending,
    saga.job.RUNNING : job_running,
    saga.job.DONE : job_done,
    saga.job.FAILED : job_failed,
}

#-----------------------------------------------------------------------------
# Helper functions

def zipdir(path, zip):
    for root, dirs, files in os.walk(path):
        for file in files:
            zip.write(os.path.join(root, file))

def process_error(line, stdin, process):
    print(line)
    #return True


def load_config( fullpath ):
    conf = {}
    with open(fullpath) as f:
        for line in f:
            # leave out comment as python/bash
            if not line.startswith('#') and len(line)>=5:
                (key, val) = line.split()
                conf[key] = val.strip(' \'"')
    return conf

#-----------------------------------------------------------------------------

def main():
    # set parameters
    config = load_config( "./saga.cfg" )
    # print config['AUTH_USER']
    # print config['AUTH_PASS']
    # print config['NMPI_HOST']
    # print config['NMPI_API']
    # print config['NMPI_ENDPOINT']
    # print config['NMPI_NEXT']
    # print config['NMPI_NEXTENDPOINT']
    # print config['WORK_HOST']
    # print config['WORK_DIR']
    # print config['WORK_FILE_ENDPOINT']
    # print config['JOB_EXECUTABLE']
    # print config['JOB_SERVICE_ADAPTOR']

    #-----------------------------------------------------------------------------
    # 1. it uses the nmpi api to retrieve the next nmpi_job (FIFO of nmpi_job with status='submitted')
    r = requests.get( config['NMPI_NEXTENDPOINT'], auth=( config['AUTH_USER'], config['AUTH_PASS'] ) )
    try:
        nmpi_job = json.loads(r.content)
    except ValueError, ex:
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1

    # if the request is giving a job
    if not 'id' in nmpi_job.keys():
        print " \nNo new jobs"
        return 0

    #-----------------------------------------------------------------------------
    # 2. reads the content of the nmpi_job
    print nmpi_job['status']                       #: u'submitted', 
    print nmpi_job['hardware_config']              #: u'', 
    print nmpi_job['experiment_description']       #: u'import pyNN.nest as sim\r\n\r\nsim.setup()\r\n\r\np = sim.Population(100000, sim.IF_cond_exp())\r\n\r\nsim.run(10.0)\r\n\r\np.write_data("output_data100000.pkl")\r\n\r\nsim.end()\r\n', 
    print nmpi_job['log']                          #: u'bad stuff happened'
    print nmpi_job['timestamp_submission']         #: u'2014-12-20T12:25:02.012200', 
    print nmpi_job['project']                      #: u'/api/v1/project/1/', 
    print nmpi_job['timestamp_completion']         #: u'2014-12-20T12:25:02.012200', 
    print nmpi_job['user']                         #: u'/api/v1/user/do/', 
    print nmpi_job['hardware_platform']            #: u'localhost', 
    print nmpi_job['id']                           #: 3, 
    print nmpi_job['resource_uri']                 #: u'/api/v1/queue/3/'
    # print nmpi_job['input_data'][0]['url']         #: [ {"resource_uri": "", "url": "http://example.com/input_data"}, ... ]

    #-----------------------------------------------------------------------------
    # 3. creates a folder for the nmpi_job
    # working directory in .../nmpi/job_<id>/
    job_folder_name = "job_" + str(nmpi_job['id'])
    workdir = '%s/%s/' % ( config['WORK_FILE_ENDPOINT'], job_folder_name )
    print "Workdir:",workdir
    if not os.path.exists(workdir):
        os.makedirs(workdir)
    # NOTE: This script assumes that, at the end of this step, a file called run.py exists!!!
    job_exe = "run.py" #"experiment_" + str(nmpi_job['id']) + ".py"
    job_end_exe = workdir+job_exe

    #-----------------------------------------------------------------------------
    # 4. tries to git clone a repository supplied in the nmpi_job description, if it does not work it create an executable file out
    try:
        # Check the experiment_description for a git url (clone it into the workdir) or a script (create a file into the workdir)
        # URL: use git clone
        git.clone( nmpi_job['experiment_description'], workdir ) #, _err=process_error ) 
    except sh.ErrorReturnCode_128 or sh.ErrorReturnCode:
        # SCRIPT: create file (in the current directory)
        print("NMPI: The experiment_description is not a valid URL (e.g. not a git repository, conflicting folder names). Defaulting to script ...")
        job_exe_file = open( job_end_exe, 'w' )
        job_exe_file.write( nmpi_job['experiment_description'] )
        job_exe_file.close() 

    #-----------------------------------------------------------------------------
    # 5. retrieves eventual additional input DataItem
    # nmpi is assuming that the script posted knows the input files are in the same folder
    if 'input_data' in nmpi_job and len(nmpi_job['input_data']):
        for indata in nmpi_job['input_data']:
            print indata['url']
            r = requests.get(indata['url'])
            # create input_data file
            path = urlparse.urlsplit(indata['url']).path
            print path
            filename = posixpath.basename(path)
            if len(r.content):
                inputfile = open( workdir+filename, 'wb' )
                inputfile.write( r.content )
                inputfile.close()

    #-----------------------------------------------------------------------------
    # 6. performs the simulation in a protected environment
    # ...

    #-----------------------------------------------------------------------------
    # 7. submits the job to the cluster with SAGA
    #    Set all relevant parameters as in http://saga-project.github.io/saga-python/doc/library/job/index.html
    #    http://saga-project.github.io/saga-python/doc/tutorial/part5.html
    # A job.Description object describes the executable/application and its requirements
    job_desc = saga.job.Description()
    # parameters
    job_desc.working_directory = workdir
    job_desc.spmd_variation    = "mpi" # to be commented out if not using MPI
    job_desc.executable        = config['JOB_EXECUTABLE']
    job_desc.queue             = config['JOB_QUEUE']
    job_desc.arguments         = [ job_end_exe, nmpi_job['hardware_config'] ]
    job_desc.output            = "saga_" + str(nmpi_job['id']) + '.out'
    job_desc.error             = "saga_" + str(nmpi_job['id']) + '.err'
    print job_desc

    # A job.Service object represents the resource manager.
    # 'local' adaptor to represent the local machine
    service = saga.job.Service( config['JOB_SERVICE_ADAPTOR'] )

    # A job is created on a service (resource manager) using the job description
    job = service.create_job( job_desc )

    # Run the job
    job.run()

    # Check STATUS and use again the nmpi api to PUT a status modification 
    print "Job ID    : %s" % (job.id)
    # Status
    if   job.get_state() == saga.job.PENDING :
        desc = "NMPI: job "+str(job.id)+" pending"
    elif job.get_state() == saga.job.RUNNING :
        desc = "NMPI: job "+str(job.id)+" running"
    else :
        desc = "NMPI: job "+str(job.id)+" error"
    # PUT status
    print desc
    nmpi_job = job_states[job.get_state()](nmpi_job, desc, "")


    #-----------------------------------------------------------------------------
    # 8. waits for the answer and updates the log and status of the nmpi_job
    job.wait()

    # Get some info about the job
    print "Job State : %s" % (job.state)
    print "Exitcode  : %s" % (job.exit_code)

    #-----------------------------------------------------------------------------
    # 9. zips the whole nmpi_job folder (for the moment) and adds it to the list of nmpi_job output data
    zipf = zipfile.ZipFile( workdir+job_folder_name+'.zip', 'w', zipfile.ZIP_STORED, True ) # allowZip64
    zipdir( workdir, zipf )
    zipf.close()
    # append the new output to the list of item data and retrieve it 
    # by POSTing to the DataItem list resource 
    try:
      r = requests.post( config['NMPI_ENDPOINT']+"/api/v1/dataitem/", data=json.dumps({"url": workdir+job_folder_name+'.zip'}), headers={"content-type":"application/json"}, auth=AUTH )
    except ValueError, ex:
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1
    else:
        # ... and PUTting to the job resource
        if 'output_data' in nmpi_job and isinstance(nmpi_job['output_data'], (list, tuple)) :
            nmpi_job['output_data'].append( {"url": workdir+job_folder_name+'.zip'} )
        else:
            nmpi_job['output_data'] = [ {"url": workdir+job_folder_name+'.zip'} ]
        put_job_log( nmpi_job, "NMPI log", "Added output_data in the working directory" )

    #-----------------------------------------------------------------------------
    # 10. final nmpi_job status modification to 'finished' or 'error'
    if   job.get_state() == saga.job.DONE :
        desc = "NMPI: job "+str(job.id)+" finished"
        with open (workdir+job_desc.output, "r") as outfile:
            outtext = outfile.read()
    elif job.get_state() == saga.job.FAILED :
        desc = "NMPI: job "+str(job.id)+" error"
        with open (workdir+job_desc.error, "r") as outfile:
            outtext = outfile.read()
    else :
        desc = "NMPI: job "+str(job.id)+" error"
        outtext = "code:"+str(job.exit_code)
    # PUT status
    print desc
    nmpi_job = job_states[job.get_state()](nmpi_job, desc, outtext)

    service.close()

    return 0


if __name__ == "__main__":
    sys.exit( main() )