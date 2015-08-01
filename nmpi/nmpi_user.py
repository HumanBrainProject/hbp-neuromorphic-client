"""
Client for interacting with the Neuromorphic Computing Platform of the Human Brain Project.

Authors: Andrew P. Davison, Domenico Guarino, UNIC, CNRS
Copyright 2014

"""

import os.path
import json
import getpass
try:
    from urlparse import urlparse
    from urllib import urlretrieve
except ImportError:  # Py3
    from urllib.parse import urlparse
    from urllib.request import urlretrieve

import requests
from requests.auth import AuthBase

import time
import datetime


class NMPAuth(AuthBase):
    """Attaches ApiKey Authentication to the given Request object."""

    def __init__(self, username, token):
        # setup any auth-related data here
        self.username = username
        self.token = token

    def __call__(self, r):
        # modify and return the request
        r.headers['Authorization'] = 'ApiKey ' + self.username + ":" + self.token
        return r


class Client(object):
    """
    Client for interacting with the Neuromorphic Computing Platform of the Human Brain Project.

    This includes submitting jobs, tracking job status, retrieving the results of completed jobs,
    and creating and administering projects.

    Arguments
    ---------

    username, password : credentials for accessing the platform
    entrypoint : the base URL of the platform. Generally the default value
                 should be used.
    token : when you authenticate with username and password, you will receive
            a token which can be used in place of the password until it expires.
    verify : in case of problems with SSL certificate verification, you can
             set this to False, but this is not recommended.
    """

    def __init__(self, username,
                 password=None,
                 entrypoint="https://www.hbpneuromorphic.eu/api/v1/",
                 token=None,
                 verify=True):
        if password is None and token is None:
            # prompt for password
            password = getpass.getpass()
        self.username = username
        self.cert = None
        self.verify = verify
        self.token = token
        (scheme, netloc, path, params, query, fragment) = urlparse(entrypoint)
        self.server = "%s://%s" % (scheme, netloc)
        # if a token has been given, no need to authenticate
        if not self.token:
            self._hbp_auth(username, password)
        self.auth = NMPAuth(username, self.token)
        # get schema
        req = requests.get(entrypoint, cert=self.cert, verify=self.verify, auth=self.auth)
        if req.ok:
            self.resource_map = {name: entry["list_endpoint"]
                                 for name, entry in req.json().items()}
        else:
            self._handle_error(req)

    def _hbp_auth(self, username, password):
        """
        """
        client_id = r'nmpi'
        client_secret = r'b8IMyR-dd-qR6k3VAbHRYYAngKySClc9olDr084HpDmr1fjtx6TMHUwjpBnKcZc2uQfIU3BAAJplhoH42BsiyQ'
        redirect_uri = self.server + '/complete/hbp-oauth2/'

        self.session = requests.Session()
        # 1. login button on NMPI
        rNMPI1 = self.session.get(self.server + "/login/hbp-oauth2/?next=/",
                                  allow_redirects=False, verify=True)
        # 2. receives a redirect
        if rNMPI1.status_code == 302:
            # Get its new destination (location)
            url = rNMPI1.headers.get('location')
            # https://services.humanbrainproject.eu/oidc/authorize?
            #   scope=openid%20profile
            #   state=jQLERcgK1xTDHcxezNYnbmLlXhHgJmsg
            #   redirect_uri=https://neuromorphic.humanbrainproject.eu/complete/hbp-oauth2/
            #   response_type=code
            #   client_id=nmpi
            # get the exchange cookie
            cookie = rNMPI1.headers.get('set-cookie').split(";")[0]
            self.session.headers.update({'cookie': cookie})
            # 3. request to the provided url at HBP
            rHBP1 = self.session.get(url, allow_redirects=False, verify=True)
            # 4. receives a redirect to HBP login page
            if rHBP1.status_code == 302:
                # Get its new destination (location)
                url = rHBP1.headers.get('location')
                cookie = rHBP1.headers.get('set-cookie').split(";")[0]
                self.session.headers.update({'cookie': cookie})
                # 5. request to the provided url at HBP
                rHBP2 = self.session.get(url, allow_redirects=False, verify=True)
                # 6. HBP responds with the auth form
                if rHBP2.text:
                    # 7. Request to the auth service url
                    formdata = {
                        'j_username': username,
                        'j_password': password,
                        'submit': 'Login',
                        'redirect_uri': redirect_uri + '&response_type=code&client_id=nmpi'
                    }
                    headers = {'accept': 'application/json'}
                    rNMPI2 = self.session.post("https://services.humanbrainproject.eu/oidc/j_spring_security_check",
                                               data=formdata,
                                               allow_redirects=True,
                                               verify=True,
                                               headers=headers)
                    # check good communication
                    if rNMPI2.status_code == requests.codes.ok:
                        # check success address
                        if rNMPI2.url == self.server + '/':
                            # print rNMPI2.text
                            res = rNMPI2.json()
                            self.token = res['access_token']
                        # unauthorized
                        else:
                            if 'error' in rNMPI2.url:
                                raise Exception("Authentication Failure: No token retrieved.")
                            else:
                                raise Exception("Unhandled error in Authentication.")
                    else:
                        raise Exception("Communication error")
                else:
                    raise Exception("Something went wrong. No text.")
            else:
                raise Exception("Something went wrong. Status code {} from HBP, expected 302".format(rHBP1.status_code))
        else:
            raise Exception("Something went wrong. Status code {} from NMPI, expected 302".format(rNMPI1.status_code))


    def _handle_error(self, request):
        """
        Deal with requests that return an error code (404, 500, etc.)
        """
        try:
            errmsg = request.json()["error_message"]
        except KeyError:
            errmsg = request.json()["error"]
        except ValueError:
            errmsg = request.content
        raise Exception("Error %s: %s" % (request.status_code, errmsg))

    def _query(self, resource_uri, verbose=False):
        """
        Retrieve a resource or list of resources.
        """
        req = requests.get(self.server + resource_uri, auth=self.auth,
                           cert=self.cert, verify=self.verify)
        if req.ok:
            if "objects" in req.json():
                objects = req.json()["objects"]
                if verbose:
                    return objects
                else:
                    return [obj["resource_uri"] for obj in objects]
            else:
                return req.json()
        else:
            self._handle_error(req)

    def _post(self, resource_uri, data):
        """
        Create a new resource.
        """
        req = requests.post(self.server + resource_uri,
                            data=json.dumps(data),
                            auth=self.auth,
                            cert=self.cert, verify=self.verify,
                            headers={"content-type": "application/json"})
        if not req.ok:
            self._handle_error(req)
        return req.json()

    def _put(self, resource_uri, data):
        """
        Updates a resource.
        """
        req = requests.put(self.server + resource_uri,
                           data=json.dumps(data),
                           auth=self.auth,
                           cert=self.cert, verify=self.verify,
                           headers={"content-type": "application/json"})
        if not req.ok:
            self._handle_error(req)
        return data

    def _delete(self, resource_uri):
        """
        Deletes a resource
        """
        req = requests.delete(self.server + resource_uri,
                              auth=self.auth,
                              cert=self.cert, verify=self.verify)
        if not req.ok:
            self._handle_error(req)

    def create_project(self, short_name, full_name=None, description=None, members=None):
        """
        Create a new project.

        Arguments
        ---------

        short_name : a unique identifier for the project, consisting of only
                     letters, numbers and underscores.
        full_name : (optional) a longer name for the project, may contain spaces.
        description : (optional) a detailed description of the project.
        members : (optional) a list of usernames allowed to access the project
                  (by default the current user has access).
        """
        if members is None:
            members = []
        project = {
            "short_name": short_name,
            "full_name": full_name,
            "description": description,
            "members": [{"username": member, "resource_uri": self.resource_map["user"] + "/" + member}
                        for member in members]
        }
        res = self._post(self.resource_map["project"], project)
        if isinstance(res, dict):
            print("Project %s created" % short_name)
        return res

    def get_project(self, project_uri):
        """
        Retrieve data about a project, given its URI.

        If you know the project name but not its URI,
        use ``c.get_project(c.get_project_uri(name))``.
        """
        req = requests.get(self.server + project_uri, auth=self.auth, cert=self.cert, verify=self.verify)
        if req.ok:
            return req.json()
        else:
            self._handle_error(req)

    def get_project_uri(self, project_name):
        """
        Obtain the URI of a project given its short name.
        """
        for project in self._query( self.resource_map["project"], verbose=True ):
            if project_name == project["short_name"]:
                return project["resource_uri"]
        print("Project '%s' not found." % project_name)
        return None

    def update_project(self, project_uri, full_name=None, description=None):
        project = self.get_project(project_uri)
        if full_name is not None:
            project["full_name"] = full_name
        if description is not None:
            project["description"] = description
        res = self._put(project_uri, project)

    def add_project_member(self, project_uri, member):
        raise NotImplementedError

    def delete_project_member(self, project_uri, member):
        raise NotImplementedError

    def list_projects(self, verbose=False):
        """
        Retrieve a list of the projects to which you have access.
        """
        return self._query(self.resource_map["project"], verbose=verbose)

    def submit_job(self, source, platform, project, config=None, inputs=None):
        """
        Submit a job to the platform.

        Arguments:

        source : the Python script to be run, the URL of a public version
                 control repository containing a file "run.py" at the top
                 level, or a zip file containing a file "run.py" at the top
                 level.
        platform : the neuromorphic hardware system to be used.
                   Either "hbp-pm-1" or "hbp-mc-1"
        project : the name of the project to which the job belongs
        config : (optional) a dictionary containing configuration information
                 for the hardware platform. See the Platform Guidebook for
                 more details.
        inputs : a list of URLs for datafiles needed as inputs to the
                 simulation.
        """
        project_uri = self.get_project_uri(project)
        if project_uri is None:
            raise Exception("Project '%s' does not exist. You must first create it." % project)
        if os.path.exists(source):
            with open(source, "r") as fp:
                source_code = fp.read()
        else:
            source_code = source
        job = {
            'experiment_description': source_code,
            'hardware_platform': platform,
            'project': project_uri,
            'user': self.resource_map["user"] + "/" + self.username
        }

        if inputs is not None:
            job['input_data'] = [self.create_data_item(input) for input in inputs]
        if config is not None:
            job['hardware_config'] = config
        result = self._post(self.resource_map["queue"], job)
        print("Job submitted")
        return result["id"]

    def job_status(self, job_id):
        """
        Return the current status of the job with ID `job_id` (integer).
        """
        return self.get_job(job_id)["status"]

    def get_job(self, job_id):
        """
        Return full details of the job with ID `job_id` (integer).
        """
        for resource_type in ("queue", "results"):
            for job in self._query(self.resource_map[resource_type], verbose=True):
                if job["id"] == job_id:
                    return job
        raise Exception("No such job: %s" % job_id)

    def remove_completed_job(self, job_id):
        """
        Remove a job from the interface.

        The job is hidden rather than being permanently deleted.
        """
        self._delete("{}/{}".format(self.resource_map["results"], job_id))

    def remove_queued_job(self, job_id):
        """
        Remove a job from the interface.

        The job is hidden rather than being permanently deleted.
        """
        self._delete("{}/{}".format(self.resource_map["queue"], job_id))

    def queued_jobs(self, verbose=False):
        """
        Return the list of jobs belonging to the current user in the queue.

        Arguments
        ---------

        verbose : if False, return just the job URIs,
                  if True, return full details.
        """
        return self._query(self.resource_map["queue"] + "/submitted/", verbose=verbose)

    def completed_jobs(self, verbose=False):
        """
        Return the list of completed jobs belonging to the current user.

        Arguments
        ---------

        verbose : if False, return just the job URIs,
                  if True, return full details.
        """
        # todo: add kwargs `project_name` to allow filtering of the jobs
        return self._query(self.resource_map["results"], verbose=verbose)

    def download_data_url(self, job, local_dir=".", include_input_data=False):
        """
        Download output data files produced by a given job to a local directory.

        Arguments
        ---------

        job : a full job description (dict), as returned by `get_job()`.
        local_dir : path to a directory into which files shall be saved.
        include_input_data : also download input data files.
        """
        filenames = []
        datalist = job["output_data"]
        if include_input_data:
            datalist.extend(job["input_data"])
        for dataitem in datalist:
            url = dataitem["url"]
            (scheme, netloc, path, params, query, fragment) = urlparse(url)
            if not scheme:
                url = "file://" + url
            local_filename = os.path.join(local_dir, os.path.split(path)[1])
            urlretrieve(url, local_filename)
            filenames.append(local_filename)
        return filenames

    def create_data_item(self, url):
        """
        Register a data item with the platform.
        """
        data_item = {"url": url}
        result = self._post(self.resource_map["dataitem"], data_item)
        return result["resource_uri"]
