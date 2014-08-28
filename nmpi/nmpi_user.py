"""
Client for interacting with the Neuromorphic Computing Platform of the Human Brain Project.

Authors: Andrew P. Davison, Domenico Guarino, UNIC, CNRS
Copyright 2014

"""

import os.path
import json
from urlparse import urlparse
from urllib import urlretrieve
import requests
import time
import datetime


class Client(object):
    """
    Client for interacting with the Neuromorphic Computing Platform of the Human Brain Project.

    This includes submitting jobs, tracking job status, retrieving the results of completed jobs,
    and creating and administering projects.

    Arguments
    ---------

    username, password : credentials for accessing the platform
    entrypoint : the base URL of the platform. Generally the default value should be used.

    """

    def __init__(self, username, password, entrypoint="http://127.0.0.1:8000/api/v1/"):
        self.auth = (username, password)
        (scheme, netloc, path, params, query, fragment) = urlparse(entrypoint)
        self.server = "%s://%s" % (scheme, netloc)
        req = requests.get(entrypoint)
        if req.ok:
            self.resource_map = {name: entry["list_endpoint"] for name, entry in req.json().items()}
        else:
            self._handle_error(req)

    def _handle_error(self, request):
        """
        Deal with requests that return an error code (404, 500, etc.)
        """
        try:
            errmsg = request.json()["error_message"]
        except ValueError:
            errmsg = request.content
        raise Exception("Error %s: %s" % (request.status_code, errmsg))

    def _query(self, resource_uri, verbose=False):
        """
        Retrieve a resource or list of resources.
        """
        req = requests.get(self.server + resource_uri, auth=self.auth)
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
                            headers={"content-type": "application/json"})
        if not req.ok:
            self._handle_error(req)
        return req.json()

    def _put(self, resource_uri, data, log_description, log_text):
        """
        Updates a resource (with desc).
        """
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S')
        data['timestamp_completion'] = unicode(st)   # not sure this should go here, could be other updates possible
        data['log'] += "\n\n" + log_description + "\n-----------------\n" + st + "\n-----------------\n" + log_text
        req = requests.put(self.server + resource_uri,
                           data=json.dumps(data),
                           auth=self.auth,
                           headers={"content-type": "application/json"})
        if not req.ok:
            self._handle_error(req)
        return data

    def create_project(self, short_name, full_name=None, description=None, members=None):
        """
        Create a new project.

        Arguments
        ---------

        short_name : a unique identifier for the project, consisting of only letters, numbers and underscores.
        full_name : (optional) a longer name for the project, may contain spaces.
        description : (optional) a detailed description of the project.
        members : (optional) a list of usernames allowed to access the project (by default the current user is in the list).
        """
        if members == None:
            members = [ self.auth.username ]
        project = {
            "short_name": short_name,
            "full_name": full_name,
            "description": description,
            "members": members
        }
        self._post(self.resource_map["project"], project)
        print("Project %s created" % short_name)

    def get_project(self, project_uri):
        """
        Retrieve data about a project, given its URI.

        If you know the project name but not its URI, use ``c.get_project(c.get_project_uri(name))``.
        """
        req = requests.get(self.server + project_uri, auth=self.auth)
        if req.ok:
            return req.json()
        else:
            self._handle_error(req)

    def get_project_uri(self, project_name):
        """
        Obtain the URI of a project given its short name.
        """
        for project in self._query(self.resource_map["project"], verbose=True):
            if project_name == project["short_name"]:
                return project["resource_uri"]
        print("Project '%s' not found." % project_name)
        return None

    def list_projects(self, verbose=False):
        """
        Retrieve a list of the projects to which you have access.
        """
        return self._query(self.resource_map["project"], verbose=verbose)

    def submit_job(self, source, platform, project, config=None, inputs=None):
        """

        """
        project_uri = self.get_project_uri(project)
        if project_uri is None:
            raise Exception("Project '%s' does not exist. You must first create it." % project)
        if os.path.exists(source):
            with open(source, "rb") as fp:
                source_code = fp.read()
        else:
            source_code = source
        job = {
            'experiment_description': source_code,
            'hardware_platform': platform,
            'project': project_uri,
            'user': '/api/v1/user/' + self.auth[0]
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

        """
        return self.get_job(job_id)["status"]

    def get_job(self, job_id):
        """

        """
        for resource_type in ("queue", "results"):
            for job in self._query(self.resource_map[resource_type], verbose=True):
                if job["id"] == job_id:
                    return job
        raise Exception("No such job: %s" % job_id)

    def queued_jobs(self, project_name=None, verbose=False):
        """

        """
        return self._query(self.resource_map["queue"] + "/submitted/", verbose=verbose)

    def completed_jobs(self, project_name=None, verbose=False):
        """

        """
        return self._query(self.resource_map["results"], verbose=verbose)

    def download_data_url(self, job, local_dir=".", include_input_data=False):
        """

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
        data_item = {"url": url}
        result = self._post(self.resource_map["dataitem"], data_item)
        return result["resource_uri"]


class HardwareClient(Client):
    """
    Client for interacting from a specific hardware, with the Neuromorphic Computing Platform of the Human Brain Project.

    This includes submitting jobs, tracking job status, retrieving the results of completed jobs,
    and creating and administering projects.

    Arguments
    ---------

    username, password : credentials for accessing the platform
    entrypoint : the base URL of the platform. Generally the default value should be used.

    """

    def __init__(self, username, password, entrypoint, platform):
        Client.__init__(self, username, password, entrypoint)
        self.platform = platform

    def get_next_job(self):
        """
        Get the nex job by oldest date in the queue.
        """
        print self.platform
        job = self._query(self.resource_map["queue"] + "/submitted/next/"+self.platform+"/", verbose=False)
        return job
        raise Exception("No pending jobs")
