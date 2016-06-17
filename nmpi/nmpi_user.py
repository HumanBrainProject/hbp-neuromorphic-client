"""
Client for interacting with the Neuromorphic Computing Platform of the Human Brain Project.

Authors: Andrew P. Davison, Domenico Guarino, UNIC, CNRS


Copyright 2016 Andrew P. Davison and Domenico Guarino, Centre National de la Recherche Scientifique

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os.path
import json
import getpass
import logging
try:
    from urlparse import urlparse
    from urllib import urlretrieve
except ImportError:  # Py3
    from urllib.parse import urlparse
    from urllib.request import urlretrieve
import errno
import requests
from requests.auth import AuthBase

logger = logging.getLogger("NMPI")

IDENTITY_SERVICE = "https://services.humanbrainproject.eu/idm/v1/api"
COLLAB_SERVICE = "https://services.humanbrainproject.eu/collab/v0"


class HBPAuth(AuthBase):
    """Attaches OIDC Bearer Authentication to the given Request object."""

    def __init__(self, token):
        # setup any auth-related data here
        self.token = token

    def __call__(self, r):
        # modify and return the request
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r


def _mkdir_p(path):
    # not needed in Python >= 3.2, use os.makedirs(path, exist_ok=True)
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class Client(object):
    """
    Client for interacting with the Neuromorphic Computing Platform of
    the Human Brain Project.

    This includes submitting jobs, tracking job status and retrieving the
    results of completed jobs.

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
                 entrypoint="https://nmpi.hbpneuromorphic.eu/api/v2/",
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
        self.auth = HBPAuth(self.token)
        self._get_user_info()
        # get schema
        req = requests.get(entrypoint, cert=self.cert, verify=self.verify, auth=self.auth)
        if req.ok:
            self._schema = req.json()
            self.resource_map = {name: entry["list_endpoint"]
                                 for name, entry in req.json().items()}
        else:
            self._handle_error(req)

    def _hbp_auth(self, username, password):
        """
        """
        redirect_uri = self.server + '/complete/hbp/'

        self.session = requests.Session()
        # 1. login button on NMPI
        rNMPI1 = self.session.get(self.server + "/login/hbp/?next=/config.json",
                                  allow_redirects=False, verify=self.verify)
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
            rHBP1 = self.session.get(url, allow_redirects=False, verify=self.verify)
            # 4. receives a redirect to HBP login page
            if rHBP1.status_code == 302:
                # Get its new destination (location)
                url = rHBP1.headers.get('location')
                cookie = rHBP1.headers.get('set-cookie').split(";")[0]
                self.session.headers.update({'cookie': cookie})
                # 5. request to the provided url at HBP
                rHBP2 = self.session.get(url, allow_redirects=False, verify=self.verify)
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
                                               verify=self.verify,
                                               headers=headers)
                    # check good communication
                    if rNMPI2.status_code == requests.codes.ok:
                        # check success address
                        if rNMPI2.url == self.server + '/config.json':
                            # print rNMPI2.text
                            res = rNMPI2.json()
                            self.token = res['auth']['token']['access_token']
                            self.config = res
                        # unauthorized
                        else:
                            if 'error' in rNMPI2.url:
                                raise Exception("Authentication Failure: No token retrieved." + rNMPI2.url)
                            else:
                                raise Exception("Unhandled error in Authentication." + rNMPI2.url)
                    else:
                        raise Exception("Communication error")
                else:
                    raise Exception("Something went wrong. No text.")
            else:
                raise Exception("Something went wrong. Status code {} from HBP, expected 302".format(rHBP1.status_code))
        else:
            raise Exception("Something went wrong. Status code {} from NMPI, expected 302".format(rNMPI1.status_code))

    def _get_user_info(self):
        req = requests.get(IDENTITY_SERVICE + "/user/me",
                           auth=self.auth)
        if req.ok:
            self.user_info = req.json()
            assert self.user_info['username'] == self.username
        else:
            self._handle_error(req)

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
        logger.error(errmsg)
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
        if 'Location' in req.headers:
            return req.headers['Location']
        else:
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

    def submit_job(self, source, platform, collab_id, config=None, inputs=None,
                   command="run.py {system}"):
        """
        Submit a job to the platform.

        Arguments:

        source : the Python script to be run, the URL of a public version
                 control repository containing Python code, or a zip file
                 containing Python code.
        platform : the neuromorphic hardware system to be used.
                   Either "hbp-pm-1" or "hbp-mc-1"
        collab_id : the ID of the collab to which the job belongs
        config : (optional) a dictionary containing configuration information
                 for the hardware platform. See the Platform Guidebook for
                 more details.
        inputs : a list of URLs for datafiles needed as inputs to the
                 simulation.
        command : (optional) the path to the main Python script relative to
                  the root of the repository or zip file. Defaults to "run.py {system}".
        """
        source = os.path.expanduser(source)
        if os.path.exists(source) and os.path.splitext(source)[1] == ".py":
            with open(source, "r") as fp:
                source_code = fp.read()
        else:
            source_code = source
        job = {
            'code': source_code,
            'command': command,
            'hardware_platform': platform,
            'collab_id': collab_id,
            'user_id': self.user_info["id"]
        }

        if inputs is not None:
            job['input_data'] = [self.create_data_item(input) for input in inputs]
        if config is not None:
            job['hardware_config'] = config
        result = self._post(self.resource_map["queue"], job)
        print("Job submitted")
        return result

    def job_status(self, job_id):
        """
        Return the current status of the job with ID `job_id` (integer).
        """
        logger.debug(type(job_id))
        logger.debug(str(job_id))
        return self.get_job(job_id, with_log=False)["status"]

    def get_job(self, job_id, with_log=True):
        """
        Return full details of the job with ID `job_id` (integer).
        """
        # we accept either an integer job id or a resource uri as identifier
        try:
            job_id = int(job_id)
        except ValueError:
            job_id = int(job_id.split("/")[-1])
        for resource_type in ("queue", "results"):
            job_uri = self._query(self.resource_map[resource_type] + "?id={}".format(job_id))
            logger.debug(job_uri)
            if job_uri:
                job = self._query(job_uri[0])
                assert job["id"] == job_id
                if with_log:
                    try:
                        log = self._query(self.resource_map['log'] + "/{}/".format(job_id))
                    except Exception:
                        job["log"] = ''
                    else:
                        assert log["resource_uri"] == '/api/v2/log/{}'.format(job_id)
                        job["log"] = log["content"]
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
        return self._query(self.resource_map["queue"] + "/submitted/?user_id=" + str(self.user_info["id"]), verbose=verbose)

    def completed_jobs(self, collab_id, verbose=False):
        """
        Return the list of completed jobs in the given collab.

        Arguments
        ---------

        verbose : if False, return just the job URIs,
                  if True, return full details.
        """
        return self._query(self.resource_map["results"] + "?collab_id=" + str(collab_id),
                           verbose=verbose)

    def download_data(self, job, local_dir=".", include_input_data=False):
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

        server_paths = [urlparse(item["url"])[2] for item in datalist]
        if len(server_paths) > 1:
            common_prefix = os.path.commonprefix(server_paths)
            assert common_prefix[-1] == "/"
        else:
            common_prefix = os.path.dirname(server_paths[0])
        relative_paths = [os.path.relpath(p, common_prefix) for p in server_paths]

        for relative_path, dataitem in zip(relative_paths, datalist):
            url = dataitem["url"]
            (scheme, netloc, path, params, query, fragment) = urlparse(url)
            if not scheme:
                url = "file://" + url
            local_path = os.path.join(local_dir, relative_path)
            dir = os.path.dirname(local_path)
            _mkdir_p(dir)
            urlretrieve(url, local_path)
            filenames.append(local_path)

        return filenames

    def copy_data_to_storage(self, job_id, destination="collab"):
        """
        Copy the data produced by the job with id `job_id` to Collaboratory
        storage or to the HPAC Platform. Note that copying data to an HPAC
        site requires that you have an account for that site.

        Example
        -------

        To copy data to the JURECA machine:

            client.copy_data_to_storage(90712, "JURECA")

        To copy data to Collab storage:

            client.copy_data_to_storage(90712, "collab")

        """
        return self._query("/copydata/{}/{}".format(destination, job_id))

    def create_data_item(self, url):
        """
        Register a data item with the platform.
        """
        data_item = {"url": url}
        result = self._post(self.resource_map["dataitem"], data_item)
        return result["resource_uri"]

    def my_collabs(self):
        """
        Return a list of collabs of which the user is a member.

        """
        collabs = []
        next = COLLAB_SERVICE + '/mycollabs'
        while next:
            req = requests.get(next, auth=self.auth)
            if req.ok:
                data = req.json()
                next = data["next"]
                collabs.extend(data["results"])
            else:
                self._handle_error(req)
        return dict((c["title"], c)
                    for c in collabs if not c["deleted"])
