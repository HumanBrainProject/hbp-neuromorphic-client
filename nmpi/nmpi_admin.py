"""
Client for interacting with the Neuromorphic Computing Platform of the Human Brain Project
as an administrator.

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

import nmpi


TEST_QUOTAS = {
    "BrainScaleS": {"limit": 0.1, "units": "wafer-hours"},
    "SpiNNaker": {"limit": 5000, "units": "core-hours"},
    "BrainScaleS-ESS": {"limit": 10, "units": "hours"},
    "Spikey": {"limit": 10, "units": "hours"}
}


class AdminClient(nmpi.Client):
    """
    Client for interacting with the Neuromorphic Computing Platform of
    the Human Brain Project, with additional methods only available to administrators.
    """

    def resource_requests(self, collab_id=None, status=None):
        """
        Return a list of compute-time resource requests.

        Arguments
        ---------

            `collab_id`: filter list by collab id (default: all collabs)
            `status`: filter list by request status (default: all statuses)


        Possible values for `status` are 'in preparation', 'under review',
        'accepted', 'rejected'.

        """
        projects = self._query(self.quotas_server + "/projects/")
        # server-side filtering not yet supported, so we filter client side
        if collab_id is not None:
            projects = [p for p in projects if p['collab'] == str(collab_id)]
        if status is not None:
            projects = [p for p in projects if p['status'] == status]
        return projects

    def accept_resource_request(self, request_uri, with_quotas=False):
        """
        Accept a resource (compute-time) allocation request.
        """
        response = self._put(self.quotas_server + request_uri,
                             {"status": "accepted"})
        if with_quotas:
            for platform, values in with_quotas.items():
                self.add_quota(request_uri,
                               platform=platform,
                               limit=values["limit"],
                               units=values["units"])
        return response

    def reject_resource_request(self, request_uri):
        """
        Reject a resource (compute-time) allocation request.
        """
        response = self._put(self.quotas_server + request_uri,
                             {"status": "rejected"})
        return response

    def add_quota(self, request_uri, platform, limit, units=None):
        """
        Add a compute-time quota to a resource request.
        """
        if units is None:
            if platform in TEST_QUOTAS:
                units = TEST_QUOTAS[platform]["units"]
            else:
                raise ValueError("Must specify units")
        project_id = request_uri.split("/")[-1]
        quota = {
            "units": units,
            "limit": limit,
            "usage": 0.0,
            "platform": platform,
            "project" : project_id
        }
        response = self._post(self.quotas_server + request_uri + "/quotas/",
                              quota)
        return response
