"""


"""

import os.path
import unittest
import tempfile
import shutil
from datetime import datetime

from nmpi import nmpi_user


ENTRYPOINT = "https://nmpi-staging.hbpneuromorphic.eu/api/v2/"
#ENTRYPOINT = "http://127.0.0.1:8000/api/v2/"
#ENTRYPOINT = "https://192.168.59.103:32768/api/v2/"
#ENTRYPOINT = "https://nmpi-tmp-1.apdavison.cont.tutum.io:49157/api/v2/"

TEST_USER = os.environ['NMPI_TEST_USER']
TEST_TOKEN = os.environ['NMPI_TEST_TOKEN']
#TEST_TOKEN = "faketoken"
TEST_COLLAB = 563
VERIFY = True

simple_test_script = r"""
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

with open(timestamp + ".txt", 'w') as fp:
  fp.write(timestamp + " 42\n")

print "done"
"""

simulation_test_script = r"""
import sys
exec("import pyNN.%s as sim" % sys.argv[1])

sim.setup()

p = sim.Population(2, sim.IF_cond_exp, {'i_offset': 0.1})
p.record_v()

sim.run(100.0)

p.print_v("simulation_data.v")

sim.end()
"""


class QueueInteractionTest(unittest.TestCase):

    def setUp(self):
        self.user_client = nmpi_user.Client("testuser", entrypoint=ENTRYPOINT, token=TEST_TOKEN, verify=VERIFY)
        self.collab_id = TEST_COLLAB
        self.job_id = None
        # print self.user_client.resource_map

    def test__1_submit_job(self):
        self.job_id = self.user_client.submit_job(source=simple_test_script,
                                                  platform="nosetest_platform",
                                                  collab_id=self.collab_id)
        print "job_id: ", self.job_id
        print self.user_client.job_status(self.job_id)
        print self.user_client.get_job(self.job_id)

    def test__2_queued_jobs(self):
        # per platform
        self.user_client.queued_jobs(verbose=True)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(QueueInteractionTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
