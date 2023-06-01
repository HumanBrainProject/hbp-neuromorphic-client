#!/usr/bin/env python

import os
import sys
import logging

from nmpi import nmpi_saga

log_file = os.path.expanduser("~/nmpi.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
)
sys.exit(nmpi_saga.main())
