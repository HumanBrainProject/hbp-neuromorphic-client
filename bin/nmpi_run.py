#!/usr/bin/env python

import sys
import logging
from nmpi import nmpi_saga

logging.basicConfig(filename='nmpi.log', level=logging.DEBUG)
sys.exit(nmpi_saga.main())
