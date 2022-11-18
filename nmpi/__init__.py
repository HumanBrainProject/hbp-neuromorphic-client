from .nmpi_user import Client

try:
    from .nmpi_saga import HardwareClient
except ImportError:
    pass
try:
    from .nmpi_admin import AdminClient
except ImportError:
    pass

SPINNAKER = "SpiNNaker"
BRAINSCALES = "BrainScaleS"
BRAINSCALES1 = "BrainScaleS"
BRAINSCALES2 = "BrainScaleS-2"
ESS = "BrainScaleS-ESS"
SPIKEY = "Spikey"

__version__ = "0.10.1"
