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
ESS = "BrainScaleS-ESS"
SPIKEY = "Spikey"

__version__ = "0.7.1"
