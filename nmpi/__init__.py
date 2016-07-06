from .nmpi_user import Client
try:
    from .nmpi_saga import HardwareClient
except ImportError:
    pass

SPINNAKER = "SpiNNaker"
BRAINSCALES = "BrainScaleS"
ESS = "BrainScaleS-ESS"
SPIKEY = "Spikey"

__version__ = "0.4.3"
