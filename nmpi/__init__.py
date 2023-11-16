from .nmpi_user import Client  # noqa: F401

try:
    from .nmpi_saga import HardwareClient  # noqa: F401
except ImportError:
    pass
try:
    from .nmpi_admin import AdminClient  # noqa: F401
except ImportError:
    pass

SPINNAKER = "SpiNNaker"
BRAINSCALES = "BrainScaleS"
BRAINSCALES1 = "BrainScaleS"
BRAINSCALES2 = "BrainScaleS-2"
ESS = "BrainScaleS-ESS"
SPIKEY = "Spikey"

__version__ = "0.11.1"
