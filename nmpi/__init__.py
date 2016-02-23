from .nmpi_user import Client
try:
    from .nmpi_saga import HardwareClient
except ImportError:
    pass

__version__ = "0.2.0"
