from .nmpi_user import Client
try:
    from .nmpi_saga import HardwareClient
except ImportError:
    pass
