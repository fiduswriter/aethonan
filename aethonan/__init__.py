from ._aethonan import __version__
from ._loops import loops
from .server import Server as Aethonan

# Alias matching the upstream project's class name, kept for familiarity
Granian = Aethonan


__all__ = ['__version__', 'loops', 'Aethonan', 'Granian']
