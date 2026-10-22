import copyreg
import sys

from ._aethonan import ListenerSpec as SocketSpec, SocketHolder


copyreg.pickle(SocketHolder, lambda v: (SocketHolder, v.__getstate__()))
copyreg.pickle(SocketSpec, lambda v: (SocketSpec, v.__getstate__()))

if sys.platform != 'win32':
    from ._aethonan import UnixListenerSpec as UnixSocketSpec

    copyreg.pickle(UnixSocketSpec, lambda v: (UnixSocketSpec, v.__getstate__()))
else:
    UnixSocketSpec = None
