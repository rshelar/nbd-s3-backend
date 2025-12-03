"""
nbdkit_plugin.py
----------------

Python plugin for nbdkit that exposes the NbdServer block device.

This module is loaded by nbdkit when invoked with:

    nbdkit python /path/to/nbdkit_plugin.py \
        export=dev1 \
        size=1073741824 \
        volatile_path=data/exports \
        bucket=nbdbucket \
        s3_endpoint=http://localhost:9000 \
        s3_access_key=minioadmin \
        s3_secret_key=minioadmin

The plugin wires nbdkit's pread/pwrite/flush operations to an NbdServer
instance backed by a volatile (FileStorage) and a non-volatile (S3Storage)
backend.
"""

import nbdkit  # Provided by nbdkit at runtime, may appear unresolved in IDE.

from nbd_server.nbd_server import NbdServer
from nbd_server.file_storage import FileStorage
from nbd_server.s3_storage import S3Storage


# ---------------------------------------------------------------------------
# Global configuration (populated via config())
# ---------------------------------------------------------------------------

_export_name: str | None = None
_total_size_bytes: int | None = None
_volatile_path: str | None = None

_s3_bucket: str | None = None
_s3_endpoint: str = "http://localhost:9000"
_s3_access_key: str = "minioadmin"
_s3_secret_key: str = "minioadmin"

# Global NbdServer instance. Created once in config_complete().
_server: NbdServer | None = None


class Handle:
    """
    Per-connection handle used by nbdkit.

    nbdkit will call open() for each new client connection and pass the
    resulting handle to pread/pwrite/flush/get_size/close.
    """
    def __init__(self, server: NbdServer) -> None:
        self.server = server


# ---------------------------------------------------------------------------
# nbdkit plugin entrypoints
# ---------------------------------------------------------------------------

def config(key: str, value: str) -> None:
    """
    Called by nbdkit for each configuration parameter.

    Example:
        nbdkit python nbdkit_plugin.py export=dev1 size=1048576 \
            volatile_path=data/exports bucket=nbdbucket
    """
    global _export_name, _total_size_bytes, _volatile_path
    global _s3_bucket, _s3_endpoint, _s3_access_key, _s3_secret_key

    if key == "export":
        _export_name = value
    elif key == "size":
        _total_size_bytes = int(value)
    elif key == "volatile_path":
        _volatile_path = value
    elif key == "bucket":
        _s3_bucket = value
    elif key == "s3_endpoint":
        _s3_endpoint = value
    elif key == "s3_access_key":
        _s3_access_key = value
    elif key == "s3_secret_key":
        _s3_secret_key = value
    else:
        # nbdkit.Error will cause nbdkit to fail fast with a useful message.
        raise nbdkit.Error(f"Unknown parameter: {key}={value}")


def config_complete() -> None:
    """
    Called by nbdkit after all config() calls are done.

    We validate configuration and create the global NbdServer instance here.
    """
    global _server

    if _export_name is None:
        raise nbdkit.Error("Missing required parameter: export=<name>")
    if _total_size_bytes is None:
        raise nbdkit.Error("Missing required parameter: size=<bytes>")
    if _volatile_path is None:
        raise nbdkit.Error("Missing required parameter: volatile_path=<dir>")
    if _s3_bucket is None:
        raise nbdkit.Error("Missing required parameter: bucket=<s3-bucket>")

    nbdkit.debug(f"nbdkit_plugin: export={_export_name}, "
                 f"size={_total_size_bytes}, "
                 f"volatile_path={_volatile_path}, "
                 f"bucket={_s3_bucket}, endpoint={_s3_endpoint}")

    volatile_storage = FileStorage(_volatile_path)
    nonvolatile_storage = S3Storage(
        bucket=_s3_bucket,
        export_name=_export_name,
        endpoint_url=_s3_endpoint,
        aws_access_key_id=_s3_access_key,
        aws_secret_access_key=_s3_secret_key,
    )

    _server = NbdServer(
        export_name=_export_name,
        total_size_bytes=_total_size_bytes,
        volatile_storage=volatile_storage,
        nonvolatile_storage=nonvolatile_storage,
    )

    nbdkit.debug("nbdkit_plugin: NbdServer created successfully")


def open(readonly: bool):
    """
    Called for each new client connection.

    Returns a per-connection handle which is passed back to other callbacks.
    """
    if _server is None:
        raise nbdkit.Error("nbdkit_plugin: server not initialized in config_complete()")

    nbdkit.debug(f"nbdkit_plugin: open(readonly={readonly})")
    return Handle(_server)


def get_size(h: Handle) -> int:
    """
    Return the size in bytes of the virtual block device.
    """
    # NbdServer tracks the total size passed at construction.
    return h.server.total_size_bytes


def pread(h: Handle, count: int, offset: int) -> bytes:
    """
    Read 'count' bytes starting at 'offset' from the device.
    """
    nbdkit.debug(f"nbdkit_plugin: pread(count={count}, offset={offset})")
    return h.server.read(offset, count)


def pwrite(h: Handle, buf: bytes, offset: int) -> None:
    """
    Write the bytes in 'buf' starting at 'offset' to the device.
    """
    nbdkit.debug(f"nbdkit_plugin: pwrite(len={len(buf)}, offset={offset})")
    h.server.write(offset, buf)


def flush(h: Handle) -> None:
    """
    Flush all completed writes to non-volatile storage (S3).

    nbdkit calls this when the client issues an NBD_CMD_FLUSH. We delegate
    to NbdServer.flush(), which copies all dirty blocks from the volatile
    FileStorage to the durable S3Storage backend.
    """
    nbdkit.debug("nbdkit_plugin: flush()")
    h.server.flush()


def close(h: Handle) -> None:
    """
    Called when a client connection is closed.
    """
    nbdkit.debug("nbdkit_plugin: close()")
    # No per-connection resources to release; NbdServer lives for the life of
    # the nbdkit process.
    return
