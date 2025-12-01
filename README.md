from abc import ABC, abstractmethod

"""
Storage Layer — Lowest Level of the Block Device Stack
------------------------------------------------------

This interface represents the *block-storage backend* for the NBD server.

Important architecture distinction:

    NBD Client (Linux kernel)
            ↓  read(offset, length) / write(offset, data)
    nbdkit NBD Server (C program)
            ↓  calls Python plugin
    nbd_backend.py  <-- handles offsets, block math, caching, RMW cycles
            ↓
    Storage (this file)  <-- operates ONLY on full fixed-size blocks
            ↓
    FileStorage / S3Storage implement actual persistence

Why this layer does NOT take offsets:
-------------------------------------
The NBD protocol sends read/write requests in terms of byte offsets.
However, underlying block storage (local files, S3 objects) works best when
reads and writes happen at *block granularity*. Therefore:

  • The NBD plugin layer breaks offsets into (block_id, block_offset).
  • It performs read-modify-write cycles for partial-block writes.
  • It assembles blocks for partial reads.
  • It tracks dirty blocks in cache for flush semantics.

By the time a request reaches the Storage interface, the plugin has already
computed exactly which *full block* needs to be persisted or retrieved.

Thus:
    Storage reads/writes EXACTLY ONE BLOCK per call.
    Storage MUST return `block_size` bytes.
    Storage MUST write full blocks, not byte ranges.

This strict boundary keeps the system simple, correct, and S3-compatible.
"""


class Storage(ABC):
    """
    Abstract storage backend interface.

    Implementations persist opaque, fixed-size blocks identified by
    (export_name, block_id). Blocks contain raw ext4/NBD data and may hold
    user file data, filesystem metadata, or any mixture — this layer does
    not interpret content.

    All reads and writes operate on EXACTLY one block at a time.
    """

    @abstractmethod
    def read_block(self, export_name: str, block_id: int, block_size: int) -> bytes:
        """
        Read one block from storage.

        :param export_name: The name of the export (namespace).
        :param block_id: The block index to read.
        :param block_size: The size of the block in bytes.
        :return: The raw block data as bytes, exactly block_size bytes long.
        """
        pass

    @abstractmethod
    def write_block(self, export_name: str, block_id: int, data: bytes) -> None:
        """
        Write one block to storage.

        :param export_name: The name of the export (namespace).
        :param block_id: The block index to write.
        :param data: The raw block data as bytes, must be exactly block_size bytes.
        """
        pass

    @abstractmethod
    def flush(self, export_name: str) -> None:
        """
        Flush any buffered writes to durable storage.

        :param export_name: The name of the export (namespace).
        """
        pass
