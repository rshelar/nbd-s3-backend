"""
NBD backend logic for block-level read/write using FileStorage.

This module is intended to be used as the core logic behind an NBD server
(e.g. via nbdkit's Python plugin interface). It does NOT speak the NBD
protocol itself; instead, it implements pread/pwrite-style operations over
a flat virtual block device backed by FileStorage.

Key responsibilities:
- Map (offset, length) byte ranges to block_ids.
- Perform multi-block reads and writes.
- Perform read-modify-write (RMW) for partial-block writes.
- Delegate full-block persistence to the Storage layer.

For now, this backend uses FileStorage only. S3Storage can be added later
and selected via configuration.
"""

from nbd_server.util import (
    BLOCK_SIZE,
    blocks_touched,
)


class NbdServer:
    """
    Core backend for an NBD-style block device.

    The backend exposes:
        - read(offset, length) -> bytes
        - write(offset, data) -> None

    It operates on a single export (export_name) and uses a Storage
    implementation (currently FileStorage) for persistence.
    """

    def __init__(
            self,
            export_name: str,
            total_size_bytes: int,
            volatile_storage = None,
            nonvolatile_storage = None,
    ) -> None:
        """
        Args:
            export_name: Logical export / device name. Used as a namespace
                         when storing blocks on disk.
            total_size_bytes: Size of the virtual block device in bytes.
                              get_size() will return this value.
            volatile_storage: FileStorage Storage object representing volatile storage.
            nonvolatile_storage: S3Storage object representing non-volatile storage.
        """
        self.export_name = export_name
        self.total_size_bytes = total_size_bytes
        self.volatile_storage = volatile_storage
        self.nonvolatile_storage = nonvolatile_storage
        self.dirty_blocks = set()

    # ---------------------------------------------------------------------
    # Public API: these are the methods your NBD server / nbdkit plugin
    # will call from its pread/pwrite callbacks.
    # ---------------------------------------------------------------------

    def get_size(self) -> int:
        """
        Return the total size of the virtual block device in bytes.
        """
        return self.total_size_bytes

    def read(self, offset: int, length: int) -> bytes:
        """
        Read exactly `length` bytes starting at `offset` from the virtual
        block device.

        This may touch one or more blocks. For each block_id in the range,
        we:
            - read the FULL block via storage.read_block()
            - slice the portion we need
            - append it to the result

        Returns:
            A bytes object of length `length`.
        """
        if self.volatile_storage is None:
            raise RuntimeError("No storage backend configured for NbdServer")

        if length == 0:
            return b""

        if offset < 0 or offset + length > self.total_size_bytes:
            raise ValueError("read range is out of bounds of the device size")

        result = bytearray()
        end = offset + length

        for block_id in blocks_touched(offset, length):
            block = self.volatile_storage.read_block(self.export_name, block_id)

            block_start = block_id * BLOCK_SIZE
            block_end = block_start + BLOCK_SIZE

            # Compute intersection of [offset, end) with [block_start, block_end)
            local_start = max(offset, block_start) - block_start
            local_end = min(end, block_end) - block_start

            if local_start < 0 or local_end < 0 or local_start > BLOCK_SIZE:
                # Should not happen; defensive check.
                continue

            result.extend(block[local_start:local_end])

        # Defensive check: we should have constructed exactly `length` bytes.
        if len(result) != length:
            raise RuntimeError(
                f"read(): expected {length} bytes, assembled {len(result)} bytes"
            )

        return bytes(result)

    def write(self, offset: int, data: bytes) -> None:
        """
        Write the contents of `data` starting at `offset` on the virtual
        block device.

        This may touch one or more blocks. For each block_id in the range,
        we determine whether the write:
            - fully covers the block (fast path)
            - partially covers the block (needs read-modify-write)

        Partial-block writes:
            1. Read existing block via storage.read_block()
            2. Modify only the overlapping slice
            3. Write back the full block via storage.write_block()
        """
        if self.volatile_storage is None:
            raise RuntimeError("No storage backend configured for NbdServer")

        length = len(data)
        if length == 0:
            return

        if offset < 0 or offset + length > self.total_size_bytes:
            raise ValueError("write range is out of bounds of the device size")

        end = offset + length

        for block_id in blocks_touched(offset, length):
            block_start = block_id * BLOCK_SIZE
            block_end = block_start + BLOCK_SIZE

            # Intersection of [offset, end) with [block_start, block_end)
            write_start_in_block = max(offset, block_start) - block_start
            write_end_in_block = min(end, block_end) - block_start
            bytes_to_write_in_block = write_end_in_block - write_start_in_block

            # Corresponding slice in the input `data`
            # Global index in data where this block's slice begins:
            src_start = max(block_start, offset) - offset
            src_end = src_start + bytes_to_write_in_block

            # Sanity checks
            if not (0 <= write_start_in_block <= BLOCK_SIZE):
                continue
            if bytes_to_write_in_block <= 0:
                continue

            # Fast path: this write fully covers the block
            if write_start_in_block == 0 and bytes_to_write_in_block == BLOCK_SIZE:
                # We can write this block directly from `data` slice.
                new_block = data[src_start:src_end]
                if len(new_block) != BLOCK_SIZE:
                    raise RuntimeError(
                        f"Expected full-block slice of {BLOCK_SIZE} bytes, "
                        f"got {len(new_block)} bytes"
                    )
                self.volatile_storage.write_block(self.export_name, block_id, new_block)
            else:
                # Partial-block write → read-modify-write
                existing_block = bytearray(
                    self.volatile_storage.read_block(self.export_name, block_id)
                )
                existing_block[
                write_start_in_block:write_end_in_block
                ] = data[src_start:src_end]

                if len(existing_block) != BLOCK_SIZE:
                    raise RuntimeError(
                        f"Existing block must remain {BLOCK_SIZE} bytes; "
                        f"got {len(existing_block)} bytes"
                    )

                self.volatile_storage.write_block(
                    self.export_name, block_id, bytes(existing_block)
                )
            self.dirty_blocks.add(block_id)