import os

from nbd_server.storage import Storage
from nbd_server.util import BLOCK_SIZE


class FileStorage(Storage):
    """
    Local filesystem-backed block storage.

    Blocks are stored under:
        data/exports/<export_name>/blocks/<block_id>

    Each block is exactly BLOCK_SIZE bytes. Missing blocks are treated
    as zero-filled blocks, consistent with block device semantics.

    This backend is simple, durable, and easy to test locally.
    """

    def __init__(self, base_path: str = "data"):
        """
        Args:
            base_path: Root directory where exports/ will live.
        """
        self.base_path = base_path

    def _block_path(self, export_name: str, block_id: int) -> str:
        """
        Returns the full filesystem path for a given block file.
        """
        return os.path.join(
            self.base_path,
            "exports",
            export_name,
            "blocks",
            str(block_id),
        )

    def read_block(self, export_name: str, block_id: int) -> bytes:
        """
        Read exactly one block from the local filesystem.

        If the file does not exist, return a zero-filled block.

        Note: Only *full* block reads are performed here.
        Partial reads (from offset,length) are handled in nbd_backend.py,
        which issues one read_block() call per block touched:

            start = offset
            end   = offset + length - 1

            first_block_id = start // BLOCK_SIZE
            last_block_id  = end   // BLOCK_SIZE

            For each block_id in [first_block_id, last_block_id]:
                read_block(export, block_id)
        """
        path = self._block_path(export_name, block_id)

        # Block not written yet → zero-filled block
        if not os.path.exists(path):
            return bytes(BLOCK_SIZE)

        with open(path, "rb") as f:
            data = f.read()

        # Normalize block size
        if len(data) != BLOCK_SIZE:
            if len(data) < BLOCK_SIZE:
                # pad short blocks (shouldn't happen in practice)
                data = data + bytes(BLOCK_SIZE - len(data))
            else:
                # truncate oversized blocks (shouldn't happen)
                data = data[:BLOCK_SIZE]

        return data

    def write_block(self, export_name: str, block_id: int, data: bytes) -> None:
        """
        Write exactly one block to the filesystem.

        Ensures directories exist and overwrites the block file atomically.
        Atomic write = write to <path>.tmp → rename to <path>.

        NOTE:
        Partial-block writes are handled by nbd_backend.py. That layer:
            1. Reads the block (read_block)
            2. Modifies the slice in-memory
            3. Writes back a FULL block via write_block()
        """
        if len(data) != BLOCK_SIZE:
            raise ValueError(
                f"Block data must be exactly {BLOCK_SIZE} bytes; got {len(data)} bytes"
            )

        path = self._block_path(export_name, block_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        temp_path = f"{path}.tmp"

        # Write atomically
        with open(temp_path, "wb") as f:
            f.write(data)

        os.replace(temp_path, path)