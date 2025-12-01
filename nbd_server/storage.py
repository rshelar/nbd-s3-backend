from abc import ABC, abstractmethod


class Storage(ABC):
    """
    Abstract storage backend interface.

    A Storage backend is responsible for persisting and retrieving fixed-size
    blocks identified by block_id under a given export name. Implementations
    may store blocks locally on disk or remotely (e.g., S3 / MinIO).

    All block data passed in or returned must be exactly BLOCK_SIZE bytes. BLOCK_SIZE = 4096 (block has constant size).

    This interface supports read, write operations in block granularity. The Storage interface always reads or writes
    exactly 1 full block, never a range inside a block
    nbd_server calculates block_id as `block_id = offset // block_size`
    """

    @abstractmethod
    def read_block(self, export_name: str, block_id: int) -> bytes:
        """
        This method reads exactly one block.
        It is typically called multiple times by nbd_backend.pread(),
        which computes the range of block_ids touched by the (offset, length) NBD request:

        pread(offset, length) calls read_block() N times where N = number of blocks touched by the byte range
        range_start = offset
        range_end   = offset + length - 1
        first_block_id = range_start // BLOCK_SIZE
        last_block_id  = range_end   // BLOCK_SIZE
        number_of_blocks = last_block_id - first_block_id + 1

        Args:
            export_name: Name of the export (namespace) the block belongs to.
            block_id: Integer block identifier.

        Returns:
            A bytes object of length BLOCK_SIZE.
        """
        raise NotImplementedError

    @abstractmethod
    def write_block(self, export_name: str, block_id: int, data: bytes) -> None:
        """
        Write a single block to persistent storage.
        It is typically called multiple times by nbd_backend.pwrite(),
        which computes the range of block_ids touched by the (offset, length) NBD request:

        pwrite(buf, offset) calculates byte range: [offset, offset + len(buf) - 1]
        This byte range spans N blocks where:
        range_start = offset
        range_end   = offset + len(buf) - 1
        first_block_id = range_start // BLOCK_SIZE
        last_block_id  = range_end   // BLOCK_SIZE
        number_of_blocks = last_block_id - first_block_id + 1

        Args:
            export_name: Name of the export (namespace).
            block_id: Integer block identifier.
            data: Bytes to write. Must be exactly BLOCK_SIZE bytes long.
        """
        raise NotImplementedError
