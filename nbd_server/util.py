"""
Utility helpers for block math and constants used by the NBD server.
"""

# Fixed block size for the entire virtual block device.
# All blocks read/written must be exactly this size.
BLOCK_SIZE = 4096

def block_id_from_offset(offset: int) -> int:
    """
    Convert a byte offset into a block ID.

    Example:
        offset = 8200
        BLOCK_SIZE = 4096
        block_id = 2
    """
    return offset // BLOCK_SIZE

def block_offset_inside_block(offset: int) -> int:
    """
    Compute the offset *inside* a block.

    Example:
        offset = 8200
        BLOCK_SIZE = 4096
        block_offset = 8200 % 4096 = 8
    """
    return offset % BLOCK_SIZE

def blocks_touched(offset: int, length: int) -> range:
    """
    Return the range of block_ids touched by reading/writing
    [offset, offset + length - 1].

    Example:
        offset = 3900
        length = 400
        BLOCK_SIZE = 4096

        This touches:
            block 0 (partial)
            block 1 (partial)

        Returned range = range(0, 2)
    """
    if length == 0:
        return range(0)

    start = offset
    end = offset + length - 1

    first_block = start // BLOCK_SIZE
    last_block = end // BLOCK_SIZE

    return range(first_block, last_block + 1)