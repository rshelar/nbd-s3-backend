import os
import shutil

from nbd_server.nbd_server import NbdServer
from nbd_server.util import BLOCK_SIZE


TEST_BASE = "test_data"


def setup_function():
    # Clean test directory before each test
    if os.path.exists(TEST_BASE):
        shutil.rmtree(TEST_BASE)
    os.makedirs(TEST_BASE)


def teardown_function():
    # Clean test directory after each test
    if os.path.exists(TEST_BASE):
        shutil.rmtree(TEST_BASE)


def make_server(size_blocks=10):
    return NbdServer(
        export_name="dev1",
        total_size_bytes=BLOCK_SIZE * size_blocks,
        base_path=TEST_BASE,
    )


def test_single_block_write_and_read():
    server = make_server()
    msg = b"hello world"
    server.write(0, msg)
    out = server.read(0, len(msg))
    assert out == msg


def test_partial_block_write():
    server = make_server()
    data = b"abc"
    server.write(100, data)   # write inside block 0

    # Now read back exactly that range
    out = server.read(100, len(data))
    assert out == data

    # Also confirm block is padded correctly
    block0 = server.storage.read_block("dev1", 0)
    assert block0[100:103] == b"abc"
    assert len(block0) == BLOCK_SIZE


def test_write_across_two_blocks():
    server = make_server()

    # Write 300 bytes starting near the end of block 0
    offset = BLOCK_SIZE - 50
    data = b"A" * 300  # spans into block 1
    server.write(offset, data)

    # Read back in one shot
    out = server.read(offset, 300)
    assert out == data

    # Validate boundary block contents
    block0 = server.storage.read_block("dev1", 0)
    block1 = server.storage.read_block("dev1", 1)

    assert block0[-50:] == b"A" * 50
    assert block1[:250] == b"A" * 250


def test_multi_block_read_with_no_writes_returns_zero_fill():
    server = make_server()

    # Read across 3 blocks without writing anything
    offset = BLOCK_SIZE - 10
    length = BLOCK_SIZE * 2 + 20  # spans blocks 0,1,2
    out = server.read(offset, length)

    assert len(out) == length
    assert out == bytes(length)  # all zeroes


def test_write_multiple_blocks_exact_alignment():
    server = make_server()

    # Write 2 full blocks
    data = b"Z" * (BLOCK_SIZE * 2)
    server.write(0, data)

    # Read block 0
    b0 = server.read(0, BLOCK_SIZE)
    # Then block 1
    b1 = server.read(BLOCK_SIZE, BLOCK_SIZE)

    assert b0 == b"Z" * BLOCK_SIZE
    assert b1 == b"Z" * BLOCK_SIZE


def test_out_of_bounds_read():
    server = make_server()

    try:
        server.read(BLOCK_SIZE * 10 - 100, 200)
    except ValueError:
        return

    assert False, "Expected read to raise ValueError for out-of-bounds"


def test_out_of_bounds_write():
    server = make_server()

    try:
        server.write(BLOCK_SIZE * 10 - 10, b"1234567890123456")
    except ValueError:
        return

    assert False, "Expected write to raise ValueError for out-of-bounds write"