import os
import shutil

from nbd_server.file_storage import FileStorage
from nbd_server.util import BLOCK_SIZE


TEST_BASE = "test_data"   # isolated test directory


def setup_function():
    """Run before each test: clean and recreate test directory."""
    if os.path.exists(TEST_BASE):
        shutil.rmtree(TEST_BASE)
    os.makedirs(TEST_BASE)


def teardown_function():
    """Run after each test: clean up."""
    if os.path.exists(TEST_BASE):
        shutil.rmtree(TEST_BASE)


def test_read_missing_block_returns_zero_fill():
    storage = FileStorage(base_path=TEST_BASE)
    data = storage.read_block("dev1", 0)

    assert isinstance(data, bytes)
    assert len(data) == BLOCK_SIZE
    assert data == bytes(BLOCK_SIZE)  # zero-filled


def test_write_and_read_roundtrip():
    storage = FileStorage(base_path=TEST_BASE)
    block_id = 3
    write_bytes = b"hello world" + bytes(BLOCK_SIZE - 11)

    storage.write_block("dev1", block_id, write_bytes)
    read_bytes = storage.read_block("dev1", block_id)

    assert read_bytes == write_bytes


def test_write_creates_correct_path():
    storage = FileStorage(base_path=TEST_BASE)
    block_id = 7
    write_bytes = b"A" * BLOCK_SIZE

    storage.write_block("dev1", block_id, write_bytes)

    expected_path = os.path.join(
        TEST_BASE, "exports", "dev1", "blocks", str(block_id)
    )
    assert os.path.exists(expected_path)

    with open(expected_path, "rb") as f:
        on_disk = f.read()
    assert on_disk == write_bytes


def test_read_block_normalizes_oversized_block():
    """If a block file is too large, FileStorage should truncate it."""
    storage = FileStorage(base_path=TEST_BASE)
    oversized = b"X" * (BLOCK_SIZE + 100)
    block_id = 1

    path = os.path.join(TEST_BASE, "exports", "dev1", "blocks", str(block_id))
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "wb") as f:
        f.write(oversized)

    normalized = storage.read_block("dev1", block_id)
    assert len(normalized) == BLOCK_SIZE
    assert normalized == b"X" * BLOCK_SIZE


def test_read_block_normalizes_short_block():
    """If a block is too small, FileStorage pads it with zeros."""
    storage = FileStorage(base_path=TEST_BASE)
    short = b"Y" * 100
    block_id = 2

    path = os.path.join(TEST_BASE, "exports", "dev1", "blocks", str(block_id))
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "wb") as f:
        f.write(short)

    normalized = storage.read_block("dev1", block_id)
    assert len(normalized) == BLOCK_SIZE
    assert normalized.startswith(b"Y" * 100)
    assert normalized[100:] == bytes(BLOCK_SIZE - 100)