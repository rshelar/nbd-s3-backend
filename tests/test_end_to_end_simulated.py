# Simulated end-to-end durability test for NbdServer

import os
import shutil

from nbd_server.nbd_server import NbdServer
from nbd_server.file_storage import FileStorage
from nbd_server.s3_storage import S3Storage

BLOCK_SIZE = 4096
EXPORT = "simtest"
VOL_PATH = "../data/exports"
BUCKET = "nbdbucket"


def setup_function():
    # Clean volatile storage for fresh test
    export_path = os.path.join(VOL_PATH, EXPORT)
    if os.path.exists(export_path):
        shutil.rmtree(export_path)


def test_simulated_end_to_end_durability():
    # Phase 1: Create server with volatile + durable storage
    volatile = FileStorage(VOL_PATH)
    durable = S3Storage(
        bucket=BUCKET,
        export_name=EXPORT,
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    server = NbdServer(
        export_name=EXPORT,
        total_size_bytes=BLOCK_SIZE * 128,
        volatile_storage=volatile,
        nonvolatile_storage=durable
    )

    # Step 1: Write data at several offsets
    server.write(0, b"AAAAAA")
    server.write(4096, b"BBBBBBBB")
    server.write(8192, b"CCCCCCCCCCCC")

    # Step 2: Flush to durable storage
    server.flush()

    # Phase 2: Simulate restart
    volatile2 = FileStorage(VOL_PATH)
    durable2 = S3Storage(
        bucket=BUCKET,
        export_name=EXPORT,
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    server2 = NbdServer(
        export_name=EXPORT,
        total_size_bytes=BLOCK_SIZE * 128,
        volatile_storage=volatile2,
        nonvolatile_storage=durable2,
    )

    # Step 3: Read data back from durable storage
    assert server2.read(0, 6) == b"AAAAAA"
    assert server2.read(4096, 8) == b"BBBBBBBB"
    assert server2.read(8192, 12) == b"CCCCCCCCCCCC"

    print("Simulated end-to-end durability test PASSED")