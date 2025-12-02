"""
bootstrap.py
------------

Bootstraps an NbdServer instance with the correct storage backend
(FileStorage or S3Storage) based on command-line flags or environment
variables.

This file is NOT the NBD protocol server itself. It simply wires the
backend and produces an NbdServer object that an actual NBD plugin
(e.g. nbdkit's Python plugin) will call into.
"""

import argparse
import os

from nbd_server.nbd_server import NbdServer
from nbd_server.file_storage import FileStorage
from nbd_server.s3_storage import S3Storage
from nbd_server.util import BLOCK_SIZE

def create_server_from_args():
    parser = argparse.ArgumentParser(description="NBD backend bootstrapper")

    parser.add_argument("--export", type=str, default="nbd0",
                        help="Export / device name")

    parser.add_argument("--size", type=int, default=1024 * 1024 * 1024,
                        help="Total device size in bytes")

    parser.add_argument("--backend", choices=["file", "s3"],
                        default=os.getenv("NBD_BACKEND", "file"),
                        help="Storage backend type")

    parser.add_argument("--path", type=str, default="data/exports",
                        help="Path for FileStorage block directory")

    # S3 configuration
    parser.add_argument("--bucket", type=str, default=os.getenv("S3_BUCKET", "nbdbucket"))
    parser.add_argument("--endpoint", type=str, default=os.getenv("S3_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", type=str, default=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
    parser.add_argument("--secret-key", type=str, default=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))

    args = parser.parse_args()

    # -------------------------------------------------------------
    # Choose backend
    # -------------------------------------------------------------
    if args.backend == "file":
        storage = FileStorage(args.path)
        print(f"[bootstrap] Using FileStorage at {args.path}")

    else:  # args.backend == "s3"
        storage = S3Storage(
            bucket=args.bucket,
            export_name=args.export,
            endpoint_url=args.endpoint,
            aws_access_key_id=args.access_key,
            aws_secret_access_key=args.secret_key,
        )
        print(f"[bootstrap] Using S3Storage bucket={args.bucket} endpoint={args.endpoint}")

    # -------------------------------------------------------------
    # Create NbdServer
    # -------------------------------------------------------------
    server = NbdServer(
        export_name=args.export,
        total_size_bytes=args.size,
        storage=storage,
    )

    print(f"[bootstrap] Created NbdServer({args.export}, size={args.size})")

    return server

def verify_file_storage():
    backend = NbdServer(export_name="testdev", total_size_bytes=BLOCK_SIZE * 10)
    backend.write(0, b"hello world")
    print(backend.read(0, 11))

def verify_s3_bucket_storage():
    storage = S3Storage(
        bucket="nbdbuket",
        export_name="testdev",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    data = b"hello" + bytes(BLOCK_SIZE - 5)
    storage.write_block("testdev", 0, data)
    out = storage.read_block("testdev", 0)
    print(out[:10])  # b'hello\x00\x00\x00\x00\x00'
    print(len(out))  # 4096

if __name__ == "__main__":
    # Manual invocation for debugging
    server = create_server_from_args()
    print("Server ready.")